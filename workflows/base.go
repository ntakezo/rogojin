package workflows

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/leasing"
)

// Base is the framework half of a workflow instance. Embed it (by value) in
// the instance's context struct and the common machinery comes with it: the
// task's deps behind accessors, the durable effect log behind Do and Once,
// and the snapshot envelope behind Snapshot — so the instance satisfies
// Snapshotter without a hand-written mirror struct, and recovery restores it
// without hand-written field copying.
//
// A Module binds Base when it builds or restores the instance; an instance
// constructed outside a Module seeds it with NewBase.
type Base struct {
	deps Deps
	// input is the task's input as the module marshaled it, carried in the
	// envelope so recovery can rebuild the instance from it.
	input json.RawMessage
	// effects caches the durable effect log: one marshaled result per Do/Once
	// key. The store is the authority — Deps.RecordEffect writes each result
	// the moment it lands, and recovery seeds this cache from Deps.Effects —
	// so a re-entered state skips recorded effects instead of repeating them.
	// With no store wired the cache is the whole log.
	effects map[string]json.RawMessage
	// durable points at the instance's durable state, registered via Persist;
	// nil means the envelope carries only input and effects.
	durable any
	// pending marks effect keys cached in memory whose durable record has
	// not landed — a RecordEffect that failed. A cache hit on a pending key
	// retries the record and keeps failing the caller until it lands, so a
	// state cannot report success while its effect's durability is still
	// owed.
	pending map[string]struct{}
}

// NewBase returns a Base bound to deps, for instances constructed outside a
// Module.
func NewBase(deps Deps) Base {
	return Base{deps: deps, effects: make(map[string]json.RawMessage)}
}

// base returns the embedded Base through however many levels of embedding
// promoted it — how this package finds the Base inside a user's instance.
func (b *Base) base() *Base { return b }

// hasBase is satisfied by any instance embedding Base, via promotion.
type hasBase interface{ base() *Base }

// bind attaches the task's deps and marshaled input to the instance, seeding
// the effect cache with the store's recorded effects when recovery carries
// them in.
func (b *Base) bind(deps Deps, input json.RawMessage) {
	b.deps = deps
	b.input = input
	if b.effects == nil {
		b.effects = make(map[string]json.RawMessage)
	}
	for key, result := range deps.Effects {
		b.effects[key] = json.RawMessage(result)
	}
}

// TaskID returns the task this instance runs under.
func (b *Base) TaskID() string { return b.deps.TaskID }

// Bus returns the bus the task coordinates with other tasks on.
func (b *Base) Bus() comms.Bus { return b.deps.Bus }

// Assignment returns the task's resolved placement for kind. A kind with no
// placement reads as the zero Assignment, so lookups need no branching.
func (b *Base) Assignment(kind leasing.Kind) Assignment {
	return b.deps.Assignments[kind]
}

// Checkpoint persists the instance's snapshot mid-state through
// Deps.Checkpoint, stamped at the state currently executing. It is a no-op
// returning nil when no checkpoint is wired — a Base built by hand.
func (b *Base) Checkpoint(ctx context.Context) error {
	if b.deps.Checkpoint == nil {
		return nil
	}
	return b.deps.Checkpoint(ctx)
}

// Persist registers a pointer to the instance's durable state, making it part
// of the snapshot envelope: Snapshot marshals through it, and recovery
// unmarshals back into it. Call it once, in the instance's constructor.
func (b *Base) Persist(durable any) { b.durable = durable }

// envelope is the snapshot's wire shape: the module-marshaled input and the
// instance's registered durable state. Effects is decode-only legacy — the
// effect log lived in the envelope before it moved to the store, so old
// snapshots still carry one and restore still honors it, but Snapshot no
// longer writes it.
type envelope struct {
	Input   json.RawMessage            `json:"input,omitempty"`
	Effects map[string]json.RawMessage `json:"effects,omitempty"`
	Durable json.RawMessage            `json:"durable,omitempty"`
}

// Snapshot implements Snapshotter over the envelope. The engine calls it
// before entering each state; like every snapshot it must be valid as the
// entry of the state it is taken before, which holds by construction — input
// and durable state are exactly what recovery rebuilds from, and the effect
// log lives in the store.
func (b *Base) Snapshot() ([]byte, error) {
	env := envelope{Input: b.input}
	if b.durable != nil {
		blob, err := json.Marshal(b.durable)
		if err != nil {
			return nil, fmt.Errorf("marshal durable state: %w", err)
		}
		env.Durable = blob
	}
	return json.Marshal(env)
}

// restore fills the effect cache and the registered durable state from a
// decoded envelope. The instance's constructor has already run, so Persist
// has already registered the durable pointer this unmarshals into.
//
// A legacy envelope still carries its effect log. Store-seeded entries win —
// the store is the authority — and every key the store lacks is written
// through RecordEffect here, eagerly: the next Snapshot no longer emits
// effects, so the first new checkpoint would erase the envelope copy, and a
// legacy effect left unmigrated until then would re-fire after the next
// crash. The write-through runs under a background context because recovery
// drives restore through RestoreInstance, which carries none.
func (b *Base) restore(env envelope) error {
	for key, result := range env.Effects {
		if _, seeded := b.effects[key]; seeded {
			continue
		}
		recorded := result
		if b.deps.RecordEffect != nil {
			stored, _, err := b.deps.RecordEffect(context.Background(), key, result)
			if err != nil {
				return fmt.Errorf("migrate recorded effect %q: %w", key, err)
			}
			recorded = stored
		}
		b.effects[key] = recorded
	}
	if b.durable != nil && len(env.Durable) > 0 {
		if err := json.Unmarshal(env.Durable, b.durable); err != nil {
			return fmt.Errorf("restore durable state: %w", err)
		}
	}
	return nil
}

// Do runs effect at most once per key for the life of the task, persisting
// its result in the task's effect log. The first call runs the effect and
// records the marshaled result durably the moment it lands; every later call
// — an in-process retry, a recovered task re-entering the state, or another
// node's run of the same task — returns the recorded result without running
// the effect. It is the guard for an external side effect a re-run must not
// repeat, and the recorded result is what the replayed state builds on, so
// no separate guard field or result field is declared. T must round-trip
// through JSON.
//
// A failed effect records nothing, so a retry re-runs it. When two runs of
// one task race the same key, exactly one result is recorded and both calls
// return it — the loser's own result is discarded, because whatever built on
// the recorded one elsewhere is what must not be contradicted. A failed
// record returns the error with the result cached only in memory: fail the
// state, and the retry that re-enters it skips the effect through the cache
// but keeps failing until the record lands — success is never reported while
// the effect's durability is still owed. With no store wired the cache is
// the whole log, living in the snapshot era's crash window knowingly.
func Do[T any](ctx context.Context, b *Base, key string, effect func(context.Context) (T, error)) (T, error) {
	var zero T
	if cached, ok := b.effects[key]; ok {
		recorded, err := b.settle(ctx, key, cached)
		if err != nil {
			return zero, err
		}
		var v T
		if err := json.Unmarshal(recorded, &v); err != nil {
			return zero, fmt.Errorf("effect %q: decode recorded result: %w", key, err)
		}
		return v, nil
	}
	v, err := effect(ctx)
	if err != nil {
		return zero, err
	}
	blob, err := json.Marshal(v)
	if err != nil {
		return zero, fmt.Errorf("effect %q: marshal result: %w", key, err)
	}
	if b.effects == nil {
		b.effects = make(map[string]json.RawMessage)
	}
	if b.deps.RecordEffect != nil {
		stored, first, rerr := b.deps.RecordEffect(ctx, key, blob)
		if rerr != nil {
			b.effects[key] = blob
			if b.pending == nil {
				b.pending = make(map[string]struct{})
			}
			b.pending[key] = struct{}{}
			return v, fmt.Errorf("effect %q: record: %w", key, rerr)
		}
		if !first {
			// A racing run of this task recorded first; its result is the
			// one every replay builds on, so ours is discarded.
			b.effects[key] = json.RawMessage(stored)
			var recorded T
			if err := json.Unmarshal(stored, &recorded); err != nil {
				return zero, fmt.Errorf("effect %q: decode recorded result: %w", key, err)
			}
			return recorded, nil
		}
	}
	b.effects[key] = blob
	return v, nil
}

// settle resolves a cache hit: a settled key returns its bytes, a pending one
// retries the durable record first — returning the store's bytes if a racing
// run won — and keeps the caller failing until the record lands.
func (b *Base) settle(ctx context.Context, key string, cached json.RawMessage) (json.RawMessage, error) {
	if _, owed := b.pending[key]; !owed || b.deps.RecordEffect == nil {
		return cached, nil
	}
	stored, _, err := b.deps.RecordEffect(ctx, key, cached)
	if err != nil {
		return nil, fmt.Errorf("effect %q: record: %w", key, err)
	}
	delete(b.pending, key)
	b.effects[key] = json.RawMessage(stored)
	return json.RawMessage(stored), nil
}

// Once is Do for an effect with no result: run it once per key, durably.
func (b *Base) Once(ctx context.Context, key string, effect func(context.Context) error) error {
	_, err := Do(ctx, b, key, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, effect(ctx)
	})
	return err
}
