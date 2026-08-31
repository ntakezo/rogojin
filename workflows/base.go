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
	// effects is the durable effect log: one marshaled result per Do/Once key,
	// snapshotted with the instance so a re-entered state skips recorded
	// effects instead of repeating them.
	effects map[string]json.RawMessage
	// durable points at the instance's durable state, registered via Persist;
	// nil means the envelope carries only input and effects.
	durable any
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

// bind attaches the task's deps and marshaled input to the instance.
func (b *Base) bind(deps Deps, input json.RawMessage) {
	b.deps = deps
	b.input = input
	if b.effects == nil {
		b.effects = make(map[string]json.RawMessage)
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

// envelope is the snapshot's wire shape: the module-marshaled input, the
// effect log, and the instance's registered durable state.
type envelope struct {
	Input   json.RawMessage            `json:"input,omitempty"`
	Effects map[string]json.RawMessage `json:"effects,omitempty"`
	Durable json.RawMessage            `json:"durable,omitempty"`
}

// Snapshot implements Snapshotter over the envelope. The engine calls it
// before entering each state; like every snapshot it must be valid as the
// entry of the state it is taken before, which holds by construction — input,
// effects, and durable state are exactly what recovery rebuilds from.
func (b *Base) Snapshot() ([]byte, error) {
	env := envelope{Input: b.input, Effects: b.effects}
	if b.durable != nil {
		blob, err := json.Marshal(b.durable)
		if err != nil {
			return nil, fmt.Errorf("marshal durable state: %w", err)
		}
		env.Durable = blob
	}
	return json.Marshal(env)
}

// restore fills the effect log and the registered durable state from a decoded
// envelope. The instance's constructor has already run, so Persist has already
// registered the durable pointer this unmarshals into.
func (b *Base) restore(env envelope) error {
	if env.Effects != nil {
		b.effects = env.Effects
	} else if b.effects == nil {
		b.effects = make(map[string]json.RawMessage)
	}
	if b.durable != nil && len(env.Durable) > 0 {
		if err := json.Unmarshal(env.Durable, b.durable); err != nil {
			return fmt.Errorf("restore durable state: %w", err)
		}
	}
	return nil
}

// Do runs effect at most once per key for the life of the task, persisting its
// result in the instance's effect log. The first call runs the effect, records
// the marshaled result, and checkpoints; every later call — an in-process
// retry, or a recovered task re-entering the state — returns the recorded
// result without running the effect. It is the guard for an external side
// effect a re-run must not repeat, and the recorded result is what the
// replayed state builds on, so no separate guard field or result field is
// declared. T must round-trip through JSON.
//
// A failed effect records nothing, so a retry re-runs it. A failed checkpoint
// returns the error with the result recorded only in memory: fail the state,
// and the retry that re-enters it still skips the effect through the
// in-memory log, while the next successful checkpoint makes the record
// durable — only a crash before then repeats the effect.
func Do[T any](ctx context.Context, b *Base, key string, effect func(context.Context) (T, error)) (T, error) {
	var zero T
	if cached, ok := b.effects[key]; ok {
		var v T
		if err := json.Unmarshal(cached, &v); err != nil {
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
	b.effects[key] = blob
	if cerr := b.Checkpoint(ctx); cerr != nil {
		return v, fmt.Errorf("effect %q: checkpoint: %w", key, cerr)
	}
	return v, nil
}

// Once is Do for an effect with no result: run it once per key, durably.
func (b *Base) Once(ctx context.Context, key string, effect func(context.Context) error) error {
	_, err := Do(ctx, b, key, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, effect(ctx)
	})
	return err
}
