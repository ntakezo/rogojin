package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/workflows"
)

// engine drives a workflow's graph: it runs an instance from state to state,
// honoring suspend/kill at each boundary and checkpointing persistable
// instances before every state. It is the private runtime owned by a task.
type engine struct {
	workflow workflows.Workflow
	deps     workflows.Deps
	repo     Repository

	mu     sync.Mutex
	cond   *sync.Cond
	state  workflows.Status
	cancel context.CancelFunc
	output []byte
	// version and node are the run's claim: every conditional write carries
	// them, and version tracks the store's bumps. A run whose write returns
	// ErrStale was usurped and must stop side-effecting.
	version int64
	node    string
	// sealed latches the engine closed for deletion: Start refuses, so a task
	// cannot begin running out from under the sweep that is removing it.
	sealed bool
	// detachWanted latches a Detach: the run exits at the suspend boundary
	// instead of parking. detachDone marks that exit: the handle is dead, the
	// durable record still suspended. Liveness keys off detachDone alone — a
	// pending detach whose handler is still in flight must stay live, or a
	// deletion sweep could remove a task that is still checkpointing.
	detachWanted bool
	detachDone   bool
	// current is the state whose handler is executing, and snap the run's
	// snapshot capability — what Deps.Checkpoint reads to persist mid-state
	// progress stamped at the right state.
	current workflows.State
	snap    snapshotState
}

func newEngine(workflow workflows.Workflow, deps workflows.Deps, repo Repository) *engine {
	e := &engine{workflow: workflow, repo: repo}
	// The instance's mid-state checkpoint and effect recorder ride in on
	// Deps, so a handler can make progress and effects durable the moment
	// they land.
	deps.Checkpoint = e.midCheckpoint
	if repo != nil {
		taskID := deps.TaskID
		deps.RecordEffect = func(ctx context.Context, key string, result []byte) ([]byte, bool, error) {
			return repo.RecordEffect(ctx, taskID, key, result)
		}
	}
	e.deps = deps
	e.cond = sync.NewCond(&e.mu)
	return e
}

// Execute builds a fresh instance and runs it until completion or error.
// It refuses with ErrAlreadyStarted if the engine has already started.
func (e *engine) Execute(ctx context.Context, input any) error {
	instance, err := e.workflow.NewInstance(input, e.deps)
	if err != nil {
		return err
	}
	return e.run(ctx, instance, nil, false)
}

// ExecuteStored builds a fresh instance from the record's persisted input and
// runs it from the graph's initial state — the dispatch path, where the node
// running the task holds only the record, never the typed input value. The
// workflow must be dispatchable, since only the module knows how to rebuild
// an In from its marshaled form. The stored effect log is seeded the same way
// recovery seeds it: a not-started record cannot normally have recorded
// effects, but reading them costs one query and keeps even an improbable
// history from re-firing one. It refuses with ErrAlreadyStarted if the engine
// has already started.
func (e *engine) ExecuteStored(ctx context.Context, input json.RawMessage) error {
	dw, ok := e.workflow.(workflows.DispatchableWorkflow)
	if !ok {
		return fmt.Errorf("workflow %s is not dispatchable", e.workflow.ID())
	}
	if e.repo != nil {
		effects, err := e.repo.ListEffects(ctx, e.deps.TaskID)
		if err != nil {
			return fmt.Errorf("list effects: %w", err)
		}
		e.deps.Effects = effects
	}
	instance, err := dw.NewStoredInstance(input, e.deps)
	if err != nil {
		return err
	}
	return e.run(ctx, instance, nil, false)
}

// Rehydrate rebuilds an instance from a snapshot and runs it from start. With
// suspended set it parks before start instead, honoring a persisted suspend
// until Resume or Kill. It refuses with ErrAlreadyStarted if the engine has
// already started.
func (e *engine) Rehydrate(ctx context.Context, snapshot []byte, start workflows.State, suspended bool) error {
	pw, ok := e.workflow.(workflows.PersistableWorkflow)
	if !ok {
		return fmt.Errorf("workflow %s is not persistable", e.workflow.ID())
	}
	// The store's effect log rides in on Deps so the rebuilt instance skips
	// recorded effects — including ones a checkpoint never saw, recorded in
	// the crash window between an effect and the next snapshot.
	if e.repo != nil {
		effects, err := e.repo.ListEffects(ctx, e.deps.TaskID)
		if err != nil {
			return fmt.Errorf("list effects: %w", err)
		}
		e.deps.Effects = effects
	}
	instance, err := pw.RestoreInstance(e.deps, snapshot)
	if err != nil {
		return err
	}
	return e.run(ctx, instance, &start, suspended)
}

// run drives the instance from start (or the graph's initial state when start
// is nil) until completion, error, or kill, stamping the durable terminal
// outcome on exit — except a detached exit, which leaves the suspended
// checkpoint as the durable truth. With suspended set it begins parked at
// start, so a recovered suspend resumes paused exactly where it left off.
func (e *engine) run(ctx context.Context, instance workflows.Instance, start *workflows.State, suspended bool) (err error) {
	e.mu.Lock()
	if e.sealed {
		e.mu.Unlock()
		return ErrTaskDeleted
	}
	if e.state != workflows.StatusNotStarted {
		// a kill latched before the first Start wins: the task never runs.
		killed := e.state == workflows.StatusKilled
		e.mu.Unlock()
		if killed {
			return context.Canceled
		}
		// Some Start already owns this engine — running, finished, or detached.
		// Refusing loudly keeps a double dispatch from reading as an instant
		// clean completion.
		return ErrAlreadyStarted
	}
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	if suspended {
		e.state = workflows.StatusSuspended
	} else {
		e.state = workflows.StatusRunning
	}
	e.mu.Unlock()

	defer cancel()
	// A stamp failure joins the returned error only after teardown runs, so
	// teardown sees the workflow's own error, not the persistence failure.
	var stampErr error
	defer func() { err = errors.Join(err, stampErr) }()
	// teardown runs after finish (defers are LIFO) so it observes the stamped
	// terminal status, and on every exit path so acquired resources never leak.
	defer func() {
		if td, ok := instance.(workflows.Teardowner); ok {
			err = errors.Join(err, td.Teardown(context.Background(), e.Status(), err))
		}
	}()
	defer func() { stampErr = e.finish(err) }()

	graph := instance.Graph()
	snapshotter, canSnapshot := instance.(workflows.Snapshotter)

	var next *workflows.State
	if start != nil {
		next = start
	} else {
		initial := graph.Initial()
		next = &initial
	}

	snap := snapshotState{snapshotter, canSnapshot}
	e.mu.Lock()
	e.snap = snap
	e.mu.Unlock()

	for {
		// boundary: honor suspend/kill after the previous state, before the next.
		e.mu.Lock()
		// no handler is executing between states, so a mid-state checkpoint
		// arriving now must refuse rather than stamp a stale resume point.
		e.current = ""
		if next != nil && e.state == workflows.StatusSuspended {
			// persist the suspend durably so recovery brings the task back
			// paused at the next unprocessed state.
			e.mu.Unlock()
			if serr := e.checkpoint(ctx, workflows.StatusSuspended, *next, snap); serr != nil {
				return serr
			}
			e.mu.Lock()
			for e.state == workflows.StatusSuspended && !e.detachWanted {
				e.cond.Wait()
			}
		}
		dead := e.state == workflows.StatusKilled
		detached := e.state == workflows.StatusSuspended && e.detachWanted
		if detached {
			e.detachDone = true
		}
		e.mu.Unlock()
		if dead {
			// a kill racing a pending detach wins: the operator asked to stop.
			return context.Canceled
		}
		if detached {
			return ErrDetached
		}

		if next == nil {
			// clean completion: harvest the instance's output so the run can
			// return it and finish can persist it with the terminal stamp.
			if herr := e.harvest(instance); herr != nil {
				return herr
			}
			return nil
		}

		handler := graph.Handler(*next)
		if handler == nil {
			return fmt.Errorf("state %v does not exist", *next)
		}

		// checkpoint before entering the state so recovery resumes here. A
		// checkpoint failure aborts the run; the last good checkpoint stands,
		// so the task stays resumable from there.
		if serr := e.checkpoint(ctx, workflows.StatusRunning, *next, snap); serr != nil {
			return serr
		}

		e.mu.Lock()
		e.current = *next
		e.mu.Unlock()
		next, err = handler(ctx)
		if err != nil {
			return err
		}
	}
}

// snapshotState bundles the snapshot capability resolved once per run.
type snapshotState struct {
	snapshotter workflows.Snapshotter
	canSnapshot bool
}

// midCheckpoint is the engine side of Deps.Checkpoint: it persists the
// instance's snapshot stamped running at the state currently executing, so a
// handler can make an effect's success durable before continuing — recovery
// then re-enters the state with that progress restored instead of repeating
// the effect. It refuses when no state is executing, since a stray call after
// the run exits would overwrite the durable outcome with a stale running
// stamp. Like every checkpoint, it is a no-op for instances that cannot
// snapshot or when no repository is wired.
func (e *engine) midCheckpoint(ctx context.Context) error {
	e.mu.Lock()
	live := (e.state == workflows.StatusRunning || e.state == workflows.StatusSuspended) && !e.detachDone
	state := e.current
	snap := e.snap
	e.mu.Unlock()
	if !live || state == "" {
		return errors.New("checkpoint: no state is executing")
	}
	return e.checkpoint(ctx, workflows.StatusRunning, state, snap)
}

// claim takes (or re-takes) the store claim for node and seeds the run's
// version from the claimed record, returning it so Start can refresh a
// recovered handle's resume point. It is a no-op without a repository.
func (e *engine) claim(ctx context.Context, node string, ttl time.Duration) (Task, error) {
	if e.repo == nil {
		return Task{}, nil
	}
	record, err := e.repo.ClaimTask(ctx, e.deps.TaskID, node, ttl)
	if err != nil {
		return Task{}, err
	}
	e.mu.Lock()
	e.version, e.node = record.Version, node
	e.mu.Unlock()
	return record, nil
}

// releaseClaim frees the store claim on exits that stamp no terminal — a
// detach, or a recovered handle refusing to run — so another node can take
// the task now rather than after the lease expires. Best-effort on a
// background context: a release racing its own usurpation is a no-op by the
// port's contract.
func (e *engine) releaseClaim() error {
	e.mu.Lock()
	node := e.node
	e.mu.Unlock()
	if e.repo == nil || node == "" {
		return nil
	}
	return e.repo.ReleaseClaim(context.Background(), e.deps.TaskID, node)
}

// checkpoint persists a snapshot stamped with status for the state about to be
// entered, carrying the run's claim. It is a no-op for instances that cannot
// snapshot or when no repository is wired. ErrStale means another node took
// the task after this one's lease expired: the run kills itself so it stops
// side-effecting — the usurper owns the record, and every write from here
// would lose the same way.
func (e *engine) checkpoint(ctx context.Context, status workflows.Status, state workflows.State, snap snapshotState) error {
	if !snap.canSnapshot || e.repo == nil {
		return nil
	}

	blob, err := snap.snapshotter.Snapshot()
	if err != nil {
		return err
	}
	e.mu.Lock()
	version, node := e.version, e.node
	e.mu.Unlock()
	newVersion, err := e.repo.SaveCheckpoint(ctx, e.deps.TaskID, version, node, string(status), string(state), blob)
	if err != nil {
		if errors.Is(err, ErrStale) {
			e.Kill()
			return fmt.Errorf("task %s usurped by another node: %w", e.deps.TaskID, err)
		}
		return err
	}
	e.mu.Lock()
	e.version = newVersion
	e.mu.Unlock()
	return nil
}

// harvest captures the instance's result on clean completion via the optional
// Outputter capability, so the run can return it and finish can persist it. It
// is a no-op for instances that produce no output. An Output failure aborts the
// run: a result that cannot be produced is a failure, not an empty success.
func (e *engine) harvest(instance workflows.Instance) error {
	out, ok := instance.(workflows.Outputter)
	if !ok {
		return nil
	}
	blob, err := out.Output()
	if err != nil {
		return fmt.Errorf("harvest output: %w", err)
	}
	e.mu.Lock()
	e.output = blob
	e.mu.Unlock()
	return nil
}

// Output returns the result harvested on clean completion, or nil if the run did
// not complete cleanly or the workflow produces no output.
func (e *engine) Output() []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.output
}

// seal latches the engine closed so a later Start refuses, reporting false —
// and sealing nothing — if the engine is live. Deletion seals every task it is
// about to remove, because Start never goes through the manager and could
// otherwise begin a run mid-sweep.
func (e *engine) seal() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if (e.state == workflows.StatusRunning || e.state == workflows.StatusSuspended) && !e.detachDone {
		return false
	}
	e.sealed = true
	return true
}

// unseal reopens an engine sealed for a deletion that was abandoned.
func (e *engine) unseal() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.sealed = false
}

// Status reports the engine's current lifecycle status.
func (e *engine) Status() workflows.Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state
}

// IsRunning reports whether the engine is started and not yet terminal. A
// detached engine is not live: its run has exited, even though its status
// truthfully remains suspended.
func (e *engine) IsRunning() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return (e.state == workflows.StatusRunning || e.state == workflows.StatusSuspended) && !e.detachDone
}

// Suspend signals the engine to park before the next state. No-op unless
// running; a detached or detaching engine is inert.
func (e *engine) Suspend() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.detachWanted || e.detachDone {
		return nil
	}
	if e.state == workflows.StatusRunning {
		e.state = workflows.StatusSuspended
	}
	return nil
}

// Resume continues a suspended engine at the next state. No-op unless
// suspended; a detached or detaching engine is inert — resuming one would
// mark a dead handle running forever, or unlatch a detach into some
// unrelated future suspend.
func (e *engine) Resume() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.detachWanted || e.detachDone {
		return nil
	}
	if e.state == workflows.StatusSuspended {
		e.state = workflows.StatusRunning
		e.cond.Signal()
	}
	return nil
}

// Detach asks a suspended run to exit at its park point, keeping the durable
// suspended checkpoint as the task's resume point. It refuses unless the
// engine is suspended; calling it again after a detach is a no-op. A
// Suspend/Detach pair issued while a handler is still in flight is honored in
// order: the run reaches the boundary, persists the suspended checkpoint, and
// only then observes the detach — so the exit is always recoverable.
func (e *engine) Detach() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.detachWanted || e.detachDone {
		return nil
	}
	if e.state != workflows.StatusSuspended {
		return fmt.Errorf("cannot detach: task is %q, want suspended", e.state)
	}
	e.detachWanted = true
	e.cond.Signal()
	return nil
}

// Kill cancels the engine immediately, interrupting the in-flight state. A
// kill before Start latches so a later Start refuses to run. No-op once
// terminal or detached.
func (e *engine) Kill() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.detachDone {
		return nil
	}
	switch e.state {
	case workflows.StatusNotStarted:
		e.state = workflows.StatusKilled
	case workflows.StatusRunning, workflows.StatusSuspended:
		e.state = workflows.StatusKilled
		e.cancel()
		e.cond.Signal() // wake a suspended loop so it observes the kill
	}
	return nil
}

// finish stamps the run's durable outcome: killed stays killed, an errored run
// is stamped failed (still recoverable from its last checkpoint), a clean one
// done. A detached exit stamps nothing — the suspended checkpoint written at
// the park boundary is the durable truth — but releases the claim, so another
// node can take the task now rather than after the lease expires. The record
// is never deleted here; removal is consumer-driven. The stamp uses a
// background context so it lands even after a kill's cancellation, and
// carries the run's claim: ErrStale means a usurper owns the record, whose
// own outcome must stand — the stamp is skipped, not forced. A stamp failure
// is returned so the run surfaces it rather than silently reporting a durable
// outcome that never landed.
func (e *engine) finish(runErr error) error {
	e.mu.Lock()
	if e.detachDone {
		e.mu.Unlock()
		if err := e.releaseClaim(); err != nil {
			return fmt.Errorf("release claim: %w", err)
		}
		return nil
	}
	if e.state != workflows.StatusKilled {
		if runErr != nil {
			e.state = workflows.StatusFailed
		} else {
			e.state = workflows.StatusDone
		}
	}
	outcome := e.state
	output := e.output
	version, node := e.version, e.node
	e.mu.Unlock()

	if e.repo == nil {
		return nil
	}
	if _, err := e.repo.MarkTerminal(context.Background(), e.deps.TaskID, version, node, string(outcome), output); err != nil {
		if errors.Is(err, ErrStale) {
			return fmt.Errorf("task %s usurped by another node; its outcome stands: %w", e.deps.TaskID, err)
		}
		return fmt.Errorf("mark terminal: %w", err)
	}
	return nil
}
