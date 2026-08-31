package tasks

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
}

func newEngine(workflow workflows.Workflow, deps workflows.Deps, repo Repository) *engine {
	e := &engine{workflow: workflow, deps: deps, repo: repo}
	e.cond = sync.NewCond(&e.mu)
	return e
}

// Execute builds a fresh instance and runs it until completion or error.
// It is a no-op if the engine has already started.
func (e *engine) Execute(ctx context.Context, input any) error {
	instance, err := e.workflow.NewInstance(input, e.deps)
	if err != nil {
		return err
	}
	return e.run(ctx, instance, nil, false)
}

// Rehydrate rebuilds an instance from a snapshot and runs it from start. With
// suspended set it parks before start instead, honoring a persisted suspend
// until Resume or Kill. It is a no-op if the engine has already started.
func (e *engine) Rehydrate(ctx context.Context, snapshot []byte, start workflows.State, suspended bool) error {
	pw, ok := e.workflow.(workflows.PersistableWorkflow)
	if !ok {
		return fmt.Errorf("workflow %s is not persistable", e.workflow.ID())
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
		return nil
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

	for {
		// boundary: honor suspend/kill after the previous state, before the next.
		e.mu.Lock()
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

// checkpoint persists a snapshot stamped with status for the state about to be
// entered. It is a no-op for instances that cannot snapshot or when no
// repository is wired.
func (e *engine) checkpoint(ctx context.Context, status workflows.Status, state workflows.State, snap snapshotState) error {
	if !snap.canSnapshot || e.repo == nil {
		return nil
	}

	blob, err := snap.snapshotter.Snapshot()
	if err != nil {
		return err
	}
	return e.repo.SaveCheckpoint(ctx, e.deps.TaskID, string(status), string(state), blob)
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
// done. A detached exit stamps nothing: the suspended checkpoint written at
// the park boundary is the durable truth, and a terminal stamp would overwrite
// it into an unrecoverable record. The record is never deleted here; removal
// is consumer-driven. The stamp uses a background context so it lands even
// after a kill's cancellation; a stamp failure is returned so the run surfaces
// it rather than silently reporting a durable outcome that never landed.
func (e *engine) finish(runErr error) error {
	e.mu.Lock()
	if e.detachDone {
		e.mu.Unlock()
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
	e.mu.Unlock()

	if e.repo == nil {
		return nil
	}
	if err := e.repo.MarkTerminal(context.Background(), e.deps.TaskID, string(outcome), output); err != nil {
		return fmt.Errorf("mark terminal: %w", err)
	}
	return nil
}
