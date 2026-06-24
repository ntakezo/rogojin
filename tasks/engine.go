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
	return e.run(ctx, instance, nil)
}

// Rehydrate rebuilds an instance from a snapshot and runs it from start.
// It is a no-op if the engine has already started.
func (e *engine) Rehydrate(ctx context.Context, snapshot []byte, start workflows.State) error {
	pw, ok := e.workflow.(workflows.PersistableWorkflow)
	if !ok {
		return fmt.Errorf("workflow %s is not persistable", e.workflow.ID())
	}
	instance, err := pw.RestoreInstance(e.deps, snapshot)
	if err != nil {
		return err
	}
	return e.run(ctx, instance, &start)
}

// run drives the instance from start (or the graph's initial state when start
// is nil) until completion, error, or kill, stamping the durable terminal
// outcome on exit.
func (e *engine) run(ctx context.Context, instance workflows.Instance, start *workflows.State) (err error) {
	e.mu.Lock()
	if e.state != workflows.StatusNotStarted {
		e.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	e.state = workflows.StatusRunning
	e.mu.Unlock()

	defer cancel()
	// teardown runs after finish (defers are LIFO) so it observes the stamped
	// terminal status, and on every exit path so acquired resources never leak.
	defer func() {
		if td, ok := instance.(workflows.Teardowner); ok {
			err = errors.Join(err, td.Teardown(context.Background(), e.Status(), err))
		}
	}()
	defer e.finish()

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
			for e.state == workflows.StatusSuspended {
				e.cond.Wait()
			}
		}
		dead := e.state == workflows.StatusKilled
		e.mu.Unlock()
		if dead {
			return context.Canceled
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

// Status reports the engine's current lifecycle status.
func (e *engine) Status() workflows.Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state
}

// IsRunning reports whether the engine is started and not yet terminal.
func (e *engine) IsRunning() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.state == workflows.StatusRunning || e.state == workflows.StatusSuspended
}

// Suspend signals the engine to park before the next state. No-op unless running.
func (e *engine) Suspend() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == workflows.StatusRunning {
		e.state = workflows.StatusSuspended
	}
	return nil
}

// Resume continues a suspended engine at the next state. No-op unless suspended.
func (e *engine) Resume() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == workflows.StatusSuspended {
		e.state = workflows.StatusRunning
		e.cond.Signal()
	}
	return nil
}

// Kill cancels the engine immediately, interrupting the in-flight state.
// No-op unless running or suspended.
func (e *engine) Kill() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state == workflows.StatusRunning || e.state == workflows.StatusSuspended {
		e.state = workflows.StatusKilled
		e.cancel()
		e.cond.Signal() // wake a suspended loop so it observes the kill
	}
	return nil
}

// finish moves a non-killed engine to done and stamps the durable terminal
// outcome. The record is never deleted here; removal is consumer-driven. The
// stamp uses a background context so it lands even after a kill's cancellation.
func (e *engine) finish() {
	e.mu.Lock()
	if e.state != workflows.StatusKilled {
		e.state = workflows.StatusDone
	}
	outcome := e.state
	output := e.output
	e.mu.Unlock()

	if e.repo == nil {
		return
	}
	_ = e.repo.MarkTerminal(context.Background(), e.deps.TaskID, string(outcome), output)
}
