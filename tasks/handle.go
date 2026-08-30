package tasks

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/workflows"
)

// A Handle is a task in motion: the live process executing its workflow's
// graph, driven through this surface. CreateTask and RecoverTask hand one
// back; the durable side of the same task is the Task the Repository stores.
type Handle interface {
	ID() string
	// Assignment reports the placement this task leases the kind under: the
	// group, and the one resource it is pinned to within it. Both are resolved
	// at creation or recovery from the task's own assignment or its task
	// group's, and are "" for a kind the task leases nothing of.
	Assignment(kind string) (groupID, resourceID string)
	// Start executes the task synchronously until completion, error, or kill,
	// returning the workflow's output on clean completion. The output is nil if
	// the workflow produces none, or if the run errors or is killed. A run whose
	// terminal stamp fails to persist returns its output alongside the error.
	// A recovered task resumes from its persisted checkpoint instead of the
	// graph's initial state; one recovered from a suspended checkpoint starts
	// parked there until Resume or Kill.
	Start(ctx context.Context) ([]byte, error)
	// Suspend signals the task to park before processing the next state.
	// It is a no-op unless the task is running.
	Suspend() error
	// Resume continues a suspended task at the next state. It is a no-op
	// unless the task is suspended.
	Resume() error
	// Kill stops the task as soon as possible, cancelling the in-flight state.
	// A kill before Start latches: Start then refuses with context.Canceled.
	Kill() error
	// IsRunning reports whether the task is started and not yet terminal.
	IsRunning() bool
	// Status reports the task's lifecycle status. A recovered task that has
	// not been started reports its persisted status (e.g. suspended).
	Status() workflows.Status
}

type handle struct {
	id          string
	input       any
	assignments map[string]workflows.Assignment
	engine      *engine

	// recovered marks a task rebuilt from a snapshot; Start resumes at
	// resumeAt with snapshot instead of executing input from the beginning.
	recovered       bool
	snapshot        []byte
	resumeAt        workflows.State
	recoveredStatus workflows.Status
}

// createHandle validates input against the workflow and returns a new unstarted
// task whose instance leases each kind under assignments, already resolved.
// The caller must not modify input or assignments after creation.
func createHandle(workflow workflows.Workflow, input any, bus comms.Bus, repo Repository, assignments map[string]workflows.Assignment) (*handle, error) {
	if err := workflow.ValidateInput(input); err != nil {
		return nil, fmt.Errorf("task input validation error: %w", err)
	}

	id := uuid.NewString()
	return &handle{
		id:          id,
		input:       input,
		assignments: assignments,
		engine:      newEngine(workflow, workflows.Deps{TaskID: id, Bus: bus, Assignments: assignments}, repo),
	}, nil
}

// rehydrateHandle rebuilds a task from a persisted snapshot so Start resumes at
// resumeAt. status is the persisted lifecycle reported until the task starts.
func rehydrateHandle(workflow workflows.Workflow, id string, snapshot []byte, resumeAt workflows.State, status workflows.Status, bus comms.Bus, repo Repository, assignments map[string]workflows.Assignment) *handle {
	return &handle{
		id:              id,
		assignments:     assignments,
		engine:          newEngine(workflow, workflows.Deps{TaskID: id, Bus: bus, Assignments: assignments}, repo),
		recovered:       true,
		snapshot:        snapshot,
		resumeAt:        resumeAt,
		recoveredStatus: status,
	}
}

func (t *handle) ID() string {
	return t.id
}

func (t *handle) Assignment(kind string) (groupID, resourceID string) {
	a := t.assignments[kind]
	return a.GroupID, a.ResourceID
}

// seal latches the task closed for deletion, reporting false if it is live.
func (t *handle) seal() bool {
	return t.engine.seal()
}

// unseal reopens a task sealed for a deletion that was abandoned.
func (t *handle) unseal() {
	t.engine.unseal()
}

func (t *handle) Start(ctx context.Context) ([]byte, error) {
	var err error
	if t.recovered {
		if t.recoveredStatus == workflows.StatusNotStarted {
			return nil, fmt.Errorf("task %s: %w", t.id, ErrNoCheckpoint)
		}
		if t.recoveredStatus.Terminal() {
			return nil, fmt.Errorf("task %s is %s: %w", t.id, t.recoveredStatus, ErrAlreadyTerminal)
		}
		err = t.engine.Rehydrate(ctx, t.snapshot, t.resumeAt, t.recoveredStatus == workflows.StatusSuspended)
	} else {
		err = t.engine.Execute(ctx, t.input)
	}
	// Output is harvested only on clean completion, so error exits still yield
	// nil — except a failed terminal stamp, where the result exists and is
	// returned alongside the error.
	return t.engine.Output(), err
}

func (t *handle) IsRunning() bool {
	return t.engine.IsRunning()
}

func (t *handle) Status() workflows.Status {
	status := t.engine.Status()
	if status == workflows.StatusNotStarted && t.recovered {
		return t.recoveredStatus
	}
	return status
}

func (t *handle) Suspend() error {
	return t.engine.Suspend()
}

func (t *handle) Resume() error {
	return t.engine.Resume()
}

func (t *handle) Kill() error {
	return t.engine.Kill()
}
