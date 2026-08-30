// Package tasks creates and manages tasks: long-running processes that execute
// workflow graphs with durable checkpoints and recovery. Tasks are collected
// into named groups, and a task or a whole group may be assigned, per resource
// kind, the group its runs lease from — a task assigned none of a kind runs
// without that resource.
//
// A kind is just a string naming one leasing manager — each resource package
// publishes its own (proxies.Kind, accounts.Kind, payments.Kind). This package
// stores and resolves placements under it and never interprets it, so a new
// resource kind needs no change here.
package tasks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/workflows"
)

// ErrNoCheckpoint is returned by Start on a recovered task that was created but
// never checkpointed. Task input is not persisted — durability begins at the
// first checkpoint — so there is nothing to resume from; the task can only be
// deleted and re-created.
var ErrNoCheckpoint = errors.New("task has no checkpoint to resume from")

// ErrTaskDeleted is returned by Start on a task the manager has deleted, or is
// in the middle of deleting. Its record is gone, so a run would checkpoint into
// nothing; the handle is dead and the task must be re-created.
var ErrTaskDeleted = errors.New("task deleted")

// ErrAlreadyTerminal is returned by Start on a task recovered with a terminal
// outcome (done or killed): its run is over, and re-executing the final state
// would duplicate side effects. A failed task is not terminal and may be
// retried from its last checkpoint.
var ErrAlreadyTerminal = errors.New("task already reached a terminal outcome")

// A Task is one task, whole: the durable record of its workflow, its
// placement (task group, plus a per-kind resource assignment), its
// last-checkpointed status and resume state with the snapshot taken there —
// and, when built by a Manager, the live run surface that starts and steers
// it. A kind absent from Assignments inherits the task group's assignment for
// that kind; see Assignment for what a stored one means. Status holds the
// lifecycle as of the last checkpoint or, once the run exits, the terminal
// outcome. The framework never deletes a record on its own.
//
// Repositories store and return the record alone — a Task built from a bare
// record has no runtime, and only a Manager attaches one. The exported fields
// are written at creation, at recovery, and by Start when the run exits, and
// belong to the goroutine driving the task; Suspend, Resume, Kill, and
// IsRunning are live and safe from any goroutine.
type Task struct {
	ID          string                      `json:"id"`
	WorkflowID  string                      `json:"workflowId"`
	GroupID     string                      `json:"groupId"`
	Assignments map[leasing.Kind]Assignment `json:"assignments,omitempty"`
	State       string                      `json:"state"`
	Snapshot    []byte                      `json:"snapshot,omitempty"`
	Status      string                      `json:"status"`
	// Output is the workflow's result, persisted when the run completes cleanly;
	// nil for tasks that have not finished or produce no output.
	Output    []byte    `json:"output,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`

	// The live run surface, attached by the Manager and absent on a bare
	// record. input feeds a fresh run; resolved is the effective placement per
	// kind with group inheritance applied; recovered marks a task rebuilt from
	// its record, so Start resumes at State from Snapshot instead of executing
	// input from the beginning.
	input     any
	resolved  map[leasing.Kind]workflows.Assignment
	engine    *engine
	recovered bool
}

// Repository is the persistence port the consumer implements: a dumb store of
// task records and task groups with no liveness logic (the manager infers
// running tasks via IsRunning). Implementations must refresh the record's
// UpdatedAt on every write, including checkpoints and terminal stamps. A nil
// Repository is allowed and selects purely in-memory operation — see
// NewManager.
type Repository interface {
	CreateTask(ctx context.Context, record Task) error
	SaveCheckpoint(ctx context.Context, id string, status string, state string, snapshot []byte) error
	MarkTerminal(ctx context.Context, id string, outcome string, output []byte) error
	RecoverTask(ctx context.Context, id string) (Task, error)
	// SaveAssignment repoints a task's placement for one kind, leaving every
	// other kind and the rest of the record untouched. A nil field clears the
	// stored value.
	SaveAssignment(ctx context.Context, id string, kind leasing.Kind, assignment Assignment) error
	RecoverAll(ctx context.Context) ([]Task, error)
	DeleteTask(ctx context.Context, id string) error
	SaveGroup(ctx context.Context, group Group) error
	// GetGroup reports the group and whether it exists, so a missing group is
	// not conflated with a store failure.
	GetGroup(ctx context.Context, id string) (Group, bool, error)
	ListGroups(ctx context.Context) ([]Group, error)
	DeleteGroup(ctx context.Context, id string) error
	// TasksInGroup returns the ids of every task in the group.
	TasksInGroup(ctx context.Context, groupID string) ([]string, error)
}

// createTask validates input against the workflow and returns a new unstarted
// task whose instance leases each kind under resolved, already settled against
// the task group. The caller must not modify input or the maps after creation.
func createTask(workflow workflows.Workflow, workflowID string, input any, bus comms.Bus, repo Repository, groupID string, stored map[leasing.Kind]Assignment, resolved map[leasing.Kind]workflows.Assignment) (*Task, error) {
	if err := workflow.ValidateInput(input); err != nil {
		return nil, fmt.Errorf("task input validation error: %w", err)
	}

	now := time.Now().UTC()
	t := &Task{
		ID:          uuid.NewString(),
		WorkflowID:  workflowID,
		GroupID:     groupID,
		Assignments: stored,
		CreatedAt:   now,
		UpdatedAt:   now,
		input:       input,
		resolved:    resolved,
	}
	t.engine = newEngine(workflow, workflows.Deps{TaskID: t.ID, Bus: bus, Assignments: resolved}, repo)
	return t, nil
}

// rehydrateTask rebuilds an unstarted task from its persisted record so Start
// resumes at its State from its Snapshot, reporting its persisted Status until
// then.
func rehydrateTask(workflow workflows.Workflow, record Task, bus comms.Bus, repo Repository, resolved map[leasing.Kind]workflows.Assignment) *Task {
	t := record
	t.input = nil
	t.resolved = resolved
	t.engine = newEngine(workflow, workflows.Deps{TaskID: t.ID, Bus: bus, Assignments: resolved}, repo)
	t.recovered = true
	return &t
}

// record returns the task's durable record alone, the run surface stripped, so
// a repository never holds live engine state.
func (t *Task) record() Task {
	rec := *t
	rec.input, rec.resolved, rec.engine, rec.recovered = nil, nil, nil, false
	return rec
}

// Assignment reports the placement this task leases the kind under: the group,
// and the one resource it is pinned to within it. Both are resolved at creation
// or recovery from the task's own assignment or its task group's, and are ""
// for a kind the task leases nothing of.
func (t *Task) Assignment(kind leasing.Kind) (groupID, resourceID string) {
	a := t.resolved[kind]
	return a.GroupID, a.ResourceID
}

// seal latches the task closed for deletion, reporting false if it is live.
func (t *Task) seal() bool {
	return t.engine.seal()
}

// unseal reopens a task sealed for a deletion that was abandoned.
func (t *Task) unseal() {
	t.engine.unseal()
}

// Start executes the task synchronously until completion, error, or kill,
// returning the workflow's output on clean completion and bringing Status,
// Output, and UpdatedAt current on exit. The output is nil if the workflow
// produces none, or if the run errors or is killed. A run whose terminal stamp
// fails to persist returns its output alongside the error. A recovered task
// resumes from its persisted checkpoint instead of the graph's initial state;
// one recovered from a suspended checkpoint starts parked there until Resume
// or Kill. A task built from a bare record has no runtime and refuses; create
// or recover it through a Manager.
func (t *Task) Start(ctx context.Context) ([]byte, error) {
	if t.engine == nil {
		return nil, fmt.Errorf("task %s has no runtime: create or recover it through a Manager to start it", t.ID)
	}

	var err error
	if t.recovered {
		status := workflows.Status(t.Status)
		if status == workflows.StatusNotStarted {
			return nil, fmt.Errorf("task %s: %w", t.ID, ErrNoCheckpoint)
		}
		if status.Terminal() {
			return nil, fmt.Errorf("task %s is %s: %w", t.ID, status, ErrAlreadyTerminal)
		}
		err = t.engine.Rehydrate(ctx, t.Snapshot, workflows.State(t.State), status == workflows.StatusSuspended)
	} else {
		err = t.engine.Execute(ctx, t.input)
	}

	// The run is over (or refused before starting): bring the record current so
	// the caller reads the outcome off the task itself. Output is harvested
	// only on clean completion, so error exits still yield nil — except a
	// failed terminal stamp, where the result exists and is returned alongside
	// the error.
	if status := t.engine.Status(); status != workflows.StatusNotStarted {
		t.Status = string(status)
		t.UpdatedAt = time.Now().UTC()
	}
	t.Output = t.engine.Output()
	return t.Output, err
}

// IsRunning reports whether the task is started and not yet terminal. A
// suspended task counts: it is parked, not finished. It reads the live run,
// so it is safe from any goroutine.
func (t *Task) IsRunning() bool {
	return t.engine != nil && t.engine.IsRunning()
}

// Suspend signals the task to park before processing the next state. It is a
// no-op unless the task is running.
func (t *Task) Suspend() error {
	if t.engine == nil {
		return errors.New("task has no runtime")
	}
	return t.engine.Suspend()
}

// Resume continues a suspended task at the next state. It is a no-op unless
// the task is suspended.
func (t *Task) Resume() error {
	if t.engine == nil {
		return errors.New("task has no runtime")
	}
	return t.engine.Resume()
}

// Kill stops the task as soon as possible, cancelling the in-flight state. A
// kill before Start latches: Start then refuses with context.Canceled.
func (t *Task) Kill() error {
	if t.engine == nil {
		return errors.New("task has no runtime")
	}
	return t.engine.Kill()
}
