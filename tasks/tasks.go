// Package tasks creates and manages tasks: long-running processes that execute
// workflow graphs with durable checkpoints and recovery. Tasks are collected
// into named groups, and a task or a whole group may be assigned, per resource
// kind, the group its runs lease from — a task assigned none of a kind runs
// without that resource.
//
// A kind is just a string the consumer picks to name one leasing manager
// ("proxy", "account", "card"). This package stores and resolves placements
// under it and never interprets it, so a new resource kind needs no change here.
package tasks

import (
	"context"
	"errors"
	"time"
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

// A Task is the durable record of one task: its workflow, its placement (task
// group, plus a per-kind resource assignment), its last-checkpointed status and
// resume state, and the snapshot taken there. A kind absent from Assignments
// inherits the task group's assignment for that kind; see Assignment for what
// a stored one means. Status holds the lifecycle as of the last checkpoint or,
// once the run exits, the terminal outcome. The framework never deletes a
// record on its own.
type Task struct {
	ID          string                `json:"id"`
	WorkflowID  string                `json:"workflowId"`
	GroupID     string                `json:"groupId"`
	Assignments map[string]Assignment `json:"assignments,omitempty"`
	State       string                `json:"state"`
	Snapshot    []byte                `json:"snapshot,omitempty"`
	Status      string                `json:"status"`
	// Output is the workflow's result, persisted when the run completes cleanly;
	// nil for tasks that have not finished or produce no output.
	Output    []byte    `json:"output,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
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
	SaveAssignment(ctx context.Context, id string, kind string, assignment Assignment) error
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
