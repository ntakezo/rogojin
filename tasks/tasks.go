// Package tasks creates and manages tasks: long-running processes that
// execute workflow graphs with durable checkpoints and recovery.
package tasks

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/workflows"
)

// A Task is a long-running process executing one workflow's graph.
type Task interface {
	ID() string
	// Start executes the task synchronously until completion, error, or kill,
	// returning the workflow's output on clean completion. The output is nil if
	// the workflow produces none, or if the run errors or is killed. A recovered
	// task resumes from its persisted checkpoint instead of the graph's initial
	// state.
	Start(ctx context.Context) ([]byte, error)
	// Suspend signals the task to park before processing the next state.
	// It is a no-op unless the task is running.
	Suspend() error
	// Resume continues a suspended task at the next state. It is a no-op
	// unless the task is suspended.
	Resume() error
	// Kill stops the task as soon as possible, cancelling the in-flight state.
	Kill() error
	// IsRunning reports whether the task is started and not yet terminal.
	IsRunning() bool
	// Status reports the task's lifecycle status. A recovered task that has
	// not been started reports its persisted status (e.g. suspended).
	Status() workflows.Status
}

type task struct {
	id     string
	input  any
	engine *engine

	// recovered marks a task rebuilt from a snapshot; Start resumes at
	// resumeAt with snapshot instead of executing input from the beginning.
	recovered       bool
	snapshot        []byte
	resumeAt        workflows.State
	recoveredStatus workflows.Status
}

// createTask validates input against the workflow and returns a new unstarted
// Task. The caller must not modify input after creation.
func createTask(workflow workflows.Workflow, input any, bus comms.Bus, repo Repository) (Task, error) {
	if err := workflow.ValidateInput(input); err != nil {
		return nil, fmt.Errorf("task input validation error: %w", err)
	}

	id := uuid.NewString()
	return &task{
		id:     id,
		input:  input,
		engine: newEngine(workflow, workflows.Deps{TaskID: id, Bus: bus}, repo),
	}, nil
}

// rehydrateTask rebuilds a task from a persisted snapshot so Start resumes at
// resumeAt. status is the persisted lifecycle reported until the task starts.
func rehydrateTask(workflow workflows.Workflow, id string, snapshot []byte, resumeAt workflows.State, status workflows.Status, bus comms.Bus, repo Repository) Task {
	return &task{
		id:              id,
		engine:          newEngine(workflow, workflows.Deps{TaskID: id, Bus: bus}, repo),
		recovered:       true,
		snapshot:        snapshot,
		resumeAt:        resumeAt,
		recoveredStatus: status,
	}
}

func (t *task) ID() string {
	return t.id
}

func (t *task) Start(ctx context.Context) ([]byte, error) {
	if t.recovered {
		if err := t.engine.Rehydrate(ctx, t.snapshot, t.resumeAt); err != nil {
			return nil, err
		}
		return t.engine.Output(), nil
	}
	if err := t.engine.Execute(ctx, t.input); err != nil {
		return nil, err
	}
	return t.engine.Output(), nil
}

func (t *task) IsRunning() bool {
	return t.engine.IsRunning()
}

func (t *task) Status() workflows.Status {
	status := t.engine.Status()
	if status == workflows.StatusNotStarted && t.recovered {
		return t.recoveredStatus
	}
	return status
}

func (t *task) Suspend() error {
	return t.engine.Suspend()
}

func (t *task) Resume() error {
	return t.engine.Resume()
}

func (t *task) Kill() error {
	return t.engine.Kill()
}
