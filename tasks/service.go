package tasks

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/workflows"
)

// A Record is the durable shape of one task: its workflow, its
// last-checkpointed status and resume state, and the snapshot taken there.
// Status holds the lifecycle as of the last checkpoint or, once the run exits,
// the terminal outcome. The framework never deletes a record on its own.
type Record struct {
	ID         string
	WorkflowID string
	State      string
	Snapshot   []byte
	Status     string
}

// Repository is the persistence port the consumer implements: a dumb store of
// task records with no liveness logic (the service infers running tasks via
// IsRunning).
type Repository interface {
	CreateTask(ctx context.Context, id string, workflowID string) error
	SaveCheckpoint(ctx context.Context, id string, status string, state string, snapshot []byte) error
	MarkTerminal(ctx context.Context, id string, outcome string) error
	RecoverTask(ctx context.Context, id string) (Record, error)
	RecoverAll(ctx context.Context) ([]Record, error)
	DeleteTask(ctx context.Context, id string) error
}

// A Service registers workflows and creates, recovers, and deletes their tasks.
type Service interface {
	// RegisterWorkflow makes workflow available under id for task creation.
	RegisterWorkflow(id string, workflow workflows.Workflow) error
	// CreateTask validates input and returns a new unstarted Task of the workflow.
	CreateTask(ctx context.Context, workflowID string, input any) (Task, error)
	// RecoverTask rehydrates a persisted task and returns it unstarted, or the
	// live task if it is already running.
	RecoverTask(ctx context.Context, id string) (Task, error)
	// RecoverAll rehydrates every persisted task and returns them unstarted,
	// terminal ones included. The caller decides what to Start.
	RecoverAll(ctx context.Context) ([]Task, error)
	// DeleteTask removes a task from the registry and the repository.
	// It refuses a running task.
	DeleteTask(ctx context.Context, id string) error
	// IsRunning reports whether a known task is started and not yet terminal.
	IsRunning(id string) bool
}

type service struct {
	workflowRegistry map[string]workflows.Workflow

	taskRegistry map[string]Task

	repository Repository

	bus comms.Bus

	workflowRegistryMu sync.RWMutex
	taskRegistryMu     sync.RWMutex
}

// NewService returns a Service that persists tasks in repository and injects
// bus into each task's workflow instance.
func NewService(repository Repository, bus comms.Bus) Service {
	return &service{
		repository:       repository,
		workflowRegistry: make(map[string]workflows.Workflow),
		taskRegistry:     make(map[string]Task),
		bus:              bus,
	}
}

func (s *service) RegisterWorkflow(id string, workflow workflows.Workflow) error {
	s.workflowRegistryMu.Lock()
	defer s.workflowRegistryMu.Unlock()

	if s.workflowRegistry[id] != nil {
		return errors.New("workflow already registered")
	}

	s.workflowRegistry[id] = workflow
	return nil
}

func (s *service) getWorkflow(id string) (workflows.Workflow, error) {
	s.workflowRegistryMu.RLock()
	defer s.workflowRegistryMu.RUnlock()

	workflow, ok := s.workflowRegistry[id]
	if !ok {
		return nil, errors.New("workflow does not exist")
	}
	return workflow, nil
}

func (s *service) CreateTask(ctx context.Context, workflowID string, input any) (Task, error) {
	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()

	workflow, err := s.getWorkflow(workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}

	task, err := createTask(workflow, input, s.bus, s.repository)
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	if err := s.repository.CreateTask(ctx, task.ID(), workflowID); err != nil {
		return nil, fmt.Errorf("failed to create task in repository: %w", err)
	}

	s.taskRegistry[task.ID()] = task

	return task, nil
}

func (s *service) IsRunning(id string) bool {
	s.taskRegistryMu.RLock()
	defer s.taskRegistryMu.RUnlock()

	t, ok := s.taskRegistry[id]
	return ok && t.IsRunning()
}

func (s *service) RecoverTask(ctx context.Context, id string) (Task, error) {
	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()

	if existing, ok := s.taskRegistry[id]; ok && existing.IsRunning() {
		return existing, nil
	}

	record, err := s.repository.RecoverTask(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to recover task: %w", err)
	}

	task, err := s.rehydrate(record)
	if err != nil {
		return nil, err
	}

	s.taskRegistry[id] = task
	return task, nil
}

func (s *service) RecoverAll(ctx context.Context) ([]Task, error) {
	records, err := s.repository.RecoverAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to recover tasks: %w", err)
	}

	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()

	tasks := make([]Task, 0, len(records))
	for _, record := range records {
		if existing, ok := s.taskRegistry[record.ID]; ok && existing.IsRunning() {
			tasks = append(tasks, existing)
			continue
		}

		task, err := s.rehydrate(record)
		if err != nil {
			return nil, fmt.Errorf("failed to rehydrate task %s: %w", record.ID, err)
		}
		s.taskRegistry[record.ID] = task
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// rehydrate rebuilds an unstarted Task from a persisted record, resolving its
// workflow from the registry.
func (s *service) rehydrate(record Record) (Task, error) {
	workflow, err := s.getWorkflow(record.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow for recovery: %w", err)
	}
	return rehydrateTask(workflow, record.ID, record.Snapshot, workflows.State(record.State), workflows.Status(record.Status), s.bus, s.repository), nil
}

func (s *service) DeleteTask(ctx context.Context, id string) error {
	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()

	if t, ok := s.taskRegistry[id]; ok && t.IsRunning() {
		return errors.New("cannot delete a running task")
	}

	delete(s.taskRegistry, id)
	return s.repository.DeleteTask(ctx, id)
}
