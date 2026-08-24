package tasks

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/workflows"
)

// A Record is the durable shape of one task: its workflow, its placement
// (task group and optional proxy-group assignment), its last-checkpointed
// status and resume state, and the snapshot taken there. ProxyGroupID nil
// inherits the task group's assignment; "" runs proxyless; anything else
// names the proxy group directly. Status holds the lifecycle as of the last
// checkpoint or, once the run exits, the terminal outcome. The framework
// never deletes a record on its own.
type Record struct {
	ID           string  `json:"id"`
	WorkflowID   string  `json:"workflowId"`
	GroupID      string  `json:"groupId"`
	ProxyGroupID *string `json:"proxyGroupId,omitempty"`
	State        string  `json:"state"`
	Snapshot     []byte  `json:"snapshot,omitempty"`
	Status       string  `json:"status"`
	// Output is the workflow's result, persisted when the run completes cleanly;
	// nil for tasks that have not finished or produce no output.
	Output    []byte    `json:"output,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Repository is the persistence port the consumer implements: a dumb store of
// task records and task groups with no liveness logic (the service infers
// running tasks via IsRunning). Implementations must refresh the record's
// UpdatedAt on every write, including checkpoints and terminal stamps. A nil
// Repository is allowed and selects purely in-memory operation — see
// NewService.
type Repository interface {
	CreateTask(ctx context.Context, record Record) error
	SaveCheckpoint(ctx context.Context, id string, status string, state string, snapshot []byte) error
	MarkTerminal(ctx context.Context, id string, outcome string, output []byte) error
	RecoverTask(ctx context.Context, id string) (Record, error)
	RecoverAll(ctx context.Context) ([]Record, error)
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

// A Service registers workflows and creates, recovers, groups, and deletes
// their tasks.
type Service interface {
	// RegisterWorkflow makes workflow available under id for task creation.
	RegisterWorkflow(id string, workflow workflows.Workflow) error
	// CreateTask validates input and returns a new unstarted Task of the
	// workflow, placed by opts (the global group, inheriting its proxy-group
	// assignment, when none are given).
	CreateTask(ctx context.Context, workflowID string, input any, opts ...CreateOption) (Task, error)
	// RecoverTask rehydrates a persisted task and returns it unstarted, or the
	// live task if it is already running. A task that never checkpointed is
	// returned but cannot be started; its Start fails with ErrNoCheckpoint.
	RecoverTask(ctx context.Context, id string) (Task, error)
	// RecoverAll rehydrates every persisted task and returns them unstarted,
	// terminal ones included. The caller decides what to Start; terminal tasks
	// recover for inspection only, and their Start fails with ErrAlreadyTerminal.
	RecoverAll(ctx context.Context) ([]Task, error)
	// DeleteTask removes a task from the registry and the repository, releasing
	// its external resources first. It refuses a running task.
	DeleteTask(ctx context.Context, id string) error
	// CreateGroup persists a new task group. The global group needs no record
	// and cannot be re-created.
	CreateGroup(ctx context.Context, group Group) error
	// DeleteGroup cascade-deletes a task group and every task in it, releasing
	// each task's external resources. It refuses if any member is running, and
	// refuses the global group.
	DeleteGroup(ctx context.Context, id string) error
	// IsRunning reports whether a known task is started and not yet terminal.
	IsRunning(id string) bool
	// RunningTasks returns the ids of every live task leasing from
	// proxyGroupID. It satisfies the usage guard a proxy manager consults
	// before deleting a group, so a pool is never torn out from under a run.
	RunningTasks(ctx context.Context, proxyGroupID string) ([]string, error)
}

type service struct {
	workflowRegistry map[string]workflows.Workflow

	taskRegistry map[string]*task

	repository Repository

	bus comms.Bus

	release ReleaseFunc

	workflowRegistryMu sync.RWMutex
	taskRegistryMu     sync.RWMutex
}

// NewService returns a Service that persists tasks in repository and injects
// bus into each task's workflow instance. A nil repository selects purely
// in-memory operation: tasks run without checkpoints, durable terminal stamps,
// or stored groups, and there is nothing to recover after a restart. Use it
// when durability and crash recovery are not needed.
func NewService(repository Repository, bus comms.Bus, opts ...ServiceOption) Service {
	s := &service{
		repository:       repository,
		workflowRegistry: make(map[string]workflows.Workflow),
		taskRegistry:     make(map[string]*task),
		bus:              bus,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
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

// getGroup resolves a task group. The global group (and, with no repository,
// any group) resolves to an implicit empty record when unstored; a missing
// non-global group is an error.
func (s *service) getGroup(ctx context.Context, id string) (Group, error) {
	if s.repository == nil {
		return Group{ID: id}, nil
	}
	group, found, err := s.repository.GetGroup(ctx, id)
	if err != nil {
		return Group{}, fmt.Errorf("failed to get task group %s: %w", id, err)
	}
	if found {
		return group, nil
	}
	if id == GlobalGroup {
		return Group{ID: GlobalGroup}, nil
	}
	return Group{}, fmt.Errorf("task group %s does not exist", id)
}

func (s *service) CreateTask(ctx context.Context, workflowID string, input any, opts ...CreateOption) (Task, error) {
	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()

	workflow, err := s.getWorkflow(workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}

	cfg := createConfig{groupID: GlobalGroup}
	for _, opt := range opts {
		opt(&cfg)
	}
	group, err := s.getGroup(ctx, cfg.groupID)
	if err != nil {
		return nil, err
	}
	proxyGroupID := group.ProxyGroupID
	if cfg.proxyGroupID != nil {
		proxyGroupID = *cfg.proxyGroupID
	}

	task, err := createTask(workflow, input, s.bus, s.repository, proxyGroupID)
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	// A nil repository is purely in-memory: skip persistence and keep the task
	// only in the registry.
	if s.repository != nil {
		now := time.Now().UTC()
		record := Record{
			ID:           task.ID(),
			WorkflowID:   workflowID,
			GroupID:      cfg.groupID,
			ProxyGroupID: cfg.proxyGroupID,
			CreatedAt:    now,
			UpdatedAt:    now,
		}
		if err := s.repository.CreateTask(ctx, record); err != nil {
			return nil, fmt.Errorf("failed to create task in repository: %w", err)
		}
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

// RunningTasks reports which live tasks lease from proxyGroupID, reading the
// group each task was actually wired to rather than what its record now says:
// a task started before a reassignment is still running against the old pool.
// It answers from this service's registry, so in a multi-process deployment it
// sees only this process's runs. It never errors; the signature matches the
// port it satisfies.
func (s *service) RunningTasks(ctx context.Context, proxyGroupID string) ([]string, error) {
	s.taskRegistryMu.RLock()
	defer s.taskRegistryMu.RUnlock()

	ids := make([]string, 0)
	for id, t := range s.taskRegistry {
		if t.IsRunning() && t.ProxyGroupID() == proxyGroupID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (s *service) RecoverTask(ctx context.Context, id string) (Task, error) {
	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()

	if existing, ok := s.taskRegistry[id]; ok && existing.IsRunning() {
		return existing, nil
	}

	// A nil repository has nothing durable to rehydrate from; fail loudly rather
	// than dereference it or hand back a zero task.
	if s.repository == nil {
		return nil, errors.New("cannot recover task: no repository configured")
	}

	record, err := s.repository.RecoverTask(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to recover task: %w", err)
	}

	group, err := s.getGroup(ctx, groupOf(record))
	if err != nil {
		return nil, err
	}
	task, err := s.rehydrate(record, group)
	if err != nil {
		return nil, err
	}

	s.taskRegistry[id] = task
	return task, nil
}

func (s *service) RecoverAll(ctx context.Context) ([]Task, error) {
	// A nil repository persists nothing, so there is nothing to recover; a
	// startup recovery sweep stays a safe no-op in in-memory mode.
	if s.repository == nil {
		return nil, nil
	}

	records, err := s.repository.RecoverAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to recover tasks: %w", err)
	}
	// Load the groups once: resolving each record's own would be a query per
	// task against a store that serializes them.
	listed, err := s.repository.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list task groups: %w", err)
	}
	groups := make(map[string]Group, len(listed)+1)
	for _, g := range listed {
		groups[g.ID] = g
	}
	if _, ok := groups[GlobalGroup]; !ok {
		groups[GlobalGroup] = Group{ID: GlobalGroup}
	}

	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()

	tasks := make([]Task, 0, len(records))
	for _, record := range records {
		if existing, ok := s.taskRegistry[record.ID]; ok && existing.IsRunning() {
			tasks = append(tasks, existing)
			continue
		}

		group, ok := groups[groupOf(record)]
		if !ok {
			return nil, fmt.Errorf("failed to rehydrate task %s: task group %s does not exist", record.ID, groupOf(record))
		}
		task, err := s.rehydrate(record, group)
		if err != nil {
			return nil, fmt.Errorf("failed to rehydrate task %s: %w", record.ID, err)
		}
		s.taskRegistry[record.ID] = task
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// groupOf names the record's task group, defaulting a blank one to global.
func groupOf(record Record) string {
	if record.GroupID == "" {
		return GlobalGroup
	}
	return record.GroupID
}

// rehydrate rebuilds an unstarted task from a persisted record, resolving its
// workflow from the registry and its effective proxy group from its own
// assignment when set, else from group's; "" runs proxyless.
func (s *service) rehydrate(record Record, group Group) (*task, error) {
	workflow, err := s.getWorkflow(record.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow for recovery: %w", err)
	}
	proxyGroupID := group.ProxyGroupID
	if record.ProxyGroupID != nil {
		proxyGroupID = *record.ProxyGroupID
	}
	return rehydrateTask(workflow, record.ID, record.Snapshot, workflows.State(record.State), workflows.Status(record.Status), s.bus, s.repository, proxyGroupID), nil
}

func (s *service) DeleteTask(ctx context.Context, id string) error {
	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()
	return s.deleteTaskLocked(ctx, id)
}

// deleteTaskLocked seals the task closed, frees its external resources, then
// removes it from the registry and the repository. A release failure abandons
// the deletion and reopens the task. Callers hold taskRegistryMu.
func (s *service) deleteTaskLocked(ctx context.Context, id string) error {
	t, known := s.taskRegistry[id]
	if known && !t.seal() {
		return errors.New("cannot delete a running task")
	}

	if err := s.releaseTask(ctx, id); err != nil {
		if known {
			t.unseal()
		}
		return err
	}

	delete(s.taskRegistry, id)
	if s.repository == nil {
		return nil
	}
	return s.repository.DeleteTask(ctx, id)
}

// releaseTask runs the configured releaser for the task, if any.
func (s *service) releaseTask(ctx context.Context, id string) error {
	if s.release == nil {
		return nil
	}
	if err := s.release(ctx, id); err != nil {
		return fmt.Errorf("failed to release resources of task %s: %w", id, err)
	}
	return nil
}

func (s *service) CreateGroup(ctx context.Context, group Group) error {
	if group.ID == "" {
		return errors.New("task group id is required")
	}
	if group.ID == GlobalGroup {
		return fmt.Errorf("task group %s exists implicitly", GlobalGroup)
	}
	if s.repository == nil {
		return errors.New("cannot create task group: no repository configured")
	}

	if _, found, err := s.repository.GetGroup(ctx, group.ID); err != nil {
		return fmt.Errorf("failed to get task group %s: %w", group.ID, err)
	} else if found {
		return fmt.Errorf("task group %s already exists", group.ID)
	}

	now := time.Now().UTC()
	group.CreatedAt, group.UpdatedAt = now, now
	if err := s.repository.SaveGroup(ctx, group); err != nil {
		return fmt.Errorf("failed to save task group %s: %w", group.ID, err)
	}
	return nil
}

func (s *service) DeleteGroup(ctx context.Context, id string) error {
	if id == GlobalGroup {
		return fmt.Errorf("task group %s cannot be deleted", GlobalGroup)
	}
	if s.repository == nil {
		return errors.New("cannot delete task group: no repository configured")
	}

	if _, found, err := s.repository.GetGroup(ctx, id); err != nil {
		return fmt.Errorf("failed to get task group %s: %w", id, err)
	} else if !found {
		return fmt.Errorf("task group %s does not exist", id)
	}

	// Membership is read under the registry lock so a task created into the
	// group mid-cascade cannot slip past the sweep.
	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()

	ids, err := s.repository.TasksInGroup(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to list tasks of group %s: %w", id, err)
	}

	// Seal every member before touching anything. Start never goes through the
	// service, so a plain is-it-running check would leave a member free to
	// begin between the check and its deletion.
	sealed := make([]*task, 0, len(ids))
	for _, taskID := range ids {
		t, known := s.taskRegistry[taskID]
		if !known {
			continue // never loaded here; nothing live to seal
		}
		if !t.seal() {
			unsealAll(sealed)
			return fmt.Errorf("cannot delete task group %s: task %s is running", id, taskID)
		}
		sealed = append(sealed, t)
	}

	// Free every member's external resources before deleting any record, so a
	// release failure abandons the cascade with the group still whole rather
	// than with some members already destroyed.
	for _, taskID := range ids {
		if err := s.releaseTask(ctx, taskID); err != nil {
			unsealAll(sealed)
			return fmt.Errorf("failed to delete task group %s: %w", id, err)
		}
	}

	for _, taskID := range ids {
		delete(s.taskRegistry, taskID)
		if err := s.repository.DeleteTask(ctx, taskID); err != nil {
			return fmt.Errorf("failed to delete task %s of group %s: %w", taskID, id, err)
		}
	}
	if err := s.repository.DeleteGroup(ctx, id); err != nil {
		return fmt.Errorf("failed to delete task group %s: %w", id, err)
	}
	return nil
}

// unsealAll reopens tasks sealed for a cascade that was abandoned.
func unsealAll(tasks []*task) {
	for _, t := range tasks {
		t.unseal()
	}
}
