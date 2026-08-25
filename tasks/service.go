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

// A Record is the durable shape of one task: its workflow, its placement (task
// group, plus a per-kind resource assignment), its last-checkpointed status and
// resume state, and the snapshot taken there. A kind absent from Assignments
// inherits the task group's assignment for that kind; see Assignment for what
// a stored one means. Status holds the lifecycle as of the last checkpoint or,
// once the run exits, the terminal outcome. The framework never deletes a
// record on its own.
type Record struct {
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
	// SaveAssignment repoints a task's placement for one kind, leaving every
	// other kind and the rest of the record untouched. A nil field clears the
	// stored value.
	SaveAssignment(ctx context.Context, id string, kind string, assignment Assignment) error
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
	// TasksPinnedTo returns every task record pinned to resourceID for the
	// kind. Whole records, not ids: which of them could still run is a rule
	// this store does not own.
	TasksPinnedTo(ctx context.Context, kind, resourceID string) ([]Record, error)
}

// A Service registers workflows and creates, recovers, groups, and deletes
// their tasks.
type Service interface {
	// RegisterWorkflow makes workflow available under id for task creation.
	RegisterWorkflow(id string, workflow workflows.Workflow) error
	// CreateTask validates input and returns a new unstarted Task of the
	// workflow, placed by opts (the global group, inheriting its resource
	// assignments, when none are given).
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
	// AssignResource repoints a task's placement for one resource kind and
	// releases any durable lock of that kind the new placement no longer fits.
	// Assignment is a deliberate act and outranks a lock, which is why this —
	// not a lease — is what resolves the two disagreeing. It takes effect the
	// next time the task is recovered: a live run keeps the placement it was
	// wired with, and the lease it already holds. Other kinds are untouched.
	AssignResource(ctx context.Context, id string, kind string, assignment Assignment) error
	// CreateGroup persists a new task group. The global group needs no record
	// and cannot be re-created.
	CreateGroup(ctx context.Context, group Group) error
	// DeleteGroup cascade-deletes a task group and every task in it, releasing
	// each task's external resources. It refuses if any member is running, and
	// refuses the global group.
	DeleteGroup(ctx context.Context, id string) error
	// IsRunning reports whether a known task is started and not yet terminal.
	// A suspended task counts: it is parked, not finished.
	IsRunning(id string) bool
	// RunningTasks returns the ids of every task actively running against the
	// kind's groupID. With TaskIsRunning it satisfies the usage guard a leasing
	// manager consults before deleting a resource or a group, so a pool is
	// never torn out from under a run. Each manager asks about its own kind, so
	// a proxy group and an account group may share a name without colliding.
	RunningTasks(ctx context.Context, kind, groupID string) ([]string, error)
	// TaskIsRunning reports whether the named task is actively running. It
	// answers the half of the usage guard that asks about one task rather than
	// a whole group, for a resource some task holds a durable lock on. The
	// question is kind-agnostic: a task runs, or it does not.
	TaskIsRunning(ctx context.Context, taskID string) (bool, error)
	// PinnedTasks returns the ids of every task pinned to the kind's resourceID
	// that could still run, running or not. It is the half of the usage guard a
	// leasing manager cannot answer for itself: a durable lock lives on the
	// resource, but a pin lives here.
	PinnedTasks(ctx context.Context, kind, resourceID string) ([]string, error)
}

type service struct {
	workflowRegistry map[string]workflows.Workflow

	taskRegistry map[string]*task

	repository Repository

	bus comms.Bus

	release ReleaseFunc

	reassign ReassignFunc

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

	task, err := createTask(workflow, input, s.bus, s.repository, resolveAll(cfg.assignments, group))
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	// A nil repository is purely in-memory: skip persistence and keep the task
	// only in the registry.
	if s.repository != nil {
		now := time.Now().UTC()
		record := Record{
			ID:          task.ID(),
			WorkflowID:  workflowID,
			GroupID:     cfg.groupID,
			Assignments: cfg.assignments,
			CreatedAt:   now,
			UpdatedAt:   now,
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

// RunningTasks reports which tasks are actively running against the kind's
// groupID, reading the group each was actually wired to rather than what its
// record now says: a task started before a reassignment is still running
// against the old pool. A suspended task is excluded — it is parked between
// states with no request in flight, which is what makes suspending a task the
// way to free its resources for deletion. It answers from this service's
// registry, so in a multi-process deployment it sees only this process's runs.
// It never errors; the signature matches the port it satisfies.
func (s *service) RunningTasks(ctx context.Context, kind, groupID string) ([]string, error) {
	s.taskRegistryMu.RLock()
	defer s.taskRegistryMu.RUnlock()

	ids := make([]string, 0)
	for id, t := range s.taskRegistry {
		if wired, _ := t.Assignment(kind); isActive(t) && wired == groupID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// TaskIsRunning reports whether the task is actively running, on the same
// suspended-is-not-running reading as RunningTasks. An unknown task — never
// created here, or already deleted — is not running.
func (s *service) TaskIsRunning(ctx context.Context, taskID string) (bool, error) {
	s.taskRegistryMu.RLock()
	defer s.taskRegistryMu.RUnlock()

	t, known := s.taskRegistry[taskID]
	return known && isActive(t), nil
}

// isActive reports whether the task is advancing right now, as opposed to
// merely started: a suspended task has stopped at a state boundary.
func isActive(t *task) bool {
	return t.Status() == workflows.StatusRunning
}

// PinnedTasks reports which tasks pinned to the kind's resourceID could still
// run, so a deletion can be weighed before it happens rather than discovered at
// the task's next lease. A task counts when it is resumable from a durable
// checkpoint, or is live in this process's registry and not yet terminal.
//
// Tasks that finished, and tasks that ran without durability and so kept no
// checkpoint to resume from, are left out: nothing can make them run again, and
// warning about them is noise.
func (s *service) PinnedTasks(ctx context.Context, kind, resourceID string) ([]string, error) {
	if resourceID == "" {
		return nil, nil
	}

	// The registry lock is dropped before the store is read: a repository call
	// under it would block every other task operation for the length of a query.
	s.taskRegistryMu.RLock()
	counted := make(map[string]bool)
	for id, t := range s.taskRegistry {
		if _, pin := t.Assignment(kind); pin == resourceID && !t.Status().Terminal() {
			counted[id] = true
		}
	}
	s.taskRegistryMu.RUnlock()

	ids := make([]string, 0, len(counted))
	for id := range counted {
		ids = append(ids, id)
	}

	if s.repository != nil {
		records, err := s.repository.TasksPinnedTo(ctx, kind, resourceID)
		if err != nil {
			return nil, fmt.Errorf("failed to list tasks pinned to %s %s: %w", kind, resourceID, err)
		}
		for _, record := range records {
			if counted[record.ID] || !resumable(record) {
				continue
			}
			ids = append(ids, record.ID)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// resumable reports whether a stored task could still be started. Durability
// begins at the first checkpoint, so a record that never checkpointed has
// nothing to resume from; a terminal one has nothing left to do.
func resumable(record Record) bool {
	status := workflows.Status(record.Status)
	return status != workflows.StatusNotStarted && !status.Terminal()
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
// workflow from the registry and its effective placement per kind.
func (s *service) rehydrate(record Record, group Group) (*task, error) {
	workflow, err := s.getWorkflow(record.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow for recovery: %w", err)
	}
	return rehydrateTask(workflow, record.ID, record.Snapshot, workflows.State(record.State), workflows.Status(record.Status), s.bus, s.repository, resolveAll(record.Assignments, group)), nil
}

// resolve settles one kind's placement: the task's own assignment wherever it
// sets a field, the task group's resource group otherwise. An unset field
// inherits; one set to the empty string is an explicit "none" and does not.
func resolve(own Assignment, group Group, kind string) workflows.Assignment {
	resolved := workflows.Assignment{GroupID: group.ResourceGroups[kind]}
	if own.GroupID != nil {
		resolved.GroupID = *own.GroupID
	}
	if own.ResourceID != nil {
		resolved.ResourceID = *own.ResourceID
	}
	return resolved
}

// resolveAll settles every kind either the task or its group names. A kind
// neither names is absent, which reads as the zero Assignment — the same answer
// resolving it would give.
func resolveAll(assignments map[string]Assignment, group Group) map[string]workflows.Assignment {
	resolved := make(map[string]workflows.Assignment, len(assignments)+len(group.ResourceGroups))
	for kind := range group.ResourceGroups {
		resolved[kind] = resolve(assignments[kind], group, kind)
	}
	for kind := range assignments {
		resolved[kind] = resolve(assignments[kind], group, kind)
	}
	return resolved
}

// AssignResource repoints the task's placement for one kind, then releases any
// durable lock of that kind the new placement no longer fits. The record is
// written first: a released lock with an unwritten placement would send the
// task back to the pool it was just moved off, while a written placement with a
// stale lock is what the next AssignResource — or a plain Unlock — can still
// repair.
//
// It refuses nothing. Reassigning a live task is legitimate and takes effect at
// its next recovery, since a run is wired with its placement at start and keeps
// the lease it already holds.
func (s *service) AssignResource(ctx context.Context, id string, kind string, assignment Assignment) error {
	s.taskRegistryMu.Lock()
	defer s.taskRegistryMu.Unlock()

	if s.repository == nil {
		return errors.New("cannot assign a resource: no repository configured")
	}
	record, err := s.repository.RecoverTask(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to load task %s: %w", id, err)
	}
	group, err := s.getGroup(ctx, groupOf(record))
	if err != nil {
		return err
	}

	if err := s.repository.SaveAssignment(ctx, id, kind, assignment); err != nil {
		return fmt.Errorf("failed to assign %s placement of task %s: %w", kind, id, err)
	}
	if s.reassign == nil {
		return nil
	}

	// The releaser is told the placement as resolved, not as stored: a nil group
	// inherits the task group's, which is what the task will actually lease from.
	resolved := resolve(assignment, group, kind)
	if err := s.reassign(ctx, id, kind, resolved.GroupID, resolved.ResourceID); err != nil {
		return fmt.Errorf("failed to release the stale %s lock of task %s: %w", kind, id, err)
	}
	return nil
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
