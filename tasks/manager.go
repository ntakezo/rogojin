package tasks

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/workflows"
)

// ResourceManager is the lock surface this manager drives per registered kind:
// Unlock frees a deleted task's durable lock, and ReleaseStaleLock drops a
// repointed task's when its new placement no longer fits. Every
// *leasing.Manager satisfies it, whatever model it is over.
//
// Both calls run while the manager holds its task registry lock. That is safe
// because a leasing manager never calls back into this package — it decides
// everything from the leases and locks it owns.
type ResourceManager interface {
	Unlock(ctx context.Context, taskID string) error
	ReleaseStaleLock(ctx context.Context, a leasing.Assignment) error
}

// An Option configures a Manager at construction.
type Option func(*manager)

// WithResource registers one resource kind's leasing manager, which is the
// whole of that wiring:
//
//	tasks.WithResource("proxy", manager)
//
// Deleting a task then unlocks the kind, and repointing one drops only the
// lock its new placement no longer fits. Register every kind whose locks
// outlive the process. A kind left unregistered still places tasks, but
// nothing ever frees its locks: deleting a task strands the resource it held,
// and repointing one leaves it leasing what it was moved off, since a binding
// outranks the group.
//
// It panics on a nil manager and on a kind registered twice, which could only
// unlock it twice.
func WithResource(kind string, rm ResourceManager) Option {
	return func(m *manager) {
		if rm == nil {
			panic(fmt.Sprintf("tasks: resource kind %q registered with a nil manager", kind))
		}
		for _, r := range m.resources {
			if r.kind == kind {
				panic(fmt.Sprintf("tasks: resource kind %q registered twice", kind))
			}
		}
		m.resources = append(m.resources, resource{kind: kind, manager: rm})
	}
}

// a resource is one registered kind's manager, kept in registration order so a
// failure names its kinds the same way every run.
type resource struct {
	kind    string
	manager ResourceManager
}

// A Manager registers workflows and creates, recovers, groups, and deletes
// their tasks.
type Manager interface {
	// RegisterWorkflow makes workflow available under id for task creation.
	RegisterWorkflow(id string, workflow workflows.Workflow) error
	// CreateTask validates input and returns a new unstarted Handle of the
	// workflow, placed by opts (the global group, inheriting its resource
	// assignments, when none are given).
	CreateTask(ctx context.Context, workflowID string, input any, opts ...CreateOption) (Handle, error)
	// RecoverTask rehydrates a persisted task and returns it unstarted, or the
	// live task if it is already running. A task that never checkpointed is
	// returned but cannot be started; its Start fails with ErrNoCheckpoint.
	RecoverTask(ctx context.Context, id string) (Handle, error)
	// RecoverAll rehydrates every persisted task and returns them unstarted,
	// terminal ones included. The caller decides what to Start; terminal tasks
	// recover for inspection only, and their Start fails with ErrAlreadyTerminal.
	RecoverAll(ctx context.Context) ([]Handle, error)
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
}

type manager struct {
	workflowRegistry map[string]workflows.Workflow

	taskRegistry map[string]*handle

	repository Repository

	bus comms.Bus

	resources []resource

	workflowRegistryMu sync.RWMutex
	taskRegistryMu     sync.RWMutex
}

// NewManager returns a Manager that persists tasks in repository and injects
// bus into each task's workflow instance. A nil repository selects purely
// in-memory operation: tasks run without checkpoints, durable terminal stamps,
// or stored groups, and there is nothing to recover after a restart. Use it
// when durability and crash recovery are not needed.
func NewManager(repository Repository, bus comms.Bus, opts ...Option) Manager {
	s := &manager{
		repository:       repository,
		workflowRegistry: make(map[string]workflows.Workflow),
		taskRegistry:     make(map[string]*handle),
		bus:              bus,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (m *manager) RegisterWorkflow(id string, workflow workflows.Workflow) error {
	m.workflowRegistryMu.Lock()
	defer m.workflowRegistryMu.Unlock()

	if m.workflowRegistry[id] != nil {
		return errors.New("workflow already registered")
	}

	m.workflowRegistry[id] = workflow
	return nil
}

func (m *manager) getWorkflow(id string) (workflows.Workflow, error) {
	m.workflowRegistryMu.RLock()
	defer m.workflowRegistryMu.RUnlock()

	workflow, ok := m.workflowRegistry[id]
	if !ok {
		return nil, errors.New("workflow does not exist")
	}
	return workflow, nil
}

// getGroup resolves a task group. The global group (and, with no repository,
// any group) resolves to an implicit empty record when unstored; a missing
// non-global group is an error.
func (m *manager) getGroup(ctx context.Context, id string) (Group, error) {
	if m.repository == nil {
		return Group{ID: id}, nil
	}
	group, found, err := m.repository.GetGroup(ctx, id)
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

func (m *manager) CreateTask(ctx context.Context, workflowID string, input any, opts ...CreateOption) (Handle, error) {
	m.taskRegistryMu.Lock()
	defer m.taskRegistryMu.Unlock()

	workflow, err := m.getWorkflow(workflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}

	cfg := createConfig{groupID: GlobalGroup}
	for _, opt := range opts {
		opt(&cfg)
	}
	group, err := m.getGroup(ctx, cfg.groupID)
	if err != nil {
		return nil, err
	}

	task, err := createHandle(workflow, input, m.bus, m.repository, resolveAll(cfg.assignments, group))
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}

	// A nil repository is purely in-memory: skip persistence and keep the task
	// only in the registry.
	if m.repository != nil {
		now := time.Now().UTC()
		record := Task{
			ID:          task.ID(),
			WorkflowID:  workflowID,
			GroupID:     cfg.groupID,
			Assignments: cfg.assignments,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := m.repository.CreateTask(ctx, record); err != nil {
			return nil, fmt.Errorf("failed to create task in repository: %w", err)
		}
	}

	m.taskRegistry[task.ID()] = task

	return task, nil
}

func (m *manager) IsRunning(id string) bool {
	m.taskRegistryMu.RLock()
	defer m.taskRegistryMu.RUnlock()

	t, ok := m.taskRegistry[id]
	return ok && t.IsRunning()
}

func (m *manager) RecoverTask(ctx context.Context, id string) (Handle, error) {
	m.taskRegistryMu.Lock()
	defer m.taskRegistryMu.Unlock()

	if existing, ok := m.taskRegistry[id]; ok && existing.IsRunning() {
		return existing, nil
	}

	// A nil repository has nothing durable to rehydrate from; fail loudly rather
	// than dereference it or hand back a zero task.
	if m.repository == nil {
		return nil, errors.New("cannot recover task: no repository configured")
	}

	record, err := m.repository.RecoverTask(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to recover task: %w", err)
	}

	group, err := m.getGroup(ctx, groupOf(record))
	if err != nil {
		return nil, err
	}
	task, err := m.rehydrate(record, group)
	if err != nil {
		return nil, err
	}

	m.taskRegistry[id] = task
	return task, nil
}

func (m *manager) RecoverAll(ctx context.Context) ([]Handle, error) {
	// A nil repository persists nothing, so there is nothing to recover; a
	// startup recovery sweep stays a safe no-op in in-memory mode.
	if m.repository == nil {
		return nil, nil
	}

	records, err := m.repository.RecoverAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to recover tasks: %w", err)
	}
	// Load the groups once: resolving each record's own would be a query per
	// task against a store that serializes them.
	listed, err := m.repository.ListGroups(ctx)
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

	m.taskRegistryMu.Lock()
	defer m.taskRegistryMu.Unlock()

	tasks := make([]Handle, 0, len(records))
	for _, record := range records {
		if existing, ok := m.taskRegistry[record.ID]; ok && existing.IsRunning() {
			tasks = append(tasks, existing)
			continue
		}

		group, ok := groups[groupOf(record)]
		if !ok {
			return nil, fmt.Errorf("failed to rehydrate task %s: task group %s does not exist", record.ID, groupOf(record))
		}
		task, err := m.rehydrate(record, group)
		if err != nil {
			return nil, fmt.Errorf("failed to rehydrate task %s: %w", record.ID, err)
		}
		m.taskRegistry[record.ID] = task
		tasks = append(tasks, task)
	}
	return tasks, nil
}

// groupOf names the record's task group, defaulting a blank one to global.
func groupOf(record Task) string {
	if record.GroupID == "" {
		return GlobalGroup
	}
	return record.GroupID
}

// rehydrate rebuilds an unstarted task from a persisted record, resolving its
// workflow from the registry and its effective placement per kind.
func (m *manager) rehydrate(record Task, group Group) (*handle, error) {
	workflow, err := m.getWorkflow(record.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow for recovery: %w", err)
	}
	return rehydrateHandle(workflow, record.ID, record.Snapshot, workflows.State(record.State), workflows.Status(record.Status), m.bus, m.repository, resolveAll(record.Assignments, group)), nil
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
func (m *manager) AssignResource(ctx context.Context, id string, kind string, assignment Assignment) error {
	m.taskRegistryMu.Lock()
	defer m.taskRegistryMu.Unlock()

	if m.repository == nil {
		return errors.New("cannot assign a resource: no repository configured")
	}
	record, err := m.repository.RecoverTask(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to load task %s: %w", id, err)
	}
	group, err := m.getGroup(ctx, groupOf(record))
	if err != nil {
		return err
	}

	if err := m.repository.SaveAssignment(ctx, id, kind, assignment); err != nil {
		return fmt.Errorf("failed to assign %s placement of task %s: %w", kind, id, err)
	}
	rm := m.managerFor(kind)
	if rm == nil {
		return nil
	}

	// The manager is told the placement as resolved, not as stored: a nil group
	// inherits the task group's, which is what the task will actually lease from.
	resolved := resolve(assignment, group, kind)
	if err := rm.ReleaseStaleLock(ctx, leasing.Assignment{TaskID: id, GroupID: resolved.GroupID, ResourceID: resolved.ResourceID}); err != nil {
		return fmt.Errorf("failed to release the stale %s lock of task %s: %w", kind, id, err)
	}
	return nil
}

func (m *manager) DeleteTask(ctx context.Context, id string) error {
	m.taskRegistryMu.Lock()
	defer m.taskRegistryMu.Unlock()
	return m.deleteTaskLocked(ctx, id)
}

// deleteTaskLocked seals the task closed, frees its external resources, then
// removes it from the registry and the repository. A release failure abandons
// the deletion and reopens the task. Callers hold taskRegistryMu.
func (m *manager) deleteTaskLocked(ctx context.Context, id string) error {
	t, known := m.taskRegistry[id]
	if known && !t.seal() {
		return errors.New("cannot delete a running task")
	}

	if err := m.releaseTask(ctx, id); err != nil {
		if known {
			t.unseal()
		}
		return err
	}

	delete(m.taskRegistry, id)
	if m.repository == nil {
		return nil
	}
	return m.repository.DeleteTask(ctx, id)
}

// releaseTask frees every registered kind's durable lock on the task. All of
// them run even when one fails — a lock left held is unleasable forever, so
// one broken store must not strand the locks the other managers would have
// freed.
func (m *manager) releaseTask(ctx context.Context, id string) error {
	var errs []error
	for _, r := range m.resources {
		if err := r.manager.Unlock(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.kind, err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return fmt.Errorf("failed to release resources of task %s: %w", id, errors.Join(errs...))
}

// managerFor returns the registered manager for the kind, or nil when no
// manager of that kind is wired.
func (m *manager) managerFor(kind string) ResourceManager {
	for _, r := range m.resources {
		if r.kind == kind {
			return r.manager
		}
	}
	return nil
}

func (m *manager) CreateGroup(ctx context.Context, group Group) error {
	if group.ID == "" {
		return errors.New("task group id is required")
	}
	if group.ID == GlobalGroup {
		return fmt.Errorf("task group %s exists implicitly", GlobalGroup)
	}
	if m.repository == nil {
		return errors.New("cannot create task group: no repository configured")
	}

	if _, found, err := m.repository.GetGroup(ctx, group.ID); err != nil {
		return fmt.Errorf("failed to get task group %s: %w", group.ID, err)
	} else if found {
		return fmt.Errorf("task group %s already exists", group.ID)
	}

	now := time.Now().UTC()
	group.CreatedAt, group.UpdatedAt = now, now
	if err := m.repository.SaveGroup(ctx, group); err != nil {
		return fmt.Errorf("failed to save task group %s: %w", group.ID, err)
	}
	return nil
}

func (m *manager) DeleteGroup(ctx context.Context, id string) error {
	if id == GlobalGroup {
		return fmt.Errorf("task group %s cannot be deleted", GlobalGroup)
	}
	if m.repository == nil {
		return errors.New("cannot delete task group: no repository configured")
	}

	if _, found, err := m.repository.GetGroup(ctx, id); err != nil {
		return fmt.Errorf("failed to get task group %s: %w", id, err)
	} else if !found {
		return fmt.Errorf("task group %s does not exist", id)
	}

	// Membership is read under the registry lock so a task created into the
	// group mid-cascade cannot slip past the sweep.
	m.taskRegistryMu.Lock()
	defer m.taskRegistryMu.Unlock()

	ids, err := m.repository.TasksInGroup(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to list tasks of group %s: %w", id, err)
	}

	// Seal every member before touching anything. Start never goes through the
	// manager, so a plain is-it-running check would leave a member free to
	// begin between the check and its deletion.
	sealed := make([]*handle, 0, len(ids))
	for _, taskID := range ids {
		t, known := m.taskRegistry[taskID]
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
		if err := m.releaseTask(ctx, taskID); err != nil {
			unsealAll(sealed)
			return fmt.Errorf("failed to delete task group %s: %w", id, err)
		}
	}

	for _, taskID := range ids {
		delete(m.taskRegistry, taskID)
		if err := m.repository.DeleteTask(ctx, taskID); err != nil {
			return fmt.Errorf("failed to delete task %s of group %s: %w", taskID, id, err)
		}
	}
	if err := m.repository.DeleteGroup(ctx, id); err != nil {
		return fmt.Errorf("failed to delete task group %s: %w", id, err)
	}
	return nil
}

// unsealAll reopens tasks sealed for a cascade that was abandoned.
func unsealAll(tasks []*handle) {
	for _, t := range tasks {
		t.unseal()
	}
}
