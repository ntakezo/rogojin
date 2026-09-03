package tasks

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/internal/nodeid"
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

// WithNode sets the manager's node identity, the name its claims are filed
// under. Every manager in a process may share one; two processes must not.
// The default — host, pid, and a random suffix — is safe unmanaged, so set
// this only to make claim rows legible under a naming scheme of your own.
// It panics on an empty id, which would file claims as unclaimed.
func WithNode(id string) Option {
	return func(m *manager) {
		if id == "" {
			panic("tasks: node id must not be empty")
		}
		m.node = id
	}
}

// WithLeaseTTL sets how long the store presumes a claimed task alive without
// renewal (default 30s). The heartbeat renews at a third of this, so the
// lease only lapses after a crash — or that long of consecutive renewal
// failures, which is the intended failure semantics: a store unreachable for
// a whole TTL has already ceded the task to whoever can reach it. It panics
// on a non-positive duration.
func WithLeaseTTL(d time.Duration) Option {
	return func(m *manager) {
		if d <= 0 {
			panic("tasks: lease TTL must be positive")
		}
		m.ttl = d
	}
}

// WithRecoverySweep runs RecoverClaimable every interval and starts what it
// claims — the loop that picks up tasks a crashed node left behind once
// their leases lapse. Losing a claim race, a terminal or never-checkpointed
// record, and a task already running here are all normal and reported to no
// one; every other failure goes to onErr (nil drops them) with the task id,
// or "" when the sweep itself failed. Swept runs execute on background
// contexts and outlive Close. It panics on a non-positive interval.
func WithRecoverySweep(interval time.Duration, onErr func(taskID string, err error)) Option {
	return func(m *manager) {
		if interval <= 0 {
			panic("tasks: sweep interval must be positive")
		}
		m.sweepInterval = interval
		m.sweepErr = onErr
	}
}

// WithResource registers one resource kind's leasing manager, which is the
// whole of that wiring:
//
//	tasks.WithResource(proxies.Kind, manager)
//
// Deleting a task then unlocks the kind, and repointing one drops only the
// lock its new placement no longer fits. Register every kind whose locks
// outlive the process. A kind left unregistered still places tasks, but
// nothing ever frees its locks: deleting a task strands the resource it held,
// and repointing one leaves it leasing what it was moved off, since a binding
// outranks the group.
//
// It panics on a nil manager, on an invalid kind (see leasing.Kind for the
// charset), and on a kind registered twice, which could only unlock it twice.
func WithResource(kind leasing.Kind, rm ResourceManager) Option {
	return func(m *manager) {
		if err := kind.Validate(); err != nil {
			panic(fmt.Sprintf("tasks: resource kind registered with an invalid name: %v", err))
		}
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
	kind    leasing.Kind
	manager ResourceManager
}

// A Manager registers workflows and creates, recovers, groups, and deletes
// their tasks.
type Manager interface {
	// RegisterWorkflow makes workflow available under id for task creation.
	RegisterWorkflow(id string, workflow workflows.Workflow) error
	// CreateTask validates input and returns a new unstarted Task of the
	// workflow, placed by opts (the global group, inheriting its resource
	// assignments, when none are given).
	CreateTask(ctx context.Context, workflowID string, input any, opts ...CreateOption) (*Task, error)
	// RecoverTask rehydrates a persisted task and returns it unstarted, or the
	// live task if it is already running. A task that never checkpointed is
	// returned but cannot be started; its Start fails with ErrNoCheckpoint.
	RecoverTask(ctx context.Context, id string) (*Task, error)
	// RecoverAll rehydrates every persisted task and returns them unstarted,
	// terminal ones included. It claims nothing — it is the listing surface,
	// and another node's live tasks recover here for inspection. The caller
	// decides what to Start; terminal tasks recover for inspection only, and
	// their Start fails with ErrAlreadyTerminal.
	RecoverAll(ctx context.Context) ([]*Task, error)
	// RecoverClaimable rehydrates the tasks whose claim is free for the
	// taking — unclaimed, or leased past expiry — and returns them
	// unstarted. It claims nothing either: Start is the single claim point,
	// and a Start losing the race there with ErrClaimHeld is how a race
	// between sweeping nodes resolves.
	RecoverClaimable(ctx context.Context) ([]*Task, error)
	// DeleteTask removes a task from the registry and the repository, releasing
	// its external resources first. It refuses a running task.
	DeleteTask(ctx context.Context, id string) error
	// AssignResource repoints a task's placement for one resource kind and
	// releases any durable lock of that kind the new placement no longer fits.
	// Assignment is a deliberate act and outranks a lock, which is why this —
	// not a lease — is what resolves the two disagreeing. It takes effect the
	// next time the task is recovered: a live run keeps the placement it was
	// wired with, and the lease it already holds. Other kinds are untouched.
	AssignResource(ctx context.Context, id string, kind leasing.Kind, assignment Assignment) error
	// CreateGroup persists a new task group. The global group needs no record
	// and cannot be re-created.
	CreateGroup(ctx context.Context, group Group) error
	// UpdateGroup edits one task group through fn and persists the result —
	// the manager's only write to an existing group, so the cache and the
	// store never diverge. The edit takes effect on tasks at their next
	// creation or recovery; a live run keeps the placement it was wired with.
	// The global group may be updated even though it exists without a record —
	// saving one is exactly how it gains resource groups. A missing non-global
	// group is an error.
	UpdateGroup(ctx context.Context, id string, fn func(*Group)) error
	// DeleteGroup cascade-deletes a task group and every task in it, releasing
	// each task's external resources. It refuses if any member is running, and
	// refuses the global group.
	DeleteGroup(ctx context.Context, id string) error
	// GetGroup reports one task group and whether it exists, named for the same
	// read on the Repository but served from the manager's group cache. It
	// answers as placement would resolve: the global group always exists,
	// implicitly when unstored — and with no repository, where groups are
	// purely implicit, so does any group asked for.
	GetGroup(id string) (Group, bool)
	// ListGroups returns every task group from the manager's cache, the
	// implicit global group included, sorted by ID. With no repository only
	// the global group lists.
	ListGroups() []Group
	// TasksInGroup returns the ids of every task in the group, sorted. With no
	// repository it answers from the live registry, the only store there is.
	TasksInGroup(ctx context.Context, groupID string) ([]string, error)
	// IsRunning reports whether a known task is started and not yet terminal.
	// A suspended task counts: it is parked, not finished.
	IsRunning(id string) bool
	// Close stops the manager's background work — the claim heartbeat and
	// the recovery sweep — and waits for it. It does not kill running tasks:
	// they run on, but their claims stop renewing, so within a lease TTL
	// other nodes may legitimately take them over. Stop or finish the work
	// first if that is not intended. Close is idempotent.
	Close() error
}

type manager struct {
	workflowRegistry map[string]workflows.Workflow

	taskRegistry map[string]*Task

	repository Repository

	bus comms.Bus

	resources []resource

	// groups is the group cache, loaded once at construction and maintained
	// write-through by CreateGroup and DeleteGroup, so reads and placement
	// resolution never round-trip to the repository. The global group is
	// always present, stored record or not.
	groups map[string]Group

	// node and ttl are the claim identity and lease length every task this
	// manager creates or recovers starts under; sweepInterval and sweepErr
	// configure the optional recovery sweep. The background context and wait
	// group are the heartbeat's and sweep's lifecycle, ended by Close.
	node          string
	ttl           time.Duration
	sweepInterval time.Duration
	sweepErr      func(taskID string, err error)
	background    context.Context
	stop          context.CancelFunc
	work          sync.WaitGroup
	closeOnce     sync.Once

	workflowRegistryMu sync.RWMutex
	taskRegistryMu     sync.RWMutex
	groupsMu           sync.RWMutex
}

// NewManager returns a Manager that persists tasks in repository and injects
// bus into each task's workflow instance, loading the task groups from the
// repository once — the cache is authoritative afterwards, and changes only
// through CreateGroup and DeleteGroup. Tasks it starts are claimed for its
// node against the store, and a heartbeat renews those claims until Close,
// so which node runs a task is the store's decision, not an accident of who
// called Start first. A nil repository selects purely in-memory operation:
// tasks run without checkpoints, durable terminal stamps, stored groups,
// claims, or the heartbeat, and there is nothing to recover after a restart.
// Use it when durability and crash recovery are not needed. Unlike the
// leasing managers, task records are not validated against groups here; a
// record naming a missing group fails at recovery instead.
func NewManager(ctx context.Context, repository Repository, bus comms.Bus, opts ...Option) (Manager, error) {
	s := &manager{
		repository:       repository,
		workflowRegistry: make(map[string]workflows.Workflow),
		taskRegistry:     make(map[string]*Task),
		bus:              bus,
		groups:           map[string]Group{GlobalGroup: {ID: GlobalGroup}},
		node:             nodeid.Default(),
		ttl:              30 * time.Second,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.background, s.stop = context.WithCancel(context.Background())
	if repository != nil {
		listed, err := repository.ListGroups(ctx)
		if err != nil {
			s.stop()
			return nil, fmt.Errorf("failed to load task groups: %w", err)
		}
		for _, g := range listed {
			s.groups[g.ID] = g
		}
		s.work.Add(1)
		go s.heartbeat()
		if s.sweepInterval > 0 {
			s.work.Add(1)
			go s.sweep()
		}
	}
	return s, nil
}

// Close stops the heartbeat and sweep and waits for them. Running tasks are
// left running; see Manager.Close for what that means for their claims.
func (m *manager) Close() error {
	m.closeOnce.Do(func() {
		m.stop()
		m.work.Wait()
	})
	return nil
}

// heartbeat renews the claim of every live task on this manager at a third
// of the lease TTL. ErrStale or a vanished record means the task is no
// longer this node's to run — usurped after a lapse, or deleted elsewhere —
// and the local run is killed so it stops side-effecting. Any other failure
// is left for the next tick: a store blip must not kill runs, and the lease
// only lapses after a full TTL of them, exactly when ceding the task is
// right.
func (m *manager) heartbeat() {
	defer m.work.Done()
	tick := time.NewTicker(m.ttl / 3)
	defer tick.Stop()
	for {
		select {
		case <-m.background.Done():
			return
		case <-tick.C:
		}
		m.taskRegistryMu.RLock()
		live := make([]*Task, 0, len(m.taskRegistry))
		for _, t := range m.taskRegistry {
			if t.IsRunning() {
				live = append(live, t)
			}
		}
		m.taskRegistryMu.RUnlock()

		for _, t := range live {
			err := m.repository.RenewClaim(m.background, t.ID, m.node, m.ttl)
			if errors.Is(err, ErrStale) || errors.Is(err, ErrTaskNotFound) {
				t.Kill()
			}
		}
	}
}

// sweep runs the recovery loop configured by WithRecoverySweep: list what is
// claimable, rehydrate it, and Start each on a background context so a
// stolen run outlives Close along with every other run. Start is the claim
// point, so racing sweeps on other nodes lose there, quietly.
func (m *manager) sweep() {
	defer m.work.Done()
	tick := time.NewTicker(m.sweepInterval)
	defer tick.Stop()
	for {
		select {
		case <-m.background.Done():
			return
		case <-tick.C:
		}
		tasks, err := m.RecoverClaimable(m.background)
		if err != nil {
			if m.sweepErr != nil && m.background.Err() == nil {
				m.sweepErr("", err)
			}
			continue
		}
		for _, t := range tasks {
			go func(t *Task) {
				_, err := t.Start(context.Background())
				if err == nil || errors.Is(err, ErrClaimHeld) || errors.Is(err, ErrAlreadyTerminal) ||
					errors.Is(err, ErrNoCheckpoint) || errors.Is(err, ErrAlreadyStarted) {
					return
				}
				if m.sweepErr != nil {
					m.sweepErr(t.ID, err)
				}
			}(t)
		}
	}
}

func (m *manager) RegisterWorkflow(id string, workflow workflows.Workflow) error {
	m.workflowRegistryMu.Lock()
	defer m.workflowRegistryMu.Unlock()

	if m.workflowRegistry[id] != nil {
		return errors.New("workflow already registered")
	}

	// A ResourceReceiver is wired here, from the same registry that unlocks:
	// the manager a workflow leases through is the instance this manager holds,
	// never a second one over the same store. A workflow missing a kind it
	// needs refuses, failing the registration at boot.
	if r, ok := workflow.(workflows.ResourceReceiver); ok {
		if err := r.UseResources(m.managers()); err != nil {
			return fmt.Errorf("failed to wire resource managers into workflow %s: %w", id, err)
		}
	}

	m.workflowRegistry[id] = workflow
	return nil
}

// managers exposes every registered kind's manager, concretely typed behind
// any, for a ResourceReceiver to assert back to what it leases through.
func (m *manager) managers() map[leasing.Kind]any {
	out := make(map[leasing.Kind]any, len(m.resources))
	for _, r := range m.resources {
		out[r.kind] = r.manager
	}
	return out
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

// getGroup resolves a task group for placement, erroring on one that does not
// exist rather than reporting the miss.
func (m *manager) getGroup(id string) (Group, error) {
	group, found := m.GetGroup(id)
	if !found {
		return Group{}, fmt.Errorf("task group %s does not exist", id)
	}
	return group, nil
}

// GetGroup reports one task group and whether it exists, from the cache,
// answering as placement would resolve: the global group exists implicitly
// when unstored — and with no repository, where groups are purely implicit,
// so does any group asked for.
func (m *manager) GetGroup(id string) (Group, bool) {
	if m.repository == nil {
		return Group{ID: id}, true
	}
	m.groupsMu.RLock()
	defer m.groupsMu.RUnlock()
	g, ok := m.groups[id]
	return g, ok
}

// ListGroups returns every task group from the cache sorted by ID, the global
// group included even when it has no stored record, so listings and placement
// agree on what exists.
func (m *manager) ListGroups() []Group {
	m.groupsMu.RLock()
	defer m.groupsMu.RUnlock()
	out := make([]Group, 0, len(m.groups))
	for _, g := range m.groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// TasksInGroup returns the ids of every task in the group, sorted. With no
// repository it answers from the live registry, the only store in-memory
// operation has.
func (m *manager) TasksInGroup(ctx context.Context, groupID string) ([]string, error) {
	if m.repository == nil {
		m.taskRegistryMu.RLock()
		defer m.taskRegistryMu.RUnlock()
		ids := make([]string, 0)
		for id, t := range m.taskRegistry {
			// Record, not record: a member's Start may be exiting right now,
			// and this read must synchronize with that write.
			if groupOf(t.Record()) == groupID {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		return ids, nil
	}
	ids, err := m.repository.TasksInGroup(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to list tasks of group %s: %w", groupID, err)
	}
	sort.Strings(ids)
	return ids, nil
}

func (m *manager) CreateTask(ctx context.Context, workflowID string, input any, opts ...CreateOption) (*Task, error) {
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
	if err := validKinds(cfg.assignments); err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}
	group, err := m.getGroup(cfg.groupID)
	if err != nil {
		return nil, err
	}

	task, err := createTask(workflow, workflowID, input, m.bus, m.repository, cfg.groupID, cfg.assignments, resolveAll(cfg.assignments, group))
	if err != nil {
		return nil, fmt.Errorf("failed to create task: %w", err)
	}
	task.node, task.ttl = m.node, m.ttl

	// A nil repository is purely in-memory: skip persistence and keep the task
	// only in the registry.
	if m.repository != nil {
		if err := m.repository.CreateTask(ctx, task.record()); err != nil {
			return nil, fmt.Errorf("failed to create task in repository: %w", err)
		}
	}

	m.taskRegistry[task.ID] = task

	return task, nil
}

func (m *manager) IsRunning(id string) bool {
	m.taskRegistryMu.RLock()
	defer m.taskRegistryMu.RUnlock()

	t, ok := m.taskRegistry[id]
	return ok && t.IsRunning()
}

func (m *manager) RecoverTask(ctx context.Context, id string) (*Task, error) {
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

	group, err := m.getGroup(groupOf(record))
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

func (m *manager) RecoverAll(ctx context.Context) ([]*Task, error) {
	// A nil repository persists nothing, so there is nothing to recover; a
	// startup recovery sweep stays a safe no-op in in-memory mode.
	if m.repository == nil {
		return nil, nil
	}

	records, err := m.repository.RecoverAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to recover tasks: %w", err)
	}
	return m.adopt(records)
}

func (m *manager) RecoverClaimable(ctx context.Context) ([]*Task, error) {
	if m.repository == nil {
		return nil, nil
	}

	records, err := m.repository.ListClaimable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list claimable tasks: %w", err)
	}
	return m.adopt(records)
}

// adopt rehydrates records into the registry, passing live tasks through
// as-is; a record naming a missing group fails the whole sweep.
func (m *manager) adopt(records []Task) ([]*Task, error) {
	// Snapshot the group cache once: resolving each record's own would take
	// the read lock per task for an answer that cannot change mid-sweep.
	m.groupsMu.RLock()
	groups := make(map[string]Group, len(m.groups))
	for id, g := range m.groups {
		groups[id] = g
	}
	m.groupsMu.RUnlock()

	m.taskRegistryMu.Lock()
	defer m.taskRegistryMu.Unlock()

	tasks := make([]*Task, 0, len(records))
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
func (m *manager) rehydrate(record Task, group Group) (*Task, error) {
	workflow, err := m.getWorkflow(record.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow for recovery: %w", err)
	}
	t := rehydrateTask(workflow, record, m.bus, m.repository, resolveAll(record.Assignments, group))
	t.node, t.ttl = m.node, m.ttl
	return t, nil
}

// resolve settles one kind's placement: the task's own assignment wherever it
// sets a field, the task group's resource group otherwise. An unset field
// inherits; one set to the empty string is an explicit "none" and does not.
func resolve(own Assignment, group Group, kind leasing.Kind) workflows.Assignment {
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
func resolveAll(assignments map[leasing.Kind]Assignment, group Group) map[leasing.Kind]workflows.Assignment {
	resolved := make(map[leasing.Kind]workflows.Assignment, len(assignments)+len(group.ResourceGroups))
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
func (m *manager) AssignResource(ctx context.Context, id string, kind leasing.Kind, assignment Assignment) error {
	if err := kind.Validate(); err != nil {
		return fmt.Errorf("failed to assign placement of task %s: %w", id, err)
	}

	m.taskRegistryMu.Lock()
	defer m.taskRegistryMu.Unlock()

	if m.repository == nil {
		return errors.New("cannot assign a resource: no repository configured")
	}
	record, err := m.repository.RecoverTask(ctx, id)
	if err != nil {
		return fmt.Errorf("failed to load task %s: %w", id, err)
	}
	group, err := m.getGroup(groupOf(record))
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
func (m *manager) managerFor(kind leasing.Kind) ResourceManager {
	for _, r := range m.resources {
		if r.kind == kind {
			return r.manager
		}
	}
	return nil
}

// validKinds checks every kind keying the map against leasing.Kind's charset,
// in sorted order so a failure names the same kind every run. Placements are
// filed and edited under these keys in the store, so an unsafe one has to be
// refused before it is written, not discovered when a later edit misfiles it.
func validKinds[V any](kinds map[leasing.Kind]V) error {
	sorted := make([]leasing.Kind, 0, len(kinds))
	for kind := range kinds {
		sorted = append(sorted, kind)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	for _, kind := range sorted {
		if err := kind.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) CreateGroup(ctx context.Context, group Group) error {
	if group.ID == "" {
		return errors.New("task group id is required")
	}
	if err := validKinds(group.ResourceGroups); err != nil {
		return fmt.Errorf("task group %s: %w", group.ID, err)
	}
	if group.ID == GlobalGroup {
		return fmt.Errorf("task group %s exists implicitly", GlobalGroup)
	}
	if m.repository == nil {
		return errors.New("cannot create task group: no repository configured")
	}

	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()
	if _, found := m.groups[group.ID]; found {
		return fmt.Errorf("task group %s already exists", group.ID)
	}

	now := time.Now().UTC()
	group.CreatedAt, group.UpdatedAt = now, now
	if err := m.repository.SaveGroup(ctx, group); err != nil {
		return fmt.Errorf("failed to save task group %s: %w", group.ID, err)
	}
	m.groups[group.ID] = group
	return nil
}

// UpdateGroup edits the group through fn under the cache lock, then persists
// and re-caches it — the same shape as leasing.Manager.Update, for the same
// reason: the cache is where placement resolves from and the save is what
// makes a change durable, so the edit has to go through both. ID, CreatedAt,
// and UpdatedAt are not fn's to change; whatever it does to them is undone
// before the save. fn runs under the lock, so it must not block.
func (m *manager) UpdateGroup(ctx context.Context, id string, fn func(*Group)) error {
	if m.repository == nil {
		return errors.New("cannot update task group: no repository configured")
	}

	m.groupsMu.Lock()
	defer m.groupsMu.Unlock()

	g, ok := m.groups[id]
	if !ok {
		return fmt.Errorf("task group %s does not exist", id)
	}
	kept := g
	fn(&g)
	g.ID, g.CreatedAt = kept.ID, kept.CreatedAt
	if err := validKinds(g.ResourceGroups); err != nil {
		return fmt.Errorf("task group %s: %w", id, err)
	}
	if g.CreatedAt.IsZero() {
		// The implicit global group gains its stored record here.
		g.CreatedAt = time.Now().UTC()
	}
	g.UpdatedAt = time.Now().UTC()
	if err := m.repository.SaveGroup(ctx, g); err != nil {
		return fmt.Errorf("failed to save task group %s: %w", id, err)
	}
	m.groups[id] = g
	return nil
}

func (m *manager) DeleteGroup(ctx context.Context, id string) error {
	if id == GlobalGroup {
		return fmt.Errorf("task group %s cannot be deleted", GlobalGroup)
	}
	if m.repository == nil {
		return errors.New("cannot delete task group: no repository configured")
	}

	if _, found := m.GetGroup(id); !found {
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
	sealed := make([]*Task, 0, len(ids))
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
	m.groupsMu.Lock()
	delete(m.groups, id)
	m.groupsMu.Unlock()
	return nil
}

// unsealAll reopens tasks sealed for a cascade that was abandoned.
func unsealAll(tasks []*Task) {
	for _, t := range tasks {
		t.unseal()
	}
}
