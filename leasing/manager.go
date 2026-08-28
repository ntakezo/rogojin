package leasing

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// An Option configures a Manager at construction.
type Option[T any] func(*Manager[T])

// WithStrategy registers factory under name, so groups may reference custom
// selection algorithms beyond round robin (or override it under its own name).
func WithStrategy[T any](name string, factory StrategyFactory[T]) Option[T] {
	return func(m *Manager[T]) { m.factories[name] = factory }
}

// A Manager allocates resources to tasks: locked resources go only to their
// owner, unlocked ones rotate within their group through the group's selection
// strategy under the resource's holder cap. It owns all live lease state; the
// Repository only stores bytes. A Manager is safe for concurrent use.
type Manager[T any] struct {
	repo      Repository[T]
	factories map[string]StrategyFactory[T]

	mu         sync.Mutex
	cond       *sync.Cond
	groups     map[string]Group
	strategies map[string]Selection[T] // one instance per group, keyed by group ID
	pool       map[string]Resource[T]
	order      []string                  // stable candidate order for selection
	holders    map[string]map[string]int // resource ID -> holding task ID -> live lease count
	bindings   map[string]string         // taskID -> locked resource ID
}

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent. Round robin is always installed and is always the
// default: a group naming no strategy rotates round robin. Groups and pool
// change afterwards only through CreateGroup, DeleteGroup, Add, and Delete.
func NewManager[T any](ctx context.Context, repo Repository[T], opts ...Option[T]) (*Manager[T], error) {
	if repo == nil {
		return nil, errors.New("repository is required")
	}

	m := &Manager[T]{
		repo: repo,
		factories: map[string]StrategyFactory[T]{
			StrategyRoundRobin: func() Selection[T] { return NewRoundRobin[T]() },
		},
		groups:     make(map[string]Group),
		strategies: make(map[string]Selection[T]),
		pool:       make(map[string]Resource[T]),
		holders:    make(map[string]map[string]int),
		bindings:   make(map[string]string),
	}
	for _, opt := range opts {
		opt(m)
	}
	m.cond = sync.NewCond(&m.mu)

	listedGroups, err := repo.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("load groups: %w", err)
	}
	for _, g := range listedGroups {
		if _, dup := m.groups[g.ID]; dup {
			return nil, fmt.Errorf("duplicate group id %s", g.ID)
		}
		if err := m.adoptGroup(g); err != nil {
			return nil, err
		}
	}
	if _, ok := m.groups[GlobalGroup]; !ok {
		now := time.Now().UTC()
		g := Group{ID: GlobalGroup, Strategy: StrategyRoundRobin, CreatedAt: now, UpdatedAt: now}
		if err := repo.SaveGroup(ctx, g); err != nil {
			return nil, fmt.Errorf("persist global group: %w", err)
		}
		if err := m.adoptGroup(g); err != nil {
			return nil, err
		}
	}

	listed, err := repo.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("load pool: %w", err)
	}
	for _, p := range listed {
		if _, dup := m.pool[p.ID]; dup {
			return nil, fmt.Errorf("duplicate resource id %s", p.ID)
		}
		if p.GroupID == "" {
			p.GroupID = GlobalGroup
		}
		if _, ok := m.groups[p.GroupID]; !ok {
			return nil, fmt.Errorf("resource %s references group %s: %w", p.ID, p.GroupID, ErrGroupNotFound)
		}
		if err := validateHolderPolicy(p.MaxHolders); err != nil {
			return nil, fmt.Errorf("resource %s: %w", p.ID, err)
		}
		m.pool[p.ID] = p
		m.order = append(m.order, p.ID)
		if p.OwnerID != "" {
			if prior, bound := m.bindings[p.OwnerID]; bound {
				return nil, fmt.Errorf("task %s locked to multiple resources (%s, %s)", p.OwnerID, prior, p.ID)
			}
			m.bindings[p.OwnerID] = p.ID
		}
	}
	return m, nil
}

// validateGroup checks g and resolves its strategy factory.
func (m *Manager[T]) validateGroup(g Group) (StrategyFactory[T], error) {
	if g.ID == "" {
		return nil, errors.New("group id is required")
	}
	name := g.Strategy
	if name == "" {
		name = StrategyRoundRobin
	}
	factory, ok := m.factories[name]
	if !ok {
		return nil, fmt.Errorf("group %s references unknown strategy %q", g.ID, name)
	}
	return factory, nil
}

// adoptGroup validates g and installs it with a fresh strategy instance.
func (m *Manager[T]) adoptGroup(g Group) error {
	factory, err := m.validateGroup(g)
	if err != nil {
		return err
	}
	m.groups[g.ID] = g
	m.strategies[g.ID] = factory()
	return nil
}

// validateHolderPolicy accepts 0 (the default of 1), UnlimitedHolders, or an
// explicit cap of at least 1.
func validateHolderPolicy(n int) error {
	if n < UnlimitedHolders {
		return fmt.Errorf("invalid holder policy %d", n)
	}
	return nil
}

// CreateGroup persists and installs a new group. ID must be unset in the
// manager; Strategy must be registered (empty selects round robin).
func (m *Manager[T]) CreateGroup(ctx context.Context, g Group) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, dup := m.groups[g.ID]; dup {
		return fmt.Errorf("group %s already exists", g.ID)
	}
	factory, err := m.validateGroup(g)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	g.CreatedAt, g.UpdatedAt = now, now
	if err := m.repo.SaveGroup(ctx, g); err != nil {
		return fmt.Errorf("persist group %s: %w", g.ID, err)
	}
	m.groups[g.ID] = g
	m.strategies[g.ID] = factory()
	return nil
}

// DeleteGroup cascade-deletes a group and every resource in it, reporting the
// tasks whose durable locks the cascade released so the caller can decide their
// fate. It refuses with ErrGroupInUse while any live lease is held on a member
// — the lease being released is what frees the group — and refuses the global
// group always. Tasks still assigned to the group are not rewritten — this
// package owns no task store — so their next lease fails with ErrGroupNotFound.
func (m *Manager[T]) DeleteGroup(ctx context.Context, id string) (unbound []string, err error) {
	if id == GlobalGroup {
		return nil, fmt.Errorf("group %s cannot be deleted", GlobalGroup)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.groups[id]; !ok {
		return nil, fmt.Errorf("delete group %s: %w", id, ErrGroupNotFound)
	}
	members := m.members(id)
	for _, p := range members {
		if holders := m.holderIDs(p.ID); len(holders) > 0 {
			return nil, fmt.Errorf("%w: %s is held by %s", ErrGroupInUse, p.ID, plural("task", holders))
		}
	}

	// The rows go before the group row: a group row that outlives its member
	// rows makes the next NewManager fail on a resource pointing at a group
	// that no longer exists. And each row goes before its binding, so an
	// aborted remove leaves the live binding matching the OwnerID still stored.
	for _, p := range members {
		if err := m.remove(ctx, p); err != nil {
			return unbound, err
		}
		if p.OwnerID != "" {
			delete(m.bindings, p.OwnerID)
			unbound = append(unbound, p.OwnerID)
		}
	}
	if err := m.repo.DeleteGroup(ctx, id); err != nil {
		return unbound, fmt.Errorf("delete group %s: %w", id, err)
	}
	delete(m.groups, id)
	delete(m.strategies, id)
	m.cond.Broadcast()
	sort.Strings(unbound)
	return unbound, nil
}

// members collects the group's resources in the pool's stable order. Callers
// hold m.mu.
func (m *Manager[T]) members(groupID string) []Resource[T] {
	members := make([]Resource[T], 0)
	for _, pid := range m.order {
		if p := m.pool[pid]; p.GroupID == groupID {
			members = append(members, p)
		}
	}
	return members
}

// Add persists and installs a new unlocked resource, defaulting an empty
// GroupID to the global group. The group must exist.
func (m *Manager[T]) Add(ctx context.Context, p Resource[T]) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p.ID == "" {
		return errors.New("resource id is required")
	}
	if _, dup := m.pool[p.ID]; dup {
		return fmt.Errorf("resource %s already exists", p.ID)
	}
	if p.OwnerID != "" {
		return fmt.Errorf("resource %s: add unlocked resources; Lock creates bindings", p.ID)
	}
	if err := validateHolderPolicy(p.MaxHolders); err != nil {
		return fmt.Errorf("resource %s: %w", p.ID, err)
	}
	if p.GroupID == "" {
		p.GroupID = GlobalGroup
	}
	if _, ok := m.groups[p.GroupID]; !ok {
		return fmt.Errorf("add resource %s to group %s: %w", p.ID, p.GroupID, ErrGroupNotFound)
	}

	now := time.Now().UTC()
	p.CreatedAt, p.UpdatedAt = now, now
	if err := m.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("persist resource %s: %w", p.ID, err)
	}
	m.pool[p.ID] = p
	m.order = append(m.order, p.ID)
	m.cond.Broadcast()
	return nil
}

// Acquire leases a resource under a: its locked resource if it has one — a
// durable binding outranks the requested group — otherwise the pinned member,
// else one rotated from the group's unlocked members. It blocks until a
// resource frees or ctx is done; a group with no resources fails immediately
// with ErrNoResources, and a pin that no longer resolves fails with
// ErrResourceNotFound.
func (m *Manager[T]) Acquire(ctx context.Context, a Assignment) (*Lease[T], error) {
	return m.acquire(ctx, a, false)
}

// Lock durably binds a.TaskID to a resource (the pinned one, or one selected
// from the group when unpinned; idempotent) and leases it. The binding outlives
// the lease and the manager until Unlock, a reassignment, or the resource's
// deletion; no other task can ever acquire it.
func (m *Manager[T]) Lock(ctx context.Context, a Assignment) (*Lease[T], error) {
	return m.acquire(ctx, a, true)
}

// acquire is the shared blocking loop behind Acquire and Lock. A bound task
// only ever leases its own resource, one lease at a time; an unbound task takes
// its pin, or rotates the group's unlocked members, durably binding the pick
// first when lock is set.
func (m *Manager[T]) acquire(ctx context.Context, a Assignment, lock bool) (*Lease[T], error) {
	if a.TaskID == "" {
		return nil, errors.New("assignment task id is required")
	}
	if a.GroupID == "" {
		a.GroupID = GlobalGroup
	}

	// cond.Wait cannot watch ctx, so a watcher wakes the loop on cancellation.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			m.cond.Broadcast()
			m.mu.Unlock()
		case <-stop:
		}
	}()

	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Checked every pass, not once: the pin can be deleted while the loop
		// waits, and a task must not park forever on a resource that is gone.
		if err := m.checkPin(a); err != nil {
			return nil, err
		}

		id, bound := m.bindings[a.TaskID]
		if _, live := m.pool[id]; bound && !live {
			// the bound resource is gone; rotate rather than lease a phantom.
			delete(m.bindings, a.TaskID)
			bound = false
		}
		if bound {
			// A pin is deliberate and outranks a stale lock, but a lease must not
			// make the durable write that resolves it; the deleter of the lock has
			// to be the reassignment that created the disagreement.
			if a.ResourceID != "" && a.ResourceID != id {
				return nil, fmt.Errorf("%w: task %s is locked to %s but pinned to %s; reassign the task to release the lock", ErrPinConflict, a.TaskID, id, a.ResourceID)
			}
			// a locked resource is exclusive to its owner: one lease at a time.
			if m.heldCount(id) == 0 {
				m.hold(id, a.TaskID)
				return &Lease[T]{manager: m, resource: m.pool[id], taskID: a.TaskID}, nil
			}
		} else {
			g, ok := m.groups[a.GroupID]
			if !ok {
				return nil, fmt.Errorf("acquire from group %s: %w", a.GroupID, ErrGroupNotFound)
			}
			if !m.groupHasResources(g.ID) {
				return nil, fmt.Errorf("group %s: %w", g.ID, ErrNoResources)
			}
			// A lock takes only an idle resource; if none is free right now the
			// loop waits, since a release or unlock can still produce one.
			p, found, err := m.selectUnlocked(a, lock)
			if err != nil {
				return nil, err
			}
			if found {
				if lock {
					p.OwnerID = a.TaskID
					p.UpdatedAt = time.Now().UTC()
					if err := m.repo.Save(ctx, p); err != nil {
						return nil, fmt.Errorf("persist lock: %w", err)
					}
					m.pool[p.ID] = p
					m.bindings[a.TaskID] = p.ID
				}
				m.hold(p.ID, a.TaskID)
				return &Lease[T]{manager: m, resource: p, taskID: a.TaskID}, nil
			}
		}

		m.cond.Wait()
	}
}

// checkPin validates a pinned assignment against the live pool: the resource
// must exist and belong to the group the assignment names. A pin that no longer
// resolves is reported rather than quietly degraded to rotation, because "run
// on this one or tell me" is the whole point of pinning — the fallback is the
// consumer's call to make. Unpinned assignments pass. Callers hold m.mu.
func (m *Manager[T]) checkPin(a Assignment) error {
	if a.ResourceID == "" {
		return nil
	}
	p, ok := m.pool[a.ResourceID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrResourceNotFound, a.ResourceID)
	}
	if p.GroupID != a.GroupID {
		return fmt.Errorf("%w: %s is in group %s, not %s", ErrResourceNotInGroup, a.ResourceID, p.GroupID, a.GroupID)
	}
	// A lease frees itself; another task's durable lock does not. Waiting on one
	// is waiting on a condition nothing will satisfy, so it fails instead.
	if p.OwnerID != "" && p.OwnerID != a.TaskID {
		return fmt.Errorf("%w: %s is locked to task %s", ErrResourceLocked, a.ResourceID, p.OwnerID)
	}
	return nil
}

// CheckAssignment reports whether a still resolves against the live pool,
// returning ErrGroupNotFound, ErrResourceNotFound, ErrResourceNotInGroup, or
// ErrResourceLocked when it does not. It is what a recovering task's fallback
// policy asks before deciding whether to run, run without a resource, or
// refuse; the acquire loop asks the same question, so there is one rule, not
// two.
func (m *Manager[T]) CheckAssignment(a Assignment) error {
	if a.GroupID == "" {
		a.GroupID = GlobalGroup
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.groups[a.GroupID]; !ok {
		return fmt.Errorf("group %s: %w", a.GroupID, ErrGroupNotFound)
	}
	return m.checkPin(a)
}

// groupHasResources reports whether any resource belongs to the group, leased
// or not. Callers hold m.mu.
func (m *Manager[T]) groupHasResources(groupID string) bool {
	for _, id := range m.order {
		if m.pool[id].GroupID == groupID {
			return true
		}
	}
	return false
}

// effectiveCap resolves the resource's holder cap: its own policy, else 1.
// Callers hold m.mu.
func (m *Manager[T]) effectiveCap(p Resource[T]) int {
	if p.MaxHolders != 0 {
		return p.MaxHolders
	}
	return 1
}

// hold records one live lease of the resource by taskID. Tracking the holder,
// not just a count, is what lets a refused delete name the tasks blocking it.
// Callers hold m.mu.
func (m *Manager[T]) hold(resourceID, taskID string) {
	holders, ok := m.holders[resourceID]
	if !ok {
		holders = make(map[string]int)
		m.holders[resourceID] = holders
	}
	holders[taskID]++
}

// unhold drops one live lease of the resource by taskID, pruning empty entries
// so heldCount and holderIDs never see a stale zero. Callers hold m.mu.
func (m *Manager[T]) unhold(resourceID, taskID string) {
	holders := m.holders[resourceID]
	if holders[taskID] > 0 {
		holders[taskID]--
	}
	if holders[taskID] == 0 {
		delete(holders, taskID)
	}
	if len(holders) == 0 {
		delete(m.holders, resourceID)
	}
}

// heldCount is the resource's live lease count across every holder, the number
// the holder cap is measured against. Callers hold m.mu.
func (m *Manager[T]) heldCount(resourceID string) int {
	n := 0
	for _, held := range m.holders[resourceID] {
		n += held
	}
	return n
}

// holderIDs lists the tasks holding a live lease on the resource, sorted so
// refusals read stably. Callers hold m.mu.
func (m *Manager[T]) holderIDs(resourceID string) []string {
	ids := make([]string, 0, len(m.holders[resourceID]))
	for taskID := range m.holders[resourceID] {
		ids = append(ids, taskID)
	}
	sort.Strings(ids)
	return ids
}

// selectUnlocked picks an unlocked, under-capacity resource from the
// assignment's group via that group's strategy instance, or the pinned resource
// alone when a names one; found is false when there are no candidates.
// requireIdle narrows the field to resources nobody is holding, for the callers
// that are about to bind one: a lock must not land on a resource another task is
// already leasing, or the owner would queue behind a stranger on its own.
// Callers hold m.mu.
func (m *Manager[T]) selectUnlocked(a Assignment, requireIdle bool) (Resource[T], bool, error) {
	candidates := make([]Resource[T], 0, len(m.order))
	eligible := make(map[string]Resource[T], len(m.order))
	for _, id := range m.order {
		p := m.pool[id]
		if p.GroupID != a.GroupID || p.OwnerID != "" {
			continue
		}
		if a.ResourceID != "" && p.ID != a.ResourceID {
			continue
		}
		held := m.heldCount(id)
		if requireIdle && held > 0 {
			continue
		}
		if cap := m.effectiveCap(p); cap == UnlimitedHolders || held < cap {
			candidates = append(candidates, p)
			eligible[p.ID] = p
		}
	}
	if len(candidates) == 0 {
		return zero[T](), false, nil
	}
	// A pin leaves exactly one candidate. Running it through the group's
	// strategy anyway would advance shared rotation state for a choice nobody
	// made, skewing what the group's other tasks get next.
	if a.ResourceID != "" {
		return candidates[0], true, nil
	}

	p, err := m.strategies[a.GroupID].Select(candidates)
	if err != nil {
		return zero[T](), false, fmt.Errorf("selection: %w", err)
	}
	// Validated against the candidates, not the pool: a strategy returning some
	// other pooled resource would hand out one already at capacity, or let a lock
	// overwrite another task's binding.
	live, ok := eligible[p.ID]
	if !ok {
		return zero[T](), false, fmt.Errorf("selection returned resource %s, which was not a candidate", p.ID)
	}
	return live, true, nil
}

// zero is the empty resource returned alongside an error or a miss.
func zero[T any]() Resource[T] {
	var empty Resource[T]
	return empty
}

// Unlock removes taskID's durable lock, returning its resource to the rotating
// pool. It is a no-op if taskID has no locked resource.
func (m *Manager[T]) Unlock(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	id, bound := m.bindings[taskID]
	if !bound {
		return nil
	}
	return m.unlock(ctx, taskID, id)
}

// ReleaseStaleLock drops a.TaskID's durable lock when its new placement no
// longer fits: a pin naming a different resource, a group the locked resource
// is not in, or no placement at all. A lock the placement still fits is kept, so
// repointing a task at what it already holds does not briefly return it to the
// pool for another task to take.
//
// It is the reassignment counterpart to Unlock — wire it into a task service
// the same way — and the sanctioned resolution of ErrPinConflict: a reassign is
// a deliberate act and outranks a lock, while a lease is not and must not.
//
// A live lease on the released resource is untouched: the run holding it keeps
// it to completion, and the new placement takes effect at the task's next lock.
// Unlike Acquire, an empty GroupID here means no group rather than the global
// one, since a task reassigned to nothing at all must lose its lock.
func (m *Manager[T]) ReleaseStaleLock(ctx context.Context, a Assignment) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	id, bound := m.bindings[a.TaskID]
	if !bound {
		return nil
	}
	if locked, live := m.pool[id]; live && fits(a, locked) {
		return nil
	}
	return m.unlock(ctx, a.TaskID, id)
}

// fits reports whether a's placement still describes p: the pin names it, or
// there is no pin and it belongs to the assigned group.
func fits[T any](a Assignment, p Resource[T]) bool {
	if a.ResourceID != "" {
		return a.ResourceID == p.ID
	}
	return a.GroupID != "" && a.GroupID == p.GroupID
}

// unlock clears the durable lock and returns the resource to the rotating pool,
// waking waiters. A binding whose resource is already gone is simply dropped.
// Callers hold m.mu.
func (m *Manager[T]) unlock(ctx context.Context, taskID, resourceID string) error {
	p, live := m.pool[resourceID]
	if !live {
		delete(m.bindings, taskID)
		return nil
	}
	p.OwnerID = ""
	p.UpdatedAt = time.Now().UTC()
	if err := m.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("persist unlock: %w", err)
	}
	m.pool[resourceID] = p
	delete(m.bindings, taskID)
	m.cond.Broadcast()
	return nil
}

// Delete removes a resource from the pool and the repository, reporting the
// task whose durable lock the deletion released — the caller decides that
// task's fate. It refuses with ErrResourceInUse while any live lease is held
// on the resource: the lease is the fact of use, and it being released is what
// frees the resource for deletion.
func (m *Manager[T]) Delete(ctx context.Context, id string) (unbound []string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.pool[id]
	if !ok {
		// Nothing live to guard, but the store may still carry a row this
		// manager never loaded.
		return nil, m.repo.Delete(ctx, id)
	}
	if holders := m.holderIDs(id); len(holders) > 0 {
		return nil, fmt.Errorf("%w: %s is held by %s", ErrResourceInUse, id, plural("task", holders))
	}

	// The row goes before the binding: an aborted remove must leave the live
	// binding matching the OwnerID still in the store.
	if err := m.remove(ctx, p); err != nil {
		return nil, err
	}
	if p.OwnerID != "" {
		delete(m.bindings, p.OwnerID)
		unbound = []string{p.OwnerID}
	}
	return unbound, nil
}

// plural renders an id list as "task t1" or "tasks t1, t2", so a refusal reads
// as a sentence and still names everything blocking it.
func plural(noun string, ids []string) string {
	if len(ids) == 1 {
		return noun + " " + ids[0]
	}
	return noun + "s " + strings.Join(ids, ", ")
}

// remove deletes p from the repository and then the live pool, waking waiters.
// The store goes first, as everywhere else here: dropping it from memory on a
// failed delete would leave a resource the manager cannot see but reloads on the
// next start — still carrying the lock this delete was meant to clear.
// Callers hold m.mu.
func (m *Manager[T]) remove(ctx context.Context, p Resource[T]) error {
	if err := m.repo.Delete(ctx, p.ID); err != nil {
		return fmt.Errorf("delete resource %s: %w", p.ID, err)
	}
	delete(m.pool, p.ID)
	delete(m.holders, p.ID)
	for i, id := range m.order {
		if id == p.ID {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.cond.Broadcast()
	return nil
}

// Lease is a live hold on one resource. Release it exactly once when done.
type Lease[T any] struct {
	manager  *Manager[T]
	resource Resource[T]
	taskID   string
	once     sync.Once
}

// Resource returns the leased resource as of acquisition.
func (l *Lease[T]) Resource() Resource[T] {
	return l.resource
}

// Release frees the resource, records the outcome selection strategies learn
// from, and persists it. Only the first call acts; later calls return nil.
func (l *Lease[T]) Release(success bool) error {
	var err error
	l.once.Do(func() { err = l.manager.release(l.resource.ID, l.taskID, success) })
	return err
}

// release updates stats, frees the holder slot, wakes waiters, and persists the
// updated record before dropping the lock — a save made outside it could land
// after a concurrent Unlock's and resurrect the stale owner durably. The save
// uses a background context so it lands even when the caller's context is gone.
func (m *Manager[T]) release(id, taskID string, success bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.pool[id]
	if ok {
		if success {
			p.Successes++
		} else {
			p.Failures++
		}
		p.UpdatedAt = time.Now().UTC()
		m.pool[id] = p
	}
	m.unhold(id, taskID)
	m.cond.Broadcast()

	if !ok {
		return nil // resource was deleted while leased; nothing to persist
	}
	return m.repo.Save(context.Background(), p)
}
