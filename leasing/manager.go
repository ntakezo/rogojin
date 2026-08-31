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
type Option[R any, P Leasable[R]] func(*Manager[R, P])

// WithStrategy registers factory under name, so groups may reference custom
// selection algorithms beyond round robin (or override it under its own name).
func WithStrategy[R any, P Leasable[R]](name string, factory StrategyFactory[R]) Option[R, P] {
	return func(m *Manager[R, P]) { m.factories[name] = factory }
}

// A Manager allocates records of one model to tasks: locked resources go only
// to their owner, unlocked ones rotate within their group through the group's
// selection strategy under the resource's holder cap. It owns all live lease
// state; the Repository only stores bytes. R is the model, a struct embedding
// Resource; P is its pointer, inferred everywhere but a type alias. A Manager
// is safe for concurrent use.
type Manager[R any, P Leasable[R]] struct {
	repo      Repository[R]
	factories map[string]StrategyFactory[R]

	mu         sync.Mutex
	cond       *sync.Cond
	groups     map[string]Group
	strategies map[string]Selection[R] // one instance per group, keyed by group ID
	pool       map[string]R
	order      []string                  // stable candidate order for selection
	holders    map[string]map[string]int // resource ID -> holding task ID -> live lease count
	bindings   map[string]string         // taskID -> locked resource ID
}

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent. Round robin is always installed and is always the
// default: a group naming no strategy rotates round robin. Groups and pool
// change afterwards only through CreateGroup, DeleteGroup, Add, and Delete.
//
// Seed groups before resources: a resource whose GroupID has no stored group
// fails construction with ErrGroupNotFound, so a repository seeded directly
// must SaveGroup before Save. Resources with no GroupID land in the global
// group, which is created here if absent.
func NewManager[R any, P Leasable[R]](ctx context.Context, repo Repository[R], opts ...Option[R, P]) (*Manager[R, P], error) {
	if repo == nil {
		return nil, errors.New("repository is required")
	}

	m := &Manager[R, P]{
		repo: repo,
		factories: map[string]StrategyFactory[R]{
			StrategyRoundRobin: func() Selection[R] { return NewRoundRobin[R]() },
		},
		groups:     make(map[string]Group),
		strategies: make(map[string]Selection[R]),
		pool:       make(map[string]R),
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
		c := m.core(&p)
		if _, dup := m.pool[c.ID]; dup {
			return nil, fmt.Errorf("duplicate resource id %s", c.ID)
		}
		if c.GroupID == "" {
			c.GroupID = GlobalGroup
		}
		if _, ok := m.groups[c.GroupID]; !ok {
			return nil, fmt.Errorf("resource %s references group %s (seed groups before resources: SaveGroup before Save): %w", c.ID, c.GroupID, ErrGroupNotFound)
		}
		if err := validateHolderPolicy(c.MaxHolders); err != nil {
			return nil, fmt.Errorf("resource %s: %w", c.ID, err)
		}
		m.pool[c.ID] = p
		m.order = append(m.order, c.ID)
		if c.OwnerID != "" {
			if prior, bound := m.bindings[c.OwnerID]; bound {
				return nil, fmt.Errorf("task %s locked to multiple resources (%s, %s)", c.OwnerID, prior, c.ID)
			}
			m.bindings[c.OwnerID] = c.ID
		}
	}
	return m, nil
}

// core is the leasing record embedded in r: the fields this package reads and
// writes, reached without knowing what else the model carries. The pool holds
// values, so callers take a local copy first and write it back only after the
// store has accepted it.
func (m *Manager[R, P]) core(r *R) *Resource {
	return P(r).core()
}

// validateGroup checks g and resolves its strategy factory.
func (m *Manager[R, P]) validateGroup(g Group) (StrategyFactory[R], error) {
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
func (m *Manager[R, P]) adoptGroup(g Group) error {
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
func (m *Manager[R, P]) CreateGroup(ctx context.Context, g Group) error {
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

// UpdateGroup edits one group through fn and persists the result — Update's
// counterpart for groups, completing their create, read, update, delete
// surface. ID, CreatedAt, and UpdatedAt are not fn's to change: whatever it
// does to them is undone before the save. A strategy change is validated
// against the registered factories and installs a fresh strategy instance, so
// rotation state (a cursor, a sampler's history) starts over; an unchanged
// strategy keeps its state. The global group is updatable — its refs and
// strategy are as legitimate as any other group's. fn runs under the
// manager's lock, so it must not block.
func (m *Manager[R, P]) UpdateGroup(ctx context.Context, id string, fn func(*Group)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	g, ok := m.groups[id]
	if !ok {
		return fmt.Errorf("update group %s: %w", id, ErrGroupNotFound)
	}
	kept := g
	fn(&g)
	g.ID, g.CreatedAt = kept.ID, kept.CreatedAt
	factory, err := m.validateGroup(g)
	if err != nil {
		return err
	}
	g.UpdatedAt = time.Now().UTC()
	if err := m.repo.SaveGroup(ctx, g); err != nil {
		return fmt.Errorf("persist group %s: %w", id, err)
	}
	m.groups[id] = g
	if strategyName(g) != strategyName(kept) {
		m.strategies[id] = factory()
	}
	return nil
}

// strategyName is the strategy a group actually runs: its own, or round robin
// when it names none.
func strategyName(g Group) string {
	if g.Strategy == "" {
		return StrategyRoundRobin
	}
	return g.Strategy
}

// DeleteGroup cascade-deletes a group and every resource in it, reporting the
// tasks whose durable locks the cascade released so the caller can decide their
// fate. It refuses with ErrGroupInUse while any live lease is held on a member
// — the lease being released is what frees the group — and refuses the global
// group always. Tasks still assigned to the group are not rewritten — this
// package owns no task store — so their next lease fails with ErrGroupNotFound.
func (m *Manager[R, P]) DeleteGroup(ctx context.Context, id string) (unbound []string, err error) {
	if id == GlobalGroup {
		return nil, fmt.Errorf("group %s cannot be deleted", GlobalGroup)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.groups[id]; !ok {
		return nil, fmt.Errorf("delete group %s: %w", id, ErrGroupNotFound)
	}
	members := m.members(id)
	for _, resourceID := range members {
		if holders := m.holderIDs(resourceID); len(holders) > 0 {
			return nil, fmt.Errorf("%w: %s is held by %s", ErrGroupInUse, resourceID, plural("task", holders))
		}
	}

	// The rows go before the group row: a group row that outlives its member
	// rows makes the next NewManager fail on a resource pointing at a group
	// that no longer exists. And each row goes before its binding, so an
	// aborted remove leaves the live binding matching the OwnerID still stored.
	for _, resourceID := range members {
		p := m.pool[resourceID]
		owner := m.core(&p).OwnerID
		if err := m.remove(ctx, resourceID); err != nil {
			return unbound, err
		}
		if owner != "" {
			delete(m.bindings, owner)
			unbound = append(unbound, owner)
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

// members collects the ids of the group's resources in the pool's stable
// order. Callers hold m.mu.
func (m *Manager[R, P]) members(groupID string) []string {
	members := make([]string, 0)
	for _, id := range m.order {
		p := m.pool[id]
		if m.core(&p).GroupID == groupID {
			members = append(members, id)
		}
	}
	return members
}

// Add persists and installs a new unlocked resource, defaulting an empty
// GroupID to the global group. The group must exist — create it first with
// CreateGroup, or ErrGroupNotFound.
func (m *Manager[R, P]) Add(ctx context.Context, p R) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	c := m.core(&p)
	if c.ID == "" {
		return errors.New("resource id is required")
	}
	if _, dup := m.pool[c.ID]; dup {
		return fmt.Errorf("resource %s already exists", c.ID)
	}
	if c.OwnerID != "" {
		return fmt.Errorf("resource %s: add unlocked resources; Lock creates bindings", c.ID)
	}
	if err := validateHolderPolicy(c.MaxHolders); err != nil {
		return fmt.Errorf("resource %s: %w", c.ID, err)
	}
	if c.GroupID == "" {
		c.GroupID = GlobalGroup
	}
	if _, ok := m.groups[c.GroupID]; !ok {
		return fmt.Errorf("add resource %s to group %s: %w", c.ID, c.GroupID, ErrGroupNotFound)
	}

	now := time.Now().UTC()
	c.CreatedAt, c.UpdatedAt = now, now
	if err := m.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("persist resource %s: %w", c.ID, err)
	}
	m.pool[c.ID] = p
	m.order = append(m.order, c.ID)
	m.cond.Broadcast()
	return nil
}

// Update applies fn to the pooled record and persists the result. It is how a
// model's own fields change after Add — an outcome count, a rotated
// credential. The manager never reads those fields, but it owns the copy every
// lease is cut from and the write that makes a change durable, so the edit has
// to go through it: a record saved around the manager would be overwritten by
// its next write of that row, and would never reach the strategies selecting
// over the pool. The leasing fields are not fn's to change — whatever it does
// to them is undone before the save — and it runs under the manager's lock,
// so it must not block.
func (m *Manager[R, P]) Update(ctx context.Context, id string, fn func(*R)) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.pool[id]
	if !ok {
		return fmt.Errorf("update resource %s: not in the pool", id)
	}
	kept := *m.core(&p)
	fn(&p)
	c := m.core(&p)
	*c = kept
	c.UpdatedAt = time.Now().UTC()
	if err := m.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("persist resource %s: %w", id, err)
	}
	m.pool[id] = p
	return nil
}

// Acquire leases a resource under a: its locked resource if it has one — a
// durable binding outranks the requested group — otherwise the pinned member,
// else one rotated from the group's unlocked members. It blocks until a
// resource frees or ctx is done; a group with no resources fails immediately
// with ErrNoResources, and a pin that no longer resolves fails with
// ErrResourceNotFound.
func (m *Manager[R, P]) Acquire(ctx context.Context, a Assignment) (*Lease[R, P], error) {
	return m.acquire(ctx, a, false)
}

// Lock durably binds a.TaskID to a resource (the pinned one, or one selected
// from the group when unpinned; idempotent) and leases it. The binding outlives
// the lease and the manager until Unlock, a reassignment, or the resource's
// deletion; no other task can ever acquire it.
func (m *Manager[R, P]) Lock(ctx context.Context, a Assignment) (*Lease[R, P], error) {
	return m.acquire(ctx, a, true)
}

// acquire is the shared blocking loop behind Acquire and Lock. A bound task
// only ever leases its own resource, one lease at a time; an unbound task takes
// its pin, or rotates the group's unlocked members, durably binding the pick
// first when lock is set.
func (m *Manager[R, P]) acquire(ctx context.Context, a Assignment, lock bool) (*Lease[R, P], error) {
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
				return m.lease(m.pool[id], a.TaskID), nil
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
				c := m.core(&p)
				if lock {
					c.OwnerID = a.TaskID
					c.UpdatedAt = time.Now().UTC()
					if err := m.repo.Save(ctx, p); err != nil {
						return nil, fmt.Errorf("persist lock: %w", err)
					}
					m.pool[c.ID] = p
					m.bindings[a.TaskID] = c.ID
				}
				m.hold(c.ID, a.TaskID)
				return m.lease(p, a.TaskID), nil
			}
		}

		m.cond.Wait()
	}
}

// lease wraps p, as of now, in the Lease handed to taskID, carrying the group
// it was acquired under. Callers hold m.mu.
func (m *Manager[R, P]) lease(p R, taskID string) *Lease[R, P] {
	return &Lease[R, P]{manager: m, resource: p, group: m.groups[m.core(&p).GroupID], taskID: taskID}
}

// checkPin validates a pinned assignment against the live pool: the resource
// must exist and belong to the group the assignment names. A pin that no longer
// resolves is reported rather than quietly degraded to rotation, because "run
// on this one or tell me" is the whole point of pinning — the fallback is the
// consumer's call to make. Unpinned assignments pass. Callers hold m.mu.
func (m *Manager[R, P]) checkPin(a Assignment) error {
	if a.ResourceID == "" {
		return nil
	}
	p, ok := m.pool[a.ResourceID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrResourceNotFound, a.ResourceID)
	}
	c := m.core(&p)
	if c.GroupID != a.GroupID {
		return fmt.Errorf("%w: %s is in group %s, not %s", ErrResourceNotInGroup, a.ResourceID, c.GroupID, a.GroupID)
	}
	// A lease frees itself; another task's durable lock does not. Waiting on one
	// is waiting on a condition nothing will satisfy, so it fails instead.
	if c.OwnerID != "" && c.OwnerID != a.TaskID {
		return fmt.Errorf("%w: %s is locked to task %s", ErrResourceLocked, a.ResourceID, c.OwnerID)
	}
	return nil
}

// CheckAssignment reports whether a still resolves against the live pool,
// returning ErrGroupNotFound, ErrResourceNotFound, ErrResourceNotInGroup, or
// ErrResourceLocked when it does not. It is what a recovering task's fallback
// policy asks before deciding whether to run, run without a resource, or
// refuse; the acquire loop asks the same question, so there is one rule, not
// two.
func (m *Manager[R, P]) CheckAssignment(a Assignment) error {
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

// GetGroup reports one group by id and whether the manager knows it, named
// for the same read on the persistence ports.
func (m *Manager[R, P]) GetGroup(id string) (Group, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[id]
	return g, ok
}

// ListGroups returns every group, sorted by ID so listings read stably. It is
// the Repository read of the same name, served from the manager's own state.
func (m *Manager[R, P]) ListGroups() []Group {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Group, 0, len(m.groups))
	for _, g := range m.groups {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Get reports one resource by id and whether the manager knows it. The
// record is a copy: its embedded Resource shows the durable lock (OwnerID) as
// of the read, and mutating it changes nothing — Add, Update, and Delete are
// the write surface.
func (m *Manager[R, P]) Get(id string) (R, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.pool[id]
	return p, ok
}

// List returns a copy of the pool in the stable order resources were adopted
// or added — the Repository read of the same name, served from the manager's
// own state. Records show durable locks; live leases are Held's to report.
func (m *Manager[R, P]) List() []R {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]R, 0, len(m.order))
	for _, id := range m.order {
		if p, ok := m.pool[id]; ok {
			out = append(out, p)
		}
	}
	return out
}

// Held reports the assignment of every live lease — in acquire or lock mode —
// whose record and group satisfy pred. It is one of the two facts this
// package owns, offered raw: the predicate brings the meaning (which field of
// the model, which ref), the manager brings who holds what. Results are sorted
// by task then resource so refusals built on them read stably.
func (m *Manager[R, P]) Held(pred func(R, Group) bool) []Assignment {
	m.mu.Lock()
	defer m.mu.Unlock()

	matched := make([]Assignment, 0)
	for resourceID, holders := range m.holders {
		p, ok := m.pool[resourceID]
		if !ok {
			continue
		}
		c := m.core(&p)
		if !pred(p, m.groups[c.GroupID]) {
			continue
		}
		for taskID := range holders {
			matched = append(matched, Assignment{TaskID: taskID, GroupID: c.GroupID, ResourceID: c.ID})
		}
	}
	sortAssignments(matched)
	return matched
}

// Locked reports the assignment of every durable lock — running or not — whose
// record and group satisfy pred. See Held; a task both locked and leasing
// appears in both reports.
func (m *Manager[R, P]) Locked(pred func(R, Group) bool) []Assignment {
	m.mu.Lock()
	defer m.mu.Unlock()

	matched := make([]Assignment, 0)
	for taskID, resourceID := range m.bindings {
		p, ok := m.pool[resourceID]
		if !ok {
			continue
		}
		c := m.core(&p)
		if !pred(p, m.groups[c.GroupID]) {
			continue
		}
		matched = append(matched, Assignment{TaskID: taskID, GroupID: c.GroupID, ResourceID: c.ID})
	}
	sortAssignments(matched)
	return matched
}

// sortAssignments orders by task then resource, the stable order Held and
// Locked report in.
func sortAssignments(a []Assignment) {
	sort.Slice(a, func(i, j int) bool {
		if a[i].TaskID != a[j].TaskID {
			return a[i].TaskID < a[j].TaskID
		}
		return a[i].ResourceID < a[j].ResourceID
	})
}

// groupHasResources reports whether any resource belongs to the group, leased
// or not. Callers hold m.mu.
func (m *Manager[R, P]) groupHasResources(groupID string) bool {
	for _, id := range m.order {
		p := m.pool[id]
		if m.core(&p).GroupID == groupID {
			return true
		}
	}
	return false
}

// effectiveCap resolves the resource's holder cap: its own policy, else 1.
func effectiveCap(c *Resource) int {
	if c.MaxHolders != 0 {
		return c.MaxHolders
	}
	return 1
}

// hold records one live lease of the resource by taskID. Tracking the holder,
// not just a count, is what lets a refused delete name the tasks blocking it.
// Callers hold m.mu.
func (m *Manager[R, P]) hold(resourceID, taskID string) {
	holders, ok := m.holders[resourceID]
	if !ok {
		holders = make(map[string]int)
		m.holders[resourceID] = holders
	}
	holders[taskID]++
}

// unhold drops one live lease of the resource by taskID, pruning empty entries
// so heldCount and holderIDs never see a stale zero. Callers hold m.mu.
func (m *Manager[R, P]) unhold(resourceID, taskID string) {
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
func (m *Manager[R, P]) heldCount(resourceID string) int {
	n := 0
	for _, held := range m.holders[resourceID] {
		n += held
	}
	return n
}

// holderIDs lists the tasks holding a live lease on the resource, sorted so
// refusals read stably. Callers hold m.mu.
func (m *Manager[R, P]) holderIDs(resourceID string) []string {
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
func (m *Manager[R, P]) selectUnlocked(a Assignment, requireIdle bool) (R, bool, error) {
	candidates := make([]R, 0, len(m.order))
	eligible := make(map[string]R, len(m.order))
	for _, id := range m.order {
		p := m.pool[id]
		c := m.core(&p)
		if c.GroupID != a.GroupID || c.OwnerID != "" {
			continue
		}
		if a.ResourceID != "" && c.ID != a.ResourceID {
			continue
		}
		held := m.heldCount(id)
		if requireIdle && held > 0 {
			continue
		}
		if cap := effectiveCap(c); cap == UnlimitedHolders || held < cap {
			candidates = append(candidates, p)
			eligible[c.ID] = p
		}
	}
	if len(candidates) == 0 {
		return zero[R](), false, nil
	}
	// A pin leaves exactly one candidate. Running it through the group's
	// strategy anyway would advance shared rotation state for a choice nobody
	// made, skewing what the group's other tasks get next.
	if a.ResourceID != "" {
		return candidates[0], true, nil
	}

	picked, err := m.strategies[a.GroupID].Select(candidates)
	if err != nil {
		return zero[R](), false, fmt.Errorf("selection: %w", err)
	}
	// Validated against the candidates, not the pool: a strategy returning some
	// other pooled resource would hand out one already at capacity, or let a lock
	// overwrite another task's binding.
	pickedID := m.core(&picked).ID
	live, ok := eligible[pickedID]
	if !ok {
		return zero[R](), false, fmt.Errorf("selection returned resource %s, which was not a candidate", pickedID)
	}
	return live, true, nil
}

// zero is the empty record returned alongside an error or a miss.
func zero[R any]() R {
	var empty R
	return empty
}

// Unlock removes taskID's durable lock, returning its resource to the rotating
// pool. It is a no-op if taskID has no locked resource.
func (m *Manager[R, P]) Unlock(ctx context.Context, taskID string) error {
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
func (m *Manager[R, P]) ReleaseStaleLock(ctx context.Context, a Assignment) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	id, bound := m.bindings[a.TaskID]
	if !bound {
		return nil
	}
	if locked, live := m.pool[id]; live && fits(a, m.core(&locked)) {
		return nil
	}
	return m.unlock(ctx, a.TaskID, id)
}

// fits reports whether a's placement still describes c: the pin names it, or
// there is no pin and it belongs to the assigned group.
func fits(a Assignment, c *Resource) bool {
	if a.ResourceID != "" {
		return a.ResourceID == c.ID
	}
	return a.GroupID != "" && a.GroupID == c.GroupID
}

// unlock clears the durable lock and returns the resource to the rotating pool,
// waking waiters. A binding whose resource is already gone is simply dropped.
// Callers hold m.mu.
func (m *Manager[R, P]) unlock(ctx context.Context, taskID, resourceID string) error {
	p, live := m.pool[resourceID]
	if !live {
		delete(m.bindings, taskID)
		return nil
	}
	c := m.core(&p)
	c.OwnerID = ""
	c.UpdatedAt = time.Now().UTC()
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
func (m *Manager[R, P]) Delete(ctx context.Context, id string) (unbound []string, err error) {
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
	owner := m.core(&p).OwnerID
	if err := m.remove(ctx, id); err != nil {
		return nil, err
	}
	if owner != "" {
		delete(m.bindings, owner)
		unbound = []string{owner}
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

// remove deletes the resource from the repository and then the live pool,
// waking waiters. The store goes first, as everywhere else here: dropping it
// from memory on a failed delete would leave a resource the manager cannot see
// but reloads on the next start — still carrying the lock this delete was
// meant to clear. Callers hold m.mu.
func (m *Manager[R, P]) remove(ctx context.Context, id string) error {
	if err := m.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("delete resource %s: %w", id, err)
	}
	delete(m.pool, id)
	delete(m.holders, id)
	for i, pooled := range m.order {
		if pooled == id {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.cond.Broadcast()
	return nil
}

// Lease is a live hold on one resource. Release it exactly once when done.
type Lease[R any, P Leasable[R]] struct {
	manager  *Manager[R, P]
	resource R
	group    Group
	taskID   string
	once     sync.Once
}

// Resource returns the leased record as of acquisition.
func (l *Lease[R, P]) Resource() R {
	return l.resource
}

// Group returns the group of the leased resource as of acquisition, so
// holders can read group-level Refs without another lookup surface.
func (l *Lease[R, P]) Group() Group {
	return l.group
}

// Release frees the resource, waking any acquire waiting on it. Only the first
// call acts.
func (l *Lease[R, P]) Release() {
	l.once.Do(func() { l.manager.release(l.manager.core(&l.resource).ID, l.taskID) })
}

// release frees the holder slot and wakes waiters. Nothing about the record
// changed, so nothing is written.
func (m *Manager[R, P]) release(id, taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.unhold(id, taskID)
	m.cond.Broadcast()
}
