package leasing

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/comms"
)

// An Option configures a Manager at construction.
type Option[R any, P Leasable[R]] func(*Manager[R, P])

// WithStrategy registers factory under name, so groups may reference custom
// selection algorithms beyond round robin (or override it under its own name,
// which also turns off the store-side rotation cursor in favor of the
// override's own state).
func WithStrategy[R any, P Leasable[R]](name string, factory StrategyFactory[R]) Option[R, P] {
	return func(m *Manager[R, P]) {
		m.factories[name] = factory
		if name == StrategyRoundRobin {
			m.rrOverridden = true
		}
	}
}

// WithNotifier sets the Notifier acquires park on while they wait for
// capacity (default: the in-process comms.NewNotifier). A distributed
// deployment installs a shared one so a release on one node wakes acquirers
// on another; even without one, waiters re-check on the lease TTL's cadence,
// since expiry frees capacity in the store no matter who is watching.
func WithNotifier[R any, P Leasable[R]](n comms.Notifier) Option[R, P] {
	if n == nil {
		panic("leasing: WithNotifier requires a notifier")
	}
	return func(m *Manager[R, P]) { m.notifier = n }
}

// WithTopic names the notifier topic this manager's waiters park on. Every
// node of a distributed deployment must name the same topic for the same
// kind — the resource packages pass their Kind — while the default is a
// per-manager string, correct for a process-local notifier.
func WithTopic[R any, P Leasable[R]](topic string) Option[R, P] {
	if topic == "" {
		panic("leasing: WithTopic requires a topic")
	}
	return func(m *Manager[R, P]) { m.topic = topic }
}

// WithLeaseTTL sets how long a hold lives between heartbeat renewals
// (default 30s). Shorter reclaims a crashed holder's capacity sooner but
// tightens the renewal deadline a healthy holder must keep meeting.
func WithLeaseTTL[R any, P Leasable[R]](d time.Duration) Option[R, P] {
	if d <= 0 {
		panic("leasing: WithLeaseTTL requires a positive duration")
	}
	return func(m *Manager[R, P]) { m.ttl = d }
}

// A Manager allocates records of one model to tasks: locked resources go only
// to their owner, unlocked ones rotate within their group through the group's
// selection strategy under the resource's holder cap. The Repository is the
// authority on capacity, locks, and versions — what a Manager holds are
// caches of it, refreshed when an acquirer runs out of candidates — so
// several managers over one store agree by construction. R is the model, a
// struct embedding Resource; P is its pointer, inferred everywhere but a type
// alias. A Manager is safe for concurrent use.
type Manager[R any, P Leasable[R]] struct {
	repo         Repository[R]
	factories    map[string]StrategyFactory[R]
	notifier     comms.Notifier
	topic        string
	ttl          time.Duration
	rrOverridden bool // a WithStrategy("roundrobin", …) supplied its own state

	closing   chan struct{}
	done      chan struct{}
	closeOnce sync.Once

	mu         sync.Mutex
	groups     map[string]Group
	strategies map[string]Selection[R] // one instance per group, keyed by group ID
	pool       map[string]R
	order      []string                  // stable candidate order for selection
	holders    map[string]map[string]int // resource ID -> holding task ID -> live lease count, this process's leases only
	bindings   map[string]string         // taskID -> locked resource ID
}

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent. Round robin is always installed and is always the
// default: a group naming no strategy rotates round robin. The caches change
// afterwards through CreateGroup, DeleteGroup, Add, and Delete — and through
// the acquire loop's refresh, which re-reads the store whenever a waiter
// runs out of candidates, so another node's changes surface without a
// restart.
//
// Seed groups before resources: a resource whose GroupID has no stored group
// fails construction with ErrGroupNotFound, so a repository seeded directly
// must SaveGroup before Save. Resources with no GroupID land in the global
// group, which is created here if absent.
//
// A nil repository runs the pool purely in memory, on the same in-process
// store NewMemoryRepository returns: capacity, locks, and versions are
// enforced for real, but the manager starts empty (seed it through Add) and
// no inventory or durable lock survives the process — the same bargain a nil
// task repository strikes.
func NewManager[R any, P Leasable[R]](ctx context.Context, repo Repository[R], opts ...Option[R, P]) (*Manager[R, P], error) {
	if repo == nil {
		repo = NewMemoryRepository[R, P]()
	}

	m := &Manager[R, P]{
		repo: repo,
		factories: map[string]StrategyFactory[R]{
			StrategyRoundRobin: func() Selection[R] { return NewRoundRobin[R]() },
		},
		ttl:        30 * time.Second,
		groups:     make(map[string]Group),
		strategies: make(map[string]Selection[R]),
		pool:       make(map[string]R),
		holders:    make(map[string]map[string]int),
		bindings:   make(map[string]string),
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.notifier == nil {
		m.notifier = comms.NewNotifier()
	}
	if m.topic == "" {
		m.topic = fmt.Sprintf("lease/%p", m)
	}

	// A task the store shows locked to several resources — two nodes' blind
	// writes of an earlier era, or a crash between a release and its retry —
	// is repaired rather than refused: the store is the authority, and the
	// oldest lock is the one some run has been working against the longest,
	// so the newer ones are released and the load re-reads what remains. The
	// bound is for a store being written under the load; repair converging is
	// the normal case in one pass.
	for attempt := 0; ; attempt++ {
		repairs, err := m.load(ctx)
		if err != nil {
			return nil, err
		}
		if len(repairs) == 0 {
			break
		}
		if attempt == 2 {
			return nil, fmt.Errorf("lock repair did not converge; the store is being written during load")
		}
		for _, r := range repairs {
			if err := repo.ReleaseLock(ctx, r.resourceID, r.taskID); err != nil {
				return nil, fmt.Errorf("release duplicate lock on %s: %w", r.resourceID, err)
			}
		}
	}

	m.closing = make(chan struct{})
	m.done = make(chan struct{})
	go m.renewLoop()
	return m, nil
}

// renewLoop is the lease heartbeat: at a third of the TTL it extends every
// hold of every task this process is leasing for, so a healthy holder never
// expires and a crashed one forfeits after one quiet TTL.
func (m *Manager[R, P]) renewLoop() {
	defer close(m.done)
	tick := time.NewTicker(m.ttl / 3)
	defer tick.Stop()
	for {
		select {
		case <-m.closing:
			return
		case <-tick.C:
			m.renewAll()
		}
	}
}

// renewAll renews once for each task holding a local lease. A store failure
// skips the tick rather than acting on it: expiry takes a full TTL of
// consecutive misses, so a blip heals on the next tick and only a store that
// stays unreachable forfeits the leases — which is then the truth of it.
func (m *Manager[R, P]) renewAll() {
	m.mu.Lock()
	tasks := make(map[string]struct{})
	for _, holders := range m.holders {
		for taskID := range holders {
			tasks[taskID] = struct{}{}
		}
	}
	m.mu.Unlock()

	for taskID := range tasks {
		_ = m.repo.RenewHolds(context.Background(), taskID, m.ttl)
	}
}

// Close stops the lease heartbeat and waits for it; idempotent. It releases
// nothing: leases this process holds simply stop renewing and drain at their
// TTL, the same way a crash would surrender them.
func (m *Manager[R, P]) Close() error {
	m.closeOnce.Do(func() {
		close(m.closing)
		<-m.done
	})
	return nil
}

// A lockRepair names one duplicate lock load found: the newer of a task's
// several bound resources, to be released.
type lockRepair struct {
	resourceID, taskID string
}

// load rebuilds the manager's caches — groups, strategies, pool, order,
// bindings — from the store, persisting the global group if absent. A group
// whose strategy is unchanged keeps its strategy instance, so a reload does
// not reset rotation state. A task locked to several resources keeps its
// oldest lock (earliest UpdatedAt, ties to the smaller id) in the bindings
// cache; the others are returned for the caller to release. Callers hold
// m.mu or are the constructor.
func (m *Manager[R, P]) load(ctx context.Context) ([]lockRepair, error) {
	listedGroups, err := m.repo.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("load groups: %w", err)
	}
	kept := m.strategies
	m.groups = make(map[string]Group, len(listedGroups))
	m.strategies = make(map[string]Selection[R], len(listedGroups))
	for _, g := range listedGroups {
		if _, dup := m.groups[g.ID]; dup {
			return nil, fmt.Errorf("duplicate group id %s", g.ID)
		}
		if err := m.adoptGroup(g); err != nil {
			return nil, err
		}
		if prior, ok := kept[g.ID]; ok {
			m.strategies[g.ID] = prior
		}
	}
	if _, ok := m.groups[GlobalGroup]; !ok {
		now := time.Now().UTC()
		g := Group{ID: GlobalGroup, Strategy: StrategyRoundRobin, CreatedAt: now, UpdatedAt: now}
		if err := m.repo.SaveGroup(ctx, g); err != nil {
			return nil, fmt.Errorf("persist global group: %w", err)
		}
		if err := m.adoptGroup(g); err != nil {
			return nil, err
		}
	}

	listed, err := m.repo.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("load pool: %w", err)
	}
	m.pool = make(map[string]R, len(listed))
	m.order = m.order[:0]
	m.bindings = make(map[string]string)
	var repairs []lockRepair
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
		if c.OwnerID == "" {
			continue
		}
		prior, bound := m.bindings[c.OwnerID]
		if !bound {
			m.bindings[c.OwnerID] = c.ID
			continue
		}
		held := m.pool[prior]
		if older(m.core(&held), c) {
			repairs = append(repairs, lockRepair{resourceID: c.ID, taskID: c.OwnerID})
		} else {
			repairs = append(repairs, lockRepair{resourceID: prior, taskID: c.OwnerID})
			m.bindings[c.OwnerID] = c.ID
		}
	}
	return repairs, nil
}

// older reports whether a's lock predates b's: earlier UpdatedAt, ties to
// the smaller resource id so both nodes repair the same way.
func older(a, b *Resource) bool {
	if !a.UpdatedAt.Equal(b.UpdatedAt) {
		return a.UpdatedAt.Before(b.UpdatedAt)
	}
	return a.ID < b.ID
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
	held, err := m.storeHolders(ctx)
	if err != nil {
		return nil, fmt.Errorf("delete group %s: %w", id, err)
	}
	for _, resourceID := range members {
		if holders := held[resourceID]; len(holders) > 0 {
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
	m.notifier.Notify(m.topic)
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
	c.Version = 0 // a create; the store assigns the first version
	version, err := m.repo.Save(ctx, p)
	if err != nil {
		return fmt.Errorf("persist resource %s: %w", c.ID, err)
	}
	c.Version = version
	m.pool[c.ID] = p
	m.order = append(m.order, c.ID)
	m.notifier.Notify(m.topic)
	return nil
}

// Update applies fn to the pooled record and persists the result. It is how a
// model's own fields change after Add — an outcome count, a rotated
// credential. The manager never reads those fields, but it holds the copy
// every lease is cut from, so the edit has to go through it to reach the
// strategies selecting over the pool; a record saved around the manager is
// caught by the version condition instead of silently overwritten, and the
// manager's own stale copy loses the same way, surfacing as ErrStale here.
// The leasing fields are not fn's to change — whatever it does to them is
// undone before the save — and it runs under the manager's lock, so it must
// not block.
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
	version, err := m.repo.Save(ctx, p)
	if err != nil {
		return fmt.Errorf("persist resource %s: %w", id, err)
	}
	c.Version = version
	m.pool[id] = p
	return nil
}

// Increment atomically adds delta to the store counter under (scope, name)
// and returns the new value — the write path for state that is a tally, not
// a record: outcome stats, rotation cursors. Counters live beside the records
// and survive them being re-saved; a delta of zero reads the current value.
// Unlike Update, nothing here touches the cached records — a model whose
// listed records project a counter pairs this with Amend so its local view
// keeps pace between refreshes.
func (m *Manager[R, P]) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return m.repo.Increment(ctx, scope, name, delta)
}

// Amend edits the cached copy of one resource without writing the store — for
// fields whose durable truth is a counter the store projects into listed
// records, where a Save would clobber nothing but a plain cache edit keeps
// the local pool as current as the increment that just landed. The edit is
// process-local and provisional: the next refresh replaces it with the
// store's projection. fn runs under the manager lock and must not block; the
// leasing fields are not fn's to change and are restored before the write.
// Amending an id not in the pool is a no-op.
func (m *Manager[R, P]) Amend(id string, fn func(*R)) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.pool[id]
	if !ok {
		return
	}
	kept := *m.core(&p)
	fn(&p)
	*m.core(&p) = kept
	m.pool[id] = p
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

// acquire is the shared blocking loop behind Acquire and Lock. Each pass
// asks the store for the actual grant, so several managers over one file
// agree; a pass that finds no candidate refreshes the caches from the store
// — a resource added or unlocked on another node becomes visible exactly
// when a waiter runs dry — and only then parks on the notifier. The park is
// bounded by a fraction of the lease TTL, because expiry frees capacity in
// the store whether or not anyone sends a wakeup.
func (m *Manager[R, P]) acquire(ctx context.Context, a Assignment, lock bool) (*Lease[R, P], error) {
	if a.TaskID == "" {
		return nil, errors.New("assignment task id is required")
	}
	if a.GroupID == "" {
		a.GroupID = GlobalGroup
	}

	refreshed := false
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		lease, err := m.tryAcquire(ctx, a, lock)
		if lease != nil {
			return lease, nil
		}
		// A configuration miss — unknown group, empty group, broken pin — is
		// reported only after a refresh has had a chance to see it created or
		// repaired on another node; anything else fails as-is.
		if err != nil && (refreshed || !configErr(err)) {
			return nil, err
		}
		if !refreshed {
			if err := m.refresh(ctx); err != nil {
				return nil, err
			}
			refreshed = true
			continue
		}
		if err := m.notifier.Wait(ctx, m.topic, m.ttl/3); err != nil {
			return nil, err
		}
		refreshed = false
	}
}

// configErr reports whether err describes the assignment rather than the
// moment: the kind of miss a stale cache can manufacture and a refresh can
// clear.
func configErr(err error) bool {
	return errors.Is(err, ErrGroupNotFound) || errors.Is(err, ErrNoResources) ||
		errors.Is(err, ErrResourceNotFound) || errors.Is(err, ErrResourceNotInGroup) ||
		errors.Is(err, ErrResourceLocked) || errors.Is(err, ErrPinConflict)
}

// refresh reloads every cache from the store, releasing any duplicate locks
// the reload surfaces; convergence is left to the next refresh.
func (m *Manager[R, P]) refresh(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	repairs, err := m.load(ctx)
	if err != nil {
		return fmt.Errorf("refresh pool: %w", err)
	}
	for _, r := range repairs {
		if err := m.repo.ReleaseLock(ctx, r.resourceID, r.taskID); err != nil {
			return fmt.Errorf("release duplicate lock on %s: %w", r.resourceID, err)
		}
	}
	return nil
}

// tryAcquire makes one pass under the lock: candidates come from the caches,
// the grant comes from the store. A nil, nil return is a miss — nothing
// acquirable right now — which the blocking loop turns into a refresh or a
// wait.
func (m *Manager[R, P]) tryAcquire(ctx context.Context, a Assignment, lock bool) (*Lease[R, P], error) {
	m.mu.Lock()
	defer m.mu.Unlock()

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
		// A locked resource is exclusive to its owner — cap 1 at the store —
		// and the store also rejects a lock the cache has not caught up to.
		switch h, err := m.repo.Acquire(ctx, id, a.TaskID, 1, m.ttl); {
		case err == nil:
			_ = h
			m.hold(id, a.TaskID)
			return m.lease(m.pool[id], a.TaskID), nil
		case errors.Is(err, ErrCapacity), errors.Is(err, ErrLockHeld):
			return nil, nil
		default:
			return nil, fmt.Errorf("acquire %s: %w", id, err)
		}
	}

	g, ok := m.groups[a.GroupID]
	if !ok {
		return nil, fmt.Errorf("acquire from group %s: %w", a.GroupID, ErrGroupNotFound)
	}
	if !m.groupHasResources(g.ID) {
		return nil, fmt.Errorf("group %s: %w", g.ID, ErrNoResources)
	}

	// The cache-filtered candidates are an optimistic guess; each pick is
	// proven against the store, and a refusal there — someone else's lock or
	// capacity the cache had not seen — just drops the candidate and rotates
	// to the next.
	remaining, err := m.eligible(a, lock)
	if err != nil {
		return nil, err
	}
	for len(remaining) > 0 {
		p, err := m.pick(ctx, a, remaining)
		if err != nil {
			return nil, err
		}
		c := m.core(&p)
		if lock {
			switch err := m.repo.ClaimLock(ctx, c.ID, a.TaskID); {
			case err == nil:
			case errors.Is(err, ErrLockHeld):
				remaining = dropCandidate[R, P](remaining, c.ID)
				continue
			default:
				return nil, fmt.Errorf("persist lock: %w", err)
			}
		}
		cap := effectiveCap(c)
		if cap == UnlimitedHolders {
			cap = 0
		}
		if lock {
			cap = 1
		}
		switch _, err := m.repo.Acquire(ctx, c.ID, a.TaskID, cap, m.ttl); {
		case err == nil:
			if lock {
				// ClaimLock bumped the stored version by one; the cache
				// tracks it so the next conditional Save still matches.
				c.OwnerID = a.TaskID
				c.Version++
				c.UpdatedAt = time.Now().UTC()
				m.pool[c.ID] = p
				m.bindings[a.TaskID] = c.ID
			}
			m.hold(c.ID, a.TaskID)
			return m.lease(p, a.TaskID), nil
		case errors.Is(err, ErrCapacity), errors.Is(err, ErrLockHeld):
			if lock {
				// The lock landed but the lease did not — someone still holds
				// a lease the idle filter missed. Undo the lock: a binding
				// without its lease would park every later pass behind it.
				if err := m.repo.ReleaseLock(ctx, c.ID, a.TaskID); err != nil {
					return nil, fmt.Errorf("undo lock on %s: %w", c.ID, err)
				}
			}
			remaining = dropCandidate[R, P](remaining, c.ID)
		default:
			return nil, fmt.Errorf("acquire %s: %w", c.ID, err)
		}
	}
	return nil, nil
}

// dropCandidate removes the resource from the candidate slice, preserving
// order for the strategies that depend on it.
func dropCandidate[R any, P Leasable[R]](candidates []R, id string) []R {
	for i := range candidates {
		if P(&candidates[i]).core().ID == id {
			return append(candidates[:i], candidates[i+1:]...)
		}
	}
	return candidates
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

// Held reports the assignment of every live lease this process holds — in
// acquire or lock mode — whose record and group satisfy pred. The predicate
// brings the meaning (which field of the model, which ref), the manager
// brings who holds what. It reads the local cache, not the store: it is the
// view of this manager's own leases, which is what its in-process callers
// guard with; cross-node liveness is the deletion guards' concern. Results
// are sorted by task then resource so refusals built on them read stably.
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

// storeHolders reads every unexpired hold from the store and groups the
// holding tasks by resource, sorted so refusals read stably. Deletion guards
// consult it, not the local holders cache, because the lease blocking a
// delete may be held through another node's manager. Expiry is filtered on
// this process's clock — close enough for a guard, whose answer a moment of
// skew can only make more conservative. Callers hold m.mu.
func (m *Manager[R, P]) storeHolders(ctx context.Context) (map[string][]string, error) {
	holds, err := m.repo.ListHolds(ctx)
	if err != nil {
		return nil, fmt.Errorf("list holds: %w", err)
	}
	now := time.Now()
	held := make(map[string][]string)
	for _, h := range holds {
		if h.ExpiresAt.After(now) {
			held[h.ResourceID] = append(held[h.ResourceID], h.TaskID)
		}
	}
	for _, ids := range held {
		sort.Strings(ids)
	}
	return held, nil
}

// eligible collects the assignment's acquirable candidates from the caches:
// group members, unlocked as far as this process knows, under their cap as
// far as this process's own leases say. requireIdle narrows the field to
// resources nobody here is holding, for the callers about to bind one: a
// lock must not land on a resource another task is already leasing, or the
// owner would queue behind a stranger on its own. The store re-checks every
// pick, so a stale answer here costs a round trip, never correctness.
// Callers hold m.mu.
func (m *Manager[R, P]) eligible(a Assignment, requireIdle bool) ([]R, error) {
	candidates := make([]R, 0, len(m.order))
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
		}
	}
	return candidates, nil
}

// pick chooses one candidate: the pin alone when a names one — running it
// through the group's strategy would advance shared rotation state for a
// choice nobody made — otherwise the group's strategy instance, validated
// against the candidates so a strategy returning some other pooled resource
// cannot hand out one already at capacity. Callers hold m.mu.
func (m *Manager[R, P]) pick(ctx context.Context, a Assignment, candidates []R) (R, error) {
	if a.ResourceID != "" {
		return candidates[0], nil
	}
	// Round robin rotates through a store-side cursor, so every node of a
	// deployment advances the same rotation. The modulo is over the local
	// candidate order, stable across nodes because pools load in the store's
	// id order; an overridden round robin keeps its own state instead.
	if g := m.groups[a.GroupID]; strategyName(g) == StrategyRoundRobin && !m.rrOverridden {
		idx, err := m.repo.Increment(ctx, g.ID, "cursor", 1)
		if err != nil {
			return zero[R](), fmt.Errorf("advance rotation cursor of group %s: %w", g.ID, err)
		}
		return candidates[int((idx-1)%int64(len(candidates)))], nil
	}
	picked, err := m.strategies[a.GroupID].Select(candidates)
	if err != nil {
		return zero[R](), fmt.Errorf("selection: %w", err)
	}
	pickedID := m.core(&picked).ID
	for _, p := range candidates {
		live := p
		if m.core(&live).ID == pickedID {
			return live, nil
		}
	}
	return zero[R](), fmt.Errorf("selection returned resource %s, which was not a candidate", pickedID)
}

// zero is the empty record returned alongside an error or a miss.
func zero[R any]() R {
	var empty R
	return empty
}

// Unlock removes taskID's durable lock, returning its resource to the rotating
// pool. It is a no-op if taskID has no locked resource. A miss in the local
// bindings is checked against the store before it counts: the lock may have
// been taken through another node's manager, and freeing a deleted task's
// resources is exactly the moment that matters.
func (m *Manager[R, P]) Unlock(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	id, bound := m.bindings[taskID]
	if !bound {
		var err error
		if id, bound, err = m.storedBinding(ctx, taskID); err != nil {
			return err
		}
		if !bound {
			return nil
		}
	}
	return m.unlock(ctx, taskID, id)
}

// storedBinding scans the store for a resource locked to the task — the
// cross-node fallback for a bindings-cache miss. Callers hold m.mu.
func (m *Manager[R, P]) storedBinding(ctx context.Context, taskID string) (string, bool, error) {
	rec, bound, err := m.storedBindingRecord(ctx, taskID)
	if err != nil || !bound {
		return "", bound, err
	}
	return m.core(&rec).ID, true, nil
}

// storedBindingRecord is storedBinding returning the whole record, for the
// callers that need to evaluate the lock, not just name it.
func (m *Manager[R, P]) storedBindingRecord(ctx context.Context, taskID string) (R, bool, error) {
	listed, err := m.repo.List(ctx)
	if err != nil {
		return zero[R](), false, fmt.Errorf("find lock of task %s: %w", taskID, err)
	}
	for _, p := range listed {
		if m.core(&p).OwnerID == taskID {
			return p, true, nil
		}
	}
	return zero[R](), false, nil
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
		// The lock may have been taken through another node's manager;
		// evaluate the stored record the same way the cached one would be.
		locked, stored, err := m.storedBindingRecord(ctx, a.TaskID)
		if err != nil {
			return err
		}
		if !stored || fits(a, m.core(&locked)) {
			return nil
		}
		return m.unlock(ctx, a.TaskID, m.core(&locked).ID)
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
	if err := m.repo.ReleaseLock(ctx, resourceID, taskID); err != nil {
		return fmt.Errorf("persist unlock: %w", err)
	}
	if p, live := m.pool[resourceID]; live {
		// ReleaseLock bumped the stored version by one; the cache tracks it
		// so the next conditional Save still matches.
		c := m.core(&p)
		c.OwnerID = ""
		c.Version++
		c.UpdatedAt = time.Now().UTC()
		m.pool[resourceID] = p
	}
	delete(m.bindings, taskID)
	m.notifier.Notify(m.topic)
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

	held, err := m.storeHolders(ctx)
	if err != nil {
		return nil, fmt.Errorf("delete resource %s: %w", id, err)
	}
	if holders := held[id]; len(holders) > 0 {
		return nil, fmt.Errorf("%w: %s is held by %s", ErrResourceInUse, id, plural("task", holders))
	}
	p, ok := m.pool[id]
	if !ok {
		// Nothing cached to guard, but the store may still carry a row this
		// manager never loaded; its holds were checked above like any other's.
		return nil, m.repo.Delete(ctx, id)
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
	m.notifier.Notify(m.topic)
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

// release frees the holder slot durably, then locally, then wakes waiters.
// The store write is best-effort on a background context: Release carries no
// ctx and no error path by design, and a hold the release could not clear
// drains at its TTL — expiry is the backstop that makes swallowing the
// failure safe.
func (m *Manager[R, P]) release(id, taskID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_ = m.repo.ReleaseHold(context.Background(), id, taskID)
	m.unhold(id, taskID)
	m.notifier.Notify(m.topic)
}
