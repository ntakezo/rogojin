package proxies

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// A Manager allocates proxies to tasks: locked proxies go only to their owner,
// unlocked ones rotate within their group through the group's selection
// strategy under the effective holder cap (the proxy's own MaxHolders, else
// its group's, else 1). It owns all live lease state; the Repository only
// stores bytes. A Manager is safe for concurrent use.
type Manager struct {
	repo      Repository
	policy    DeletionPolicy
	usage     UsagePolicy
	factories map[string]StrategyFactory

	mu         sync.Mutex
	cond       *sync.Cond
	groups     map[string]Group
	strategies map[string]Selection // one instance per group, keyed by group ID
	pool       map[string]Proxy
	order      []string          // stable candidate order for selection
	holders    map[string]int    // live lease count per proxy
	bindings   map[string]string // taskID -> locked proxy ID
}

// A ManagerOption configures a Manager at construction.
type ManagerOption func(*Manager)

// WithStrategy registers factory under name, so groups may reference custom
// selection algorithms beyond the built-ins (or override a built-in).
func WithStrategy(name string, factory StrategyFactory) ManagerOption {
	return func(m *Manager) { m.factories[name] = factory }
}

// WithUsagePolicy wires the guard DeleteGroup consults to refuse deleting a
// group a running task leases from. Without one, DeleteGroup cannot tell a
// live group from an idle one and deletes either.
func WithUsagePolicy(usage UsagePolicy) ManagerOption {
	return func(m *Manager) { m.usage = usage }
}

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent. Groups and pool change afterwards only through
// CreateGroup, DeleteGroup, AddProxy, and DeleteProxy.
func NewManager(ctx context.Context, repo Repository, policy DeletionPolicy, opts ...ManagerOption) (*Manager, error) {
	if repo == nil {
		return nil, errors.New("repository is required")
	}

	m := &Manager{
		repo:   repo,
		policy: policy,
		factories: map[string]StrategyFactory{
			StrategyRoundRobin: func() Selection { return NewRoundRobin() },
			StrategyBayesian:   func() Selection { return NewBayesian() },
		},
		groups:     make(map[string]Group),
		strategies: make(map[string]Selection),
		pool:       make(map[string]Proxy),
		holders:    make(map[string]int),
		bindings:   make(map[string]string),
	}
	m.cond = sync.NewCond(&m.mu)
	for _, opt := range opts {
		opt(m)
	}

	listedGroups, err := repo.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("load proxy groups: %w", err)
	}
	for _, g := range listedGroups {
		if _, dup := m.groups[g.ID]; dup {
			return nil, fmt.Errorf("duplicate proxy group id %s", g.ID)
		}
		if err := m.adoptGroup(g); err != nil {
			return nil, err
		}
	}
	if _, ok := m.groups[GlobalGroup]; !ok {
		now := time.Now().UTC()
		g := Group{ID: GlobalGroup, Strategy: StrategyRoundRobin, MaxHolders: 1, CreatedAt: now, UpdatedAt: now}
		if err := repo.SaveGroup(ctx, g); err != nil {
			return nil, fmt.Errorf("persist global proxy group: %w", err)
		}
		if err := m.adoptGroup(g); err != nil {
			return nil, err
		}
	}

	listed, err := repo.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("load proxy pool: %w", err)
	}
	for _, p := range listed {
		if _, dup := m.pool[p.ID]; dup {
			return nil, fmt.Errorf("duplicate proxy id %s", p.ID)
		}
		if p.GroupID == "" {
			p.GroupID = GlobalGroup
		}
		if _, ok := m.groups[p.GroupID]; !ok {
			return nil, fmt.Errorf("proxy %s references group %s: %w", p.ID, p.GroupID, ErrGroupNotFound)
		}
		if err := validateHolderPolicy(p.MaxHolders); err != nil {
			return nil, fmt.Errorf("proxy %s: %w", p.ID, err)
		}
		m.pool[p.ID] = p
		m.order = append(m.order, p.ID)
		if p.OwnerID != "" {
			if prior, bound := m.bindings[p.OwnerID]; bound {
				return nil, fmt.Errorf("task %s locked to multiple proxies (%s, %s)", p.OwnerID, prior, p.ID)
			}
			m.bindings[p.OwnerID] = p.ID
		}
	}
	return m, nil
}

// validateGroup checks g and resolves its strategy factory.
func (m *Manager) validateGroup(g Group) (StrategyFactory, error) {
	if g.ID == "" {
		return nil, errors.New("proxy group id is required")
	}
	if err := validateHolderPolicy(g.MaxHolders); err != nil {
		return nil, fmt.Errorf("proxy group %s: %w", g.ID, err)
	}
	name := g.Strategy
	if name == "" {
		name = StrategyRoundRobin
	}
	factory, ok := m.factories[name]
	if !ok {
		return nil, fmt.Errorf("proxy group %s references unknown strategy %q", g.ID, name)
	}
	return factory, nil
}

// adoptGroup validates g and installs it with a fresh strategy instance.
func (m *Manager) adoptGroup(g Group) error {
	factory, err := m.validateGroup(g)
	if err != nil {
		return err
	}
	m.groups[g.ID] = g
	m.strategies[g.ID] = factory()
	return nil
}

// validateHolderPolicy accepts 0 (inherit/default), UnlimitedHolders, or an
// explicit cap of at least 1.
func validateHolderPolicy(n int) error {
	if n < UnlimitedHolders {
		return fmt.Errorf("invalid holder policy %d", n)
	}
	return nil
}

// CreateGroup persists and installs a new group. ID must be unset in the
// manager; Strategy must be registered (empty selects round robin).
func (m *Manager) CreateGroup(ctx context.Context, g Group) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, dup := m.groups[g.ID]; dup {
		return fmt.Errorf("proxy group %s already exists", g.ID)
	}
	factory, err := m.validateGroup(g)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	g.CreatedAt, g.UpdatedAt = now, now
	if err := m.repo.SaveGroup(ctx, g); err != nil {
		return fmt.Errorf("persist proxy group %s: %w", g.ID, err)
	}
	m.groups[g.ID] = g
	m.strategies[g.ID] = factory()
	return nil
}

// DeleteGroup cascade-deletes a group and every proxy in it. It refuses with
// ErrGroupInUse while the usage policy reports a running task leasing from the
// group — suspend or kill those tasks first; the check cannot catch a task
// that starts after it, since nothing here can hold a task still. Locked
// members consult the
// deletion policy; because the whole group is going away there is nothing
// in-group to reassign to, so a Reassign decision degrades to Unbind and is
// reported in the returned (joined) error alongside any ErrTaskOrphaned from
// Fail decisions. The global group cannot be deleted. With locked members and
// no policy wired it refuses before mutating anything. Tasks still assigned to
// the group are not rewritten — this package owns no task store — so their
// next lease fails with ErrGroupNotFound.
func (m *Manager) DeleteGroup(ctx context.Context, id string) error {
	if id == GlobalGroup {
		return fmt.Errorf("proxy group %s cannot be deleted", GlobalGroup)
	}
	// Asked before m.mu is taken: the task service calls back into Unlock while
	// holding its own registry lock, and holding both would invert lock order.
	if m.usage != nil {
		running, err := m.usage.RunningTasks(ctx, id)
		if err != nil {
			return fmt.Errorf("check proxy group %s usage: %w", id, err)
		}
		if len(running) > 0 {
			return fmt.Errorf("%w: %s leased by %s", ErrGroupInUse, id, strings.Join(running, ", "))
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.groups[id]; !ok {
		return fmt.Errorf("delete proxy group %s: %w", id, ErrGroupNotFound)
	}

	members := make([]Proxy, 0)
	for _, pid := range m.order {
		if p := m.pool[pid]; p.GroupID == id {
			members = append(members, p)
			if p.OwnerID != "" && m.policy == nil {
				return fmt.Errorf("proxy %s is locked to task %s and no deletion policy is set", p.ID, p.OwnerID)
			}
		}
	}

	// orphaned collects the advisory notices: which tasks lost a binding and
	// how. A store failure aborts instead, before the group row is deleted —
	// a group row that outlives its member rows makes the next NewManager fail
	// on a proxy pointing at a group that no longer exists.
	var orphaned []error
	for _, p := range members {
		var notice error
		if p.OwnerID != "" {
			switch decision := m.policy.OnProxyDeleted(ctx, p.OwnerID, p); decision {
			case Reassign:
				notice = fmt.Errorf("task %s: cannot reassign within deleted group %s; unbound instead", p.OwnerID, id)
			case Unbind:
			case Fail:
				notice = fmt.Errorf("%w: %s", ErrTaskOrphaned, p.OwnerID)
			default:
				notice = fmt.Errorf("unknown deletion decision %d for proxy %s; unbound instead", decision, p.ID)
			}
		}
		// The row goes before the binding: an aborted remove must leave the
		// live binding matching the OwnerID still in the store.
		if err := m.remove(ctx, p); err != nil {
			return errors.Join(append(orphaned, err)...)
		}
		if p.OwnerID != "" {
			delete(m.bindings, p.OwnerID)
		}
		if notice != nil {
			orphaned = append(orphaned, notice)
		}
	}

	if err := m.repo.DeleteGroup(ctx, id); err != nil {
		return errors.Join(append(orphaned, fmt.Errorf("delete proxy group %s: %w", id, err))...)
	}
	delete(m.groups, id)
	delete(m.strategies, id)
	m.cond.Broadcast()
	return errors.Join(orphaned...)
}

// AddProxy persists and installs a new unlocked proxy, defaulting an empty
// GroupID to the global group. The group must exist.
func (m *Manager) AddProxy(ctx context.Context, p Proxy) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if p.ID == "" {
		return errors.New("proxy id is required")
	}
	if _, dup := m.pool[p.ID]; dup {
		return fmt.Errorf("proxy %s already exists", p.ID)
	}
	if p.OwnerID != "" {
		return fmt.Errorf("proxy %s: add unlocked proxies; Lock creates bindings", p.ID)
	}
	if err := validateHolderPolicy(p.MaxHolders); err != nil {
		return fmt.Errorf("proxy %s: %w", p.ID, err)
	}
	if p.GroupID == "" {
		p.GroupID = GlobalGroup
	}
	if _, ok := m.groups[p.GroupID]; !ok {
		return fmt.Errorf("add proxy %s to group %s: %w", p.ID, p.GroupID, ErrGroupNotFound)
	}

	now := time.Now().UTC()
	p.CreatedAt, p.UpdatedAt = now, now
	if err := m.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("persist proxy %s: %w", p.ID, err)
	}
	m.pool[p.ID] = p
	m.order = append(m.order, p.ID)
	m.cond.Broadcast()
	return nil
}

// Acquire leases a proxy for taskID from groupID (the global group when
// empty): its locked proxy if it has one — a durable binding outranks the
// requested group — otherwise one rotated from the group's unlocked members.
// It blocks until a proxy frees or ctx is done; a group with no proxies fails
// immediately with ErrNoProxies.
func (m *Manager) Acquire(ctx context.Context, taskID, groupID string) (*Lease, error) {
	return m.acquire(ctx, taskID, groupID, false)
}

// Lock durably binds taskID to a proxy from groupID (selecting one if
// unbound, idempotent) and leases it. The binding outlives the lease and the
// manager until Unlock or the proxy's deletion; no other task can ever
// acquire the proxy.
func (m *Manager) Lock(ctx context.Context, taskID, groupID string) (*Lease, error) {
	return m.acquire(ctx, taskID, groupID, true)
}

// acquire is the shared blocking loop behind Acquire and Lock. A bound task
// only ever leases its own proxy, one lease at a time; an unbound task rotates
// the group's unlocked members, durably binding the pick first when lock is set.
func (m *Manager) acquire(ctx context.Context, taskID, groupID string, lock bool) (*Lease, error) {
	if groupID == "" {
		groupID = GlobalGroup
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

		id, bound := m.bindings[taskID]
		if _, live := m.pool[id]; bound && !live {
			// the bound proxy is gone; rotate rather than lease a phantom.
			delete(m.bindings, taskID)
			bound = false
		}
		if bound {
			// a locked proxy is exclusive to its owner: one lease at a time.
			if m.holders[id] == 0 {
				m.holders[id]++
				return &Lease{manager: m, proxy: m.pool[id]}, nil
			}
		} else {
			g, ok := m.groups[groupID]
			if !ok {
				return nil, fmt.Errorf("acquire from group %s: %w", groupID, ErrGroupNotFound)
			}
			if !m.groupHasProxies(g.ID) {
				return nil, fmt.Errorf("group %s: %w", g.ID, ErrNoProxies)
			}
			// A lock takes only an idle proxy; if none is free right now the loop
			// waits, since a release or unlock can still produce one.
			p, found, err := m.selectUnlocked(g.ID, lock)
			if err != nil {
				return nil, err
			}
			if found {
				if lock {
					p.OwnerID = taskID
					p.UpdatedAt = time.Now().UTC()
					if err := m.repo.Save(ctx, p); err != nil {
						return nil, fmt.Errorf("persist lock: %w", err)
					}
					m.pool[p.ID] = p
					m.bindings[taskID] = p.ID
				}
				m.holders[p.ID]++
				return &Lease{manager: m, proxy: p}, nil
			}
		}

		m.cond.Wait()
	}
}

// groupHasProxies reports whether any proxy belongs to the group, leased or
// not. Callers hold m.mu.
func (m *Manager) groupHasProxies(groupID string) bool {
	for _, id := range m.order {
		if m.pool[id].GroupID == groupID {
			return true
		}
	}
	return false
}

// effectiveCap resolves the proxy's holder cap: its own policy, else its
// group's, else 1. Callers hold m.mu.
func (m *Manager) effectiveCap(p Proxy) int {
	if p.MaxHolders != 0 {
		return p.MaxHolders
	}
	if g := m.groups[p.GroupID]; g.MaxHolders != 0 {
		return g.MaxHolders
	}
	return 1
}

// selectUnlocked picks an unlocked, under-capacity proxy from the group via
// the group's strategy instance; found is false when there are no candidates.
// requireIdle narrows the field to proxies nobody is holding, for the callers
// that are about to bind one: a lock must not land on a proxy another task is
// already leasing, or the owner would queue behind a stranger on its own proxy.
// Callers hold m.mu.
func (m *Manager) selectUnlocked(groupID string, requireIdle bool) (Proxy, bool, error) {
	candidates := make([]Proxy, 0, len(m.order))
	eligible := make(map[string]Proxy, len(m.order))
	for _, id := range m.order {
		p := m.pool[id]
		if p.GroupID != groupID || p.OwnerID != "" {
			continue
		}
		held := m.holders[id]
		if requireIdle && held > 0 {
			continue
		}
		if cap := m.effectiveCap(p); cap == UnlimitedHolders || held < cap {
			candidates = append(candidates, p)
			eligible[p.ID] = p
		}
	}
	if len(candidates) == 0 {
		return Proxy{}, false, nil
	}

	p, err := m.strategies[groupID].Select(candidates)
	if err != nil {
		return Proxy{}, false, fmt.Errorf("selection: %w", err)
	}
	// Validated against the candidates, not the pool: a strategy returning some
	// other pooled proxy would hand out one already at capacity, or let a lock
	// overwrite another task's binding.
	live, ok := eligible[p.ID]
	if !ok {
		return Proxy{}, false, fmt.Errorf("selection returned proxy %s, which was not a candidate", p.ID)
	}
	return live, true, nil
}

// Unlock removes taskID's durable lock, returning its proxy to the rotating
// pool. It is a no-op if taskID has no locked proxy.
func (m *Manager) Unlock(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	id, bound := m.bindings[taskID]
	if !bound {
		return nil
	}

	p := m.pool[id]
	p.OwnerID = ""
	p.UpdatedAt = time.Now().UTC()
	if err := m.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("persist unlock: %w", err)
	}
	m.pool[id] = p
	delete(m.bindings, taskID)
	m.cond.Broadcast()
	return nil
}

// DeleteProxy removes a proxy from the pool and the repository. Deleting a
// locked proxy runs the deletion policy and executes its decision — Reassign
// selects the replacement from the deleted proxy's group; a Fail decision
// returns ErrTaskOrphaned naming the task so the deleter can act.
func (m *Manager) DeleteProxy(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.pool[id]
	if !ok {
		return m.repo.Delete(ctx, id)
	}
	if p.OwnerID == "" {
		return m.remove(ctx, p)
	}

	if m.policy == nil {
		return fmt.Errorf("proxy %s is locked to task %s and no deletion policy is set", id, p.OwnerID)
	}
	// Every branch removes the proxy before touching the binding: an aborted
	// remove must not leave a task unbound in memory while the store still
	// carries the lock, which a restart would resurrect.
	owner := p.OwnerID
	switch decision := m.policy.OnProxyDeleted(ctx, owner, p); decision {
	case Reassign:
		next, found, err := m.selectUnlocked(p.GroupID, true)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("reassign task %s: %w", owner, ErrNoProxies)
		}
		if err := m.remove(ctx, p); err != nil {
			return err
		}
		delete(m.bindings, owner)
		next.OwnerID = owner
		next.UpdatedAt = time.Now().UTC()
		if err := m.repo.Save(ctx, next); err != nil {
			return fmt.Errorf("persist reassign: %w", err)
		}
		m.pool[next.ID] = next
		m.bindings[owner] = next.ID
		return nil
	case Unbind:
		if err := m.remove(ctx, p); err != nil {
			return err
		}
		delete(m.bindings, owner)
		return nil
	case Fail:
		if err := m.remove(ctx, p); err != nil {
			return err
		}
		delete(m.bindings, owner)
		return fmt.Errorf("%w: %s", ErrTaskOrphaned, owner)
	default:
		return fmt.Errorf("unknown deletion decision %d", decision)
	}
}

// remove deletes p from the repository and then the live pool, waking waiters.
// The store goes first, as everywhere else here: dropping it from memory on a
// failed delete would leave a proxy the manager cannot see but reloads on the
// next start — still carrying the lock this delete was meant to clear.
// Callers hold m.mu.
func (m *Manager) remove(ctx context.Context, p Proxy) error {
	if err := m.repo.Delete(ctx, p.ID); err != nil {
		return fmt.Errorf("delete proxy %s: %w", p.ID, err)
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

// Lease is a live hold on one proxy. Release it exactly once when done.
type Lease struct {
	manager *Manager
	proxy   Proxy
	once    sync.Once
}

// Proxy returns the leased proxy as of acquisition.
func (l *Lease) Proxy() Proxy {
	return l.proxy
}

// Release frees the proxy, records the outcome the bayesian strategy learns
// from, and persists it. Only the first call acts; later calls return nil.
func (l *Lease) Release(success bool) error {
	var err error
	l.once.Do(func() { err = l.manager.release(l.proxy.ID, success) })
	return err
}

// release updates stats, frees the holder slot, wakes waiters, and persists the
// updated record before dropping the lock — a save made outside it could land
// after a concurrent Unlock's and resurrect the stale owner durably. The save
// uses a background context so it lands even when the caller's context is gone.
func (m *Manager) release(id string, success bool) error {
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
	if m.holders[id] > 0 {
		m.holders[id]--
	}
	m.cond.Broadcast()

	if !ok {
		return nil // proxy was deleted while leased; nothing to persist
	}
	return m.repo.Save(context.Background(), p)
}
