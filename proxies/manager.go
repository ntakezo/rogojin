package proxies

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// deleteAttempts bounds how many times a delete re-asks the usage policy after
// the pool moved underneath it. The guard must be asked without m.mu, so its
// answer is verified against a fresh reading before the delete commits.
const deleteAttempts = 3

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
	order      []string                  // stable candidate order for selection
	holders    map[string]map[string]int // proxy ID -> holding task ID -> live lease count
	bindings   map[string]string         // taskID -> locked proxy ID
}

// A ManagerOption configures a Manager at construction.
type ManagerOption func(*Manager)

// WithStrategy registers factory under name, so groups may reference custom
// selection algorithms beyond the built-ins (or override a built-in).
func WithStrategy(name string, factory StrategyFactory) ManagerOption {
	return func(m *Manager) { m.factories[name] = factory }
}

// WithUsagePolicy wires the guard DeleteProxy and DeleteGroup consult to
// refuse deleting a proxy a running task is leasing, locked to, or rotating
// through. Without one, neither can tell a running task from a parked one, so
// both fall back to refusing any proxy with a live lease on it — safe, but
// blind to a task that is merely assigned the group and between leases, and
// unable to free a proxy by suspending its task.
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
		holders:    make(map[string]map[string]int),
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
// group, holding a lease on a member, or locked to one — suspend or kill those
// tasks first. Locked members consult the deletion policy; because the whole
// group is going away there is nothing in-group to reassign to, so a Reassign
// decision degrades to Unbind and is reported in the returned (joined) error
// alongside any ErrTaskOrphaned from Fail decisions. The global group cannot be
// deleted. With locked members and no policy wired it refuses before mutating
// anything. Tasks still assigned to the group are not rewritten — this package
// owns no task store — so their next lease fails with ErrGroupNotFound.
func (m *Manager) DeleteGroup(ctx context.Context, id string) error {
	if id == GlobalGroup {
		return fmt.Errorf("proxy group %s cannot be deleted", GlobalGroup)
	}

	for attempt := 0; attempt < deleteAttempts; attempt++ {
		m.mu.Lock()
		if _, ok := m.groups[id]; !ok {
			m.mu.Unlock()
			return fmt.Errorf("delete proxy group %s: %w", id, ErrGroupNotFound)
		}
		snap := m.snapshotUsage(m.members(id))
		m.mu.Unlock()

		if err := m.checkIdle(ctx, id, snap, ErrGroupInUse); err != nil {
			return err
		}

		m.mu.Lock()
		if _, ok := m.groups[id]; !ok {
			m.mu.Unlock()
			return fmt.Errorf("delete proxy group %s: %w", id, ErrGroupNotFound)
		}
		if !m.matchesSnapshot(snap) || len(m.members(id)) != len(snap.doomed) {
			m.mu.Unlock()
			continue // the guard answered about a group that has since moved
		}
		err := m.deleteGroup(ctx, id, snap.doomed)
		m.mu.Unlock()
		return err
	}
	return fmt.Errorf("delete proxy group %s: the pool kept changing under the usage guard", id)
}

// deleteGroup removes every member and then the group row. Callers hold m.mu
// and have already cleared the usage guard for members.
func (m *Manager) deleteGroup(ctx context.Context, id string, members []Proxy) error {
	for _, p := range members {
		if p.OwnerID != "" && m.policy == nil {
			return fmt.Errorf("proxy %s is locked to task %s and no deletion policy is set", p.ID, p.OwnerID)
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

// members collects the group's proxies in the pool's stable order. Callers
// hold m.mu.
func (m *Manager) members(groupID string) []Proxy {
	members := make([]Proxy, 0)
	for _, pid := range m.order {
		if p := m.pool[pid]; p.GroupID == groupID {
			members = append(members, p)
		}
	}
	return members
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

// Acquire leases a proxy under a: its locked proxy if it has one — a durable
// binding outranks the requested group — otherwise the pinned member, else one
// rotated from the group's unlocked members. It blocks until a proxy frees or
// ctx is done; a group with no proxies fails immediately with ErrNoProxies, and
// a pin that no longer resolves fails with ErrProxyNotFound.
func (m *Manager) Acquire(ctx context.Context, a Assignment) (*Lease, error) {
	return m.acquire(ctx, a, false)
}

// Lock durably binds a.TaskID to a proxy (the pinned one, or one selected from
// the group when unpinned; idempotent) and leases it. The binding outlives the
// lease and the manager until Unlock, a reassignment, or the proxy's deletion;
// no other task can ever acquire the proxy.
func (m *Manager) Lock(ctx context.Context, a Assignment) (*Lease, error) {
	return m.acquire(ctx, a, true)
}

// acquire is the shared blocking loop behind Acquire and Lock. A bound task
// only ever leases its own proxy, one lease at a time; an unbound task takes
// its pin, or rotates the group's unlocked members, durably binding the pick
// first when lock is set.
func (m *Manager) acquire(ctx context.Context, a Assignment, lock bool) (*Lease, error) {
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
		// waits, and a task must not park forever on a proxy that is gone.
		if err := m.checkPin(a); err != nil {
			return nil, err
		}

		id, bound := m.bindings[a.TaskID]
		if _, live := m.pool[id]; bound && !live {
			// the bound proxy is gone; rotate rather than lease a phantom.
			delete(m.bindings, a.TaskID)
			bound = false
		}
		if bound {
			// A pin is deliberate and outranks a stale lock, but a lease must not
			// make the durable write that resolves it; the deleter of the lock has
			// to be the reassignment that created the disagreement.
			if a.ProxyID != "" && a.ProxyID != id {
				return nil, fmt.Errorf("%w: task %s is locked to %s but pinned to %s; reassign the task to release the lock", ErrPinConflict, a.TaskID, id, a.ProxyID)
			}
			// a locked proxy is exclusive to its owner: one lease at a time.
			if m.heldCount(id) == 0 {
				m.hold(id, a.TaskID)
				return &Lease{manager: m, proxy: m.pool[id], taskID: a.TaskID}, nil
			}
		} else {
			g, ok := m.groups[a.GroupID]
			if !ok {
				return nil, fmt.Errorf("acquire from group %s: %w", a.GroupID, ErrGroupNotFound)
			}
			if !m.groupHasProxies(g.ID) {
				return nil, fmt.Errorf("group %s: %w", g.ID, ErrNoProxies)
			}
			// A lock takes only an idle proxy; if none is free right now the loop
			// waits, since a release or unlock can still produce one.
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
				return &Lease{manager: m, proxy: p, taskID: a.TaskID}, nil
			}
		}

		m.cond.Wait()
	}
}

// checkPin validates a pinned assignment against the live pool: the proxy must
// exist and belong to the group the assignment names. A pin that no longer
// resolves is reported rather than quietly degraded to rotation, because "run
// on this proxy or tell me" is the whole point of pinning — the fallback is the
// consumer's call to make. Unpinned assignments pass. Callers hold m.mu.
func (m *Manager) checkPin(a Assignment) error {
	if a.ProxyID == "" {
		return nil
	}
	p, ok := m.pool[a.ProxyID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrProxyNotFound, a.ProxyID)
	}
	if p.GroupID != a.GroupID {
		return fmt.Errorf("%w: %s is in group %s, not %s", ErrProxyNotInGroup, a.ProxyID, p.GroupID, a.GroupID)
	}
	// A lease frees itself; another task's durable lock does not. Waiting on one
	// is waiting on a condition nothing will satisfy, so it fails instead.
	if p.OwnerID != "" && p.OwnerID != a.TaskID {
		return fmt.Errorf("%w: %s is locked to task %s", ErrProxyLocked, a.ProxyID, p.OwnerID)
	}
	return nil
}

// CheckAssignment reports whether a still resolves against the live pool,
// returning ErrGroupNotFound, ErrProxyNotFound, ErrProxyNotInGroup, or
// ErrProxyLocked when it does not. It is what a recovering task's fallback
// policy asks before deciding whether to run, run proxyless, or refuse; the
// acquire loop asks the same question, so there is one rule, not two.
func (m *Manager) CheckAssignment(a Assignment) error {
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

// hold records one live lease of the proxy by taskID. Tracking the holder, not
// just a count, is what lets a delete ask the usage policy whether the tasks
// actually leasing a proxy are running. Callers hold m.mu.
func (m *Manager) hold(proxyID, taskID string) {
	holders, ok := m.holders[proxyID]
	if !ok {
		holders = make(map[string]int)
		m.holders[proxyID] = holders
	}
	holders[taskID]++
}

// unhold drops one live lease of the proxy by taskID, pruning empty entries so
// heldCount and holderIDs never see a stale zero. Callers hold m.mu.
func (m *Manager) unhold(proxyID, taskID string) {
	holders := m.holders[proxyID]
	if holders[taskID] > 0 {
		holders[taskID]--
	}
	if holders[taskID] == 0 {
		delete(holders, taskID)
	}
	if len(holders) == 0 {
		delete(m.holders, proxyID)
	}
}

// heldCount is the proxy's live lease count across every holder, the number
// the effective holder cap is measured against. Callers hold m.mu.
func (m *Manager) heldCount(proxyID string) int {
	n := 0
	for _, held := range m.holders[proxyID] {
		n += held
	}
	return n
}

// holderIDs lists the tasks holding a live lease on the proxy, sorted so
// snapshots compare and messages read stably. Callers hold m.mu.
func (m *Manager) holderIDs(proxyID string) []string {
	ids := make([]string, 0, len(m.holders[proxyID]))
	for taskID := range m.holders[proxyID] {
		ids = append(ids, taskID)
	}
	sort.Strings(ids)
	return ids
}

// selectUnlocked picks an unlocked, under-capacity proxy from the assignment's
// group via that group's strategy instance, or the pinned proxy alone when a
// names one; found is false when there are no candidates.
// requireIdle narrows the field to proxies nobody is holding, for the callers
// that are about to bind one: a lock must not land on a proxy another task is
// already leasing, or the owner would queue behind a stranger on its own proxy.
// Callers hold m.mu.
func (m *Manager) selectUnlocked(a Assignment, requireIdle bool) (Proxy, bool, error) {
	candidates := make([]Proxy, 0, len(m.order))
	eligible := make(map[string]Proxy, len(m.order))
	for _, id := range m.order {
		p := m.pool[id]
		if p.GroupID != a.GroupID || p.OwnerID != "" {
			continue
		}
		if a.ProxyID != "" && p.ID != a.ProxyID {
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
		return Proxy{}, false, nil
	}
	// A pin leaves exactly one candidate. Running it through the group's
	// strategy anyway would advance shared rotation state for a choice nobody
	// made, skewing what the group's other tasks get next.
	if a.ProxyID != "" {
		return candidates[0], true, nil
	}

	p, err := m.strategies[a.GroupID].Select(candidates)
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
	return m.unlock(ctx, taskID, id)
}

// ReleaseStaleLock drops a.TaskID's durable lock when its new placement no
// longer fits: a pin naming a different proxy, a group the locked proxy is not
// in, or no placement at all. A lock the placement still fits is kept, so
// repointing a task at the proxy it already holds does not briefly return that
// proxy to the pool for another task to take.
//
// It is the reassignment counterpart to Unlock — wire it into a task service
// the same way — and the sanctioned resolution of ErrPinConflict: a reassign is
// a deliberate act and outranks a lock, while a lease is not and must not.
//
// A live lease on the released proxy is untouched: the run holding it keeps it
// to completion, and the new placement takes effect at the task's next lock.
// Unlike Acquire, an empty GroupID here means no group rather than the global
// one, since a task reassigned to no proxies at all must lose its lock.
func (m *Manager) ReleaseStaleLock(ctx context.Context, a Assignment) error {
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
func fits(a Assignment, p Proxy) bool {
	if a.ProxyID != "" {
		return a.ProxyID == p.ID
	}
	return a.GroupID != "" && a.GroupID == p.GroupID
}

// unlock clears the durable lock and returns the proxy to the rotating pool,
// waking waiters. A binding whose proxy is already gone is simply dropped.
// Callers hold m.mu.
func (m *Manager) unlock(ctx context.Context, taskID, proxyID string) error {
	p, live := m.pool[proxyID]
	if !live {
		delete(m.bindings, taskID)
		return nil
	}
	p.OwnerID = ""
	p.UpdatedAt = time.Now().UTC()
	if err := m.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("persist unlock: %w", err)
	}
	m.pool[proxyID] = p
	delete(m.bindings, taskID)
	m.cond.Broadcast()
	return nil
}

// DeleteProxy removes a proxy from the pool and the repository. It refuses
// with ErrProxyInUse while the usage policy reports a running task holding a
// lease on it, locked to it, or leasing from its group — suspend or kill those
// tasks first. Deleting an idle but locked proxy runs the deletion policy and
// executes its decision — Reassign selects the replacement from the deleted
// proxy's group; a Fail decision returns ErrTaskOrphaned naming the task so
// the deleter can act.
func (m *Manager) DeleteProxy(ctx context.Context, id string) error {
	for attempt := 0; attempt < deleteAttempts; attempt++ {
		m.mu.Lock()
		p, ok := m.pool[id]
		if !ok {
			m.mu.Unlock()
			// Nothing live to guard, but the store may still carry a row this
			// manager never loaded.
			return m.repo.Delete(ctx, id)
		}
		snap := m.snapshotUsage([]Proxy{p})
		m.mu.Unlock()

		if err := m.checkIdle(ctx, p.GroupID, snap, ErrProxyInUse); err != nil {
			return err
		}

		m.mu.Lock()
		if !m.matchesSnapshot(snap) {
			m.mu.Unlock()
			continue // the guard answered about a proxy that has since moved
		}
		err := m.deleteProxy(ctx, p)
		m.mu.Unlock()
		return err
	}
	return fmt.Errorf("delete proxy %s: the pool kept changing under the usage guard", id)
}

// deleteProxy removes p, running the deletion policy when it carries a lock.
// Callers hold m.mu and have already cleared the usage guard.
func (m *Manager) deleteProxy(ctx context.Context, p Proxy) error {
	if p.OwnerID == "" {
		return m.remove(ctx, p)
	}
	if m.policy == nil {
		return fmt.Errorf("proxy %s is locked to task %s and no deletion policy is set", p.ID, p.OwnerID)
	}
	// Every branch removes the proxy before touching the binding: an aborted
	// remove must not leave a task unbound in memory while the store still
	// carries the lock, which a restart would resurrect.
	owner := p.OwnerID
	switch decision := m.policy.OnProxyDeleted(ctx, owner, p); decision {
	case Reassign:
		next, found, err := m.selectUnlocked(Assignment{GroupID: p.GroupID}, true)
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

// usageSnapshot is the reading of the pool the usage guard is asked about: the
// proxies a delete would destroy, plus the tasks holding a live lease on each.
// The guard cannot be asked under m.mu, so its answer only binds a pool that
// still matches this.
type usageSnapshot struct {
	doomed  []Proxy
	holders map[string][]string // proxy ID -> task IDs holding a live lease
}

// holderIDs lists every task holding a lease on any doomed proxy, deduped and
// sorted. It is what the guard falls back to when no usage policy can say which
// of them is running.
func (s usageSnapshot) holderIDs() []string {
	seen := make(map[string]bool)
	for _, held := range s.holders {
		for _, taskID := range held {
			seen[taskID] = true
		}
	}
	ids := make([]string, 0, len(seen))
	for taskID := range seen {
		ids = append(ids, taskID)
	}
	sort.Strings(ids)
	return ids
}

// snapshotUsage reads doomed and their live holders. Callers hold m.mu.
func (m *Manager) snapshotUsage(doomed []Proxy) usageSnapshot {
	snap := usageSnapshot{doomed: doomed, holders: make(map[string][]string, len(doomed))}
	for _, p := range doomed {
		if ids := m.holderIDs(p.ID); len(ids) > 0 {
			snap.holders[p.ID] = ids
		}
	}
	return snap
}

// matchesSnapshot reports whether the pool still reads as it did when snap was
// taken: same proxies, same groups and owners, same holders. Callers hold m.mu.
func (m *Manager) matchesSnapshot(snap usageSnapshot) bool {
	for _, p := range snap.doomed {
		live, ok := m.pool[p.ID]
		if !ok || live.GroupID != p.GroupID || live.OwnerID != p.OwnerID {
			return false
		}
		if !slices.Equal(m.holderIDs(p.ID), snap.holders[p.ID]) {
			return false
		}
	}
	return true
}

// checkIdle asks the usage policy whether destroying snap.doomed — every one a
// member of groupID — would disrupt a live run, wrapping any refusal in inUse.
// Three questions cover it: is a running task rotating through the group, is
// one holding a lease on a doomed proxy, and is one locked to a doomed proxy.
// The last two are asked per task rather than folded into the first, because a
// durable binding outranks the group a task is assigned: its holder may be
// running against some other group entirely.
//
// Without a usage policy there is nothing to ask and every delete proceeds.
// Callers must not hold m.mu (see UsagePolicy).
func (m *Manager) checkIdle(ctx context.Context, groupID string, snap usageSnapshot, inUse error) error {
	running, err := m.runningFor(ctx, groupID, snap)
	if err != nil {
		return err
	}
	if len(running) > 0 {
		return fmt.Errorf("%w: %s is linked to running %s", inUse, groupID, plural("task", running))
	}
	return nil
}

// runningFor collects every running task the destruction of snap.doomed would
// disrupt: those rotating groupID, those holding a lease on a doomed proxy, and
// those a doomed proxy's durable lock names. The last two are asked per task
// rather than folded into the first, because a durable binding outranks the
// group a task is assigned: its holder may be running against some other group
// entirely.
//
// With no usage policy wired it falls back to the one fact the manager owns
// outright — who is holding a lease right now — and treats every holder as
// running. Nothing can tell a live holder from a parked one without the policy,
// and of the two ways to be wrong, refusing a delete is reversible (wire the
// policy and retry) while deleting a proxy out from under a request is not.
// Callers must not hold m.mu (see UsagePolicy).
func (m *Manager) runningFor(ctx context.Context, groupID string, snap usageSnapshot) ([]string, error) {
	if m.usage == nil {
		return snap.holderIDs(), nil
	}

	inGroup, err := m.usage.RunningTasks(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("check proxy group %s usage: %w", groupID, err)
	}
	found := make(map[string]bool, len(inGroup))
	for _, taskID := range inGroup {
		found[taskID] = true
	}

	asked := make(map[string]bool)
	for _, p := range snap.doomed {
		linked := append(append([]string{}, snap.holders[p.ID]...), p.OwnerID)
		for _, taskID := range linked {
			if taskID == "" || found[taskID] {
				continue
			}
			live, err := m.taskRunning(ctx, asked, taskID)
			if err != nil {
				return nil, err
			}
			if live {
				found[taskID] = true
			}
		}
	}

	ids := make([]string, 0, len(found))
	for taskID := range found {
		ids = append(ids, taskID)
	}
	sort.Strings(ids)
	return ids, nil
}

// plural renders an id list as "task t1" or "tasks t1, t2", so a refusal reads
// as a sentence and still names everything blocking it.
func plural(noun string, ids []string) string {
	if len(ids) == 1 {
		return noun + " " + ids[0]
	}
	return noun + "s " + strings.Join(ids, ", ")
}

// An Impact is what deleting a proxy, or a whole group, would cost the tasks
// linked to it. Running names the tasks the deletion is refused for; Pinned
// names the resumable tasks that would keep their assignment and be unable to
// run under it until they are reassigned. Pinned is a warning, not a refusal —
// deleting a proxy is a deliberate act, and what it costs is the deleter's call.
type Impact struct {
	Running []string
	Pinned  []string
}

// Empty reports whether the deletion would disturb no task at all.
func (i Impact) Empty() bool {
	return len(i.Running) == 0 && len(i.Pinned) == 0
}

// DeletionImpact reports what deleting the proxy would cost, without deleting
// anything: which running tasks would refuse it, and which resumable tasks are
// pinned to it and would be stranded until reassigned. Render it as the warning
// a deliberate deletion deserves, then call DeleteProxy — which enforces only
// the Running half.
//
// A proxy the manager does not know disturbs nothing, and without a usage
// policy wired it reports nothing: the same blindness that lets DeleteProxy
// delete anything.
func (m *Manager) DeletionImpact(ctx context.Context, proxyID string) (Impact, error) {
	m.mu.Lock()
	p, ok := m.pool[proxyID]
	if !ok {
		m.mu.Unlock()
		return Impact{}, nil
	}
	snap := m.snapshotUsage([]Proxy{p})
	m.mu.Unlock()

	return m.impact(ctx, p.GroupID, snap)
}

// GroupDeletionImpact reports what cascade-deleting the group would cost,
// pooling the impact of every member. See DeletionImpact.
func (m *Manager) GroupDeletionImpact(ctx context.Context, groupID string) (Impact, error) {
	m.mu.Lock()
	if _, ok := m.groups[groupID]; !ok {
		m.mu.Unlock()
		return Impact{}, fmt.Errorf("proxy group %s: %w", groupID, ErrGroupNotFound)
	}
	snap := m.snapshotUsage(m.members(groupID))
	m.mu.Unlock()

	return m.impact(ctx, groupID, snap)
}

// impact answers both deletion-impact queries. Callers must not hold m.mu.
func (m *Manager) impact(ctx context.Context, groupID string, snap usageSnapshot) (Impact, error) {
	running, err := m.runningFor(ctx, groupID, snap)
	if err != nil {
		return Impact{}, err
	}
	report := Impact{Running: running}
	if m.usage == nil {
		return report, nil
	}

	// A running task is already the stronger finding; listing it as merely
	// pinned as well would double-count it in the warning.
	seen := make(map[string]bool, len(running))
	for _, taskID := range running {
		seen[taskID] = true
	}
	for _, p := range snap.doomed {
		pinned, err := m.usage.PinnedTasks(ctx, p.ID)
		if err != nil {
			return Impact{}, fmt.Errorf("check tasks pinned to proxy %s: %w", p.ID, err)
		}
		for _, taskID := range pinned {
			if seen[taskID] {
				continue
			}
			seen[taskID] = true
			report.Pinned = append(report.Pinned, taskID)
		}
	}
	sort.Strings(report.Pinned)
	return report, nil
}

// taskRunning asks the usage policy whether taskID is running, memoizing in
// asked so a task holding or locking several doomed proxies is asked about once.
func (m *Manager) taskRunning(ctx context.Context, asked map[string]bool, taskID string) (bool, error) {
	if live, done := asked[taskID]; done {
		return live, nil
	}
	live, err := m.usage.TaskIsRunning(ctx, taskID)
	if err != nil {
		return false, fmt.Errorf("check task %s liveness: %w", taskID, err)
	}
	asked[taskID] = live
	return live, nil
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
	taskID  string
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
	l.once.Do(func() { err = l.manager.release(l.proxy.ID, l.taskID, success) })
	return err
}

// release updates stats, frees the holder slot, wakes waiters, and persists the
// updated record before dropping the lock — a save made outside it could land
// after a concurrent Unlock's and resurrect the stale owner durably. The save
// uses a background context so it lands even when the caller's context is gone.
func (m *Manager) release(id, taskID string, success bool) error {
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
		return nil // proxy was deleted while leased; nothing to persist
	}
	return m.repo.Save(context.Background(), p)
}
