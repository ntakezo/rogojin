package proxies

import (
	"context"

	"github.com/ntakezo/rogojin/leasing"
)

// A Manager allocates proxies to tasks: locked proxies go only to their owner,
// unlocked ones rotate within their group through the group's selection
// strategy under the effective holder cap (the proxy's own MaxHolders, else
// its group's, else 1). It owns all live lease state; the Repository only
// stores bytes. A Manager is safe for concurrent use.
type Manager struct {
	core *leasing.Manager[attrs]
}

// A ManagerOption configures a Manager at construction.
type ManagerOption func(*managerConfig)

// managerConfig collects what the options set before the core is built.
type managerConfig struct {
	strategies map[string]StrategyFactory
	usage      UsagePolicy
}

// WithStrategy registers factory under name, so groups may reference custom
// selection algorithms beyond the built-ins (or override a built-in).
func WithStrategy(name string, factory StrategyFactory) ManagerOption {
	return func(c *managerConfig) { c.strategies[name] = factory }
}

// WithUsagePolicy wires the guard DeleteProxy and DeleteGroup consult to
// refuse deleting a proxy a running task is leasing, locked to, or rotating
// through. Without one, neither can tell a running task from a parked one, so
// both fall back to refusing any proxy with a live lease on it — safe, but
// blind to a task that is merely assigned the group and between leases, and
// unable to free a proxy by suspending its task.
func WithUsagePolicy(usage UsagePolicy) ManagerOption {
	return func(c *managerConfig) { c.usage = usage }
}

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent. Groups and pool change afterwards only through
// CreateGroup, DeleteGroup, AddProxy, and DeleteProxy.
func NewManager(ctx context.Context, repo Repository, policy DeletionPolicy, opts ...ManagerOption) (*Manager, error) {
	cfg := managerConfig{strategies: make(map[string]StrategyFactory)}
	for _, opt := range opts {
		opt(&cfg)
	}

	// Built-ins are installed first so a WithStrategy under the same name
	// replaces one, which is the documented way to override a built-in.
	strategies := map[string]leasing.StrategyFactory[attrs]{
		StrategyRoundRobin: func() leasing.Selection[attrs] { return leasing.NewRoundRobin[attrs]() },
		StrategyBayesian:   coreFactory(func() Selection { return NewBayesian() }),
	}
	for name, factory := range cfg.strategies {
		strategies[name] = coreFactory(factory)
	}

	var repository leasing.Repository[attrs]
	if repo != nil {
		repository = repoAdapter{repo: repo}
	}
	var deletion leasing.DeletionPolicy[attrs]
	if policy != nil {
		deletion = policyAdapter{policy: policy}
	}

	core, err := leasing.NewManager(ctx, leasing.Config[attrs]{
		Noun:            "proxy",
		Repository:      repository,
		Policy:          deletion,
		Usage:           cfg.usage,
		Strategies:      strategies,
		DefaultStrategy: StrategyRoundRobin,
	})
	if err != nil {
		return nil, err
	}
	return &Manager{core: core}, nil
}

// CreateGroup persists and installs a new group. ID must be unset in the
// manager; Strategy must be registered (empty selects round robin).
func (m *Manager) CreateGroup(ctx context.Context, g Group) error {
	return m.core.CreateGroup(ctx, g)
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
	return m.core.DeleteGroup(ctx, id)
}

// AddProxy persists and installs a new unlocked proxy, defaulting an empty
// GroupID to the global group. The group must exist.
func (m *Manager) AddProxy(ctx context.Context, p Proxy) error {
	return m.core.Add(ctx, toResource(p))
}

// Acquire leases a proxy under a: its locked proxy if it has one — a durable
// binding outranks the requested group — otherwise the pinned member, else one
// rotated from the group's unlocked members. It blocks until a proxy frees or
// ctx is done; a group with no proxies fails immediately with ErrNoProxies, and
// a pin that no longer resolves fails with ErrProxyNotFound.
func (m *Manager) Acquire(ctx context.Context, a Assignment) (*Lease, error) {
	return wrapLease(m.core.Acquire(ctx, toAssignment(a)))
}

// Lock durably binds a.TaskID to a proxy (the pinned one, or one selected from
// the group when unpinned; idempotent) and leases it. The binding outlives the
// lease and the manager until Unlock, a reassignment, or the proxy's deletion;
// no other task can ever acquire the proxy.
func (m *Manager) Lock(ctx context.Context, a Assignment) (*Lease, error) {
	return wrapLease(m.core.Lock(ctx, toAssignment(a)))
}

// Unlock removes taskID's durable lock, returning its proxy to the rotating
// pool. It is a no-op if taskID has no locked proxy.
func (m *Manager) Unlock(ctx context.Context, taskID string) error {
	return m.core.Unlock(ctx, taskID)
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
	return m.core.ReleaseStaleLock(ctx, toAssignment(a))
}

// CheckAssignment reports whether a still resolves against the live pool,
// returning ErrGroupNotFound, ErrProxyNotFound, ErrProxyNotInGroup, or
// ErrProxyLocked when it does not. It is what a recovering task's fallback
// policy asks before deciding whether to run, run proxyless, or refuse; the
// acquire loop asks the same question, so there is one rule, not two.
func (m *Manager) CheckAssignment(a Assignment) error {
	return m.core.CheckAssignment(toAssignment(a))
}

// DeleteProxy removes a proxy from the pool and the repository. It refuses
// with ErrProxyInUse while the usage policy reports a running task holding a
// lease on it, locked to it, or leasing from its group — suspend or kill those
// tasks first. Deleting an idle but locked proxy runs the deletion policy and
// executes its decision — Reassign selects the replacement from the deleted
// proxy's group; a Fail decision returns ErrTaskOrphaned naming the task so
// the deleter can act.
func (m *Manager) DeleteProxy(ctx context.Context, id string) error {
	return m.core.Delete(ctx, id)
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
	return m.core.DeletionImpact(ctx, proxyID)
}

// GroupDeletionImpact reports what cascade-deleting the group would cost,
// pooling the impact of every member. See DeletionImpact.
func (m *Manager) GroupDeletionImpact(ctx context.Context, groupID string) (Impact, error) {
	return m.core.GroupDeletionImpact(ctx, groupID)
}

// toAssignment renames the pin for the core, which knows no proxies.
func toAssignment(a Assignment) leasing.Assignment {
	return leasing.Assignment{TaskID: a.TaskID, GroupID: a.GroupID, ResourceID: a.ProxyID}
}

// Lease is a live hold on one proxy. Release it exactly once when done.
type Lease struct {
	core *leasing.Lease[attrs]
}

// wrapLease adapts a core lease, leaving a failed acquire's nil lease nil.
func wrapLease(core *leasing.Lease[attrs], err error) (*Lease, error) {
	if err != nil {
		return nil, err
	}
	return &Lease{core: core}, nil
}

// Proxy returns the leased proxy as of acquisition.
func (l *Lease) Proxy() Proxy {
	return fromResource(l.core.Resource())
}

// Release frees the proxy, records the outcome the bayesian strategy learns
// from, and persists it. Only the first call acts; later calls return nil.
func (l *Lease) Release(success bool) error {
	return l.core.Release(success)
}

// repoAdapter presents a proxies.Repository to the core as a store of leasing
// resources. The consumer's store stays proxy-shaped; the translation is here.
type repoAdapter struct {
	repo Repository
}

func (a repoAdapter) List(ctx context.Context) ([]leasing.Resource[attrs], error) {
	listed, err := a.repo.List(ctx)
	if err != nil {
		return nil, err
	}
	resources := make([]leasing.Resource[attrs], len(listed))
	for i, p := range listed {
		resources[i] = toResource(p)
	}
	return resources, nil
}

func (a repoAdapter) Save(ctx context.Context, r leasing.Resource[attrs]) error {
	return a.repo.Save(ctx, fromResource(r))
}

func (a repoAdapter) Delete(ctx context.Context, id string) error {
	return a.repo.Delete(ctx, id)
}

func (a repoAdapter) ListGroups(ctx context.Context) ([]Group, error) {
	return a.repo.ListGroups(ctx)
}

func (a repoAdapter) SaveGroup(ctx context.Context, g Group) error {
	return a.repo.SaveGroup(ctx, g)
}

func (a repoAdapter) DeleteGroup(ctx context.Context, id string) error {
	return a.repo.DeleteGroup(ctx, id)
}

// policyAdapter presents a proxies.DeletionPolicy to the core.
type policyAdapter struct {
	policy DeletionPolicy
}

func (a policyAdapter) OnDeleted(ctx context.Context, taskID string, deleted leasing.Resource[attrs]) leasing.Decision {
	return a.policy.OnProxyDeleted(ctx, taskID, fromResource(deleted))
}

// coreFactory presents a proxy-shaped strategy to the core, which selects over
// resources. The core validates the pick by ID, so the round trip is safe.
func coreFactory(factory StrategyFactory) leasing.StrategyFactory[attrs] {
	return func() leasing.Selection[attrs] { return selectionAdapter{selection: factory()} }
}

type selectionAdapter struct {
	selection Selection
}

func (a selectionAdapter) Select(candidates []leasing.Resource[attrs]) (leasing.Resource[attrs], error) {
	proxied := make([]Proxy, len(candidates))
	for i, c := range candidates {
		proxied[i] = fromResource(c)
	}
	picked, err := a.selection.Select(proxied)
	if err != nil {
		return leasing.Resource[attrs]{}, err
	}
	return toResource(picked), nil
}
