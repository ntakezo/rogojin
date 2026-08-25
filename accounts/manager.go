package accounts

import (
	"context"
	"encoding/json"

	"github.com/ntakezo/rogojin/leasing"
)

// rotation is the one selection algorithm accounts use: members of a group are
// handed out in turn. It is not configurable, so it is named only here.
const rotation = leasing.StrategyRoundRobin

// A Manager allocates accounts to tasks: locked accounts go only to their
// owner, unlocked ones are handed out in turn within their group under the
// effective holder cap (the account's own MaxHolders, else its group's, else
// 1). It owns all live lease state; the Repository only stores bytes. A Manager
// is safe for concurrent use.
type Manager struct {
	core *leasing.Manager[json.RawMessage]
}

// A ManagerOption configures a Manager at construction.
type ManagerOption func(*managerConfig)

// managerConfig collects what the options set before the core is built.
type managerConfig struct {
	usage UsagePolicy
}

// WithUsagePolicy wires the guard DeleteAccount and DeleteGroup consult to
// refuse deleting an account a running task is leasing or locked to. Without
// one, neither can tell a running task from a parked one, so both fall back to
// refusing any account with a live lease on it — safe, but unable to free an
// account by suspending its task.
func WithUsagePolicy(usage UsagePolicy) ManagerOption {
	return func(c *managerConfig) { c.usage = usage }
}

// NewManager loads the groups and accounts from the repository, persisting the
// global group if absent. Groups and accounts change afterwards only through
// CreateGroup, DeleteGroup, AddAccount, and DeleteAccount.
func NewManager(ctx context.Context, repo Repository, policy DeletionPolicy, opts ...ManagerOption) (*Manager, error) {
	var cfg managerConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	var repository leasing.Repository[json.RawMessage]
	if repo != nil {
		repository = repoAdapter{repo: repo}
	}
	var deletion leasing.DeletionPolicy[json.RawMessage]
	if policy != nil {
		deletion = policyAdapter{policy: policy}
	}

	core, err := leasing.NewManager(ctx, leasing.Config[json.RawMessage]{
		Noun:       "account",
		Repository: repository,
		Policy:     deletion,
		Usage:      cfg.usage,
		Strategies: map[string]leasing.StrategyFactory[json.RawMessage]{
			rotation: func() leasing.Selection[json.RawMessage] {
				return leasing.NewRoundRobin[json.RawMessage]()
			},
		},
		DefaultStrategy: rotation,
	})
	if err != nil {
		return nil, err
	}
	return &Manager{core: core}, nil
}

// CreateGroup persists and installs a new group. ID must be unset in the
// manager.
func (m *Manager) CreateGroup(ctx context.Context, g Group) error {
	return m.core.CreateGroup(ctx, toGroup(g))
}

// DeleteGroup cascade-deletes a group and every account in it. It refuses with
// ErrGroupInUse while the usage policy reports a running task leasing from the
// group, holding a lease on a member, or locked to one — suspend or kill those
// tasks first. Locked members consult the deletion policy; because the whole
// group is going away there is nothing in-group to reassign to, so a Reassign
// decision degrades to Unbind and is reported in the returned (joined) error
// alongside any ErrTaskOrphaned from Fail decisions. The global group cannot be
// deleted. With locked members and no policy wired it refuses before mutating
// anything.
func (m *Manager) DeleteGroup(ctx context.Context, id string) error {
	return m.core.DeleteGroup(ctx, id)
}

// AddAccount persists and installs a new unlocked account, defaulting an empty
// GroupID to the global group. The group must exist.
func (m *Manager) AddAccount(ctx context.Context, a Account) error {
	return m.core.Add(ctx, toResource(a))
}

// Acquire leases an account under a: its locked account if it has one — a
// durable binding outranks the requested group — otherwise the pinned member,
// else the next unlocked member of the group. It blocks until an account frees
// or ctx is done; a group with no accounts fails immediately with
// ErrNoAccounts, and a pin that no longer resolves fails with
// ErrAccountNotFound.
func (m *Manager) Acquire(ctx context.Context, a Assignment) (*Lease, error) {
	return wrapLease(m.core.Acquire(ctx, toAssignment(a)))
}

// Lock durably binds a.TaskID to an account (the pinned one, or the next free
// member of the group when unpinned; idempotent) and leases it. The binding
// outlives the lease and the manager until Unlock, a reassignment, or the
// account's deletion; no other task can ever acquire it.
//
// This is the usual way to take an account: a task that logged in once should
// come back to the same identity after a restart, and the lock is what makes
// that survive the process.
func (m *Manager) Lock(ctx context.Context, a Assignment) (*Lease, error) {
	return wrapLease(m.core.Lock(ctx, toAssignment(a)))
}

// Unlock removes taskID's durable lock, returning its account to the pool. It
// is a no-op if taskID has no locked account. Wire it into the task service's
// releaser so deleting a task never strands an identity.
func (m *Manager) Unlock(ctx context.Context, taskID string) error {
	return m.core.Unlock(ctx, taskID)
}

// ReleaseStaleLock drops a.TaskID's durable lock when its new placement no
// longer fits: a pin naming a different account, a group the locked account is
// not in, or no placement at all. A lock the placement still fits is kept, so
// repointing a task at the account it already holds does not briefly return it
// to the pool for another task to take.
//
// It is the reassignment counterpart to Unlock and the sanctioned resolution of
// ErrPinConflict: a reassign is a deliberate act and outranks a lock, while a
// lease is not and must not.
//
// A live lease on the released account is untouched: the run holding it keeps
// it to completion, and the new placement takes effect at the task's next lock.
// Unlike Acquire, an empty GroupID here means no group rather than the global
// one, since a task reassigned to no accounts at all must lose its lock.
func (m *Manager) ReleaseStaleLock(ctx context.Context, a Assignment) error {
	return m.core.ReleaseStaleLock(ctx, toAssignment(a))
}

// CheckAssignment reports whether a still resolves against the live pool,
// returning ErrGroupNotFound, ErrAccountNotFound, ErrAccountNotInGroup, or
// ErrAccountLocked when it does not. It is what a recovering task's fallback
// policy asks before deciding whether to run; the acquire loop asks the same
// question, so there is one rule, not two.
func (m *Manager) CheckAssignment(a Assignment) error {
	return m.core.CheckAssignment(toAssignment(a))
}

// DeleteAccount removes an account from the pool and the repository. It refuses
// with ErrAccountInUse while the usage policy reports a running task holding a
// lease on it, locked to it, or leasing from its group — suspend or kill those
// tasks first. Deleting an idle but locked account runs the deletion policy and
// executes its decision; a Fail decision returns ErrTaskOrphaned naming the task
// so the deleter can act.
func (m *Manager) DeleteAccount(ctx context.Context, id string) error {
	return m.core.Delete(ctx, id)
}

// DeletionImpact reports what deleting the account would cost, without deleting
// anything: which running tasks would refuse it, and which resumable tasks are
// pinned to it and would be stranded until reassigned. Render it as the warning
// a deliberate deletion deserves, then call DeleteAccount — which enforces only
// the Running half.
//
// An account the manager does not know disturbs nothing, and without a usage
// policy wired it reports nothing.
func (m *Manager) DeletionImpact(ctx context.Context, accountID string) (Impact, error) {
	return m.core.DeletionImpact(ctx, accountID)
}

// GroupDeletionImpact reports what cascade-deleting the group would cost,
// pooling the impact of every member. See DeletionImpact.
func (m *Manager) GroupDeletionImpact(ctx context.Context, groupID string) (Impact, error) {
	return m.core.GroupDeletionImpact(ctx, groupID)
}

// toAssignment renames the pin for the core, which knows no accounts.
func toAssignment(a Assignment) leasing.Assignment {
	return leasing.Assignment{TaskID: a.TaskID, GroupID: a.GroupID, ResourceID: a.AccountID}
}

// Lease is a live hold on one account. Release it exactly once when done.
type Lease struct {
	core *leasing.Lease[json.RawMessage]
}

// wrapLease adapts a core lease, leaving a failed acquire's nil lease nil.
func wrapLease(core *leasing.Lease[json.RawMessage], err error) (*Lease, error) {
	if err != nil {
		return nil, err
	}
	return &Lease{core: core}, nil
}

// Account returns the leased account as of acquisition.
func (l *Lease) Account() Account {
	return fromResource(l.core.Resource())
}

// Release frees the account, records the outcome — which is how a burned
// account becomes visible in its Failures — and persists it. Only the first
// call acts; later calls return nil.
func (l *Lease) Release(success bool) error {
	return l.core.Release(success)
}

// repoAdapter presents an accounts.Repository to the core as a store of leasing
// resources. The consumer's store stays account-shaped; the translation is here.
type repoAdapter struct {
	repo Repository
}

func (a repoAdapter) List(ctx context.Context) ([]leasing.Resource[json.RawMessage], error) {
	listed, err := a.repo.List(ctx)
	if err != nil {
		return nil, err
	}
	resources := make([]leasing.Resource[json.RawMessage], len(listed))
	for i, account := range listed {
		resources[i] = toResource(account)
	}
	return resources, nil
}

func (a repoAdapter) Save(ctx context.Context, r leasing.Resource[json.RawMessage]) error {
	return a.repo.Save(ctx, fromResource(r))
}

func (a repoAdapter) Delete(ctx context.Context, id string) error {
	return a.repo.Delete(ctx, id)
}

func (a repoAdapter) ListGroups(ctx context.Context) ([]leasing.Group, error) {
	listed, err := a.repo.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	groups := make([]leasing.Group, len(listed))
	for i, g := range listed {
		groups[i] = toGroup(g)
	}
	return groups, nil
}

func (a repoAdapter) SaveGroup(ctx context.Context, g leasing.Group) error {
	return a.repo.SaveGroup(ctx, fromGroup(g))
}

func (a repoAdapter) DeleteGroup(ctx context.Context, id string) error {
	return a.repo.DeleteGroup(ctx, id)
}

// policyAdapter presents an accounts.DeletionPolicy to the core.
type policyAdapter struct {
	policy DeletionPolicy
}

func (a policyAdapter) OnDeleted(ctx context.Context, taskID string, deleted leasing.Resource[json.RawMessage]) leasing.Decision {
	return a.policy.OnAccountDeleted(ctx, taskID, fromResource(deleted))
}
