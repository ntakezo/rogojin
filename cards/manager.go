package cards

import (
	"context"
	"encoding/json"

	"github.com/ntakezo/rogojin/leasing"
)

// rotation is the one selection algorithm cards use: members of a group are
// handed out in turn. It is not configurable, so it is named only here.
const rotation = leasing.StrategyRoundRobin

// A Manager allocates cards to tasks: locked cards go only to their owner,
// unlocked ones are handed out in turn within their group under the effective
// holder cap (the card's own MaxHolders, else its group's, else 1). It owns all
// live lease state; the Repository only stores bytes. A Manager is safe for
// concurrent use.
type Manager struct {
	core *leasing.Manager[json.RawMessage]
}

// A ManagerOption configures a Manager at construction.
type ManagerOption func(*managerConfig)

// managerConfig collects what the options set before the core is built.
type managerConfig struct {
	usage UsagePolicy
}

// WithUsagePolicy wires the guard DeleteCard and DeleteGroup consult to refuse
// deleting a card a running task is leasing or locked to. Without one, neither
// can tell a running task from a parked one, so both fall back to refusing any
// card with a live lease on it — safe, but unable to free a card by suspending
// its task.
func WithUsagePolicy(usage UsagePolicy) ManagerOption {
	return func(c *managerConfig) { c.usage = usage }
}

// NewManager loads the groups and cards from the repository, persisting the
// global group if absent. Groups and cards change afterwards only through
// CreateGroup, DeleteGroup, AddCard, and DeleteCard.
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
		Noun:       "card",
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

// DeleteGroup cascade-deletes a group and every card in it. It refuses with
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

// AddCard persists and installs a new unlocked card, defaulting an empty
// GroupID to the global group. The group must exist.
func (m *Manager) AddCard(ctx context.Context, c Card) error {
	return m.core.Add(ctx, toResource(c))
}

// Acquire leases a card under a: its locked card if it has one — a durable
// binding outranks the requested group — otherwise the pinned member, else the
// next unlocked member of the group. It blocks until a card frees or ctx is
// done; a group with no cards fails immediately with ErrNoCards, and a pin that
// no longer resolves fails with ErrCardNotFound.
func (m *Manager) Acquire(ctx context.Context, a Assignment) (*Lease, error) {
	return wrapLease(m.core.Acquire(ctx, toAssignment(a)))
}

// Lock durably binds a.TaskID to a card (the pinned one, or the next free
// member of the group when unpinned; idempotent) and leases it. The binding
// outlives the lease and the manager until Unlock, a reassignment, or the card's
// deletion; no other task can ever acquire it.
//
// This is the usual way to take a card: a checkout that put one instrument on
// the order should come back to it after a restart rather than settling against
// another, and the lock is what makes that survive the process.
func (m *Manager) Lock(ctx context.Context, a Assignment) (*Lease, error) {
	return wrapLease(m.core.Lock(ctx, toAssignment(a)))
}

// Unlock removes taskID's durable lock, returning its card to the pool. It is a
// no-op if taskID has no locked card. Wire it into the task service's releaser
// so deleting a task never strands an instrument.
func (m *Manager) Unlock(ctx context.Context, taskID string) error {
	return m.core.Unlock(ctx, taskID)
}

// ReleaseStaleLock drops a.TaskID's durable lock when its new placement no
// longer fits: a pin naming a different card, a group the locked card is not in,
// or no placement at all. A lock the placement still fits is kept, so repointing
// a task at the card it already holds does not briefly return it to the pool for
// another task to take.
//
// It is the reassignment counterpart to Unlock and the sanctioned resolution of
// ErrPinConflict: a reassign is a deliberate act and outranks a lock, while a
// lease is not and must not.
//
// A live lease on the released card is untouched: the run holding it keeps it to
// completion, and the new placement takes effect at the task's next lock. Unlike
// Acquire, an empty GroupID here means no group rather than the global one,
// since a task reassigned to no cards at all must lose its lock.
func (m *Manager) ReleaseStaleLock(ctx context.Context, a Assignment) error {
	return m.core.ReleaseStaleLock(ctx, toAssignment(a))
}

// StaleLockReleaser adapts m.ReleaseStaleLock to the plain strings a task
// service hands each of its resource kinds, so wiring one is a single line:
//
//	tasks.WithResource("card", m.Unlock, cards.StaleLockReleaser(m))
//
// An empty groupID means no group rather than the global one, exactly as it does
// for ReleaseStaleLock.
func StaleLockReleaser(m *Manager) func(ctx context.Context, taskID, groupID, cardID string) error {
	return func(ctx context.Context, taskID, groupID, cardID string) error {
		return m.ReleaseStaleLock(ctx, Assignment{TaskID: taskID, GroupID: groupID, CardID: cardID})
	}
}

// CheckAssignment reports whether a still resolves against the live pool,
// returning ErrGroupNotFound, ErrCardNotFound, ErrCardNotInGroup, or
// ErrCardLocked when it does not. It is what a recovering task's fallback policy
// asks before deciding whether to run; the acquire loop asks the same question,
// so there is one rule, not two.
func (m *Manager) CheckAssignment(a Assignment) error {
	return m.core.CheckAssignment(toAssignment(a))
}

// DeleteCard removes a card from the pool and the repository. It refuses with
// ErrCardInUse while the usage policy reports a running task holding a lease on
// it, locked to it, or leasing from its group — suspend or kill those tasks
// first. Deleting an idle but locked card runs the deletion policy and executes
// its decision; a Fail decision returns ErrTaskOrphaned naming the task so the
// deleter can act.
func (m *Manager) DeleteCard(ctx context.Context, id string) error {
	return m.core.Delete(ctx, id)
}

// DeletionImpact reports what deleting the card would cost, without deleting
// anything: which running tasks would refuse it, and which resumable tasks are
// pinned to it and would be stranded until reassigned. Render it as the warning
// a deliberate deletion deserves, then call DeleteCard — which enforces only the
// Running half.
//
// A card the manager does not know disturbs nothing, and without a usage policy
// wired it reports nothing.
func (m *Manager) DeletionImpact(ctx context.Context, cardID string) (Impact, error) {
	return m.core.DeletionImpact(ctx, cardID)
}

// GroupDeletionImpact reports what cascade-deleting the group would cost,
// pooling the impact of every member. See DeletionImpact.
func (m *Manager) GroupDeletionImpact(ctx context.Context, groupID string) (Impact, error) {
	return m.core.GroupDeletionImpact(ctx, groupID)
}

// toAssignment renames the pin for the core, which knows no cards.
func toAssignment(a Assignment) leasing.Assignment {
	return leasing.Assignment{TaskID: a.TaskID, GroupID: a.GroupID, ResourceID: a.CardID}
}

// Lease is a live hold on one card. Release it exactly once when done.
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

// Card returns the leased card as of acquisition.
func (l *Lease) Card() Card {
	return fromResource(l.core.Resource())
}

// Release frees the card, records the outcome — which is how a declining card
// becomes visible in its Failures — and persists it. Only the first call acts;
// later calls return nil.
func (l *Lease) Release(success bool) error {
	return l.core.Release(success)
}

// repoAdapter presents a cards.Repository to the core as a store of leasing
// resources. The consumer's store stays card-shaped; the translation is here.
type repoAdapter struct {
	repo Repository
}

func (a repoAdapter) List(ctx context.Context) ([]leasing.Resource[json.RawMessage], error) {
	listed, err := a.repo.List(ctx)
	if err != nil {
		return nil, err
	}
	resources := make([]leasing.Resource[json.RawMessage], len(listed))
	for i, card := range listed {
		resources[i] = toResource(card)
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

// policyAdapter presents a cards.DeletionPolicy to the core.
type policyAdapter struct {
	policy DeletionPolicy
}

func (a policyAdapter) OnDeleted(ctx context.Context, taskID string, deleted leasing.Resource[json.RawMessage]) leasing.Decision {
	return a.policy.OnCardDeleted(ctx, taskID, fromResource(deleted))
}
