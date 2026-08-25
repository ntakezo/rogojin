package leasing

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests cover the leasing layer itself — pooling, groups, holder caps,
// durable locks, pins, deletion policies, and the usage guard. They were
// written against the proxies package, which owned this machinery before it was
// extracted, and moved here with it. What stayed behind in proxies is what is
// actually proxy-specific: the payload, the strategy registry, and the
// translation between the two shapes.

// payload stands in for whatever a resource kind carries — a proxy's URL, an
// account's credentials. Most tests here never read it; the ones that do are
// checking the manager copies it without ever inspecting it.
type payload struct {
	secret string
	region string
}

// res is the resource under test, spelled short because it appears everywhere.
type res = Resource[payload]

// fakeRepo is an in-memory Repository recording saves so tests can assert
// persistence without sqlite.
type fakeRepo struct {
	mu         sync.Mutex
	order      []string
	records    map[string]res
	groupOrder []string
	groups     map[string]Group
}

func newFakeRepo(seed ...res) *fakeRepo {
	r := &fakeRepo{records: map[string]res{}, groups: map[string]Group{}}
	for _, p := range seed {
		r.records[p.ID] = p
		r.order = append(r.order, p.ID)
	}
	return r
}

func (r *fakeRepo) List(ctx context.Context) ([]res, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]res, 0, len(r.order))
	for _, id := range r.order {
		if p, ok := r.records[id]; ok {
			out = append(out, p)
		}
	}
	return out, nil
}

func (r *fakeRepo) Save(ctx context.Context, p res) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.records[p.ID]; !ok {
		r.order = append(r.order, p.ID)
	}
	r.records[p.ID] = p
	return nil
}

func (r *fakeRepo) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, id)
	return nil
}

func (r *fakeRepo) ListGroups(ctx context.Context) ([]Group, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Group, 0, len(r.groupOrder))
	for _, id := range r.groupOrder {
		if g, ok := r.groups[id]; ok {
			out = append(out, g)
		}
	}
	return out, nil
}

func (r *fakeRepo) SaveGroup(ctx context.Context, g Group) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.groups[g.ID]; !ok {
		r.groupOrder = append(r.groupOrder, g.ID)
	}
	r.groups[g.ID] = g
	return nil
}

func (r *fakeRepo) DeleteGroup(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.groups, id)
	return nil
}

func (r *fakeRepo) get(t *testing.T, id string) res {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.records[id]
	if !ok {
		t.Fatalf("resource %s not in repo", id)
	}
	return p
}

func (r *fakeRepo) has(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.records[id]
	return ok
}

func (r *fakeRepo) hasGroup(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.groups[id]
	return ok
}

// firstSelection always picks the first candidate, isolating manager mechanics
// from strategy behavior (which each consuming package's strategies cover).
type firstSelection struct{}

func (firstSelection) Select(candidates []res) (res, error) {
	return candidates[0], nil
}

// fixedPolicy returns a fixed decision and records what it was asked about.
type fixedPolicy struct {
	decision Decision
	taskID   string
	deleted  res
	calls    int
}

func (p *fixedPolicy) OnDeleted(ctx context.Context, taskID string, deleted res) Decision {
	p.calls++
	p.taskID = taskID
	p.deleted = deleted
	return p.decision
}

// A configOption adjusts the Config newTestManager builds, standing in for the
// functional options each consuming package wraps this layer in.
type configOption func(*Config[payload])

func withUsage(u UsagePolicy) configOption {
	return func(c *Config[payload]) { c.Usage = u }
}

func withStrategy(name string, f StrategyFactory[payload]) configOption {
	return func(c *Config[payload]) { c.Strategies[name] = f }
}

// testConfig registers the deterministic "first" strategy alongside the real
// default, so a group naming neither still resolves the way production does.
func testConfig(repo Repository[payload], policy DeletionPolicy[payload], opts ...configOption) Config[payload] {
	cfg := Config[payload]{
		Repository: repo,
		Policy:     policy,
		Strategies: map[string]StrategyFactory[payload]{
			"first":            func() Selection[payload] { return firstSelection{} },
			StrategyRoundRobin: func() Selection[payload] { return NewRoundRobin[payload]() },
		},
		DefaultStrategy: StrategyRoundRobin,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// newTestManager seeds the global group with the deterministic "first"
// strategy and cap as its holder policy, then builds the manager.
func newTestManager(t *testing.T, repo Repository[payload], cap int, policy DeletionPolicy[payload], opts ...configOption) *Manager[payload] {
	t.Helper()
	if err := repo.SaveGroup(context.Background(), Group{ID: GlobalGroup, Strategy: "first", MaxHolders: cap}); err != nil {
		t.Fatalf("seed global group: %v", err)
	}
	return rebuildManager(t, repo, policy, opts...)
}

// rebuildManager builds a manager over a repository that already carries its
// groups, standing in for a restart.
func rebuildManager(t *testing.T, repo Repository[payload], policy DeletionPolicy[payload], opts ...configOption) *Manager[payload] {
	t.Helper()
	m, err := NewManager(context.Background(), testConfig(repo, policy, opts...))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

type acquireResult struct {
	lease *Lease[payload]
	err   error
}

// acquireAsync runs Acquire in a goroutine so tests can observe blocking.
func acquireAsync(ctx context.Context, m *Manager[payload], taskID, groupID string) chan acquireResult {
	ch := make(chan acquireResult, 1)
	go func() {
		l, err := m.Acquire(ctx, Assignment{TaskID: taskID, GroupID: groupID})
		ch <- acquireResult{l, err}
	}()
	return ch
}

func mustBlock(t *testing.T, ch chan acquireResult) {
	t.Helper()
	select {
	case res := <-ch:
		t.Fatalf("expected acquire to block, got lease=%+v err=%v", res.lease, res.err)
	case <-time.After(50 * time.Millisecond):
	}
}

func mustComplete(t *testing.T, ch chan acquireResult) acquireResult {
	t.Helper()
	select {
	case res := <-ch:
		return res
	case <-time.After(2 * time.Second):
		t.Fatal("acquire did not complete")
		return acquireResult{}
	}
}

// TestAcquireEmptyPool verifies an empty group fails immediately with
// ErrNoResources rather than blocking, because waiting can never be satisfied
// with nothing to rotate.
func TestAcquireEmptyPool(t *testing.T) {
	m := newTestManager(t, newFakeRepo(), 1, nil)
	if _, err := m.Acquire(context.Background(), Assignment{TaskID: "t1"}); !errors.Is(err, ErrNoResources) {
		t.Fatalf("err = %v, want ErrNoResources", err)
	}
}

// TestAcquireUnknownGroup verifies acquiring from a group the manager does not
// know fails with ErrGroupNotFound instead of blocking or falling back.
func TestAcquireUnknownGroup(t *testing.T) {
	m := newTestManager(t, newFakeRepo(res{ID: "p1"}), 1, nil)
	if _, err := m.Acquire(context.Background(), Assignment{TaskID: "t1", GroupID: "missing"}); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
}

// TestNewManagerRejectsInvalidHolderPolicy verifies a holder policy below
// UnlimitedHolders fails loud at construction, on groups and resources alike.
func TestNewManagerRejectsInvalidHolderPolicy(t *testing.T) {
	repo := newFakeRepo()
	repo.SaveGroup(context.Background(), Group{ID: "g", MaxHolders: -2})
	if _, err := NewManager(context.Background(), testConfig(repo, nil)); err == nil {
		t.Fatal("expected error for group MaxHolders -2")
	}

	repo = newFakeRepo(res{ID: "p1", MaxHolders: -2})
	if _, err := NewManager(context.Background(), testConfig(repo, nil)); err == nil {
		t.Fatal("expected error for resource MaxHolders -2")
	}
}

// TestNewManagerRejectsUnknownStrategy verifies a group referencing an
// unregistered strategy fails at construction, because its members could
// never be selected.
func TestNewManagerRejectsUnknownStrategy(t *testing.T) {
	repo := newFakeRepo()
	repo.SaveGroup(context.Background(), Group{ID: "g", Strategy: "nope"})
	if _, err := NewManager(context.Background(), testConfig(repo, nil)); err == nil {
		t.Fatal("expected error for unknown strategy")
	}
}

// TestNewManagerRejectsUnknownResourceGroup verifies a resource referencing a group
// that does not exist fails at construction rather than rotating nowhere.
func TestNewManagerRejectsUnknownResourceGroup(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ghost"})
	if _, err := NewManager(context.Background(), testConfig(repo, nil)); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
}

// TestNewManagerPersistsGlobalGroup verifies the global group is materialized
// durably when absent, so ungrouped resources always have a namespace to land in.
func TestNewManagerPersistsGlobalGroup(t *testing.T) {
	repo := newFakeRepo()
	if _, err := NewManager(context.Background(), testConfig(repo, nil)); err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if !repo.hasGroup(GlobalGroup) {
		t.Fatal("global group not persisted")
	}
	g := repo.groups[GlobalGroup]
	if g.CreatedAt.IsZero() || g.UpdatedAt.IsZero() {
		t.Fatal("global group timestamps not stamped")
	}
}

// TestNewManagerRejectsDoubleBinding verifies a repo claiming one task owns two
// resources is rejected, because the lock contract is at most one resource per task.
func TestNewManagerRejectsDoubleBinding(t *testing.T) {
	repo := newFakeRepo(
		res{ID: "p1", OwnerID: "t1"},
		res{ID: "p2", OwnerID: "t1"},
	)
	if _, err := NewManager(context.Background(), testConfig(repo, nil)); err == nil {
		t.Fatal("expected error for double binding")
	}
}

// TestExclusiveBlocksUntilRelease verifies a second task cannot lease a held
// resource until it is released, because the default holder policy is one at a time.
func TestExclusiveBlocksUntilRelease(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", Attrs: payload{secret: "s1"}})
	m := newTestManager(t, repo, 1, nil)

	lease, err := m.Acquire(context.Background(), Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ch := acquireAsync(context.Background(), m, "t2", "")
	mustBlock(t, ch)

	if err := lease.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}
	res := mustComplete(t, ch)
	if res.err != nil {
		t.Fatalf("second acquire: %v", res.err)
	}
	if res.lease.Resource().ID != "p1" {
		t.Fatalf("got %s, want p1", res.lease.Resource().ID)
	}
}

// TestGroupCapAllowsConcurrentHolders verifies a group holder policy of 2
// admits two concurrent leases and blocks the third, because the cap bounds
// concurrent use per resource.
func TestGroupCapAllowsConcurrentHolders(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	m := newTestManager(t, repo, 2, nil)
	ctx := context.Background()

	l1, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t2"}); err != nil {
		t.Fatalf("second acquire under cap: %v", err)
	}

	ch := acquireAsync(ctx, m, "t3", "")
	mustBlock(t, ch)

	if err := l1.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}
	if res := mustComplete(t, ch); res.err != nil {
		t.Fatalf("third acquire after release: %v", res.err)
	}
}

// TestResourceCapOverridesGroupCap verifies a resource's own holder policy wins over
// its group's, in both directions.
func TestResourceCapOverridesGroupCap(t *testing.T) {
	// group tolerates 2, resource insists on 1: second acquire blocks.
	repo := newFakeRepo(res{ID: "p1", MaxHolders: 1})
	m := newTestManager(t, repo, 2, nil)
	ctx := context.Background()

	if _, err := m.Acquire(ctx, Assignment{TaskID: "t1"}); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	mustBlock(t, acquireAsync(ctx, m, "t2", ""))

	// group tolerates 1, resource tolerates 2: second acquire succeeds.
	repo = newFakeRepo(res{ID: "p1", MaxHolders: 2})
	m = newTestManager(t, repo, 1, nil)
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t1"}); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t2"}); err != nil {
		t.Fatalf("second acquire under resource cap: %v", err)
	}
}

// TestUnlimitedHolders verifies UnlimitedHolders admits arbitrarily many
// concurrent leases on one resource.
func TestUnlimitedHolders(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", MaxHolders: UnlimitedHolders})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	for i := range 25 {
		if _, err := m.Acquire(ctx, Assignment{TaskID: "t"}); err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
	}
}

// TestAcquireScopedToGroup verifies rotation never crosses group boundaries:
// a group whose only resource is busy blocks even while another group has idle
// resources.
func TestAcquireScopedToGroup(t *testing.T) {
	repo := newFakeRepo(
		res{ID: "a1", GroupID: "ga"},
		res{ID: "b1", GroupID: "gb"},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: "first"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("acquire from ga: %v", err)
	}
	if l.Resource().ID != "a1" {
		t.Fatalf("acquired %s, want a1", l.Resource().ID)
	}

	// ga exhausted: blocks despite b1 idling in gb.
	mustBlock(t, acquireAsync(ctx, m, "t2", "ga"))

	// gb unaffected.
	got, err := m.Acquire(ctx, Assignment{TaskID: "t3", GroupID: "gb"})
	if err != nil {
		t.Fatalf("acquire from gb: %v", err)
	}
	if got.Resource().ID != "b1" {
		t.Fatalf("acquired %s, want b1", got.Resource().ID)
	}
}

// TestPerGroupStrategyState verifies each group runs its own strategy
// instance: one group's rotation must not advance another's cursor.
func TestPerGroupStrategyState(t *testing.T) {
	repo := newFakeRepo(
		res{ID: "a1", GroupID: "ga", MaxHolders: UnlimitedHolders},
		res{ID: "a2", GroupID: "ga", MaxHolders: UnlimitedHolders},
		res{ID: "b1", GroupID: "gb", MaxHolders: UnlimitedHolders},
		res{ID: "b2", GroupID: "gb", MaxHolders: UnlimitedHolders},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: StrategyRoundRobin})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: StrategyRoundRobin})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	first, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("acquire ga: %v", err)
	}
	if first.Resource().ID != "a1" {
		t.Fatalf("ga first pick = %s, want a1", first.Resource().ID)
	}
	// If cursors were shared, ga's acquire above would push gb's pick to b2.
	got, err := m.Acquire(ctx, Assignment{TaskID: "t2", GroupID: "gb"})
	if err != nil {
		t.Fatalf("acquire gb: %v", err)
	}
	if got.Resource().ID != "b1" {
		t.Fatalf("gb first pick = %s, want its own cursor's b1", got.Resource().ID)
	}
}

// TestAcquireHonorsContextCancel verifies a blocked Acquire returns the
// context's error on cancellation, because blocking must always be escapable.
func TestAcquireHonorsContextCancel(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)

	if _, err := m.Acquire(context.Background(), Assignment{TaskID: "t1"}); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := acquireAsync(ctx, m, "t2", "")
	mustBlock(t, ch)

	cancel()
	res := mustComplete(t, ch)
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", res.err)
	}
}

// TestReleaseRecordsOutcomeAndPersists verifies Release(success) feeds the
// stats bayesian selection learns from and writes them through to the repo so
// learning survives restarts, refreshing UpdatedAt as it goes.
func TestReleaseRecordsOutcomeAndPersists(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l1, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l1.Release(true); err != nil {
		t.Fatalf("release success: %v", err)
	}
	l2, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l2.Release(false); err != nil {
		t.Fatalf("release failure: %v", err)
	}

	p := repo.get(t, "p1")
	if p.Successes != 1 || p.Failures != 1 {
		t.Fatalf("persisted stats = %d/%d, want 1/1", p.Successes, p.Failures)
	}
	if p.UpdatedAt.IsZero() {
		t.Fatal("release did not refresh UpdatedAt")
	}
}

// TestDoubleReleaseFreesOnce verifies a second Release does not free a slot it
// no longer holds, or a later acquire could over-admit past the cap.
func TestDoubleReleaseFreesOnce(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l.Release(true); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := l.Release(true); err != nil {
		t.Fatalf("second release should be a no-op, got %v", err)
	}

	if _, err := m.Acquire(ctx, Assignment{TaskID: "t2"}); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	// if the double release had leaked a slot, this would succeed instead of block.
	ch := acquireAsync(ctx, m, "t3", "")
	mustBlock(t, ch)
}

// TestLockExcludesResourceFromRotation verifies a locked resource can never be leased
// by another task even while idle, because the lock is owner-exclusive past runtime.
func TestLockExcludesResourceFromRotation(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)

	l, err := m.Lock(context.Background(), Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := l.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := acquireAsync(ctx, m, "t2", "")
	mustBlock(t, ch)
	cancel()
	if res := mustComplete(t, ch); !errors.Is(res.err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", res.err)
	}
}

// TestLockPersistsBinding verifies the lock lands in the repo as OwnerID,
// because the binding must be durable past the task's runtime.
func TestLockPersistsBinding(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)

	l, err := m.Lock(context.Background(), Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if owner := repo.get(t, "p1").OwnerID; owner != "t1" {
		t.Fatalf("persisted OwnerID = %q, want t1", owner)
	}
}

// TestAcquireReturnsLockedResource verifies the owner's Acquire always returns its
// locked resource rather than rotating, because reuse is the point of locking.
func TestAcquireReturnsLockedResource(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"}, res{ID: "p2"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if l.Resource().ID != "p1" {
		t.Fatalf("locked %s, want p1", l.Resource().ID)
	}
	l.Release(true)

	got, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Resource().ID != "p1" {
		t.Fatalf("owner acquired %s, want its locked p1", got.Resource().ID)
	}
}

// TestLockIdempotent verifies a second Lock returns the existing binding
// instead of binding a second resource.
func TestLockIdempotent(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"}, res{ID: "p2"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l1, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	l1.Release(true)

	l2, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("second lock: %v", err)
	}
	if l2.Resource().ID != "p1" {
		t.Fatalf("second lock got %s, want p1", l2.Resource().ID)
	}
	if owner := repo.get(t, "p2").OwnerID; owner != "" {
		t.Fatalf("p2 OwnerID = %q, want unbound", owner)
	}
}

// TestReclaimAcrossRestart verifies a manager rebuilt from the same repo hands
// the owner its locked resource back, because the binding's durability is the
// requirement locking exists for.
func TestReclaimAcrossRestart(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"}, res{ID: "p2"})
	ctx := context.Background()

	m1 := newTestManager(t, repo, 1, nil)
	l, err := m1.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	m2, err := NewManager(ctx, testConfig(repo, nil))
	if err != nil {
		t.Fatalf("NewManager after restart: %v", err)
	}
	got, err := m2.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire after restart: %v", err)
	}
	if got.Resource().ID != "p1" {
		t.Fatalf("reclaimed %s, want p1", got.Resource().ID)
	}
}

// TestUnlockReturnsResourceToPool verifies Unlock clears the durable binding so
// other tasks can rotate onto the resource again.
func TestUnlockReturnsResourceToPool(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.Unlock(ctx, "t1"); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if owner := repo.get(t, "p1").OwnerID; owner != "" {
		t.Fatalf("persisted OwnerID = %q, want cleared", owner)
	}

	got, err := m.Acquire(ctx, Assignment{TaskID: "t2"})
	if err != nil {
		t.Fatalf("acquire after unlock: %v", err)
	}
	if got.Resource().ID != "p1" {
		t.Fatalf("acquired %s, want p1", got.Resource().ID)
	}
}

// TestUnlockWithoutBinding verifies Unlock for an unbound task is a no-op, so
// callers can unlock defensively.
func TestUnlockWithoutBinding(t *testing.T) {
	m := newTestManager(t, newFakeRepo(res{ID: "p1"}), 1, nil)
	if err := m.Unlock(context.Background(), "t1"); err != nil {
		t.Fatalf("unlock without binding: %v", err)
	}
}

// TestDeleteUnlockedResource verifies deleting an unbound resource removes it without
// consulting the policy, because no task's fate is in question.
func TestDeleteUnlockedResource(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"}, res{ID: "p2"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	if err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if policy.calls != 0 {
		t.Fatalf("policy consulted %d times for unbound resource, want 0", policy.calls)
	}
	if repo.has("p1") {
		t.Fatal("p1 still in repo after delete")
	}
	got, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Resource().ID != "p2" {
		t.Fatalf("acquired %s, want p2", got.Resource().ID)
	}
}

// TestDeleteLockedResourceReassign verifies the Reassign decision durably rebinds
// the orphaned task to a freshly selected resource from the same group, even
// though the task may be offline.
func TestDeleteLockedResourceReassign(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"}, res{ID: "p2"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if policy.calls != 1 || policy.taskID != "t1" || policy.deleted.ID != "p1" {
		t.Fatalf("policy saw calls=%d task=%q deleted=%q, want 1/t1/p1", policy.calls, policy.taskID, policy.deleted.ID)
	}
	if owner := repo.get(t, "p2").OwnerID; owner != "t1" {
		t.Fatalf("p2 OwnerID = %q, want rebound to t1", owner)
	}
	if repo.has("p1") {
		t.Fatal("p1 still in repo after delete")
	}

	got, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Resource().ID != "p2" {
		t.Fatalf("owner acquired %s, want reassigned p2", got.Resource().ID)
	}
}

// TestDeleteLockedResourceReassignStaysInGroup verifies reassignment selects from
// the deleted resource's own group, never a stranger's.
func TestDeleteLockedResourceReassignStaysInGroup(t *testing.T) {
	repo := newFakeRepo(
		res{ID: "a1", GroupID: "ga"},
		res{ID: "a2", GroupID: "ga"},
		res{ID: "b1", GroupID: "gb"},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: "first"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.Delete(ctx, "a1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if owner := repo.get(t, "a2").OwnerID; owner != "t1" {
		t.Fatalf("a2 OwnerID = %q, want rebound to t1", owner)
	}
	if owner := repo.get(t, "b1").OwnerID; owner != "" {
		t.Fatalf("b1 OwnerID = %q, want untouched", owner)
	}
}

// TestDeleteLockedResourceUnbind verifies the Unbind decision returns the task to
// the rotating pool with no replacement binding.
func TestDeleteLockedResourceUnbind(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"}, res{ID: "p2"})
	policy := &fixedPolicy{decision: Unbind}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if owner := repo.get(t, "p2").OwnerID; owner != "" {
		t.Fatalf("p2 OwnerID = %q, want unbound", owner)
	}

	got, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Resource().ID != "p2" {
		t.Fatalf("acquired %s, want rotation onto p2", got.Resource().ID)
	}
}

// TestDeleteLockedResourceFail verifies the Fail decision surfaces ErrTaskOrphaned
// naming the task, so the deleter can kill or quarantine it.
func TestDeleteLockedResourceFail(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	policy := &fixedPolicy{decision: Fail}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	err = m.Delete(ctx, "p1")
	if !errors.Is(err, ErrTaskOrphaned) {
		t.Fatalf("err = %v, want ErrTaskOrphaned", err)
	}
	if !strings.Contains(err.Error(), "t1") {
		t.Fatalf("error %q does not name the orphaned task", err)
	}
	if repo.has("p1") {
		t.Fatal("p1 still in repo after delete")
	}
}

// TestDeleteLockedResourceWithoutPolicy verifies deleting a locked resource with no
// policy wired fails loud and leaves the resource intact, because the framework
// must not orphan a task silently.
func TestDeleteLockedResourceWithoutPolicy(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.Delete(ctx, "p1"); err == nil {
		t.Fatal("expected error deleting locked resource without a policy")
	}
	if !repo.has("p1") {
		t.Fatal("p1 deleted despite missing policy")
	}
}

// TestAdd verifies Add defaults the group to global, stamps
// timestamps, persists, and makes the resource immediately acquirable.
func TestAdd(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	if err := m.Add(ctx, res{ID: "p1", Attrs: payload{secret: "s1"}}); err != nil {
		t.Fatalf("add: %v", err)
	}
	stored := repo.get(t, "p1")
	if stored.GroupID != GlobalGroup {
		t.Fatalf("GroupID = %q, want global", stored.GroupID)
	}
	if stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() {
		t.Fatal("timestamps not stamped")
	}
	if stored.Attrs != (payload{secret: "s1"}) {
		t.Fatalf("Attrs = %+v, want the payload handed in verbatim", stored.Attrs)
	}

	got, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Resource().ID != "p1" {
		t.Fatalf("acquired %s, want p1", got.Resource().ID)
	}
}

// TestAddRejectsUnknownGroupAndDuplicates verifies AddProxy fails loud on
// a group the manager does not know and on an id already pooled.
func TestAddRejectsUnknownGroupAndDuplicates(t *testing.T) {
	m := newTestManager(t, newFakeRepo(res{ID: "p1"}), 1, nil)
	ctx := context.Background()

	if err := m.Add(ctx, res{ID: "p2", GroupID: "ghost"}); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
	if err := m.Add(ctx, res{ID: "p1"}); err == nil {
		t.Fatal("expected error for duplicate id")
	}
}

// TestCreateGroup verifies CreateGroup persists the group with timestamps and
// makes it immediately usable, and refuses duplicates and unknown strategies.
func TestCreateGroup(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	if err := m.CreateGroup(ctx, Group{ID: "g", Strategy: StrategyRoundRobin}); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if !repo.hasGroup("g") {
		t.Fatal("group not persisted")
	}
	if g := repo.groups["g"]; g.CreatedAt.IsZero() || g.UpdatedAt.IsZero() {
		t.Fatal("timestamps not stamped")
	}

	if err := m.Add(ctx, res{ID: "p1", GroupID: "g"}); err != nil {
		t.Fatalf("add to new group: %v", err)
	}
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "g"}); err != nil {
		t.Fatalf("acquire from new group: %v", err)
	}

	if err := m.CreateGroup(ctx, Group{ID: "g"}); err == nil {
		t.Fatal("expected error for duplicate group")
	}
	if err := m.CreateGroup(ctx, Group{ID: "g2", Strategy: "nope"}); err == nil {
		t.Fatal("expected error for unknown strategy")
	}
}

// TestDeleteGroupCascades verifies DeleteGroup removes the group and all its
// resources, freeing durable locks via the policy and surfacing Fail decisions
// as ErrTaskOrphaned.
func TestDeleteGroupCascades(t *testing.T) {
	repo := newFakeRepo(
		res{ID: "a1", GroupID: "ga"},
		res{ID: "a2", GroupID: "ga"},
		res{ID: "b1", GroupID: "gb"},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: "first"})
	policy := &fixedPolicy{decision: Fail}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	err = m.DeleteGroup(ctx, "ga")
	if !errors.Is(err, ErrTaskOrphaned) {
		t.Fatalf("err = %v, want ErrTaskOrphaned surfaced from cascade", err)
	}
	if repo.has("a1") || repo.has("a2") {
		t.Fatal("member resources survived group delete")
	}
	if repo.hasGroup("ga") {
		t.Fatal("group row survived delete")
	}
	if !repo.has("b1") || !repo.hasGroup("gb") {
		t.Fatal("cascade leaked into another group")
	}

	// the freed task rotates again: its durable lock died with the group.
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "gb"}); err != nil {
		t.Fatalf("acquire after cascade: %v", err)
	}
}

// TestDeleteGroupUnbindFreesLocksSilently verifies an Unbind policy cascades
// with no error: locks freed, resources gone.
func TestDeleteGroupUnbindFreesLocksSilently(t *testing.T) {
	repo := newFakeRepo(res{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	policy := &fixedPolicy{decision: Unbind}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.DeleteGroup(ctx, "ga"); err != nil {
		t.Fatalf("delete group: %v", err)
	}
	if repo.has("a1") || repo.hasGroup("ga") {
		t.Fatal("group or member survived delete")
	}
}

// TestDeleteGroupRefusals verifies the global group cannot be deleted, an
// unknown group fails with ErrGroupNotFound, and locked members without a
// policy refuse before mutating anything.
func TestDeleteGroupRefusals(t *testing.T) {
	repo := newFakeRepo(res{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	if err := m.DeleteGroup(ctx, GlobalGroup); err == nil {
		t.Fatal("expected refusal for global group")
	}
	if err := m.DeleteGroup(ctx, "ghost"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}

	l, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)
	if err := m.DeleteGroup(ctx, "ga"); err == nil {
		t.Fatal("expected refusal: locked member and no policy")
	}
	if !repo.has("a1") || !repo.hasGroup("ga") {
		t.Fatal("refused delete mutated state")
	}
}

// fixedUsage reports a canned set of running tasks, standing in for the task
// service the manager consults before deleting a resource or a group. running
// maps a resource group to the tasks rotating through it; live names the tasks
// that answer the per-task half of the guard; pinned maps a resource to the tasks
// whose records name it.
type fixedUsage struct {
	running map[string][]string
	live    map[string]bool
	pinned  map[string][]string
	err     error
	asked   []string
	askedID []string
}

func (u *fixedUsage) RunningTasks(ctx context.Context, groupID string) ([]string, error) {
	u.asked = append(u.asked, groupID)
	if u.err != nil {
		return nil, u.err
	}
	return u.running[groupID], nil
}

func (u *fixedUsage) PinnedTasks(ctx context.Context, resourceID string) ([]string, error) {
	if u.err != nil {
		return nil, u.err
	}
	return u.pinned[resourceID], nil
}

func (u *fixedUsage) TaskIsRunning(ctx context.Context, taskID string) (bool, error) {
	u.askedID = append(u.askedID, taskID)
	if u.err != nil {
		return false, u.err
	}
	return u.live[taskID], nil
}

// TestDeleteGroupRefusesGroupInUse verifies a group a running task leases from
// cannot be deleted: pulling the pool out from under a live run would strand
// its in-flight requests. The refusal must land before anything is mutated,
// and must name the tasks blocking it.
func TestDeleteGroupRefusesGroupInUse(t *testing.T) {
	repo := newFakeRepo(res{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{running: map[string][]string{"ga": {"t1", "t2"}}}
	m, err := NewManager(context.Background(), testConfig(repo, nil, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	ctx := context.Background()

	err = m.DeleteGroup(ctx, "ga")
	if !errors.Is(err, ErrGroupInUse) {
		t.Fatalf("err = %v, want ErrGroupInUse", err)
	}
	if !strings.Contains(err.Error(), "t1") || !strings.Contains(err.Error(), "t2") {
		t.Fatalf("error %q does not name the running tasks", err)
	}
	if !repo.has("a1") || !repo.hasGroup("ga") {
		t.Fatal("refused delete mutated state")
	}

	// Once the tasks stop, the same delete goes through.
	usage.running = nil
	if err := m.DeleteGroup(ctx, "ga"); err != nil {
		t.Fatalf("delete after tasks stopped: %v", err)
	}
	if repo.has("a1") || repo.hasGroup("ga") {
		t.Fatal("group or member survived delete")
	}
}

// TestDeleteGroupSurfacesUsageError verifies an unanswerable usage check
// refuses the delete rather than assuming the group is idle, because guessing
// wrong destroys a live pool.
func TestDeleteGroupSurfacesUsageError(t *testing.T) {
	repo := newFakeRepo(res{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{err: errors.New("task service down")}
	m, err := NewManager(context.Background(), testConfig(repo, nil, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if err := m.DeleteGroup(context.Background(), "ga"); err == nil {
		t.Fatal("expected the usage failure to refuse the delete")
	}
	if !repo.has("a1") || !repo.hasGroup("ga") {
		t.Fatal("failed usage check still mutated state")
	}
}

// TestDeleteGroupSkipsUsageCheckForGlobal verifies the global group's blanket
// refusal short-circuits ahead of the usage policy, so the guard is never
// consulted about a group that can never be deleted.
func TestDeleteGroupSkipsUsageCheckForGlobal(t *testing.T) {
	usage := &fixedUsage{}
	m, err := NewManager(context.Background(), testConfig(newFakeRepo(), nil, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if err := m.DeleteGroup(context.Background(), GlobalGroup); err == nil {
		t.Fatal("expected refusal for the global group")
	}
	if len(usage.asked) != 0 {
		t.Fatalf("usage policy consulted %v, want not at all", usage.asked)
	}
}

// TestBlockedAcquireFailsWhenGroupDeleted verifies a waiter blocked on a
// group's capacity is woken and failed when the group is deleted, instead of
// waiting forever on resources that no longer exist.
func TestBlockedAcquireFailsWhenGroupDeleted(t *testing.T) {
	repo := newFakeRepo(res{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	// A policy is wired because the holder t1 would otherwise block the delete:
	// with none, the guard cannot tell a live holder from a parked one and
	// refuses. Here it reports nothing running, the state of a suspended task.
	m := newTestManager(t, repo, 1, nil, withUsage(&fixedUsage{}))
	ctx := context.Background()

	if _, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga"}); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	ch := acquireAsync(ctx, m, "t2", "ga")
	mustBlock(t, ch)

	if err := m.DeleteGroup(ctx, "ga"); err != nil {
		t.Fatalf("delete group: %v", err)
	}
	res := mustComplete(t, ch)
	if !errors.Is(res.err, ErrGroupNotFound) && !errors.Is(res.err, ErrNoResources) {
		t.Fatalf("err = %v, want ErrGroupNotFound or ErrNoResources", res.err)
	}
}

// failingRepo fails the named operations, standing in for a store that is
// briefly unreachable mid-delete.
type failingRepo struct {
	*fakeRepo
	failDelete      map[string]bool
	failDeleteGroup bool
}

func (r *failingRepo) Delete(ctx context.Context, id string) error {
	if r.failDelete[id] {
		return errors.New("boom: transient store failure")
	}
	return r.fakeRepo.Delete(ctx, id)
}

func (r *failingRepo) DeleteGroup(ctx context.Context, id string) error {
	if r.failDeleteGroup {
		return errors.New("boom: transient store failure")
	}
	return r.fakeRepo.DeleteGroup(ctx, id)
}

// TestFailedRemoveLeavesStoreAndMemoryAgreeing verifies a resource whose store
// delete fails stays in the live pool. Dropping it from memory anyway would
// hide a row that the next NewManager reloads — and if its group row had been
// deleted meanwhile, that reload fails permanently, leaving a manager that
// cannot be constructed at all.
func TestFailedRemoveLeavesStoreAndMemoryAgreeing(t *testing.T) {
	base := newFakeRepo(res{ID: "a1", GroupID: "ga"}, res{ID: "a2", GroupID: "ga"})
	repo := &failingRepo{fakeRepo: base, failDelete: map[string]bool{"a2": true}}
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	if err := m.DeleteGroup(ctx, "ga"); err == nil {
		t.Fatal("expected the store failure to surface")
	}
	// a2 could not be deleted, so the group row must survive with it.
	if !repo.has("a2") {
		t.Fatal("a2 vanished from the store")
	}
	if !repo.hasGroup("ga") {
		t.Fatal("group row deleted while a member row survived: the next NewManager would fail on a2")
	}

	// The proof: a fresh manager over the same store still constructs.
	if _, err := NewManager(ctx, testConfig(repo, nil)); err != nil {
		t.Fatalf("manager unstartable after failed cascade: %v", err)
	}
	// And the surviving member is still leasable.
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga"}); err != nil {
		t.Fatalf("surviving member not leasable: %v", err)
	}
}

// TestFailedRemoveKeepsDurableLockVisible verifies an unbind whose store
// delete fails does not drop the binding in memory. The row still carries the
// OwnerID, so forgetting it here would let a restart resurrect a lock this
// deletion was meant to clear.
func TestFailedRemoveKeepsDurableLockVisible(t *testing.T) {
	base := newFakeRepo(res{ID: "p1"})
	repo := &failingRepo{fakeRepo: base, failDelete: map[string]bool{"p1": true}}
	policy := &fixedPolicy{decision: Unbind}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.Delete(ctx, "p1"); err == nil {
		t.Fatal("expected the store failure to surface")
	}
	if owner := repo.get(t, "p1").OwnerID; owner != "t1" {
		t.Fatalf("stored owner = %q, want t1 still locked", owner)
	}
	// Memory must still agree: the owner reclaims its resource, as a restart would.
	got, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("owner cannot reclaim its still-locked resource: %v", err)
	}
	if got.Resource().ID != "p1" {
		t.Fatalf("acquired %s, want p1", got.Resource().ID)
	}
}

// TestLockNeverStealsAResourceInUse verifies a lock only ever lands on an idle
// resource. Under a cap above 1 a resource can be under capacity while another task
// holds it; binding that one would both break the owner-exclusive contract and
// park the new owner behind a stranger on its own resource.
func TestLockNeverStealsAResourceInUse(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"})
	m := newTestManager(t, repo, 2, nil)
	ctx := context.Background()

	holder, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// p1 is under the cap of 2, but t1 holds it: the lock must wait, not steal.
	locked := make(chan acquireResult, 1)
	go func() {
		l, err := m.Lock(ctx, Assignment{TaskID: "t2"})
		locked <- acquireResult{l, err}
	}()
	select {
	case res := <-locked:
		t.Fatalf("Lock took a resource already held: lease=%+v err=%v", res.lease, res.err)
	case <-time.After(50 * time.Millisecond):
	}
	if owner := repo.get(t, "p1").OwnerID; owner != "" {
		t.Fatalf("persisted OwnerID = %q while another task held the resource", owner)
	}

	// Once the holder leaves, the lock lands and the owner can use it at once.
	holder.Release(true)
	res := mustComplete(t, locked)
	if res.err != nil {
		t.Fatalf("lock after release: %v", res.err)
	}
	if err := res.lease.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}
	owned, err := m.Acquire(ctx, Assignment{TaskID: "t2"})
	if err != nil {
		t.Fatalf("owner blocked on its own locked resource: %v", err)
	}
	if owned.Resource().ID != "p1" {
		t.Fatalf("owner acquired %s, want p1", owned.Resource().ID)
	}
}

// TestReassignNeverStealsAResourceInUse verifies reassignment picks an idle
// replacement, for the same reason a lock does — the orphaned task must not be
// rebound onto a resource someone else is mid-request on.
func TestReassignNeverStealsAResourceInUse(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1"}, res{ID: "p2"}, res{ID: "p3"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, 2, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)
	// Occupy p2 so only p3 is idle; firstSelection would otherwise pick p2.
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t2"}); err != nil {
		t.Fatalf("acquire p2: %v", err)
	}

	if err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if owner := repo.get(t, "p2").OwnerID; owner != "" {
		t.Fatalf("p2 was bound to %q while another task held it", owner)
	}
	if owner := repo.get(t, "p3").OwnerID; owner != "t1" {
		t.Fatalf("p3 OwnerID = %q, want the idle resource rebound to t1", owner)
	}
}

// strayCandidateSelection returns a resource outside the candidate set, modeling a
// buggy custom strategy.
type strayCandidateSelection struct{ stray res }

func (s strayCandidateSelection) Select(candidates []res) (res, error) {
	return s.stray, nil
}

// TestSelectionMustReturnACandidate verifies a strategy that returns a resource
// outside the candidates it was handed is rejected. Trusting it would hand out
// a resource already at capacity, or let a lock overwrite another task's binding.
func TestSelectionMustReturnACandidate(t *testing.T) {
	repo := newFakeRepo(
		res{ID: "free", GroupID: "ga"},
		res{ID: "taken", GroupID: "ga", OwnerID: "someone"},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "stray"})
	repo.SaveGroup(context.Background(), Group{ID: GlobalGroup, Strategy: "stray"})
	m, err := NewManager(context.Background(), testConfig(repo, nil,
		withStrategy("stray", func() Selection[payload] { return strayCandidateSelection{res{ID: "taken"}} })))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if _, err := m.Acquire(context.Background(), Assignment{TaskID: "t1", GroupID: "ga"}); err == nil {
		t.Fatal("expected an error for a selection outside the candidate set")
	}
	if owner := repo.get(t, "taken").OwnerID; owner != "someone" {
		t.Fatalf("taken OwnerID = %q, want its original owner untouched", owner)
	}
}

// gateRepo wraps fakeRepo to park exactly one armed Save until the gate opens,
// so a test can force a specific interleaving of concurrent saves.
type gateRepo struct {
	*fakeRepo
	arm     atomic.Bool
	entered chan struct{}
	gate    chan struct{}
}

func (r *gateRepo) Save(ctx context.Context, p res) error {
	if r.arm.CompareAndSwap(true, false) {
		close(r.entered)
		<-r.gate
	}
	return r.fakeRepo.Save(ctx, p)
}

// TestReleaseCannotResurrectStaleLock verifies Release persists atomically with
// its in-memory update. A Release whose save raced ahead of a concurrent
// Unlock's could land last with the stale owner still set, so a restart would
// load a durable lock no live task holds — the resource would be leased to a
// ghost forever.
func TestReleaseCannotResurrectStaleLock(t *testing.T) {
	repo := &gateRepo{
		fakeRepo: newFakeRepo(res{ID: "p1", Attrs: payload{secret: "s1"}}),
		entered:  make(chan struct{}),
		gate:     make(chan struct{}),
	}
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}

	// Arm the gate so Release's save parks mid-flight, then race an Unlock.
	repo.arm.Store(true)
	releaseDone := make(chan error, 1)
	go func() { releaseDone <- lease.Release(true) }()
	<-repo.entered

	unlockDone := make(chan error, 1)
	go func() { unlockDone <- m.Unlock(ctx, "t1") }()

	// Give a racy Unlock time to finish before the parked save lands; a correct
	// Release holds the manager lock across its save, so Unlock cannot pass it.
	var unlockErr error
	unlockFinished := false
	select {
	case unlockErr = <-unlockDone:
		unlockFinished = true
	case <-time.After(100 * time.Millisecond):
	}
	close(repo.gate)

	if err := <-releaseDone; err != nil {
		t.Fatalf("Release: %v", err)
	}
	if !unlockFinished {
		unlockErr = <-unlockDone
	}
	if unlockErr != nil {
		t.Fatalf("Unlock: %v", unlockErr)
	}

	stored := repo.get(t, "p1")
	if stored.OwnerID != "" {
		t.Fatalf("stored owner = %q, want unlocked: a stale Release save must not resurrect the lock", stored.OwnerID)
	}
	if stored.Successes != 1 {
		t.Fatalf("stored successes = %d, want 1: the release outcome must survive the unlock", stored.Successes)
	}
}

// TestDeleteRefusesLeaseHeldByRunningTask verifies a resource a running task
// is leasing right now cannot be deleted out from under it. Once that task
// stops advancing — suspended or dead — the same delete goes through, even
// though the lease object still exists: a parked task has no request in flight.
func TestDeleteRefusesLeaseHeldByRunningTask(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"}, res{ID: "p2", GroupID: "ga"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{live: map[string]bool{"t1": true}}
	m, err := NewManager(ctx, testConfig(repo, nil, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Resource().ID != "p1" {
		t.Fatalf("leased %s, want p1", lease.Resource().ID)
	}

	err = m.Delete(ctx, "p1")
	if !errors.Is(err, ErrResourceInUse) {
		t.Fatalf("err = %v, want ErrResourceInUse", err)
	}
	if !strings.Contains(err.Error(), "t1") {
		t.Fatalf("error %q does not name the holding task", err)
	}
	if !repo.has("p1") {
		t.Fatal("refused delete removed the resource anyway")
	}

	// An idle holder no longer blocks it: the guard reads liveness, not the lease.
	usage.live = nil
	if err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("delete once the holder stopped: %v", err)
	}
	if repo.has("p1") {
		t.Fatal("resource survived the delete")
	}
}

// TestDeleteRefusesLockHeldByRunningTask verifies a resource durably locked
// to a running task is refused even when that task runs against a different
// group — a binding outranks the group a task is assigned, so the group
// question alone would miss it. The refusal lands before the deletion policy
// runs, so the task's fate is never decided for a delete that does not happen.
func TestDeleteRefusesLockHeldByRunningTask(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga", OwnerID: "t1"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{live: map[string]bool{"t1": true}}
	policy := &fixedPolicy{decision: Unbind}
	m, err := NewManager(ctx, testConfig(repo, policy, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = m.Delete(ctx, "p1")
	if !errors.Is(err, ErrResourceInUse) {
		t.Fatalf("err = %v, want ErrResourceInUse", err)
	}
	if !strings.Contains(err.Error(), "t1") {
		t.Fatalf("error %q does not name the owning task", err)
	}
	if policy.calls != 0 {
		t.Fatalf("deletion policy consulted %d times on a refused delete", policy.calls)
	}
	if !repo.has("p1") {
		t.Fatal("refused delete removed the resource anyway")
	}

	usage.live = nil
	if err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("delete once the owner stopped: %v", err)
	}
	if policy.calls != 1 || policy.taskID != "t1" {
		t.Fatalf("policy calls = %d for %q, want 1 for t1", policy.calls, policy.taskID)
	}
}

// TestDeleteRefusesWhileGroupInUse verifies a resource cannot be pulled out
// of a group a running task rotates through, even one the task has not leased
// yet: its next acquire would otherwise find a pool that shrank mid-run.
func TestDeleteRefusesWhileGroupInUse(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"}, res{ID: "p2", GroupID: "gb"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(ctx, Group{ID: "gb", Strategy: "first"})
	usage := &fixedUsage{running: map[string][]string{"ga": {"t9"}}}
	m, err := NewManager(ctx, testConfig(repo, nil, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = m.Delete(ctx, "p1")
	if !errors.Is(err, ErrResourceInUse) {
		t.Fatalf("err = %v, want ErrResourceInUse", err)
	}
	if !strings.Contains(err.Error(), "t9") {
		t.Fatalf("error %q does not name the running task", err)
	}
	if !repo.has("p1") {
		t.Fatal("refused delete removed the resource anyway")
	}

	// A quiet group is untouched by the noisy one next door.
	if err := m.Delete(ctx, "p2"); err != nil {
		t.Fatalf("delete from an idle group: %v", err)
	}
}

// TestDeleteSurfacesUsageError verifies an unanswerable usage check
// refuses the delete rather than assuming the resource is idle, because guessing
// wrong strands a live run.
func TestDeleteSurfacesUsageError(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: GlobalGroup})
	ctx := context.Background()
	usage := &fixedUsage{err: errors.New("task service down")}
	m := newTestManager(t, repo, 1, nil, withUsage(usage))

	if err := m.Delete(ctx, "p1"); err == nil {
		t.Fatal("expected the usage failure to refuse the delete")
	}
	if !repo.has("p1") {
		t.Fatal("failed usage check still removed the resource")
	}
}

// TestDeleteWithoutUsagePolicyRefusesHeldResources verifies the guard falls back
// to the one fact the manager owns outright when no policy is wired: a resource
// with a live lease on it is not deleted. Of the two ways to be wrong here,
// refusing is reversible — wire the policy and retry — while deleting a resource
// out from under a request is not. An unheld resource is untouched by the
// fallback; there is nothing to be wrong about.
func TestDeleteWithoutUsagePolicyRefusesHeldResources(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: GlobalGroup}, res{ID: "p2", GroupID: GlobalGroup})
	ctx := context.Background()
	m := newTestManager(t, repo, 1, nil)

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: GlobalGroup})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Resource().ID != "p1" {
		t.Fatalf("leased %s, want p1", lease.Resource().ID)
	}

	err = m.Delete(ctx, "p1")
	if !errors.Is(err, ErrResourceInUse) {
		t.Fatalf("err = %v, want ErrResourceInUse", err)
	}
	if !strings.Contains(err.Error(), "t1") {
		t.Fatalf("error %q does not name the holder", err)
	}
	if !repo.has("p1") {
		t.Fatal("refused delete removed the resource anyway")
	}

	// Nobody holds p2, so the fallback has nothing to protect.
	if err := m.Delete(ctx, "p2"); err != nil {
		t.Fatalf("Delete of an unheld resource: %v", err)
	}

	// Releasing frees it, and the same delete then goes through.
	if err := lease.Release(true); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("Delete after release: %v", err)
	}
}

// TestUsagePolicyLetsAParkedHolderGo verifies wiring the policy is what buys
// back the precision the fallback lacks: a suspended task still holds its
// lease, but the policy can say it is not running, so its resource is deletable.
// That is the escape hatch a refusal points at, and it exists only with a
// policy wired.
func TestUsagePolicyLetsAParkedHolderGo(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: GlobalGroup})
	ctx := context.Background()
	usage := &fixedUsage{live: map[string]bool{"t1": true}}
	m := newTestManager(t, repo, 1, nil, withUsage(usage))

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: GlobalGroup})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := m.Delete(ctx, "p1"); !errors.Is(err, ErrResourceInUse) {
		t.Fatalf("err = %v, want ErrResourceInUse while t1 runs", err)
	}

	// t1 suspends: the lease is still held, but nothing is in flight.
	usage.live = nil
	if err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("Delete once the holder parked: %v", err)
	}
	if err := lease.Release(true); err != nil {
		t.Fatalf("Release of a deleted resource: %v", err)
	}
}

// TestDeleteGroupRefusesMemberLockedToRunningTask verifies the cascade cannot
// be used to sidestep the per-resource guard: a member locked to a task running
// against some other group blocks the whole group delete.
func TestDeleteGroupRefusesMemberLockedToRunningTask(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga", OwnerID: "t1"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{live: map[string]bool{"t1": true}}
	policy := &fixedPolicy{decision: Unbind}
	m, err := NewManager(ctx, testConfig(repo, policy, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	err = m.DeleteGroup(ctx, "ga")
	if !errors.Is(err, ErrGroupInUse) {
		t.Fatalf("err = %v, want ErrGroupInUse", err)
	}
	if policy.calls != 0 {
		t.Fatalf("deletion policy consulted %d times on a refused cascade", policy.calls)
	}
	if !repo.has("p1") || !repo.hasGroup("ga") {
		t.Fatal("refused cascade mutated state")
	}

	usage.live = nil
	if err := m.DeleteGroup(ctx, "ga"); err != nil {
		t.Fatalf("cascade once the owner stopped: %v", err)
	}
}

// TestDeleteAsksEachHolderOnce verifies every task holding a shared resource
// is checked, and each only once — the answer is memoized per delete so a busy
// pool does not hammer the task service.
func TestDeleteAsksEachHolderOnce(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga", MaxHolders: UnlimitedHolders})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{live: map[string]bool{"t2": true}}
	m, err := NewManager(ctx, testConfig(repo, nil, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	for _, taskID := range []string{"t1", "t2"} {
		if _, err := m.Acquire(ctx, Assignment{TaskID: taskID, GroupID: "ga"}); err != nil {
			t.Fatalf("Acquire for %s: %v", taskID, err)
		}
	}

	err = m.Delete(ctx, "p1")
	if !errors.Is(err, ErrResourceInUse) {
		t.Fatalf("err = %v, want ErrResourceInUse: t2 is still running", err)
	}
	if !strings.Contains(err.Error(), "t2") {
		t.Fatalf("error %q does not name the running holder", err)
	}
	if len(usage.askedID) != 2 {
		t.Fatalf("asked about %v, want each holder once up to the refusal", usage.askedID)
	}
}

// TestPinnedAssignmentLeasesOnlyItsResource verifies a pin is a group assignment
// minus the rotation: every lease is the pinned resource, however many members the
// group has.
func TestPinnedAssignmentLeasesOnlyItsResource(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"}, res{ID: "p2", GroupID: "ga"}, res{ID: "p3", GroupID: "ga"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: StrategyRoundRobin})
	m, err := NewManager(ctx, testConfig(repo, nil))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	pinned := Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "p2"}
	for i := range 3 {
		lease, err := m.Acquire(ctx, pinned)
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		if lease.Resource().ID != "p2" {
			t.Fatalf("lease %d = %s, want p2 every time: a pin does not rotate", i, lease.Resource().ID)
		}
		if err := lease.Release(true); err != nil {
			t.Fatalf("Release %d: %v", i, err)
		}
	}
}

// TestPinnedAssignmentLeavesRotationUntouched verifies a pin bypasses the
// group's strategy rather than passing through it with one candidate: advancing
// a shared round-robin cursor for a choice nobody made would skew what the
// group's other tasks get next.
func TestPinnedAssignmentLeavesRotationUntouched(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"}, res{ID: "p2", GroupID: "ga"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: StrategyRoundRobin})
	m, err := NewManager(ctx, testConfig(repo, nil))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	pin, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "p2"})
	if err != nil {
		t.Fatalf("pinned Acquire: %v", err)
	}
	pin.Release(true)

	rotating, err := m.Acquire(ctx, Assignment{TaskID: "t2", GroupID: "ga"})
	if err != nil {
		t.Fatalf("rotating Acquire: %v", err)
	}
	if rotating.Resource().ID != "p1" {
		t.Fatalf("rotating lease = %s, want p1: the pin advanced the group cursor", rotating.Resource().ID)
	}
}

// TestPinRefusesUnresolvableResource verifies a pin that no longer resolves is
// reported rather than quietly degraded to rotation — running on some other
// resource is exactly what a pinned task did not ask for. The distinct errors are
// what a recovered task's fallback policy branches on.
func TestPinRefusesUnresolvableResource(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"}, res{ID: "q1", GroupID: "gb"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(ctx, Group{ID: "gb", Strategy: "first"})
	m, err := NewManager(ctx, testConfig(repo, nil))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	_, err = m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "gone"})
	if !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("err = %v, want ErrResourceNotFound", err)
	}

	// A pin outside its assigned group is a misconfiguration, not a fallback:
	// resolving it either way would silently move the task off one or the other.
	_, err = m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "q1"})
	if !errors.Is(err, ErrResourceNotInGroup) {
		t.Fatalf("err = %v, want ErrResourceNotInGroup", err)
	}
}

// TestPinConflictWithDurableLock verifies a lease never resolves a pin that
// disagrees with an existing lock. Dropping a durable lock is a deliberate act,
// so acquire reports it and ReleaseStaleLock — the reassignment path — is what
// clears it.
func TestPinConflictWithDurableLock(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"}, res{ID: "p2", GroupID: "ga"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	m, err := NewManager(ctx, testConfig(repo, nil))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	lease, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if lease.Resource().ID != "p1" {
		t.Fatalf("locked %s, want p1", lease.Resource().ID)
	}
	lease.Release(true)

	repinned := Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "p2"}
	_, err = m.Acquire(ctx, repinned)
	if !errors.Is(err, ErrPinConflict) {
		t.Fatalf("err = %v, want ErrPinConflict", err)
	}
	if repo.get(t, "p1").OwnerID != "t1" {
		t.Fatal("a refused lease dropped the durable lock")
	}

	// The reassignment resolves it, and the next lease lands on the pin.
	if err := m.ReleaseStaleLock(ctx, repinned); err != nil {
		t.Fatalf("ReleaseStaleLock: %v", err)
	}
	if repo.get(t, "p1").OwnerID != "" {
		t.Fatal("stale lock survived the reassignment")
	}
	got, err := m.Acquire(ctx, repinned)
	if err != nil {
		t.Fatalf("Acquire after reassignment: %v", err)
	}
	if got.Resource().ID != "p2" {
		t.Fatalf("lease = %s, want the pinned p2", got.Resource().ID)
	}
}

// TestReleaseStaleLockKeepsLocksThatStillFit verifies the reassignment path is
// not a blanket unlock: repointing a task at the resource it already holds, or at
// the group that resource is in, keeps the lock. Dropping it would return the
// resource to the pool for another task to take between the release and the
// task's next lock.
func TestReleaseStaleLockKeepsLocksThatStillFit(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"}, res{ID: "q1", GroupID: "gb"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(ctx, Group{ID: "gb", Strategy: "first"})
	m, err := NewManager(ctx, testConfig(repo, nil))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	lease.Release(true)

	kept := []struct {
		name string
		a    Assignment
	}{
		{"pinned to the resource it holds", Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "p1"}},
		{"still in the locked resource's group", Assignment{TaskID: "t1", GroupID: "ga"}},
	}
	for _, tc := range kept {
		if err := m.ReleaseStaleLock(ctx, tc.a); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if repo.get(t, "p1").OwnerID != "t1" {
			t.Fatalf("%s: lock dropped though the placement still fits", tc.name)
		}
	}

	// Moving the task to another group no longer fits, so the lock goes.
	if err := m.ReleaseStaleLock(ctx, Assignment{TaskID: "t1", GroupID: "gb"}); err != nil {
		t.Fatalf("ReleaseStaleLock to another group: %v", err)
	}
	if repo.get(t, "p1").OwnerID != "" {
		t.Fatal("lock survived a move to another group")
	}
}

// TestReleaseStaleLockOnEmptyPlacementReleases verifies a task reassigned to no
// resources at all loses its lock. Unlike Acquire, an empty group here means none
// rather than the global one — a proxyless task must not keep a resource bound.
func TestReleaseStaleLockOnEmptyPlacementReleases(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: GlobalGroup})
	ctx := context.Background()
	m := newTestManager(t, repo, 1, nil)
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	lease.Release(true)

	if err := m.ReleaseStaleLock(ctx, Assignment{TaskID: "t1"}); err != nil {
		t.Fatalf("ReleaseStaleLock: %v", err)
	}
	if repo.get(t, "p1").OwnerID != "" {
		t.Fatal("lock survived a reassignment to no resources")
	}
}

// TestPinRefusesResourceLockedToAnotherTask verifies a pin on a resource another task
// durably owns fails instead of blocking. The acquire loop waits out a lease
// because a lease ends on its own; a durable lock does not, so waiting on one
// is waiting on a condition that never arrives.
func TestPinRefusesResourceLockedToAnotherTask(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga", OwnerID: "t2"}, res{ID: "p2", GroupID: "ga"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	m, err := NewManager(ctx, testConfig(repo, nil))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "p1"})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrResourceLocked) {
			t.Fatalf("err = %v, want ErrResourceLocked", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire blocked on a pin no release can satisfy")
	}

	// The owner itself is not shut out of its own locked resource.
	lease, err := m.Acquire(ctx, Assignment{TaskID: "t2", GroupID: "ga", ResourceID: "p1"})
	if err != nil {
		t.Fatalf("owner Acquire: %v", err)
	}
	if lease.Resource().ID != "p1" {
		t.Fatalf("owner leased %s, want p1", lease.Resource().ID)
	}
}

// TestEmptyGroupFailsRatherThanWaiting verifies an assignment to a group with
// no members at all fails immediately. A group whose resources are merely busy is
// worth blocking on — one will free — but an empty group is a misconfiguration
// far more often than a swap in progress, and blocking would hide it as a hang.
func TestEmptyGroupFailsRatherThanWaiting(t *testing.T) {
	repo := newFakeRepo()
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	m, err := NewManager(ctx, testConfig(repo, nil))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrNoResources) {
			t.Fatalf("err = %v, want ErrNoResources", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Acquire blocked on an empty group instead of failing")
	}
}

// TestCheckAssignmentReportsEveryWayAPlacementCanFail verifies the question a
// recovering task's fallback policy asks answers with the same rule the acquire
// loop applies, so a policy cannot be told a placement resolves and then have
// the lease disagree.
func TestCheckAssignmentReportsEveryWayAPlacementCanFail(t *testing.T) {
	repo := newFakeRepo(
		res{ID: "p1", GroupID: "ga"},
		res{ID: "p2", GroupID: "ga", OwnerID: "t9"},
		res{ID: "q1", GroupID: "gb"},
	)
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(ctx, Group{ID: "gb", Strategy: "first"})
	m, err := NewManager(ctx, testConfig(repo, nil))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	for _, tc := range []struct {
		name string
		a    Assignment
		want error
	}{
		{"resolves", Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "p1"}, nil},
		{"unpinned resolves", Assignment{TaskID: "t1", GroupID: "ga"}, nil},
		{"group gone", Assignment{TaskID: "t1", GroupID: "missing"}, ErrGroupNotFound},
		{"pin gone", Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "gone"}, ErrResourceNotFound},
		{"pin elsewhere", Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "q1"}, ErrResourceNotInGroup},
		{"pin owned", Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "p2"}, ErrResourceLocked},
		{"owner's own pin", Assignment{TaskID: "t9", GroupID: "ga", ResourceID: "p2"}, nil},
	} {
		err := m.CheckAssignment(tc.a)
		if tc.want == nil && err != nil {
			t.Fatalf("%s: CheckAssignment = %v, want nil", tc.name, err)
		}
		if tc.want != nil && !errors.Is(err, tc.want) {
			t.Fatalf("%s: CheckAssignment = %v, want %v", tc.name, err, tc.want)
		}
	}
}

// TestDeletionImpactWarnsWithoutRefusing verifies the query that makes a
// deletion deliberate: it names the running tasks that would refuse it and the
// pinned tasks that would be stranded by it, changes nothing, and leaves the
// decision to the caller — Delete still enforces only the running half.
func TestDeletionImpactWarnsWithoutRefusing(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"}, res{ID: "p2", GroupID: "ga"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{pinned: map[string][]string{"p1": {"t5", "t4"}}}
	m, err := NewManager(ctx, testConfig(repo, nil, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	impact, err := m.DeletionImpact(ctx, "p1")
	if err != nil {
		t.Fatalf("DeletionImpact: %v", err)
	}
	if len(impact.Running) != 0 {
		t.Fatalf("Running = %v, want none", impact.Running)
	}
	if len(impact.Pinned) != 2 || impact.Pinned[0] != "t4" || impact.Pinned[1] != "t5" {
		t.Fatalf("Pinned = %v, want sorted [t4 t5]", impact.Pinned)
	}
	if !repo.has("p1") {
		t.Fatal("the impact query deleted something")
	}

	// A pin is a warning, not a refusal: the deletion still goes through.
	if err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("Delete despite pinned tasks: %v", err)
	}

	// A resource nobody is linked to reports nothing.
	impact, err = m.DeletionImpact(ctx, "p2")
	if err != nil {
		t.Fatalf("DeletionImpact p2: %v", err)
	}
	if !impact.Empty() {
		t.Fatalf("impact = %+v, want empty", impact)
	}
}

// TestDeletionImpactCountsARunningTaskOnce verifies a task that is both running
// and pinned is reported only as running: it is the stronger finding, and
// listing it twice would overstate what the deletion costs.
func TestDeletionImpactCountsARunningTaskOnce(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{
		running: map[string][]string{"ga": {"t1"}},
		pinned:  map[string][]string{"p1": {"t1", "t2"}},
	}
	m, err := NewManager(ctx, testConfig(repo, nil, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	impact, err := m.DeletionImpact(ctx, "p1")
	if err != nil {
		t.Fatalf("DeletionImpact: %v", err)
	}
	if len(impact.Running) != 1 || impact.Running[0] != "t1" {
		t.Fatalf("Running = %v, want [t1]", impact.Running)
	}
	if len(impact.Pinned) != 1 || impact.Pinned[0] != "t2" {
		t.Fatalf("Pinned = %v, want [t2]: t1 is already counted as running", impact.Pinned)
	}
}

// TestGroupDeletionImpactPoolsItsMembers verifies the cascade's warning covers
// every member, so deleting a group cannot strand a pinned task more quietly
// than deleting that task's resource directly would.
func TestGroupDeletionImpactPoolsItsMembers(t *testing.T) {
	repo := newFakeRepo(res{ID: "p1", GroupID: "ga"}, res{ID: "p2", GroupID: "ga"}, res{ID: "q1", GroupID: "gb"})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(ctx, Group{ID: "gb", Strategy: "first"})
	usage := &fixedUsage{pinned: map[string][]string{"p1": {"t1"}, "p2": {"t2"}, "q1": {"t3"}}}
	m, err := NewManager(ctx, testConfig(repo, nil, withUsage(usage)))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	impact, err := m.GroupDeletionImpact(ctx, "ga")
	if err != nil {
		t.Fatalf("GroupDeletionImpact: %v", err)
	}
	if len(impact.Pinned) != 2 || impact.Pinned[0] != "t1" || impact.Pinned[1] != "t2" {
		t.Fatalf("Pinned = %v, want [t1 t2]: t3 is pinned in another group", impact.Pinned)
	}

	if _, err := m.GroupDeletionImpact(ctx, "missing"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
}
