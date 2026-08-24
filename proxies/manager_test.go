package proxies

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRepo is an in-memory Repository recording saves so tests can assert
// persistence without sqlite.
type fakeRepo struct {
	mu         sync.Mutex
	order      []string
	records    map[string]Proxy
	groupOrder []string
	groups     map[string]Group
}

func newFakeRepo(seed ...Proxy) *fakeRepo {
	r := &fakeRepo{records: map[string]Proxy{}, groups: map[string]Group{}}
	for _, p := range seed {
		r.records[p.ID] = p
		r.order = append(r.order, p.ID)
	}
	return r
}

func (r *fakeRepo) List(ctx context.Context) ([]Proxy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Proxy, 0, len(r.order))
	for _, id := range r.order {
		if p, ok := r.records[id]; ok {
			out = append(out, p)
		}
	}
	return out, nil
}

func (r *fakeRepo) Save(ctx context.Context, p Proxy) error {
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

func (r *fakeRepo) get(t *testing.T, id string) Proxy {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	p, ok := r.records[id]
	if !ok {
		t.Fatalf("proxy %s not in repo", id)
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
// from strategy behavior (which the strategy tests cover).
type firstSelection struct{}

func (firstSelection) Select(candidates []Proxy) (Proxy, error) {
	return candidates[0], nil
}

// withFirst registers the deterministic test strategy under "first".
func withFirst() ManagerOption {
	return WithStrategy("first", func() Selection { return firstSelection{} })
}

// fixedPolicy returns a fixed decision and records what it was asked about.
type fixedPolicy struct {
	decision Decision
	taskID   string
	deleted  Proxy
	calls    int
}

func (p *fixedPolicy) OnProxyDeleted(ctx context.Context, taskID string, deleted Proxy) Decision {
	p.calls++
	p.taskID = taskID
	p.deleted = deleted
	return p.decision
}

// newTestManager seeds the global group with the deterministic "first"
// strategy and cap as its holder policy, then builds the manager.
func newTestManager(t *testing.T, repo Repository, cap int, policy DeletionPolicy) *Manager {
	t.Helper()
	if err := repo.SaveGroup(context.Background(), Group{ID: GlobalGroup, Strategy: "first", MaxHolders: cap}); err != nil {
		t.Fatalf("seed global group: %v", err)
	}
	m, err := NewManager(context.Background(), repo, policy, withFirst())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

type acquireResult struct {
	lease *Lease
	err   error
}

// acquireAsync runs Acquire in a goroutine so tests can observe blocking.
func acquireAsync(ctx context.Context, m *Manager, taskID, groupID string) chan acquireResult {
	ch := make(chan acquireResult, 1)
	go func() {
		l, err := m.Acquire(ctx, taskID, groupID)
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
// ErrNoProxies rather than blocking, because waiting can never be satisfied
// with nothing to rotate.
func TestAcquireEmptyPool(t *testing.T) {
	m := newTestManager(t, newFakeRepo(), 1, nil)
	if _, err := m.Acquire(context.Background(), "t1", ""); !errors.Is(err, ErrNoProxies) {
		t.Fatalf("err = %v, want ErrNoProxies", err)
	}
}

// TestAcquireUnknownGroup verifies acquiring from a group the manager does not
// know fails with ErrGroupNotFound instead of blocking or falling back.
func TestAcquireUnknownGroup(t *testing.T) {
	m := newTestManager(t, newFakeRepo(Proxy{ID: "p1"}), 1, nil)
	if _, err := m.Acquire(context.Background(), "t1", "missing"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
}

// TestNewManagerRejectsInvalidHolderPolicy verifies a holder policy below
// UnlimitedHolders fails loud at construction, on groups and proxies alike.
func TestNewManagerRejectsInvalidHolderPolicy(t *testing.T) {
	repo := newFakeRepo()
	repo.SaveGroup(context.Background(), Group{ID: "g", MaxHolders: -2})
	if _, err := NewManager(context.Background(), repo, nil); err == nil {
		t.Fatal("expected error for group MaxHolders -2")
	}

	repo = newFakeRepo(Proxy{ID: "p1", MaxHolders: -2})
	if _, err := NewManager(context.Background(), repo, nil); err == nil {
		t.Fatal("expected error for proxy MaxHolders -2")
	}
}

// TestNewManagerRejectsUnknownStrategy verifies a group referencing an
// unregistered strategy fails at construction, because its members could
// never be selected.
func TestNewManagerRejectsUnknownStrategy(t *testing.T) {
	repo := newFakeRepo()
	repo.SaveGroup(context.Background(), Group{ID: "g", Strategy: "nope"})
	if _, err := NewManager(context.Background(), repo, nil); err == nil {
		t.Fatal("expected error for unknown strategy")
	}
}

// TestNewManagerRejectsUnknownProxyGroup verifies a proxy referencing a group
// that does not exist fails at construction rather than rotating nowhere.
func TestNewManagerRejectsUnknownProxyGroup(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1", GroupID: "ghost"})
	if _, err := NewManager(context.Background(), repo, nil); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
}

// TestNewManagerPersistsGlobalGroup verifies the global group is materialized
// durably when absent, so ungrouped proxies always have a namespace to land in.
func TestNewManagerPersistsGlobalGroup(t *testing.T) {
	repo := newFakeRepo()
	if _, err := NewManager(context.Background(), repo, nil); err != nil {
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
// proxies is rejected, because the lock contract is at most one proxy per task.
func TestNewManagerRejectsDoubleBinding(t *testing.T) {
	repo := newFakeRepo(
		Proxy{ID: "p1", OwnerID: "t1"},
		Proxy{ID: "p2", OwnerID: "t1"},
	)
	if _, err := NewManager(context.Background(), repo, nil); err == nil {
		t.Fatal("expected error for double binding")
	}
}

// TestExclusiveBlocksUntilRelease verifies a second task cannot lease a held
// proxy until it is released, because the default holder policy is one at a time.
func TestExclusiveBlocksUntilRelease(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1", URL: "http://p1"})
	m := newTestManager(t, repo, 1, nil)

	lease, err := m.Acquire(context.Background(), "t1", "")
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
	if res.lease.Proxy().ID != "p1" {
		t.Fatalf("got %s, want p1", res.lease.Proxy().ID)
	}
}

// TestGroupCapAllowsConcurrentHolders verifies a group holder policy of 2
// admits two concurrent leases and blocks the third, because the cap bounds
// concurrent use per proxy.
func TestGroupCapAllowsConcurrentHolders(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, 2, nil)
	ctx := context.Background()

	l1, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := m.Acquire(ctx, "t2", ""); err != nil {
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

// TestProxyCapOverridesGroupCap verifies a proxy's own holder policy wins over
// its group's, in both directions.
func TestProxyCapOverridesGroupCap(t *testing.T) {
	// group tolerates 2, proxy insists on 1: second acquire blocks.
	repo := newFakeRepo(Proxy{ID: "p1", MaxHolders: 1})
	m := newTestManager(t, repo, 2, nil)
	ctx := context.Background()

	if _, err := m.Acquire(ctx, "t1", ""); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	mustBlock(t, acquireAsync(ctx, m, "t2", ""))

	// group tolerates 1, proxy tolerates 2: second acquire succeeds.
	repo = newFakeRepo(Proxy{ID: "p1", MaxHolders: 2})
	m = newTestManager(t, repo, 1, nil)
	if _, err := m.Acquire(ctx, "t1", ""); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := m.Acquire(ctx, "t2", ""); err != nil {
		t.Fatalf("second acquire under proxy cap: %v", err)
	}
}

// TestUnlimitedHolders verifies UnlimitedHolders admits arbitrarily many
// concurrent leases on one proxy.
func TestUnlimitedHolders(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1", MaxHolders: UnlimitedHolders})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	for i := range 25 {
		if _, err := m.Acquire(ctx, "t", ""); err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
	}
}

// TestAcquireScopedToGroup verifies rotation never crosses group boundaries:
// a group whose only proxy is busy blocks even while another group has idle
// proxies.
func TestAcquireScopedToGroup(t *testing.T) {
	repo := newFakeRepo(
		Proxy{ID: "a1", GroupID: "ga"},
		Proxy{ID: "b1", GroupID: "gb"},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: "first"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Acquire(ctx, "t1", "ga")
	if err != nil {
		t.Fatalf("acquire from ga: %v", err)
	}
	if l.Proxy().ID != "a1" {
		t.Fatalf("acquired %s, want a1", l.Proxy().ID)
	}

	// ga exhausted: blocks despite b1 idling in gb.
	mustBlock(t, acquireAsync(ctx, m, "t2", "ga"))

	// gb unaffected.
	got, err := m.Acquire(ctx, "t3", "gb")
	if err != nil {
		t.Fatalf("acquire from gb: %v", err)
	}
	if got.Proxy().ID != "b1" {
		t.Fatalf("acquired %s, want b1", got.Proxy().ID)
	}
}

// TestPerGroupStrategyState verifies each group runs its own strategy
// instance: one group's rotation must not advance another's cursor.
func TestPerGroupStrategyState(t *testing.T) {
	repo := newFakeRepo(
		Proxy{ID: "a1", GroupID: "ga", MaxHolders: UnlimitedHolders},
		Proxy{ID: "a2", GroupID: "ga", MaxHolders: UnlimitedHolders},
		Proxy{ID: "b1", GroupID: "gb", MaxHolders: UnlimitedHolders},
		Proxy{ID: "b2", GroupID: "gb", MaxHolders: UnlimitedHolders},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: StrategyRoundRobin})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: StrategyRoundRobin})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	first, err := m.Acquire(ctx, "t1", "ga")
	if err != nil {
		t.Fatalf("acquire ga: %v", err)
	}
	if first.Proxy().ID != "a1" {
		t.Fatalf("ga first pick = %s, want a1", first.Proxy().ID)
	}
	// If cursors were shared, ga's acquire above would push gb's pick to b2.
	got, err := m.Acquire(ctx, "t2", "gb")
	if err != nil {
		t.Fatalf("acquire gb: %v", err)
	}
	if got.Proxy().ID != "b1" {
		t.Fatalf("gb first pick = %s, want its own cursor's b1", got.Proxy().ID)
	}
}

// TestAcquireHonorsContextCancel verifies a blocked Acquire returns the
// context's error on cancellation, because blocking must always be escapable.
func TestAcquireHonorsContextCancel(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)

	if _, err := m.Acquire(context.Background(), "t1", ""); err != nil {
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
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l1, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l1.Release(true); err != nil {
		t.Fatalf("release success: %v", err)
	}
	l2, err := m.Acquire(ctx, "t1", "")
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
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l.Release(true); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := l.Release(true); err != nil {
		t.Fatalf("second release should be a no-op, got %v", err)
	}

	if _, err := m.Acquire(ctx, "t2", ""); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	// if the double release had leaked a slot, this would succeed instead of block.
	ch := acquireAsync(ctx, m, "t3", "")
	mustBlock(t, ch)
}

// TestLockExcludesProxyFromRotation verifies a locked proxy can never be leased
// by another task even while idle, because the lock is owner-exclusive past runtime.
func TestLockExcludesProxyFromRotation(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)

	l, err := m.Lock(context.Background(), "t1", "")
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
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)

	l, err := m.Lock(context.Background(), "t1", "")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if owner := repo.get(t, "p1").OwnerID; owner != "t1" {
		t.Fatalf("persisted OwnerID = %q, want t1", owner)
	}
}

// TestAcquireReturnsLockedProxy verifies the owner's Acquire always returns its
// locked proxy rather than rotating, because reuse is the point of locking.
func TestAcquireReturnsLockedProxy(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if l.Proxy().ID != "p1" {
		t.Fatalf("locked %s, want p1", l.Proxy().ID)
	}
	l.Release(true)

	got, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Proxy().ID != "p1" {
		t.Fatalf("owner acquired %s, want its locked p1", got.Proxy().ID)
	}
}

// TestLockIdempotent verifies a second Lock returns the existing binding
// instead of binding a second proxy.
func TestLockIdempotent(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l1, err := m.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	l1.Release(true)

	l2, err := m.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("second lock: %v", err)
	}
	if l2.Proxy().ID != "p1" {
		t.Fatalf("second lock got %s, want p1", l2.Proxy().ID)
	}
	if owner := repo.get(t, "p2").OwnerID; owner != "" {
		t.Fatalf("p2 OwnerID = %q, want unbound", owner)
	}
}

// TestReclaimAcrossRestart verifies a manager rebuilt from the same repo hands
// the owner its locked proxy back, because the binding's durability is the
// requirement locking exists for.
func TestReclaimAcrossRestart(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"})
	ctx := context.Background()

	m1 := newTestManager(t, repo, 1, nil)
	l, err := m1.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	m2, err := NewManager(ctx, repo, nil, withFirst())
	if err != nil {
		t.Fatalf("NewManager after restart: %v", err)
	}
	got, err := m2.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("acquire after restart: %v", err)
	}
	if got.Proxy().ID != "p1" {
		t.Fatalf("reclaimed %s, want p1", got.Proxy().ID)
	}
}

// TestUnlockReturnsProxyToPool verifies Unlock clears the durable binding so
// other tasks can rotate onto the proxy again.
func TestUnlockReturnsProxyToPool(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "")
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

	got, err := m.Acquire(ctx, "t2", "")
	if err != nil {
		t.Fatalf("acquire after unlock: %v", err)
	}
	if got.Proxy().ID != "p1" {
		t.Fatalf("acquired %s, want p1", got.Proxy().ID)
	}
}

// TestUnlockWithoutBinding verifies Unlock for an unbound task is a no-op, so
// callers can unlock defensively.
func TestUnlockWithoutBinding(t *testing.T) {
	m := newTestManager(t, newFakeRepo(Proxy{ID: "p1"}), 1, nil)
	if err := m.Unlock(context.Background(), "t1"); err != nil {
		t.Fatalf("unlock without binding: %v", err)
	}
}

// TestDeleteUnlockedProxy verifies deleting an unbound proxy removes it without
// consulting the policy, because no task's fate is in question.
func TestDeleteUnlockedProxy(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	if err := m.DeleteProxy(ctx, "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if policy.calls != 0 {
		t.Fatalf("policy consulted %d times for unbound proxy, want 0", policy.calls)
	}
	if repo.has("p1") {
		t.Fatal("p1 still in repo after delete")
	}
	got, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Proxy().ID != "p2" {
		t.Fatalf("acquired %s, want p2", got.Proxy().ID)
	}
}

// TestDeleteLockedProxyReassign verifies the Reassign decision durably rebinds
// the orphaned task to a freshly selected proxy from the same group, even
// though the task may be offline.
func TestDeleteLockedProxyReassign(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.DeleteProxy(ctx, "p1"); err != nil {
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

	got, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Proxy().ID != "p2" {
		t.Fatalf("owner acquired %s, want reassigned p2", got.Proxy().ID)
	}
}

// TestDeleteLockedProxyReassignStaysInGroup verifies reassignment selects from
// the deleted proxy's own group, never a stranger's.
func TestDeleteLockedProxyReassignStaysInGroup(t *testing.T) {
	repo := newFakeRepo(
		Proxy{ID: "a1", GroupID: "ga"},
		Proxy{ID: "a2", GroupID: "ga"},
		Proxy{ID: "b1", GroupID: "gb"},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: "first"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "ga")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.DeleteProxy(ctx, "a1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if owner := repo.get(t, "a2").OwnerID; owner != "t1" {
		t.Fatalf("a2 OwnerID = %q, want rebound to t1", owner)
	}
	if owner := repo.get(t, "b1").OwnerID; owner != "" {
		t.Fatalf("b1 OwnerID = %q, want untouched", owner)
	}
}

// TestDeleteLockedProxyUnbind verifies the Unbind decision returns the task to
// the rotating pool with no replacement binding.
func TestDeleteLockedProxyUnbind(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"})
	policy := &fixedPolicy{decision: Unbind}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.DeleteProxy(ctx, "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if owner := repo.get(t, "p2").OwnerID; owner != "" {
		t.Fatalf("p2 OwnerID = %q, want unbound", owner)
	}

	got, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Proxy().ID != "p2" {
		t.Fatalf("acquired %s, want rotation onto p2", got.Proxy().ID)
	}
}

// TestDeleteLockedProxyFail verifies the Fail decision surfaces ErrTaskOrphaned
// naming the task, so the deleter can kill or quarantine it.
func TestDeleteLockedProxyFail(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	policy := &fixedPolicy{decision: Fail}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	err = m.DeleteProxy(ctx, "p1")
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

// TestDeleteLockedProxyWithoutPolicy verifies deleting a locked proxy with no
// policy wired fails loud and leaves the proxy intact, because the framework
// must not orphan a task silently.
func TestDeleteLockedProxyWithoutPolicy(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.DeleteProxy(ctx, "p1"); err == nil {
		t.Fatal("expected error deleting locked proxy without a policy")
	}
	if !repo.has("p1") {
		t.Fatal("p1 deleted despite missing policy")
	}
}

// TestAddProxy verifies AddProxy defaults the group to global, stamps
// timestamps, persists, and makes the proxy immediately acquirable.
func TestAddProxy(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	if err := m.AddProxy(ctx, Proxy{ID: "p1", URL: "http://p1"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	stored := repo.get(t, "p1")
	if stored.GroupID != GlobalGroup {
		t.Fatalf("GroupID = %q, want global", stored.GroupID)
	}
	if stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() {
		t.Fatal("timestamps not stamped")
	}

	got, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Proxy().ID != "p1" {
		t.Fatalf("acquired %s, want p1", got.Proxy().ID)
	}
}

// TestAddProxyRejectsUnknownGroupAndDuplicates verifies AddProxy fails loud on
// a group the manager does not know and on an id already pooled.
func TestAddProxyRejectsUnknownGroupAndDuplicates(t *testing.T) {
	m := newTestManager(t, newFakeRepo(Proxy{ID: "p1"}), 1, nil)
	ctx := context.Background()

	if err := m.AddProxy(ctx, Proxy{ID: "p2", GroupID: "ghost"}); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
	if err := m.AddProxy(ctx, Proxy{ID: "p1"}); err == nil {
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

	if err := m.AddProxy(ctx, Proxy{ID: "p1", GroupID: "g"}); err != nil {
		t.Fatalf("add to new group: %v", err)
	}
	if _, err := m.Acquire(ctx, "t1", "g"); err != nil {
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
// proxies, freeing durable locks via the policy and surfacing Fail decisions
// as ErrTaskOrphaned.
func TestDeleteGroupCascades(t *testing.T) {
	repo := newFakeRepo(
		Proxy{ID: "a1", GroupID: "ga"},
		Proxy{ID: "a2", GroupID: "ga"},
		Proxy{ID: "b1", GroupID: "gb"},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: "first"})
	policy := &fixedPolicy{decision: Fail}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "ga")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	err = m.DeleteGroup(ctx, "ga")
	if !errors.Is(err, ErrTaskOrphaned) {
		t.Fatalf("err = %v, want ErrTaskOrphaned surfaced from cascade", err)
	}
	if repo.has("a1") || repo.has("a2") {
		t.Fatal("member proxies survived group delete")
	}
	if repo.hasGroup("ga") {
		t.Fatal("group row survived delete")
	}
	if !repo.has("b1") || !repo.hasGroup("gb") {
		t.Fatal("cascade leaked into another group")
	}

	// the freed task rotates again: its durable lock died with the group.
	if _, err := m.Acquire(ctx, "t1", "gb"); err != nil {
		t.Fatalf("acquire after cascade: %v", err)
	}
}

// TestDeleteGroupUnbindFreesLocksSilently verifies an Unbind policy cascades
// with no error: locks freed, proxies gone.
func TestDeleteGroupUnbindFreesLocksSilently(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	policy := &fixedPolicy{decision: Unbind}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "ga")
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
	repo := newFakeRepo(Proxy{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	if err := m.DeleteGroup(ctx, GlobalGroup); err == nil {
		t.Fatal("expected refusal for global group")
	}
	if err := m.DeleteGroup(ctx, "ghost"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}

	l, err := m.Lock(ctx, "t1", "ga")
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
// service the manager consults before deleting a group.
type fixedUsage struct {
	running map[string][]string
	err     error
	asked   []string
}

func (u *fixedUsage) RunningTasks(ctx context.Context, proxyGroupID string) ([]string, error) {
	u.asked = append(u.asked, proxyGroupID)
	if u.err != nil {
		return nil, u.err
	}
	return u.running[proxyGroupID], nil
}

// TestDeleteGroupRefusesGroupInUse verifies a group a running task leases from
// cannot be deleted: pulling the pool out from under a live run would strand
// its in-flight requests. The refusal must land before anything is mutated,
// and must name the tasks blocking it.
func TestDeleteGroupRefusesGroupInUse(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{running: map[string][]string{"ga": {"t1", "t2"}}}
	m, err := NewManager(context.Background(), repo, nil, withFirst(), WithUsagePolicy(usage))
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
	repo := newFakeRepo(Proxy{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	usage := &fixedUsage{err: errors.New("task service down")}
	m, err := NewManager(context.Background(), repo, nil, withFirst(), WithUsagePolicy(usage))
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
	m, err := NewManager(context.Background(), newFakeRepo(), nil, withFirst(), WithUsagePolicy(usage))
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
// waiting forever on proxies that no longer exist.
func TestBlockedAcquireFailsWhenGroupDeleted(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "a1", GroupID: "ga"})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	if _, err := m.Acquire(ctx, "t1", "ga"); err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	ch := acquireAsync(ctx, m, "t2", "ga")
	mustBlock(t, ch)

	if err := m.DeleteGroup(ctx, "ga"); err != nil {
		t.Fatalf("delete group: %v", err)
	}
	res := mustComplete(t, ch)
	if !errors.Is(res.err, ErrGroupNotFound) && !errors.Is(res.err, ErrNoProxies) {
		t.Fatalf("err = %v, want ErrGroupNotFound or ErrNoProxies", res.err)
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

// TestFailedRemoveLeavesStoreAndMemoryAgreeing verifies a proxy whose store
// delete fails stays in the live pool. Dropping it from memory anyway would
// hide a row that the next NewManager reloads — and if its group row had been
// deleted meanwhile, that reload fails permanently, leaving a manager that
// cannot be constructed at all.
func TestFailedRemoveLeavesStoreAndMemoryAgreeing(t *testing.T) {
	base := newFakeRepo(Proxy{ID: "a1", GroupID: "ga"}, Proxy{ID: "a2", GroupID: "ga"})
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
	if _, err := NewManager(ctx, repo, nil, withFirst()); err != nil {
		t.Fatalf("manager unstartable after failed cascade: %v", err)
	}
	// And the surviving member is still leasable.
	if _, err := m.Acquire(ctx, "t1", "ga"); err != nil {
		t.Fatalf("surviving member not leasable: %v", err)
	}
}

// TestFailedRemoveKeepsDurableLockVisible verifies an unbind whose store
// delete fails does not drop the binding in memory. The row still carries the
// OwnerID, so forgetting it here would let a restart resurrect a lock this
// deletion was meant to clear.
func TestFailedRemoveKeepsDurableLockVisible(t *testing.T) {
	base := newFakeRepo(Proxy{ID: "p1"})
	repo := &failingRepo{fakeRepo: base, failDelete: map[string]bool{"p1": true}}
	policy := &fixedPolicy{decision: Unbind}
	m := newTestManager(t, repo, 1, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	if err := m.DeleteProxy(ctx, "p1"); err == nil {
		t.Fatal("expected the store failure to surface")
	}
	if owner := repo.get(t, "p1").OwnerID; owner != "t1" {
		t.Fatalf("stored owner = %q, want t1 still locked", owner)
	}
	// Memory must still agree: the owner reclaims its proxy, as a restart would.
	got, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("owner cannot reclaim its still-locked proxy: %v", err)
	}
	if got.Proxy().ID != "p1" {
		t.Fatalf("acquired %s, want p1", got.Proxy().ID)
	}
}

// TestLockNeverStealsAProxyInUse verifies a lock only ever lands on an idle
// proxy. Under a cap above 1 a proxy can be under capacity while another task
// holds it; binding that one would both break the owner-exclusive contract and
// park the new owner behind a stranger on its own proxy.
func TestLockNeverStealsAProxyInUse(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, 2, nil)
	ctx := context.Background()

	holder, err := m.Acquire(ctx, "t1", "")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	// p1 is under the cap of 2, but t1 holds it: the lock must wait, not steal.
	locked := make(chan acquireResult, 1)
	go func() {
		l, err := m.Lock(ctx, "t2", "")
		locked <- acquireResult{l, err}
	}()
	select {
	case res := <-locked:
		t.Fatalf("Lock took a proxy already held: lease=%+v err=%v", res.lease, res.err)
	case <-time.After(50 * time.Millisecond):
	}
	if owner := repo.get(t, "p1").OwnerID; owner != "" {
		t.Fatalf("persisted OwnerID = %q while another task held the proxy", owner)
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
	owned, err := m.Acquire(ctx, "t2", "")
	if err != nil {
		t.Fatalf("owner blocked on its own locked proxy: %v", err)
	}
	if owned.Proxy().ID != "p1" {
		t.Fatalf("owner acquired %s, want p1", owned.Proxy().ID)
	}
}

// TestReassignNeverStealsAProxyInUse verifies reassignment picks an idle
// replacement, for the same reason a lock does — the orphaned task must not be
// rebound onto a proxy someone else is mid-request on.
func TestReassignNeverStealsAProxyInUse(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"}, Proxy{ID: "p3"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, 2, policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1", "")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)
	// Occupy p2 so only p3 is idle; firstSelection would otherwise pick p2.
	if _, err := m.Acquire(ctx, "t2", ""); err != nil {
		t.Fatalf("acquire p2: %v", err)
	}

	if err := m.DeleteProxy(ctx, "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if owner := repo.get(t, "p2").OwnerID; owner != "" {
		t.Fatalf("p2 was bound to %q while another task held it", owner)
	}
	if owner := repo.get(t, "p3").OwnerID; owner != "t1" {
		t.Fatalf("p3 OwnerID = %q, want the idle proxy rebound to t1", owner)
	}
}

// strayCandidateSelection returns a proxy outside the candidate set, modeling a
// buggy custom strategy.
type strayCandidateSelection struct{ stray Proxy }

func (s strayCandidateSelection) Select(candidates []Proxy) (Proxy, error) {
	return s.stray, nil
}

// TestSelectionMustReturnACandidate verifies a strategy that returns a proxy
// outside the candidates it was handed is rejected. Trusting it would hand out
// a proxy already at capacity, or let a lock overwrite another task's binding.
func TestSelectionMustReturnACandidate(t *testing.T) {
	repo := newFakeRepo(
		Proxy{ID: "free", GroupID: "ga"},
		Proxy{ID: "taken", GroupID: "ga", OwnerID: "someone"},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "stray"})
	repo.SaveGroup(context.Background(), Group{ID: GlobalGroup, Strategy: "stray"})
	m, err := NewManager(context.Background(), repo, nil,
		WithStrategy("stray", func() Selection { return strayCandidateSelection{Proxy{ID: "taken"}} }))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if _, err := m.Acquire(context.Background(), "t1", "ga"); err == nil {
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

func (r *gateRepo) Save(ctx context.Context, p Proxy) error {
	if r.arm.CompareAndSwap(true, false) {
		close(r.entered)
		<-r.gate
	}
	return r.fakeRepo.Save(ctx, p)
}

// TestReleaseCannotResurrectStaleLock verifies Release persists atomically with
// its in-memory update. A Release whose save raced ahead of a concurrent
// Unlock's could land last with the stale owner still set, so a restart would
// load a durable lock no live task holds — the proxy would be leased to a
// ghost forever.
func TestReleaseCannotResurrectStaleLock(t *testing.T) {
	repo := &gateRepo{
		fakeRepo: newFakeRepo(Proxy{ID: "p1", URL: "http://p1"}),
		entered:  make(chan struct{}),
		gate:     make(chan struct{}),
	}
	m := newTestManager(t, repo, 1, nil)
	ctx := context.Background()

	lease, err := m.Lock(ctx, "t1", "")
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
