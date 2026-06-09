package proxies

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeRepo is an in-memory Repository recording saves so tests can assert
// persistence without sqlite.
type fakeRepo struct {
	mu      sync.Mutex
	order   []string
	records map[string]Proxy
}

func newFakeRepo(seed ...Proxy) *fakeRepo {
	r := &fakeRepo{records: map[string]Proxy{}}
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

// firstSelection always picks the first candidate, isolating manager mechanics
// from strategy behavior (which the strategy tests cover).
type firstSelection struct{}

func (firstSelection) Select(candidates []Proxy) (Proxy, error) {
	return candidates[0], nil
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

func newTestManager(t *testing.T, repo Repository, excl Exclusivity, policy DeletionPolicy) *Manager {
	t.Helper()
	m, err := NewManager(context.Background(), repo, firstSelection{}, excl, policy)
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
func acquireAsync(ctx context.Context, m *Manager, taskID string) chan acquireResult {
	ch := make(chan acquireResult, 1)
	go func() {
		l, err := m.Acquire(ctx, taskID)
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

// TestAcquireEmptyPool verifies an empty pool fails immediately with ErrNoProxies
// rather than blocking, because waiting can never be satisfied with nothing to rotate.
func TestAcquireEmptyPool(t *testing.T) {
	m := newTestManager(t, newFakeRepo(), Exclusive(), nil)
	if _, err := m.Acquire(context.Background(), "t1"); !errors.Is(err, ErrNoProxies) {
		t.Fatalf("err = %v, want ErrNoProxies", err)
	}
}

// TestNewManagerRejectsInvalidCap verifies a capacity below 1 fails loud at
// construction, because it would silently deadlock every Acquire.
func TestNewManagerRejectsInvalidCap(t *testing.T) {
	if _, err := NewManager(context.Background(), newFakeRepo(), firstSelection{}, Capped(0), nil); err == nil {
		t.Fatal("expected error for Capped(0)")
	}
}

// TestNewManagerRejectsDoubleBinding verifies a repo claiming one task owns two
// proxies is rejected, because the lock contract is at most one proxy per task.
func TestNewManagerRejectsDoubleBinding(t *testing.T) {
	repo := newFakeRepo(
		Proxy{ID: "p1", OwnerID: "t1"},
		Proxy{ID: "p2", OwnerID: "t1"},
	)
	if _, err := NewManager(context.Background(), repo, firstSelection{}, Exclusive(), nil); err == nil {
		t.Fatal("expected error for double binding")
	}
}

// TestExclusiveBlocksUntilRelease verifies a second task cannot lease an
// exclusively held proxy until it is released, because Exclusive guarantees one
// holder at a time.
func TestExclusiveBlocksUntilRelease(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1", URL: "http://p1"})
	m := newTestManager(t, repo, Exclusive(), nil)

	lease, err := m.Acquire(context.Background(), "t1")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ch := acquireAsync(context.Background(), m, "t2")
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

// TestCappedAllowsConcurrentHolders verifies Capped(2) admits two concurrent
// leases and blocks the third, because the cap bounds concurrent use per proxy.
func TestCappedAllowsConcurrentHolders(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, Capped(2), nil)
	ctx := context.Background()

	l1, err := m.Acquire(ctx, "t1")
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	if _, err := m.Acquire(ctx, "t2"); err != nil {
		t.Fatalf("second acquire under cap: %v", err)
	}

	ch := acquireAsync(ctx, m, "t3")
	mustBlock(t, ch)

	if err := l1.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}
	if res := mustComplete(t, ch); res.err != nil {
		t.Fatalf("third acquire after release: %v", res.err)
	}
}

// TestAcquireHonorsContextCancel verifies a blocked Acquire returns the
// context's error on cancellation, because blocking must always be escapable.
func TestAcquireHonorsContextCancel(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, Exclusive(), nil)

	if _, err := m.Acquire(context.Background(), "t1"); err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := acquireAsync(ctx, m, "t2")
	mustBlock(t, ch)

	cancel()
	res := mustComplete(t, ch)
	if !errors.Is(res.err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", res.err)
	}
}

// TestReleaseRecordsOutcomeAndPersists verifies Release(success) feeds the
// stats bayesian selection learns from and writes them through to the repo so
// learning survives restarts.
func TestReleaseRecordsOutcomeAndPersists(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, Exclusive(), nil)
	ctx := context.Background()

	l1, err := m.Acquire(ctx, "t1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l1.Release(true); err != nil {
		t.Fatalf("release success: %v", err)
	}
	l2, err := m.Acquire(ctx, "t1")
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
}

// TestDoubleReleaseFreesOnce verifies a second Release does not free a slot it
// no longer holds, or a later acquire could over-admit past the cap.
func TestDoubleReleaseFreesOnce(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, Exclusive(), nil)
	ctx := context.Background()

	l, err := m.Acquire(ctx, "t1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l.Release(true); err != nil {
		t.Fatalf("first release: %v", err)
	}
	if err := l.Release(true); err != nil {
		t.Fatalf("second release should be a no-op, got %v", err)
	}

	if _, err := m.Acquire(ctx, "t2"); err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	// if the double release had leaked a slot, this would succeed instead of block.
	ch := acquireAsync(ctx, m, "t3")
	mustBlock(t, ch)
}

// TestLockExcludesProxyFromRotation verifies a locked proxy can never be leased
// by another task even while idle, because the lock is owner-exclusive past runtime.
func TestLockExcludesProxyFromRotation(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"})
	m := newTestManager(t, repo, Exclusive(), nil)

	l, err := m.Lock(context.Background(), "t1")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := l.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := acquireAsync(ctx, m, "t2")
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
	m := newTestManager(t, repo, Exclusive(), nil)

	l, err := m.Lock(context.Background(), "t1")
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
	m := newTestManager(t, repo, Exclusive(), nil)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if l.Proxy().ID != "p1" {
		t.Fatalf("locked %s, want p1", l.Proxy().ID)
	}
	l.Release(true)

	got, err := m.Acquire(ctx, "t1")
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
	m := newTestManager(t, repo, Exclusive(), nil)
	ctx := context.Background()

	l1, err := m.Lock(ctx, "t1")
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	l1.Release(true)

	l2, err := m.Lock(ctx, "t1")
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

	m1 := newTestManager(t, repo, Exclusive(), nil)
	l, err := m1.Lock(ctx, "t1")
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release(true)

	m2 := newTestManager(t, repo, Exclusive(), nil)
	got, err := m2.Acquire(ctx, "t1")
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
	m := newTestManager(t, repo, Exclusive(), nil)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1")
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

	got, err := m.Acquire(ctx, "t2")
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
	m := newTestManager(t, newFakeRepo(Proxy{ID: "p1"}), Exclusive(), nil)
	if err := m.Unlock(context.Background(), "t1"); err != nil {
		t.Fatalf("unlock without binding: %v", err)
	}
}

// TestDeleteUnlockedProxy verifies deleting an unbound proxy removes it without
// consulting the policy, because no task's fate is in question.
func TestDeleteUnlockedProxy(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, Exclusive(), policy)
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
	got, err := m.Acquire(ctx, "t1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Proxy().ID != "p2" {
		t.Fatalf("acquired %s, want p2", got.Proxy().ID)
	}
}

// TestDeleteLockedProxyReassign verifies the Reassign decision durably rebinds
// the orphaned task to a freshly selected proxy, even though the task may be offline.
func TestDeleteLockedProxyReassign(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"})
	policy := &fixedPolicy{decision: Reassign}
	m := newTestManager(t, repo, Exclusive(), policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1")
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

	got, err := m.Acquire(ctx, "t1")
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Proxy().ID != "p2" {
		t.Fatalf("owner acquired %s, want reassigned p2", got.Proxy().ID)
	}
}

// TestDeleteLockedProxyUnbind verifies the Unbind decision returns the task to
// the rotating pool with no replacement binding.
func TestDeleteLockedProxyUnbind(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1"}, Proxy{ID: "p2"})
	policy := &fixedPolicy{decision: Unbind}
	m := newTestManager(t, repo, Exclusive(), policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1")
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

	got, err := m.Acquire(ctx, "t1")
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
	m := newTestManager(t, repo, Exclusive(), policy)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1")
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
	m := newTestManager(t, repo, Exclusive(), nil)
	ctx := context.Background()

	l, err := m.Lock(ctx, "t1")
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
