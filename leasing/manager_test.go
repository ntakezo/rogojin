package leasing

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests cover the leasing layer itself — pooling, groups, holder caps,
// durable locks, pins, and the lease-guarded deletes. The layer is mechanism,
// not policy: it decides everything from the leases and locks it owns, so the
// tests never stand up a task service to answer for it.

// payload stands in for whatever a resource kind carries — a proxy's URL, an
// account's credentials. Most tests here never read it; the ones that do are
// checking the manager copies it without ever inspecting it.
type payload struct {
	secret string
	region string
}

// res is the model under test — the leasing core plus a payload, embedded the
// way a real kind embeds Resource — spelled short because it appears
// everywhere.
type res struct {
	Resource
	payload
}

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

// Save is a blind upsert that still hands back a bumped version: the manager
// tests exercise its flows, not the store's conditional-write rules, which
// storetest owns against the real adapters.
func (r *fakeRepo) Save(ctx context.Context, p res) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if kept, ok := r.records[p.ID]; ok {
		p.Version = kept.Version + 1
	} else {
		r.order = append(r.order, p.ID)
		p.Version = 1
	}
	r.records[p.ID] = p
	return p.Version, nil
}

func (r *fakeRepo) Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (Hold, error) {
	return Hold{ResourceID: resourceID, TaskID: taskID, Count: 1, ExpiresAt: time.Now().Add(ttl)}, nil
}
func (r *fakeRepo) ReleaseHold(ctx context.Context, resourceID, taskID string) error { return nil }
func (r *fakeRepo) RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	return nil
}
func (r *fakeRepo) ListHolds(ctx context.Context) ([]Hold, error)                  { return nil, nil }
func (r *fakeRepo) ClaimLock(ctx context.Context, resourceID, taskID string) error { return nil }
func (r *fakeRepo) ReleaseLock(ctx context.Context, resourceID, taskID string) error {
	return nil
}
func (r *fakeRepo) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return delta, nil
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

// withFirst registers the deterministic "first" strategy, so tests that need a
// predictable pick have one alongside the always-installed round robin.
func withFirst() Option[res, *res] {
	return WithStrategy("first", func() Selection[res] { return firstSelection{} })
}

// newTestManager seeds the global group with the deterministic "first"
// strategy, then builds the manager.
func newTestManager(t *testing.T, repo Repository[res], opts ...Option[res, *res]) *Manager[res, *res] {
	t.Helper()
	if err := repo.SaveGroup(context.Background(), Group{ID: GlobalGroup, Strategy: "first"}); err != nil {
		t.Fatalf("seed global group: %v", err)
	}
	return rebuildManager(t, repo, opts...)
}

// rebuildManager builds a manager over a repository that already carries its
// groups, standing in for a restart.
func rebuildManager(t *testing.T, repo Repository[res], opts ...Option[res, *res]) *Manager[res, *res] {
	t.Helper()
	m, err := NewManager(context.Background(), repo, append([]Option[res, *res]{withFirst()}, opts...)...)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

type acquireResult struct {
	lease *Lease[res, *res]
	err   error
}

// acquireAsync runs Acquire in a goroutine so tests can observe blocking.
func acquireAsync(ctx context.Context, m *Manager[res, *res], taskID, groupID string) chan acquireResult {
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
	m := newTestManager(t, newFakeRepo())
	if _, err := m.Acquire(context.Background(), Assignment{TaskID: "t1"}); !errors.Is(err, ErrNoResources) {
		t.Fatalf("err = %v, want ErrNoResources", err)
	}
}

// TestAcquireUnknownGroup verifies acquiring from a group the manager does not
// know fails with ErrGroupNotFound instead of blocking or falling back.
func TestAcquireUnknownGroup(t *testing.T) {
	m := newTestManager(t, newFakeRepo(res{Resource: Resource{ID: "p1"}}))
	if _, err := m.Acquire(context.Background(), Assignment{TaskID: "t1", GroupID: "missing"}); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
}

// TestNewManagerRejectsInvalidHolderPolicy verifies a holder policy below
// UnlimitedHolders fails loud at construction.
func TestNewManagerRejectsInvalidHolderPolicy(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", MaxHolders: -2}})
	if _, err := NewManager(context.Background(), repo); err == nil {
		t.Fatal("expected error for resource MaxHolders -2")
	}
}

// TestNewManagerRejectsUnknownStrategy verifies a group referencing an
// unregistered strategy fails at construction, because its members could
// never be selected.
func TestNewManagerRejectsUnknownStrategy(t *testing.T) {
	repo := newFakeRepo()
	repo.SaveGroup(context.Background(), Group{ID: "g", Strategy: "nope"})
	if _, err := NewManager(context.Background(), repo); err == nil {
		t.Fatal("expected error for unknown strategy")
	}
}

// TestNewManagerRejectsUnknownResourceGroup verifies a resource referencing a group
// that does not exist fails at construction rather than rotating nowhere.
func TestNewManagerRejectsUnknownResourceGroup(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", GroupID: "ghost"}})
	if _, err := NewManager(context.Background(), repo); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
}

// TestNewManagerPersistsGlobalGroup verifies the global group is materialized
// durably when absent, so ungrouped resources always have a namespace to land in.
func TestNewManagerPersistsGlobalGroup(t *testing.T) {
	repo := newFakeRepo()
	if _, err := NewManager(context.Background(), repo); err != nil {
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
		res{Resource: Resource{ID: "p1", OwnerID: "t1"}},
		res{Resource: Resource{ID: "p2", OwnerID: "t1"}},
	)
	if _, err := NewManager(context.Background(), repo); err == nil {
		t.Fatal("expected error for double binding")
	}
}

// TestExclusiveBlocksUntilRelease verifies a second task cannot lease a held
// resource until it is released, because the default holder policy is one at a time.
func TestExclusiveBlocksUntilRelease(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}, payload: payload{secret: "s1"}})
	m := newTestManager(t, repo)

	lease, err := m.Acquire(context.Background(), Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	ch := acquireAsync(context.Background(), m, "t2", "")
	mustBlock(t, ch)

	lease.Release()
	res := mustComplete(t, ch)
	if res.err != nil {
		t.Fatalf("second acquire: %v", res.err)
	}
	if res.lease.Resource().ID != "p1" {
		t.Fatalf("got %s, want p1", res.lease.Resource().ID)
	}
}

// TestResourceCapAllowsConcurrentHolders verifies a holder policy of 2 admits
// two concurrent leases and blocks the third, because the cap bounds concurrent
// use per resource.
func TestResourceCapAllowsConcurrentHolders(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", MaxHolders: 2}})
	m := newTestManager(t, repo)
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

	l1.Release()
	if res := mustComplete(t, ch); res.err != nil {
		t.Fatalf("third acquire after release: %v", res.err)
	}
}

// TestUnlimitedHolders verifies UnlimitedHolders admits arbitrarily many
// concurrent leases on one resource.
func TestUnlimitedHolders(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", MaxHolders: UnlimitedHolders}})
	m := newTestManager(t, repo)
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
		res{Resource: Resource{ID: "a1", GroupID: "ga"}},
		res{Resource: Resource{ID: "b1", GroupID: "gb"}},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: "first"})
	m := newTestManager(t, repo)
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
		res{Resource: Resource{ID: "a1", GroupID: "ga", MaxHolders: UnlimitedHolders}},
		res{Resource: Resource{ID: "a2", GroupID: "ga", MaxHolders: UnlimitedHolders}},
		res{Resource: Resource{ID: "b1", GroupID: "gb", MaxHolders: UnlimitedHolders}},
		res{Resource: Resource{ID: "b2", GroupID: "gb", MaxHolders: UnlimitedHolders}},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: StrategyRoundRobin})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: StrategyRoundRobin})
	m := newTestManager(t, repo)
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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}})
	m := newTestManager(t, repo)

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

// TestDoubleReleaseFreesOnce verifies a second Release does not free a slot it
// no longer holds, or a later acquire could over-admit past the cap.
func TestDoubleReleaseFreesOnce(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	l, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	l.Release()
	l.Release()

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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}})
	m := newTestManager(t, repo)

	l, err := m.Lock(context.Background(), Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release()

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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}})
	m := newTestManager(t, repo)

	l, err := m.Lock(context.Background(), Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release()

	if owner := repo.get(t, "p1").OwnerID; owner != "t1" {
		t.Fatalf("persisted OwnerID = %q, want t1", owner)
	}
}

// TestAcquireReturnsLockedResource verifies the owner's Acquire always returns its
// locked resource rather than rotating, because reuse is the point of locking.
func TestAcquireReturnsLockedResource(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}}, res{Resource: Resource{ID: "p2"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if l.Resource().ID != "p1" {
		t.Fatalf("locked %s, want p1", l.Resource().ID)
	}
	l.Release()

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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}}, res{Resource: Resource{ID: "p2"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	l1, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("first lock: %v", err)
	}
	l1.Release()

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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}}, res{Resource: Resource{ID: "p2"}})
	ctx := context.Background()

	m1 := newTestManager(t, repo)
	l, err := m1.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release()

	m2 := rebuildManager(t, repo)
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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release()

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
	m := newTestManager(t, newFakeRepo(res{Resource: Resource{ID: "p1"}}))
	if err := m.Unlock(context.Background(), "t1"); err != nil {
		t.Fatalf("unlock without binding: %v", err)
	}
}

// TestDeleteUnlockedResource verifies deleting an unbound, unheld resource
// removes it and reports nothing unbound, because no task's fate is in question.
func TestDeleteUnlockedResource(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}}, res{Resource: Resource{ID: "p2"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	unbound, err := m.Delete(ctx, "p1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(unbound) != 0 {
		t.Fatalf("unbound = %v, want none for an unlocked resource", unbound)
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

// TestDeleteLockedResourceReportsUnboundOwner verifies deleting an idle but
// locked resource releases the lock and names the task it belonged to. What to
// do about that task is the caller's policy; the manager's whole duty is to
// report the fact and return the task to rotation.
func TestDeleteLockedResourceReportsUnboundOwner(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}}, res{Resource: Resource{ID: "p2"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release()

	unbound, err := m.Delete(ctx, "p1")
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !slices.Equal(unbound, []string{"t1"}) {
		t.Fatalf("unbound = %v, want [t1]", unbound)
	}
	if repo.has("p1") {
		t.Fatal("p1 still in repo after delete")
	}
	if owner := repo.get(t, "p2").OwnerID; owner != "" {
		t.Fatalf("p2 OwnerID = %q, want untouched", owner)
	}

	// The freed task rotates the pool again on its next acquire.
	got, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Resource().ID != "p2" {
		t.Fatalf("acquired %s, want rotation onto p2", got.Resource().ID)
	}
}

// TestDeleteRefusesWhileLeaseHeld verifies a resource with a live lease on it
// cannot be deleted out from under its holder — the lease is the fact of use —
// and that the refusal names the holders. Releasing the lease is what frees
// the resource, and the same delete then goes through.
func TestDeleteRefusesWhileLeaseHeld(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}}, res{Resource: Resource{ID: "p2"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if lease.Resource().ID != "p1" {
		t.Fatalf("leased %s, want p1", lease.Resource().ID)
	}

	if _, err := m.Delete(ctx, "p1"); !errors.Is(err, ErrResourceInUse) {
		t.Fatalf("err = %v, want ErrResourceInUse", err)
	} else if !strings.Contains(err.Error(), "t1") {
		t.Fatalf("error %q does not name the holder", err)
	}
	if !repo.has("p1") {
		t.Fatal("refused delete removed the resource anyway")
	}

	// Nobody holds p2, so there is nothing to protect.
	if _, err := m.Delete(ctx, "p2"); err != nil {
		t.Fatalf("delete of an unheld resource: %v", err)
	}

	// Releasing frees it, and the same delete then goes through.
	lease.Release()
	if _, err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("delete after release: %v", err)
	}
}

// TestDeleteRefusesWhileLockedOwnerHoldsLease verifies the guard reads the
// lease, not the lock: a locked resource whose owner is mid-lease refuses, and
// the release — not any question about the task — is what lets it through.
func TestDeleteRefusesWhileLockedOwnerHoldsLease(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}

	if _, err := m.Delete(ctx, "p1"); !errors.Is(err, ErrResourceInUse) {
		t.Fatalf("err = %v, want ErrResourceInUse while the owner holds a lease", err)
	}

	lease.Release()
	unbound, err := m.Delete(ctx, "p1")
	if err != nil {
		t.Fatalf("delete after release: %v", err)
	}
	if !slices.Equal(unbound, []string{"t1"}) {
		t.Fatalf("unbound = %v, want [t1]", unbound)
	}
}

// TestReleaseOfDeletedResource verifies a lease released after its resource was
// deleted (deleted while parked, released later) is a clean no-op.
func TestReleaseOfDeletedResource(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	// The holder releases; the resource is deleted; the stale lease object's
	// second release must not error or resurrect anything.
	lease.Release()
	if _, err := m.Delete(ctx, "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	lease.Release()
	if repo.has("p1") {
		t.Fatal("a stale release resurrected the deleted resource")
	}
}

// TestAdd verifies Add defaults the group to global, stamps
// timestamps, persists, and makes the resource immediately acquirable.
func TestAdd(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo)
	ctx := context.Background()

	if err := m.Add(ctx, res{Resource: Resource{ID: "p1"}, payload: payload{secret: "s1"}}); err != nil {
		t.Fatalf("add: %v", err)
	}
	stored := repo.get(t, "p1")
	if stored.GroupID != GlobalGroup {
		t.Fatalf("GroupID = %q, want global", stored.GroupID)
	}
	if stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() {
		t.Fatal("timestamps not stamped")
	}
	if stored.payload != (payload{secret: "s1"}) {
		t.Fatalf("payload = %+v, want the payload handed in verbatim", stored.payload)
	}

	got, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if got.Resource().ID != "p1" {
		t.Fatalf("acquired %s, want p1", got.Resource().ID)
	}
}

// TestAddRejectsUnknownGroupAndDuplicates verifies Add fails loud on
// a group the manager does not know and on an id already pooled.
func TestAddRejectsUnknownGroupAndDuplicates(t *testing.T) {
	m := newTestManager(t, newFakeRepo(res{Resource: Resource{ID: "p1"}}))
	ctx := context.Background()

	if err := m.Add(ctx, res{Resource: Resource{ID: "p2", GroupID: "ghost"}}); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
	if err := m.Add(ctx, res{Resource: Resource{ID: "p1"}}); err == nil {
		t.Fatal("expected error for duplicate id")
	}
}

// TestCreateGroup verifies CreateGroup persists the group with timestamps and
// makes it immediately usable, and refuses duplicates and unknown strategies.
func TestCreateGroup(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo)
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

	if err := m.Add(ctx, res{Resource: Resource{ID: "p1", GroupID: "g"}}); err != nil {
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
// resources, releasing durable locks and reporting every task it unbound —
// sorted, deduplicated facts for the caller's policy to act on.
func TestDeleteGroupCascades(t *testing.T) {
	repo := newFakeRepo(
		res{Resource: Resource{ID: "a1", GroupID: "ga"}},
		res{Resource: Resource{ID: "a2", GroupID: "ga"}},
		res{Resource: Resource{ID: "b1", GroupID: "gb"}},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(context.Background(), Group{ID: "gb", Strategy: "first"})
	m := newTestManager(t, repo)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release()

	unbound, err := m.DeleteGroup(ctx, "ga")
	if err != nil {
		t.Fatalf("delete group: %v", err)
	}
	if !slices.Equal(unbound, []string{"t1"}) {
		t.Fatalf("unbound = %v, want [t1]", unbound)
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

// TestDeleteGroupRefusals verifies the global group cannot be deleted and an
// unknown group fails with ErrGroupNotFound.
func TestDeleteGroupRefusals(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "a1", GroupID: "ga"}})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	m := newTestManager(t, repo)
	ctx := context.Background()

	if _, err := m.DeleteGroup(ctx, GlobalGroup); err == nil {
		t.Fatal("expected refusal for global group")
	}
	if _, err := m.DeleteGroup(ctx, "ghost"); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("err = %v, want ErrGroupNotFound", err)
	}
}

// TestDeleteGroupRefusesWhileMemberHeld verifies a group with a live lease on
// any member cannot be cascade-deleted, the refusal lands before anything is
// mutated, and the lease's release is what lets the same delete through.
func TestDeleteGroupRefusesWhileMemberHeld(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "a1", GroupID: "ga"}}, res{Resource: Resource{ID: "a2", GroupID: "ga"}})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	m := newTestManager(t, repo)
	ctx := context.Background()

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	if _, err := m.DeleteGroup(ctx, "ga"); !errors.Is(err, ErrGroupInUse) {
		t.Fatalf("err = %v, want ErrGroupInUse", err)
	} else if !strings.Contains(err.Error(), "t1") {
		t.Fatalf("error %q does not name the holder", err)
	}
	if !repo.has("a1") || !repo.has("a2") || !repo.hasGroup("ga") {
		t.Fatal("refused cascade mutated state")
	}

	lease.Release()
	if _, err := m.DeleteGroup(ctx, "ga"); err != nil {
		t.Fatalf("delete after release: %v", err)
	}
	if repo.has("a1") || repo.hasGroup("ga") {
		t.Fatal("group or member survived delete")
	}
}

// TestBlockedAcquireFailsWhenGroupDeleted verifies a waiter blocked on a
// group's capacity is woken and failed when the group is deleted, instead of
// waiting forever on resources that no longer exist. The group's one member is
// locked but unleased, so nothing blocks the cascade.
func TestBlockedAcquireFailsWhenGroupDeleted(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "a1", GroupID: "ga"}})
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	m := newTestManager(t, repo)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release()

	// a1 is locked to t1, so t2 sees a non-empty group with no candidate: it waits.
	ch := acquireAsync(ctx, m, "t2", "ga")
	mustBlock(t, ch)

	unbound, err := m.DeleteGroup(ctx, "ga")
	if err != nil {
		t.Fatalf("delete group: %v", err)
	}
	if !slices.Equal(unbound, []string{"t1"}) {
		t.Fatalf("unbound = %v, want [t1]", unbound)
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
	base := newFakeRepo(res{Resource: Resource{ID: "a1", GroupID: "ga"}}, res{Resource: Resource{ID: "a2", GroupID: "ga"}})
	repo := &failingRepo{fakeRepo: base, failDelete: map[string]bool{"a2": true}}
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "first"})
	m := newTestManager(t, repo)
	ctx := context.Background()

	if _, err := m.DeleteGroup(ctx, "ga"); err == nil {
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
	if _, err := NewManager(ctx, Repository[res](repo), withFirst()); err != nil {
		t.Fatalf("manager unstartable after failed cascade: %v", err)
	}
	// And the surviving member is still leasable.
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga"}); err != nil {
		t.Fatalf("surviving member not leasable: %v", err)
	}
}

// TestFailedRemoveKeepsDurableLockVisible verifies a delete whose store
// removal fails does not drop the binding in memory. The row still carries the
// OwnerID, so forgetting it here would let a restart resurrect a lock this
// deletion was meant to clear.
func TestFailedRemoveKeepsDurableLockVisible(t *testing.T) {
	base := newFakeRepo(res{Resource: Resource{ID: "p1"}})
	repo := &failingRepo{fakeRepo: base, failDelete: map[string]bool{"p1": true}}
	m := newTestManager(t, repo)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release()

	unbound, err := m.Delete(ctx, "p1")
	if err == nil {
		t.Fatal("expected the store failure to surface")
	}
	if len(unbound) != 0 {
		t.Fatalf("unbound = %v, want none reported for a failed delete", unbound)
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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", MaxHolders: 2}})
	m := newTestManager(t, repo)
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
	holder.Release()
	res := mustComplete(t, locked)
	if res.err != nil {
		t.Fatalf("lock after release: %v", res.err)
	}
	res.lease.Release()
	owned, err := m.Acquire(ctx, Assignment{TaskID: "t2"})
	if err != nil {
		t.Fatalf("owner blocked on its own locked resource: %v", err)
	}
	if owned.Resource().ID != "p1" {
		t.Fatalf("owner acquired %s, want p1", owned.Resource().ID)
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
		res{Resource: Resource{ID: "free", GroupID: "ga"}},
		res{Resource: Resource{ID: "taken", GroupID: "ga", OwnerID: "someone"}},
	)
	repo.SaveGroup(context.Background(), Group{ID: "ga", Strategy: "stray"})
	m, err := NewManager(context.Background(), Repository[res](repo),
		WithStrategy("stray", func() Selection[res] { return strayCandidateSelection{res{Resource: Resource{ID: "taken"}}} }))
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

func (r *gateRepo) Save(ctx context.Context, p res) (int64, error) {
	if r.arm.CompareAndSwap(true, false) {
		close(r.entered)
		<-r.gate
	}
	return r.fakeRepo.Save(ctx, p)
}

// TestPinnedAssignmentLeasesOnlyItsResource verifies a pin is a group assignment
// minus the rotation: every lease is the pinned resource, however many members the
// group has.
func TestPinnedAssignmentLeasesOnlyItsResource(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", GroupID: "ga"}}, res{Resource: Resource{ID: "p2", GroupID: "ga"}}, res{Resource: Resource{ID: "p3", GroupID: "ga"}})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: StrategyRoundRobin})
	m := rebuildManager(t, repo)

	pinned := Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "p2"}
	for i := range 3 {
		lease, err := m.Acquire(ctx, pinned)
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		if lease.Resource().ID != "p2" {
			t.Fatalf("lease %d = %s, want p2 every time: a pin does not rotate", i, lease.Resource().ID)
		}
		lease.Release()
	}
}

// TestPinnedAssignmentLeavesRotationUntouched verifies a pin bypasses the
// group's strategy rather than passing through it with one candidate: advancing
// a shared round-robin cursor for a choice nobody made would skew what the
// group's other tasks get next.
func TestPinnedAssignmentLeavesRotationUntouched(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", GroupID: "ga"}}, res{Resource: Resource{ID: "p2", GroupID: "ga"}})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: StrategyRoundRobin})
	m := rebuildManager(t, repo)

	pin, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "p2"})
	if err != nil {
		t.Fatalf("pinned Acquire: %v", err)
	}
	pin.Release()

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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", GroupID: "ga"}}, res{Resource: Resource{ID: "q1", GroupID: "gb"}})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(ctx, Group{ID: "gb", Strategy: "first"})
	m := rebuildManager(t, repo)

	_, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "ga", ResourceID: "gone"})
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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", GroupID: "ga"}}, res{Resource: Resource{ID: "p2", GroupID: "ga"}})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	m := rebuildManager(t, repo)

	lease, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if lease.Resource().ID != "p1" {
		t.Fatalf("locked %s, want p1", lease.Resource().ID)
	}
	lease.Release()

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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", GroupID: "ga"}}, res{Resource: Resource{ID: "q1", GroupID: "gb"}})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(ctx, Group{ID: "gb", Strategy: "first"})
	m := rebuildManager(t, repo)
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "ga"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	lease.Release()

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
// rather than the global one — a task assigned nothing must not keep a resource
// bound.
func TestReleaseStaleLockOnEmptyPlacementReleases(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", GroupID: GlobalGroup}})
	ctx := context.Background()
	m := newTestManager(t, repo)
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	lease.Release()

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
	repo := newFakeRepo(res{Resource: Resource{ID: "p1", GroupID: "ga", OwnerID: "t2"}}, res{Resource: Resource{ID: "p2", GroupID: "ga"}})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	m := rebuildManager(t, repo)

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
	m := rebuildManager(t, repo)

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
		res{Resource: Resource{ID: "p1", GroupID: "ga"}},
		res{Resource: Resource{ID: "p2", GroupID: "ga", OwnerID: "t9"}},
		res{Resource: Resource{ID: "q1", GroupID: "gb"}},
	)
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "ga", Strategy: "first"})
	repo.SaveGroup(ctx, Group{ID: "gb", Strategy: "first"})
	m := rebuildManager(t, repo)

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

// TestUpdatePersistsAModelsOwnFields verifies Update is the one door for a
// change to a record's own fields: the edit lands in the store and in the
// pool the next lease is cut from, UpdatedAt moves, and the leasing fields
// are not fn's to touch — an edit to them is undone before the save.
func TestUpdatePersistsAModelsOwnFields(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}, payload: payload{secret: "old"}})
	m := newTestManager(t, repo)
	ctx := context.Background()

	l, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	l.Release()

	if err := m.Update(ctx, "p1", func(r *res) {
		r.secret = "new"
		r.OwnerID = "intruder"
		r.GroupID = "elsewhere"
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	stored := repo.get(t, "p1")
	if stored.secret != "new" {
		t.Fatalf("stored secret = %q, want the update persisted", stored.secret)
	}
	if stored.OwnerID != "t1" || stored.GroupID != GlobalGroup {
		t.Fatalf("stored core = owner %q group %q, want the leasing fields untouched", stored.OwnerID, stored.GroupID)
	}
	if stored.UpdatedAt.IsZero() {
		t.Fatal("update did not refresh UpdatedAt")
	}

	again, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer again.Release()
	if again.Resource().secret != "new" {
		t.Fatalf("leased secret = %q, want the pool updated too", again.Resource().secret)
	}

	if err := m.Update(ctx, "ghost", func(r *res) {}); err == nil {
		t.Fatal("expected an update to an unknown resource to fail")
	}
}

// TestUpdateCannotResurrectStaleLock verifies Update persists atomically with
// its in-memory change. An Update whose save raced ahead of a concurrent
// Unlock's could land last with the stale owner still set, so a restart would
// load a durable lock no live task holds — the resource would be leased to a
// ghost forever.
func TestUpdateCannotResurrectStaleLock(t *testing.T) {
	repo := &gateRepo{
		fakeRepo: newFakeRepo(res{Resource: Resource{ID: "p1"}}),
		entered:  make(chan struct{}),
		gate:     make(chan struct{}),
	}
	m := newTestManager(t, repo)
	ctx := context.Background()

	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	lease.Release()

	// Arm the gate so Update's save parks mid-flight, then race an Unlock.
	repo.arm.Store(true)
	updateDone := make(chan error, 1)
	go func() { updateDone <- m.Update(ctx, "p1", func(r *res) { r.secret = "edited" }) }()
	<-repo.entered

	unlockDone := make(chan error, 1)
	go func() { unlockDone <- m.Unlock(ctx, "t1") }()

	// Give a racy Unlock time to finish before the parked save lands; a correct
	// Update holds the manager lock across its save, so Unlock cannot pass it.
	var unlockErr error
	unlockFinished := false
	select {
	case unlockErr = <-unlockDone:
		unlockFinished = true
	case <-time.After(100 * time.Millisecond):
	}
	close(repo.gate)

	if err := <-updateDone; err != nil {
		t.Fatalf("Update: %v", err)
	}
	if !unlockFinished {
		unlockErr = <-unlockDone
	}
	if unlockErr != nil {
		t.Fatalf("Unlock: %v", unlockErr)
	}

	stored := repo.get(t, "p1")
	if stored.OwnerID != "" {
		t.Fatalf("stored owner = %q, want unlocked: a stale Update save must not resurrect the lock", stored.OwnerID)
	}
	if stored.secret != "edited" {
		t.Fatalf("stored secret = %q, want the update to survive the unlock", stored.secret)
	}
}

// TestReadSurface verifies the manager's read accessors: groups listed sorted,
// resources in adoption order, lookups reporting existence, and every record
// handed out as a copy that shows durable locks without exposing the pool to
// mutation.
func TestReadSurface(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo(
		res{Resource: Resource{ID: "p2"}, payload: payload{region: "eu"}},
		res{Resource: Resource{ID: "p1"}, payload: payload{region: "us"}},
	)
	m := newTestManager(t, repo)
	if err := m.CreateGroup(ctx, Group{ID: "alpha", Strategy: "first"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	groups := m.ListGroups()
	if len(groups) != 2 || groups[0].ID != "alpha" || groups[1].ID != GlobalGroup {
		t.Fatalf("ListGroups() = %v, want [alpha global]", groups)
	}
	if g, ok := m.GetGroup(GlobalGroup); !ok || g.Strategy != "first" {
		t.Fatalf("Group(global) = %v, %v; want the seeded group", g, ok)
	}
	if _, ok := m.GetGroup("missing"); ok {
		t.Fatal("GetGroup(missing) reported an unknown group as existing")
	}

	// Resources keep adoption order — the seed order, not lexical.
	pool := m.List()
	if len(pool) != 2 || pool[0].ID != "p2" || pool[1].ID != "p1" {
		t.Fatalf("List() = %v, want [p2 p1]", pool)
	}

	// A durable lock shows on the records read back.
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: GlobalGroup})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	locked, ok := m.Get(lease.Resource().ID)
	if !ok || locked.OwnerID != "t1" {
		t.Fatalf("Resource(%s) = %v, %v; want OwnerID t1", lease.Resource().ID, locked, ok)
	}

	// Reads are copies: mutating one changes nothing behind it.
	locked.OwnerID, locked.region = "intruder", "mars"
	if again, _ := m.Get(lease.Resource().ID); again.OwnerID != "t1" || again.region == "mars" {
		t.Fatalf("pool mutated through a read copy: %+v", again)
	}
	if _, ok := m.Get("missing"); ok {
		t.Fatal("Get(missing) reported an unknown resource as existing")
	}
}

// TestUpdateGroup verifies UpdateGroup completes the group CRUD: edits persist
// write-through the cache, identity fields survive whatever fn does to them,
// an unknown strategy rejects the whole edit, and a missing group errors.
func TestUpdateGroup(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo(res{Resource: Resource{ID: "p1"}})
	m := newTestManager(t, repo)

	created, _ := m.GetGroup(GlobalGroup)
	if err := m.UpdateGroup(ctx, GlobalGroup, func(g *Group) {
		g.Refs = map[string]string{"email": "inbox-1"}
		g.ID, g.CreatedAt = "hijacked", created.CreatedAt.Add(-1) // not fn's to change
	}); err != nil {
		t.Fatalf("UpdateGroup: %v", err)
	}
	got, found := m.GetGroup(GlobalGroup)
	if !found || got.Refs["email"] != "inbox-1" {
		t.Fatalf("GetGroup after update = %v, %v; want the new ref", got, found)
	}
	if got.ID != GlobalGroup || !got.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("identity fields mutated: %+v", got)
	}
	stored, err := repo.ListGroups(ctx)
	if err != nil || len(stored) != 1 || stored[0].Refs["email"] != "inbox-1" {
		t.Fatalf("store after update = %v, %v; want the edit persisted", stored, err)
	}

	// An unknown strategy rejects the edit whole: nothing cached, nothing saved.
	if err := m.UpdateGroup(ctx, GlobalGroup, func(g *Group) {
		g.Strategy = "nonsense"
		g.Refs = nil
	}); err == nil {
		t.Fatal("UpdateGroup accepted an unknown strategy")
	}
	if got, _ := m.GetGroup(GlobalGroup); got.Refs["email"] != "inbox-1" {
		t.Fatalf("rejected edit still landed: %+v", got)
	}

	if err := m.UpdateGroup(ctx, "missing", func(g *Group) {}); !errors.Is(err, ErrGroupNotFound) {
		t.Fatalf("UpdateGroup(missing) err = %v, want ErrGroupNotFound", err)
	}
}

// TestUpdateGroupStrategyChange verifies a strategy change takes effect on the
// next selection with a fresh instance, while an update that keeps the
// strategy keeps its rotation state.
func TestUpdateGroupStrategyChange(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo(
		res{Resource: Resource{ID: "p1", MaxHolders: UnlimitedHolders}},
		res{Resource: Resource{ID: "p2", MaxHolders: UnlimitedHolders}},
	)
	// The global group starts round robin so rotation state is observable.
	if err := repo.SaveGroup(ctx, Group{ID: GlobalGroup, Strategy: StrategyRoundRobin}); err != nil {
		t.Fatalf("seed global group: %v", err)
	}
	m := rebuildManager(t, repo)

	first, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: GlobalGroup})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	first.Release()

	// A refs-only update keeps the cursor: the next pick advances, not repeats.
	if err := m.UpdateGroup(ctx, GlobalGroup, func(g *Group) {
		g.Refs = map[string]string{"note": "kept"}
	}); err != nil {
		t.Fatalf("UpdateGroup refs: %v", err)
	}
	second, err := m.Acquire(ctx, Assignment{TaskID: "t2", GroupID: GlobalGroup})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if second.Resource().ID == first.Resource().ID {
		t.Fatalf("refs-only update reset rotation: picked %s twice", first.Resource().ID)
	}
	second.Release()

	// Switching to the deterministic "first" strategy installs it immediately.
	if err := m.UpdateGroup(ctx, GlobalGroup, func(g *Group) { g.Strategy = "first" }); err != nil {
		t.Fatalf("UpdateGroup strategy: %v", err)
	}
	for range 2 {
		l, err := m.Acquire(ctx, Assignment{TaskID: "t3", GroupID: GlobalGroup})
		if err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if l.Resource().ID != "p1" {
			t.Fatalf("after strategy change picked %s, want the deterministic first p1", l.Resource().ID)
		}
		l.Release()
	}
}
