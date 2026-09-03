package proxies

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
)

// These tests cover what this package adds to the leasing core: the proxy
// payload and the bayesian strategy registration. The leasing behavior itself
// — groups, caps, locks, pins, deletes — is covered where it lives.

// fakeRepo is a minimal in-memory Repository.
type fakeRepo struct {
	mu       sync.Mutex
	order    []string
	records  map[string]Proxy
	groups   map[string]Group
	counters map[string]int64
}

func newFakeRepo(seed ...Proxy) *fakeRepo {
	r := &fakeRepo{records: map[string]Proxy{}, groups: map[string]Group{}, counters: map[string]int64{}}
	for _, p := range seed {
		r.records[p.ID] = p
		r.order = append(r.order, p.ID)
	}
	return r
}

// counter reads one tally, the way the tests assert what ReleaseOutcome wrote.
func (r *fakeRepo) counter(scope, name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[scope+"/"+name]
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

// Save is a blind upsert handing back a bumped version; the conditional-write
// rules are storetest's to verify against the real adapters.
func (r *fakeRepo) Save(ctx context.Context, p Proxy) (int64, error) {
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

func (r *fakeRepo) Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	return leasing.Hold{ResourceID: resourceID, TaskID: taskID, Count: 1, ExpiresAt: time.Now().Add(ttl)}, nil
}
func (r *fakeRepo) ReleaseHold(ctx context.Context, resourceID, taskID string) error { return nil }
func (r *fakeRepo) RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	return nil
}
func (r *fakeRepo) ListHolds(ctx context.Context) ([]leasing.Hold, error) { return nil, nil }
func (r *fakeRepo) ClaimLock(ctx context.Context, resourceID, taskID string) error {
	return nil
}
func (r *fakeRepo) ReleaseLock(ctx context.Context, resourceID, taskID string) error {
	return nil
}
func (r *fakeRepo) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters[scope+"/"+name] += delta
	return r.counters[scope+"/"+name], nil
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
	out := make([]Group, 0, len(r.groups))
	for _, g := range r.groups {
		out = append(out, g)
	}
	return out, nil
}

func (r *fakeRepo) SaveGroup(ctx context.Context, g Group) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groups[g.ID] = g
	return nil
}

func (r *fakeRepo) DeleteGroup(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.groups, id)
	return nil
}

// TestURLTravelsThroughThePool verifies the payload this package exists for:
// a proxy's URL survives the trip through the manager, a lock, and a release.
func TestURLTravelsThroughThePool(t *testing.T) {
	repo := newFakeRepo(Proxy{Resource: leasing.Resource{ID: "p1"}, URL: "http://10.0.0.1:8080"})
	ctx := context.Background()

	m, err := NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if got := lease.Resource().URL; got != "http://10.0.0.1:8080" {
		t.Fatalf("URL = %q, want the seeded one", got)
	}
	if err := lease.ReleaseOutcome(ctx, true); err != nil {
		t.Fatalf("ReleaseOutcome: %v", err)
	}

	// The tally lands in the counters; the URL, the lock, and the amended
	// stat show on the manager's view of the pool.
	if got := repo.counter("p1", "successes"); got != 1 {
		t.Fatalf("successes counter = %d, want 1", got)
	}
	p, ok := m.Get("p1")
	if !ok || p.URL != "http://10.0.0.1:8080" || p.OwnerID != "t1" || p.Successes != 1 {
		t.Fatalf("pool view = %+v, want URL, lock, and amended stat intact", p)
	}
}

// TestReleaseWithoutOutcomeCountsNeither verifies the plain Release frees the
// proxy without tallying anything — an outcome-free release must not poison
// the sampler with phantom data.
func TestReleaseWithoutOutcomeCountsNeither(t *testing.T) {
	repo := newFakeRepo(Proxy{Resource: leasing.Resource{ID: "p1"}, URL: "http://10.0.0.1:8080"})
	ctx := context.Background()

	m, err := NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	lease.Release()

	if s, f := repo.counter("p1", "successes"), repo.counter("p1", "failures"); s != 0 || f != 0 {
		t.Fatalf("counters = %d/%d, want 0/0 for an outcome-free release", s, f)
	}
	// The slot is free again: a second acquire under the default cap succeeds.
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t2"}); err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
}

// TestOutcomeAndReleaseActOnce verifies the two release paths share one latch:
// whichever runs first wins, so a uniform Teardown calling Release after an
// earlier ReleaseOutcome tallies once and frees once.
func TestOutcomeAndReleaseActOnce(t *testing.T) {
	repo := newFakeRepo(Proxy{Resource: leasing.Resource{ID: "p1"}, URL: "http://10.0.0.1:8080"})
	ctx := context.Background()

	m, err := NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	// ReleaseOutcome first: the later Release must not free a second time.
	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if err := lease.ReleaseOutcome(ctx, true); err != nil {
		t.Fatalf("ReleaseOutcome: %v", err)
	}
	lease.Release()
	if got := repo.counter("p1", "successes"); got != 1 {
		t.Fatalf("successes counter = %d, want 1 after outcome-then-release", got)
	}

	// Release first: the later ReleaseOutcome must not tally.
	lease, err = m.Acquire(ctx, Assignment{TaskID: "t2"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	lease.Release()
	if err := lease.ReleaseOutcome(ctx, true); err != nil {
		t.Fatalf("ReleaseOutcome after Release: %v", err)
	}
	if got := repo.counter("p1", "successes"); got != 1 {
		t.Fatalf("successes counter = %d, want still 1 after release-then-outcome", got)
	}
}

// TestBuiltInStrategiesAreRegistered verifies a group may name either built-in
// with no extra wiring, and that an unknown name still fails loud.
func TestBuiltInStrategiesAreRegistered(t *testing.T) {
	repo := newFakeRepo(Proxy{Resource: leasing.Resource{ID: "p1", GroupID: "learned"}})
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: "learned", Strategy: StrategyBayesian})
	repo.SaveGroup(ctx, Group{ID: "even", Strategy: StrategyRoundRobin})

	m, err := NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager with built-in strategies: %v", err)
	}
	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "learned"})
	if err != nil {
		t.Fatalf("Acquire via bayesian: %v", err)
	}
	if err := lease.ReleaseOutcome(ctx, true); err != nil {
		t.Fatalf("ReleaseOutcome: %v", err)
	}

	repo.SaveGroup(ctx, Group{ID: "typo", Strategy: "bayseian"})
	if _, err := NewManager(ctx, repo); err == nil {
		t.Fatal("expected an unknown strategy name to be refused")
	}
}

// TestWithStrategyRegistersCustom verifies consumer strategies register through
// the pass-through option and see proxy-shaped candidates.
func TestWithStrategyRegistersCustom(t *testing.T) {
	repo := newFakeRepo(
		Proxy{Resource: leasing.Resource{ID: "p1"}, URL: "http://a"},
		Proxy{Resource: leasing.Resource{ID: "p2"}, URL: "http://b"},
	)
	ctx := context.Background()
	repo.SaveGroup(ctx, Group{ID: GlobalGroup, Strategy: "by-url"})

	m, err := NewManager(ctx, repo, WithStrategy("by-url", func() Selection {
		return urlSelection{want: "http://b"}
	}))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Resource().ID != "p2" {
		t.Fatalf("acquired %s, want the strategy's pick p2", lease.Resource().ID)
	}
}

// urlSelection picks the candidate with the wanted URL, proving strategies see
// the payload.
type urlSelection struct{ want string }

func (s urlSelection) Select(candidates []Proxy) (Proxy, error) {
	for _, p := range candidates {
		if p.URL == s.want {
			return p, nil
		}
	}
	return candidates[0], nil
}

// TestManagerSatisfiesTasksContract verifies the alias really is the leasing
// manager: the methods a task service drives are present without adapters.
func TestManagerSatisfiesTasksContract(t *testing.T) {
	m, err := NewManager(context.Background(), newFakeRepo())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	var _ interface {
		Unlock(ctx context.Context, taskID string) error
		ReleaseStaleLock(ctx context.Context, a leasing.Assignment) error
	} = m
}
