package proxies

import (
	"context"
	"sync"
	"testing"

	"github.com/ntakezo/rogojin/leasing"
)

// These tests cover what this package adds to the leasing core: the proxy
// payload and the bayesian strategy registration. The leasing behavior itself
// — groups, caps, locks, pins, deletes — is covered where it lives.

// fakeRepo is a minimal in-memory Repository.
type fakeRepo struct {
	mu      sync.Mutex
	order   []string
	records map[string]Proxy
	groups  map[string]Group
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
	repo := newFakeRepo(Proxy{ID: "p1", Attrs: Attrs{URL: "http://10.0.0.1:8080"}})
	ctx := context.Background()

	m, err := NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if got := lease.Resource().Attrs.URL; got != "http://10.0.0.1:8080" {
		t.Fatalf("URL = %q, want the seeded one", got)
	}
	if err := lease.Release(true); err != nil {
		t.Fatalf("Release: %v", err)
	}

	r := repo.records["p1"]
	if r.Attrs.URL != "http://10.0.0.1:8080" || r.Successes != 1 || r.OwnerID != "t1" {
		t.Fatalf("persisted = %+v, want URL, stats, and lock intact", r)
	}
}

// TestBuiltInStrategiesAreRegistered verifies a group may name either built-in
// with no extra wiring, and that an unknown name still fails loud.
func TestBuiltInStrategiesAreRegistered(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1", GroupID: "learned"})
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
	if err := lease.Release(true); err != nil {
		t.Fatalf("Release: %v", err)
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
		Proxy{ID: "p1", Attrs: Attrs{URL: "http://a"}},
		Proxy{ID: "p2", Attrs: Attrs{URL: "http://b"}},
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
		if p.Attrs.URL == s.want {
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
