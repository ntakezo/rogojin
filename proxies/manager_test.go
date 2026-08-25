package proxies

import (
	"context"
	"errors"
	"testing"

	"github.com/ntakezo/rogojin/leasing"
)

// The leasing behaviour these tests used to cover — pooling, groups, holder
// caps, durable locks, pins, deletion policies, the usage guard — moved to the
// leasing package along with the code, where it is exercised once for every
// resource kind. What is left here is what is actually proxy-specific: the URL
// payload, the strategy registry, and the translation between this package's
// shapes and the core's.

// fakeRepo is an in-memory Repository recording what the manager stores, so a
// test can assert the consumer's store is handed proxy-shaped records.
type fakeRepo struct {
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
	out := make([]Proxy, 0, len(r.order))
	for _, id := range r.order {
		if p, ok := r.records[id]; ok {
			out = append(out, p)
		}
	}
	return out, nil
}

func (r *fakeRepo) Save(ctx context.Context, p Proxy) error {
	if _, ok := r.records[p.ID]; !ok {
		r.order = append(r.order, p.ID)
	}
	r.records[p.ID] = p
	return nil
}

func (r *fakeRepo) Delete(ctx context.Context, id string) error {
	delete(r.records, id)
	return nil
}

func (r *fakeRepo) ListGroups(ctx context.Context) ([]Group, error) {
	out := make([]Group, 0, len(r.groups))
	for _, g := range r.groups {
		out = append(out, g)
	}
	return out, nil
}

func (r *fakeRepo) SaveGroup(ctx context.Context, g Group) error {
	r.groups[g.ID] = g
	return nil
}

func (r *fakeRepo) DeleteGroup(ctx context.Context, id string) error {
	delete(r.groups, id)
	return nil
}

func newTestManager(t *testing.T, repo Repository, policy DeletionPolicy, opts ...ManagerOption) *Manager {
	t.Helper()
	m, err := NewManager(context.Background(), repo, policy, opts...)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

// TestURLTravelsThroughThePool verifies the one field this package adds to the
// core's record survives everything the core does to it: a store round trip, a
// lease, a durable lock, and a restart. The core carries it opaquely, so
// nothing else proves the translation is wired both ways.
func TestURLTravelsThroughThePool(t *testing.T) {
	repo := newFakeRepo()
	ctx := context.Background()
	m := newTestManager(t, repo, nil)

	if err := m.AddProxy(ctx, Proxy{ID: "p1", URL: "http://u:p@h1:80"}); err != nil {
		t.Fatalf("AddProxy: %v", err)
	}
	if stored := repo.records["p1"]; stored.URL != "http://u:p@h1:80" {
		t.Fatalf("stored URL = %q, want the one added", stored.URL)
	}

	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	if lease.Proxy().URL != "http://u:p@h1:80" {
		t.Fatalf("leased URL = %q, want the one added", lease.Proxy().URL)
	}
	if err := lease.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}

	restarted := newTestManager(t, repo, nil)
	regained, err := restarted.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire after restart: %v", err)
	}
	if regained.Proxy().URL != "http://u:p@h1:80" {
		t.Fatalf("URL after restart = %q, want the one added", regained.Proxy().URL)
	}
}

// recordingPolicy captures the proxy it is handed, so a test can prove the
// policy port speaks this package's shape rather than the core's.
type recordingPolicy struct {
	deleted Proxy
	taskID  string
}

func (p *recordingPolicy) OnProxyDeleted(ctx context.Context, taskID string, deleted Proxy) Decision {
	p.taskID = taskID
	p.deleted = deleted
	return Unbind
}

// TestDeletionPolicySeesAProxy verifies the port a consumer implements is handed
// a Proxy with its URL, not the core's record. A policy deciding what to do
// about a deleted proxy usually wants to log or report which one it was.
func TestDeletionPolicySeesAProxy(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1", URL: "http://h1:80", GroupID: GlobalGroup, OwnerID: "t1"})
	policy := &recordingPolicy{}
	ctx := context.Background()
	m := newTestManager(t, repo, policy, WithUsagePolicy(Usage{}))

	if err := m.DeleteProxy(ctx, "p1"); err != nil {
		t.Fatalf("DeleteProxy: %v", err)
	}
	if policy.taskID != "t1" {
		t.Fatalf("policy asked about %q, want t1", policy.taskID)
	}
	if policy.deleted.ID != "p1" || policy.deleted.URL != "http://h1:80" {
		t.Fatalf("policy saw %+v, want the full proxy", policy.deleted)
	}
}

// urlSelection picks by URL, which only works if the strategy port is handed
// proxies rather than the core's records.
type urlSelection struct{ want string }

func (s urlSelection) Select(candidates []Proxy) (Proxy, error) {
	for _, p := range candidates {
		if p.URL == s.want {
			return p, nil
		}
	}
	return Proxy{}, errors.New("no candidate with the wanted URL")
}

// TestStrategiesSeeProxiesAndTheirPickIsHonored verifies a consumer's selection
// algorithm receives proxies, and that the proxy it names is the one leased.
// The core validates the pick by id against its own candidates, so this is what
// proves the round trip through the adapter keeps them the same records.
func TestStrategiesSeeProxiesAndTheirPickIsHonored(t *testing.T) {
	repo := newFakeRepo(
		Proxy{ID: "p1", URL: "http://h1:80", GroupID: GlobalGroup},
		Proxy{ID: "p2", URL: "http://h2:80", GroupID: GlobalGroup},
	)
	ctx := context.Background()
	m := newTestManager(t, repo, nil,
		WithStrategy("by-url", func() Selection { return urlSelection{want: "http://h2:80"} }))
	if err := m.CreateGroup(ctx, Group{ID: "picky", Strategy: "by-url"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := m.AddProxy(ctx, Proxy{ID: "p3", URL: "http://h2:80", GroupID: "picky"}); err != nil {
		t.Fatalf("AddProxy: %v", err)
	}
	if err := m.AddProxy(ctx, Proxy{ID: "p4", URL: "http://h4:80", GroupID: "picky"}); err != nil {
		t.Fatalf("AddProxy: %v", err)
	}

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "picky"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Proxy().ID != "p3" {
		t.Fatalf("leased %s, want p3 — the one the strategy chose by URL", lease.Proxy().ID)
	}
}

// TestBuiltInStrategiesAreRegistered verifies both names this package documents
// resolve, that an unknown one is refused, and that WithStrategy can replace a
// built-in — the documented way to make bayesian deterministic.
func TestBuiltInStrategiesAreRegistered(t *testing.T) {
	ctx := context.Background()
	for _, name := range []string{StrategyRoundRobin, StrategyBayesian, ""} {
		repo := newFakeRepo(Proxy{ID: "p1", URL: "http://h1:80", GroupID: "g"})
		repo.SaveGroup(ctx, Group{ID: "g", Strategy: name})
		m := newTestManager(t, repo, nil)
		if _, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "g"}); err != nil {
			t.Fatalf("acquire under strategy %q: %v", name, err)
		}
	}

	repo := newFakeRepo()
	repo.SaveGroup(ctx, Group{ID: "g", Strategy: "retired"})
	if _, err := NewManager(ctx, repo, nil); err == nil {
		t.Fatal("expected a group naming an unregistered strategy to be refused")
	}

	// Overriding a built-in is how a consumer seeds the sampler for a test.
	overridden := newFakeRepo(Proxy{ID: "p1", URL: "http://h1:80", GroupID: "g"})
	overridden.SaveGroup(ctx, Group{ID: "g", Strategy: StrategyBayesian})
	m := newTestManager(t, overridden, nil,
		WithStrategy(StrategyBayesian, func() Selection { return NewBayesian(WithSeed(1)) }))
	if _, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "g"}); err != nil {
		t.Fatalf("acquire under an overridden built-in: %v", err)
	}
}

// TestAssignmentPinTravelsAsTheProxyID verifies this package's ProxyID reaches
// the core as its ResourceID: the two structs are spelled differently, and a
// pin silently dropped in translation would look like ordinary rotation.
func TestAssignmentPinTravelsAsTheProxyID(t *testing.T) {
	repo := newFakeRepo(
		Proxy{ID: "p1", URL: "http://h1:80", GroupID: GlobalGroup},
		Proxy{ID: "p2", URL: "http://h2:80", GroupID: GlobalGroup},
	)
	ctx := context.Background()
	m := newTestManager(t, repo, nil)

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1", ProxyID: "p2"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Proxy().ID != "p2" {
		t.Fatalf("leased %s, want the pinned p2", lease.Proxy().ID)
	}
	if err := m.CheckAssignment(Assignment{TaskID: "t1", ProxyID: "p2"}); err != nil {
		t.Fatalf("CheckAssignment on a good pin: %v", err)
	}
	if err := m.CheckAssignment(Assignment{TaskID: "t1", ProxyID: "ghost"}); !errors.Is(err, ErrProxyNotFound) {
		t.Fatalf("err = %v, want ErrProxyNotFound", err)
	}
}

// TestSentinelsAliasTheCore verifies every error this package documents is the
// core's, so errors.Is answers the same question either side of the boundary. A
// sentinel that drifted into a private copy would silently stop matching.
func TestSentinelsAliasTheCore(t *testing.T) {
	for _, pair := range []struct {
		name  string
		here  error
		there error
	}{
		{"ErrNoProxies", ErrNoProxies, leasing.ErrNoResources},
		{"ErrGroupNotFound", ErrGroupNotFound, leasing.ErrGroupNotFound},
		{"ErrGroupInUse", ErrGroupInUse, leasing.ErrGroupInUse},
		{"ErrProxyInUse", ErrProxyInUse, leasing.ErrResourceInUse},
		{"ErrProxyNotFound", ErrProxyNotFound, leasing.ErrResourceNotFound},
		{"ErrProxyNotInGroup", ErrProxyNotInGroup, leasing.ErrResourceNotInGroup},
		{"ErrProxyLocked", ErrProxyLocked, leasing.ErrResourceLocked},
		{"ErrPinConflict", ErrPinConflict, leasing.ErrPinConflict},
		{"ErrTaskOrphaned", ErrTaskOrphaned, leasing.ErrTaskOrphaned},
	} {
		if pair.here != pair.there {
			t.Fatalf("%s is not the core sentinel", pair.name)
		}
	}
}

// TestUsageWiresEachQuestionToItsOwnFunc verifies the three closures land on the
// three questions the guard asks, and that a nil one reports nothing rather
// than panicking — the state a consumer with only a task service has.
func TestUsageWiresEachQuestionToItsOwnFunc(t *testing.T) {
	repo := newFakeRepo(Proxy{ID: "p1", URL: "http://h1:80", GroupID: GlobalGroup})
	ctx := context.Background()
	asked := map[string]string{}
	m := newTestManager(t, repo, nil, WithUsagePolicy(Usage{
		RunningInGroup: func(ctx context.Context, groupID string) ([]string, error) {
			asked["group"] = groupID
			return nil, nil
		},
		PinnedToProxy: func(ctx context.Context, proxyID string) ([]string, error) {
			asked["pinned"] = proxyID
			return []string{"t9"}, nil
		},
	}))

	impact, err := m.DeletionImpact(ctx, "p1")
	if err != nil {
		t.Fatalf("DeletionImpact: %v", err)
	}
	if asked["group"] != GlobalGroup {
		t.Fatalf("RunningInGroup asked about %q, want the global group", asked["group"])
	}
	if asked["pinned"] != "p1" {
		t.Fatalf("PinnedToProxy asked about %q, want p1", asked["pinned"])
	}
	if len(impact.Pinned) != 1 || impact.Pinned[0] != "t9" {
		t.Fatalf("pinned = %v, want [t9]", impact.Pinned)
	}
	// TaskRunning was left nil, and the guard neither panicked nor refused.
	if len(impact.Running) != 0 {
		t.Fatalf("running = %v, want none", impact.Running)
	}
}
