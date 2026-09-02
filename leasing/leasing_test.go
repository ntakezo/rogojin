package leasing

import (
	"context"
	"testing"
)

// These cover this package's own API surface — the parts a consumer building a
// new resource kind touches directly, rather than the leasing behaviour in
// manager_test.go.

// TestAResourceKindNeedsNoConfiguration verifies the package is usable on its
// own terms: a kind with nothing to tune supplies a repository and gets
// pooling, holder caps, and durable locks with no strategy wiring at all. It is
// how accounts is built, and how a third kind would be.
func TestAResourceKindNeedsNoConfiguration(t *testing.T) {
	repo := newFakeRepo(
		res{Resource: Resource{ID: "k1"}, payload: payload{secret: "s1", region: "eu"}},
		res{Resource: Resource{ID: "k2"}, payload: payload{secret: "s2", region: "us"}},
	)
	ctx := context.Background()

	m, err := NewManager(ctx, Repository[res](repo))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	first, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}
	second, err := m.Acquire(ctx, Assignment{TaskID: "t2"})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if first.Resource().ID == second.Resource().ID {
		t.Fatalf("both tasks got %s, want one each", first.Resource().ID)
	}
}

// TestPayloadSurvivesALockAndARestart verifies the manager only ever copies the
// payload it is handed: a lock, a release, and a reload leave it untouched.
// Everything above this layer depends on that — it is where a proxy's URL and
// an account's credentials live.
func TestPayloadSurvivesALockAndARestart(t *testing.T) {
	want := payload{secret: "s1", region: "eu"}
	repo := newFakeRepo(res{Resource: Resource{ID: "k1"}, payload: want})
	ctx := context.Background()

	m, err := NewManager(ctx, Repository[res](repo))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	lease.Release()

	restarted, err := NewManager(ctx, Repository[res](repo))
	if err != nil {
		t.Fatalf("NewManager after restart: %v", err)
	}
	regained, err := restarted.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire after restart: %v", err)
	}
	if regained.Resource().ID != "k1" {
		t.Fatalf("reclaimed %s, want k1", regained.Resource().ID)
	}
	if regained.Resource().payload != want {
		t.Fatalf("payload = %+v, want %+v", regained.Resource().payload, want)
	}
}

// TestNilRepositoryRunsInMemory verifies the documented in-memory mode: a nil
// repository yields an empty manager whose whole leasing surface — seeding,
// groups, acquisition, durable locks, deletion — works with nothing durable
// behind it.
func TestNilRepositoryRunsInMemory(t *testing.T) {
	ctx := context.Background()
	m, err := NewManager[res, *res](ctx, nil)
	if err != nil {
		t.Fatalf("NewManager with nil repo: %v", err)
	}

	if err := m.CreateGroup(ctx, Group{ID: "eu"}); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := m.Add(ctx, res{Resource: Resource{ID: "k1", GroupID: "eu"}, payload: payload{secret: "s1"}}); err != nil {
		t.Fatalf("add: %v", err)
	}

	lease, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "eu"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if got := lease.Resource().payload.secret; got != "s1" {
		t.Fatalf("payload secret = %q, want s1", got)
	}
	lease.Release()
	if err := m.Unlock(ctx, "t1"); err != nil {
		t.Fatalf("unlock: %v", err)
	}
	if unbound, err := m.Delete(ctx, "k1"); err != nil || len(unbound) != 0 {
		t.Fatalf("delete = (%v, %v), want no stranded tasks and no error", unbound, err)
	}
}

// TestWithStrategyOverridesRoundRobin verifies a custom factory registered
// under the round-robin name replaces the built-in, which is the documented way
// to override the default.
func TestWithStrategyOverridesRoundRobin(t *testing.T) {
	repo := newFakeRepo(res{Resource: Resource{ID: "k1"}}, res{Resource: Resource{ID: "k2"}})
	ctx := context.Background()

	m, err := NewManager(ctx, Repository[res](repo),
		WithStrategy(StrategyRoundRobin, func() Selection[res] { return firstSelection{} }))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	for i := range 2 {
		lease, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		if lease.Resource().ID != "k1" {
			t.Fatalf("acquire %d = %s, want k1 every time under the override", i, lease.Resource().ID)
		}
		lease.Release()
	}
}

// TestKindValidate pins the kind charset: the names placements are filed under
// become JSON keys and JSON paths in stores, so a character with meaning there
// ('.', '[', a quote) must be refused at validation rather than misfile a
// placement at write time. The three shipped kinds must, of course, pass.
func TestKindValidate(t *testing.T) {
	for _, k := range []Kind{"proxy", "account", "payment", "sms-inbox", "Kind_2"} {
		if err := k.Validate(); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", k, err)
		}
	}
	for _, k := range []Kind{"", "x.y", "a[0]", `q"o`, "sp ace", "ünïcode"} {
		if err := k.Validate(); err == nil {
			t.Errorf("Validate(%q) = nil, want an error", k)
		}
	}
}
