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
		res{ID: "k1", Attrs: payload{secret: "s1", region: "eu"}},
		res{ID: "k2", Attrs: payload{secret: "s2", region: "us"}},
	)
	ctx := context.Background()

	m, err := NewManager(ctx, Config[payload]{Noun: "api key", Repository: repo})
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
	repo := newFakeRepo(res{ID: "k1", Attrs: want})
	ctx := context.Background()

	m, err := NewManager(ctx, Config[payload]{Noun: "api key", Repository: repo})
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if err := lease.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}

	restarted, err := NewManager(ctx, Config[payload]{Noun: "api key", Repository: repo})
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
	if regained.Resource().Attrs != want {
		t.Fatalf("payload = %+v, want %+v", regained.Resource().Attrs, want)
	}
	if regained.Resource().Successes != 1 {
		t.Fatalf("successes = %d, want the release recorded", regained.Resource().Successes)
	}
}

// TestConfigIsValidatedNotRepaired verifies a misconfigured default strategy is
// reported. Quietly substituting round robin would hand every group an
// algorithm nobody asked for, and the mistake would only show as odd rotation.
func TestConfigIsValidatedNotRepaired(t *testing.T) {
	ctx := context.Background()
	repo := newFakeRepo()

	if _, err := NewManager(ctx, Config[payload]{Repository: repo, DefaultStrategy: "absent"}); err == nil {
		t.Fatal("expected an unregistered default strategy to be refused")
	}

	strategies := map[string]StrategyFactory[payload]{
		"mine": func() Selection[payload] { return firstSelection{} },
	}
	if _, err := NewManager(ctx, Config[payload]{Repository: repo, Strategies: strategies}); err == nil {
		t.Fatal("expected configured strategies with no named default to be refused")
	}
	if _, err := NewManager(ctx, Config[payload]{Repository: repo, Strategies: strategies, DefaultStrategy: "mine"}); err != nil {
		t.Fatalf("a named, registered default: %v", err)
	}
	if _, err := NewManager(ctx, Config[payload]{}); err == nil {
		t.Fatal("expected a repository to be required")
	}
}
