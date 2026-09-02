package proxies_test

import (
	"context"
	"testing"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/memory"
	"github.com/ntakezo/rogojin/proxies"
)

// These tests need a real store shared between managers, which means the
// memory adapter — importable only from outside the package, since memory
// itself imports proxies.

// TestOutcomesFromEveryManagerSum verifies the point of the counters: two
// managers over one store report outcomes and none is lost — the read-modify-
// write undercount the row-borne stats suffered cannot happen to an atomic
// increment.
func TestOutcomesFromEveryManagerSum(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewProxies()

	a, err := proxies.NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager a: %v", err)
	}
	defer a.Close()
	if err := a.Add(ctx, proxies.Proxy{Resource: leasing.Resource{ID: "p1"}, URL: "http://p1.example"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	b, err := proxies.NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager b: %v", err)
	}
	defer b.Close()

	report := func(m *proxies.Manager, task string, success bool) {
		t.Helper()
		lease, err := m.Acquire(ctx, proxies.Assignment{TaskID: task})
		if err != nil {
			t.Fatalf("Acquire %s: %v", task, err)
		}
		if err := lease.ReleaseOutcome(ctx, success); err != nil {
			t.Fatalf("ReleaseOutcome %s: %v", task, err)
		}
	}
	report(a, "t1", true)
	report(b, "t2", true)
	report(a, "t3", false)

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 1 || listed[0].Successes != 2 || listed[0].Failures != 1 {
		t.Fatalf("stats = %+v, want exactly 2 successes and 1 failure summed across managers", listed)
	}
}

// TestSamplerLearnsFromHydratedCounters verifies the loop closes: outcomes
// tallied through one manager reach another manager's bayesian strategy via
// the counters its boot-time List projects into the pool.
func TestSamplerLearnsFromHydratedCounters(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewProxies()

	seed, err := proxies.NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	defer seed.Close()
	if err := seed.CreateGroup(ctx, proxies.Group{ID: "learned", Strategy: proxies.StrategyBayesian}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	for _, p := range []proxies.Proxy{
		{Resource: leasing.Resource{ID: "flaky", GroupID: "learned"}, URL: "http://flaky.example"},
		{Resource: leasing.Resource{ID: "solid", GroupID: "learned"}, URL: "http://solid.example"},
	} {
		if err := seed.Add(ctx, p); err != nil {
			t.Fatalf("Add %s: %v", p.ID, err)
		}
	}
	// A lopsided history, written store-side as ReleaseOutcome would.
	for range 50 {
		if _, err := repo.Increment(ctx, "solid", "successes", 1); err != nil {
			t.Fatalf("Increment: %v", err)
		}
		if _, err := repo.Increment(ctx, "flaky", "failures", 1); err != nil {
			t.Fatalf("Increment: %v", err)
		}
	}

	// A manager booted after the history exists sees it through hydration and
	// overwhelmingly prefers the solid proxy. Ten sequential draws must all
	// land there: Beta(51,1) against Beta(1,51) leaves no realistic overlap.
	fresh, err := proxies.NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager fresh: %v", err)
	}
	defer fresh.Close()
	for i := range 10 {
		lease, err := fresh.Acquire(ctx, proxies.Assignment{TaskID: "chooser", GroupID: "learned"})
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		if got := lease.Resource().ID; got != "solid" {
			t.Fatalf("draw %d picked %s, want the solid proxy", i, got)
		}
		lease.Release()
	}
}
