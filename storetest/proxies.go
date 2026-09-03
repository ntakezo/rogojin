package storetest

import (
	"context"
	"testing"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/proxies"
)

// Proxies exercises the proxies.Repository contract: the shared leasing
// record behavior plus the model's own columns — URL and outcome stats.
func Proxies(t *testing.T, open func(t *testing.T) proxies.Repository) {
	ctx := context.Background()

	t.Run("Leasing", func(t *testing.T) {
		Leasing(t, open,
			func(id string) proxies.Proxy {
				return proxies.Proxy{Resource: leasing.Resource{ID: id}, URL: "http://" + id + ".example:8080"}
			},
			func(p *proxies.Proxy) *leasing.Resource { return &p.Resource })
	})

	// The URL rides the record; a re-save lands its update.
	t.Run("ModelFieldsRoundTrip", func(t *testing.T) {
		repo := open(t)
		p := proxies.Proxy{
			Resource: leasing.Resource{ID: "p1", GroupID: "residential"},
			URL:      "http://p1.example:8080",
		}
		version, err := repo.Save(ctx, p)
		if err != nil {
			t.Fatalf("Save: %v", err)
		}

		listed, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got := listed[0]; got.URL != "http://p1.example:8080" {
			t.Fatalf("record = %+v", got)
		}

		p.URL, p.Version = "http://new.example", version
		if _, err := repo.Save(ctx, p); err != nil {
			t.Fatalf("second Save: %v", err)
		}
		listed, _ = repo.List(ctx)
		if len(listed) != 1 || listed[0].URL != "http://new.example" {
			t.Fatalf("got %+v, want the replaced record alone", listed)
		}
	})

	// Outcome stats live in the counters: increments surface on List, and a
	// record save can neither smuggle stats in nor clobber ones already
	// tallied — the lost update the counters exist to end.
	t.Run("StatsProjectTheCounters", func(t *testing.T) {
		repo := open(t)
		p := proxies.Proxy{
			Resource:  leasing.Resource{ID: "p1"},
			URL:       "http://p1.example:8080",
			Successes: 7, Failures: 2, // smuggled; must not surface
		}
		version, err := repo.Save(ctx, p)
		if err != nil {
			t.Fatalf("Save: %v", err)
		}

		listed, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if got := listed[0]; got.Successes != 0 || got.Failures != 0 {
			t.Fatalf("stats = %d/%d, want 0/0: record fields must not reach the counters", got.Successes, got.Failures)
		}

		for range 3 {
			if _, err := repo.Increment(ctx, "p1", "successes", 1); err != nil {
				t.Fatalf("Increment: %v", err)
			}
		}
		if _, err := repo.Increment(ctx, "p1", "failures", 1); err != nil {
			t.Fatalf("Increment: %v", err)
		}
		listed, _ = repo.List(ctx)
		if got := listed[0]; got.Successes != 3 || got.Failures != 1 {
			t.Fatalf("stats = %d/%d, want 3/1 from the counters", got.Successes, got.Failures)
		}

		// A re-save of the listed record — stats and all — leaves the tally alone.
		relisted := listed[0]
		relisted.URL, relisted.Version = "http://moved.example", version
		if _, err := repo.Save(ctx, relisted); err != nil {
			t.Fatalf("re-Save: %v", err)
		}
		listed, _ = repo.List(ctx)
		if got := listed[0]; got.URL != "http://moved.example" || got.Successes != 3 || got.Failures != 1 {
			t.Fatalf("after re-save got %+v, want the tally untouched", got)
		}
	})
}
