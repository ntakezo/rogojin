package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/storetest"
)

// satisfiesRepositoryPort fails to compile if Proxies drifts from the persistence port it exists to implement.
var _ proxies.Repository = (*Proxies)(nil)

// newTestProxies opens the proxies store on a fresh temp-file database.
func newTestProxies(t *testing.T) proxies.Repository {
	t.Helper()
	repo, err := NewProxies(openTestDB(t))
	if err != nil {
		t.Fatalf("NewProxies: %v", err)
	}
	return repo
}

// TestProxiesContract runs the shared store contract against the sqlite
// proxies store.
func TestProxiesContract(t *testing.T) {
	storetest.Proxies(t, newTestProxies)
}

// TestProxiesPersistsAcrossReopen verifies a saved proxy is what a fresh
// open reads back, stats and lock owner included.
func TestProxiesPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "proxies.db")

	repoDB := openAt(t, dsn)
	repo, err := NewProxies(repoDB)
	if err != nil {
		t.Fatalf("NewProxies: %v", err)
	}
	saved := proxies.Proxy{Resource: leasing.Resource{ID: "p1", OwnerID: "t1"}, URL: "http://h:80"}
	if _, err := repo.Save(ctx, saved); err != nil {
		t.Fatalf("save: %v", err)
	}
	// Stats live in the counters, written the way ReleaseOutcome writes them.
	for name, tally := range map[string]int{"successes": 7, "failures": 3} {
		for range tally {
			if _, err := repo.Increment(ctx, "p1", name, 1); err != nil {
				t.Fatalf("increment %s: %v", name, err)
			}
		}
	}
	saved.Version = 1 // the create landed the record at version 1
	saved.Successes, saved.Failures = 7, 3
	repoDB.Close()

	reopenedDB := openAt(t, dsn)
	reopened, err := NewProxies(reopenedDB)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { reopenedDB.Close() })

	listed, err := reopened.List(ctx)
	if err != nil {
		t.Fatalf("list after reopen: %v", err)
	}
	if len(listed) != 1 || listed[0] != saved {
		t.Fatalf("got %+v, want [%+v]", listed, saved)
	}
}

// TestBackfillCarriesLegacyStatsIntoCounters verifies the upgrade path: a
// database whose proxies still carry row-borne stats — written before the
// counters existed — surfaces them unchanged once the backfill migrations run.
func TestBackfillCarriesLegacyStatsIntoCounters(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "legacy.db")

	// A pre-counters database: the history up to the counters table, with a
	// row whose stats live in the legacy columns.
	legacy := openRawAt(t, dsn)
	preBackfill := proxyMigrations[:len(proxyMigrations)-2]
	if err := migrate(legacy, "proxies", preBackfill); err != nil {
		t.Fatalf("migrate legacy: %v", err)
	}
	if _, err := legacy.Exec(
		`INSERT INTO proxies (id, url, successes, failures, version) VALUES ('p1', 'http://h:80', 9, 4, 1)`); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatalf("close legacy: %v", err)
	}

	upgradedDB := openAt(t, dsn)
	upgraded, err := NewProxies(upgradedDB)
	if err != nil {
		t.Fatalf("NewProxies on legacy database: %v", err)
	}
	listed, err := upgraded.List(ctx)
	if err != nil {
		t.Fatalf("list after upgrade: %v", err)
	}
	if len(listed) != 1 || listed[0].Successes != 9 || listed[0].Failures != 4 {
		t.Fatalf("got %+v, want the legacy stats projected from the backfilled counters", listed)
	}
}
