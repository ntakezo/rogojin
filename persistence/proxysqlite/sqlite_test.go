package proxysqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ntakezo/rogojin/proxies"
)

// satisfiesRepositoryPort fails to compile if SQLite drifts from the persistence port it exists to implement.
var _ proxies.Repository = (*SQLite)(nil)

// newTestRepo opens a SQLite repository backed by a fresh temp-file database.
func newTestRepo(t *testing.T) *SQLite {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "proxies.db")
	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { repo.Close() })
	return repo
}

// TestSaveListRoundTrip verifies every field — URL, OwnerID, and stats —
// survives storage, because lock reclamation and bayesian learning read them
// back verbatim.
func TestSaveListRoundTrip(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	locked := proxies.Proxy{ID: "p1", URL: "http://u:p@h1:80", OwnerID: "t1", Successes: 3, Failures: 2}
	free := proxies.Proxy{ID: "p2", URL: "http://h2:80"}
	if err := repo.Save(ctx, locked); err != nil {
		t.Fatalf("save locked: %v", err)
	}
	if err := repo.Save(ctx, free); err != nil {
		t.Fatalf("save free: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("got %d proxies, want 2", len(listed))
	}
	byID := map[string]proxies.Proxy{}
	for _, p := range listed {
		byID[p.ID] = p
	}
	if byID["p1"] != locked {
		t.Fatalf("p1 round-trip: got %+v, want %+v", byID["p1"], locked)
	}
	if byID["p2"] != free {
		t.Fatalf("p2 round-trip: got %+v, want %+v", byID["p2"], free)
	}
}

// TestSaveUpserts verifies a second Save with the same ID replaces the record,
// because Save is how both stat updates and binding changes are persisted.
func TestSaveUpserts(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.Save(ctx, proxies.Proxy{ID: "p1", URL: "http://h:80"}); err != nil {
		t.Fatalf("first save: %v", err)
	}
	updated := proxies.Proxy{ID: "p1", URL: "http://h:80", OwnerID: "t1", Successes: 5, Failures: 1}
	if err := repo.Save(ctx, updated); err != nil {
		t.Fatalf("second save: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("got %d proxies after upsert, want 1", len(listed))
	}
	if listed[0] != updated {
		t.Fatalf("got %+v, want %+v", listed[0], updated)
	}
}

// TestDelete verifies a deleted proxy no longer appears, because DeleteProxy
// removes records the manager has dropped from its pool.
func TestDelete(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.Save(ctx, proxies.Proxy{ID: "p1"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := repo.Delete(ctx, "p1"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("got %d proxies after delete, want 0", len(listed))
	}
}

// TestListEmpty verifies an empty store lists cleanly, because a fresh install
// starts with no proxies.
func TestListEmpty(t *testing.T) {
	listed, err := newTestRepo(t).List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("got %d proxies, want 0", len(listed))
	}
}

// TestPersistsAcrossReopen verifies records — including the OwnerID lock —
// survive closing and reopening the database file, because durable locks past
// a process's lifetime are the requirement this store exists for.
func TestPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "proxies.db")

	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	saved := proxies.Proxy{ID: "p1", URL: "http://h:80", OwnerID: "t1", Successes: 7, Failures: 3}
	if err := repo.Save(ctx, saved); err != nil {
		t.Fatalf("save: %v", err)
	}
	repo.Close()

	reopened, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })

	listed, err := reopened.List(ctx)
	if err != nil {
		t.Fatalf("list after reopen: %v", err)
	}
	if len(listed) != 1 || listed[0] != saved {
		t.Fatalf("got %+v, want [%+v]", listed, saved)
	}
}
