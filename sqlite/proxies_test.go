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
	saved := proxies.Proxy{Resource: leasing.Resource{ID: "p1", OwnerID: "t1"}, Successes: 7, Failures: 3, URL: "http://h:80"}
	if _, err := repo.Save(ctx, saved); err != nil {
		t.Fatalf("save: %v", err)
	}
	saved.Version = 1 // the create landed the record at version 1
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
