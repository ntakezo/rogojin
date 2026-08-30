package sqlite

import (
	"context"
	"database/sql"
	"github.com/ntakezo/rogojin/leasing"
	"path/filepath"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/proxies"
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

// TestProxiesSaveListRoundTrip verifies every field — URL, OwnerID, and stats —
// survives storage, because lock reclamation and bayesian learning read them
// back verbatim.
func TestProxiesSaveListRoundTrip(t *testing.T) {
	repo := newTestProxies(t)
	ctx := context.Background()

	locked := proxies.Proxy{Resource: leasing.Resource{ID: "p1", OwnerID: "t1"}, Successes: 3, Failures: 2, URL: "http://u:p@h1:80"}
	free := proxies.Proxy{Resource: leasing.Resource{ID: "p2"}, URL: "http://h2:80"}
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

// TestProxiesSaveUpserts verifies a second Save with the same ID replaces the record,
// because Save is how both stat updates and binding changes are persisted.
func TestProxiesSaveUpserts(t *testing.T) {
	repo := newTestProxies(t)
	ctx := context.Background()

	if err := repo.Save(ctx, proxies.Proxy{Resource: leasing.Resource{ID: "p1"}, URL: "http://h:80"}); err != nil {
		t.Fatalf("first save: %v", err)
	}
	updated := proxies.Proxy{Resource: leasing.Resource{ID: "p1", OwnerID: "t1"}, Successes: 5, Failures: 1, URL: "http://h:80"}
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

// TestProxiesDelete verifies a deleted proxy no longer appears, because DeleteProxy
// removes records the manager has dropped from its pool.
func TestProxiesDelete(t *testing.T) {
	repo := newTestProxies(t)
	ctx := context.Background()

	if err := repo.Save(ctx, proxies.Proxy{Resource: leasing.Resource{ID: "p1"}}); err != nil {
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

// TestProxiesListEmpty verifies an empty store lists cleanly, because a fresh install
// starts with no proxies.
func TestProxiesListEmpty(t *testing.T) {
	listed, err := newTestProxies(t).List(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("got %d proxies, want 0", len(listed))
	}
}

// TestProxiesGroupAndHolderPolicyRoundTrip verifies a proxy's group and holder policy
// survive storage, including UnlimitedHolders — a negative sentinel a naive
// unsigned column would mangle into a cap nobody asked for.
func TestProxiesGroupAndHolderPolicyRoundTrip(t *testing.T) {
	repo := newTestProxies(t)
	ctx := context.Background()

	for _, p := range []proxies.Proxy{
		{Resource: leasing.Resource{ID: "inherit", GroupID: "residential"}},
		{Resource: leasing.Resource{ID: "capped", GroupID: "residential", MaxHolders: 4}},
		{Resource: leasing.Resource{ID: "unlimited", GroupID: "datacenter", MaxHolders: proxies.UnlimitedHolders}},
	} {
		if err := repo.Save(ctx, p); err != nil {
			t.Fatalf("save %s: %v", p.ID, err)
		}
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	byID := map[string]proxies.Proxy{}
	for _, p := range listed {
		byID[p.ID] = p
	}
	if got := byID["inherit"]; got.GroupID != "residential" || got.MaxHolders != 0 {
		t.Fatalf("inherit = %+v, want group residential, policy 0", got)
	}
	if got := byID["capped"]; got.MaxHolders != 4 {
		t.Fatalf("capped MaxHolders = %d, want 4", got.MaxHolders)
	}
	if got := byID["unlimited"]; got.MaxHolders != proxies.UnlimitedHolders || got.GroupID != "datacenter" {
		t.Fatalf("unlimited = %+v, want group datacenter, policy %d", got, proxies.UnlimitedHolders)
	}
}

// TestProxiesProxyTimestampsRoundTrip verifies stamped times survive storage to the
// millisecond, so a consumer can tell when a proxy was added and last used.
func TestProxiesProxyTimestampsRoundTrip(t *testing.T) {
	repo := newTestProxies(t)
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	updated := time.Now().UTC().Truncate(time.Millisecond)
	if err := repo.Save(ctx, proxies.Proxy{Resource: leasing.Resource{ID: "p1", CreatedAt: created, UpdatedAt: updated}}); err != nil {
		t.Fatalf("save: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if !listed[0].CreatedAt.Equal(created) || !listed[0].UpdatedAt.Equal(updated) {
		t.Fatalf("timestamps = %v/%v, want %v/%v", listed[0].CreatedAt, listed[0].UpdatedAt, created, updated)
	}
}

// TestProxiesCreatedAtSurvivesUpserts verifies a re-save never revises creation time.
// Every lease outcome re-saves its proxy, so a Save that carried created_at
// through would let routine stat updates rewrite when a proxy was added.
func TestProxiesCreatedAtSurvivesUpserts(t *testing.T) {
	repo := newTestProxies(t)
	ctx := context.Background()

	created := time.Now().UTC().Add(-24 * time.Hour).Truncate(time.Millisecond)
	if err := repo.Save(ctx, proxies.Proxy{Resource: leasing.Resource{ID: "p1", CreatedAt: created, UpdatedAt: created}}); err != nil {
		t.Fatalf("first save: %v", err)
	}
	later := time.Now().UTC().Truncate(time.Millisecond)
	if err := repo.Save(ctx, proxies.Proxy{Resource: leasing.Resource{ID: "p1", CreatedAt: later, UpdatedAt: later}, Successes: 1}); err != nil {
		t.Fatalf("second save: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if !listed[0].CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt = %v, want the original %v", listed[0].CreatedAt, created)
	}
	if !listed[0].UpdatedAt.Equal(later) {
		t.Fatalf("UpdatedAt = %v, want %v", listed[0].UpdatedAt, later)
	}

	if err := repo.SaveGroup(ctx, proxies.Group{ID: "g", CreatedAt: created, UpdatedAt: created}); err != nil {
		t.Fatalf("first SaveGroup: %v", err)
	}
	if err := repo.SaveGroup(ctx, proxies.Group{ID: "g", Strategy: proxies.StrategyBayesian, CreatedAt: later, UpdatedAt: later}); err != nil {
		t.Fatalf("second SaveGroup: %v", err)
	}
	groups, _ := repo.ListGroups(ctx)
	if !groups[0].CreatedAt.Equal(created) {
		t.Fatalf("group CreatedAt = %v, want the original %v", groups[0].CreatedAt, created)
	}
	if groups[0].Strategy != proxies.StrategyBayesian {
		t.Fatalf("group Strategy = %q, want the updated bayesian", groups[0].Strategy)
	}
}

// TestProxiesGroupCRUD verifies a proxy group round-trips with its strategy, lists in
// id order, and deletes cleanly.
func TestProxiesGroupCRUD(t *testing.T) {
	repo := newTestProxies(t)
	ctx := context.Background()

	if listed, err := repo.ListGroups(ctx); err != nil || len(listed) != 0 {
		t.Fatalf("ListGroups on empty store = %v, err %v; want empty, nil", listed, err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	g := proxies.Group{ID: "residential", Strategy: proxies.StrategyBayesian, CreatedAt: now, UpdatedAt: now}
	if err := repo.SaveGroup(ctx, g); err != nil {
		t.Fatalf("SaveGroup: %v", err)
	}
	if err := repo.SaveGroup(ctx, proxies.Group{ID: "datacenter"}); err != nil {
		t.Fatalf("SaveGroup datacenter: %v", err)
	}

	listed, err := repo.ListGroups(ctx)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListGroups = %d groups, err %v; want 2, nil", len(listed), err)
	}
	if listed[0].ID != "datacenter" || listed[1].ID != "residential" {
		t.Fatalf("groups not in id order: %+v", listed)
	}
	if listed[0].Strategy != "" {
		t.Fatalf("datacenter Strategy = %q, want the empty default", listed[0].Strategy)
	}
	if got := listed[1]; got.Strategy != proxies.StrategyBayesian || !got.CreatedAt.Equal(now) {
		t.Fatalf("residential = %+v, want bayesian/%v", got, now)
	}

	if err := repo.DeleteGroup(ctx, "residential"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	listed, _ = repo.ListGroups(ctx)
	if len(listed) != 1 || listed[0].ID != "datacenter" {
		t.Fatalf("after delete: %+v, want [datacenter]", listed)
	}
}

// TestProxiesLegacyProxiesLandInGlobalGroup verifies the group migration places
// pre-group proxies in the global namespace, so an upgraded pool keeps
// rotating instead of referencing a group that does not exist.
func TestProxiesLegacyProxiesLandInGlobalGroup(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "legacy.db")

	raw, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE proxies (
		id        TEXT PRIMARY KEY,
		url       TEXT NOT NULL DEFAULT '',
		owner_id  TEXT NOT NULL DEFAULT '',
		successes INTEGER NOT NULL DEFAULT 0,
		failures  INTEGER NOT NULL DEFAULT 0
	)`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO proxies (id, url, owner_id, successes, failures)
		VALUES ('p1', 'http://h:80', 't1', 7, 3)`); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	raw.Close()

	repoDB := openAt(t, dsn)
	repo, err := NewProxies(repoDB)
	if err != nil {
		t.Fatalf("NewProxies on legacy db: %v", err)
	}
	t.Cleanup(func() { repoDB.Close() })

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("got %d proxies, want 1", len(listed))
	}
	got := listed[0]
	if got.GroupID != proxies.GlobalGroup {
		t.Fatalf("GroupID = %q, want %q", got.GroupID, proxies.GlobalGroup)
	}
	if got.OwnerID != "t1" || got.Successes != 7 || got.Failures != 3 || got.URL != "http://h:80" {
		t.Fatalf("legacy row not preserved: %+v", got)
	}
	if got.MaxHolders != 0 {
		t.Fatalf("MaxHolders = %d, want 0 (inherit the group's policy)", got.MaxHolders)
	}
}

// TestProxiesPersistsAcrossReopen verifies records — including the OwnerID lock —
// survive closing and reopening the database file, because durable locks past
// a process's lifetime are the requirement this store exists for.
func TestProxiesPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "proxies.db")

	repoDB := openAt(t, dsn)
	repo, err := NewProxies(repoDB)
	if err != nil {
		t.Fatalf("NewProxies: %v", err)
	}
	saved := proxies.Proxy{Resource: leasing.Resource{ID: "p1", OwnerID: "t1"}, Successes: 7, Failures: 3, URL: "http://h:80"}
	if err := repo.Save(ctx, saved); err != nil {
		t.Fatalf("save: %v", err)
	}
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
