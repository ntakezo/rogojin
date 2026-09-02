package memory

import (
	"context"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/proxies"
)

// TestProxiesSaveListRoundTrip verifies a proxy round-trips whole — URL,
// group, policy, stats — and lists in stable id order.
func TestProxiesSaveListRoundTrip(t *testing.T) {
	repo := NewProxies()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, id := range []string{"p2", "p1"} {
		p := proxies.Proxy{
			Resource:  leasing.Resource{ID: id, GroupID: "residential", MaxHolders: 3, CreatedAt: now, UpdatedAt: now},
			URL:       "http://" + id + ".example:8080",
			Successes: 7, Failures: 2,
		}
		if err := repo.Save(ctx, p); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 2 || listed[0].ID != "p1" || listed[1].ID != "p2" {
		t.Fatalf("got %+v, want p1 then p2", listed)
	}
	got := listed[0]
	if got.URL != "http://p1.example:8080" || got.GroupID != "residential" || got.MaxHolders != 3 {
		t.Fatalf("record = %+v", got)
	}
	if got.Successes != 7 || got.Failures != 2 {
		t.Fatalf("stats = %d/%d, want 7/2", got.Successes, got.Failures)
	}
	if !got.CreatedAt.Equal(now) || !got.UpdatedAt.Equal(now) {
		t.Fatalf("timestamps = %v/%v, want %v", got.CreatedAt, got.UpdatedAt, now)
	}
}

// TestProxiesSaveUpserts verifies a second save under the same id replaces
// the record instead of erroring or duplicating.
func TestProxiesSaveUpserts(t *testing.T) {
	repo := NewProxies()
	ctx := context.Background()

	p := proxies.Proxy{Resource: leasing.Resource{ID: "p1"}, URL: "http://old.example"}
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("Save: %v", err)
	}
	p.URL, p.Successes = "http://new.example", 5
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	listed, _ := repo.List(ctx)
	if len(listed) != 1 || listed[0].URL != "http://new.example" || listed[0].Successes != 5 {
		t.Fatalf("got %+v, want the replaced record alone", listed)
	}
}

// TestProxiesCreatedAtSurvivesUpserts verifies a stat update or lock write
// never revises when the proxy was created.
func TestProxiesCreatedAtSurvivesUpserts(t *testing.T) {
	repo := NewProxies()
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	p := proxies.Proxy{Resource: leasing.Resource{ID: "p1", CreatedAt: created, UpdatedAt: created}}
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("Save: %v", err)
	}

	later := time.Now().UTC().Truncate(time.Millisecond)
	p.OwnerID, p.Failures, p.CreatedAt, p.UpdatedAt = "t1", 1, later, later
	if err := repo.Save(ctx, p); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	listed, _ := repo.List(ctx)
	if !listed[0].CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt = %v, want the original %v", listed[0].CreatedAt, created)
	}
	if listed[0].OwnerID != "t1" || listed[0].Failures != 1 || !listed[0].UpdatedAt.Equal(later) {
		t.Fatalf("upsert did not land: %+v", listed[0])
	}
}

// TestProxiesDelete verifies deletes remove the record and absent ids are a
// no-op.
func TestProxiesDelete(t *testing.T) {
	repo := NewProxies()
	ctx := context.Background()

	if err := repo.Save(ctx, proxies.Proxy{Resource: leasing.Resource{ID: "p1"}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.Delete(ctx, "p1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := repo.Delete(ctx, "p1"); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if listed, _ := repo.List(ctx); len(listed) != 0 {
		t.Fatalf("record survived delete: %+v", listed)
	}
}

// TestProxiesListEmpty verifies an empty store lists an empty, non-nil slice.
func TestProxiesListEmpty(t *testing.T) {
	repo := NewProxies()
	listed, err := repo.List(context.Background())
	if err != nil || listed == nil || len(listed) != 0 {
		t.Fatalf("List = %v, %v; want empty non-nil, nil", listed, err)
	}
}

// TestProxiesGroupCRUD verifies groups round-trip with strategy and refs in
// stable order, preserve CreatedAt across upserts, and delete idempotently.
func TestProxiesGroupCRUD(t *testing.T) {
	repo := NewProxies()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, id := range []string{"residential", "datacenter"} {
		g := proxies.Group{ID: id, Strategy: "bayesian", Refs: map[string]string{"region": "us"}, CreatedAt: now, UpdatedAt: now}
		if err := repo.SaveGroup(ctx, g); err != nil {
			t.Fatalf("SaveGroup %s: %v", id, err)
		}
	}

	listed, err := repo.ListGroups(ctx)
	if err != nil || len(listed) != 2 {
		t.Fatalf("ListGroups = %d, err %v; want 2, nil", len(listed), err)
	}
	if listed[0].ID != "datacenter" || listed[1].ID != "residential" {
		t.Fatalf("order = %s, %s; want datacenter then residential", listed[0].ID, listed[1].ID)
	}
	if listed[0].Strategy != "bayesian" || listed[0].Refs["region"] != "us" || !listed[0].CreatedAt.Equal(now) {
		t.Fatalf("group = %+v", listed[0])
	}

	g := listed[1]
	g.Strategy, g.CreatedAt = "roundrobin", now.Add(time.Hour)
	if err := repo.SaveGroup(ctx, g); err != nil {
		t.Fatalf("upsert group: %v", err)
	}
	listed, _ = repo.ListGroups(ctx)
	if listed[1].Strategy != "roundrobin" || !listed[1].CreatedAt.Equal(now) {
		t.Fatalf("group upsert = %+v, want roundrobin with CreatedAt %v kept", listed[1], now)
	}

	if err := repo.DeleteGroup(ctx, "residential"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	if err := repo.DeleteGroup(ctx, "residential"); err != nil {
		t.Fatalf("second DeleteGroup: %v", err)
	}
	if listed, _ = repo.ListGroups(ctx); len(listed) != 1 || listed[0].ID != "datacenter" {
		t.Fatalf("groups after delete = %+v, want datacenter alone", listed)
	}
}

// TestProxiesGroupRefsDoNotAliasTheStore verifies a listed group's refs map
// and the store's never share memory.
func TestProxiesGroupRefsDoNotAliasTheStore(t *testing.T) {
	repo := NewProxies()
	ctx := context.Background()

	refs := map[string]string{"region": "us"}
	if err := repo.SaveGroup(ctx, proxies.Group{ID: "g1", Refs: refs}); err != nil {
		t.Fatalf("SaveGroup: %v", err)
	}
	refs["region"] = "hijacked"

	listed, _ := repo.ListGroups(ctx)
	if listed[0].Refs["region"] != "us" {
		t.Fatalf("saved refs mutated through the caller: %v", listed[0].Refs)
	}
	listed[0].Refs["region"] = "leaked"
	again, _ := repo.ListGroups(ctx)
	if again[0].Refs["region"] != "us" {
		t.Fatalf("listed refs share memory with the store: %v", again[0].Refs)
	}
}
