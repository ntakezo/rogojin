package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
)

// Leasing exercises the leasing.Repository contract shared by every
// leasable model. newRecord builds a fresh record carrying the given id and
// whatever model fields the caller wants along for the ride; resource
// projects out the embedded leasing.Resource so the suite can set and read
// the leasing fields. The projection is a parameter because Leasable's
// core() is unexported by design — only the model's own package can vouch
// for the embedding, so it hands the suite the pointer itself.
//
// Model-specific columns (a proxy's URL and stats, a payment's fields) are
// covered by the per-model suites; this one owns what every store of
// leasables must do with the shared record and its groups.
func Leasing[R any](t *testing.T,
	open func(t *testing.T) leasing.Repository[R],
	newRecord func(id string) R,
	resource func(*R) *leasing.Resource) {
	ctx := context.Background()

	// Records round-trip whole — group, lock owner, holder policy,
	// timestamps — and list in stable id order; an empty store lists an
	// empty, non-nil slice.
	t.Run("SaveListRoundTrip", func(t *testing.T) {
		repo := open(t)
		if listed, err := repo.List(ctx); err != nil || listed == nil || len(listed) != 0 {
			t.Fatalf("List = %v, %v; want empty non-nil, nil", listed, err)
		}

		now := time.Now().UTC().Truncate(time.Millisecond)
		for _, id := range []string{"r2", "r1"} {
			rec := newRecord(id)
			*resource(&rec) = leasing.Resource{ID: id, GroupID: "g1", OwnerID: "t1", MaxHolders: 3, CreatedAt: now, UpdatedAt: now}
			if err := repo.Save(ctx, rec); err != nil {
				t.Fatalf("Save %s: %v", id, err)
			}
		}

		listed, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(listed) != 2 || resource(&listed[0]).ID != "r1" || resource(&listed[1]).ID != "r2" {
			t.Fatalf("got %d records, want r1 then r2", len(listed))
		}
		got := resource(&listed[0])
		if got.GroupID != "g1" || got.OwnerID != "t1" || got.MaxHolders != 3 {
			t.Fatalf("record = %+v", got)
		}
		if !got.CreatedAt.Equal(now) || !got.UpdatedAt.Equal(now) {
			t.Fatalf("timestamps = %v/%v, want %v", got.CreatedAt, got.UpdatedAt, now)
		}
	})

	// The holder policy's whole range survives storage: default 0, a cap,
	// and unlimited.
	t.Run("HolderPolicyRoundTrip", func(t *testing.T) {
		repo := open(t)
		for id, cap := range map[string]int{"inherit": 0, "capped": 4, "unlimited": leasing.UnlimitedHolders} {
			rec := newRecord(id)
			resource(&rec).ID, resource(&rec).MaxHolders = id, cap
			if err := repo.Save(ctx, rec); err != nil {
				t.Fatalf("Save %s: %v", id, err)
			}
		}
		listed, _ := repo.List(ctx)
		byID := map[string]int{}
		for i := range listed {
			byID[resource(&listed[i]).ID] = resource(&listed[i]).MaxHolders
		}
		if byID["inherit"] != 0 || byID["capped"] != 4 || byID["unlimited"] != leasing.UnlimitedHolders {
			t.Fatalf("policies = %v", byID)
		}
	})

	// A second save under the same id replaces the record instead of
	// erroring or duplicating, and never revises CreatedAt — routine
	// writes must not rewrite when a resource was added.
	t.Run("SaveUpsertsPreservingCreatedAt", func(t *testing.T) {
		repo := open(t)
		created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
		rec := newRecord("r1")
		resource(&rec).ID, resource(&rec).CreatedAt, resource(&rec).UpdatedAt = "r1", created, created
		if err := repo.Save(ctx, rec); err != nil {
			t.Fatalf("Save: %v", err)
		}

		later := time.Now().UTC().Truncate(time.Millisecond)
		resource(&rec).OwnerID, resource(&rec).CreatedAt, resource(&rec).UpdatedAt = "t1", later, later
		if err := repo.Save(ctx, rec); err != nil {
			t.Fatalf("second Save: %v", err)
		}

		listed, _ := repo.List(ctx)
		if len(listed) != 1 {
			t.Fatalf("got %d records, want the replaced record alone", len(listed))
		}
		got := resource(&listed[0])
		if !got.CreatedAt.Equal(created) {
			t.Fatalf("CreatedAt = %v, want the original %v", got.CreatedAt, created)
		}
		if got.OwnerID != "t1" || !got.UpdatedAt.Equal(later) {
			t.Fatalf("upsert did not land: %+v", got)
		}
	})

	// Deletes remove the record and absent ids are a no-op.
	t.Run("DeleteIsIdempotent", func(t *testing.T) {
		repo := open(t)
		rec := newRecord("r1")
		resource(&rec).ID = "r1"
		if err := repo.Save(ctx, rec); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if err := repo.Delete(ctx, "r1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if err := repo.Delete(ctx, "r1"); err != nil {
			t.Fatalf("second Delete: %v", err)
		}
		if listed, _ := repo.List(ctx); len(listed) != 0 {
			t.Fatalf("record survived delete: %d left", len(listed))
		}
	})

	// Groups round-trip with strategy and refs in stable id order,
	// preserve CreatedAt across upserts, and delete idempotently.
	t.Run("GroupCRUD", func(t *testing.T) {
		repo := open(t)
		now := time.Now().UTC().Truncate(time.Millisecond)
		for _, id := range []string{"gb", "ga"} {
			g := leasing.Group{ID: id, Strategy: "roundrobin", Refs: map[string]string{"region": "us"}, CreatedAt: now, UpdatedAt: now}
			if err := repo.SaveGroup(ctx, g); err != nil {
				t.Fatalf("SaveGroup %s: %v", id, err)
			}
		}

		listed, err := repo.ListGroups(ctx)
		if err != nil || len(listed) != 2 {
			t.Fatalf("ListGroups = %d, err %v; want 2, nil", len(listed), err)
		}
		if listed[0].ID != "ga" || listed[1].ID != "gb" {
			t.Fatalf("order = %s, %s; want ga then gb", listed[0].ID, listed[1].ID)
		}
		if listed[0].Strategy != "roundrobin" || listed[0].Refs["region"] != "us" || !listed[0].CreatedAt.Equal(now) {
			t.Fatalf("group = %+v", listed[0])
		}

		g := listed[0]
		g.Strategy, g.CreatedAt = "other", now.Add(time.Hour)
		if err := repo.SaveGroup(ctx, g); err != nil {
			t.Fatalf("upsert group: %v", err)
		}
		listed, _ = repo.ListGroups(ctx)
		if listed[0].Strategy != "other" || !listed[0].CreatedAt.Equal(now) {
			t.Fatalf("group upsert = %+v, want strategy other with CreatedAt %v kept", listed[0], now)
		}

		if err := repo.DeleteGroup(ctx, "ga"); err != nil {
			t.Fatalf("DeleteGroup: %v", err)
		}
		if err := repo.DeleteGroup(ctx, "ga"); err != nil {
			t.Fatalf("second DeleteGroup: %v", err)
		}
		if listed, _ = repo.ListGroups(ctx); len(listed) != 1 || listed[0].ID != "gb" {
			t.Fatalf("groups after delete = %d, want gb alone", len(listed))
		}
	})

	// A listed group's refs map and the store's never share memory.
	t.Run("GroupRefsDoNotAlias", func(t *testing.T) {
		repo := open(t)
		refs := map[string]string{"region": "us"}
		if err := repo.SaveGroup(ctx, leasing.Group{ID: "g1", Refs: refs}); err != nil {
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
	})

	// Concurrent writers on distinct ids interleave without losing
	// records. Hold, lock, and counter races join this section as the
	// port grows those primitives.
	t.Run("ConcurrentUse", func(t *testing.T) {
		repo := open(t)
		ids := []string{"c1", "c2", "c3", "c4"}
		var wg sync.WaitGroup
		for _, id := range ids {
			wg.Go(func() {
				rec := newRecord(id)
				resource(&rec).ID = id
				if err := repo.Save(ctx, rec); err != nil {
					t.Errorf("Save %s: %v", id, err)
					return
				}
				if _, err := repo.List(ctx); err != nil {
					t.Errorf("List: %v", err)
				}
			})
		}
		wg.Wait()

		listed, err := repo.List(ctx)
		if err != nil || len(listed) != len(ids) {
			t.Fatalf("List = %d records, err %v; want all %d", len(listed), err, len(ids))
		}
	})
}
