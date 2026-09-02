package storetest

import (
	"context"
	"errors"
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
// leasables must do with the shared record, its groups, and the
// coordination facts — holds, locks, counters — the store is the authority
// on.
func Leasing[R any](t *testing.T,
	open func(t *testing.T) leasing.Repository[R],
	newRecord func(id string) R,
	resource func(*R) *leasing.Resource) {
	ctx := context.Background()

	// create saves a fresh record at version 0 and fails the test on refusal.
	create := func(t *testing.T, repo leasing.Repository[R], id string, mutate func(*leasing.Resource)) R {
		t.Helper()
		rec := newRecord(id)
		c := resource(&rec)
		c.ID = id
		if mutate != nil {
			mutate(c)
		}
		version, err := repo.Save(ctx, rec)
		if err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
		c.Version = version
		return rec
	}

	// Records round-trip whole — group, lock owner, holder policy,
	// timestamps — and list in stable id order with the store's version; an
	// empty store lists an empty, non-nil slice.
	t.Run("SaveListRoundTrip", func(t *testing.T) {
		repo := open(t)
		if listed, err := repo.List(ctx); err != nil || listed == nil || len(listed) != 0 {
			t.Fatalf("List = %v, %v; want empty non-nil, nil", listed, err)
		}

		now := time.Now().UTC().Truncate(time.Millisecond)
		for _, id := range []string{"r2", "r1"} {
			create(t, repo, id, func(c *leasing.Resource) {
				c.GroupID, c.OwnerID, c.MaxHolders = "g1", "t1", 3
				c.CreatedAt, c.UpdatedAt = now, now
			})
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
		if got.Version != 1 {
			t.Fatalf("Version = %d, want 1 after the create", got.Version)
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
			create(t, repo, id, func(c *leasing.Resource) { c.MaxHolders = cap })
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

	// Save is a conditional write: version 0 creates once and a second
	// create of the id loses; a matching version replaces, preserving
	// CreatedAt and bumping the version; a stale version and a row deleted
	// under the writer both lose with ErrStale.
	t.Run("SaveIsConditionalOnVersion", func(t *testing.T) {
		repo := open(t)
		created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
		rec := create(t, repo, "r1", func(c *leasing.Resource) { c.CreatedAt, c.UpdatedAt = created, created })

		dup := newRecord("r1")
		resource(&dup).ID = "r1"
		if _, err := repo.Save(ctx, dup); !errors.Is(err, leasing.ErrStale) {
			t.Fatalf("second create = %v, want ErrStale", err)
		}

		later := time.Now().UTC().Truncate(time.Millisecond)
		c := resource(&rec)
		c.OwnerID, c.CreatedAt, c.UpdatedAt = "t1", later, later
		version, err := repo.Save(ctx, rec)
		if err != nil || version != 2 {
			t.Fatalf("conditional replace = version %d, %v; want 2, nil", version, err)
		}

		// The copy still carrying version 1 lost the race and must not land.
		if _, err := repo.Save(ctx, rec); !errors.Is(err, leasing.ErrStale) {
			t.Fatalf("stale replace = %v, want ErrStale", err)
		}

		listed, _ := repo.List(ctx)
		if len(listed) != 1 {
			t.Fatalf("got %d records, want the replaced record alone", len(listed))
		}
		got := resource(&listed[0])
		if !got.CreatedAt.Equal(created) {
			t.Fatalf("CreatedAt = %v, want the original %v", got.CreatedAt, created)
		}
		if got.OwnerID != "t1" || got.Version != 2 {
			t.Fatalf("replace did not land: %+v", got)
		}

		if err := repo.Delete(ctx, "r1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		c.Version = 2
		if _, err := repo.Save(ctx, rec); !errors.Is(err, leasing.ErrStale) {
			t.Fatalf("replace of a deleted row = %v, want ErrStale", err)
		}
	})

	// Deletes remove the record and absent ids are a no-op.
	t.Run("DeleteIsIdempotent", func(t *testing.T) {
		repo := open(t)
		create(t, repo, "r1", nil)
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

	// Acquire admits distinct tasks up to cap and refuses past it with
	// ErrCapacity; re-entering a live hold deepens it without taking a slot;
	// release decrements to removal and no-ops on absent holds.
	t.Run("HoldsEnforceTheCap", func(t *testing.T) {
		repo := open(t)
		ttl := time.Minute

		if _, err := repo.Acquire(ctx, "r1", "t1", 2, ttl); err != nil {
			t.Fatalf("first Acquire: %v", err)
		}
		if _, err := repo.Acquire(ctx, "r1", "t2", 2, ttl); err != nil {
			t.Fatalf("second Acquire: %v", err)
		}
		if _, err := repo.Acquire(ctx, "r1", "t3", 2, ttl); !errors.Is(err, leasing.ErrCapacity) {
			t.Fatalf("over-cap Acquire = %v, want ErrCapacity", err)
		}
		hold, err := repo.Acquire(ctx, "r1", "t1", 2, ttl)
		if err != nil || hold.Count != 2 {
			t.Fatalf("re-entrant Acquire = %+v, %v; want Count 2, nil", hold, err)
		}
		if _, err := repo.Acquire(ctx, "r2", "t3", 0, ttl); err != nil {
			t.Fatalf("unlimited Acquire: %v", err)
		}

		if err := repo.ReleaseHold(ctx, "r1", "t1"); err != nil {
			t.Fatalf("ReleaseHold: %v", err)
		}
		if holds, _ := repo.ListHolds(ctx); len(holds) != 3 {
			t.Fatalf("holds after one release = %d, want 3 (t1 at depth 1, t2, t3)", len(holds))
		}
		if err := repo.ReleaseHold(ctx, "r1", "t1"); err != nil {
			t.Fatalf("second ReleaseHold: %v", err)
		}
		if _, err := repo.Acquire(ctx, "r1", "t3", 2, ttl); err != nil {
			t.Fatalf("Acquire after release = %v, want the freed slot", err)
		}
		if err := repo.ReleaseHold(ctx, "r1", "absent"); err != nil {
			t.Fatalf("ReleaseHold with no hold = %v, want no-op", err)
		}
	})

	// N racers under cap 1 admit exactly one — the property the payments
	// promise stands on.
	t.Run("HoldRacesAdmitOne", func(t *testing.T) {
		repo := open(t)
		var admitted sync.Map
		var wg sync.WaitGroup
		errs := make(chan error, 8)
		for i := range 8 {
			wg.Go(func() {
				_, err := repo.Acquire(ctx, "r1", string(rune('a'+i)), 1, time.Minute)
				if err == nil {
					admitted.Store(i, true)
				} else if !errors.Is(err, leasing.ErrCapacity) {
					errs <- err
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("Acquire: %v", err)
		}
		n := 0
		admitted.Range(func(_, _ any) bool { n++; return true })
		if n != 1 {
			t.Fatalf("admitted %d tasks under cap 1, want exactly 1", n)
		}
	})

	// An expired hold frees its capacity, cannot be revived by renewal, and
	// is superseded on the next acquire; a live hold renews forward.
	t.Run("ExpiryFreesCapacity", func(t *testing.T) {
		repo := open(t)
		if _, err := repo.Acquire(ctx, "r1", "t1", 1, 20*time.Millisecond); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if _, err := repo.Acquire(ctx, "r1", "t2", 1, time.Minute); !errors.Is(err, leasing.ErrCapacity) {
			t.Fatalf("Acquire before expiry = %v, want ErrCapacity", err)
		}
		time.Sleep(30 * time.Millisecond)

		// The lapsed hold is dead: renewal must not bring it back.
		if err := repo.RenewHolds(ctx, "t1", time.Minute); err != nil {
			t.Fatalf("RenewHolds: %v", err)
		}
		if _, err := repo.Acquire(ctx, "r1", "t2", 1, time.Minute); err != nil {
			t.Fatalf("Acquire after expiry = %v, want the slot", err)
		}

		// A live hold renews: t2 keeps its slot past the original ttl.
		if err := repo.RenewHolds(ctx, "t2", time.Hour); err != nil {
			t.Fatalf("RenewHolds t2: %v", err)
		}
		holds, _ := repo.ListHolds(ctx)
		for _, h := range holds {
			if h.TaskID == "t2" && time.Until(h.ExpiresAt) < 30*time.Minute {
				t.Fatalf("t2 expiry = %v, want renewed far forward", h.ExpiresAt)
			}
			if h.TaskID == "t1" {
				t.Fatalf("t1's expired hold survived: %+v", h)
			}
		}
	})

	// RenewHolds extends every hold of the task and nobody else's.
	t.Run("RenewIsPerTask", func(t *testing.T) {
		repo := open(t)
		short := 50 * time.Millisecond
		for _, id := range []string{"r1", "r2"} {
			if _, err := repo.Acquire(ctx, id, "t1", 0, short); err != nil {
				t.Fatalf("Acquire %s: %v", id, err)
			}
		}
		if _, err := repo.Acquire(ctx, "r1", "t2", 0, short); err != nil {
			t.Fatalf("Acquire t2: %v", err)
		}
		if err := repo.RenewHolds(ctx, "t1", time.Hour); err != nil {
			t.Fatalf("RenewHolds: %v", err)
		}
		holds, _ := repo.ListHolds(ctx)
		if len(holds) != 3 {
			t.Fatalf("holds = %d, want 3", len(holds))
		}
		for _, h := range holds {
			far := time.Until(h.ExpiresAt) > 30*time.Minute
			if (h.TaskID == "t1") != far {
				t.Fatalf("hold %+v: only t1's holds renew", h)
			}
		}
	})

	// ClaimLock races admit one owner; the owner re-claims freely; a
	// non-owner's release is a no-op and the owner's frees the lock.
	// A resource locked by another task refuses Acquire with ErrLockHeld —
	// an acquirer's cache may not know about the lock yet, so the store is
	// what says no — while the lock's own task leases it freely.
	t.Run("LockExcludesAcquire", func(t *testing.T) {
		repo := open(t)
		create(t, repo, "r1", nil)
		if err := repo.ClaimLock(ctx, "r1", "owner"); err != nil {
			t.Fatalf("ClaimLock: %v", err)
		}
		if _, err := repo.Acquire(ctx, "r1", "stranger", 0, time.Minute); !errors.Is(err, leasing.ErrLockHeld) {
			t.Fatalf("Acquire by a stranger = %v, want ErrLockHeld", err)
		}
		if _, err := repo.Acquire(ctx, "r1", "owner", 1, time.Minute); err != nil {
			t.Fatalf("Acquire by the owner: %v", err)
		}
	})

	t.Run("LockClaims", func(t *testing.T) {
		repo := open(t)
		create(t, repo, "r1", nil)

		errs := make([]error, 4)
		var wg sync.WaitGroup
		for i := range errs {
			wg.Go(func() {
				errs[i] = repo.ClaimLock(ctx, "r1", string(rune('a'+i)))
			})
		}
		wg.Wait()
		winners := 0
		for _, err := range errs {
			if err == nil {
				winners++
			} else if !errors.Is(err, leasing.ErrLockHeld) {
				t.Fatalf("ClaimLock: %v", err)
			}
		}
		if winners != 1 {
			t.Fatalf("lock winners = %d, want exactly 1", winners)
		}

		listed, _ := repo.List(ctx)
		owner := resource(&listed[0]).OwnerID
		if owner == "" {
			t.Fatal("no owner recorded after a won claim")
		}
		if err := repo.ClaimLock(ctx, "r1", owner); err != nil {
			t.Fatalf("owner re-claim = %v, want idempotent success", err)
		}
		if err := repo.ClaimLock(ctx, "missing", owner); err == nil {
			t.Fatal("ClaimLock on an absent resource = nil, want an error")
		}

		if err := repo.ReleaseLock(ctx, "r1", "not-the-owner"); err != nil {
			t.Fatalf("stranger ReleaseLock = %v, want no-op", err)
		}
		listed, _ = repo.List(ctx)
		if resource(&listed[0]).OwnerID != owner {
			t.Fatal("a stranger's release cleared the lock")
		}
		if err := repo.ReleaseLock(ctx, "r1", owner); err != nil {
			t.Fatalf("owner ReleaseLock: %v", err)
		}
		listed, _ = repo.List(ctx)
		if got := resource(&listed[0]).OwnerID; got != "" {
			t.Fatalf("OwnerID = %q after release, want free", got)
		}
	})

	// Increment starts absent counters at 0, returns the running value, and
	// loses nothing under concurrency.
	t.Run("CountersSumExactly", func(t *testing.T) {
		repo := open(t)
		if v, err := repo.Increment(ctx, "r1", "successes", 3); err != nil || v != 3 {
			t.Fatalf("first Increment = %d, %v; want 3, nil", v, err)
		}
		if v, err := repo.Increment(ctx, "r1", "successes", -1); err != nil || v != 2 {
			t.Fatalf("negative Increment = %d, %v; want 2, nil", v, err)
		}
		if v, err := repo.Increment(ctx, "r1", "failures", 1); err != nil || v != 1 {
			t.Fatalf("sibling counter = %d, %v; want its own 1, nil", v, err)
		}

		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 25 {
					if _, err := repo.Increment(ctx, "g1", "cursor", 1); err != nil {
						t.Errorf("Increment: %v", err)
						return
					}
				}
			})
		}
		wg.Wait()
		if v, err := repo.Increment(ctx, "g1", "cursor", 0); err != nil || v != 200 {
			t.Fatalf("cursor = %d, %v; want every increment counted (200)", v, err)
		}
	})

	// Deleting a resource takes its hold rows with it, so a re-added id
	// starts unheld.
	t.Run("DeleteDropsHolds", func(t *testing.T) {
		repo := open(t)
		create(t, repo, "r1", nil)
		if _, err := repo.Acquire(ctx, "r1", "t1", 1, time.Minute); err != nil {
			t.Fatalf("Acquire: %v", err)
		}
		if err := repo.Delete(ctx, "r1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		create(t, repo, "r1", nil)
		if _, err := repo.Acquire(ctx, "r1", "t2", 1, time.Minute); err != nil {
			t.Fatalf("Acquire after re-add = %v, want the slot free", err)
		}
	})

	// Concurrent writers on distinct ids interleave without losing
	// records.
	t.Run("ConcurrentUse", func(t *testing.T) {
		repo := open(t)
		ids := []string{"c1", "c2", "c3", "c4"}
		var wg sync.WaitGroup
		for _, id := range ids {
			wg.Go(func() {
				rec := newRecord(id)
				resource(&rec).ID = id
				if _, err := repo.Save(ctx, rec); err != nil {
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
