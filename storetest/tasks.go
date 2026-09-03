package storetest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/tasks"
)

// The resource kinds the suite stores placements under; a conforming store
// never interprets a kind beyond validating its charset.
const (
	proxyKind   leasing.Kind = "proxy"
	accountKind leasing.Kind = "account"
)

// seedTask inserts a task in the global group with fresh timestamps.
func seedTask(t *testing.T, repo tasks.Repository, id, workflowID string) {
	t.Helper()
	now := time.Now().UTC()
	rec := tasks.Task{ID: id, WorkflowID: workflowID, GroupID: tasks.GlobalGroup, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateTask(context.Background(), rec); err != nil {
		t.Fatalf("CreateTask %s: %v", id, err)
	}
}

// claimed takes node's claim on the task and returns the version its next
// conditional write must carry — every write is conditional, so cases whose
// subject is something else still claim like a run would.
func claimed(t *testing.T, repo tasks.Repository, id, node string) int64 {
	t.Helper()
	rec, err := repo.ClaimTask(context.Background(), id, node, time.Minute)
	if err != nil {
		t.Fatalf("ClaimTask %s: %v", id, err)
	}
	return rec.Version
}

// Tasks exercises the full tasks.Repository contract against the store the
// factory opens. The factory runs once per subtest; use t.Cleanup to close.
func Tasks(t *testing.T, open func(t *testing.T) tasks.Repository) {
	ctx := context.Background()

	// A created task is recoverable by id with its workflow and no
	// checkpoint yet; a duplicate id is refused; checkpoint fields smuggled
	// into the create are dropped, exactly as the insert's column list
	// drops them.
	t.Run("CreateTask", func(t *testing.T) {
		repo := open(t)
		seedTask(t, repo, "t1", "wf1")

		rec, err := repo.RecoverTask(ctx, "t1")
		if err != nil {
			t.Fatalf("RecoverTask: %v", err)
		}
		if rec.ID != "t1" || rec.WorkflowID != "wf1" {
			t.Fatalf("got %+v, want id=t1 workflow=wf1", rec)
		}
		if rec.State != "" || rec.Status != "" || len(rec.Snapshot) != 0 {
			t.Fatalf("fresh task should have no checkpoint, got %+v", rec)
		}

		if err := repo.CreateTask(ctx, tasks.Task{ID: "t1", WorkflowID: "wf2"}); err == nil {
			t.Fatal("CreateTask duplicate: want an error, got nil")
		}

		smuggled := tasks.Task{ID: "t2", WorkflowID: "wf1", State: "smuggled", Status: "running",
			Snapshot: []byte("snap"), Output: []byte("out")}
		if err := repo.CreateTask(ctx, smuggled); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
		got, _ := repo.RecoverTask(ctx, "t2")
		if got.State != "" || got.Status != "" || got.Snapshot != nil || got.Output != nil {
			t.Fatalf("create persisted checkpoint fields: %+v", got)
		}
	})

	// A checkpoint's status, state, and snapshot survive recovery, and a
	// later checkpoint replaces an earlier one.
	t.Run("SaveCheckpoint", func(t *testing.T) {
		repo := open(t)
		seedTask(t, repo, "t1", "wf1")

		v := claimed(t, repo, "t1", "node-a")
		v, err := repo.SaveCheckpoint(ctx, "t1", v, "node-a", "running", "add_to_cart", []byte(`{"cart":"abc"}`))
		if err != nil {
			t.Fatalf("SaveCheckpoint: %v", err)
		}
		rec, err := repo.RecoverTask(ctx, "t1")
		if err != nil {
			t.Fatalf("RecoverTask: %v", err)
		}
		if rec.Status != "running" || rec.State != "add_to_cart" || string(rec.Snapshot) != `{"cart":"abc"}` {
			t.Fatalf("got %+v, want running/add_to_cart checkpoint", rec)
		}

		if _, err := repo.SaveCheckpoint(ctx, "t1", v, "node-a", "suspended", "s2", []byte("b")); err != nil {
			t.Fatalf("SaveCheckpoint 2: %v", err)
		}
		rec, _ = repo.RecoverTask(ctx, "t1")
		if rec.Status != "suspended" || rec.State != "s2" || string(rec.Snapshot) != "b" {
			t.Fatalf("got %+v, want last checkpoint suspended/s2/b", rec)
		}
	})

	// A terminal outcome updates status and stores the output while leaving
	// state and snapshot intact as a resume entry.
	t.Run("MarkTerminal", func(t *testing.T) {
		repo := open(t)
		seedTask(t, repo, "t1", "wf1")
		v := claimed(t, repo, "t1", "node-a")
		v, err := repo.SaveCheckpoint(ctx, "t1", v, "node-a", "running", "submit", []byte("snap"))
		if err != nil {
			t.Fatalf("SaveCheckpoint: %v", err)
		}
		out := []byte(`{"orderID":"order-1"}`)
		if _, err := repo.MarkTerminal(ctx, "t1", v, "node-a", "done", out); err != nil {
			t.Fatalf("MarkTerminal: %v", err)
		}

		rec, _ := repo.RecoverTask(ctx, "t1")
		if rec.Status != "done" || rec.State != "submit" || string(rec.Snapshot) != "snap" {
			t.Fatalf("terminal disturbed state/snapshot: %+v", rec)
		}
		if string(rec.Output) != string(out) {
			t.Fatalf("output = %q, want %q", rec.Output, out)
		}
	})

	// A missing task is tasks.ErrTaskNotFound — never a silent no-row
	// update, never conflated with a store failure.
	t.Run("MissingTaskFailsLoud", func(t *testing.T) {
		repo := open(t)
		if _, err := repo.RecoverTask(ctx, "nope"); !errors.Is(err, tasks.ErrTaskNotFound) {
			t.Fatalf("RecoverTask: err = %v, want tasks.ErrTaskNotFound", err)
		}
		if _, err := repo.SaveCheckpoint(ctx, "ghost", 1, "node-a", "running", "s1", nil); !errors.Is(err, tasks.ErrTaskNotFound) {
			t.Fatalf("SaveCheckpoint: err = %v, want tasks.ErrTaskNotFound", err)
		}
		if _, err := repo.MarkTerminal(ctx, "ghost", 1, "node-a", "done", nil); !errors.Is(err, tasks.ErrTaskNotFound) {
			t.Fatalf("MarkTerminal: err = %v, want tasks.ErrTaskNotFound", err)
		}
	})

	// RecoverAll returns every persisted task, terminal ones included, in
	// insertion order; an empty store returns an empty, non-nil slice.
	t.Run("RecoverAll", func(t *testing.T) {
		repo := open(t)
		if recs, err := repo.RecoverAll(ctx); err != nil || recs == nil || len(recs) != 0 {
			t.Fatalf("RecoverAll = %v, %v; want empty non-nil, nil", recs, err)
		}

		seedTask(t, repo, "t1", "wf1")
		seedTask(t, repo, "t2", "wf2")
		if _, err := repo.MarkTerminal(ctx, "t2", claimed(t, repo, "t2", "node-a"), "node-a", "done", nil); err != nil {
			t.Fatalf("MarkTerminal: %v", err)
		}

		recs, err := repo.RecoverAll(ctx)
		if err != nil {
			t.Fatalf("RecoverAll: %v", err)
		}
		if len(recs) != 2 || recs[0].ID != "t1" || recs[1].ID != "t2" {
			t.Fatalf("got %+v, want t1 then t2", recs)
		}
		if recs[0].WorkflowID != "wf1" || recs[1].Status != "done" {
			t.Fatalf("unexpected records: %+v", recs)
		}
	})

	// A deleted task is gone, and a second delete is a no-op.
	t.Run("DeleteTask", func(t *testing.T) {
		repo := open(t)
		seedTask(t, repo, "t1", "wf1")
		if err := repo.DeleteTask(ctx, "t1"); err != nil {
			t.Fatalf("DeleteTask: %v", err)
		}
		if _, err := repo.RecoverTask(ctx, "t1"); !errors.Is(err, tasks.ErrTaskNotFound) {
			t.Fatalf("err = %v, want tasks.ErrTaskNotFound after delete", err)
		}
		if err := repo.DeleteTask(ctx, "t1"); err != nil {
			t.Fatalf("second DeleteTask: %v", err)
		}
	})

	// The three distinct group assignments a task can carry survive
	// storage: nil (inherit), "" (lease none), and a named group.
	t.Run("AssignmentTriState", func(t *testing.T) {
		repo := open(t)
		none, named := "", "residential"
		for _, tc := range []struct {
			id       string
			assigned *string
		}{
			{"inherit", nil},
			{"opted-out", &none},
			{"named", &named},
		} {
			rec := tasks.Task{ID: tc.id, WorkflowID: "wf1", GroupID: "g1",
				Assignments: map[leasing.Kind]tasks.Assignment{proxyKind: {GroupID: tc.assigned}}}
			if err := repo.CreateTask(ctx, rec); err != nil {
				t.Fatalf("CreateTask %s: %v", tc.id, err)
			}
			got, err := repo.RecoverTask(ctx, tc.id)
			if err != nil {
				t.Fatalf("RecoverTask %s: %v", tc.id, err)
			}
			stored := got.Assignments[proxyKind].GroupID
			switch {
			case tc.assigned == nil && stored != nil:
				t.Fatalf("%s: group = %q, want nil (inherit)", tc.id, *stored)
			case tc.assigned != nil && stored == nil:
				t.Fatalf("%s: group = nil, want %q", tc.id, *tc.assigned)
			case tc.assigned != nil && *stored != *tc.assigned:
				t.Fatalf("%s: group = %q, want %q", tc.id, *stored, *tc.assigned)
			}
		}
	})

	// A pin carries the same tri-state as a group assignment.
	t.Run("PinTriState", func(t *testing.T) {
		repo := open(t)
		group, none, named := "residential", "", "p7"
		for _, tc := range []struct {
			id     string
			pinned *string
		}{
			{"unpinned", nil},
			{"cleared", &none},
			{"pinned", &named},
		} {
			rec := tasks.Task{ID: tc.id, WorkflowID: "wf1", GroupID: "g1",
				Assignments: map[leasing.Kind]tasks.Assignment{proxyKind: {GroupID: &group, ResourceID: tc.pinned}}}
			if err := repo.CreateTask(ctx, rec); err != nil {
				t.Fatalf("CreateTask %s: %v", tc.id, err)
			}
			got, err := repo.RecoverTask(ctx, tc.id)
			if err != nil {
				t.Fatalf("RecoverTask %s: %v", tc.id, err)
			}
			stored := got.Assignments[proxyKind].ResourceID
			switch {
			case tc.pinned == nil && stored != nil:
				t.Fatalf("%s: pin = %q, want nil (unpinned)", tc.id, *stored)
			case tc.pinned != nil && stored == nil:
				t.Fatalf("%s: pin = nil, want %q", tc.id, *tc.pinned)
			case tc.pinned != nil && *stored != *tc.pinned:
				t.Fatalf("%s: pin = %q, want %q", tc.id, *stored, *tc.pinned)
			}
		}
	})

	// One record carries a distinct placement per kind, each with its own
	// tri-state halves.
	t.Run("AssignmentsIndependentPerKind", func(t *testing.T) {
		repo := open(t)
		proxyGroup, none, accountPin := "residential", "", "buyer-1"
		rec := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: "g1", Assignments: map[leasing.Kind]tasks.Assignment{
			proxyKind:   {GroupID: &proxyGroup, ResourceID: &none},
			accountKind: {ResourceID: &accountPin},
		}}
		if err := repo.CreateTask(ctx, rec); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}

		got, err := repo.RecoverTask(ctx, "t1")
		if err != nil {
			t.Fatalf("RecoverTask: %v", err)
		}
		proxy, account := got.Assignments[proxyKind], got.Assignments[accountKind]
		if proxy.GroupID == nil || *proxy.GroupID != "residential" {
			t.Fatalf("proxy group = %v, want residential", proxy.GroupID)
		}
		if proxy.ResourceID == nil || *proxy.ResourceID != "" {
			t.Fatalf("proxy pin = %v, want explicit empty", proxy.ResourceID)
		}
		if account.GroupID != nil {
			t.Fatalf("account group = %q, want nil (inherit)", *account.GroupID)
		}
		if account.ResourceID == nil || *account.ResourceID != "buyer-1" {
			t.Fatalf("account pin = %v, want buyer-1", account.ResourceID)
		}
	})

	// SaveAssignment rewrites one kind's placement whole, leaves every
	// other kind and the checkpoint alone, adds a kind the record never
	// carried, lands on an unplaced task, and reports a missing one.
	t.Run("SaveAssignment", func(t *testing.T) {
		repo := open(t)
		group, pin, accountPin := "residential", "p1", "buyer-1"
		rec := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: "g1", Assignments: map[leasing.Kind]tasks.Assignment{
			proxyKind:   {GroupID: &group, ResourceID: &pin},
			accountKind: {ResourceID: &accountPin},
		}}
		if err := repo.CreateTask(ctx, rec); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
		if _, err := repo.SaveCheckpoint(ctx, "t1", claimed(t, repo, "t1", "node-a"), "node-a", "running", "s1", []byte(`{"v":1}`)); err != nil {
			t.Fatalf("SaveCheckpoint: %v", err)
		}

		moved, repinned := "datacenter", "p9"
		if err := repo.SaveAssignment(ctx, "t1", proxyKind, tasks.Assignment{GroupID: &moved, ResourceID: &repinned}); err != nil {
			t.Fatalf("SaveAssignment: %v", err)
		}
		got, err := repo.RecoverTask(ctx, "t1")
		if err != nil {
			t.Fatalf("RecoverTask: %v", err)
		}
		proxy := got.Assignments[proxyKind]
		if proxy.GroupID == nil || *proxy.GroupID != "datacenter" || proxy.ResourceID == nil || *proxy.ResourceID != "p9" {
			t.Fatalf("proxy placement = %v/%v, want datacenter/p9", proxy.GroupID, proxy.ResourceID)
		}
		if account := got.Assignments[accountKind].ResourceID; account == nil || *account != "buyer-1" {
			t.Fatalf("account pin = %v, want the untouched buyer-1", account)
		}
		if got.State != "s1" || got.Status != "running" || string(got.Snapshot) != `{"v":1}` {
			t.Fatalf("reassignment disturbed the checkpoint: %+v", got)
		}

		// Clearing both halves stores the empty assignment, not a removal.
		if err := repo.SaveAssignment(ctx, "t1", proxyKind, tasks.Assignment{}); err != nil {
			t.Fatalf("SaveAssignment clearing: %v", err)
		}
		got, _ = repo.RecoverTask(ctx, "t1")
		if cleared := got.Assignments[proxyKind]; cleared.GroupID != nil || cleared.ResourceID != nil {
			t.Fatalf("cleared placement = %v/%v, want nil/nil", cleared.GroupID, cleared.ResourceID)
		}

		// A kind the record never carried is added, not rejected.
		fresh := "sms"
		if err := repo.SaveAssignment(ctx, "t1", "phone", tasks.Assignment{GroupID: &fresh}); err != nil {
			t.Fatalf("SaveAssignment new kind: %v", err)
		}
		got, _ = repo.RecoverTask(ctx, "t1")
		if phone := got.Assignments["phone"].GroupID; phone == nil || *phone != "sms" {
			t.Fatalf("phone group = %v, want sms", phone)
		}

		// The first assignment on an unplaced task lands.
		seedTask(t, repo, "t2", "wf1")
		if err := repo.SaveAssignment(ctx, "t2", proxyKind, tasks.Assignment{GroupID: &group}); err != nil {
			t.Fatalf("SaveAssignment on unplaced: %v", err)
		}
		got, _ = repo.RecoverTask(ctx, "t2")
		if stored := got.Assignments[proxyKind].GroupID; stored == nil || *stored != "residential" {
			t.Fatalf("proxy group = %v, want residential", stored)
		}

		if err := repo.SaveAssignment(ctx, "missing", proxyKind, tasks.Assignment{GroupID: &moved}); err == nil {
			t.Fatal("expected an error assigning a task that does not exist")
		}
	})

	// The store's own kind guard refuses names outside the charset and
	// leaves the record untouched — a kind becomes a key in the stored
	// placement, so an unsafe one must never reach it.
	t.Run("SaveAssignmentRefusesUnsafeKind", func(t *testing.T) {
		repo := open(t)
		group := "residential"
		rec := tasks.Task{ID: "t1", WorkflowID: "wf1", Assignments: map[leasing.Kind]tasks.Assignment{
			proxyKind: {GroupID: &group},
		}}
		if err := repo.CreateTask(ctx, rec); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}

		other := "g"
		for _, kind := range []leasing.Kind{"x.y", "a[0]", "", `q"o`} {
			if err := repo.SaveAssignment(ctx, "t1", kind, tasks.Assignment{GroupID: &other}); err == nil {
				t.Errorf("SaveAssignment with kind %q = nil, want an error", kind)
			}
		}

		got, _ := repo.RecoverTask(ctx, "t1")
		if len(got.Assignments) != 1 {
			t.Fatalf("assignments = %v, want the single proxy placement untouched", got.Assignments)
		}
	})

	// CreatedAt survives storage unchanged while the store stamps
	// UpdatedAt forward on every write it makes.
	t.Run("Timestamps", func(t *testing.T) {
		repo := open(t)
		created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
		rec := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: tasks.GlobalGroup, CreatedAt: created, UpdatedAt: created}
		if err := repo.CreateTask(ctx, rec); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}

		got, _ := repo.RecoverTask(ctx, "t1")
		if !got.CreatedAt.Equal(created) || !got.UpdatedAt.Equal(created) {
			t.Fatalf("timestamps = %v/%v, want both %v", got.CreatedAt, got.UpdatedAt, created)
		}

		if _, err := repo.SaveCheckpoint(ctx, "t1", claimed(t, repo, "t1", "node-a"), "node-a", "running", "s1", nil); err != nil {
			t.Fatalf("SaveCheckpoint: %v", err)
		}
		got, _ = repo.RecoverTask(ctx, "t1")
		if !got.CreatedAt.Equal(created) {
			t.Fatalf("CreatedAt = %v, want it untouched at %v", got.CreatedAt, created)
		}
		if !got.UpdatedAt.After(created) {
			t.Fatalf("UpdatedAt = %v, want it moved past %v", got.UpdatedAt, created)
		}
	})

	// A task group round-trips with its per-kind resource assignments, a
	// missing group reports found=false, upserts keep CreatedAt, and
	// deletion removes it.
	t.Run("GroupCRUD", func(t *testing.T) {
		repo := open(t)
		if _, found, err := repo.GetGroup(ctx, "ghost"); err != nil || found {
			t.Fatalf("GetGroup(ghost) = found %v, err %v; want false, nil", found, err)
		}

		now := time.Now().UTC().Truncate(time.Millisecond)
		g := tasks.Group{ID: "g1", CreatedAt: now, UpdatedAt: now, ResourceGroups: map[leasing.Kind]string{
			proxyKind:   "residential",
			accountKind: "shoppers",
		}}
		if err := repo.SaveGroup(ctx, g); err != nil {
			t.Fatalf("SaveGroup: %v", err)
		}

		got, found, err := repo.GetGroup(ctx, "g1")
		if err != nil || !found {
			t.Fatalf("GetGroup(g1) = found %v, err %v; want true, nil", found, err)
		}
		if got.ResourceGroups[proxyKind] != "residential" || got.ResourceGroups[accountKind] != "shoppers" {
			t.Fatalf("resource groups = %v", got.ResourceGroups)
		}
		if !got.CreatedAt.Equal(now) {
			t.Fatalf("CreatedAt = %v, want %v", got.CreatedAt, now)
		}

		if err := repo.SaveGroup(ctx, tasks.Group{ID: "g1", CreatedAt: now.Add(time.Hour), UpdatedAt: now.Add(time.Hour)}); err != nil {
			t.Fatalf("SaveGroup again: %v", err)
		}
		got, _, _ = repo.GetGroup(ctx, "g1")
		if !got.CreatedAt.Equal(now) {
			t.Fatalf("CreatedAt revised to %v, want %v kept", got.CreatedAt, now)
		}

		listed, err := repo.ListGroups(ctx)
		if err != nil || len(listed) != 1 {
			t.Fatalf("ListGroups = %d groups, err %v; want 1, nil", len(listed), err)
		}

		if err := repo.DeleteGroup(ctx, "g1"); err != nil {
			t.Fatalf("DeleteGroup: %v", err)
		}
		if _, found, _ := repo.GetGroup(ctx, "g1"); found {
			t.Fatal("group survived delete")
		}
	})

	// Membership lookup returns exactly the group's tasks in stable id
	// order, and an unknown group is empty, not an error.
	t.Run("TasksInGroup", func(t *testing.T) {
		repo := open(t)
		for _, tc := range []struct{ id, group string }{
			{"a2", "ga"}, {"a1", "ga"}, {"b1", "gb"},
		} {
			if err := repo.CreateTask(ctx, tasks.Task{ID: tc.id, WorkflowID: "wf1", GroupID: tc.group}); err != nil {
				t.Fatalf("CreateTask %s: %v", tc.id, err)
			}
		}

		ids, err := repo.TasksInGroup(ctx, "ga")
		if err != nil {
			t.Fatalf("TasksInGroup: %v", err)
		}
		if len(ids) != 2 || ids[0] != "a1" || ids[1] != "a2" {
			t.Fatalf("got %v, want [a1 a2]", ids)
		}
		if empty, err := repo.TasksInGroup(ctx, "ghost"); err != nil || len(empty) != 0 {
			t.Fatalf("TasksInGroup(ghost) = %v, err %v; want empty, nil", empty, err)
		}
	})

	// The store and its callers never share memory: mutating what was
	// saved or recovered leaves the store's copy alone — the property
	// serialization gives the file-backed stores for free.
	t.Run("CopiesAtTheBoundary", func(t *testing.T) {
		repo := open(t)
		group := "residential"
		rec := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: "g1",
			Assignments: map[leasing.Kind]tasks.Assignment{proxyKind: {GroupID: &group}}}
		if err := repo.CreateTask(ctx, rec); err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
		snap := []byte("snap")
		if _, err := repo.SaveCheckpoint(ctx, "t1", claimed(t, repo, "t1", "node-a"), "node-a", "running", "s1", snap); err != nil {
			t.Fatalf("SaveCheckpoint: %v", err)
		}

		group = "hijacked"
		rec.Assignments[proxyKind] = tasks.Assignment{}
		snap[0] = 'X'

		got, _ := repo.RecoverTask(ctx, "t1")
		if stored := got.Assignments[proxyKind].GroupID; stored == nil || *stored != "residential" {
			t.Fatalf("saved assignment mutated through the caller: %v", stored)
		}
		if string(got.Snapshot) != "snap" {
			t.Fatalf("saved snapshot mutated through the caller: %q", got.Snapshot)
		}

		*got.Assignments[proxyKind].GroupID = "leaked"
		got.Snapshot[0] = 'Y'
		again, _ := repo.RecoverTask(ctx, "t1")
		if stored := again.Assignments[proxyKind].GroupID; *stored != "residential" || string(again.Snapshot) != "snap" {
			t.Fatalf("recovered copy shares memory with the store: %v %q", *stored, again.Snapshot)
		}
	})

	// Concurrent writers and readers on distinct tasks interleave without
	// losing records. Claim, version, and effect races join this section
	// as the port grows those primitives.
	t.Run("ConcurrentUse", func(t *testing.T) {
		repo := open(t)
		var wg sync.WaitGroup
		for _, id := range []string{"c1", "c2", "c3", "c4"} {
			wg.Go(func() {
				now := time.Now().UTC()
				rec := tasks.Task{ID: id, WorkflowID: "wf1", GroupID: tasks.GlobalGroup, CreatedAt: now, UpdatedAt: now}
				if err := repo.CreateTask(ctx, rec); err != nil {
					t.Errorf("CreateTask %s: %v", id, err)
					return
				}
				claimedRec, err := repo.ClaimTask(ctx, id, "node-"+id, time.Minute)
				if err != nil {
					t.Errorf("ClaimTask %s: %v", id, err)
					return
				}
				if _, err := repo.SaveCheckpoint(ctx, id, claimedRec.Version, "node-"+id, "running", "s1", []byte(id)); err != nil {
					t.Errorf("SaveCheckpoint %s: %v", id, err)
				}
				if _, err := repo.RecoverAll(ctx); err != nil {
					t.Errorf("RecoverAll: %v", err)
				}
			})
		}
		wg.Wait()

		recs, err := repo.RecoverAll(ctx)
		if err != nil || len(recs) != 4 {
			t.Fatalf("RecoverAll = %d records, err %v; want all 4", len(recs), err)
		}
	})

	// The claim primitives: the store is the authority on which node runs a
	// task and until when. Expiry is judged by the store's own clock.
	t.Run("Claims", func(t *testing.T) {
		// A ttl long enough to never expire inside a test.
		const forever = time.Minute

		t.Run("ClaimRoundTrip", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")

			before := time.Now()
			rec, err := repo.ClaimTask(ctx, "t1", "node-a", forever)
			if err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}
			if rec.OwnerNode != "node-a" || rec.Version == 0 {
				t.Fatalf("claimed record = owner %q version %d, want node-a with a bumped version", rec.OwnerNode, rec.Version)
			}
			if !rec.LeaseExpiresAt.After(before) {
				t.Fatalf("lease expires %v, want after the claim", rec.LeaseExpiresAt)
			}

			kept, err := repo.RecoverTask(ctx, "t1")
			if err != nil {
				t.Fatalf("RecoverTask: %v", err)
			}
			if kept.OwnerNode != rec.OwnerNode || kept.Version != rec.Version || !kept.LeaseExpiresAt.Equal(rec.LeaseExpiresAt) {
				t.Fatalf("recovered claim %q v%d %v, want the claimed %q v%d %v",
					kept.OwnerNode, kept.Version, kept.LeaseExpiresAt, rec.OwnerNode, rec.Version, rec.LeaseExpiresAt)
			}

			if _, err := repo.ClaimTask(ctx, "ghost", "node-a", forever); !errors.Is(err, tasks.ErrTaskNotFound) {
				t.Fatalf("ClaimTask ghost: err = %v, want tasks.ErrTaskNotFound", err)
			}
		})

		t.Run("HeldClaimRefusesAnotherNode", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			if _, err := repo.ClaimTask(ctx, "t1", "node-a", forever); err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}

			if _, err := repo.ClaimTask(ctx, "t1", "node-b", forever); !errors.Is(err, tasks.ErrClaimHeld) {
				t.Fatalf("ClaimTask by node-b: err = %v, want tasks.ErrClaimHeld", err)
			}
			// The same node re-claims freely — recovery after a local restart
			// inside one lease.
			if _, err := repo.ClaimTask(ctx, "t1", "node-a", forever); err != nil {
				t.Fatalf("re-claim by the owner: %v", err)
			}
		})

		t.Run("ExpiredLeaseIsClaimable", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			if _, err := repo.ClaimTask(ctx, "t1", "node-a", 10*time.Millisecond); err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}
			time.Sleep(30 * time.Millisecond)

			rec, err := repo.ClaimTask(ctx, "t1", "node-b", forever)
			if err != nil {
				t.Fatalf("ClaimTask after expiry: %v", err)
			}
			if rec.OwnerNode != "node-b" {
				t.Fatalf("owner = %q, want node-b", rec.OwnerNode)
			}
		})

		t.Run("RenewMovesOnlyTheLease", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			rec, err := repo.ClaimTask(ctx, "t1", "node-a", 10*time.Millisecond)
			if err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}

			// A late renewal that nobody usurped still wins.
			time.Sleep(30 * time.Millisecond)
			if err := repo.RenewClaim(ctx, "t1", "node-a", forever); err != nil {
				t.Fatalf("late RenewClaim: %v", err)
			}

			kept, err := repo.RecoverTask(ctx, "t1")
			if err != nil {
				t.Fatalf("RecoverTask: %v", err)
			}
			if kept.Version != rec.Version {
				t.Fatalf("renewal bumped the version %d -> %d; it must move only the lease", rec.Version, kept.Version)
			}
			if !kept.LeaseExpiresAt.After(rec.LeaseExpiresAt) {
				t.Fatalf("lease %v not extended past %v", kept.LeaseExpiresAt, rec.LeaseExpiresAt)
			}

			if err := repo.RenewClaim(ctx, "ghost", "node-a", forever); !errors.Is(err, tasks.ErrTaskNotFound) {
				t.Fatalf("RenewClaim ghost: err = %v, want tasks.ErrTaskNotFound", err)
			}
		})

		t.Run("UsurpationEndsTheOldOwner", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			if _, err := repo.ClaimTask(ctx, "t1", "node-a", 10*time.Millisecond); err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}
			time.Sleep(30 * time.Millisecond)
			if _, err := repo.ClaimTask(ctx, "t1", "node-b", forever); err != nil {
				t.Fatalf("usurping claim: %v", err)
			}

			// The usurped node's renewal fails loud - its cue to stop - and
			// its release is a silent no-op that leaves the usurper's claim
			// standing.
			if err := repo.RenewClaim(ctx, "t1", "node-a", forever); !errors.Is(err, tasks.ErrStale) {
				t.Fatalf("RenewClaim after usurpation: err = %v, want tasks.ErrStale", err)
			}
			if err := repo.ReleaseClaim(ctx, "t1", "node-a"); err != nil {
				t.Fatalf("ReleaseClaim after usurpation: %v", err)
			}
			kept, err := repo.RecoverTask(ctx, "t1")
			if err != nil {
				t.Fatalf("RecoverTask: %v", err)
			}
			if kept.OwnerNode != "node-b" {
				t.Fatalf("owner = %q after the loser's release, want node-b", kept.OwnerNode)
			}
		})

		t.Run("ReleaseFreesTheClaim", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			if _, err := repo.ClaimTask(ctx, "t1", "node-a", forever); err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}
			if err := repo.ReleaseClaim(ctx, "t1", "node-a"); err != nil {
				t.Fatalf("ReleaseClaim: %v", err)
			}

			kept, err := repo.RecoverTask(ctx, "t1")
			if err != nil {
				t.Fatalf("RecoverTask: %v", err)
			}
			if kept.OwnerNode != "" || !kept.LeaseExpiresAt.IsZero() {
				t.Fatalf("claim = %q %v after release, want cleared", kept.OwnerNode, kept.LeaseExpiresAt)
			}
			if _, err := repo.ClaimTask(ctx, "t1", "node-b", forever); err != nil {
				t.Fatalf("ClaimTask after release: %v", err)
			}
		})

		t.Run("CreateDropsClaimFields", func(t *testing.T) {
			repo := open(t)
			now := time.Now().UTC()
			smuggled := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: tasks.GlobalGroup,
				CreatedAt: now, UpdatedAt: now,
				Version: 9, OwnerNode: "smuggler", LeaseExpiresAt: now.Add(time.Hour)}
			if err := repo.CreateTask(ctx, smuggled); err != nil {
				t.Fatalf("CreateTask: %v", err)
			}
			rec, err := repo.RecoverTask(ctx, "t1")
			if err != nil {
				t.Fatalf("RecoverTask: %v", err)
			}
			if rec.Version != 0 || rec.OwnerNode != "" || !rec.LeaseExpiresAt.IsZero() {
				t.Fatalf("created record carries a claim: v%d %q %v; a task starts unclaimed at version 0",
					rec.Version, rec.OwnerNode, rec.LeaseExpiresAt)
			}
		})

		// Two nodes racing the same claim: exactly one wins each round, the
		// other reads ErrClaimHeld.
		t.Run("ClaimRace", func(t *testing.T) {
			repo := open(t)
			for round := range 10 {
				id := fmt.Sprintf("race-%d", round)
				seedTask(t, repo, id, "wf1")

				errs := make([]error, 4)
				var wg sync.WaitGroup
				for i := range errs {
					wg.Go(func() {
						_, errs[i] = repo.ClaimTask(ctx, id, fmt.Sprintf("node-%d", i), forever)
					})
				}
				wg.Wait()

				winners := 0
				for i, err := range errs {
					switch {
					case err == nil:
						winners++
					case !errors.Is(err, tasks.ErrClaimHeld):
						t.Fatalf("round %d racer %d: err = %v, want nil or tasks.ErrClaimHeld", round, i, err)
					}
				}
				if winners != 1 {
					t.Fatalf("round %d: %d winners, want exactly 1", round, winners)
				}
			}
		})
	})

	// The conditional writes: version and ownership decide whose checkpoint
	// lands, so a usurped node's stale writes fail loud instead of clobbering
	// the new owner's progress.
	t.Run("ConditionalWrites", func(t *testing.T) {
		const forever = time.Minute

		t.Run("StaleVersionLoses", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			rec, err := repo.ClaimTask(ctx, "t1", "node-a", forever)
			if err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}

			v2, err := repo.SaveCheckpoint(ctx, "t1", rec.Version, "node-a", "running", "s1", []byte("one"))
			if err != nil {
				t.Fatalf("SaveCheckpoint: %v", err)
			}
			if v2 <= rec.Version {
				t.Fatalf("version %d after checkpoint at %d, want a bump", v2, rec.Version)
			}
			if _, err := repo.SaveCheckpoint(ctx, "t1", rec.Version, "node-a", "running", "s2", []byte("two")); !errors.Is(err, tasks.ErrStale) {
				t.Fatalf("stale SaveCheckpoint: err = %v, want tasks.ErrStale", err)
			}

			// The lost write left no trace.
			kept, err := repo.RecoverTask(ctx, "t1")
			if err != nil {
				t.Fatalf("RecoverTask: %v", err)
			}
			if kept.State != "s1" || string(kept.Snapshot) != "one" {
				t.Fatalf("record = %s/%s, want the winning checkpoint s1/one", kept.State, kept.Snapshot)
			}
		})

		t.Run("NonOwnerLoses", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			rec, err := repo.ClaimTask(ctx, "t1", "node-a", forever)
			if err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}

			if _, err := repo.SaveCheckpoint(ctx, "t1", rec.Version, "node-b", "running", "s1", nil); !errors.Is(err, tasks.ErrStale) {
				t.Fatalf("non-owner SaveCheckpoint: err = %v, want tasks.ErrStale", err)
			}
			if _, err := repo.MarkTerminal(ctx, "t1", rec.Version, "node-b", "done", nil); !errors.Is(err, tasks.ErrStale) {
				t.Fatalf("non-owner MarkTerminal: err = %v, want tasks.ErrStale", err)
			}
		})

		t.Run("VersionsClimbToTerminal", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			rec, err := repo.ClaimTask(ctx, "t1", "node-a", forever)
			if err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}

			v := rec.Version
			for i, state := range []string{"s1", "s2"} {
				next, err := repo.SaveCheckpoint(ctx, "t1", v, "node-a", "running", state, nil)
				if err != nil {
					t.Fatalf("SaveCheckpoint %d: %v", i, err)
				}
				if next <= v {
					t.Fatalf("checkpoint %d: version %d after %d, want strictly climbing", i, next, v)
				}
				v = next
			}
			final, err := repo.MarkTerminal(ctx, "t1", v, "node-a", "done", []byte("out"))
			if err != nil {
				t.Fatalf("MarkTerminal: %v", err)
			}
			if final <= v {
				t.Fatalf("terminal version %d after %d, want a bump", final, v)
			}

			// The terminal stamp cleared the claim: a finished task is
			// nobody's to run.
			kept, err := repo.RecoverTask(ctx, "t1")
			if err != nil {
				t.Fatalf("RecoverTask: %v", err)
			}
			if kept.OwnerNode != "" || !kept.LeaseExpiresAt.IsZero() {
				t.Fatalf("terminal task still claimed by %q until %v", kept.OwnerNode, kept.LeaseExpiresAt)
			}
		})

		// There is no unconditional write: a caller without the claim and
		// its version cannot write at all.
		t.Run("NoUnconditionalWrite", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			if _, err := repo.ClaimTask(ctx, "t1", "node-a", forever); err != nil {
				t.Fatalf("ClaimTask: %v", err)
			}
			if _, err := repo.SaveCheckpoint(ctx, "t1", 0, "", "running", "s1", nil); !errors.Is(err, tasks.ErrStale) {
				t.Fatalf("claimless SaveCheckpoint: err = %v, want tasks.ErrStale", err)
			}
		})
	})

	// ListClaimable is the recovery sweep's worklist: what any node may take.
	t.Run("ListClaimable", func(t *testing.T) {
		repo := open(t)
		const forever = time.Minute
		for _, id := range []string{"unclaimed", "live", "expired", "finished"} {
			seedTask(t, repo, id, "wf1")
		}
		if _, err := repo.ClaimTask(ctx, "live", "node-a", forever); err != nil {
			t.Fatalf("claim live: %v", err)
		}
		if _, err := repo.ClaimTask(ctx, "expired", "node-a", 10*time.Millisecond); err != nil {
			t.Fatalf("claim expired: %v", err)
		}
		if _, err := repo.MarkTerminal(ctx, "finished", claimed(t, repo, "finished", "node-a"), "node-a", "done", nil); err != nil {
			t.Fatalf("finish finished: %v", err)
		}
		time.Sleep(30 * time.Millisecond)

		listed, err := repo.ListClaimable(ctx)
		if err != nil {
			t.Fatalf("ListClaimable: %v", err)
		}
		ids := make([]string, 0, len(listed))
		for _, rec := range listed {
			ids = append(ids, rec.ID)
		}
		sort.Strings(ids)
		if len(ids) != 2 || ids[0] != "expired" || ids[1] != "unclaimed" {
			t.Fatalf("ListClaimable = %v, want [expired unclaimed]: live claims and terminal tasks are not for taking", ids)
		}
	})

	t.Run("Effects", func(t *testing.T) {
		t.Run("FirstRecordWins", func(t *testing.T) {
			repo := open(t)
			stored, first, err := repo.RecordEffect(ctx, "t1", "mint", []byte(`"ours"`))
			if err != nil || !first || string(stored) != `"ours"` {
				t.Fatalf("first record = %q/%v/%v, want ours/true/nil", stored, first, err)
			}
			stored, first, err = repo.RecordEffect(ctx, "t1", "mint", []byte(`"theirs"`))
			if err != nil || first || string(stored) != `"ours"` {
				t.Fatalf("second record = %q/%v/%v, want the first record back with first=false", stored, first, err)
			}
		})

		t.Run("KeysAreScopedToTheTask", func(t *testing.T) {
			repo := open(t)
			if _, _, err := repo.RecordEffect(ctx, "t1", "mint", []byte(`1`)); err != nil {
				t.Fatalf("record t1: %v", err)
			}
			stored, first, err := repo.RecordEffect(ctx, "t2", "mint", []byte(`2`))
			if err != nil || !first || string(stored) != `2` {
				t.Fatalf("t2 record = %q/%v/%v, want its own first record", stored, first, err)
			}
		})

		t.Run("EmptyResultRoundTrips", func(t *testing.T) {
			repo := open(t)
			if _, first, err := repo.RecordEffect(ctx, "t1", "ping", nil); err != nil || !first {
				t.Fatalf("record nil result: first=%v err=%v", first, err)
			}
			effects, err := repo.ListEffects(ctx, "t1")
			if err != nil {
				t.Fatalf("ListEffects: %v", err)
			}
			result, ok := effects["ping"]
			if !ok || len(result) != 0 {
				t.Fatalf("effects[ping] = %q/%v, want a present empty record", result, ok)
			}
		})

		t.Run("ListRoundTrip", func(t *testing.T) {
			repo := open(t)
			want := map[string][]byte{"a": []byte(`{"n":1}`), "b": []byte(`"x"`)}
			for key, result := range want {
				if _, _, err := repo.RecordEffect(ctx, "t1", key, result); err != nil {
					t.Fatalf("record %s: %v", key, err)
				}
			}
			effects, err := repo.ListEffects(ctx, "t1")
			if err != nil {
				t.Fatalf("ListEffects: %v", err)
			}
			if len(effects) != len(want) {
				t.Fatalf("effects = %d entries, want %d", len(effects), len(want))
			}
			for key, result := range want {
				if string(effects[key]) != string(result) {
					t.Fatalf("effects[%s] = %q, want %q", key, effects[key], result)
				}
			}
			// A task with no effects lists empty, not an error.
			effects, err = repo.ListEffects(ctx, "t2")
			if err != nil || len(effects) != 0 {
				t.Fatalf("ListEffects of an effectless task = %v/%v, want empty/nil", effects, err)
			}
		})

		t.Run("DeleteTaskCascades", func(t *testing.T) {
			repo := open(t)
			seedTask(t, repo, "t1", "wf1")
			if _, _, err := repo.RecordEffect(ctx, "t1", "mint", []byte(`1`)); err != nil {
				t.Fatalf("record: %v", err)
			}
			if err := repo.DeleteTask(ctx, "t1"); err != nil {
				t.Fatalf("DeleteTask: %v", err)
			}
			effects, err := repo.ListEffects(ctx, "t1")
			if err != nil || len(effects) != 0 {
				t.Fatalf("effects after delete = %v/%v, want empty/nil", effects, err)
			}
		})

		t.Run("RecordRace", func(t *testing.T) {
			// Racing recorders on one key must resolve to exactly one first
			// and one recorded result — the loser reads the winner's bytes.
			repo := open(t)
			for round := range 10 {
				key := fmt.Sprintf("effect-%d", round)
				const racers = 4
				var wg sync.WaitGroup
				firsts := make([]bool, racers)
				results := make([][]byte, racers)
				for i := range racers {
					wg.Go(func() {
						mine := []byte(fmt.Sprintf(`"racer-%d"`, i))
						stored, first, err := repo.RecordEffect(ctx, "t1", key, mine)
						if err != nil {
							t.Errorf("round %d racer %d: %v", round, i, err)
							return
						}
						firsts[i], results[i] = first, stored
					})
				}
				wg.Wait()
				if t.Failed() {
					return
				}
				winners := 0
				for _, first := range firsts {
					if first {
						winners++
					}
				}
				if winners != 1 {
					t.Fatalf("round %d: %d firsts, want exactly 1", round, winners)
				}
				for i := 1; i < racers; i++ {
					if string(results[i]) != string(results[0]) {
						t.Fatalf("round %d: racers saw different results: %q vs %q", round, results[0], results[i])
					}
				}
			}
		})
	})
}
