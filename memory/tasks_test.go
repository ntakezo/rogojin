package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/tasks"
)

// The resource kinds these tests store placements under; the store never
// interprets a kind.
const (
	proxyKind   = "proxy"
	accountKind = "account"
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

// TestTasksCreateTaskRecoverable verifies a created task is recoverable by id
// with its workflow and no checkpoint yet.
func TestTasksCreateTaskRecoverable(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

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
}

// TestTasksCreateTaskRefusesDuplicate verifies a second create under the same
// id fails, mirroring the primary-key refusal.
func TestTasksCreateTaskRefusesDuplicate(t *testing.T) {
	repo := NewTasks()
	seedTask(t, repo, "t1", "wf1")
	if err := repo.CreateTask(context.Background(), tasks.Task{ID: "t1", WorkflowID: "wf2"}); err == nil {
		t.Fatal("CreateTask duplicate: want an error, got nil")
	}
}

// TestTasksCreateTaskDropsCheckpointFields verifies a record created carrying
// checkpoint or output bytes stores without them, exactly as the insert's
// column list drops them.
func TestTasksCreateTaskDropsCheckpointFields(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	rec := tasks.Task{ID: "t1", WorkflowID: "wf1", State: "smuggled", Status: "running",
		Snapshot: []byte("snap"), Output: []byte("out")}
	if err := repo.CreateTask(ctx, rec); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	got, _ := repo.RecoverTask(ctx, "t1")
	if got.State != "" || got.Status != "" || got.Snapshot != nil || got.Output != nil {
		t.Fatalf("create persisted checkpoint fields: %+v", got)
	}
}

// TestTasksSaveCheckpointPersistsState verifies a checkpoint's status, state,
// and snapshot survive recovery.
func TestTasksSaveCheckpointPersistsState(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	snap := []byte(`{"cart":"abc"}`)
	if err := repo.SaveCheckpoint(ctx, "t1", "running", "add_to_cart", snap); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	rec, err := repo.RecoverTask(ctx, "t1")
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if rec.Status != "running" || rec.State != "add_to_cart" || string(rec.Snapshot) != string(snap) {
		t.Fatalf("got %+v, want running/add_to_cart/%s", rec, snap)
	}
}

// TestTasksSaveCheckpointOverwrites verifies a later checkpoint replaces an
// earlier one.
func TestTasksSaveCheckpointOverwrites(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	if err := repo.SaveCheckpoint(ctx, "t1", "running", "s1", []byte("a")); err != nil {
		t.Fatalf("SaveCheckpoint 1: %v", err)
	}
	if err := repo.SaveCheckpoint(ctx, "t1", "suspended", "s2", []byte("b")); err != nil {
		t.Fatalf("SaveCheckpoint 2: %v", err)
	}

	rec, _ := repo.RecoverTask(ctx, "t1")
	if rec.Status != "suspended" || rec.State != "s2" || string(rec.Snapshot) != "b" {
		t.Fatalf("got %+v, want last checkpoint suspended/s2/b", rec)
	}
}

// TestTasksMarkTerminalKeepsStateAndSnapshot verifies a terminal outcome
// updates status but leaves state and snapshot intact as a resume entry.
func TestTasksMarkTerminalKeepsStateAndSnapshot(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	if err := repo.SaveCheckpoint(ctx, "t1", "running", "submit", []byte("snap")); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	if err := repo.MarkTerminal(ctx, "t1", "done", nil); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	rec, _ := repo.RecoverTask(ctx, "t1")
	if rec.Status != "done" || rec.State != "submit" || string(rec.Snapshot) != "snap" {
		t.Fatalf("terminal disturbed state/snapshot: %+v", rec)
	}
}

// TestTasksMarkTerminalPersistsOutput verifies the workflow's output is
// stored with the terminal stamp.
func TestTasksMarkTerminalPersistsOutput(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	out := []byte(`{"orderID":"order-1"}`)
	if err := repo.MarkTerminal(ctx, "t1", "done", out); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}
	rec, _ := repo.RecoverTask(ctx, "t1")
	if string(rec.Output) != string(out) {
		t.Fatalf("output = %q, want %q", rec.Output, out)
	}
}

// TestTasksRecoverTaskNotFound verifies recovering an unknown task fails with
// tasks.ErrTaskNotFound.
func TestTasksRecoverTaskNotFound(t *testing.T) {
	repo := NewTasks()
	if _, err := repo.RecoverTask(context.Background(), "nope"); !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Fatalf("err = %v, want tasks.ErrTaskNotFound", err)
	}
}

// TestTasksRecoverAll verifies every persisted task is returned, terminal
// ones included, in insertion order.
func TestTasksRecoverAll(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	seedTask(t, repo, "t2", "wf2")
	if err := repo.MarkTerminal(ctx, "t2", "done", nil); err != nil {
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
}

// TestTasksRecoverAllEmpty verifies an empty store returns an empty, non-nil
// slice and no error.
func TestTasksRecoverAllEmpty(t *testing.T) {
	repo := NewTasks()
	recs, err := repo.RecoverAll(context.Background())
	if err != nil || recs == nil || len(recs) != 0 {
		t.Fatalf("RecoverAll = %v, %v; want empty non-nil, nil", recs, err)
	}
}

// TestTasksDeleteTask verifies a deleted task is no longer recoverable and a
// second delete is a no-op.
func TestTasksDeleteTask(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

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
}

// TestTasksAssignmentTriStateRoundTrips verifies the three distinct
// assignments a task can carry survive storage: nil (inherit), "" (lease
// none), and a named group.
func TestTasksAssignmentTriStateRoundTrips(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

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
}

// TestTasksAssignmentsAreIndependentPerKind verifies one record carries a
// distinct placement per kind, each with its own tri-state.
func TestTasksAssignmentsAreIndependentPerKind(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

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
}

// TestTasksTimestampsRoundTripAndRefresh verifies CreatedAt survives storage
// unchanged while writes move UpdatedAt forward.
func TestTasksTimestampsRoundTripAndRefresh(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	rec := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: tasks.GlobalGroup, CreatedAt: created, UpdatedAt: created}
	if err := repo.CreateTask(ctx, rec); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	got, _ := repo.RecoverTask(ctx, "t1")
	if !got.CreatedAt.Equal(created) || !got.UpdatedAt.Equal(created) {
		t.Fatalf("timestamps = %v/%v, want both %v", got.CreatedAt, got.UpdatedAt, created)
	}

	if err := repo.SaveCheckpoint(ctx, "t1", "running", "s1", nil); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	got, _ = repo.RecoverTask(ctx, "t1")
	if !got.CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt = %v, want it untouched at %v", got.CreatedAt, created)
	}
	if !got.UpdatedAt.After(created) {
		t.Fatalf("UpdatedAt = %v, want it moved past %v", got.UpdatedAt, created)
	}
}

// TestTasksGroupCRUD verifies a task group round-trips with its per-kind
// resource assignments, a missing group reports found=false, and deletion
// removes it.
func TestTasksGroupCRUD(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

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

	// A later save refreshes everything but CreatedAt.
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
}

// TestTasksTasksInGroup verifies membership lookup returns exactly the
// group's tasks in stable id order.
func TestTasksTasksInGroup(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

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
}

// TestTasksWritesToMissingTaskFailLoud verifies a checkpoint or terminal
// stamp for an id with no record fails with tasks.ErrTaskNotFound.
func TestTasksWritesToMissingTaskFailLoud(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	if err := repo.SaveCheckpoint(ctx, "ghost", "running", "s1", nil); !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Fatalf("SaveCheckpoint on missing task: err = %v, want tasks.ErrTaskNotFound", err)
	}
	if err := repo.MarkTerminal(ctx, "ghost", "done", nil); !errors.Is(err, tasks.ErrTaskNotFound) {
		t.Fatalf("MarkTerminal on missing task: err = %v, want tasks.ErrTaskNotFound", err)
	}
}

// TestTasksSaveAssignmentRepointsOneKind verifies a reassignment rewrites one
// kind's placement, leaves every other kind and the checkpoint alone, adds a
// kind the record never carried, and reports a missing task.
func TestTasksSaveAssignmentRepointsOneKind(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	group, pin, accountPin := "residential", "p1", "buyer-1"
	rec := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: "g1", Assignments: map[leasing.Kind]tasks.Assignment{
		proxyKind:   {GroupID: &group, ResourceID: &pin},
		accountKind: {ResourceID: &accountPin},
	}}
	if err := repo.CreateTask(ctx, rec); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := repo.SaveCheckpoint(ctx, "t1", "running", "s1", []byte(`{"v":1}`)); err != nil {
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

	if err := repo.SaveAssignment(ctx, "missing", proxyKind, tasks.Assignment{GroupID: &moved}); err == nil {
		t.Fatal("expected an error assigning a task that does not exist")
	}
}

// TestTasksSaveAssignmentOnUnplacedTask verifies the first assignment on a
// task stored with no placement lands.
func TestTasksSaveAssignmentOnUnplacedTask(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	group := "residential"
	if err := repo.SaveAssignment(ctx, "t1", proxyKind, tasks.Assignment{GroupID: &group}); err != nil {
		t.Fatalf("SaveAssignment: %v", err)
	}
	got, _ := repo.RecoverTask(ctx, "t1")
	if stored := got.Assignments[proxyKind].GroupID; stored == nil || *stored != "residential" {
		t.Fatalf("proxy group = %v, want residential", stored)
	}
}

// TestTasksSaveAssignmentRefusesUnsafeKind verifies the store's own kind
// guard refuses names outside the charset and leaves the record untouched.
func TestTasksSaveAssignmentRefusesUnsafeKind(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

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
}

// TestTasksCopiesAtTheBoundary verifies the store and its callers never
// share memory: mutating what was saved or recovered leaves the store's copy
// alone — the property serialization gives the file-backed stores for free.
func TestTasksCopiesAtTheBoundary(t *testing.T) {
	repo := NewTasks()
	ctx := context.Background()

	group := "residential"
	rec := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: "g1",
		Assignments: map[leasing.Kind]tasks.Assignment{proxyKind: {GroupID: &group}}}
	if err := repo.CreateTask(ctx, rec); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	snap := []byte("snap")
	if err := repo.SaveCheckpoint(ctx, "t1", "running", "s1", snap); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	// Mutations after the writes must not reach the store.
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

	// Mutations through a recovered copy must not reach the store either.
	*got.Assignments[proxyKind].GroupID = "leaked"
	got.Snapshot[0] = 'Y'
	again, _ := repo.RecoverTask(ctx, "t1")
	if stored := again.Assignments[proxyKind].GroupID; *stored != "residential" || string(again.Snapshot) != "snap" {
		t.Fatalf("recovered copy shares memory with the store: %v %q", *stored, again.Snapshot)
	}
}
