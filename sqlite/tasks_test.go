package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/tasks"
)

// newTestTasks opens the tasks store on a fresh temp-file database.
func newTestTasks(t *testing.T) tasks.Repository {
	t.Helper()
	repo, err := NewTasks(openTestDB(t))
	if err != nil {
		t.Fatalf("NewTasks: %v", err)
	}
	return repo
}

// satisfiesRepositoryPort fails to compile if Tasks drifts from the persistence port it exists to implement.
var _ tasks.Repository = (*Tasks)(nil)

// The resource kinds these tests store placements under. The store never
// interprets a kind; it is only a key in the JSON column.
const (
	proxyKind   = "proxy"
	accountKind = "account"
)

// seedTask inserts a task in the global group with fresh timestamps, the
// placement most tests do not care about.
func seedTask(t *testing.T, repo tasks.Repository, id, workflowID string) {
	t.Helper()
	now := time.Now().UTC()
	rec := tasks.Task{ID: id, WorkflowID: workflowID, GroupID: tasks.GlobalGroup, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateTask(context.Background(), rec); err != nil {
		t.Fatalf("CreateTask %s: %v", id, err)
	}
}

// TestTasksCreateTaskRecoverable verifies a created task is recoverable by id with its workflow and no checkpoint yet,
// because recovery must be able to resolve the workflow before any state has run.
func TestTasksCreateTaskRecoverable(t *testing.T) {
	repo := newTestTasks(t)
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

// TestTasksSaveCheckpointPersistsState verifies a checkpoint's status, state, and snapshot survive recovery,
// because recovery resumes the engine from exactly the bytes and state last checkpointed.
func TestTasksSaveCheckpointPersistsState(t *testing.T) {
	repo := newTestTasks(t)
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
	if rec.Status != "running" || rec.State != "add_to_cart" {
		t.Fatalf("got status=%q state=%q, want running/add_to_cart", rec.Status, rec.State)
	}
	if string(rec.Snapshot) != string(snap) {
		t.Fatalf("snapshot mismatch: got %q want %q", rec.Snapshot, snap)
	}
}

// TestTasksSaveCheckpointOverwrites verifies a later checkpoint replaces an earlier one,
// because the repository records only a task's last checkpoint, not a history.
func TestTasksSaveCheckpointOverwrites(t *testing.T) {
	repo := newTestTasks(t)
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

// TestTasksMarkTerminalKeepsStateAndSnapshot verifies a terminal outcome updates status but leaves state and snapshot intact,
// because a terminal record must stay a valid resume entry for a consumer-driven re-run.
func TestTasksMarkTerminalKeepsStateAndSnapshot(t *testing.T) {
	repo := newTestTasks(t)
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	if err := repo.SaveCheckpoint(ctx, "t1", "running", "submit", []byte("snap")); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	if err := repo.MarkTerminal(ctx, "t1", "done", nil); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	rec, _ := repo.RecoverTask(ctx, "t1")
	if rec.Status != "done" {
		t.Fatalf("status = %q, want done", rec.Status)
	}
	if rec.State != "submit" || string(rec.Snapshot) != "snap" {
		t.Fatalf("terminal wiped state/snapshot: got %+v", rec)
	}
}

// TestTasksMarkTerminalPersistsOutput verifies the workflow's output is stored with the
// terminal stamp and survives recovery, because delivering output from Start is
// only half the contract — a finished task's result must also be durably readable.
func TestTasksMarkTerminalPersistsOutput(t *testing.T) {
	repo := newTestTasks(t)
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	out := []byte(`{"orderID":"order-1"}`)
	if err := repo.MarkTerminal(ctx, "t1", "done", out); err != nil {
		t.Fatalf("MarkTerminal: %v", err)
	}

	rec, err := repo.RecoverTask(ctx, "t1")
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if string(rec.Output) != string(out) {
		t.Fatalf("output = %q, want %q", rec.Output, out)
	}
}

// TestTasksRecoverTaskNotFound verifies recovering an unknown task is an errors.Is(ErrTaskNotFound) failure,
// because the service distinguishes a missing task from a store error.
func TestTasksRecoverTaskNotFound(t *testing.T) {
	repo := newTestTasks(t)

	_, err := repo.RecoverTask(context.Background(), "nope")
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("err = %v, want ErrTaskNotFound", err)
	}
}

// TestTasksRecoverAll verifies every persisted task is returned, terminal ones included,
// because the caller decides which recovered tasks to restart.
func TestTasksRecoverAll(t *testing.T) {
	repo := newTestTasks(t)
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
	if len(recs) != 2 {
		t.Fatalf("got %d records, want 2", len(recs))
	}
	byID := map[string]tasks.Task{}
	for _, r := range recs {
		byID[r.ID] = r
	}
	if byID["t1"].WorkflowID != "wf1" || byID["t2"].Status != "done" {
		t.Fatalf("unexpected records: %+v", recs)
	}
}

// TestTasksRecoverAllEmpty verifies a store with no tasks returns an empty, non-nil slice and no error.
func TestTasksRecoverAllEmpty(t *testing.T) {
	repo := newTestTasks(t)

	recs, err := repo.RecoverAll(context.Background())
	if err != nil {
		t.Fatalf("RecoverAll: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("got %d records, want 0", len(recs))
	}
}

// TestTasksDeleteTask verifies a deleted task is no longer recoverable,
// because DeleteTask removes the record the service has dropped from its registry.
func TestTasksDeleteTask(t *testing.T) {
	repo := newTestTasks(t)
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	if err := repo.DeleteTask(ctx, "t1"); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	if _, err := repo.RecoverTask(ctx, "t1"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("err = %v, want ErrTaskNotFound after delete", err)
	}
}

// TestTasksPersistsAcrossReopen verifies records survive closing and reopening the same database file,
// because durability is the whole point of a file-backed repository.
func TestTasksPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "tasks.db")

	repoDB := openAt(t, dsn)
	repo, err := NewTasks(repoDB)
	if err != nil {
		t.Fatalf("NewTasks: %v", err)
	}
	seedTask(t, repo, "t1", "wf1")
	if err := repo.SaveCheckpoint(ctx, "t1", "suspended", "wait", []byte("snap")); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	repoDB.Close()

	reopenedDB := openAt(t, dsn)
	reopened, err := NewTasks(reopenedDB)
	if err != nil {
		t.Fatalf("reopen NewTasks: %v", err)
	}
	t.Cleanup(func() { reopenedDB.Close() })

	rec, err := reopened.RecoverTask(ctx, "t1")
	if err != nil {
		t.Fatalf("RecoverTask after reopen: %v", err)
	}
	if rec.Status != "suspended" || rec.State != "wait" || string(rec.Snapshot) != "snap" {
		t.Fatalf("checkpoint did not survive reopen: %+v", rec)
	}
}

// TestTasksAssignmentTriStateRoundTrips verifies the three distinct assignments a
// task can carry survive storage as JSON: nil (inherit the task group's), ""
// (lease none of the kind), and a named group. JSON has one null but two empty
// strings' worth of meaning here — collapsing nil and "" would silently turn an
// inheriting task into an opted-out one.
func TestTasksAssignmentTriStateRoundTrips(t *testing.T) {
	repo := newTestTasks(t)
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
		if got.GroupID != "g1" {
			t.Fatalf("%s: GroupID = %q, want g1", tc.id, got.GroupID)
		}
	}
}

// TestTasksAssignmentsAreIndependentPerKind verifies one record carries a distinct
// placement for each kind, each with its own tri-state. Nothing about storing
// them in one column may let one kind read as another.
func TestTasksAssignmentsAreIndependentPerKind(t *testing.T) {
	repo := newTestTasks(t)
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
// unchanged while writes move UpdatedAt forward, so a consumer can tell when a
// task last progressed.
func TestTasksTimestampsRoundTripAndRefresh(t *testing.T) {
	repo := newTestTasks(t)
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	rec := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: tasks.GlobalGroup, CreatedAt: created, UpdatedAt: created}
	if err := repo.CreateTask(ctx, rec); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	got, err := repo.RecoverTask(ctx, "t1")
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
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

// TestTasksGroupCRUD verifies a task group round-trips with its per-kind resource
// assignments, that a missing group reports found=false rather than an error,
// and that deletion removes it.
func TestTasksGroupCRUD(t *testing.T) {
	repo := newTestTasks(t)
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
		t.Fatalf("resource groups = %v, want residential proxies and shoppers accounts", got.ResourceGroups)
	}
	if !got.CreatedAt.Equal(now) {
		t.Fatalf("CreatedAt = %v, want %v", got.CreatedAt, now)
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

// TestTasksTasksInGroup verifies membership lookup returns exactly the group's
// tasks, because the service drives its cascade delete off this list.
func TestTasksTasksInGroup(t *testing.T) {
	repo := newTestTasks(t)
	ctx := context.Background()

	for _, tc := range []struct{ id, group string }{
		{"a1", "ga"}, {"a2", "ga"}, {"b1", "gb"},
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

// TestTasksWritesToMissingTaskFailLoud verifies a checkpoint or terminal stamp for
// an id with no record fails with ErrTaskNotFound instead of silently updating
// zero rows — the engine would otherwise believe progress is durable when
// nothing was written.
func TestTasksWritesToMissingTaskFailLoud(t *testing.T) {
	repo := newTestTasks(t)
	ctx := context.Background()

	if err := repo.SaveCheckpoint(ctx, "ghost", "running", "s1", nil); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("SaveCheckpoint on missing task: err = %v, want ErrTaskNotFound", err)
	}
	if err := repo.MarkTerminal(ctx, "ghost", "done", nil); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("MarkTerminal on missing task: err = %v, want ErrTaskNotFound", err)
	}
}

// TestTasksPinRoundTrips verifies a task's pin survives storage on the same
// three-way distinction as its group: nil (unpinned, rotate), "" (explicitly
// none), and a named resource.
func TestTasksPinRoundTrips(t *testing.T) {
	repo := newTestTasks(t)
	ctx := context.Background()

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
}

// TestTasksSaveAssignmentRepointsOneKind verifies a reassignment rewrites both
// halves of one kind's placement, leaves every other kind and the checkpoint
// alone, and reports a task that does not exist rather than silently updating
// no rows. Editing one kind in place is what makes a stored map safe to
// repoint without reading it back first.
func TestTasksSaveAssignmentRepointsOneKind(t *testing.T) {
	repo := newTestTasks(t)
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
	if proxy.GroupID == nil || *proxy.GroupID != "datacenter" {
		t.Fatalf("proxy group = %v, want datacenter", proxy.GroupID)
	}
	if proxy.ResourceID == nil || *proxy.ResourceID != "p9" {
		t.Fatalf("proxy pin = %v, want p9", proxy.ResourceID)
	}
	if account := got.Assignments[accountKind].ResourceID; account == nil || *account != "buyer-1" {
		t.Fatalf("account pin = %v, want the untouched buyer-1", account)
	}
	if got.State != "s1" || got.Status != "running" || string(got.Snapshot) != `{"v":1}` {
		t.Fatalf("reassignment disturbed the checkpoint: %+v", got)
	}

	// Clearing both halves stores JSON null, not the empty string.
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

// TestTasksSaveAssignmentOnUnplacedTask verifies the first assignment on a task
// stored with no placement lands, rather than failing on the empty column the
// task was created with.
func TestTasksSaveAssignmentOnUnplacedTask(t *testing.T) {
	repo := newTestTasks(t)
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
