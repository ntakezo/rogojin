package tasksqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/tasks"
)

// newTestRepo opens a SQLite repository backed by a fresh temp-file database.
func newTestRepo(t *testing.T) *SQLite {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "tasks.db")
	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { repo.Close() })
	return repo
}

// satisfiesRepositoryPort fails to compile if SQLite drifts from the persistence port it exists to implement.
var _ tasks.Repository = (*SQLite)(nil)

// The resource kinds these tests store placements under. The store never
// interprets a kind; it is only a key in the JSON column.
const (
	proxyKind   = "proxy"
	accountKind = "account"
)

// seedTask inserts a task in the global group with fresh timestamps, the
// placement most tests do not care about.
func seedTask(t *testing.T, repo *SQLite, id, workflowID string) {
	t.Helper()
	now := time.Now().UTC()
	rec := tasks.Record{ID: id, WorkflowID: workflowID, GroupID: tasks.GlobalGroup, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateTask(context.Background(), rec); err != nil {
		t.Fatalf("CreateTask %s: %v", id, err)
	}
}

// TestCreateTaskRecoverable verifies a created task is recoverable by id with its workflow and no checkpoint yet,
// because recovery must be able to resolve the workflow before any state has run.
func TestCreateTaskRecoverable(t *testing.T) {
	repo := newTestRepo(t)
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

// TestSaveCheckpointPersistsState verifies a checkpoint's status, state, and snapshot survive recovery,
// because recovery resumes the engine from exactly the bytes and state last checkpointed.
func TestSaveCheckpointPersistsState(t *testing.T) {
	repo := newTestRepo(t)
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

// TestSaveCheckpointOverwrites verifies a later checkpoint replaces an earlier one,
// because the repository records only a task's last checkpoint, not a history.
func TestSaveCheckpointOverwrites(t *testing.T) {
	repo := newTestRepo(t)
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

// TestMarkTerminalKeepsStateAndSnapshot verifies a terminal outcome updates status but leaves state and snapshot intact,
// because a terminal record must stay a valid resume entry for a consumer-driven re-run.
func TestMarkTerminalKeepsStateAndSnapshot(t *testing.T) {
	repo := newTestRepo(t)
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

// TestMarkTerminalPersistsOutput verifies the workflow's output is stored with the
// terminal stamp and survives recovery, because delivering output from Start is
// only half the contract — a finished task's result must also be durably readable.
func TestMarkTerminalPersistsOutput(t *testing.T) {
	repo := newTestRepo(t)
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

// TestRecoverTaskNotFound verifies recovering an unknown task is an errors.Is(ErrNotFound) failure,
// because the service distinguishes a missing task from a store error.
func TestRecoverTaskNotFound(t *testing.T) {
	repo := newTestRepo(t)

	_, err := repo.RecoverTask(context.Background(), "nope")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// TestRecoverAll verifies every persisted task is returned, terminal ones included,
// because the caller decides which recovered tasks to restart.
func TestRecoverAll(t *testing.T) {
	repo := newTestRepo(t)
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
	byID := map[string]tasks.Record{}
	for _, r := range recs {
		byID[r.ID] = r
	}
	if byID["t1"].WorkflowID != "wf1" || byID["t2"].Status != "done" {
		t.Fatalf("unexpected records: %+v", recs)
	}
}

// TestRecoverAllEmpty verifies a store with no tasks returns an empty, non-nil slice and no error.
func TestRecoverAllEmpty(t *testing.T) {
	repo := newTestRepo(t)

	recs, err := repo.RecoverAll(context.Background())
	if err != nil {
		t.Fatalf("RecoverAll: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("got %d records, want 0", len(recs))
	}
}

// TestDeleteTask verifies a deleted task is no longer recoverable,
// because DeleteTask removes the record the service has dropped from its registry.
func TestDeleteTask(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	seedTask(t, repo, "t1", "wf1")
	if err := repo.DeleteTask(ctx, "t1"); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	if _, err := repo.RecoverTask(ctx, "t1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound after delete", err)
	}
}

// TestPersistsAcrossReopen verifies records survive closing and reopening the same database file,
// because durability is the whole point of a file-backed repository.
func TestPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "tasks.db")

	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	seedTask(t, repo, "t1", "wf1")
	if err := repo.SaveCheckpoint(ctx, "t1", "suspended", "wait", []byte("snap")); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	repo.Close()

	reopened, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("reopen NewSQLite: %v", err)
	}
	t.Cleanup(func() { reopened.Close() })

	rec, err := reopened.RecoverTask(ctx, "t1")
	if err != nil {
		t.Fatalf("RecoverTask after reopen: %v", err)
	}
	if rec.Status != "suspended" || rec.State != "wait" || string(rec.Snapshot) != "snap" {
		t.Fatalf("checkpoint did not survive reopen: %+v", rec)
	}
}

// TestMigratesLegacyDatabaseAddingOutput verifies opening a pre-output database
// (the original tasks schema, with no output column and no recorded version)
// migrates it in place: the output column is added and existing task rows survive
// untouched, because a version upgrade must never drop a consumer's durable tasks.
func TestMigratesLegacyDatabaseAddingOutput(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "legacy.db")

	// Hand-build the old schema (no output column, user_version stays 0) with a row,
	// exactly as a database created before the output migration would look.
	raw, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE tasks (
		id          TEXT PRIMARY KEY,
		workflow_id TEXT NOT NULL,
		state       TEXT NOT NULL DEFAULT '',
		status      TEXT NOT NULL DEFAULT '',
		snapshot    BLOB
	)`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO tasks (id, workflow_id, state, status, snapshot)
		VALUES ('t1', 'wf1', 'submit', 'running', 'snap')`); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	raw.Close()

	// Opening through NewSQLite must migrate the existing file in place.
	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("NewSQLite on legacy db: %v", err)
	}
	t.Cleanup(func() { repo.Close() })

	// The legacy row survives the migration, now reporting a nil output.
	rec, err := repo.RecoverTask(ctx, "t1")
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if rec.WorkflowID != "wf1" || rec.State != "submit" || rec.Status != "running" || string(rec.Snapshot) != "snap" {
		t.Fatalf("legacy row not preserved across migration: %+v", rec)
	}
	if rec.Output != nil {
		t.Fatalf("legacy row output = %q, want nil", rec.Output)
	}

	// The newly added output column is writable end to end.
	out := []byte(`{"orderID":"order-1"}`)
	if err := repo.MarkTerminal(ctx, "t1", "done", out); err != nil {
		t.Fatalf("MarkTerminal after migration: %v", err)
	}
	rec, _ = repo.RecoverTask(ctx, "t1")
	if string(rec.Output) != string(out) {
		t.Fatalf("output after migration = %q, want %q", rec.Output, out)
	}
}

// TestAssignmentTriStateRoundTrips verifies the three distinct assignments a
// task can carry survive storage as JSON: nil (inherit the task group's), ""
// (lease none of the kind), and a named group. JSON has one null but two empty
// strings' worth of meaning here — collapsing nil and "" would silently turn an
// inheriting task into an opted-out one.
func TestAssignmentTriStateRoundTrips(t *testing.T) {
	repo := newTestRepo(t)
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
		rec := tasks.Record{ID: tc.id, WorkflowID: "wf1", GroupID: "g1",
			Assignments: map[string]tasks.Assignment{proxyKind: {GroupID: tc.assigned}}}
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

// TestAssignmentsAreIndependentPerKind verifies one record carries a distinct
// placement for each kind, each with its own tri-state. Nothing about storing
// them in one column may let one kind read as another.
func TestAssignmentsAreIndependentPerKind(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	proxyGroup, none, accountPin := "residential", "", "buyer-1"
	rec := tasks.Record{ID: "t1", WorkflowID: "wf1", GroupID: "g1", Assignments: map[string]tasks.Assignment{
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

// TestTimestampsRoundTripAndRefresh verifies CreatedAt survives storage
// unchanged while writes move UpdatedAt forward, so a consumer can tell when a
// task last progressed.
func TestTimestampsRoundTripAndRefresh(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	rec := tasks.Record{ID: "t1", WorkflowID: "wf1", GroupID: tasks.GlobalGroup, CreatedAt: created, UpdatedAt: created}
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

// TestGroupCRUD verifies a task group round-trips with its per-kind resource
// assignments, that a missing group reports found=false rather than an error,
// and that deletion removes it.
func TestGroupCRUD(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if _, found, err := repo.GetGroup(ctx, "ghost"); err != nil || found {
		t.Fatalf("GetGroup(ghost) = found %v, err %v; want false, nil", found, err)
	}

	now := time.Now().UTC().Truncate(time.Millisecond)
	g := tasks.Group{ID: "g1", CreatedAt: now, UpdatedAt: now, ResourceGroups: map[string]string{
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

// TestTasksInGroup verifies membership lookup returns exactly the group's
// tasks, because the service drives its cascade delete off this list.
func TestTasksInGroup(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	for _, tc := range []struct{ id, group string }{
		{"a1", "ga"}, {"a2", "ga"}, {"b1", "gb"},
	} {
		if err := repo.CreateTask(ctx, tasks.Record{ID: tc.id, WorkflowID: "wf1", GroupID: tc.group}); err != nil {
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

// TestLegacyRowsLandInGlobalGroup verifies the group migration places
// pre-group tasks in the global namespace rather than an empty one, so an
// upgraded database's tasks stay addressable as a group.
func TestLegacyRowsLandInGlobalGroup(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "legacy.db")

	raw, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE tasks (
		id          TEXT PRIMARY KEY,
		workflow_id TEXT NOT NULL,
		state       TEXT NOT NULL DEFAULT '',
		status      TEXT NOT NULL DEFAULT '',
		snapshot    BLOB
	)`); err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO tasks (id, workflow_id) VALUES ('t1', 'wf1')`); err != nil {
		t.Fatalf("seed legacy row: %v", err)
	}
	raw.Close()

	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("NewSQLite on legacy db: %v", err)
	}
	t.Cleanup(func() { repo.Close() })

	rec, err := repo.RecoverTask(ctx, "t1")
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if rec.GroupID != tasks.GlobalGroup {
		t.Fatalf("GroupID = %q, want %q", rec.GroupID, tasks.GlobalGroup)
	}
	if len(rec.Assignments) != 0 {
		t.Fatalf("Assignments = %v, want none (inherit every kind)", rec.Assignments)
	}
	if !rec.CreatedAt.IsZero() || !rec.UpdatedAt.IsZero() {
		t.Fatalf("legacy timestamps = %v/%v, want zero", rec.CreatedAt, rec.UpdatedAt)
	}
	ids, err := repo.TasksInGroup(ctx, tasks.GlobalGroup)
	if err != nil || len(ids) != 1 || ids[0] != "t1" {
		t.Fatalf("TasksInGroup(global) = %v, err %v; want [t1], nil", ids, err)
	}
}

// TestWritesToMissingTaskFailLoud verifies a checkpoint or terminal stamp for
// an id with no record fails with ErrNotFound instead of silently updating
// zero rows — the engine would otherwise believe progress is durable when
// nothing was written.
func TestWritesToMissingTaskFailLoud(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.SaveCheckpoint(ctx, "ghost", "running", "s1", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SaveCheckpoint on missing task: err = %v, want ErrNotFound", err)
	}
	if err := repo.MarkTerminal(ctx, "ghost", "done", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MarkTerminal on missing task: err = %v, want ErrNotFound", err)
	}
}

// TestPinRoundTrips verifies a task's pin survives storage on the same
// three-way distinction as its group: nil (unpinned, rotate), "" (explicitly
// none), and a named resource.
func TestPinRoundTrips(t *testing.T) {
	repo := newTestRepo(t)
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
		rec := tasks.Record{ID: tc.id, WorkflowID: "wf1", GroupID: "g1",
			Assignments: map[string]tasks.Assignment{proxyKind: {GroupID: &group, ResourceID: tc.pinned}}}
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

// TestSaveAssignmentRepointsOneKind verifies a reassignment rewrites both
// halves of one kind's placement, leaves every other kind and the checkpoint
// alone, and reports a task that does not exist rather than silently updating
// no rows. Editing one kind in place is what makes a stored map safe to
// repoint without reading it back first.
func TestSaveAssignmentRepointsOneKind(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	group, pin, accountPin := "residential", "p1", "buyer-1"
	rec := tasks.Record{ID: "t1", WorkflowID: "wf1", GroupID: "g1", Assignments: map[string]tasks.Assignment{
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

// TestSaveAssignmentOnUnplacedTask verifies the first assignment on a task
// stored with no placement lands, rather than failing on the empty column the
// task was created with.
func TestSaveAssignmentOnUnplacedTask(t *testing.T) {
	repo := newTestRepo(t)
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

// TestTasksPinnedToSelectsByKindAndPin verifies the store answers the pin
// question the deletion warning is built on: whole records for exactly the
// tasks naming that resource under that kind, so the caller can apply its own
// can-this-still-run rule. A task with no placement at all must not break the
// query — its column holds no JSON to look into.
func TestTasksPinnedToSelectsByKindAndPin(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	group, pin, other := "residential", "p1", "p2"
	pinned := func(kind, resourceID string) map[string]tasks.Assignment {
		return map[string]tasks.Assignment{kind: {GroupID: &group, ResourceID: &resourceID}}
	}
	for _, rec := range []tasks.Record{
		{ID: "a", WorkflowID: "wf", GroupID: "g1", Assignments: pinned(proxyKind, pin)},
		{ID: "b", WorkflowID: "wf", GroupID: "g1", Assignments: pinned(proxyKind, pin)},
		{ID: "c", WorkflowID: "wf", GroupID: "g1", Assignments: pinned(proxyKind, other)},
		{ID: "d", WorkflowID: "wf", GroupID: "g1", Assignments: map[string]tasks.Assignment{proxyKind: {GroupID: &group}}},
		{ID: "e", WorkflowID: "wf", GroupID: "g1", Assignments: pinned(accountKind, pin)},
		{ID: "f", WorkflowID: "wf", GroupID: "g1"},
	} {
		if err := repo.CreateTask(ctx, rec); err != nil {
			t.Fatalf("CreateTask %s: %v", rec.ID, err)
		}
	}
	if err := repo.SaveCheckpoint(ctx, "a", "failed", "s2", []byte(`{"v":2}`)); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}

	got, err := repo.TasksPinnedTo(ctx, proxyKind, "p1")
	if err != nil {
		t.Fatalf("TasksPinnedTo: %v", err)
	}
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("TasksPinnedTo = %v, want records a and b", got)
	}
	// Whole records: the caller needs status and state to judge resumability.
	if got[0].Status != "failed" || got[0].State != "s2" {
		t.Fatalf("record a = %+v, want its checkpoint carried through", got[0])
	}

	// The account "p1" is a different resource that happens to share a name.
	if accounts, err := repo.TasksPinnedTo(ctx, accountKind, "p1"); err != nil || len(accounts) != 1 || accounts[0].ID != "e" {
		t.Fatalf("account TasksPinnedTo = %v, %v, want record e", accounts, err)
	}

	if empty, err := repo.TasksPinnedTo(ctx, proxyKind, "nobody"); err != nil || len(empty) != 0 {
		t.Fatalf("TasksPinnedTo(nobody) = %v, %v, want none", empty, err)
	}
}

// TestMigratesProxyPlacementIntoAssignments verifies the backfill carries a
// shipped database's proxy placement into the kinded column without losing the
// distinction it was storing. SQL NULL, the empty string, and a named value are
// three different instructions — inherit, lease none, lease this — and a
// migration that flattened them would silently repoint live tasks.
func TestMigratesProxyPlacementIntoAssignments(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "legacy.db")

	// The schema as of the task_groups migration, stamped at that version so
	// only the assignments migrations run on it.
	raw, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE tasks (
		id             TEXT PRIMARY KEY,
		workflow_id    TEXT NOT NULL,
		state          TEXT NOT NULL DEFAULT '',
		status         TEXT NOT NULL DEFAULT '',
		snapshot       BLOB,
		output         BLOB,
		group_id       TEXT NOT NULL DEFAULT 'global',
		proxy_group_id TEXT,
		created_at     TEXT NOT NULL DEFAULT '',
		updated_at     TEXT NOT NULL DEFAULT '',
		proxy_id       TEXT
	)`); err != nil {
		t.Fatalf("create legacy tasks: %v", err)
	}
	if _, err := raw.Exec(`CREATE TABLE task_groups (
		id             TEXT PRIMARY KEY,
		proxy_group_id TEXT NOT NULL DEFAULT '',
		created_at     TEXT NOT NULL DEFAULT '',
		updated_at     TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		t.Fatalf("create legacy task_groups: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO tasks (id, workflow_id, group_id, proxy_group_id, proxy_id) VALUES
		('inherits',  'wf1', 'checkout', NULL,          NULL),
		('opted-out', 'wf1', 'checkout', '',            NULL),
		('assigned',  'wf1', 'checkout', 'residential', NULL),
		('pinned',    'wf1', 'checkout', 'residential', 'p7'),
		('pin-only',  'wf1', 'checkout', NULL,          'p8')`); err != nil {
		t.Fatalf("seed legacy tasks: %v", err)
	}
	if _, err := raw.Exec(`INSERT INTO task_groups (id, proxy_group_id) VALUES ('checkout', 'residential'), ('bare', '')`); err != nil {
		t.Fatalf("seed legacy task_groups: %v", err)
	}
	if _, err := raw.Exec(`PRAGMA user_version = 8`); err != nil {
		t.Fatalf("stamp legacy version: %v", err)
	}
	raw.Close()

	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("NewSQLite on legacy db: %v", err)
	}
	t.Cleanup(func() { repo.Close() })

	for _, tc := range []struct {
		id          string
		wantGroup   *string
		wantPin     *string
		description string
	}{
		{"inherits", nil, nil, "no placement of its own, so it must keep inheriting"},
		{"opted-out", ptr(""), nil, "an explicit opt-out must not become an inherit"},
		{"assigned", ptr("residential"), nil, "a named group must survive"},
		{"pinned", ptr("residential"), ptr("p7"), "a pin must survive alongside its group"},
		{"pin-only", nil, ptr("p8"), "a pin whose group is inherited must not gain an opt-out"},
	} {
		got, err := repo.RecoverTask(ctx, tc.id)
		if err != nil {
			t.Fatalf("RecoverTask %s: %v", tc.id, err)
		}
		a := got.Assignments[proxyKind]
		if !samePtr(a.GroupID, tc.wantGroup) {
			t.Fatalf("%s: group = %v, want %v — %s", tc.id, show(a.GroupID), show(tc.wantGroup), tc.description)
		}
		if !samePtr(a.ResourceID, tc.wantPin) {
			t.Fatalf("%s: pin = %v, want %v — %s", tc.id, show(a.ResourceID), show(tc.wantPin), tc.description)
		}
	}

	// A wholly unplaced row leaves the column at its default, which the pin
	// query must tolerate rather than reject as malformed JSON.
	if got, err := repo.TasksPinnedTo(ctx, proxyKind, "p7"); err != nil || len(got) != 1 || got[0].ID != "pinned" {
		t.Fatalf("TasksPinnedTo after migration = %v, %v, want the pinned record", got, err)
	}

	group, found, err := repo.GetGroup(ctx, "checkout")
	if err != nil || !found {
		t.Fatalf("GetGroup(checkout) = found %v, err %v", found, err)
	}
	if group.ResourceGroups[proxyKind] != "residential" {
		t.Fatalf("group resource groups = %v, want residential proxies", group.ResourceGroups)
	}
	bare, _, err := repo.GetGroup(ctx, "bare")
	if err != nil {
		t.Fatalf("GetGroup(bare): %v", err)
	}
	if len(bare.ResourceGroups) != 0 {
		t.Fatalf("bare group resource groups = %v, want none", bare.ResourceGroups)
	}
}

// ptr takes the address of a string literal, for the optional halves of an
// assignment.
func ptr(s string) *string { return &s }

// samePtr compares two optional assignment halves, nil included.
func samePtr(got, want *string) bool {
	if got == nil || want == nil {
		return got == nil && want == nil
	}
	return *got == *want
}

// show renders an optional assignment half for a failure message.
func show(v *string) string {
	if v == nil {
		return "nil"
	}
	return `"` + *v + `"`
}
