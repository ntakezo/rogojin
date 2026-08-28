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
