package tasksqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

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

// TestCreateTaskRecoverable verifies a created task is recoverable by id with its workflow and no checkpoint yet,
// because recovery must be able to resolve the workflow before any state has run.
func TestCreateTaskRecoverable(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.CreateTask(ctx, "t1", "wf1"); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

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

	if err := repo.CreateTask(ctx, "t1", "wf1"); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
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

	if err := repo.CreateTask(ctx, "t1", "wf1"); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
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

	if err := repo.CreateTask(ctx, "t1", "wf1"); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
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

	if err := repo.CreateTask(ctx, "t1", "wf1"); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
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

	if err := repo.CreateTask(ctx, "t1", "wf1"); err != nil {
		t.Fatalf("CreateTask t1: %v", err)
	}
	if err := repo.CreateTask(ctx, "t2", "wf2"); err != nil {
		t.Fatalf("CreateTask t2: %v", err)
	}
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

	if err := repo.CreateTask(ctx, "t1", "wf1"); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
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
	if err := repo.CreateTask(ctx, "t1", "wf1"); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
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
