package tasksqlite

import (
	"context"
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
	if err := repo.MarkTerminal(ctx, "t1", "done"); err != nil {
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
	if err := repo.MarkTerminal(ctx, "t2", "done"); err != nil {
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
