package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/storetest"
	"github.com/ntakezo/rogojin/tasks"
)

// satisfiesRepositoryPort fails to compile if Tasks drifts from the persistence port it exists to implement.
var _ tasks.Repository = (*Tasks)(nil)

// newTestTasks opens the tasks store on a fresh temp-file database.
func newTestTasks(t *testing.T) tasks.Repository {
	t.Helper()
	repo, err := NewTasks(openTestDB(t))
	if err != nil {
		t.Fatalf("NewTasks: %v", err)
	}
	return repo
}

// TestTasksContract runs the shared store contract against the sqlite tasks
// store; everything the store promises is asserted there. What follows is
// only what is genuinely sqlite's: surviving a file reopen.
func TestTasksContract(t *testing.T) {
	storetest.Tasks(t, newTestTasks)
}

// TestTasksPersistsAcrossReopen verifies a checkpoint written to the file is
// what a fresh open reads back — the durability the store exists for.
func TestTasksPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dsn := filepath.Join(t.TempDir(), "tasks.db")

	repoDB := openAt(t, dsn)
	repo, err := NewTasks(repoDB)
	if err != nil {
		t.Fatalf("NewTasks: %v", err)
	}
	now := time.Now().UTC()
	rec := tasks.Task{ID: "t1", WorkflowID: "wf1", GroupID: tasks.GlobalGroup, CreatedAt: now, UpdatedAt: now}
	if err := repo.CreateTask(ctx, rec); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
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

	got, err := reopened.RecoverTask(ctx, "t1")
	if err != nil {
		t.Fatalf("RecoverTask after reopen: %v", err)
	}
	if got.Status != "suspended" || got.State != "wait" || string(got.Snapshot) != "snap" {
		t.Fatalf("checkpoint did not survive reopen: %+v", got)
	}
}
