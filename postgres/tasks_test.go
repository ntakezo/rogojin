package postgres

import (
	"testing"

	"github.com/ntakezo/rogojin/storetest"
	"github.com/ntakezo/rogojin/tasks"
)

// satisfiesRepositoryPort fails to compile if Tasks drifts from the persistence port it exists to implement.
var _ tasks.Repository = (*Tasks)(nil)

// newTestTasks opens the tasks store on a fresh schema.
func newTestTasks(t *testing.T) tasks.Repository {
	t.Helper()
	repo, err := NewTasks(openTestDB(t))
	if err != nil {
		t.Fatalf("NewTasks: %v", err)
	}
	return repo
}

// TestTasksContract runs the shared store contract against the postgres tasks
// store; everything the store promises is asserted there.
func TestTasksContract(t *testing.T) {
	storetest.Tasks(t, newTestTasks)
}
