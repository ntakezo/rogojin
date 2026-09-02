package memory

import (
	"testing"

	"github.com/ntakezo/rogojin/storetest"
	"github.com/ntakezo/rogojin/tasks"
)

// TestTasksContract runs the shared store contract against the in-memory
// tasks store; everything the store promises is asserted there.
func TestTasksContract(t *testing.T) {
	storetest.Tasks(t, func(t *testing.T) tasks.Repository { return NewTasks() })
}
