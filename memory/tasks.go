package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/tasks"
)

// Tasks is the tasks.Repository: one record per task carrying its placement,
// last checkpoint, and terminal outcome, and one per task group.
type Tasks struct {
	mu      sync.Mutex
	records map[string]tasks.Task
	order   []string
	groups  map[string]tasks.Group
}

var _ tasks.Repository = (*Tasks)(nil)

// NewTasks builds an empty in-memory tasks store.
func NewTasks() tasks.Repository {
	return &Tasks{records: make(map[string]tasks.Task), groups: make(map[string]tasks.Group)}
}

// CreateTask inserts a fresh task record: workflow, placement, and
// timestamps, with no checkpoint yet. A duplicate id is refused.
func (s *Tasks) CreateTask(ctx context.Context, rec tasks.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[rec.ID]; ok {
		return fmt.Errorf("create task %s: a record already exists", rec.ID)
	}
	s.records[rec.ID] = tasks.Task{
		ID:          rec.ID,
		WorkflowID:  rec.WorkflowID,
		GroupID:     rec.GroupID,
		Assignments: copyAssignments(rec.Assignments),
		CreatedAt:   storeTime(rec.CreatedAt),
		UpdatedAt:   storeTime(rec.UpdatedAt),
	}
	s.order = append(s.order, rec.ID)
	return nil
}

// SaveCheckpoint overwrites the task's last-checkpointed status, state, and
// snapshot, refreshing UpdatedAt. It fails with tasks.ErrTaskNotFound if no
// record exists.
func (s *Tasks) SaveCheckpoint(ctx context.Context, id, status, state string, snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	if !ok {
		return fmt.Errorf("save checkpoint %s: %w", id, tasks.ErrTaskNotFound)
	}
	rec.Status, rec.State, rec.Snapshot = status, state, copyBytes(snapshot)
	rec.UpdatedAt = time.Now().UTC()
	s.records[id] = rec
	return nil
}

// SaveAssignment repoints a task's placement for one kind, leaving every
// other kind and the rest of the record untouched. The kind is validated
// here even though the manager already refuses bad ones, matching the
// sqlite store's own guard.
func (s *Tasks) SaveAssignment(ctx context.Context, id string, kind leasing.Kind, a tasks.Assignment) error {
	if err := kind.Validate(); err != nil {
		return fmt.Errorf("assign placement of task %s: %w", id, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	if !ok {
		return fmt.Errorf("assign %s placement: task %s does not exist", kind, id)
	}
	assignments := make(map[leasing.Kind]tasks.Assignment, len(rec.Assignments)+1)
	for k, v := range rec.Assignments {
		assignments[k] = v
	}
	assignments[kind] = copyAssignment(a)
	rec.Assignments = assignments
	rec.UpdatedAt = time.Now().UTC()
	s.records[id] = rec
	return nil
}

// MarkTerminal stamps the terminal outcome and the run's output, refreshing
// UpdatedAt and leaving state and snapshot intact as a valid resume entry.
// It fails with tasks.ErrTaskNotFound if no record exists.
func (s *Tasks) MarkTerminal(ctx context.Context, id, outcome string, output []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	if !ok {
		return fmt.Errorf("mark terminal %s: %w", id, tasks.ErrTaskNotFound)
	}
	rec.Status, rec.Output = outcome, copyBytes(output)
	rec.UpdatedAt = time.Now().UTC()
	s.records[id] = rec
	return nil
}

// RecoverTask returns the record for id, or tasks.ErrTaskNotFound if none
// exists.
func (s *Tasks) RecoverTask(ctx context.Context, id string) (tasks.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	if !ok {
		return tasks.Task{}, fmt.Errorf("recover task %s: %w", id, tasks.ErrTaskNotFound)
	}
	return copyTask(rec), nil
}

// RecoverAll returns every persisted record in insertion order, terminal
// ones included.
func (s *Tasks) RecoverAll(ctx context.Context) ([]tasks.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	records := make([]tasks.Task, 0, len(s.order))
	for _, id := range s.order {
		records = append(records, copyTask(s.records[id]))
	}
	return records, nil
}

// DeleteTask removes the task's record; absent ids are a no-op.
func (s *Tasks) DeleteTask(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[id]; !ok {
		return nil
	}
	delete(s.records, id)
	for i, kept := range s.order {
		if kept == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	return nil
}

// SaveGroup upserts the task group's record. CreatedAt is written on insert
// and never overwritten.
func (s *Tasks) SaveGroup(ctx context.Context, g tasks.Group) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g.ResourceGroups = copyMap(g.ResourceGroups)
	g.CreatedAt, g.UpdatedAt = storeTime(g.CreatedAt), storeTime(g.UpdatedAt)
	if kept, ok := s.groups[g.ID]; ok {
		g.CreatedAt = kept.CreatedAt
	}
	s.groups[g.ID] = g
	return nil
}

// GetGroup returns the group and whether a record exists for the id.
func (s *Tasks) GetGroup(ctx context.Context, id string) (tasks.Group, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g, ok := s.groups[id]
	if !ok {
		return tasks.Group{}, false, nil
	}
	g.ResourceGroups = copyMap(g.ResourceGroups)
	return g, true, nil
}

// ListGroups returns every stored task group in stable id order.
func (s *Tasks) ListGroups(ctx context.Context) ([]tasks.Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	listed := make([]tasks.Group, 0, len(s.groups))
	for _, id := range sortedIDs(s.groups) {
		g := s.groups[id]
		g.ResourceGroups = copyMap(g.ResourceGroups)
		listed = append(listed, g)
	}
	return listed, nil
}

// DeleteGroup removes the group's record; absent ids are a no-op. Member
// tasks are the task manager's to delete — the store cascades nothing.
func (s *Tasks) DeleteGroup(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.groups, id)
	return nil
}

// TasksInGroup returns the ids of every task in the group, in stable id
// order.
func (s *Tasks) TasksInGroup(ctx context.Context, groupID string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0)
	for id, rec := range s.records {
		if rec.GroupID == groupID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

// copyTask deep-copies a record so the store's copy is never reachable.
func copyTask(rec tasks.Task) tasks.Task {
	rec.Snapshot = copyBytes(rec.Snapshot)
	rec.Output = copyBytes(rec.Output)
	if rec.Assignments != nil {
		assignments := make(map[leasing.Kind]tasks.Assignment, len(rec.Assignments))
		for k, v := range rec.Assignments {
			assignments[k] = copyAssignment(v)
		}
		rec.Assignments = assignments
	}
	return rec
}

// copyAssignment clones the tri-state pointers so caller and store never
// share them.
func copyAssignment(a tasks.Assignment) tasks.Assignment {
	if a.GroupID != nil {
		g := *a.GroupID
		a.GroupID = &g
	}
	if a.ResourceID != nil {
		r := *a.ResourceID
		a.ResourceID = &r
	}
	return a
}

// copyAssignments mirrors the assignments column round trip: an empty map
// stores as nil.
func copyAssignments(a map[leasing.Kind]tasks.Assignment) map[leasing.Kind]tasks.Assignment {
	if len(a) == 0 {
		return nil
	}
	out := make(map[leasing.Kind]tasks.Assignment, len(a))
	for k, v := range a {
		out[k] = copyAssignment(v)
	}
	return out
}
