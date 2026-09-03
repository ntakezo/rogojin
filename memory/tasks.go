package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/tasks"
	"github.com/ntakezo/rogojin/workflows"
)

// Tasks is the tasks.Repository: one record per task carrying its placement,
// last checkpoint, and terminal outcome, and one per task group.
type Tasks struct {
	mu      sync.Mutex
	records map[string]tasks.Task
	order   []string
	groups  map[string]tasks.Group
	// effects is the durable effect log, task id -> effect key -> result.
	effects map[string]map[string][]byte
}

var _ tasks.Repository = (*Tasks)(nil)

// NewTasks builds an empty in-memory tasks store.
func NewTasks() tasks.Repository {
	return &Tasks{
		records: make(map[string]tasks.Task),
		groups:  make(map[string]tasks.Group),
		effects: make(map[string]map[string][]byte),
	}
}

// CreateTask inserts a fresh task record: workflow, placement, input, and
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
		Input:       copyBytes(rec.Input),
		CreatedAt:   storeTime(rec.CreatedAt),
		UpdatedAt:   storeTime(rec.UpdatedAt),
	}
	s.order = append(s.order, rec.ID)
	return nil
}

// ClaimTask atomically takes ownership for node iff the task is unclaimed,
// already node's, or leased past expiry, returning the claimed record with
// its new version; ErrClaimHeld reports a live claim by another node. The
// store's own clock decides expiry.
func (s *Tasks) ClaimTask(ctx context.Context, id, node string, ttl time.Duration) (tasks.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	if !ok {
		return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, tasks.ErrTaskNotFound)
	}
	now := time.Now().UTC()
	if !claimFree(rec, node, now) {
		return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, tasks.ErrClaimHeld)
	}
	rec.OwnerNode, rec.LeaseExpiresAt = node, now.Add(ttl)
	rec.Version++
	rec.UpdatedAt = now
	s.records[id] = rec
	return copyTask(rec), nil
}

// claimFree reports whether node may take the task's claim: unclaimed,
// already node's own, or expired.
func claimFree(rec tasks.Task, node string, now time.Time) bool {
	return rec.OwnerNode == "" || rec.OwnerNode == node ||
		(!rec.LeaseExpiresAt.IsZero() && rec.LeaseExpiresAt.Before(now))
}

// RenewClaim extends the lease iff node still owns the claim, without
// bumping the version — renewal moves only the lease clock, so it never
// invalidates the owner's in-flight conditional writes. ErrStale reports
// the claim gone or another node's.
func (s *Tasks) RenewClaim(ctx context.Context, id, node string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	if !ok {
		return fmt.Errorf("renew claim %s: %w", id, tasks.ErrTaskNotFound)
	}
	if node == "" || rec.OwnerNode != node {
		return fmt.Errorf("renew claim %s: %w", id, tasks.ErrStale)
	}
	now := time.Now().UTC()
	rec.LeaseExpiresAt = now.Add(ttl)
	rec.UpdatedAt = now
	s.records[id] = rec
	return nil
}

// ReleaseClaim clears the claim iff node owns it, silently a no-op
// otherwise: a release racing its own usurpation is a shutdown path, not an
// error.
func (s *Tasks) ReleaseClaim(ctx context.Context, id, node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	if !ok {
		return fmt.Errorf("release claim %s: %w", id, tasks.ErrTaskNotFound)
	}
	if node == "" || rec.OwnerNode != node {
		return nil
	}
	rec.OwnerNode, rec.LeaseExpiresAt = "", time.Time{}
	rec.Version++
	rec.UpdatedAt = time.Now().UTC()
	s.records[id] = rec
	return nil
}

// SaveCheckpoint overwrites the task's last-checkpointed status, state, and
// snapshot iff version matches and node owns the claim, bumping and
// returning the version; ErrStale reports the write lost. It fails with
// tasks.ErrTaskNotFound if no record exists.
func (s *Tasks) SaveCheckpoint(ctx context.Context, id string, version int64, node, status, state string, snapshot []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	if !ok {
		return 0, fmt.Errorf("save checkpoint %s: %w", id, tasks.ErrTaskNotFound)
	}
	if err := writeAllowed(rec, version, node); err != nil {
		return 0, fmt.Errorf("save checkpoint %s: %w", id, err)
	}
	rec.Status, rec.State, rec.Snapshot = status, state, copyBytes(snapshot)
	rec.Version++
	rec.UpdatedAt = time.Now().UTC()
	s.records[id] = rec
	return rec.Version, nil
}

// writeAllowed applies the conditional-write predicate shared by
// SaveCheckpoint and MarkTerminal.
func writeAllowed(rec tasks.Task, version int64, node string) error {
	if rec.Version != version || rec.OwnerNode != node {
		return tasks.ErrStale
	}
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

// MarkTerminal stamps the terminal outcome and the run's output under
// SaveCheckpoint's conditionality, additionally clearing the claim — a
// finished task is nobody's to run. State and snapshot stay intact as a
// valid resume entry. It fails with tasks.ErrTaskNotFound if no record
// exists.
func (s *Tasks) MarkTerminal(ctx context.Context, id string, version int64, node, outcome string, output []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[id]
	if !ok {
		return 0, fmt.Errorf("mark terminal %s: %w", id, tasks.ErrTaskNotFound)
	}
	if err := writeAllowed(rec, version, node); err != nil {
		return 0, fmt.Errorf("mark terminal %s: %w", id, err)
	}
	rec.Status, rec.Output = outcome, copyBytes(output)
	rec.OwnerNode, rec.LeaseExpiresAt = "", time.Time{}
	rec.Version++
	rec.UpdatedAt = time.Now().UTC()
	s.records[id] = rec
	return rec.Version, nil
}

// ListClaimable returns the non-terminal tasks whose claim is free for any
// taker — unclaimed or leased past expiry — in insertion order.
func (s *Tasks) ListClaimable(ctx context.Context) ([]tasks.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	listed := make([]tasks.Task, 0)
	for _, id := range s.order {
		rec := s.records[id]
		if workflows.Status(rec.Status).Terminal() {
			continue
		}
		if rec.OwnerNode == "" || (!rec.LeaseExpiresAt.IsZero() && rec.LeaseExpiresAt.Before(now)) {
			listed = append(listed, copyTask(rec))
		}
	}
	return listed, nil
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

// RecordEffect stores result under (taskID, key) if no record exists, and
// returns the stored result either way; first reports whether this call
// created it. The store does not require the task record to exist.
func (s *Tasks) RecordEffect(ctx context.Context, taskID, key string, result []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stored, ok := s.effects[taskID][key]; ok {
		return copyBytes(stored), false, nil
	}
	if s.effects[taskID] == nil {
		s.effects[taskID] = make(map[string][]byte)
	}
	s.effects[taskID][key] = copyBytes(result)
	return copyBytes(result), true, nil
}

// ListEffects returns every effect recorded for the task, keyed by effect key.
func (s *Tasks) ListEffects(ctx context.Context, taskID string) (map[string][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	effects := make(map[string][]byte, len(s.effects[taskID]))
	for key, result := range s.effects[taskID] {
		effects[key] = copyBytes(result)
	}
	return effects, nil
}

// DeleteTask removes the task's record and its recorded effects; absent ids
// are a no-op.
func (s *Tasks) DeleteTask(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.effects, id)
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
	rec.Input = copyBytes(rec.Input)
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
