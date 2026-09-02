package leasing

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// pairKey addresses one hold row (resource, task); the same pair type keys
// counters as (scope, name).
type pairKey struct {
	scope, name string
}

// memoryRepository is the Repository a nil one is swapped for, and the
// reference in-process store: maps behind one mutex, with the same
// coordination semantics the durable adapters implement — versioned saves,
// TTL'd holds measured against this process's clock, conditional locks,
// counters. Records are stored and returned by value as given; callers
// wanting a serialization boundary (deep copies, normalization) wrap it, as
// the memory package's stores do.
type memoryRepository[R any, P Leasable[R]] struct {
	mu       sync.Mutex
	records  map[string]R
	holds    map[pairKey]Hold
	counters map[pairKey]int64
	groups   map[string]Group
}

// NewMemoryRepository returns an empty in-process Repository over one model.
// It is what a Manager built with a nil repository runs on: capacity, locks,
// and versions are enforced for real, but nothing survives the process.
func NewMemoryRepository[R any, P Leasable[R]]() Repository[R] {
	return &memoryRepository[R, P]{
		records:  make(map[string]R),
		holds:    make(map[pairKey]Hold),
		counters: make(map[pairKey]int64),
		groups:   make(map[string]Group),
	}
}

// errMemoryResourceMissing reports a lock claim on a resource the store does
// not hold. Deliberately not ErrResourceNotFound, whose contract is pins.
var errMemoryResourceMissing = errors.New("resource not found")

func (s *memoryRepository[R, P]) core(r *R) *Resource { return P(r).core() }

func (s *memoryRepository[R, P]) List(context.Context) ([]R, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.records))
	for id := range s.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	listed := make([]R, 0, len(ids))
	for _, id := range ids {
		listed = append(listed, s.records[id])
	}
	return listed, nil
}

// Save is the conditional write: version 0 creates, version N replaces the
// stored version N preserving CreatedAt, anything else is ErrStale.
func (s *memoryRepository[R, P]) Save(_ context.Context, record R) (int64, error) {
	c := s.core(&record)
	c.CreatedAt, c.UpdatedAt = c.CreatedAt.UTC(), c.UpdatedAt.UTC()

	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.records[c.ID]
	switch {
	case c.Version == 0 && !ok:
		c.Version = 1
	case c.Version == 0, !ok:
		return 0, fmt.Errorf("save %s: %w", c.ID, ErrStale)
	default:
		kept := s.core(&existing)
		if kept.Version != c.Version {
			return 0, fmt.Errorf("save %s: %w", c.ID, ErrStale)
		}
		c.CreatedAt = kept.CreatedAt
		c.Version++
	}
	s.records[c.ID] = record
	return c.Version, nil
}

// Delete removes the record and its hold rows; absent ids are a no-op.
func (s *memoryRepository[R, P]) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	for k := range s.holds {
		if k.scope == id {
			delete(s.holds, k)
		}
	}
	return nil
}

// Acquire takes or re-enters a hold under cap, against this process's clock.
// Expired rows of the resource are pruned here, on the write path, so a
// superseded hold cannot be revived by its owner's next heartbeat. A
// resource locked by another task refuses with ErrLockHeld — the caller's
// cache may not know about the lock yet, so the store is what says no. A
// resource with no record reads as unlocked: the store does not police
// existence, its manager does.
func (s *memoryRepository[R, P]) Acquire(_ context.Context, resourceID, taskID string, cap int, ttl time.Duration) (Hold, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rec, ok := s.records[resourceID]; ok {
		if owner := s.core(&rec).OwnerID; owner != "" && owner != taskID {
			return Hold{}, fmt.Errorf("acquire %s for task %s: %w", resourceID, taskID, ErrLockHeld)
		}
	}

	now := time.Now()
	live := 0
	for k, h := range s.holds {
		if k.scope != resourceID {
			continue
		}
		if !h.ExpiresAt.After(now) {
			delete(s.holds, k)
			continue
		}
		if k.name != taskID {
			live++
		}
	}
	key := pairKey{resourceID, taskID}
	own, reentering := s.holds[key]
	if !reentering {
		if cap > 0 && live >= cap {
			return Hold{}, fmt.Errorf("acquire %s for task %s: %w", resourceID, taskID, ErrCapacity)
		}
		own = Hold{ResourceID: resourceID, TaskID: taskID}
	}
	own.Count++
	own.ExpiresAt = now.Add(ttl)
	s.holds[key] = own
	return own, nil
}

// ReleaseHold decrements the task's hold, removing it at zero; no hold is a
// no-op.
func (s *memoryRepository[R, P]) ReleaseHold(_ context.Context, resourceID, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := pairKey{resourceID, taskID}
	own, ok := s.holds[key]
	if !ok {
		return nil
	}
	own.Count--
	if own.Count <= 0 {
		delete(s.holds, key)
		return nil
	}
	s.holds[key] = own
	return nil
}

// RenewHolds extends every unexpired hold the task has; expired ones stay
// expired — their capacity may already be promised elsewhere.
func (s *memoryRepository[R, P]) RenewHolds(_ context.Context, taskID string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, h := range s.holds {
		if k.name == taskID && h.ExpiresAt.After(now) {
			h.ExpiresAt = now.Add(ttl)
			s.holds[k] = h
		}
	}
	return nil
}

// ListHolds returns every hold row, expired ones included, ordered by
// resource then task.
func (s *memoryRepository[R, P]) ListHolds(context.Context) ([]Hold, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	listed := make([]Hold, 0, len(s.holds))
	for _, h := range s.holds {
		listed = append(listed, h)
	}
	sort.Slice(listed, func(i, j int) bool {
		if listed[i].ResourceID != listed[j].ResourceID {
			return listed[i].ResourceID < listed[j].ResourceID
		}
		return listed[i].TaskID < listed[j].TaskID
	})
	return listed, nil
}

// ClaimLock binds the resource to the task iff unlocked or already its own.
func (s *memoryRepository[R, P]) ClaimLock(_ context.Context, resourceID, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[resourceID]
	if !ok {
		return fmt.Errorf("claim lock on %s: %w", resourceID, errMemoryResourceMissing)
	}
	c := s.core(&rec)
	if c.OwnerID != "" && c.OwnerID != taskID {
		return fmt.Errorf("claim lock on %s for task %s: %w", resourceID, taskID, ErrLockHeld)
	}
	c.OwnerID = taskID
	c.Version++
	c.UpdatedAt = time.Now().UTC()
	s.records[resourceID] = rec
	return nil
}

// ReleaseLock clears the lock iff the task owns it; otherwise a no-op.
func (s *memoryRepository[R, P]) ReleaseLock(_ context.Context, resourceID, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[resourceID]
	if !ok {
		return nil
	}
	c := s.core(&rec)
	if c.OwnerID != taskID {
		return nil
	}
	c.OwnerID = ""
	c.Version++
	c.UpdatedAt = time.Now().UTC()
	s.records[resourceID] = rec
	return nil
}

func (s *memoryRepository[R, P]) ListGroups(context.Context) ([]Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.groups))
	for id := range s.groups {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	listed := make([]Group, 0, len(ids))
	for _, id := range ids {
		listed = append(listed, s.groups[id])
	}
	return listed, nil
}

// SaveGroup upserts the group, preserving CreatedAt across replacements.
func (s *memoryRepository[R, P]) SaveGroup(_ context.Context, g Group) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	g.CreatedAt, g.UpdatedAt = g.CreatedAt.UTC(), g.UpdatedAt.UTC()
	if kept, ok := s.groups[g.ID]; ok {
		g.CreatedAt = kept.CreatedAt
	}
	s.groups[g.ID] = g
	return nil
}

func (s *memoryRepository[R, P]) DeleteGroup(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.groups, id)
	return nil
}

// Increment adds delta to the counter under (scope, name), creating it at 0.
func (s *memoryRepository[R, P]) Increment(_ context.Context, scope, name string, delta int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := pairKey{scope, name}
	s.counters[key] += delta
	return s.counters[key], nil
}
