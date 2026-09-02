package memory

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/leasing"
)

// holdKey addresses one hold row: one task's stake in one resource. The same
// pair type keys counters as scope and name.
type holdKey struct {
	scope, name string
}

// leaseStore is the record half every leasing-shaped store shares: versioned
// conditional saves, holds, locks, and counters over one model. The model
// package wires it with a projection to the embedded leasing.Resource and a
// copy hook that normalizes and deep-copies the model's own fields, the same
// division of labor the storetest suite uses. noun names the model in errors
// ("payment", "proxy", "account").
type leaseStore[R any] struct {
	mu       sync.Mutex
	noun     string
	records  map[string]R
	holds    map[holdKey]leasing.Hold
	counters map[holdKey]int64
	groups   groupStore
	resource func(*R) *leasing.Resource
	copyRec  func(R) (R, error)
}

func newLeaseStore[R any](noun string, resource func(*R) *leasing.Resource, copyRec func(R) (R, error)) leaseStore[R] {
	return leaseStore[R]{
		noun:     noun,
		records:  make(map[string]R),
		holds:    make(map[holdKey]leasing.Hold),
		counters: make(map[holdKey]int64),
		groups:   newGroupStore(),
		resource: resource,
		copyRec:  copyRec,
	}
}

// list returns every record in stable id order, deep-copied.
func (s *leaseStore[R]) list() ([]R, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	listed := make([]R, 0, len(s.records))
	for _, id := range sortedIDs(s.records) {
		rec, err := s.copyRec(s.records[id])
		if err != nil {
			return nil, fmt.Errorf("list %ss: %w", s.noun, err)
		}
		listed = append(listed, rec)
	}
	return listed, nil
}

// save is the conditional write: version 0 creates, version N replaces the
// stored version N preserving CreatedAt, anything else is ErrStale.
func (s *leaseStore[R]) save(rec R) (int64, error) {
	rec, err := s.copyRec(rec)
	if err != nil {
		return 0, fmt.Errorf("save %s: %w", s.noun, err)
	}
	c := s.resource(&rec)
	c.CreatedAt, c.UpdatedAt = storeTime(c.CreatedAt), storeTime(c.UpdatedAt)

	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.records[c.ID]
	switch {
	case c.Version == 0 && !ok:
		c.Version = 1
	case c.Version == 0, !ok:
		return 0, fmt.Errorf("save %s %s: %w", s.noun, c.ID, leasing.ErrStale)
	default:
		kept := s.resource(&existing)
		if kept.Version != c.Version {
			return 0, fmt.Errorf("save %s %s: %w", s.noun, c.ID, leasing.ErrStale)
		}
		c.CreatedAt = kept.CreatedAt
		c.Version++
	}
	s.records[c.ID] = rec
	return c.Version, nil
}

// delete removes the record and its hold rows; absent ids are a no-op.
func (s *leaseStore[R]) delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	for k := range s.holds {
		if k.scope == id {
			delete(s.holds, k)
		}
	}
}

// acquire takes or re-enters a hold under cap, against the store's clock.
// Expired rows of the resource are pruned here, on the write path, so a
// superseded hold cannot be revived by its owner's next heartbeat.
func (s *leaseStore[R]) acquire(resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

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
	key := holdKey{resourceID, taskID}
	own, reentering := s.holds[key]
	if !reentering {
		if cap > 0 && live >= cap {
			return leasing.Hold{}, fmt.Errorf("acquire %s %s for task %s: %w", s.noun, resourceID, taskID, leasing.ErrCapacity)
		}
		own = leasing.Hold{ResourceID: resourceID, TaskID: taskID}
	}
	own.Count++
	own.ExpiresAt = now.Add(ttl)
	s.holds[key] = own
	return own, nil
}

// releaseHold decrements the task's hold, removing it at zero; no hold is a
// no-op.
func (s *leaseStore[R]) releaseHold(resourceID, taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := holdKey{resourceID, taskID}
	own, ok := s.holds[key]
	if !ok {
		return
	}
	own.Count--
	if own.Count <= 0 {
		delete(s.holds, key)
		return
	}
	s.holds[key] = own
}

// renewHolds extends every unexpired hold the task has; expired ones stay
// expired — their capacity may already be promised elsewhere.
func (s *leaseStore[R]) renewHolds(taskID string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for k, h := range s.holds {
		if k.name == taskID && h.ExpiresAt.After(now) {
			h.ExpiresAt = now.Add(ttl)
			s.holds[k] = h
		}
	}
}

// listHolds returns every hold row, expired ones included, ordered by
// resource then task.
func (s *leaseStore[R]) listHolds() []leasing.Hold {
	s.mu.Lock()
	defer s.mu.Unlock()
	listed := make([]leasing.Hold, 0, len(s.holds))
	for _, h := range s.holds {
		listed = append(listed, h)
	}
	sort.Slice(listed, func(i, j int) bool {
		if listed[i].ResourceID != listed[j].ResourceID {
			return listed[i].ResourceID < listed[j].ResourceID
		}
		return listed[i].TaskID < listed[j].TaskID
	})
	return listed
}

// claimLock binds the resource to the task iff unlocked or already its own.
func (s *leaseStore[R]) claimLock(resourceID, taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[resourceID]
	if !ok {
		return fmt.Errorf("claim lock on %s %s: %w", s.noun, resourceID, errResourceMissing)
	}
	c := s.resource(&rec)
	if c.OwnerID != "" && c.OwnerID != taskID {
		return fmt.Errorf("claim lock on %s %s for task %s: %w", s.noun, resourceID, taskID, leasing.ErrLockHeld)
	}
	c.OwnerID = taskID
	c.Version++
	c.UpdatedAt = time.Now().UTC()
	s.records[resourceID] = rec
	return nil
}

// releaseLock clears the lock iff the task owns it; otherwise a no-op.
func (s *leaseStore[R]) releaseLock(resourceID, taskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[resourceID]
	if !ok {
		return
	}
	c := s.resource(&rec)
	if c.OwnerID != taskID {
		return
	}
	c.OwnerID = ""
	c.Version++
	c.UpdatedAt = time.Now().UTC()
	s.records[resourceID] = rec
}

// listGroups, saveGroup, and deleteGroup guard the shared groupStore with the
// store's own lock.
func (s *leaseStore[R]) listGroups() []leasing.Group {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groups.list()
}

func (s *leaseStore[R]) saveGroup(g leasing.Group) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups.save(g)
}

func (s *leaseStore[R]) deleteGroup(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups.delete(id)
}

// increment adds delta to the counter under (scope, name), creating it at 0.
func (s *leaseStore[R]) increment(scope, name string, delta int64) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := holdKey{scope, name}
	s.counters[key] += delta
	return s.counters[key]
}

// errResourceMissing reports a lock claim on a resource the store does not
// hold. It is deliberately not leasing.ErrResourceNotFound, whose contract is
// about pins.
var errResourceMissing = errors.New("resource not found")
