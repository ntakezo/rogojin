package memory

import (
	"context"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/leasing"
)

// leaseStore is the record half every leasing-shaped store shares. The
// coordination semantics — versioned saves, holds, locks, counters — live in
// leasing.NewMemoryRepository, the same store a nil-repository manager runs
// on; this wrapper adds what a durable adapter's encoding gives for free: a
// serialization boundary. The model package wires a copy hook that
// normalizes and deep-copies the model's own fields, so a caller mutating
// what it saved or listed never reaches the store's copy. noun names the
// model in errors ("payment", "proxy", "account").
type leaseStore[R any] struct {
	noun    string
	inner   leasing.Repository[R]
	copyRec func(R) (R, error)
}

func newLeaseStore[R any, P leasing.Leasable[R]](noun string, copyRec func(R) (R, error)) leaseStore[R] {
	return leaseStore[R]{noun: noun, inner: leasing.NewMemoryRepository[R, P](), copyRec: copyRec}
}

// list returns every record in stable id order, deep-copied.
func (s *leaseStore[R]) list(ctx context.Context) ([]R, error) {
	listed, err := s.inner.List(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]R, 0, len(listed))
	for _, rec := range listed {
		copied, err := s.copyRec(rec)
		if err != nil {
			return nil, fmt.Errorf("list %ss: %w", s.noun, err)
		}
		out = append(out, copied)
	}
	return out, nil
}

// save deep-copies and validates on the way in, then hands the conditional
// write to the inner store.
func (s *leaseStore[R]) save(ctx context.Context, rec R) (int64, error) {
	copied, err := s.copyRec(rec)
	if err != nil {
		return 0, fmt.Errorf("save %s: %w", s.noun, err)
	}
	return s.inner.Save(ctx, copied)
}

func (s *leaseStore[R]) delete(ctx context.Context, id string) error {
	return s.inner.Delete(ctx, id)
}

func (s *leaseStore[R]) acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	return s.inner.Acquire(ctx, resourceID, taskID, cap, ttl)
}

func (s *leaseStore[R]) releaseHold(ctx context.Context, resourceID, taskID string) error {
	return s.inner.ReleaseHold(ctx, resourceID, taskID)
}

func (s *leaseStore[R]) renewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	return s.inner.RenewHolds(ctx, taskID, ttl)
}

func (s *leaseStore[R]) listHolds(ctx context.Context) ([]leasing.Hold, error) {
	return s.inner.ListHolds(ctx)
}

func (s *leaseStore[R]) claimLock(ctx context.Context, resourceID, taskID string) error {
	return s.inner.ClaimLock(ctx, resourceID, taskID)
}

func (s *leaseStore[R]) releaseLock(ctx context.Context, resourceID, taskID string) error {
	return s.inner.ReleaseLock(ctx, resourceID, taskID)
}

func (s *leaseStore[R]) increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return s.inner.Increment(ctx, scope, name, delta)
}

// listGroups and saveGroup copy Refs at the boundary, since the inner store
// keeps values as given.
func (s *leaseStore[R]) listGroups(ctx context.Context) ([]leasing.Group, error) {
	listed, err := s.inner.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	for i := range listed {
		listed[i].Refs = copyMap(listed[i].Refs)
	}
	return listed, nil
}

func (s *leaseStore[R]) saveGroup(ctx context.Context, g leasing.Group) error {
	g.Refs = copyMap(g.Refs)
	g.CreatedAt, g.UpdatedAt = storeTime(g.CreatedAt), storeTime(g.UpdatedAt)
	return s.inner.SaveGroup(ctx, g)
}

func (s *leaseStore[R]) deleteGroup(ctx context.Context, id string) error {
	return s.inner.DeleteGroup(ctx, id)
}
