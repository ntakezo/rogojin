package memory

import (
	"context"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/proxies"
)

// Proxies is the proxies.Repository: one record per proxy, with the URL and
// the success and failure counts the bayesian strategy learns from.
type Proxies struct {
	store leaseStore[proxies.Proxy]
}

var _ proxies.Repository = (*Proxies)(nil)

// NewProxies builds an empty in-memory proxies store.
func NewProxies() proxies.Repository {
	return &Proxies{store: newLeaseStore("proxy",
		func(p *proxies.Proxy) *leasing.Resource { return &p.Resource },
		func(p proxies.Proxy) (proxies.Proxy, error) { return p, nil })}
}

// List returns every stored proxy in stable id order, so the manager's pool
// order is deterministic.
func (s *Proxies) List(ctx context.Context) ([]proxies.Proxy, error) {
	return s.store.list()
}

// Save writes the proxy's record conditionally on its Version — see
// leasing.Repository. CreatedAt is written on insert and never overwritten,
// so a lock or a stat update cannot revise it.
func (s *Proxies) Save(ctx context.Context, p proxies.Proxy) (int64, error) {
	return s.store.save(p)
}

// Delete removes the proxy's record and its holds; absent ids are a no-op.
func (s *Proxies) Delete(ctx context.Context, id string) error {
	s.store.delete(id)
	return nil
}

// Acquire takes or re-enters a hold on the proxy under cap — see
// leasing.Repository.
func (s *Proxies) Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	return s.store.acquire(resourceID, taskID, cap, ttl)
}

// ReleaseHold decrements the task's hold, removing it at zero.
func (s *Proxies) ReleaseHold(ctx context.Context, resourceID, taskID string) error {
	s.store.releaseHold(resourceID, taskID)
	return nil
}

// RenewHolds extends every unexpired hold the task has.
func (s *Proxies) RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	s.store.renewHolds(taskID, ttl)
	return nil
}

// ListHolds returns every hold row, expired ones included.
func (s *Proxies) ListHolds(ctx context.Context) ([]leasing.Hold, error) {
	return s.store.listHolds(), nil
}

// ClaimLock binds the proxy to the task iff unlocked or already its own.
func (s *Proxies) ClaimLock(ctx context.Context, resourceID, taskID string) error {
	return s.store.claimLock(resourceID, taskID)
}

// ReleaseLock clears the lock iff the task owns it.
func (s *Proxies) ReleaseLock(ctx context.Context, resourceID, taskID string) error {
	s.store.releaseLock(resourceID, taskID)
	return nil
}

// Increment atomically adds delta to the counter under (scope, name).
func (s *Proxies) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return s.store.increment(scope, name, delta), nil
}

// ListGroups returns every stored proxy group in stable id order.
func (s *Proxies) ListGroups(ctx context.Context) ([]proxies.Group, error) {
	return s.store.listGroups(), nil
}

// SaveGroup upserts the group's record; CreatedAt is written on insert and
// never overwritten.
func (s *Proxies) SaveGroup(ctx context.Context, g proxies.Group) error {
	s.store.saveGroup(g)
	return nil
}

// DeleteGroup removes the group's record; absent ids are a no-op. Member
// proxies are the manager's to delete — the store cascades nothing.
func (s *Proxies) DeleteGroup(ctx context.Context, id string) error {
	s.store.deleteGroup(id)
	return nil
}
