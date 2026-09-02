package memory

import (
	"context"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/payments"
)

// Payments is the payments.Repository: one record per payment, its
// checkout-defined fields held verbatim. They live in the clear, in process
// memory, and vanish with it — the embedded counterpart of the sqlite store's
// plain file.
type Payments struct {
	store leaseStore[payments.Payment]
}

var _ payments.Repository = (*Payments)(nil)

// NewPayments builds an empty in-memory payments store.
func NewPayments() payments.Repository {
	return &Payments{store: newLeaseStore[payments.Payment, *payments.Payment]("payment",
		func(c payments.Payment) (payments.Payment, error) {
			fields, err := copyFields(c.Fields)
			c.Fields = fields
			return c, err
		})}
}

// List returns every stored payment in stable id order, so the manager's
// pool order is deterministic.
func (s *Payments) List(ctx context.Context) ([]payments.Payment, error) {
	return s.store.list(ctx)
}

// Save writes the payment's record conditionally on its Version — see
// leasing.Repository. CreatedAt is written on insert and never overwritten,
// so a later lock cannot revise it.
func (s *Payments) Save(ctx context.Context, c payments.Payment) (int64, error) {
	return s.store.save(ctx, c)
}

// Delete removes the payment's record and its holds; absent ids are a no-op.
func (s *Payments) Delete(ctx context.Context, id string) error {
	return s.store.delete(ctx, id)
}

// Acquire takes or re-enters a hold on the payment under cap — see
// leasing.Repository.
func (s *Payments) Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	return s.store.acquire(ctx, resourceID, taskID, cap, ttl)
}

// ReleaseHold decrements the task's hold, removing it at zero.
func (s *Payments) ReleaseHold(ctx context.Context, resourceID, taskID string) error {
	return s.store.releaseHold(ctx, resourceID, taskID)
}

// RenewHolds extends every unexpired hold the task has.
func (s *Payments) RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	return s.store.renewHolds(ctx, taskID, ttl)
}

// ListHolds returns every hold row, expired ones included.
func (s *Payments) ListHolds(ctx context.Context) ([]leasing.Hold, error) {
	return s.store.listHolds(ctx)
}

// ClaimLock binds the payment to the task iff unlocked or already its own.
func (s *Payments) ClaimLock(ctx context.Context, resourceID, taskID string) error {
	return s.store.claimLock(ctx, resourceID, taskID)
}

// ReleaseLock clears the lock iff the task owns it.
func (s *Payments) ReleaseLock(ctx context.Context, resourceID, taskID string) error {
	return s.store.releaseLock(ctx, resourceID, taskID)
}

// Increment atomically adds delta to the counter under (scope, name).
func (s *Payments) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return s.store.increment(ctx, scope, name, delta)
}

// ListGroups returns every stored payment group in stable id order.
func (s *Payments) ListGroups(ctx context.Context) ([]payments.Group, error) {
	return s.store.listGroups(ctx)
}

// SaveGroup upserts the group's record; CreatedAt is written on insert and
// never overwritten.
func (s *Payments) SaveGroup(ctx context.Context, g payments.Group) error {
	return s.store.saveGroup(ctx, g)
}

// DeleteGroup removes the group's record; absent ids are a no-op. Member
// payments are the manager's to delete — the store cascades nothing.
func (s *Payments) DeleteGroup(ctx context.Context, id string) error {
	return s.store.deleteGroup(ctx, id)
}
