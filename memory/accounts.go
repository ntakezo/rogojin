package memory

import (
	"context"
	"time"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/leasing"
)

// Accounts is the accounts.Repository: one record per account, its
// workflow's own fields held verbatim, credentials in the clear in process
// memory.
type Accounts struct {
	store leaseStore[accounts.Account]
}

var _ accounts.Repository = (*Accounts)(nil)

// NewAccounts builds an empty in-memory accounts store.
func NewAccounts() accounts.Repository {
	return &Accounts{store: newLeaseStore[accounts.Account, *accounts.Account]("account",
		func(a accounts.Account) (accounts.Account, error) {
			fields, err := copyFields(a.Fields)
			a.Fields = fields
			return a, err
		})}
}

// List returns every stored account in stable id order, so the manager's
// pool order is deterministic.
func (s *Accounts) List(ctx context.Context) ([]accounts.Account, error) {
	return s.store.list(ctx)
}

// Save writes the account's record conditionally on its Version — see
// leasing.Repository. CreatedAt is written on insert and never overwritten,
// so a later lock cannot revise it.
func (s *Accounts) Save(ctx context.Context, a accounts.Account) (int64, error) {
	return s.store.save(ctx, a)
}

// Delete removes the account's record and its holds; absent ids are a no-op.
func (s *Accounts) Delete(ctx context.Context, id string) error {
	return s.store.delete(ctx, id)
}

// Acquire takes or re-enters a hold on the account under cap — see
// leasing.Repository.
func (s *Accounts) Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	return s.store.acquire(ctx, resourceID, taskID, cap, ttl)
}

// ReleaseHold decrements the task's hold, removing it at zero.
func (s *Accounts) ReleaseHold(ctx context.Context, resourceID, taskID string) error {
	return s.store.releaseHold(ctx, resourceID, taskID)
}

// RenewHolds extends every unexpired hold the task has.
func (s *Accounts) RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	return s.store.renewHolds(ctx, taskID, ttl)
}

// ListHolds returns every hold row, expired ones included.
func (s *Accounts) ListHolds(ctx context.Context) ([]leasing.Hold, error) {
	return s.store.listHolds(ctx)
}

// ClaimLock binds the account to the task iff unlocked or already its own.
func (s *Accounts) ClaimLock(ctx context.Context, resourceID, taskID string) error {
	return s.store.claimLock(ctx, resourceID, taskID)
}

// ReleaseLock clears the lock iff the task owns it.
func (s *Accounts) ReleaseLock(ctx context.Context, resourceID, taskID string) error {
	return s.store.releaseLock(ctx, resourceID, taskID)
}

// Increment atomically adds delta to the counter under (scope, name).
func (s *Accounts) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return s.store.increment(ctx, scope, name, delta)
}

// ListGroups returns every stored account group in stable id order.
func (s *Accounts) ListGroups(ctx context.Context) ([]accounts.Group, error) {
	return s.store.listGroups(ctx)
}

// SaveGroup upserts the group's record; CreatedAt is written on insert and
// never overwritten.
func (s *Accounts) SaveGroup(ctx context.Context, g accounts.Group) error {
	return s.store.saveGroup(ctx, g)
}

// DeleteGroup removes the group's record; absent ids are a no-op. Member
// accounts are the manager's to delete — the store cascades nothing.
func (s *Accounts) DeleteGroup(ctx context.Context, id string) error {
	return s.store.deleteGroup(ctx, id)
}
