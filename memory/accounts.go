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
	return &Accounts{store: newLeaseStore("account",
		func(a *accounts.Account) *leasing.Resource { return &a.Resource },
		func(a accounts.Account) (accounts.Account, error) {
			fields, err := copyFields(a.Fields)
			a.Fields = fields
			return a, err
		})}
}

// List returns every stored account in stable id order, so the manager's
// pool order is deterministic.
func (s *Accounts) List(ctx context.Context) ([]accounts.Account, error) {
	return s.store.list()
}

// Save writes the account's record conditionally on its Version — see
// leasing.Repository. CreatedAt is written on insert and never overwritten,
// so a later lock cannot revise it.
func (s *Accounts) Save(ctx context.Context, a accounts.Account) (int64, error) {
	return s.store.save(a)
}

// Delete removes the account's record and its holds; absent ids are a no-op.
func (s *Accounts) Delete(ctx context.Context, id string) error {
	s.store.delete(id)
	return nil
}

// Acquire takes or re-enters a hold on the account under cap — see
// leasing.Repository.
func (s *Accounts) Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	return s.store.acquire(resourceID, taskID, cap, ttl)
}

// ReleaseHold decrements the task's hold, removing it at zero.
func (s *Accounts) ReleaseHold(ctx context.Context, resourceID, taskID string) error {
	s.store.releaseHold(resourceID, taskID)
	return nil
}

// RenewHolds extends every unexpired hold the task has.
func (s *Accounts) RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	s.store.renewHolds(taskID, ttl)
	return nil
}

// ListHolds returns every hold row, expired ones included.
func (s *Accounts) ListHolds(ctx context.Context) ([]leasing.Hold, error) {
	return s.store.listHolds(), nil
}

// ClaimLock binds the account to the task iff unlocked or already its own.
func (s *Accounts) ClaimLock(ctx context.Context, resourceID, taskID string) error {
	return s.store.claimLock(resourceID, taskID)
}

// ReleaseLock clears the lock iff the task owns it.
func (s *Accounts) ReleaseLock(ctx context.Context, resourceID, taskID string) error {
	s.store.releaseLock(resourceID, taskID)
	return nil
}

// Increment atomically adds delta to the counter under (scope, name).
func (s *Accounts) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return s.store.increment(scope, name, delta), nil
}

// ListGroups returns every stored account group in stable id order.
func (s *Accounts) ListGroups(ctx context.Context) ([]accounts.Group, error) {
	return s.store.listGroups(), nil
}

// SaveGroup upserts the group's record; CreatedAt is written on insert and
// never overwritten.
func (s *Accounts) SaveGroup(ctx context.Context, g accounts.Group) error {
	s.store.saveGroup(g)
	return nil
}

// DeleteGroup removes the group's record; absent ids are a no-op. Member
// accounts are the manager's to delete — the store cascades nothing.
func (s *Accounts) DeleteGroup(ctx context.Context, id string) error {
	s.store.deleteGroup(id)
	return nil
}
