package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/ntakezo/rogojin/accounts"
)

// Accounts is the accounts.Repository: one record per account, its
// workflow's own fields held verbatim, credentials in the clear in process
// memory.
type Accounts struct {
	mu      sync.Mutex
	records map[string]accounts.Account
	groups  groupStore
}

var _ accounts.Repository = (*Accounts)(nil)

// NewAccounts builds an empty in-memory accounts store.
func NewAccounts() accounts.Repository {
	return &Accounts{records: make(map[string]accounts.Account), groups: newGroupStore()}
}

// List returns every stored account in stable id order, so the manager's
// pool order is deterministic.
func (s *Accounts) List(ctx context.Context) ([]accounts.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	listed := make([]accounts.Account, 0, len(s.records))
	for _, id := range sortedIDs(s.records) {
		a := s.records[id]
		a.Fields = copyBytes(a.Fields)
		listed = append(listed, a)
	}
	return listed, nil
}

// Save upserts the account's record: group, holder policy, lock owner, the
// forwarding email, fields, and UpdatedAt. CreatedAt is written on insert
// and never overwritten, so a later lock cannot revise it.
func (s *Accounts) Save(ctx context.Context, a accounts.Account) error {
	fields, err := copyFields(a.Fields)
	if err != nil {
		return fmt.Errorf("save account %s: %w", a.ID, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a.Fields = fields
	a.CreatedAt, a.UpdatedAt = storeTime(a.CreatedAt), storeTime(a.UpdatedAt)
	if kept, ok := s.records[a.ID]; ok {
		a.CreatedAt = kept.CreatedAt
	}
	s.records[a.ID] = a
	return nil
}

// Delete removes the account's record; absent ids are a no-op.
func (s *Accounts) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	return nil
}

// ListGroups returns every stored account group in stable id order.
func (s *Accounts) ListGroups(ctx context.Context) ([]accounts.Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groups.list(), nil
}

// SaveGroup upserts the group's record; CreatedAt is written on insert and
// never overwritten.
func (s *Accounts) SaveGroup(ctx context.Context, g accounts.Group) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups.save(g)
	return nil
}

// DeleteGroup removes the group's record; absent ids are a no-op. Member
// accounts are the manager's to delete — the store cascades nothing.
func (s *Accounts) DeleteGroup(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups.delete(id)
	return nil
}
