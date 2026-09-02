package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/ntakezo/rogojin/payments"
)

// Payments is the payments.Repository: one record per payment, its
// checkout-defined fields held verbatim. They live in the clear, in process
// memory, and vanish with it — the embedded counterpart of the sqlite store's
// plain file.
type Payments struct {
	mu      sync.Mutex
	records map[string]payments.Payment
	groups  groupStore
}

var _ payments.Repository = (*Payments)(nil)

// NewPayments builds an empty in-memory payments store.
func NewPayments() payments.Repository {
	return &Payments{records: make(map[string]payments.Payment), groups: newGroupStore()}
}

// List returns every stored payment in stable id order, so the manager's
// pool order is deterministic.
func (s *Payments) List(ctx context.Context) ([]payments.Payment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	listed := make([]payments.Payment, 0, len(s.records))
	for _, id := range sortedIDs(s.records) {
		c := s.records[id]
		c.Fields = copyBytes(c.Fields)
		listed = append(listed, c)
	}
	return listed, nil
}

// Save upserts the payment's record: group, holder policy, lock owner,
// fields, and UpdatedAt. CreatedAt is written on insert and never
// overwritten, so a later lock cannot revise it.
func (s *Payments) Save(ctx context.Context, c payments.Payment) error {
	fields, err := copyFields(c.Fields)
	if err != nil {
		return fmt.Errorf("save payment %s: %w", c.ID, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c.Fields = fields
	c.CreatedAt, c.UpdatedAt = storeTime(c.CreatedAt), storeTime(c.UpdatedAt)
	if kept, ok := s.records[c.ID]; ok {
		c.CreatedAt = kept.CreatedAt
	}
	s.records[c.ID] = c
	return nil
}

// Delete removes the payment's record; absent ids are a no-op.
func (s *Payments) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	return nil
}

// ListGroups returns every stored payment group in stable id order.
func (s *Payments) ListGroups(ctx context.Context) ([]payments.Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groups.list(), nil
}

// SaveGroup upserts the group's record; CreatedAt is written on insert and
// never overwritten.
func (s *Payments) SaveGroup(ctx context.Context, g payments.Group) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups.save(g)
	return nil
}

// DeleteGroup removes the group's record; absent ids are a no-op. Member
// payments are the manager's to delete — the store cascades nothing.
func (s *Payments) DeleteGroup(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups.delete(id)
	return nil
}
