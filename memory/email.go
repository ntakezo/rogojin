package memory

import (
	"context"
	"sync"

	"github.com/ntakezo/rogojin/email"
)

// Emails is the email.Repository: one record per email, inbox credentials in
// the clear in process memory. An inbox without a vendor stores as no inbox
// at all, exactly as the vendor column's empty marker reads back.
type Emails struct {
	mu      sync.Mutex
	records map[string]email.Email
}

var _ email.Repository = (*Emails)(nil)

// NewEmails builds an empty in-memory email store.
func NewEmails() email.Repository {
	return &Emails{records: make(map[string]email.Email)}
}

// List returns every stored email in stable id order, so the manager's
// inventory order is deterministic.
func (s *Emails) List(ctx context.Context) ([]email.Email, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	listed := make([]email.Email, 0, len(s.records))
	for _, id := range sortedIDs(s.records) {
		listed = append(listed, copyEmail(s.records[id]))
	}
	return listed, nil
}

// Save upserts the email's record: address, inbox credentials, cursor, and
// UpdatedAt. CreatedAt is written on insert and never overwritten, so a
// cursor advance cannot revise it.
func (s *Emails) Save(ctx context.Context, e email.Email) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e = copyEmail(e)
	e.CreatedAt, e.UpdatedAt = storeTime(e.CreatedAt), storeTime(e.UpdatedAt)
	if kept, ok := s.records[e.ID]; ok {
		e.CreatedAt = kept.CreatedAt
	}
	s.records[e.ID] = e
	return nil
}

// Delete removes the email's record; absent ids are a no-op.
func (s *Emails) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	return nil
}

// copyEmail clones the record, normalizing a vendor-less inbox to none the
// way the columns round-trip it.
func copyEmail(e email.Email) email.Email {
	if e.Inbox == nil || e.Inbox.Vendor == "" {
		e.Inbox = nil
		return e
	}
	in := *e.Inbox
	e.Inbox = &in
	return e
}
