package memory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/email"
)

// Emails is the email.Repository: one record per email, inbox credentials in
// the clear in process memory. An inbox without a vendor stores as no inbox
// at all, exactly as the vendor column's empty marker reads back. Listener
// claims live beside the records, not in them, so Save can neither read nor
// clobber a claim; expiry compares against this process's clock — the store
// clock, per the port's contract.
type Emails struct {
	mu      sync.Mutex
	records map[string]email.Email
	claims  map[string]listenerClaim
}

// listenerClaim is one inbox's listener ownership row.
type listenerClaim struct {
	node      string
	expiresAt time.Time
}

var _ email.Repository = (*Emails)(nil)

// NewEmails builds an empty in-memory email store.
func NewEmails() email.Repository {
	return &Emails{
		records: make(map[string]email.Email),
		claims:  make(map[string]listenerClaim),
	}
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

// Delete removes the email's record and its listener claim; absent ids are
// a no-op.
func (s *Emails) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	delete(s.claims, id)
	return nil
}

// ClaimListener takes the inbox's listener claim iff it is unheld, already
// node's, or expired. Claim bookkeeping never touches UpdatedAt.
func (s *Emails) ClaimListener(ctx context.Context, emailID, node string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[emailID]; !ok {
		return fmt.Errorf("claim listener of email %s: %w", emailID, email.ErrEmailNotFound)
	}
	if c, held := s.claims[emailID]; held && c.node != node && time.Now().Before(c.expiresAt) {
		return fmt.Errorf("claim listener of email %s: %w", emailID, email.ErrListenerHeld)
	}
	s.claims[emailID] = listenerClaim{node: node, expiresAt: time.Now().Add(ttl)}
	return nil
}

// RenewListener extends the claim iff node holds it, expired or not — a late
// but unusurped renewal wins.
func (s *Emails) RenewListener(ctx context.Context, emailID, node string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[emailID]; !ok {
		return fmt.Errorf("renew listener of email %s: %w", emailID, email.ErrEmailNotFound)
	}
	if c, held := s.claims[emailID]; !held || c.node != node {
		return fmt.Errorf("renew listener of email %s: %w", emailID, email.ErrListenerHeld)
	}
	s.claims[emailID] = listenerClaim{node: node, expiresAt: time.Now().Add(ttl)}
	return nil
}

// ReleaseListener clears the claim iff node holds it; a release after being
// usurped is a silent no-op.
func (s *Emails) ReleaseListener(ctx context.Context, emailID, node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[emailID]; !ok {
		return fmt.Errorf("release listener of email %s: %w", emailID, email.ErrEmailNotFound)
	}
	if c, held := s.claims[emailID]; held && c.node == node {
		delete(s.claims, emailID)
	}
	return nil
}

// AdvanceCursor moves the cursor under the claim holder's hand only, and
// only forward: same validity with a higher UID, or a changed validity — the
// reset. A same-validity move that is not forward is a silent no-op and
// leaves UpdatedAt alone, exactly as the refused conditional UPDATE does.
func (s *Emails) AdvanceCursor(ctx context.Context, emailID, node string, uidValidity, lastUID uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.records[emailID]
	if !ok {
		return fmt.Errorf("advance cursor of email %s: %w", emailID, email.ErrEmailNotFound)
	}
	if c, held := s.claims[emailID]; !held || c.node != node {
		return fmt.Errorf("advance cursor of email %s: %w", emailID, email.ErrListenerHeld)
	}
	if e.Inbox == nil {
		return nil
	}
	if uidValidity == e.Inbox.UIDValidity && lastUID <= e.Inbox.LastUID {
		return nil
	}
	inbox := *e.Inbox
	inbox.UIDValidity, inbox.LastUID = uidValidity, lastUID
	e.Inbox = &inbox
	e.UpdatedAt = time.Now().UTC()
	s.records[emailID] = e
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
