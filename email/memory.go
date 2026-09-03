package email

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// memoryRepository is the Repository a nil one is swapped for, and the
// reference in-process store: maps behind one mutex, with the same
// coordination semantics the durable adapters implement — upserts that
// preserve CreatedAt, TTL'd listener claims measured against this process's
// clock, forward-only cursor advances under the claim holder's hand. Records
// are stored and returned by value as given; callers wanting a serialization
// boundary (deep copies, normalization) wrap it, as the memory package's
// store does.
type memoryRepository struct {
	mu      sync.Mutex
	records map[string]Email
	claims  map[string]listenerClaim
}

// listenerClaim is one inbox's listener ownership row.
type listenerClaim struct {
	node      string
	expiresAt time.Time
}

// NewMemoryRepository returns an empty in-process Repository. It is what a
// Manager built with a nil repository runs on: claims and cursors are
// enforced for real, but nothing survives the process.
func NewMemoryRepository() Repository {
	return &memoryRepository{
		records: make(map[string]Email),
		claims:  make(map[string]listenerClaim),
	}
}

// List returns every stored email in stable id order, so the manager's
// inventory order is deterministic.
func (s *memoryRepository) List(ctx context.Context) ([]Email, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.records))
	for id := range s.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	listed := make([]Email, 0, len(ids))
	for _, id := range ids {
		listed = append(listed, s.records[id])
	}
	return listed, nil
}

// Save upserts the email's record: address, inbox credentials, cursor, and
// UpdatedAt. CreatedAt is written on insert and never overwritten, so a
// cursor advance cannot revise it.
func (s *memoryRepository) Save(ctx context.Context, e Email) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if kept, ok := s.records[e.ID]; ok {
		e.CreatedAt = kept.CreatedAt
	}
	s.records[e.ID] = e
	return nil
}

// Delete removes the email's record and its listener claim; absent ids are
// a no-op.
func (s *memoryRepository) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	delete(s.claims, id)
	return nil
}

// ClaimListener takes the inbox's listener claim iff it is unheld, already
// node's, or expired. Claim bookkeeping never touches UpdatedAt.
func (s *memoryRepository) ClaimListener(ctx context.Context, emailID, node string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[emailID]; !ok {
		return fmt.Errorf("claim listener of email %s: %w", emailID, ErrEmailNotFound)
	}
	if c, held := s.claims[emailID]; held && c.node != node && time.Now().Before(c.expiresAt) {
		return fmt.Errorf("claim listener of email %s: %w", emailID, ErrListenerHeld)
	}
	s.claims[emailID] = listenerClaim{node: node, expiresAt: time.Now().Add(ttl)}
	return nil
}

// RenewListener extends the claim iff node holds it, expired or not — a late
// but unusurped renewal wins.
func (s *memoryRepository) RenewListener(ctx context.Context, emailID, node string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[emailID]; !ok {
		return fmt.Errorf("renew listener of email %s: %w", emailID, ErrEmailNotFound)
	}
	if c, held := s.claims[emailID]; !held || c.node != node {
		return fmt.Errorf("renew listener of email %s: %w", emailID, ErrListenerHeld)
	}
	s.claims[emailID] = listenerClaim{node: node, expiresAt: time.Now().Add(ttl)}
	return nil
}

// ReleaseListener clears the claim iff node holds it; a release after being
// usurped is a silent no-op.
func (s *memoryRepository) ReleaseListener(ctx context.Context, emailID, node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.records[emailID]; !ok {
		return fmt.Errorf("release listener of email %s: %w", emailID, ErrEmailNotFound)
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
// The inbox is replaced, not mutated, so a caller still holding the record
// it saved never sees the store's write.
func (s *memoryRepository) AdvanceCursor(ctx context.Context, emailID, node string, uidValidity, lastUID uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.records[emailID]
	if !ok {
		return fmt.Errorf("advance cursor of email %s: %w", emailID, ErrEmailNotFound)
	}
	if c, held := s.claims[emailID]; !held || c.node != node {
		return fmt.Errorf("advance cursor of email %s: %w", emailID, ErrListenerHeld)
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
