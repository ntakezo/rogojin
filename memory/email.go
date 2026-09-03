package memory

import (
	"context"
	"time"

	"github.com/ntakezo/rogojin/email"
)

// Emails is the email.Repository: one record per email, inbox credentials in
// the clear in process memory. The coordination semantics — CreatedAt
// preservation, listener claims, forward-only cursors — live in
// email.NewMemoryRepository, the same store a nil-repository manager runs
// on; this wrapper adds what a durable adapter's encoding gives for free: a
// serialization boundary. An inbox without a vendor stores as no inbox at
// all, exactly as the vendor column's empty marker reads back, and records
// are deep-copied both ways, so a caller mutating what it saved or listed
// never reaches the store's copy.
type Emails struct {
	inner email.Repository
}

var _ email.Repository = (*Emails)(nil)

// NewEmails builds an empty in-memory email store.
func NewEmails() email.Repository {
	return &Emails{inner: email.NewMemoryRepository()}
}

// List returns every stored email in stable id order, deep-copied.
func (s *Emails) List(ctx context.Context) ([]email.Email, error) {
	listed, err := s.inner.List(ctx)
	if err != nil {
		return nil, err
	}
	for i := range listed {
		listed[i] = copyEmail(listed[i])
	}
	return listed, nil
}

// Save deep-copies and normalizes on the way in — vendor-less inboxes to
// none, timestamps the way the text column round-trips them — then hands the
// upsert to the inner store.
func (s *Emails) Save(ctx context.Context, e email.Email) error {
	e = copyEmail(e)
	e.CreatedAt, e.UpdatedAt = storeTime(e.CreatedAt), storeTime(e.UpdatedAt)
	return s.inner.Save(ctx, e)
}

// Delete removes the email's record and its listener claim; absent ids are
// a no-op.
func (s *Emails) Delete(ctx context.Context, id string) error {
	return s.inner.Delete(ctx, id)
}

// ClaimListener takes the inbox's listener claim — see email.Repository.
func (s *Emails) ClaimListener(ctx context.Context, emailID, node string, ttl time.Duration) error {
	return s.inner.ClaimListener(ctx, emailID, node, ttl)
}

// RenewListener extends the claim iff node holds it.
func (s *Emails) RenewListener(ctx context.Context, emailID, node string, ttl time.Duration) error {
	return s.inner.RenewListener(ctx, emailID, node, ttl)
}

// ReleaseListener clears the claim iff node holds it.
func (s *Emails) ReleaseListener(ctx context.Context, emailID, node string) error {
	return s.inner.ReleaseListener(ctx, emailID, node)
}

// AdvanceCursor moves the cursor under the claim holder's hand only — see
// email.Repository.
func (s *Emails) AdvanceCursor(ctx context.Context, emailID, node string, uidValidity, lastUID uint32) error {
	return s.inner.AdvanceCursor(ctx, emailID, node, uidValidity, lastUID)
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
