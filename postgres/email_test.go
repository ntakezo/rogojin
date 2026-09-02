package postgres

import (
	"testing"

	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/storetest"
)

// satisfiesRepositoryPort fails to compile if Emails drifts from the persistence port it exists to implement.
var _ email.Repository = (*Emails)(nil)

// newTestEmails opens the email store on a fresh schema.
func newTestEmails(t *testing.T) email.Repository {
	t.Helper()
	repo, err := NewEmails(openTestDB(t))
	if err != nil {
		t.Fatalf("NewEmails: %v", err)
	}
	return repo
}

// TestEmailsContract runs the shared store contract against the postgres
// email store.
func TestEmailsContract(t *testing.T) {
	storetest.Emails(t, newTestEmails)
}
