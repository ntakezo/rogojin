package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/storetest"
)

// satisfiesRepositoryPort fails to compile if Emails drifts from the persistence port it exists to implement.
var _ email.Repository = (*Emails)(nil)

// newTestEmails opens the email store on a fresh temp-file database.
func newTestEmails(t *testing.T) email.Repository {
	t.Helper()
	repo, err := NewEmails(openTestDB(t))
	if err != nil {
		t.Fatalf("NewEmails: %v", err)
	}
	return repo
}

// TestEmailsContract runs the shared store contract against the sqlite email
// store.
func TestEmailsContract(t *testing.T) {
	storetest.Emails(t, newTestEmails)
}

// TestEmailsSharesADatabaseWithOtherStores verifies the email store advances
// its own ledger history in a file another store already claimed.
func TestEmailsSharesADatabaseWithOtherStores(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "shared.db")
	db := openAt(t, dsn)
	if _, err := NewAccounts(db); err != nil {
		t.Fatalf("accounts store: %v", err)
	}
	repo, err := NewEmails(db)
	if err != nil {
		t.Fatalf("email store on the same database: %v", err)
	}

	if err := repo.Save(context.Background(), email.Email{ID: "e1", Address: "a@example.com"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	reopened, err := NewEmails(openAt(t, dsn))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	listed, err := reopened.List(context.Background())
	if err != nil || len(listed) != 1 {
		t.Fatalf("list after reopen = %v, %v; want the saved email", listed, err)
	}
}
