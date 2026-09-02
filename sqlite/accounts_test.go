package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/storetest"
)

// satisfiesRepositoryPort fails to compile if Accounts drifts from the persistence port it exists to implement.
var _ accounts.Repository = (*Accounts)(nil)

// newTestAccounts opens the accounts store on a fresh temp-file database.
func newTestAccounts(t *testing.T) accounts.Repository {
	t.Helper()
	repo, err := NewAccounts(openTestDB(t))
	if err != nil {
		t.Fatalf("NewAccounts: %v", err)
	}
	return repo
}

// TestAccountsContract runs the shared store contract against the sqlite
// accounts store.
func TestAccountsContract(t *testing.T) {
	storetest.Accounts(t, newTestAccounts)
}

// TestAccountsSchemaReopensCleanly verifies the migration ledger holds: a
// second open of the same file applies nothing and loses nothing.
func TestAccountsSchemaReopensCleanly(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "accounts.db")
	ctx := context.Background()

	firstDB := openAt(t, dsn)
	first, err := NewAccounts(firstDB)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := first.Save(ctx, accounts.Account{Resource: leasing.Resource{ID: "a1"}, Fields: mustJSON(t, map[string]string{"email": "a@b.c"})}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	secondDB := openAt(t, dsn)
	second, err := NewAccounts(secondDB)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer secondDB.Close()

	listed, err := second.List(ctx)
	if err != nil {
		t.Fatalf("list after reopen: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != "a1" {
		t.Fatalf("got %+v, want the stored account", listed)
	}
}
