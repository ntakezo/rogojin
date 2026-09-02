package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/payments"
	"github.com/ntakezo/rogojin/storetest"
)

// satisfiesRepositoryPort fails to compile if Payments drifts from the persistence port it exists to implement.
var _ payments.Repository = (*Payments)(nil)

// newTestPayments opens the payments store on a fresh temp-file database.
func newTestPayments(t *testing.T) payments.Repository {
	t.Helper()
	repo, err := NewPayments(openTestDB(t))
	if err != nil {
		t.Fatalf("NewPayments: %v", err)
	}
	return repo
}

// TestPaymentsContract runs the shared store contract against the sqlite
// payments store.
func TestPaymentsContract(t *testing.T) {
	storetest.Payments(t, newTestPayments)
}

// TestPaymentsAndAccountsShareOneDatabase verifies two stores on one file
// each keep their own records — the shared-ledger arrangement the sqlite
// package exists to provide.
func TestPaymentsAndAccountsShareOneDatabase(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	accountStore, err := NewAccounts(db)
	if err != nil {
		t.Fatalf("open accounts: %v", err)
	}
	paymentStore, err := NewPayments(db)
	if err != nil {
		t.Fatalf("open payments on the same database: %v", err)
	}

	if err := accountStore.Save(ctx, accounts.Account{Resource: leasing.Resource{ID: "a1", GroupID: "site"}}); err != nil {
		t.Fatalf("save account: %v", err)
	}
	if err := paymentStore.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1", GroupID: "bin"}}); err != nil {
		t.Fatalf("save payment: %v", err)
	}

	listedAccounts, err := accountStore.List(ctx)
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	if len(listedAccounts) != 1 || listedAccounts[0].ID != "a1" {
		t.Fatalf("accounts = %+v, want the stored account", listedAccounts)
	}
	listedPayments, err := paymentStore.List(ctx)
	if err != nil {
		t.Fatalf("list payments: %v", err)
	}
	if len(listedPayments) != 1 || listedPayments[0].ID != "c1" {
		t.Fatalf("payments = %+v, want the stored payment", listedPayments)
	}
}

// TestPaymentsSchemaReopensCleanly verifies the migration ledger holds: a
// second open of the same file applies nothing and loses nothing.
func TestPaymentsSchemaReopensCleanly(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "payments.db")
	ctx := context.Background()

	firstDB := openAt(t, dsn)
	first, err := NewPayments(firstDB)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := first.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1"}, Fields: mustJSON(t, map[string]string{"number": "4111111111111111"})}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	secondDB := openAt(t, dsn)
	second, err := NewPayments(secondDB)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer secondDB.Close()

	listed, err := second.List(ctx)
	if err != nil {
		t.Fatalf("list after reopen: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != "c1" {
		t.Fatalf("got %+v, want the stored payment", listed)
	}
}
