package postgres

import (
	"context"
	"testing"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/payments"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/storetest"
)

// The compile-time assertions fail if a store drifts from the port it
// implements.
var (
	_ payments.Repository = (*Payments)(nil)
	_ proxies.Repository  = (*Proxies)(nil)
	_ accounts.Repository = (*Accounts)(nil)
)

func newTestPayments(t *testing.T) payments.Repository {
	t.Helper()
	repo, err := NewPayments(openTestDB(t))
	if err != nil {
		t.Fatalf("NewPayments: %v", err)
	}
	return repo
}

func newTestProxies(t *testing.T) proxies.Repository {
	t.Helper()
	repo, err := NewProxies(openTestDB(t))
	if err != nil {
		t.Fatalf("NewProxies: %v", err)
	}
	return repo
}

func newTestAccounts(t *testing.T) accounts.Repository {
	t.Helper()
	repo, err := NewAccounts(openTestDB(t))
	if err != nil {
		t.Fatalf("NewAccounts: %v", err)
	}
	return repo
}

// The three model stores each run the shared contract; everything a
// leasing-shaped store promises is asserted there.
func TestPaymentsContract(t *testing.T) { storetest.Payments(t, newTestPayments) }
func TestProxiesContract(t *testing.T)  { storetest.Proxies(t, newTestProxies) }
func TestAccountsContract(t *testing.T) { storetest.Accounts(t, newTestAccounts) }

// TestStoresShareOneDatabase verifies two stores on one schema each keep
// their own records under the shared migration ledger — the one-database
// arrangement a fleet deploys.
func TestStoresShareOneDatabase(t *testing.T) {
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

	if _, err := accountStore.Save(ctx, accounts.Account{Resource: leasing.Resource{ID: "a1", GroupID: "site"}}); err != nil {
		t.Fatalf("save account: %v", err)
	}
	if _, err := paymentStore.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1", GroupID: "bin"}}); err != nil {
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
