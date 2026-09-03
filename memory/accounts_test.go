package memory

import (
	"testing"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/storetest"
)

// TestAccountsContract runs the shared store contract against the in-memory
// accounts store.
func TestAccountsContract(t *testing.T) {
	storetest.Accounts(t, func(t *testing.T) accounts.Repository { return NewAccounts() })
}
