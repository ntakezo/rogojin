package memory

import (
	"testing"

	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/storetest"
)

// TestEmailsContract runs the shared store contract against the in-memory
// email store.
func TestEmailsContract(t *testing.T) {
	storetest.Emails(t, func(t *testing.T) email.Repository { return NewEmails() })
}
