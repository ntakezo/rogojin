package memory

import (
	"testing"

	"github.com/ntakezo/rogojin/payments"
	"github.com/ntakezo/rogojin/storetest"
)

// TestPaymentsContract runs the shared store contract against the in-memory
// payments store.
func TestPaymentsContract(t *testing.T) {
	storetest.Payments(t, func(t *testing.T) payments.Repository { return NewPayments() })
}
