// The contract suites live in storetest, which imports comms, so they are
// invoked from an external test package rather than alongside the
// implementation.
package comms_test

import (
	"testing"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/storetest"
)

// TestBusContract runs the shared transport contract against the in-memory
// bus.
func TestBusContract(t *testing.T) {
	storetest.Bus(t, func(t *testing.T) comms.Bus { return comms.NewBus() })
}

// TestNotifierContract runs the shared wakeup contract against the
// in-process notifier.
func TestNotifierContract(t *testing.T) {
	storetest.Notifier(t, func(t *testing.T) comms.Notifier { return comms.NewNotifier() })
}
