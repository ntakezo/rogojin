// Package storetest is the contract every adapter must honor, written as
// runnable suites. Each exported function exercises one port —
// tasks.Repository, leasing.Repository, email.Repository, comms.Bus,
// comms.Notifier — against whatever implementation the factory it is handed
// opens, so the same assertions vet the memory adapter, the sqlite adapter,
// and any store a third party writes.
//
// A behavioral promise lives here or it does not exist: the adapters' own
// test files hold only what is genuinely theirs (file reopening, shared-file
// ledgers, tuning options). When a port grows, its suite grows first, and an
// adapter conforms when its factory passes.
//
// Factories are called once per subtest and clean up via t.Cleanup, so every
// case starts on a fresh store.
package storetest

import (
	"encoding/json"
	"testing"
	"time"
)

// mustJSON marshals v or fails the test.
func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// recv pulls one value within a deadline, failing if none arrives — the
// expectation that delivery is prompt, not eventually-maybe.
func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("channel closed, expected a value")
		}
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a value")
	}
	var zero T
	return zero
}

// assertEmpty fails if a value arrives within a short window, proving a
// message was not delivered (drop, late subscribe).
func assertEmpty[T any](t *testing.T, ch <-chan T) {
	t.Helper()
	select {
	case v, ok := <-ch:
		if ok {
			t.Fatalf("expected no value, got %v", v)
		}
	case <-time.After(50 * time.Millisecond):
	}
}
