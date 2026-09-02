package comms

import (
	"context"
	"testing"
	"time"
)

// The Notifier wakeup contract is asserted by storetest.Notifier (see
// contract_test.go); this test needs the notifier's internals.

// TestNotifierHoldsNoRetiredTopics verifies departed waiters leave nothing
// behind: with per-task topic names, an entry per topic ever waited on would
// grow without bound.
func TestNotifierHoldsNoRetiredTopics(t *testing.T) {
	n := NewNotifier()
	inner := n.(*notifier)
	ctx := context.Background()

	// A waiter that timed out cleans up its own entry.
	if err := n.Wait(ctx, "timed-out", 10*time.Millisecond); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	// A waiter woken by notify does too.
	done := make(chan error, 1)
	go func() { done <- n.Wait(ctx, "notified", 0) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		inner.mu.Lock()
		parked := len(inner.topics) > 0
		inner.mu.Unlock()
		if parked {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter never parked")
		}
		time.Sleep(time.Millisecond)
	}
	n.Notify("notified")
	if err := <-done; err != nil {
		t.Fatalf("Wait: %v", err)
	}

	for {
		inner.mu.Lock()
		left := len(inner.topics)
		inner.mu.Unlock()
		if left == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("topics = %d entries, want none once every waiter left", left)
		}
		time.Sleep(time.Millisecond)
	}
}
