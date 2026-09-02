package comms

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// waitAsync runs Wait in a goroutine and reports its result on a channel, so
// a test can assert both that a wait finished and that it hadn't yet.
func waitAsync(n Notifier, ctx context.Context, topic string, timeout time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() { done <- n.Wait(ctx, topic, timeout) }()
	return done
}

// mustFinish asserts the wait completes promptly with the wanted error.
func mustFinish(t *testing.T, done <-chan error, want error) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("Wait = %v, want %v", err, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not finish")
	}
}

// mustBlock asserts the wait is still parked after a grace period.
func mustBlock(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("Wait finished early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestNotifyWakesWaiter verifies the basic contract: a parked waiter returns
// nil promptly once the topic is notified.
func TestNotifyWakesWaiter(t *testing.T) {
	n := NewNotifier()
	done := waitAsync(n, context.Background(), "t", 0)
	mustBlock(t, done)

	n.Notify("t")
	mustFinish(t, done, nil)
}

// TestNotifyWakesEveryWaiter verifies notify is a broadcast, not a signal:
// every waiter parked on the topic wakes, none is left for a second call.
func TestNotifyWakesEveryWaiter(t *testing.T) {
	n := NewNotifier()
	var waits []<-chan error
	for range 5 {
		waits = append(waits, waitAsync(n, context.Background(), "t", 0))
	}
	for _, done := range waits {
		mustBlock(t, done)
	}

	n.Notify("t")
	for _, done := range waits {
		mustFinish(t, done, nil)
	}
}

// TestNotifyIsScopedToItsTopic verifies a notification on one topic leaves
// waiters on another parked.
func TestNotifyIsScopedToItsTopic(t *testing.T) {
	n := NewNotifier()
	other := waitAsync(n, context.Background(), "other", 0)

	n.Notify("t")
	mustBlock(t, other)

	n.Notify("other")
	mustFinish(t, other, nil)
}

// TestTimeoutReturnsNil verifies a timeout is a scheduled re-check, not a
// failure: Wait returns nil so the caller re-reads the store and parks again.
func TestTimeoutReturnsNil(t *testing.T) {
	n := NewNotifier()
	mustFinish(t, waitAsync(n, context.Background(), "t", 10*time.Millisecond), nil)
}

// TestContextExpiryIsTheOnlyError verifies ctx expiry surfaces as ctx.Err(),
// distinguishing "the caller gave up" from a wakeup.
func TestContextExpiryIsTheOnlyError(t *testing.T) {
	n := NewNotifier()
	ctx, cancel := context.WithCancel(context.Background())
	done := waitAsync(n, ctx, "t", 0)
	mustBlock(t, done)

	cancel()
	mustFinish(t, done, context.Canceled)
}

// TestNotifyBeforeWaitIsLost verifies notifications are hints for current
// waiters only — one sent before Wait began is not queued. The waiter's own
// timeout is what covers that race, so this is the contract, not a bug.
func TestNotifyBeforeWaitIsLost(t *testing.T) {
	n := NewNotifier()
	n.Notify("t")

	done := waitAsync(n, context.Background(), "t", 200*time.Millisecond)
	mustBlock(t, done)       // the earlier notify did not carry over,
	mustFinish(t, done, nil) // and the timeout is what finishes the wait
}

// TestNotifierIsReusable verifies the broadcast re-arms: a waiter can loop
// wait-notify-wait indefinitely, each round waking on its own notify.
func TestNotifierIsReusable(t *testing.T) {
	n := NewNotifier()
	for range 3 {
		done := waitAsync(n, context.Background(), "t", 0)
		mustBlock(t, done)
		n.Notify("t")
		mustFinish(t, done, nil)
	}
}

// TestNotifierHoldsNoRetiredTopics verifies departed waiters leave nothing
// behind: with per-task topic names, an entry per topic ever waited on would
// grow without bound.
func TestNotifierHoldsNoRetiredTopics(t *testing.T) {
	n := NewNotifier()
	inner := n.(*notifier)

	// A waiter that timed out cleans up its own entry.
	mustFinish(t, waitAsync(n, context.Background(), "timed-out", 10*time.Millisecond), nil)
	// A waiter woken by notify does too.
	done := waitAsync(n, context.Background(), "notified", 0)
	mustBlock(t, done)
	n.Notify("notified")
	mustFinish(t, done, nil)

	deadline := time.Now().Add(2 * time.Second)
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

// TestConcurrentNotifyAndWait hammers the notifier from both sides so the
// race detector can vet the locking; every wait must finish.
func TestConcurrentNotifyAndWait(t *testing.T) {
	n := NewNotifier()
	ctx := context.Background()

	stop := make(chan struct{})
	var notifying sync.WaitGroup
	notifying.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				n.Notify("t")
			}
		}
	})

	var waiting sync.WaitGroup
	for range 8 {
		waiting.Go(func() {
			for range 50 {
				if err := n.Wait(ctx, "t", time.Millisecond); err != nil {
					t.Errorf("Wait: %v", err)
					return
				}
			}
		})
	}
	waiting.Wait()
	close(stop)
	notifying.Wait()
}
