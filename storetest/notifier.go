package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/comms"
)

// notifyWait runs Wait in a goroutine and reports its result on a channel,
// so a case can assert both that a wait finished and that it hadn't yet.
func notifyWait(n comms.Notifier, ctx context.Context, topic string, timeout time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() { done <- n.Wait(ctx, topic, timeout) }()
	return done
}

// mustFinishWait asserts the wait completes promptly with the wanted error.
func mustFinishWait(t *testing.T, done <-chan error, want error) {
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

// mustStillWait asserts the wait is still parked after a grace period.
func mustStillWait(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("Wait finished early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

// Notifier exercises the comms.Notifier contract: notifications are
// broadcast wakeup hints for current waiters, a timeout is a scheduled
// re-check rather than a failure, and only context expiry is an error.
func Notifier(t *testing.T, open func(t *testing.T) comms.Notifier) {
	ctx := context.Background()

	// A parked waiter returns nil promptly once the topic is notified.
	t.Run("NotifyWakesWaiter", func(t *testing.T) {
		n := open(t)
		done := notifyWait(n, ctx, "t", 0)
		mustStillWait(t, done)

		n.Notify("t")
		mustFinishWait(t, done, nil)
	})

	// Notify is a broadcast, not a signal: every waiter parked on the
	// topic wakes.
	t.Run("NotifyWakesEveryWaiter", func(t *testing.T) {
		n := open(t)
		var waits []<-chan error
		for range 5 {
			waits = append(waits, notifyWait(n, ctx, "t", 0))
		}
		for _, done := range waits {
			mustStillWait(t, done)
		}

		n.Notify("t")
		for _, done := range waits {
			mustFinishWait(t, done, nil)
		}
	})

	// A notification on one topic leaves waiters on another parked.
	t.Run("NotifyIsScopedToItsTopic", func(t *testing.T) {
		n := open(t)
		other := notifyWait(n, ctx, "other", 0)

		n.Notify("t")
		mustStillWait(t, other)

		n.Notify("other")
		mustFinishWait(t, other, nil)
	})

	// A timeout is a scheduled re-check, not a failure: Wait returns nil
	// so the caller re-reads the store and parks again.
	t.Run("TimeoutReturnsNil", func(t *testing.T) {
		n := open(t)
		mustFinishWait(t, notifyWait(n, ctx, "t", 10*time.Millisecond), nil)
	})

	// Context expiry surfaces as ctx.Err(), distinguishing "the caller
	// gave up" from a wakeup.
	t.Run("ContextExpiryIsTheOnlyError", func(t *testing.T) {
		n := open(t)
		waitCtx, cancel := context.WithCancel(ctx)
		done := notifyWait(n, waitCtx, "t", 0)
		mustStillWait(t, done)

		cancel()
		mustFinishWait(t, done, context.Canceled)
	})

	// Notifications are hints for current waiters only — one sent before
	// Wait began is not queued. The waiter's own timeout covers that
	// race, so this is the contract, not a defect.
	t.Run("NotifyBeforeWaitIsLost", func(t *testing.T) {
		n := open(t)
		n.Notify("t")

		done := notifyWait(n, ctx, "t", 200*time.Millisecond)
		mustStillWait(t, done)
		mustFinishWait(t, done, nil)
	})

	// The broadcast re-arms: a waiter can loop wait-notify-wait
	// indefinitely, each round waking on its own notify.
	t.Run("Reusable", func(t *testing.T) {
		n := open(t)
		for range 3 {
			done := notifyWait(n, ctx, "t", 0)
			mustStillWait(t, done)
			n.Notify("t")
			mustFinishWait(t, done, nil)
		}
	})

	// Hammer the notifier from both sides so the race detector can vet
	// the locking; every wait must finish.
	t.Run("ConcurrentNotifyAndWait", func(t *testing.T) {
		n := open(t)
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
	})
}
