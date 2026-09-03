package storetest

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/comms"
)

// samePayload reports whether a delivered payload matches want in either of
// the port's documented delivery forms: the value itself from an in-process
// bus, or its JSON encoding from a transport that crossed a process boundary.
func samePayload(got, want any) bool {
	if got == want {
		return true
	}
	raw, ok := got.(json.RawMessage)
	if !ok {
		return false
	}
	encoded, err := json.Marshal(want)
	return err == nil && bytes.Equal(bytes.TrimSpace(raw), encoded)
}

// Bus exercises the comms.Bus contract: fan-out to every subscriber current
// at publish time, per-subscriber publish order, at-most-once delivery that
// drops rather than blocks, and subscriptions that are live feeds, not
// replays.
func Bus(t *testing.T, open func(t *testing.T) comms.Bus) {
	ctx := context.Background()

	// Each current subscriber sees every later publish.
	t.Run("FanOut", func(t *testing.T) {
		b := open(t)
		s1, _ := b.Subscribe(ctx, "topic")
		s2, _ := b.Subscribe(ctx, "topic")
		defer s1.Close()
		defer s2.Close()

		b.Publish(ctx, "topic", "hello")

		if got := recv(t, s1.C()); !samePayload(got, "hello") {
			t.Errorf("s1 got %v, want hello", got)
		}
		if got := recv(t, s2.C()); !samePayload(got, "hello") {
			t.Errorf("s2 got %v, want hello", got)
		}
	})

	// Publishing into the void is a harmless no-op, not an error — the
	// decoupling pub/sub exists to provide.
	t.Run("PublishNoSubscribers", func(t *testing.T) {
		b := open(t)
		if err := b.Publish(ctx, "topic", "into-the-void"); err != nil {
			t.Fatalf("publish to empty topic errored: %v", err)
		}
	})

	// A subscriber observes payloads in publish order.
	t.Run("Ordering", func(t *testing.T) {
		b := open(t)
		s, _ := b.Subscribe(ctx, "topic")
		defer s.Close()

		for i := 0; i < 5; i++ {
			b.Publish(ctx, "topic", i)
		}
		for i := 0; i < 5; i++ {
			if got := recv(t, s.C()); !samePayload(got, i) {
				t.Errorf("position %d got %v, want %d", i, got, i)
			}
		}
	})

	// Close means unsubscribe: no further delivery, channel closed, and
	// calling it again is safe.
	t.Run("CloseUnsubscribes", func(t *testing.T) {
		b := open(t)
		s, _ := b.Subscribe(ctx, "topic")
		s.Close()
		s.Close()

		b.Publish(ctx, "topic", "after-close")

		if _, ok := <-s.C(); ok {
			t.Fatal("received on a closed subscription")
		}
	})

	// A subscriber that never reads must not stall the publisher; excess
	// payloads are dropped for the slow one (at-most-once).
	t.Run("SlowSubscriberDropsNotBlocks", func(t *testing.T) {
		b := open(t)
		slow, _ := b.Subscribe(ctx, "topic")
		defer slow.Close()

		done := make(chan struct{})
		go func() {
			for i := 0; i < 4096; i++ {
				b.Publish(ctx, "topic", i)
			}
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("publisher blocked on a slow subscriber")
		}
	})

	// Subscribing after a publish must not retroactively deliver it —
	// subscriptions are live, not replayed.
	t.Run("LateSubscriberMissesEarlier", func(t *testing.T) {
		b := open(t)
		b.Publish(ctx, "topic", "early")

		s, _ := b.Subscribe(ctx, "topic")
		defer s.Close()

		assertEmpty(t, s.C())
	})
}
