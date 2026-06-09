package comms

import (
	"context"
	"testing"
	"time"
)

// recv pulls one message within a deadline, failing if none arrives. Encodes the
// expectation that delivery is prompt, not eventually-maybe.
func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v, ok := <-ch:
		if !ok {
			t.Fatal("channel closed, expected a value")
		}
		return v
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for a value")
	}
	var zero T
	return zero
}

// assertEmpty fails if a value arrives within a short window. Used to prove a
// message was NOT delivered (drop, late-subscribe, type mismatch).
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

// Fan-out is the core promise: each current subscriber sees every later publish.
func TestBusFanOut(t *testing.T) {
	b := NewBus()
	ctx := context.Background()

	s1, _ := b.Subscribe(ctx, "topic")
	s2, _ := b.Subscribe(ctx, "topic")
	defer s1.Close()
	defer s2.Close()

	b.Publish(ctx, "topic", "hello")

	if got := recv(t, s1.C()); got != "hello" {
		t.Errorf("s1 got %v, want hello", got)
	}
	if got := recv(t, s2.C()); got != "hello" {
		t.Errorf("s2 got %v, want hello", got)
	}
}

// Publishing into the void must be a harmless no-op, not an error — that is the
// decoupling pub/sub exists to provide.
func TestPublishNoSubscribers(t *testing.T) {
	b := NewBus()
	if err := b.Publish(context.Background(), "topic", "into-the-void"); err != nil {
		t.Fatalf("publish to empty topic errored: %v", err)
	}
}

// A subscriber must observe messages in the order they were published, so
// downstream coordination logic can rely on sequence.
func TestBusOrdering(t *testing.T) {
	b := NewBus()
	ctx := context.Background()

	s, _ := b.Subscribe(ctx, "topic")
	defer s.Close()

	for i := 0; i < 5; i++ {
		b.Publish(ctx, "topic", i)
	}
	for i := 0; i < 5; i++ {
		if got := recv(t, s.C()); got != i {
			t.Errorf("position %d got %v, want %d", i, got, i)
		}
	}
}

// Close means unsubscribe: no further delivery, channel closed, and calling it
// again is safe. A leaked subscription is a memory leak on the one box we run on.
func TestCloseUnsubscribes(t *testing.T) {
	b := NewBus()
	ctx := context.Background()

	s, _ := b.Subscribe(ctx, "topic")
	s.Close()
	s.Close() // idempotent

	b.Publish(ctx, "topic", "after-close")

	if _, ok := <-s.C(); ok {
		t.Fatal("received on a closed subscription")
	}
}

// A subscriber that never reads must not stall the publisher or other
// subscribers; excess messages are dropped for the slow one (at-most-once).
func TestSlowSubscriberDropsNotBlocks(t *testing.T) {
	b := NewBus()
	ctx := context.Background()

	slow, _ := b.Subscribe(ctx, "topic")
	defer slow.Close()

	done := make(chan struct{})
	go func() {
		for i := 0; i < defaultBuffer*4; i++ {
			b.Publish(ctx, "topic", i)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publisher blocked on a slow subscriber")
	}
}

// Subscribing after a publish must not retroactively deliver it — subscriptions
// are live, not replayed.
func TestLateSubscriberMissesEarlier(t *testing.T) {
	b := NewBus()
	ctx := context.Background()

	b.Publish(ctx, "topic", "early")

	s, _ := b.Subscribe(ctx, "topic")
	defer s.Close()

	assertEmpty(t, s.C())
}

// The typed layer is the ergonomics modules actually use: a value emitted
// through a Topic arrives on its feed asserting back to the emitted type.
func TestTopicRoundTrip(t *testing.T) {
	b := NewBus()
	ctx := context.Background()

	type result struct{ URL string }
	topic := NewTopic[result](b, "results")

	sub, _ := topic.On(ctx)
	defer sub.Close()

	if err := topic.Emit(ctx, result{URL: "https://x"}); err != nil {
		t.Fatalf("emit: %v", err)
	}

	got, ok := recv(t, sub.C()).(result)
	if !ok {
		t.Fatal("payload did not assert back to the emitted type")
	}
	if got.URL != "https://x" {
		t.Errorf("got %q, want https://x", got.URL)
	}
}
