package redis

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ntakezo/rogojin/comms"
	goredis "github.com/redis/go-redis/v9"
)

// pair returns two independent clients on one miniredis — two nodes sharing
// one Redis, the shape the in-memory bus cannot take.
func pair(t *testing.T) (*goredis.Client, *goredis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	a := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	b := goredis.NewClient(&goredis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

// recv pulls one value within a deadline, failing if none arrives.
func recv(t *testing.T, ch <-chan any) any {
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
	return nil
}

// TestCrossInstanceDelivery is the point of the package: a payload published
// through one Bus arrives at a subscriber on another, which no in-process
// bus can do.
func TestCrossInstanceDelivery(t *testing.T) {
	ca, cb := pair(t)
	ctx := context.Background()

	a := NewBus(ca)
	b := NewBus(cb)
	defer a.Close()
	defer b.Close()

	sub, err := b.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	if err := a.Publish(ctx, "topic", "hello"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	raw, ok := recv(t, sub.C()).(json.RawMessage)
	if !ok || string(raw) != `"hello"` {
		t.Fatalf("got %v, want the wire form of the published payload", raw)
	}
}

// TestTypedTopicAcrossInstances proves the invariant the typed layer exists
// for: the example's queue-cookie code — Emit on one node, receive and
// assert back to string on another — runs unchanged over the wire.
func TestTypedTopicAcrossInstances(t *testing.T) {
	ca, cb := pair(t)
	ctx := context.Background()

	a := NewBus(ca)
	b := NewBus(cb)
	defer a.Close()
	defer b.Close()

	// The receiving side, exactly as _examples' wait_in_queue writes it.
	topicB := comms.NewTopic[string](b, "queue-cookie")
	sub, err := topicB.On(ctx)
	if err != nil {
		t.Fatalf("on: %v", err)
	}
	defer sub.Close()

	topicA := comms.NewTopic[string](a, "queue-cookie")
	if err := topicA.Emit(ctx, "cookie-123"); err != nil {
		t.Fatalf("emit: %v", err)
	}

	cookie := recv(t, sub.C())
	if got, ok := cookie.(string); !ok || got != "cookie-123" {
		t.Fatalf("got %v (%T), want the emitted string back", cookie, cookie)
	}
}

// TestNotifierCrossInstanceWake verifies a notify on one node wakes a waiter
// parked on another.
func TestNotifierCrossInstanceWake(t *testing.T) {
	ca, cb := pair(t)

	a := NewNotifier(ca)
	b := NewNotifier(cb)
	defer a.Close()
	defer b.Close()

	done := make(chan error, 1)
	go func() { done <- b.Wait(context.Background(), "capacity", 0) }()

	// Give the waiter a moment to park (its subscribe is confirmed before it
	// parks, so this sleep is about the goroutine, not the transport).
	time.Sleep(20 * time.Millisecond)
	a.Notify("capacity")

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Wait: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cross-instance notify did not wake the waiter")
	}
}

// TestPublishRefusesUnmarshalablePayload verifies the wire boundary is loud:
// a payload JSON cannot carry is refused at Publish, not silently mangled.
func TestPublishRefusesUnmarshalablePayload(t *testing.T) {
	b := NewBus(miniClient(t))
	defer b.Close()

	if err := b.Publish(context.Background(), "topic", make(chan int)); err == nil {
		t.Fatal("publish accepted a payload with no wire form")
	}
}

// TestPrefixIsolation verifies two deployments on one Redis never hear each
// other: same topic, different prefixes, no delivery.
func TestPrefixIsolation(t *testing.T) {
	ca, cb := pair(t)
	ctx := context.Background()

	a := NewBus(ca, WithPrefix("deploy-a:"))
	b := NewBus(cb, WithPrefix("deploy-b:"))
	defer a.Close()
	defer b.Close()

	sub, err := b.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	if err := a.Publish(ctx, "topic", "leak?"); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case v := <-sub.C():
		t.Fatalf("crossed prefixes: got %v", v)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestDropOnFullReportsLocally verifies the local delivery half of the
// contract holds over the wire: a full subscriber buffer drops and the drop
// handler hears about it.
func TestDropOnFullReportsLocally(t *testing.T) {
	var drops atomic.Int64
	b := NewBus(miniClient(t), WithBuffer(1), WithDropHandler(func(topic string, payload any) {
		drops.Add(1)
	}))
	defer b.Close()
	ctx := context.Background()

	sub, err := b.Subscribe(ctx, "topic")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	b.Publish(ctx, "topic", "kept")
	b.Publish(ctx, "topic", "lost")

	deadline := time.Now().Add(2 * time.Second)
	for drops.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no drop reported for the payload past the buffer")
		}
		time.Sleep(time.Millisecond)
	}
	if got := drops.Load(); got != 1 {
		t.Fatalf("drops = %d, want 1", got)
	}
}
