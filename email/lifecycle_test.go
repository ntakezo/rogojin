package email

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests cover the lifecycle edges of the listening engine: shutdown
// ordering between the manager and its subscriptions, the ownership handoff
// when one inbox's listener is replaced mid-fetch, and Close's obligation to
// cancel and collect every goroutine it spawned.

// TestManagerCloseThenSubscriptionCloseDoesNotPanic verifies the two close
// routes compose in either order. A process shutting down closes the manager
// first and unwinds task defers after; the deferred subscription Close must
// find its channel already retired and do nothing, not close it twice.
func TestManagerCloseThenSubscriptionCloseDoesNotPanic(t *testing.T) {
	server := newFakeServer()
	repo := newMemRepo()
	m := newTestManager(t, repo, server)
	addEmail(t, m, testEmail("inbox-1"))

	sub, err := m.Listen(context.Background(), "t1", "inbox-1")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	awaitListening(t, repo, "inbox-1", 1)

	if err := m.Close(); err != nil {
		t.Fatalf("manager close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("subscription close after manager close: %v", err)
	}
	if _, ok := <-sub.C(); ok {
		t.Fatalf("subscription channel still open after close")
	}
}

// gatedSession wraps a fake session so the test can park its second fetch —
// the shape of an in-flight fetch on a mailbox that ignores cancellation —
// until the test says otherwise.
type gatedSession struct {
	Mailbox
	fetches atomic.Int32
	started sync.Once
	begun   chan struct{} // closed when the gated fetch is in flight
	gate    chan struct{} // released by the test to let it finish
}

func (s *gatedSession) FetchSince(ctx context.Context, uid uint32) ([]Message, error) {
	if s.fetches.Add(1) >= 2 {
		s.started.Do(func() { close(s.begun) })
		<-s.gate
	}
	return s.Mailbox.FetchSince(ctx, uid)
}

// TestSupersededListenerLeavesTheCursorAlone verifies the handoff invariant:
// a listener replaced while a fetch was in flight delivers nothing and moves
// the cursor nowhere, so the mail it fetched into the void is fetched again
// by the inbox's current listener and delivered. Without the ownership check
// the dying listener fans out to its emptied subs set and advances the
// cursor past mail nobody received.
func TestSupersededListenerLeavesTheCursorAlone(t *testing.T) {
	server := newFakeServer()
	repo := newMemRepo()

	first := &gatedSession{begun: make(chan struct{}), gate: make(chan struct{})}
	dial2 := make(chan struct{})
	var dials atomic.Int32
	dialer := func(ctx context.Context, e Email) (Mailbox, error) {
		n := dials.Add(1)
		if n == 2 {
			<-dial2 // hold the successor back until the test is ready
		}
		mb, err := server.dial(ctx, e)
		if err != nil || n != 1 {
			return mb, err
		}
		first.Mailbox = mb
		return first, nil
	}
	m := newTestManager(t, repo, server, WithDialer(dialer))
	addEmail(t, m, testEmail("inbox-1"))

	sub1, err := m.Listen(context.Background(), "t1", "inbox-1")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	awaitListening(t, repo, "inbox-1", 1)

	// Mail lands; the first listener wakes and its fetch parks on the gate.
	server.deliver(Message{Subject: "handoff"})
	<-first.begun

	// The last subscriber hangs up mid-fetch, and a new task listens before
	// the old session has noticed: two listeners now exist for one inbox,
	// exactly one of them registered.
	sub1.Close()
	sub2, err := m.Listen(context.Background(), "t2", "inbox-1")
	if err != nil {
		t.Fatalf("re-listen: %v", err)
	}
	defer sub2.Close()

	// The old session's fetch completes into nobody's hands. It must not
	// write the cursor on its way out; its exit is what we wait on.
	close(first.gate)
	eventually(t, "the superseded session closed", func() bool { return server.closes.Load() >= 1 })

	// Only now may the successor select and fetch — from a cursor the old
	// listener left alone, so the mail arrives after all.
	close(dial2)
	if msg := recv(t, sub2); msg.Subject != "handoff" {
		t.Fatalf("successor received %q, want the mail the dying listener fetched", msg.Subject)
	}
}

// TestCloseCancelsAndWaitsForBackfills verifies Close owns every goroutine
// the manager spawned: a backfill (and a listener) parked in a dial that
// honors cancellation is cancelled and collected, so no session is dialed on
// the manager's behalf after Close returns — and Close itself returns.
func TestCloseCancelsAndWaitsForBackfills(t *testing.T) {
	repo := newMemRepo()
	var dials atomic.Int32
	dialer := func(ctx context.Context, e Email) (Mailbox, error) {
		dials.Add(1)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	m, err := NewManager(context.Background(), repo, WithDialer(dialer))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	addEmail(t, m, testEmail("inbox-1"))

	sub, err := m.Listen(context.Background(), "t1", "inbox-1", WithBackfill(time.Now().Add(-time.Hour)))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	eventually(t, "the listener and backfill dials in flight", func() bool { return dials.Load() >= 2 })

	closed := make(chan struct{})
	go func() {
		m.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatalf("Close did not cancel the parked dials and return")
	}
	if _, ok := <-sub.C(); ok {
		t.Fatalf("subscription channel still open after close")
	}
}
