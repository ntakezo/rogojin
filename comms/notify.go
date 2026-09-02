package comms

import (
	"context"
	"sync"
	"time"
)

// A Notifier wakes waiters across whatever boundary the deployment has:
// goroutines in one process, or nodes sharing a store. A notification is a
// wakeup hint, never a payload — a woken waiter re-reads whatever durable
// state it was waiting on, and Wait's timeout has it re-read on its own
// schedule regardless, so a lost notification costs one timeout of latency,
// never a deadlock. That discipline is what lets a best-effort transport
// carry the port.
type Notifier interface {
	// Notify wakes every waiter currently blocked on topic. Best-effort:
	// a waiter that arrives a moment later is woken by its own timeout.
	Notify(topic string)
	// Wait blocks until Notify(topic), ctx is done, or timeout elapses. It
	// returns ctx.Err() for context expiry and nil otherwise — a timeout is
	// a scheduled re-check, not a failure. A non-positive timeout waits on
	// the notification and ctx alone.
	Wait(ctx context.Context, topic string, timeout time.Duration) error
}

// notifier is the in-process Notifier: per-topic broadcast by closing a
// shared channel, the many-topic equivalent of a sync.Cond broadcast that a
// select can wait on alongside a context.
type notifier struct {
	mu     sync.Mutex
	topics map[string]*waitpoint
}

// A waitpoint is one topic's live broadcast channel and the count of waiters
// parked on it, kept so the last waiter to leave can drop the entry and a
// topic waited on once does not occupy the map forever.
type waitpoint struct {
	ch      chan struct{}
	waiters int
}

// NewNotifier returns the in-process Notifier for single-machine operation.
func NewNotifier() Notifier {
	return &notifier{topics: make(map[string]*waitpoint)}
}

// Notify wakes the topic's current waiters by closing their channel and
// retiring it; the next Wait starts a fresh one. Waking no one is a no-op.
func (n *notifier) Notify(topic string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if wp, ok := n.topics[topic]; ok {
		close(wp.ch)
		delete(n.topics, topic)
	}
}

// Wait parks on the topic's channel until it is closed by Notify, the timer
// fires, or ctx is done.
func (n *notifier) Wait(ctx context.Context, topic string, timeout time.Duration) error {
	n.mu.Lock()
	wp, ok := n.topics[topic]
	if !ok {
		wp = &waitpoint{ch: make(chan struct{})}
		n.topics[topic] = wp
	}
	wp.waiters++
	n.mu.Unlock()

	defer func() {
		n.mu.Lock()
		wp.waiters--
		// The map may already hold a successor channel if a Notify retired
		// this one while we were parked, so only remove our own entry.
		if wp.waiters == 0 && n.topics[topic] == wp {
			delete(n.topics, topic)
		}
		n.mu.Unlock()
	}()

	var fire <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		fire = timer.C
	}

	select {
	case <-wp.ch:
		return nil
	case <-fire:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
