package redis

import (
	"context"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/comms"
	goredis "github.com/redis/go-redis/v9"
)

// notifyNamespace keeps notifier channels apart from bus channels sharing a
// prefix, so a topic name used by both never crosses wires.
const notifyNamespace = "notify:"

// Notifier is a comms.Notifier over Redis pub/sub: Notify publishes an empty
// message — notifications are wakeup hints, never payloads — and Wait parks
// on a local broadcast armed by the shared subscription connection.
//
// Server-side subscriptions are sticky: the first Wait on a topic subscribes
// it and it stays subscribed until Close. The topics a deployment waits on
// are few and fixed (one per resource kind), so stickiness buys wait-loop
// cheapness without growing anything unbounded. A transport blip never fails
// a Wait — the contract's timeout is the backstop, and a notification lost
// while the client reconnects costs one timeout of latency, never a
// deadlock.
type Notifier struct {
	client *goredis.Client
	ps     *goredis.PubSub
	prefix string

	mu        sync.Mutex
	topics    map[string]*waitpoint
	confirmed map[string]bool
	issued    map[string]bool
	pending   map[string][]chan struct{}
	closed    bool

	done      chan struct{}
	pumped    sync.WaitGroup
	closeOnce sync.Once
}

// A waitpoint is one topic's live broadcast channel and its waiter count,
// retired by the notification that fires it or by its last waiter leaving.
type waitpoint struct {
	ch      chan struct{}
	waiters int
}

var _ comms.Notifier = (*Notifier)(nil)

// NewNotifier returns a Notifier carried by client. The client stays the
// caller's; Close here tears down only the Notifier's own subscription
// connection and goroutine. Build every node's Notifier with the same prefix
// or their wakeups pass each other silently.
func NewNotifier(client *goredis.Client, opts ...Option) *Notifier {
	s := newSettings(opts)
	n := &Notifier{
		client:    client,
		ps:        client.Subscribe(context.Background()),
		prefix:    s.prefix + notifyNamespace,
		topics:    make(map[string]*waitpoint),
		confirmed: make(map[string]bool),
		issued:    make(map[string]bool),
		pending:   make(map[string][]chan struct{}),
		done:      make(chan struct{}),
	}
	n.pumped.Add(1)
	go n.receive()
	return n
}

// Notify publishes a wakeup for the topic's current waiters, on every node.
// Best-effort by contract: a publish that fails is a notification that was
// lost, which waiters' timeouts already absorb, so there is nothing to
// return.
func (n *Notifier) Notify(topic string) {
	n.client.Publish(context.Background(), channelName(n.prefix, topic), nil)
}

// Wait parks until a notification, the timeout, or ctx. It first sees the
// topic's server subscription confirmed — waiting is pointless before the
// server routes the topic here — with the same timeout bounding that wait,
// so a broken transport degrades to bounded polling instead of an error.
func (n *Notifier) Wait(ctx context.Context, topic string, timeout time.Duration) error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil // a spurious wakeup; only ctx expiry is an error
	}
	wp := n.topics[topic]
	if wp == nil {
		wp = &waitpoint{ch: make(chan struct{})}
		n.topics[topic] = wp
	}
	wp.waiters++

	var confirm chan struct{}
	if !n.confirmed[topic] {
		if !n.issued[topic] {
			// A failed SUBSCRIBE is a transport blip: skip the confirmation
			// wait and let the timeout carry this round; the next Wait
			// retries the subscribe.
			if err := n.ps.Subscribe(ctx, channelName(n.prefix, topic)); err == nil {
				n.issued[topic] = true
			}
		}
		if n.issued[topic] {
			confirm = make(chan struct{})
			n.pending[topic] = append(n.pending[topic], confirm)
		}
	}
	n.mu.Unlock()

	defer func() {
		n.mu.Lock()
		wp.waiters--
		// A notification may have retired this waitpoint already; only
		// remove the entry that is still ours.
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

	if confirm != nil {
		select {
		case <-confirm:
		case <-wp.ch:
			return nil
		case <-fire:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return nil
		}
	}

	select {
	case <-wp.ch:
		return nil
	case <-fire:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return nil
	}
}

// Close stops the receive goroutine, closes the subscription connection, and
// wakes every parked waiter — a spurious wakeup, which the contract permits.
// The caller's client is untouched. Safe to call more than once.
func (n *Notifier) Close() error {
	n.closeOnce.Do(func() {
		close(n.done)
		n.ps.Close()
		n.pumped.Wait()

		n.mu.Lock()
		n.closed = true
		for _, wp := range n.topics {
			close(wp.ch)
		}
		n.topics = make(map[string]*waitpoint)
		for _, waiters := range n.pending {
			for _, w := range waiters {
				close(w)
			}
		}
		n.pending = make(map[string][]chan struct{})
		n.mu.Unlock()
	})
	return nil
}

// receive drains the shared connection: subscription confirmations release
// pending Waits, and each notification fires its topic's current broadcast.
func (n *Notifier) receive() {
	defer n.pumped.Done()
	for {
		msg, err := n.ps.Receive(context.Background())
		if err != nil {
			select {
			case <-n.done:
				return
			default:
			}
			time.Sleep(receiveBackoff)
			continue
		}
		if channel, ok := subscribed(msg); ok {
			n.confirm(topicName(n.prefix, channel))
			continue
		}
		if m, ok := msg.(*goredis.Message); ok {
			n.wake(topicName(n.prefix, m.Channel))
		}
	}
}

func (n *Notifier) confirm(topic string) {
	n.mu.Lock()
	n.confirmed[topic] = true
	waiters := n.pending[topic]
	delete(n.pending, topic)
	n.mu.Unlock()
	for _, w := range waiters {
		close(w)
	}
}

// wake fires the topic's broadcast by closing its channel and retiring it;
// the next Wait starts a fresh one, mirroring the in-process notifier.
func (n *Notifier) wake(topic string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if wp, ok := n.topics[topic]; ok {
		close(wp.ch)
		delete(n.topics, topic)
	}
}
