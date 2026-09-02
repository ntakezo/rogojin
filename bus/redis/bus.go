package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/comms"
	goredis "github.com/redis/go-redis/v9"
)

// defaultBuffer matches the in-memory bus: beyond it, deliveries are dropped
// for the slow subscriber rather than blocking the feed.
const defaultBuffer = 16

// receiveBackoff paces the receive loop's retry after a transport error while
// the client reconnects underneath it.
const receiveBackoff = 50 * time.Millisecond

// Bus is a comms.Bus over Redis pub/sub. One subscription connection serves
// every local subscriber; payloads cross the wire as JSON and arrive as
// json.RawMessage (see the package doc). Subscribe returns only once the
// server confirms the subscription, so a publish that happens-after a
// Subscribe is never missed for want of a settled SUBSCRIBE.
type Bus struct {
	client *goredis.Client
	ps     *goredis.PubSub
	prefix string
	buffer int
	onDrop func(topic string, payload any)

	mu        sync.Mutex
	subs      map[string]map[*subscription]struct{}
	confirmed map[string]bool            // topics the server has acknowledged
	issued    map[string]bool            // topics whose SUBSCRIBE is in flight or live
	pending   map[string][]chan struct{} // waiters for a topic's confirmation
	closed    bool

	done      chan struct{}
	pumped    sync.WaitGroup
	closeOnce sync.Once
}

var _ comms.Bus = (*Bus)(nil)

// NewBus returns a Bus carried by client. The client stays the caller's —
// its pooling, auth, and Close are not this Bus's to manage; Close here tears
// down only the Bus's own subscription connection and goroutine.
func NewBus(client *goredis.Client, opts ...Option) *Bus {
	s := newSettings(opts)
	b := &Bus{
		client:    client,
		ps:        client.Subscribe(context.Background()),
		prefix:    s.prefix,
		buffer:    s.buffer,
		onDrop:    s.onDrop,
		subs:      make(map[string]map[*subscription]struct{}),
		confirmed: make(map[string]bool),
		issued:    make(map[string]bool),
		pending:   make(map[string][]chan struct{}),
		done:      make(chan struct{}),
	}
	b.pumped.Add(1)
	go b.receive()
	return b
}

// Publish marshals payload and publishes it to the topic's channel. It is
// the one Publish that can refuse a payload: a value JSON cannot carry has
// no wire form, and failing loudly here beats delivering nothing silently.
func (b *Bus) Publish(ctx context.Context, topic string, payload any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("payload has no wire form: %w", err)
	}
	return b.client.Publish(ctx, channelName(b.prefix, topic), encoded).Err()
}

// Subscribe registers a buffered local feed and, for the topic's first local
// subscriber, subscribes the shared connection server-side — returning only
// once the server confirms, so the subscription is live when Subscribe is.
func (b *Bus) Subscribe(ctx context.Context, topic string) (comms.Subscription, error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, errors.New("redis bus is closed")
	}
	s := &subscription{bus: b, topic: topic, ch: make(chan any, b.buffer)}
	if b.subs[topic] == nil {
		b.subs[topic] = make(map[*subscription]struct{})
	}
	b.subs[topic][s] = struct{}{}

	var confirm chan struct{}
	if !b.confirmed[topic] {
		// Issued under the lock so two first-subscribers cannot both send
		// SUBSCRIBE; the write is a single small command, not a round trip.
		if !b.issued[topic] {
			if err := b.ps.Subscribe(ctx, channelName(b.prefix, topic)); err != nil {
				delete(b.subs[topic], s)
				b.mu.Unlock()
				return nil, fmt.Errorf("subscribe %s: %w", topic, err)
			}
			b.issued[topic] = true
		}
		confirm = make(chan struct{})
		b.pending[topic] = append(b.pending[topic], confirm)
	}
	b.mu.Unlock()

	if confirm != nil {
		select {
		case <-confirm:
		case <-ctx.Done():
			s.Close()
			return nil, ctx.Err()
		case <-b.done:
			return nil, errors.New("redis bus is closed")
		}
	}
	return s, nil
}

// Close tears down the Bus: the receive goroutine stops, every open
// subscription's channel closes, and the shared subscription connection is
// closed. The caller's client is untouched. Safe to call more than once.
func (b *Bus) Close() error {
	b.closeOnce.Do(func() {
		b.mu.Lock()
		b.closed = true
		var open []*subscription
		for _, set := range b.subs {
			for s := range set {
				open = append(open, s)
			}
		}
		b.subs = make(map[string]map[*subscription]struct{})
		b.mu.Unlock()

		close(b.done)
		b.ps.Close()
		b.pumped.Wait()
		// The pump has exited, so nothing can send on these anymore.
		for _, s := range open {
			s.closeFeed()
		}
	})
	return nil
}

// receive is the pump: one goroutine draining the shared connection,
// confirming subscriptions and fanning messages out to local subscribers.
// The client reconnects and resubscribes underneath it on transport errors;
// messages lost meanwhile are at-most-once doing its job.
func (b *Bus) receive() {
	defer b.pumped.Done()
	for {
		msg, err := b.ps.Receive(context.Background())
		if err != nil {
			select {
			case <-b.done:
				return
			default:
			}
			time.Sleep(receiveBackoff)
			continue
		}
		if channel, ok := subscribed(msg); ok {
			b.confirm(topicName(b.prefix, channel))
			continue
		}
		if m, ok := msg.(*goredis.Message); ok {
			b.dispatch(topicName(b.prefix, m.Channel), json.RawMessage(m.Payload))
		}
	}
}

// confirm marks the topic live and releases every Subscribe waiting on it.
func (b *Bus) confirm(topic string) {
	b.mu.Lock()
	b.confirmed[topic] = true
	waiters := b.pending[topic]
	delete(b.pending, topic)
	b.mu.Unlock()
	for _, w := range waiters {
		close(w)
	}
}

// dispatch fans one wire payload out to the topic's local subscribers,
// dropping it for any whose buffer is full, exactly like the in-memory bus.
func (b *Bus) dispatch(topic string, payload json.RawMessage) {
	var dropped int
	b.mu.Lock()
	for s := range b.subs[topic] {
		select {
		case s.ch <- payload:
		default:
			dropped++
		}
	}
	b.mu.Unlock()

	if b.onDrop != nil {
		for range dropped {
			b.onDrop(topic, payload)
		}
	}
}

type subscription struct {
	bus   *Bus
	topic string
	ch    chan any
	once  sync.Once
}

func (s *subscription) C() <-chan any {
	return s.ch
}

// Close removes the subscription under the bus lock before closing its
// channel, so a concurrent dispatch can never send on a closed channel; the
// topic's last local subscriber also unsubscribes the shared connection.
func (s *subscription) Close() error {
	s.bus.mu.Lock()
	delete(s.bus.subs[s.topic], s)
	if len(s.bus.subs[s.topic]) == 0 {
		delete(s.bus.subs, s.topic)
		delete(s.bus.confirmed, s.topic)
		delete(s.bus.issued, s.topic)
		// Best-effort: a failed unsubscribe leaves a channel the dispatch
		// map no longer routes, which costs nothing but the subscription.
		s.bus.ps.Unsubscribe(context.Background(), channelName(s.bus.prefix, s.topic))
	}
	s.bus.mu.Unlock()
	s.closeFeed()
	return nil
}

func (s *subscription) closeFeed() {
	s.once.Do(func() { close(s.ch) })
}
