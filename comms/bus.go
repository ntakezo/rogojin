package comms

import (
	"context"
	"sync"
)

// defaultBuffer is the per-subscriber channel capacity. Beyond it, publishes are
// dropped for that subscriber rather than blocking the bus.
const defaultBuffer = 16

// bus is the default in-memory Bus adapter for single-machine operation.
type bus struct {
	mu     sync.Mutex
	subs   map[string]map[*subscription]struct{}
	buffer int
	onDrop func(topic string, payload any)
}

// A BusOption configures the in-memory bus returned by NewBus.
type BusOption func(*bus)

// WithBuffer sets each subscriber's channel capacity (default 16). A subscriber
// that falls further behind loses payloads; size it for the topic's burstiness.
// Values below 1 are raised to 1.
func WithBuffer(n int) BusOption {
	return func(b *bus) { b.buffer = max(n, 1) }
}

// WithDropHandler installs fn, invoked once per subscriber that a publish was
// dropped for because its buffer was full. It runs on the publisher's
// goroutine, outside the bus lock, so it may safely call back into the bus.
func WithDropHandler(fn func(topic string, payload any)) BusOption {
	return func(b *bus) { b.onDrop = fn }
}

// NewBus returns an in-memory Bus for single-machine operation.
func NewBus(opts ...BusOption) Bus {
	b := &bus{subs: make(map[string]map[*subscription]struct{}), buffer: defaultBuffer}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Publish fans the payload out to each current subscriber, dropping it for any
// whose buffer is full so one slow reader cannot stall the others. Drops are
// reported to the WithDropHandler hook, if one is set.
func (b *bus) Publish(ctx context.Context, topic string, payload any) error {
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
	return nil
}

// Subscribe registers a new buffered feed for the topic. The in-memory bus uses
// Close as the sole teardown and does not act on ctx.
func (b *bus) Subscribe(ctx context.Context, topic string) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := &subscription{bus: b, topic: topic, ch: make(chan any, b.buffer)}
	if b.subs[topic] == nil {
		b.subs[topic] = make(map[*subscription]struct{})
	}
	b.subs[topic][s] = struct{}{}
	return s, nil
}

type subscription struct {
	bus   *bus
	topic string
	ch    chan any
	once  sync.Once
}

func (s *subscription) C() <-chan any {
	return s.ch
}

// Close removes the subscription under the bus lock before closing its channel,
// so a concurrent Publish can never send on a closed channel.
func (s *subscription) Close() error {
	s.once.Do(func() {
		s.bus.mu.Lock()
		delete(s.bus.subs[s.topic], s)
		if len(s.bus.subs[s.topic]) == 0 {
			delete(s.bus.subs, s.topic)
		}
		s.bus.mu.Unlock()
		close(s.ch)
	})
	return nil
}
