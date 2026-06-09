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
	mu   sync.Mutex
	subs map[string]map[*subscription]struct{}
}

// NewBus returns an in-memory Bus for single-machine operation.
func NewBus() Bus {
	return &bus{subs: make(map[string]map[*subscription]struct{})}
}

// Publish fans the payload out to each current subscriber, dropping it for any
// whose buffer is full so one slow reader cannot stall the others.
func (b *bus) Publish(ctx context.Context, topic string, payload any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for s := range b.subs[topic] {
		select {
		case s.ch <- payload:
		default:
		}
	}
	return nil
}

// Subscribe registers a new buffered feed for the topic. The in-memory bus uses
// Close as the sole teardown and does not act on ctx.
func (b *bus) Subscribe(ctx context.Context, topic string) (Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := &subscription{bus: b, topic: topic, ch: make(chan any, defaultBuffer)}
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
