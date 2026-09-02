package comms

import (
	"context"
	"encoding/json"
	"sync"
)

// Topic is a typed view over a single Bus topic: Emit gives compile-time safety
// on the publish side, so as long as a topic name is only ever used through one
// Topic[T], every payload on its feed is a T.
type Topic[T any] struct {
	bus  Bus
	name string
}

// NewTopic returns a typed view over the named bus topic.
func NewTopic[T any](bus Bus, name string) Topic[T] {
	return Topic[T]{bus: bus, name: name}
}

// Emit publishes v on the topic.
func (t Topic[T]) Emit(ctx context.Context, v T) error {
	return t.bus.Publish(ctx, t.name, v)
}

// On subscribes to the topic. Receivers assert payloads back to T: the
// subscription decodes a payload that crossed a process boundary — delivered
// as its JSON encoding, see Bus — back into T, so the same receive code runs
// over an in-process bus and a wire transport alike. A payload that is
// neither a T nor JSON that decodes into one is dropped, which at-most-once
// delivery already permits. Delivery is at-most-once: a subscriber that falls
// behind its buffer loses payloads, so coordination that blocks on a single
// message must size the bus buffer for the topic's burstiness or tolerate
// loss.
func (t Topic[T]) On(ctx context.Context) (Subscription, error) {
	inner, err := t.bus.Subscribe(ctx, t.name)
	if err != nil {
		return nil, err
	}
	return newDecodedSub[T](inner), nil
}

// decodedSub re-emits a raw subscription's payloads as T, decoding the wire
// form. It owns a pump goroutine, torn down by Close or by the inner feed
// closing; the pump owns the outgoing channel, so it closes exactly once.
type decodedSub[T any] struct {
	inner Subscription
	ch    chan any
	done  chan struct{}
	once  sync.Once
}

func newDecodedSub[T any](inner Subscription) *decodedSub[T] {
	s := &decodedSub[T]{inner: inner, ch: make(chan any), done: make(chan struct{})}
	go s.pump()
	return s
}

func (s *decodedSub[T]) pump() {
	defer close(s.ch)
	for payload := range s.inner.C() {
		v, ok := decodePayload[T](payload)
		if !ok {
			continue
		}
		select {
		case s.ch <- v:
		case <-s.done:
			return
		}
	}
}

// decodePayload maps one raw payload to a T: the value itself when the bus
// passed it in-process, or a JSON decode when it arrived as wire bytes.
func decodePayload[T any](payload any) (any, bool) {
	if v, ok := payload.(T); ok {
		return v, true
	}
	raw, ok := payload.(json.RawMessage)
	if !ok {
		return nil, false
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, false
	}
	return v, true
}

func (s *decodedSub[T]) C() <-chan any {
	return s.ch
}

func (s *decodedSub[T]) Close() error {
	s.once.Do(func() { close(s.done) })
	return s.inner.Close()
}
