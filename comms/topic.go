package comms

import "context"

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

// On subscribes to the topic. Receivers assert payloads back to T.
func (t Topic[T]) On(ctx context.Context) (Subscription, error) {
	return t.bus.Subscribe(ctx, t.name)
}
