// Package comms is the inter-task communication port. Workflow modules program
// against the Bus interface and the typed Topic layer; the framework supplies
// the implementation, so a module never depends on the concrete transport.
package comms

import "context"

// Bus is the transport port. An implementation fans out each published payload
// to every subscriber that was subscribed to the topic at publish time. Publish
// never blocks on a slow subscriber: if a subscriber's buffer is full the payload
// is dropped for that subscriber (at-most-once). Each subscriber observes
// payloads in publish order. Publishing to a topic with no subscribers is a
// no-op returning nil.
type Bus interface {
	Publish(ctx context.Context, topic string, payload any) error
	Subscribe(ctx context.Context, topic string) (Subscription, error)
}

// Subscription is a live feed for one topic. Close unsubscribes and closes C; it
// is safe to call more than once. A subscription delivers only payloads
// published after it was created.
type Subscription interface {
	C() <-chan any
	Close() error
}
