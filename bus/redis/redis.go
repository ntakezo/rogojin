// Package redis carries the framework's inter-node communication over Redis
// pub/sub: a comms.Bus for task-to-task messages and a comms.Notifier for
// wakeup hints, so a fleet's processes hear each other the way one process's
// goroutines already do.
//
// Redis pub/sub is fire-and-forget, which is exactly the contract both ports
// document: at-most-once, fan-out to whoever is subscribed at publish time,
// publishing into the void a harmless no-op. Nothing is retried or replayed
// here — the framework's coordination is built to survive a lost message (a
// waiter's timeout re-checks the store; a topic consumer tolerates loss), so
// the transport is allowed to be this simple.
//
// Payloads cross the wire as JSON: Publish marshals and refuses a payload
// JSON cannot carry — the one error the in-memory bus never returns — and
// subscribers receive each payload as a json.RawMessage. The typed
// comms.Topic layer decodes that form back, so workflow code written against
// a Topic runs unchanged over either bus. Ordering is per publisher
// connection, as Redis delivers it: subscribers sharing a topic see one
// publisher's messages in order, not a global order across publishers.
//
// Both constructors take the caller's *redis.Client — pooling, auth, and TLS
// are the caller's, and so is closing it. Closing a Bus or Notifier tears
// down only its own subscription connection and goroutines.
package redis

import (
	"strings"

	goredis "github.com/redis/go-redis/v9"
)

// defaultPrefix namespaces every channel so one Redis can serve several
// deployments; WithPrefix/WithNotifierPrefix override it per instance.
const defaultPrefix = "rogojin:"

// channelName joins prefix and topic; topicName is its inverse for messages
// coming off the wire.
func channelName(prefix, topic string) string {
	return prefix + topic
}

func topicName(prefix, channel string) string {
	return strings.TrimPrefix(channel, prefix)
}

// subscribed reports whether a raw pub/sub event is the server confirming a
// SUBSCRIBE, which is the moment a subscription is actually live.
func subscribed(msg any) (string, bool) {
	s, ok := msg.(*goredis.Subscription)
	if !ok || s.Kind != "subscribe" {
		return "", false
	}
	return s.Channel, true
}
