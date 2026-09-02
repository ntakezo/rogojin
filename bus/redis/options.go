package redis

// settings is what the shared options configure; each constructor reads the
// fields that concern it.
type settings struct {
	prefix string
	buffer int
	onDrop func(topic string, payload any)
}

// An Option configures a Bus or a Notifier. WithBuffer and WithDropHandler
// tune a Bus's local delivery; a Notifier carries no payloads and ignores
// them.
type Option func(*settings)

// WithPrefix namespaces every channel under prefix (default "rogojin:"), so
// one Redis serves several deployments without their topics meeting. A Bus
// and a Notifier meant to work together must be built with the same prefix
// on every node.
func WithPrefix(prefix string) Option {
	return func(s *settings) { s.prefix = prefix }
}

// WithBuffer sets each subscriber's channel capacity (default 16). A
// subscriber that falls further behind loses payloads; size it for the
// topic's burstiness.  Values below 1 are raised to 1.
func WithBuffer(n int) Option {
	return func(s *settings) { s.buffer = max(n, 1) }
}

// WithDropHandler installs fn, invoked once per subscriber that a delivery
// was dropped for because its buffer was full. It runs on the receiving
// goroutine, outside the bus lock.
func WithDropHandler(fn func(topic string, payload any)) Option {
	return func(s *settings) { s.onDrop = fn }
}

func newSettings(opts []Option) settings {
	s := settings{prefix: defaultPrefix, buffer: defaultBuffer}
	for _, opt := range opts {
		opt(&s)
	}
	return s
}
