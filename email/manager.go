package email

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// An Option configures a Manager at construction.
type Option func(*Manager)

// WithDropHandler registers fn, invoked outside the manager lock whenever a
// subscriber's full buffer drops a message; dropped is that subscription's
// running total. Drops are observability, not loss: the mail is still on the
// server, and WithBackfill re-fetches it.
func WithDropHandler(fn func(emailID, taskID string, dropped uint64)) Option {
	return func(m *Manager) { m.dropHandler = fn }
}

// WithListenerErrorHandler registers fn, invoked whenever an inbox listener
// fails to dial, authenticate, or serve. The listener retries with backoff
// regardless; the handler is how a consumer sees it struggling.
func WithListenerErrorHandler(fn func(emailID string, err error)) Option {
	return func(m *Manager) { m.errHandler = fn }
}

// WithDialer substitutes how listeners and backfills open their sessions,
// so tests and examples can serve scripted mail without a vendor. Left
// alone, the manager dials the vendor's IMAP endpoint.
func WithDialer(d Dialer) Option {
	return func(m *Manager) { m.dial = d }
}

// WithNode names this process in listener claims. Left alone, the manager
// generates an id unique to this process start, so a restarted process never
// inherits its dead predecessor's claims. Set it only to make claim rows
// legible under a fleet's own naming.
func WithNode(id string) Option {
	return func(m *Manager) {
		if id != "" {
			m.node = id
		}
	}
}

// WithListenerTTL sets how long a listener claim outlives its last renewal
// (default 30s). Renewal runs at a third of the TTL; the TTL is how long an
// inbox stays deaf after the node holding it dies without releasing, so
// shorter means faster takeover and chattier heartbeats.
func WithListenerTTL(d time.Duration) Option {
	return func(m *Manager) {
		if d > 0 {
			m.listenerTTL = d
		}
	}
}

// defaultListenerTTL is the claim lifetime between renewals.
const defaultListenerTTL = 30 * time.Second

// defaultNode identifies this process in claims: host and pid for the
// operator reading a row, a random suffix so a recycled pid is never
// mistaken for its predecessor. One place on purpose — a shared
// internal/nodeid package arrives with the task claims manager, and this is
// the one line that will swap to it.
func defaultNode() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "node"
	}
	return fmt.Sprintf("%s/%d/%s", host, os.Getpid(), uuid.NewString()[:8])
}

// A Manager owns the email inventory and every live listener. Listeners are
// established lazily by the first Listen covering an inbox and torn down by
// the last Close; every message fans out to every attached subscription
// whose filter matches. A Manager is safe for concurrent use.
type Manager struct {
	repo        Repository
	dial        Dialer
	node        string
	listenerTTL time.Duration
	dropHandler func(emailID, taskID string, dropped uint64)
	errHandler  func(emailID string, err error)

	// backfills tracks every live backfill goroutine; closing, closed at
	// shutdown, is what cancels their contexts so Close never waits on a
	// fetch still in flight against a real server. runners tracks listener
	// goroutines the same way — including ones already unhooked by a last
	// unsubscribe but still draining — because a claim is only released on a
	// runner's way out, and Close promises every claim is settled when it
	// returns.
	backfills sync.WaitGroup
	runners   sync.WaitGroup
	closing   chan struct{}

	mu        sync.Mutex
	guard     func(emailID string) (held, locked []string)
	inventory map[string]Email
	listeners map[string]*listener
	closed    bool
}

// GuardDeletes installs the referential check Delete consults before
// removing an email: which tasks hold a live lease on — and which merely
// durably lock — an account forwarding to it. accounts.NewManager installs
// this when handed the email manager; consumers never call it. Without a
// guard, Delete checks only active subscriptions. The guard reads the
// installing manager's process-local view: a lease held through another
// node's account manager is not consulted, so fleet deployments delete
// emails from the node whose accounts reference them.
func (m *Manager) GuardDeletes(guard func(emailID string) (held, locked []string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.guard = guard
}

// NewManager loads the inventory from the repository. The inventory changes
// afterwards only through Add and Delete; listener cursors are the one field
// the manager writes back on its own.
//
// A nil repository runs the inventory purely in memory, on the same
// in-process store NewMemoryRepository returns: claims and cursors are
// enforced for real, but the manager starts empty (seed it through Add) and
// no inbox survives the process — the same bargain a nil task repository
// strikes.
func NewManager(ctx context.Context, repo Repository, opts ...Option) (*Manager, error) {
	if repo == nil {
		repo = NewMemoryRepository()
	}
	m := &Manager{
		repo:        repo,
		dial:        dialIMAP,
		node:        defaultNode(),
		listenerTTL: defaultListenerTTL,
		closing:     make(chan struct{}),
		inventory:   make(map[string]Email),
		listeners:   make(map[string]*listener),
	}
	for _, opt := range opts {
		opt(m)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("load emails: %w", err)
	}
	for _, e := range listed {
		if _, dup := m.inventory[e.ID]; dup {
			return nil, fmt.Errorf("duplicate email id %s", e.ID)
		}
		m.inventory[e.ID] = e
	}
	return m, nil
}

// Add persists and installs a new email. The address is required; an inbox,
// when present, must name a known vendor and carry credentials its vendor
// accepts.
func (m *Manager) Add(ctx context.Context, e Email) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if e.ID == "" {
		return errors.New("email id is required")
	}
	if e.Address == "" {
		return fmt.Errorf("email %s: address is required", e.ID)
	}
	if _, dup := m.inventory[e.ID]; dup {
		return fmt.Errorf("email %s already exists", e.ID)
	}
	if e.Inbox != nil {
		if err := validateInbox(*e.Inbox); err != nil {
			return fmt.Errorf("email %s: %w", e.ID, err)
		}
	}

	now := time.Now().UTC()
	e.CreatedAt, e.UpdatedAt = now, now
	if err := m.repo.Save(ctx, e); err != nil {
		return fmt.Errorf("persist email %s: %w", e.ID, err)
	}
	m.inventory[e.ID] = e
	return nil
}

// Get returns the email by id. It exists because address-only use is real:
// a workflow filling a form needs the address of its forwarding inbox
// without listening to it.
func (m *Manager) Get(ctx context.Context, id string) (Email, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.inventory[id]
	if !ok {
		return Email{}, fmt.Errorf("email %s: %w", id, ErrEmailNotFound)
	}
	return e, nil
}

// Delete removes an email, applying the same policy leasing institutes for
// deleting resources under running tasks: it refuses with ErrEmailInUse
// while any subscription covers the email or the guard reports a live lease
// on a referencing account, and reports — rather than blocks on — the tasks
// whose idle durable locks the deletion strands, so the caller decides
// their fate.
func (m *Manager) Delete(ctx context.Context, id string) (stranded []string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.inventory[id]; !ok {
		// Nothing live to guard, but the store may still carry a row this
		// manager never loaded.
		return nil, m.repo.Delete(ctx, id)
	}
	if l := m.listeners[id]; l != nil {
		if tasks := l.taskIDs(); len(tasks) > 0 {
			return nil, fmt.Errorf("%w: %s is being listened to by %s", ErrEmailInUse, id, plural("task", tasks))
		}
	}
	if m.guard != nil {
		held, locked := m.guard(id)
		if len(held) > 0 {
			sort.Strings(held)
			return nil, fmt.Errorf("%w: %s is the forwarding inbox of accounts held by %s", ErrEmailInUse, id, plural("task", held))
		}
		stranded = locked
		sort.Strings(stranded)
	}

	if err := m.repo.Delete(ctx, id); err != nil {
		return nil, fmt.Errorf("delete email %s: %w", id, err)
	}
	delete(m.inventory, id)
	return stranded, nil
}

// Close stops every listener, cancels every backfill, and closes every
// subscription, waiting for their goroutines to exit. For process shutdown;
// not part of the task path. Safe to call more than once, and safe to
// interleave with subscription Closes: both routes retire a subscription
// through the same guarded close, so neither panics on the other's work.
func (m *Manager) Close() error {
	m.mu.Lock()
	if !m.closed {
		m.closed = true
		close(m.closing)
	}
	stopped := make([]*listener, 0, len(m.listeners))
	for id, l := range m.listeners {
		close(l.stop)
		for s := range l.subs {
			s.retire()
		}
		l.subs = make(map[*subscription]struct{})
		stopped = append(stopped, l)
		delete(m.listeners, id)
	}
	m.mu.Unlock()

	for _, l := range stopped {
		<-l.done
	}
	m.runners.Wait()
	m.backfills.Wait()
	return nil
}

// A ListenOption configures one subscription.
type ListenOption func(*listenConfig)

type listenConfig struct {
	from     string
	buffer   int
	backfill time.Time
}

// FromSender restricts the subscription to messages whose From addr-spec
// equals address, case-insensitively. Default: all mail. The workflow, not
// the framework, decides this — and owns all parsing of what arrives.
func FromSender(address string) ListenOption {
	return func(c *listenConfig) { c.from = strings.ToLower(address) }
}

// WithBackfill re-delivers messages already in the inbox dated on or after
// since, fetched from the server on subscribe. This is how a recovered task
// catches mail that arrived while it was down: the IMAP server is the
// replay log, so nothing needs to have been stored locally.
func WithBackfill(since time.Time) ListenOption {
	return func(c *listenConfig) { c.backfill = since }
}

// WithBuffer sets the subscription's channel capacity (default 64). A full
// buffer drops for that subscriber only; see WithDropHandler.
func WithBuffer(n int) ListenOption {
	return func(c *listenConfig) { c.buffer = n }
}

// A Subscription is one task's live feed from one inbox. Close it exactly
// once when done; the last close covering an inbox hangs up its listener.
type Subscription interface {
	C() <-chan Message
	Close() error
}

// Listen subscribes taskID to the email's inbox, lazily establishing the
// one listener per inbox on first use — claiming the inbox in the store
// first, so of all nodes sharing it exactly one opens a session. It fails
// fast — ErrEmailNotFound for an unknown id (including a dangling account
// reference), ErrNoInbox for an address-only email, ErrListenerHeld while
// another node holds the inbox — because waiting on an inbox that cannot
// exist, or that this node cannot serve, would hide the situation as a
// hang. Cross-node subscribers arrive with a distributed bus; until then a
// refused Listen names the node problem honestly.
func (m *Manager) Listen(ctx context.Context, taskID, emailID string, opts ...ListenOption) (Subscription, error) {
	if taskID == "" {
		return nil, errors.New("task id is required")
	}
	cfg := listenConfig{buffer: 64}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.buffer < 1 {
		return nil, fmt.Errorf("listen buffer %d: need at least 1", cfg.buffer)
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, errors.New("manager is closed")
	}
	e, ok := m.inventory[emailID]
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("listen to email %s: %w", emailID, ErrEmailNotFound)
	}
	if e.Inbox == nil {
		m.mu.Unlock()
		return nil, fmt.Errorf("listen to email %s (%s): %w", emailID, e.Address, ErrNoInbox)
	}

	l, live := m.listeners[emailID]
	if !live {
		// Claim before dialing, under the lock like every other store write
		// here: the claim and the listener registration must be one step, or
		// a concurrent exiting listener could release the claim between them.
		if err := m.repo.ClaimListener(ctx, emailID, m.node, m.listenerTTL); err != nil {
			m.mu.Unlock()
			return nil, fmt.Errorf("listen to email %s: %w", emailID, err)
		}
		l = newListener(e)
		m.listeners[emailID] = l
		m.runners.Add(1)
		go func() {
			defer m.runners.Done()
			m.run(l)
		}()
	}
	s := &subscription{
		manager: m,
		emailID: emailID,
		taskID:  taskID,
		from:    cfg.from,
		ch:      make(chan Message, cfg.buffer),
	}
	l.subs[s] = struct{}{}
	if !cfg.backfill.IsZero() {
		// Registered under the lock, while closed is known false: Close sets
		// closed first, so it can never miss a backfill it must wait for.
		m.backfills.Add(1)
		go func() {
			defer m.backfills.Done()
			bctx, cancel := context.WithCancel(ctx)
			defer cancel()
			go func() {
				select {
				case <-m.closing:
					cancel()
				case <-bctx.Done():
				}
			}()
			m.backfill(bctx, e, s, cfg.backfill)
		}()
	}
	m.mu.Unlock()
	return s, nil
}

// A subscription is the fan-out endpoint for one Listen call. Its channel
// is closed under the manager lock only after it leaves the listener's set,
// so a concurrent delivery can never send on a closed channel.
type subscription struct {
	manager *Manager
	emailID string
	taskID  string
	from    string
	ch      chan Message
	dropped uint64
	closed  bool
}

func (s *subscription) C() <-chan Message { return s.ch }

// retire marks the subscription closed and closes its channel. It is the one
// place the channel closes, shared by Close and Manager.Close so the two
// routes cannot double-close it whichever runs first. Callers hold m.mu.
func (s *subscription) retire() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

// Close detaches the subscription; the last one covering an inbox stops its
// listener. Only the first close acts, whether it was this or the manager's.
func (s *subscription) Close() error {
	m := s.manager
	m.mu.Lock()
	if l, ok := m.listeners[s.emailID]; ok {
		delete(l.subs, s)
		if len(l.subs) == 0 {
			close(l.stop)
			delete(m.listeners, s.emailID)
		}
	}
	s.retire()
	m.mu.Unlock()
	return nil
}

// matches reports whether the subscription's sender filter admits msg.
func (s *subscription) matches(msg Message) bool {
	return s.from == "" || s.from == strings.ToLower(msg.From)
}

// A drop names one message a full buffer refused, reported to the drop
// handler outside the lock.
type drop struct {
	emailID string
	taskID  string
	total   uint64
}

// offer delivers msg to s without blocking, recording a drop on a full
// buffer. Callers hold m.mu; the returned drop is reported once released.
func offer(s *subscription, msg Message) (drop, bool) {
	if s.closed || !s.matches(msg) {
		return drop{}, false
	}
	select {
	case s.ch <- msg:
		return drop{}, false
	default:
		s.dropped++
		return drop{emailID: s.emailID, taskID: s.taskID, total: s.dropped}, true
	}
}

// fanout offers every message to every subscription of the listener — all
// of them, never only the first — and returns the highest UID seen so the
// cursor advances only after delivery was attempted. owned is false when l
// has been stopped or replaced since the fetch: its subs set is empty by
// then, so delivering would deliver to nobody, and the caller must not let
// the cursor move past mail the inbox's current listener still owes.
func (m *Manager) fanout(l *listener, msgs []Message) (maxUID uint32, owned bool) {
	var drops []drop

	m.mu.Lock()
	if !m.owns(l) {
		m.mu.Unlock()
		return 0, false
	}
	for _, msg := range msgs {
		msg.EmailID = l.emailID
		for s := range l.subs {
			if d, dropped := offer(s, msg); dropped {
				drops = append(drops, d)
			}
		}
		if msg.UID > maxUID {
			maxUID = msg.UID
		}
	}
	m.mu.Unlock()

	m.reportDrops(drops)
	return maxUID, true
}

// backfill re-fetches server-side history for one new subscription on its
// own short-lived connection, so the live listener's IDLE loop is never
// interrupted. Duplicates with live delivery are possible and documented:
// subscribers deduplicate by MessageID.
func (m *Manager) backfill(ctx context.Context, e Email, s *subscription, since time.Time) {
	mb, err := m.dial(ctx, e)
	if err != nil {
		if ctx.Err() == nil {
			m.reportError(e.ID, fmt.Errorf("backfill dial: %w", err))
		}
		return
	}
	defer mb.Close()
	if _, _, err := mb.Select(ctx); err != nil {
		m.reportError(e.ID, fmt.Errorf("backfill select: %w", err))
		return
	}
	msgs, err := mb.FetchSinceDate(ctx, since)
	if err != nil {
		m.reportError(e.ID, fmt.Errorf("backfill fetch: %w", err))
		return
	}

	var drops []drop
	m.mu.Lock()
	for _, msg := range msgs {
		msg.EmailID = e.ID
		if d, dropped := offer(s, msg); dropped {
			drops = append(drops, d)
		}
	}
	m.mu.Unlock()
	m.reportDrops(drops)
}

func (m *Manager) reportDrops(drops []drop) {
	if m.dropHandler == nil {
		return
	}
	for _, d := range drops {
		m.dropHandler(d.emailID, d.taskID, d.total)
	}
}

func (m *Manager) reportError(emailID string, err error) {
	if m.errHandler != nil {
		m.errHandler(emailID, err)
	}
}

// plural renders an id list as "task t1" or "tasks t1, t2", so a refusal
// reads as a sentence and still names everything blocking it.
func plural(noun string, ids []string) string {
	if len(ids) == 1 {
		return noun + " " + ids[0]
	}
	return noun + "s " + strings.Join(ids, ", ")
}
