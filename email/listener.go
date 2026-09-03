package email

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

// A Mailbox is one authenticated session against an inbox — the seam
// between a listener and the wire. The production implementation wraps
// go-imap; tests and examples substitute scripted fakes through WithDialer.
// Select reports the mailbox's UIDVALIDITY and its current last UID,
// FetchSince returns messages with UIDs strictly above uid, FetchSinceDate
// returns messages dated on or after since (the backfill path), and Idle
// returns nil when new mail may exist or an error when the session broke.
type Mailbox interface {
	Select(ctx context.Context) (uidValidity, lastUID uint32, err error)
	FetchSince(ctx context.Context, uid uint32) ([]Message, error)
	FetchSinceDate(ctx context.Context, since time.Time) ([]Message, error)
	Idle(ctx context.Context) error
	Close() error
}

// A Dialer opens an authenticated Mailbox session for one email. The context
// bounds establishment only — dial, token mint, authenticate — not the
// session's life: a listener hangs up by closing the Mailbox, and a dial that
// cannot honor cancellation would otherwise hold up manager shutdown.
type Dialer func(ctx context.Context, e Email) (Mailbox, error)

// Reconnect backoff bounds: doubling from backoffFloor, capped at
// backoffCeil, reset after a serve that stayed up healthyAfter.
const (
	backoffFloor = time.Second
	backoffCeil  = 2 * time.Minute
	healthyAfter = 30 * time.Second
)

// A listener is the single connection serving one inbox. Its subs set is
// guarded by the manager's mutex; stop is closed by the last unsubscribe
// (or manager shutdown), done by the goroutine on its way out.
type listener struct {
	emailID string
	email   Email // credentials snapshot for dialing
	subs    map[*subscription]struct{}
	stop    chan struct{}
	done    chan struct{}
}

func newListener(e Email) *listener {
	return &listener{
		emailID: e.ID,
		email:   e,
		subs:    make(map[*subscription]struct{}),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// taskIDs lists the tasks currently subscribed, sorted by plural's caller.
// Callers hold m.mu.
func (l *listener) taskIDs() []string {
	ids := make(map[string]struct{}, len(l.subs))
	for s := range l.subs {
		ids[s.taskID] = struct{}{}
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// run is the listener goroutine: dial, serve until the session breaks, back
// off, redial — never giving up while subscribers remain. The loop never
// takes the caller's context; it lives exactly as long as its stop channel.
// On the way out it releases the inbox claim, unless a successor listener in
// this process has already been registered over it — the claim is then the
// successor's to keep.
func (m *Manager) run(l *listener) {
	defer close(l.done)
	defer m.releaseClaim(l)
	go m.renewLoop(l)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-l.stop
		cancel()
	}()

	backoff := backoffFloor
	for {
		select {
		case <-l.stop:
			return
		default:
		}

		started := time.Now()
		if err := m.serveOnce(ctx, l); err != nil {
			m.reportError(l.emailID, err)
		}
		select {
		case <-l.stop:
			return
		case <-time.After(backoff):
		}
		if time.Since(started) > healthyAfter {
			backoff = backoffFloor
		} else if backoff *= 2; backoff > backoffCeil {
			backoff = backoffCeil
		}
	}
}

// serveOnce runs one session: dial, reconcile the cursor, then alternate
// fetch and IDLE until the session errors, the listener stops, or another
// listener supersedes this one.
func (m *Manager) serveOnce(ctx context.Context, l *listener) error {
	mb, err := m.dial(ctx, l.email)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("dial inbox %s: %w", l.emailID, err)
	}
	defer mb.Close()

	uidValidity, lastUID, err := mb.Select(ctx)
	if err != nil {
		return fmt.Errorf("select inbox %s: %w", l.emailID, err)
	}
	cursor, ok := m.cursor(l)
	if !ok {
		return nil // deleted or superseded under the session; the stop follows
	}
	if uidValidity != cursor.UIDValidity {
		// History renumbered: jump to the mailbox's present rather than
		// replaying it whole under new numbers.
		if err := m.advanceCursor(l, uidValidity, lastUID, true); err != nil {
			return err
		}
	}

	for {
		cursor, ok := m.cursor(l)
		if !ok {
			return nil
		}
		msgs, err := mb.FetchSince(ctx, cursor.LastUID)
		if err != nil {
			return fmt.Errorf("fetch inbox %s: %w", l.emailID, err)
		}
		maxUID, owned := m.fanout(l, msgs)
		if !owned {
			// Superseded mid-fetch: the mail went to nobody, so the cursor
			// must not move past it — the inbox's current listener will fetch
			// it again and deliver it.
			return nil
		}
		if maxUID > cursor.LastUID {
			// After fan-out, not before: a crash between fetch and persist
			// re-delivers on restart. At-least-once, deduped by MessageID.
			if err := m.advanceCursor(l, uidValidity, maxUID, false); err != nil {
				return err
			}
		}
		if err := mb.Idle(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("idle inbox %s: %w", l.emailID, err)
		}
	}
}

// owns reports whether l is still the inbox's registered listener. A stopped
// or replaced listener keeps running until its goroutine notices, and in that
// window it must neither deliver nor write the cursor: only the current
// listener speaks for the inbox. Callers hold m.mu.
func (m *Manager) owns(l *listener) bool {
	return m.listeners[l.emailID] == l
}

// renewLoop is the claim heartbeat: while the listener lives, its node's
// claim never expires. A renewal refused with ErrListenerHeld means another
// node took the inbox after this claim lapsed — the listener is torn down at
// once, because everything it fetches from here on is the new owner's mail.
// Any other renewal failure is reported and retried on the next tick: the
// claim survives a store hiccup as long as no one else takes it.
func (m *Manager) renewLoop(l *listener) {
	tick := time.NewTicker(max(m.listenerTTL/3, time.Millisecond))
	defer tick.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-tick.C:
			err := m.repo.RenewListener(context.Background(), l.emailID, m.node, m.listenerTTL)
			switch {
			case err == nil:
			case errors.Is(err, ErrListenerHeld):
				m.mu.Lock()
				m.usurpedLocked(l)
				m.mu.Unlock()
				m.reportError(l.emailID, fmt.Errorf("inbox listener usurped: %w", err))
				return
			default:
				m.reportError(l.emailID, fmt.Errorf("renew listener claim: %w", err))
			}
		}
	}
}

// usurpedLocked tears the listener down after another node took its inbox:
// subscriptions are retired — a closed channel is the honest signal that
// this feed has ended — and the listener leaves the registry so its dying
// goroutine can neither deliver nor write the cursor. Callers hold m.mu.
func (m *Manager) usurpedLocked(l *listener) {
	if !m.owns(l) {
		return
	}
	close(l.stop)
	for s := range l.subs {
		s.retire()
	}
	l.subs = make(map[*subscription]struct{})
	delete(m.listeners, l.emailID)
}

// releaseClaim hands the inbox claim back on listener exit — unless a
// successor listener is registered, in which case the claim now belongs to
// it, or this node was usurped, in which case ReleaseListener is a no-op by
// contract. Best-effort on a background context: the process may be shutting
// down, and an unreleased claim only costs its TTL.
func (m *Manager) releaseClaim(l *listener) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.listeners[l.emailID] != nil {
		return
	}
	err := m.repo.ReleaseListener(context.Background(), l.emailID, m.node)
	if err != nil && !errors.Is(err, ErrEmailNotFound) {
		// A deleted email has no claim left to release; anything else is
		// worth a report, though the claim expiring covers for it.
		m.reportError(l.emailID, fmt.Errorf("release listener claim: %w", err))
	}
}

// cursor reads the email's durable cursor; ok is false once the email is
// gone from the inventory or l is no longer its listener.
func (m *Manager) cursor(l *listener) (Inbox, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.inventory[l.emailID]
	if !ok || e.Inbox == nil || !m.owns(l) {
		return Inbox{}, false
	}
	return *e.Inbox, true
}

// advanceCursor persists and installs the cursor, store first like every
// other write here. The store's AdvanceCursor enforces what used to be
// reasoned about locally: only the claim holder writes, only forward moves
// land (a changed UIDVALIDITY is the reset that may move LastUID back), and
// a lagging duplicate write is a silent no-op — so a stale writer can never
// roll the cursor back over a successor's progress. A refusal naming
// ErrListenerHeld means another node took the inbox; the listener is torn
// down on the spot rather than served to nobody until its next renewal.
func (m *Manager) advanceCursor(l *listener, uidValidity, uid uint32, reset bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.inventory[l.emailID]
	if !ok || e.Inbox == nil || !m.owns(l) {
		return nil
	}
	// Background, not the session context: a cursor whose mail was fanned out
	// should land even as the listener is being cancelled.
	if err := m.repo.AdvanceCursor(context.Background(), l.emailID, m.node, uidValidity, uid); err != nil {
		if errors.Is(err, ErrListenerHeld) {
			m.usurpedLocked(l)
			return nil
		}
		return fmt.Errorf("persist cursor of email %s: %w", l.emailID, err)
	}
	inbox := *e.Inbox
	inbox.UIDValidity = uidValidity
	if reset || uid > inbox.LastUID {
		inbox.LastUID = uid
	}
	e.Inbox = &inbox
	e.UpdatedAt = time.Now().UTC()
	m.inventory[l.emailID] = e
	return nil
}
