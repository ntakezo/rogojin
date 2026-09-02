package email

import (
	"context"
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
func (m *Manager) run(l *listener) {
	defer close(l.done)

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
// other write here. reset moves LastUID backwards too, for UIDVALIDITY
// changes; ordinary advances only ever move it forward. A listener that lost
// its inbox writes nothing — its successor owns the cursor now, and a stale
// reset landing over the successor's fresh one would replay or skip mail.
func (m *Manager) advanceCursor(l *listener, uidValidity, uid uint32, reset bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.inventory[l.emailID]
	if !ok || e.Inbox == nil || !m.owns(l) {
		return nil
	}
	inbox := *e.Inbox
	inbox.UIDValidity = uidValidity
	if reset || uid > inbox.LastUID {
		inbox.LastUID = uid
	}
	e.Inbox = &inbox
	e.UpdatedAt = time.Now().UTC()
	// Background, not the session context: a cursor whose mail was fanned out
	// should land even as the listener is being cancelled.
	if err := m.repo.Save(context.Background(), e); err != nil {
		return fmt.Errorf("persist cursor of email %s: %w", l.emailID, err)
	}
	m.inventory[l.emailID] = e
	return nil
}
