package email

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// These tests cover the email layer's own guarantees — the flat inventory,
// the lazily-established one-listener-per-inbox rule, fan-out to every
// subscriber, the durable cursor, and the leasing-mirrored delete policy.
// The IMAP boundary is the mailbox seam; a scripted fake server stands in
// so no test needs a network.

// memRepo is an in-memory Repository recording saves so tests can assert
// persistence without sqlite.
type memRepo struct {
	mu    sync.Mutex
	rows  map[string]Email
	order []string
}

func newMemRepo() *memRepo {
	return &memRepo{rows: map[string]Email{}}
}

func (r *memRepo) List(ctx context.Context) ([]Email, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Email, 0, len(r.order))
	for _, id := range r.order {
		if e, ok := r.rows[id]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

func (r *memRepo) Save(ctx context.Context, e Email) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rows[e.ID]; !ok {
		r.order = append(r.order, e.ID)
	}
	r.rows[e.ID] = e
	return nil
}

func (r *memRepo) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.rows, id)
	return nil
}

func (r *memRepo) get(t *testing.T, id string) Email {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.rows[id]
	if !ok {
		t.Fatalf("repo has no email %s", id)
	}
	return e
}

// fakeServer is the mail that "exists server-side" for one inbox, shared by
// every session dialed against it — the live listener, backfills, redials.
type fakeServer struct {
	mu          sync.Mutex
	uidValidity uint32
	nextUID     uint32
	msgs        []Message
	wakes       map[chan struct{}]struct{}

	dials  atomic.Int32
	closes atomic.Int32
}

func newFakeServer() *fakeServer {
	return &fakeServer{uidValidity: 1, nextUID: 1, wakes: map[chan struct{}]struct{}{}}
}

// deliver lands one message on the server and wakes every idling session.
func (f *fakeServer) deliver(msg Message) {
	f.mu.Lock()
	msg.UID = f.nextUID
	f.nextUID++
	f.msgs = append(f.msgs, msg)
	for wake := range f.wakes {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	f.mu.Unlock()
}

func (f *fakeServer) dial(e Email) (Mailbox, error) {
	f.dials.Add(1)
	wake := make(chan struct{}, 1)
	f.mu.Lock()
	f.wakes[wake] = struct{}{}
	f.mu.Unlock()
	return &fakeSession{server: f, wake: wake}, nil
}

type fakeSession struct {
	server *fakeServer
	wake   chan struct{}
}

func (s *fakeSession) Select(ctx context.Context) (uint32, uint32, error) {
	s.server.mu.Lock()
	defer s.server.mu.Unlock()
	return s.server.uidValidity, s.server.nextUID - 1, nil
}

func (s *fakeSession) FetchSince(ctx context.Context, uid uint32) ([]Message, error) {
	s.server.mu.Lock()
	defer s.server.mu.Unlock()
	out := make([]Message, 0)
	for _, m := range s.server.msgs {
		if m.UID > uid {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *fakeSession) FetchSinceDate(ctx context.Context, since time.Time) ([]Message, error) {
	s.server.mu.Lock()
	defer s.server.mu.Unlock()
	out := make([]Message, 0)
	for _, m := range s.server.msgs {
		if !m.Date.Before(since) {
			out = append(out, m)
		}
	}
	return out, nil
}

func (s *fakeSession) Idle(ctx context.Context) error {
	select {
	case <-s.wake:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *fakeSession) Close() error {
	s.server.mu.Lock()
	delete(s.server.wakes, s.wake)
	s.server.mu.Unlock()
	s.server.closes.Add(1)
	return nil
}

// testEmail is the inbox rows under test; the auth passes validateInbox but
// is never dialed for real.
func testEmail(id string) Email {
	return Email{
		ID:      id,
		Address: id + "@example.com",
		Inbox:   &Inbox{Vendor: Gmail, Auth: Auth{Kind: AuthPassword, Password: "app-pass"}},
	}
}

func newTestManager(t *testing.T, repo Repository, server *fakeServer, opts ...Option) *Manager {
	t.Helper()
	m, err := NewManager(context.Background(), repo, append([]Option{WithDialer(server.dial)}, opts...)...)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	t.Cleanup(func() { m.Close() })
	return m
}

func addEmail(t *testing.T, m *Manager, e Email) {
	t.Helper()
	if err := m.Add(context.Background(), e); err != nil {
		t.Fatalf("add %s: %v", e.ID, err)
	}
}

// recv fails the test unless the subscription yields a message in time.
func recv(t *testing.T, sub Subscription) Message {
	t.Helper()
	select {
	case msg, ok := <-sub.C():
		if !ok {
			t.Fatalf("subscription closed while a message was expected")
		}
		return msg
	case <-time.After(2 * time.Second):
		t.Fatalf("no message within 2s")
	}
	return Message{}
}

// assertQuiet fails the test if the subscription yields anything soon.
func assertQuiet(t *testing.T, sub Subscription) {
	t.Helper()
	select {
	case msg, ok := <-sub.C():
		if ok {
			t.Fatalf("unexpected message %q from %s", msg.Subject, msg.From)
		}
	case <-time.After(50 * time.Millisecond):
	}
}

// eventually polls cond until it holds or the deadline passes, for state the
// listener goroutine settles asynchronously (cursor saves, teardowns).
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("never observed: %s", what)
}

// awaitListening waits until the listener's first SELECT persisted the
// mailbox's validity — the moment "listen from now" begins. Mail delivered
// before it is deliberately behind the fresh cursor.
func awaitListening(t *testing.T, repo *memRepo, emailID string, uidValidity uint32) {
	t.Helper()
	eventually(t, "listener established", func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		e, ok := repo.rows[emailID]
		return ok && e.Inbox != nil && e.Inbox.UIDValidity == uidValidity
	})
}

// TestEveryListeningTaskReceivesTheMessage verifies the headline guarantee:
// delivery is fan-out, never first-taker-wins. Many tasks share one
// forwarding inbox, and every one of them gets the mail.
func TestEveryListeningTaskReceivesTheMessage(t *testing.T) {
	server := newFakeServer()
	repo := newMemRepo()
	m := newTestManager(t, repo, server)
	addEmail(t, m, testEmail("inbox-1"))

	subs := make([]Subscription, 3)
	for i := range subs {
		sub, err := m.Listen(context.Background(), fmt.Sprintf("t%d", i), "inbox-1")
		if err != nil {
			t.Fatalf("listen %d: %v", i, err)
		}
		defer sub.Close()
		subs[i] = sub
	}
	awaitListening(t, repo, "inbox-1", 1)

	server.deliver(Message{From: "no-reply@store.example", Subject: "order confirmed"})

	for i, sub := range subs {
		msg := recv(t, sub)
		if msg.Subject != "order confirmed" || msg.EmailID != "inbox-1" {
			t.Fatalf("sub %d got %+v, want the delivered message stamped with its inbox", i, msg)
		}
	}
}

// TestSenderFilterIsPerSubscription verifies filtered and unfiltered
// subscribers coexist on one inbox: the filter narrows one task's feed, not
// the inbox's.
func TestSenderFilterIsPerSubscription(t *testing.T) {
	server := newFakeServer()
	repo := newMemRepo()
	m := newTestManager(t, repo, server)
	addEmail(t, m, testEmail("inbox-1"))

	filtered, err := m.Listen(context.Background(), "t-filtered", "inbox-1", FromSender("Wanted@Store.example"))
	if err != nil {
		t.Fatalf("listen filtered: %v", err)
	}
	defer filtered.Close()
	all, err := m.Listen(context.Background(), "t-all", "inbox-1")
	if err != nil {
		t.Fatalf("listen all: %v", err)
	}
	defer all.Close()
	awaitListening(t, repo, "inbox-1", 1)

	server.deliver(Message{From: "noise@elsewhere.example", Subject: "noise"})
	if msg := recv(t, all); msg.Subject != "noise" {
		t.Fatalf("unfiltered got %q, want noise", msg.Subject)
	}
	assertQuiet(t, filtered)

	server.deliver(Message{From: "wanted@store.example", Subject: "signal"})
	if msg := recv(t, filtered); msg.Subject != "signal" {
		t.Fatalf("filtered got %q, want signal", msg.Subject)
	}
	if msg := recv(t, all); msg.Subject != "signal" {
		t.Fatalf("unfiltered got %q, want signal too", msg.Subject)
	}
}

// TestListenerIsEstablishedLazilyAndOnlyOnce verifies no connection exists
// before the first Listen and racing Listens coalesce onto one connection —
// one listener per inbox is the design's core resource bound.
func TestListenerIsEstablishedLazilyAndOnlyOnce(t *testing.T) {
	server := newFakeServer()
	repo := newMemRepo()
	m := newTestManager(t, repo, server)
	addEmail(t, m, testEmail("inbox-1"))

	if n := server.dials.Load(); n != 0 {
		t.Fatalf("dials before any Listen = %d, want 0", n)
	}

	var wg sync.WaitGroup
	subs := make([]Subscription, 8)
	for i := range subs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sub, err := m.Listen(context.Background(), fmt.Sprintf("t%d", i), "inbox-1")
			if err != nil {
				t.Errorf("listen %d: %v", i, err)
				return
			}
			subs[i] = sub
		}(i)
	}
	wg.Wait()
	defer func() {
		for _, sub := range subs {
			if sub != nil {
				sub.Close()
			}
		}
	}()
	awaitListening(t, repo, "inbox-1", 1)

	server.deliver(Message{Subject: "hello"})
	for _, sub := range subs {
		recv(t, sub)
	}
	if n := server.dials.Load(); n != 1 {
		t.Fatalf("dials after %d concurrent Listens = %d, want 1", len(subs), n)
	}
}

// TestLastCloseTearsDownTheListener verifies the refcount teardown: the
// last unsubscribe hangs up the connection and empties the registry, and
// the next Listen dials fresh.
func TestLastCloseTearsDownTheListener(t *testing.T) {
	server := newFakeServer()
	repo := newMemRepo()
	m := newTestManager(t, repo, server)
	addEmail(t, m, testEmail("inbox-1"))

	first, err := m.Listen(context.Background(), "t1", "inbox-1")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	second, err := m.Listen(context.Background(), "t2", "inbox-1")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	awaitListening(t, repo, "inbox-1", 1)

	first.Close()
	server.deliver(Message{Subject: "still-served"})
	recv(t, second)

	second.Close()
	eventually(t, "session closed", func() bool { return server.closes.Load() >= 1 })
	m.mu.Lock()
	remaining := len(m.listeners)
	m.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("listeners after last close = %d, want 0", remaining)
	}

	relisten, err := m.Listen(context.Background(), "t3", "inbox-1")
	if err != nil {
		t.Fatalf("re-listen: %v", err)
	}
	defer relisten.Close()
	eventually(t, "a fresh dial", func() bool { return server.dials.Load() >= 2 })
}

// TestCursorSurvivesRestart verifies mail consumed before a shutdown is not
// re-delivered after one — the cursor is the only thing persisted, and it
// is enough.
func TestCursorSurvivesRestart(t *testing.T) {
	server := newFakeServer()
	repo := newMemRepo()
	m := newTestManager(t, repo, server)
	addEmail(t, m, testEmail("inbox-1"))

	sub, err := m.Listen(context.Background(), "t1", "inbox-1")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	awaitListening(t, repo, "inbox-1", 1)
	server.deliver(Message{Subject: "before-restart"})
	recv(t, sub)
	eventually(t, "cursor persisted", func() bool {
		e := repo.get(t, "inbox-1")
		return e.Inbox != nil && e.Inbox.LastUID == 1
	})
	sub.Close()
	m.Close()

	restarted := newTestManager(t, repo, server)
	resub, err := restarted.Listen(context.Background(), "t1", "inbox-1")
	if err != nil {
		t.Fatalf("re-listen: %v", err)
	}
	defer resub.Close()
	assertQuiet(t, resub)

	server.deliver(Message{Subject: "after-restart"})
	if msg := recv(t, resub); msg.Subject != "after-restart" {
		t.Fatalf("got %q, want only the new message", msg.Subject)
	}
}

// TestUIDValidityChangeResetsInsteadOfReplaying verifies a renumbered
// mailbox jumps the cursor to the present: replaying the whole mailbox
// under new UIDs would flood every subscriber with ancient mail.
func TestUIDValidityChangeResetsInsteadOfReplaying(t *testing.T) {
	server := newFakeServer()
	repo := newMemRepo()
	m := newTestManager(t, repo, server)
	addEmail(t, m, testEmail("inbox-1"))

	sub, err := m.Listen(context.Background(), "t1", "inbox-1")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	awaitListening(t, repo, "inbox-1", 1)
	server.deliver(Message{Subject: "old-world"})
	recv(t, sub)
	sub.Close()
	m.Close()

	// The server renumbers: same mail, new validity, new UID space.
	server.mu.Lock()
	server.uidValidity = 2
	server.mu.Unlock()

	restarted := newTestManager(t, repo, server)
	resub, err := restarted.Listen(context.Background(), "t1", "inbox-1")
	if err != nil {
		t.Fatalf("re-listen: %v", err)
	}
	defer resub.Close()
	assertQuiet(t, resub)
	eventually(t, "reset cursor persisted", func() bool {
		e := repo.get(t, "inbox-1")
		return e.Inbox != nil && e.Inbox.UIDValidity == 2
	})
}

// TestBackfillRedeliversServerHistory verifies WithBackfill replays what
// the server still holds, because a recovered task must catch mail that
// arrived while it was down without this package storing bodies.
func TestBackfillRedeliversServerHistory(t *testing.T) {
	server := newFakeServer()
	repo := newMemRepo()
	m := newTestManager(t, repo, server)
	addEmail(t, m, testEmail("inbox-1"))

	yesterday := time.Now().Add(-24 * time.Hour)
	server.deliver(Message{Subject: "ancient", Date: yesterday.Add(-time.Hour)})
	server.deliver(Message{Subject: "missed", Date: time.Now()})

	sub, err := m.Listen(context.Background(), "t1", "inbox-1", WithBackfill(yesterday))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer sub.Close()

	// The live cursor starts at the mailbox's present, so what arrives is
	// backfill alone — and only from the requested window.
	if msg := recv(t, sub); msg.Subject != "missed" {
		t.Fatalf("backfill delivered %q, want missed", msg.Subject)
	}
	assertQuiet(t, sub)
}

// TestAddressOnlyEmailRefusesToListen verifies the two halves of an
// address-only email: readable as data, unlistenable as an inbox.
func TestAddressOnlyEmailRefusesToListen(t *testing.T) {
	server := newFakeServer()
	m := newTestManager(t, newMemRepo(), server)
	addressOnly := Email{ID: "addr-1", Address: "plus-tag@example.com"}
	addEmail(t, m, addressOnly)

	got, err := m.Get(context.Background(), "addr-1")
	if err != nil || got.Address != "plus-tag@example.com" {
		t.Fatalf("get = %+v, %v; want the address readable", got, err)
	}
	if _, err := m.Listen(context.Background(), "t1", "addr-1"); !errors.Is(err, ErrNoInbox) {
		t.Fatalf("listen err = %v, want ErrNoInbox", err)
	}
	if _, err := m.Listen(context.Background(), "t1", "no-such"); !errors.Is(err, ErrEmailNotFound) {
		t.Fatalf("listen unknown err = %v, want ErrEmailNotFound", err)
	}
}

// TestDeleteRefusesWhileTasksAreListening verifies the subscription side of
// the delete policy: active listeners block and are named, and the freed
// email deletes cleanly afterwards.
func TestDeleteRefusesWhileTasksAreListening(t *testing.T) {
	server := newFakeServer()
	m := newTestManager(t, newMemRepo(), server)
	addEmail(t, m, testEmail("inbox-1"))

	sub, err := m.Listen(context.Background(), "t1", "inbox-1")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	if _, err := m.Delete(context.Background(), "inbox-1"); !errors.Is(err, ErrEmailInUse) {
		t.Fatalf("delete err = %v, want ErrEmailInUse", err)
	} else if !contains(err.Error(), "t1") {
		t.Fatalf("refusal %q does not name the listening task", err)
	}

	sub.Close()
	if _, err := m.Delete(context.Background(), "inbox-1"); err != nil {
		t.Fatalf("delete after close: %v", err)
	}
}

// TestDeleteRefusesWhileAReferencingAccountIsHeld verifies the guard side
// of the delete policy — the same policy leasing institutes for resources
// under running tasks: live leases block, idle durable locks report as
// stranded rather than blocking. The guard here is a fake; accounts covers
// installing the real one through WithEmail.
func TestDeleteRefusesWhileAReferencingAccountIsHeld(t *testing.T) {
	server := newFakeServer()
	held := []string{"t-running"}
	guard := func(emailID string) ([]string, []string) {
		if emailID != "inbox-1" {
			return nil, nil
		}
		return held, []string{"t-suspended"}
	}
	m := newTestManager(t, newMemRepo(), server)
	m.GuardDeletes(guard)
	addEmail(t, m, testEmail("inbox-1"))

	if _, err := m.Delete(context.Background(), "inbox-1"); !errors.Is(err, ErrEmailInUse) {
		t.Fatalf("delete err = %v, want ErrEmailInUse", err)
	} else if !contains(err.Error(), "t-running") {
		t.Fatalf("refusal %q does not name the holding task", err)
	}

	held = nil
	stranded, err := m.Delete(context.Background(), "inbox-1")
	if err != nil {
		t.Fatalf("delete with only idle locks: %v", err)
	}
	if len(stranded) != 1 || stranded[0] != "t-suspended" {
		t.Fatalf("stranded = %v, want the idle-locked task reported", stranded)
	}
}

// TestSlowSubscriberDropsWithoutStallingPeers verifies one full buffer
// costs only its own subscriber: peers still receive everything, and the
// drop is surfaced, not silent.
func TestSlowSubscriberDropsWithoutStallingPeers(t *testing.T) {
	server := newFakeServer()
	var droppedTask string
	var droppedTotal uint64
	var dropMu sync.Mutex
	onDrop := func(emailID, taskID string, dropped uint64) {
		dropMu.Lock()
		droppedTask, droppedTotal = taskID, dropped
		dropMu.Unlock()
	}
	repo := newMemRepo()
	m := newTestManager(t, repo, server, WithDropHandler(onDrop))
	addEmail(t, m, testEmail("inbox-1"))

	slow, err := m.Listen(context.Background(), "t-slow", "inbox-1", WithBuffer(1))
	if err != nil {
		t.Fatalf("listen slow: %v", err)
	}
	defer slow.Close()
	fast, err := m.Listen(context.Background(), "t-fast", "inbox-1")
	if err != nil {
		t.Fatalf("listen fast: %v", err)
	}
	defer fast.Close()
	awaitListening(t, repo, "inbox-1", 1)

	for i := 0; i < 3; i++ {
		server.deliver(Message{Subject: fmt.Sprintf("m%d", i)})
		if msg := recv(t, fast); msg.Subject != fmt.Sprintf("m%d", i) {
			t.Fatalf("fast got %q, want m%d in order", msg.Subject, i)
		}
	}

	eventually(t, "a reported drop", func() bool {
		dropMu.Lock()
		defer dropMu.Unlock()
		return droppedTask == "t-slow" && droppedTotal >= 1
	})
	if msg := recv(t, slow); msg.Subject != "m0" {
		t.Fatalf("slow kept %q, want the first undropped message m0", msg.Subject)
	}
}

func contains(s, sub string) bool {
	return strings.Contains(s, sub)
}
