// Command example runs the checkout workflow end-to-end as a real task: it spins
// a canned test site and a local forward proxy, registers the workflow on a task
// manager backed by an in-memory repository, leases a proxy from a round-robin
// proxy manager and locks a site account from an account manager, then creates
// and starts one task, printing each state it checkpoints through and each
// request the proxy forwards.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	"github.com/ntakezo/rogojin/leasing"

	example_checkout "github.com/ntakezo/rogojin/_examples/workflows/example/checkout"
	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/memory"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/tasks"
)

func main() {
	ctx := context.Background()

	// The mail server stands in for the vendor: the site's login handler
	// "sends" the verification mail by delivering into it, and the email
	// manager's listeners dial it instead of imap.gmail.com.
	mailServer := newMemMailServer()

	site := newSite(mailServer.deliver)
	defer site.Close()

	forward := newForwardProxy()
	defer forward.Close()

	// Each manager stands alone: it guards its own pool from the leases and
	// locks it owns and asks nothing of the task manager.
	proxyRepo := memory.NewProxies()
	if err := proxyRepo.Save(ctx, proxies.Proxy{Resource: leasing.Resource{ID: "local-1", GroupID: proxies.GlobalGroup}, URL: forward.URL}); err != nil {
		log.Fatalf("seed proxy: %v", err)
	}
	proxyManager, err := proxies.NewManager(ctx, logProxyStats{proxyRepo})
	if err != nil {
		log.Fatalf("proxy manager: %v", err)
	}

	// Email is not a leased resource — no groups, no rotation, no locks. The
	// inventory holds the forwarding inboxes accounts point at; a workflow
	// reaches its inbox through its locked account (ForwardingEmail, then
	// Listen with a sender filter and a backfill window). The nil repository
	// runs the inventory in memory — this example wants no rows surviving it —
	// where the proxy and task repositories below are written out because they
	// do something with their writes.
	emailManager, err := email.NewManager(ctx, nil, email.WithDialer(mailServer.dial))
	if err != nil {
		log.Fatalf("email manager: %v", err)
	}
	defer emailManager.Close()
	if err := emailManager.Add(ctx, email.Email{
		ID:      "inbox-1",
		Address: "orders@example.com",
		Inbox:   &email.Inbox{Vendor: email.Gmail, Auth: email.Auth{Kind: email.AuthPassword, Password: "app-password"}},
	}); err != nil {
		log.Fatalf("add email: %v", err)
	}

	// Accounts are the same machinery minus the rotation knob. The account
	// group named "global" is never confused with the proxy group of the same
	// name: a kind resolves only against its own manager. EmailID is the
	// account's guaranteed field — its forwarding inbox — and WithEmail
	// closes the referential loop: an inbox a held or locked account forwards
	// to refuses deletion, exactly like a leased resource would.
	accountManager, err := accounts.NewManager(ctx, nil, accounts.WithEmail(emailManager))
	if err != nil {
		log.Fatalf("account manager: %v", err)
	}
	if err := accountManager.Add(ctx, accounts.Account{
		Resource: leasing.Resource{ID: "buyer-1", GroupID: accounts.GlobalGroup},
		EmailID:  "inbox-1",
		Fields:   profileFields(example_checkout.Profile{Email: "buyer@example.com", Name: "Buyer", Address: "1 Example St"}),
	}); err != nil {
		log.Fatalf("add account: %v", err)
	}

	// Both kinds of lock outlive the process, so each manager registers under
	// its kind: deleting a task unlocks both, while repointing one drops only
	// the lock that moved — a task sent to other proxies must keep the account
	// it is halfway through a checkout as. Each manager enters the system
	// exactly once, here: RegisterWorkflow hands them to the workflow through
	// UseResources, so the instance a task leases through is the instance a
	// deletion unlocks through.
	taskManager, err := tasks.NewManager(ctx, logCheckpoints{memory.NewTasks()}, comms.NewBus(),
		tasks.WithResource(proxies.Kind, proxyManager),
		tasks.WithResource(accounts.Kind, accountManager))
	if err != nil {
		log.Fatalf("task manager: %v", err)
	}
	checkout := example_checkout.New(emailManager)
	if err := taskManager.RegisterWorkflow(example_checkout.Name, checkout); err != nil {
		log.Fatalf("register workflow: %v", err)
	}

	input := example_checkout.Input{
		ProductURL: site.URL + "/product",
		Size:       "M",
	}

	// Placement, one option per kind: the workflow reads both back off Deps.
	// Creating against the module infers the input and output types, so the
	// task handle's Start and Output carry a decoded Order.
	task, err := tasks.Create(ctx, taskManager, checkout, input,
		tasks.WithResourceGroup(proxies.Kind, proxies.GlobalGroup),
		tasks.WithResourceGroup(accounts.Kind, accounts.GlobalGroup))
	if err != nil {
		log.Fatalf("create task: %v", err)
	}
	fmt.Printf("created task %s (status %q) against %s\n", task.ID, task.Status, site.URL)

	order, err := task.Start(ctx)
	if err != nil {
		log.Fatalf("start task: %v", err)
	}
	fmt.Printf("task %s finished with status %q: order %s is %s\n", task.ID, task.Status, order.OrderID, order.Status)
}

// newForwardProxy serves a minimal HTTP forward proxy so the workflow's traffic
// demonstrably routes through the leased proxy.
func newForwardProxy() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Printf("  proxy forwarding %s %s\n", r.Method, r.URL)
		r.RequestURI = ""
		resp, err := http.DefaultTransport.RoundTrip(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
	}))
}

// newSite serves the canned product, login, cart, and checkout responses the
// workflow drives against. The login handler answers by mail: it delivers
// the verification message — link and all — into the forwarding inbox.
func newSite(deliver func(email.Message)) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/product", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"variantID": "variant-M", "csrfToken": "csrf-abc"})
	})
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		deliver(email.Message{
			From:    example_checkout.VerificationSender,
			Subject: "confirm your sign-in",
			Text:    "Follow http://" + r.Host + "/follow?token=tok-123 to finish signing in.",
			Date:    time.Now(),
		})
		json.NewEncoder(w).Encode(map[string]string{"status": "mail-sent"})
	})
	mux.HandleFunc("/follow", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"status": "verified"})
	})
	mux.HandleFunc("/cart", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"cartID": "cart-123"})
	})
	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"orderID": "order-999", "status": "confirmed"})
	})
	return httptest.NewServer(mux)
}

// memMailServer stands in for the vendor's IMAP host: it holds the inbox's
// mail and serves email.Mailbox sessions to the manager's listeners and
// backfills, waking idlers as mail lands.
type memMailServer struct {
	mu      sync.Mutex
	nextUID uint32
	msgs    []email.Message
	wakes   map[chan struct{}]struct{}
}

func newMemMailServer() *memMailServer {
	return &memMailServer{nextUID: 1, wakes: make(map[chan struct{}]struct{})}
}

// deliver lands one message on the server and wakes every idling session.
func (s *memMailServer) deliver(msg email.Message) {
	s.mu.Lock()
	msg.UID = s.nextUID
	s.nextUID++
	if msg.Date.IsZero() {
		msg.Date = time.Now()
	}
	s.msgs = append(s.msgs, msg)
	for wake := range s.wakes {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	s.mu.Unlock()
}

func (s *memMailServer) dial(ctx context.Context, e email.Email) (email.Mailbox, error) {
	wake := make(chan struct{}, 1)
	s.mu.Lock()
	s.wakes[wake] = struct{}{}
	s.mu.Unlock()
	return &mailSession{server: s, wake: wake}, nil
}

type mailSession struct {
	server *memMailServer
	wake   chan struct{}
}

func (m *mailSession) Select(ctx context.Context) (uint32, uint32, error) {
	m.server.mu.Lock()
	defer m.server.mu.Unlock()
	return 1, m.server.nextUID - 1, nil
}

func (m *mailSession) FetchSince(ctx context.Context, uid uint32) ([]email.Message, error) {
	m.server.mu.Lock()
	defer m.server.mu.Unlock()
	out := make([]email.Message, 0)
	for _, msg := range m.server.msgs {
		if msg.UID > uid {
			out = append(out, msg)
		}
	}
	return out, nil
}

func (m *mailSession) FetchSinceDate(ctx context.Context, since time.Time) ([]email.Message, error) {
	m.server.mu.Lock()
	defer m.server.mu.Unlock()
	out := make([]email.Message, 0)
	for _, msg := range m.server.msgs {
		if !msg.Date.Before(since) {
			out = append(out, msg)
		}
	}
	return out, nil
}

func (m *mailSession) Idle(ctx context.Context) error {
	select {
	case <-m.wake:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *mailSession) Close() error {
	m.server.mu.Lock()
	delete(m.server.wakes, m.wake)
	m.server.mu.Unlock()
	return nil
}

// logProxyStats prints each stat write the manager persists, so the leased
// proxy's rotation feedback stays visible in the example's output.
type logProxyStats struct{ proxies.Repository }

func (l logProxyStats) Save(ctx context.Context, p proxies.Proxy) error {
	fmt.Printf("  proxy %s stats now %d/%d\n", p.ID, p.Successes, p.Failures)
	return l.Repository.Save(ctx, p)
}

// logCheckpoints prints each state the task checkpoints through on its way
// into the store.
type logCheckpoints struct{ tasks.Repository }

func (l logCheckpoints) SaveCheckpoint(ctx context.Context, id string, version int64, node, status, state string, snapshot []byte) (int64, error) {
	fmt.Printf("  checkpoint %-16s [%s] snapshot=%s\n", state, status, snapshot)
	return l.Repository.SaveCheckpoint(ctx, id, version, node, status, state, snapshot)
}

// profileFields marshals this workflow's account shape into the opaque JSON the
// accounts module stores. Another workflow's accounts would carry other fields
// entirely, against the same store.
func profileFields(p example_checkout.Profile) json.RawMessage {
	raw, err := json.Marshal(p)
	if err != nil {
		log.Fatalf("encode account fields: %v", err)
	}
	return raw
}
