// Command example runs the checkout workflow end-to-end as a real task: it spins
// a canned test site and a local forward proxy, registers the workflow on a task
// service backed by an in-memory repository, leases a proxy from a round-robin
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

	example_checkout "github.com/ntakezo/rogojin/_examples/workflows/example/checkout"
	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/states"
	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/tasks"
)

func main() {
	ctx := context.Background()

	site := newSite()
	defer site.Close()

	forward := newForwardProxy()
	defer forward.Close()

	// Each manager stands alone: it guards its own pool from the leases and
	// locks it owns and asks nothing of the task service.
	manager, err := proxies.NewManager(ctx, newMemProxyRepo(proxies.Proxy{ID: "local-1", GroupID: proxies.GlobalGroup, Attrs: proxies.Attrs{URL: forward.URL}}))
	if err != nil {
		log.Fatalf("proxy manager: %v", err)
	}

	// Email is not a leased resource — no groups, no rotation, no locks. The
	// inventory holds the forwarding inboxes accounts point at; a workflow
	// reaches its inbox through its locked account (ForwardingEmail, then
	// Listen with a sender filter and a backfill window).
	emailManager, err := email.NewManager(ctx, newMemEmailRepo(email.Email{
		ID:      "inbox-1",
		Address: "orders@example.com",
		Inbox:   &email.Inbox{Vendor: email.Gmail, Auth: email.Auth{Kind: email.AuthPassword, Password: "app-password"}},
	}))
	if err != nil {
		log.Fatalf("email manager: %v", err)
	}
	defer emailManager.Close()

	// Accounts are the same machinery minus the rotation knob. The account
	// group named "global" is never confused with the proxy group of the same
	// name: a kind resolves only against its own manager. EmailID is the
	// account's guaranteed field — its forwarding inbox — and WithEmail
	// closes the referential loop: an inbox a held or locked account forwards
	// to refuses deletion, exactly like a leased resource would.
	accountManager, err := accounts.NewManager(ctx, newMemAccountRepo(accounts.Account{
		ID:      "buyer-1",
		GroupID: accounts.GlobalGroup,
		Attrs: accounts.Attrs{
			EmailID: "inbox-1",
			Fields:  profileFields(states.Profile{Email: "buyer@example.com", Name: "Buyer", Address: "1 Example St"}),
		},
	}), accounts.WithEmail(emailManager))
	if err != nil {
		log.Fatalf("account manager: %v", err)
	}

	// Both kinds of lock outlive the process, so each manager registers under
	// its kind: deleting a task unlocks both, while repointing one drops only
	// the lock that moved — a task sent to other proxies must keep the account
	// it is halfway through a checkout as.
	svc := tasks.NewService(newMemRepo(), comms.NewBus(),
		tasks.WithResource(states.ProxyKind, manager),
		tasks.WithResource(states.AccountKind, accountManager))
	if err := svc.RegisterWorkflow(example_checkout.Name, example_checkout.New(manager, accountManager)); err != nil {
		log.Fatalf("register workflow: %v", err)
	}

	input := states.StaticContext{
		ProductURL: site.URL + "/product",
		Size:       "M",
	}

	// Placement, one option per kind: the workflow reads both back off Deps.
	task, err := svc.CreateTask(ctx, example_checkout.Name, input,
		tasks.WithResourceGroup(states.ProxyKind, proxies.GlobalGroup),
		tasks.WithResourceGroup(states.AccountKind, accounts.GlobalGroup))
	if err != nil {
		log.Fatalf("create task: %v", err)
	}
	fmt.Printf("created task %s (status %q) against %s\n", task.ID(), task.Status(), site.URL)

	output, err := task.Start(ctx)
	if err != nil {
		log.Fatalf("start task: %v", err)
	}
	fmt.Printf("task %s finished with status %q, output %s\n", task.ID(), task.Status(), output)
}

// memEmailRepo is a minimal in-memory email.Repository; the manager owns all
// listener state, so this only stores the inventory and its cursors.
type memEmailRepo struct {
	mu      sync.Mutex
	records map[string]email.Email
	order   []string
}

func newMemEmailRepo(seed ...email.Email) *memEmailRepo {
	r := &memEmailRepo{records: make(map[string]email.Email)}
	for _, e := range seed {
		r.records[e.ID] = e
		r.order = append(r.order, e.ID)
	}
	return r
}

func (r *memEmailRepo) List(ctx context.Context) ([]email.Email, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]email.Email, 0, len(r.order))
	for _, id := range r.order {
		if e, ok := r.records[id]; ok {
			out = append(out, e)
		}
	}
	return out, nil
}

func (r *memEmailRepo) Save(ctx context.Context, e email.Email) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.records[e.ID]; !ok {
		r.order = append(r.order, e.ID)
	}
	r.records[e.ID] = e
	return nil
}

func (r *memEmailRepo) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, id)
	return nil
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

// newSite serves the canned product, cart, and checkout responses the workflow drives against.
func newSite() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/product", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"variantID": "variant-M", "csrfToken": "csrf-abc"})
	})
	mux.HandleFunc("/cart", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"cartID": "cart-123"})
	})
	mux.HandleFunc("/checkout", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{"orderID": "order-999", "status": "confirmed"})
	})
	return httptest.NewServer(mux)
}

// memProxyRepo is a minimal in-memory proxies.Repository seeded with a fixed
// pool; the manager owns all live lease state, so this only stores the records.
type memProxyRepo struct {
	mu      sync.Mutex
	records map[string]proxies.Proxy
	order   []string
	groups  map[string]proxies.Group
}

func newMemProxyRepo(seed ...proxies.Proxy) *memProxyRepo {
	r := &memProxyRepo{records: make(map[string]proxies.Proxy), groups: make(map[string]proxies.Group)}
	for _, p := range seed {
		r.records[p.ID] = p
		r.order = append(r.order, p.ID)
	}
	return r
}

func (r *memProxyRepo) List(ctx context.Context) ([]proxies.Proxy, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]proxies.Proxy, 0, len(r.order))
	for _, id := range r.order {
		if p, ok := r.records[id]; ok {
			out = append(out, p)
		}
	}
	return out, nil
}

func (r *memProxyRepo) Save(ctx context.Context, p proxies.Proxy) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.records[p.ID]; !ok {
		r.order = append(r.order, p.ID)
	}
	r.records[p.ID] = p
	fmt.Printf("  proxy %s stats now %d/%d\n", p.ID, p.Successes, p.Failures)
	return nil
}

func (r *memProxyRepo) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, id)
	return nil
}

func (r *memProxyRepo) ListGroups(ctx context.Context) ([]proxies.Group, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]proxies.Group, 0, len(r.groups))
	for _, g := range r.groups {
		out = append(out, g)
	}
	return out, nil
}

func (r *memProxyRepo) SaveGroup(ctx context.Context, g proxies.Group) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groups[g.ID] = g
	return nil
}

func (r *memProxyRepo) DeleteGroup(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.groups, id)
	return nil
}

// profileFields marshals this workflow's account shape into the opaque JSON the
// accounts module stores. Another workflow's accounts would carry other fields
// entirely, against the same store.
func profileFields(p states.Profile) json.RawMessage {
	raw, err := json.Marshal(p)
	if err != nil {
		log.Fatalf("encode account fields: %v", err)
	}
	return raw
}

// memAccountRepo is a minimal in-memory accounts.Repository seeded with a fixed
// set of logins; the manager owns all live lease state, so this only stores the
// records.
type memAccountRepo struct {
	mu      sync.Mutex
	records map[string]accounts.Account
	order   []string
	groups  map[string]accounts.Group
}

func newMemAccountRepo(seed ...accounts.Account) *memAccountRepo {
	r := &memAccountRepo{records: make(map[string]accounts.Account), groups: make(map[string]accounts.Group)}
	for _, a := range seed {
		r.records[a.ID] = a
		r.order = append(r.order, a.ID)
	}
	return r
}

func (r *memAccountRepo) List(ctx context.Context) ([]accounts.Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]accounts.Account, 0, len(r.order))
	for _, id := range r.order {
		if a, ok := r.records[id]; ok {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *memAccountRepo) Save(ctx context.Context, a accounts.Account) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.records[a.ID]; !ok {
		r.order = append(r.order, a.ID)
	}
	r.records[a.ID] = a
	return nil
}

func (r *memAccountRepo) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, id)
	return nil
}

func (r *memAccountRepo) ListGroups(ctx context.Context) ([]accounts.Group, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]accounts.Group, 0, len(r.groups))
	for _, g := range r.groups {
		out = append(out, g)
	}
	return out, nil
}

func (r *memAccountRepo) SaveGroup(ctx context.Context, g accounts.Group) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groups[g.ID] = g
	return nil
}

func (r *memAccountRepo) DeleteGroup(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.groups, id)
	return nil
}

// memRepo is a minimal in-memory Repository: a dumb byte store that records each
// task's last checkpoint and prints the states it advances through.
type memRepo struct {
	mu      sync.Mutex
	records map[string]tasks.Record
	groups  map[string]tasks.Group
}

func newMemRepo() *memRepo {
	return &memRepo{records: make(map[string]tasks.Record), groups: make(map[string]tasks.Group)}
}

func (r *memRepo) CreateTask(ctx context.Context, rec tasks.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records[rec.ID] = rec
	return nil
}

func (r *memRepo) SaveCheckpoint(ctx context.Context, id, status, state string, snapshot []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[id]
	rec.Status, rec.State, rec.Snapshot = status, state, snapshot
	r.records[id] = rec
	fmt.Printf("  checkpoint %-16s [%s] snapshot=%s\n", state, status, snapshot)
	return nil
}

func (r *memRepo) MarkTerminal(ctx context.Context, id, outcome string, output []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec := r.records[id]
	rec.Status, rec.Output = outcome, output
	r.records[id] = rec
	return nil
}

func (r *memRepo) RecoverTask(ctx context.Context, id string) (tasks.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[id]
	if !ok {
		return tasks.Record{}, fmt.Errorf("task %s not found", id)
	}
	return rec, nil
}

func (r *memRepo) RecoverAll(ctx context.Context) ([]tasks.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]tasks.Record, 0, len(r.records))
	for _, rec := range r.records {
		out = append(out, rec)
	}
	return out, nil
}

func (r *memRepo) DeleteTask(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, id)
	return nil
}

func (r *memRepo) SaveGroup(ctx context.Context, g tasks.Group) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groups[g.ID] = g
	return nil
}

func (r *memRepo) GetGroup(ctx context.Context, id string) (tasks.Group, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[id]
	return g, ok, nil
}

func (r *memRepo) ListGroups(ctx context.Context) ([]tasks.Group, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]tasks.Group, 0, len(r.groups))
	for _, g := range r.groups {
		out = append(out, g)
	}
	return out, nil
}

func (r *memRepo) DeleteGroup(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.groups, id)
	return nil
}

func (r *memRepo) TasksInGroup(ctx context.Context, groupID string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0)
	for id, rec := range r.records {
		if rec.GroupID == groupID {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// SaveAssignment rewrites one kind and copies the rest, so repointing a task's
// proxies leaves its account placement alone.
func (r *memRepo) SaveAssignment(ctx context.Context, id string, kind string, a tasks.Assignment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.records[id]
	if !ok {
		return fmt.Errorf("task %s not found", id)
	}
	assignments := make(map[string]tasks.Assignment, len(rec.Assignments)+1)
	for k, v := range rec.Assignments {
		assignments[k] = v
	}
	assignments[kind] = a
	rec.Assignments = assignments
	r.records[id] = rec
	return nil
}

func (r *memRepo) TasksPinnedTo(ctx context.Context, kind, resourceID string) ([]tasks.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pinned := make([]tasks.Record, 0)
	for _, rec := range r.records {
		if pin := rec.Assignments[kind].ResourceID; pin != nil && *pin == resourceID {
			pinned = append(pinned, rec)
		}
	}
	return pinned, nil
}
