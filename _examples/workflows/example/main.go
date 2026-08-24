// Command example runs the checkout workflow end-to-end as a real task: it spins
// a canned test site and a local forward proxy, registers the workflow on a task
// service backed by an in-memory repository, leases a proxy from a round-robin
// proxy manager, then creates and starts one task, printing each state it
// checkpoints through and each request the proxy forwards.
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
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/tasks"
)

func main() {
	ctx := context.Background()

	site := newSite()
	defer site.Close()

	forward := newForwardProxy()
	defer forward.Close()

	// The manager is built first but the task service answers "is this in use?",
	// so the usage guard closes over svc and resolves it at call time.
	var svc tasks.Service
	manager, err := proxies.NewManager(ctx, newMemProxyRepo(proxies.Proxy{ID: "local-1", URL: forward.URL}), nil,
		proxies.WithUsagePolicy(proxies.Usage{
			RunningInGroup: func(ctx context.Context, groupID string) ([]string, error) {
				return svc.RunningTasks(ctx, groupID)
			},
			TaskRunning: func(ctx context.Context, taskID string) (bool, error) {
				return svc.TaskIsRunning(ctx, taskID)
			},
			PinnedToProxy: func(ctx context.Context, proxyID string) ([]string, error) {
				return svc.PinnedTasks(ctx, proxyID)
			},
		}))
	if err != nil {
		log.Fatalf("proxy manager: %v", err)
	}

	// Proxy locks outlive the process, so deleting a task must free its own —
	// and repointing a task must drop the lock its new placement no longer fits.
	svc = tasks.NewService(newMemRepo(), comms.NewBus(),
		tasks.WithTaskReleaser(manager.Unlock),
		tasks.WithTaskReassigner(func(ctx context.Context, taskID, proxyGroupID, proxyID string) error {
			return manager.ReleaseStaleLock(ctx, proxies.Assignment{TaskID: taskID, GroupID: proxyGroupID, ProxyID: proxyID})
		}))
	if err := svc.RegisterWorkflow(example_checkout.Name, example_checkout.New(manager)); err != nil {
		log.Fatalf("register workflow: %v", err)
	}

	input := states.StaticContext{
		ProductURL: site.URL + "/product",
		Size:       "M",
		Profile:    states.Profile{Email: "buyer@example.com", Name: "Buyer", Address: "1 Example St"},
	}

	task, err := svc.CreateTask(ctx, example_checkout.Name, input, tasks.WithProxyGroup(proxies.GlobalGroup))
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
