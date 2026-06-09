package states

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	http "github.com/bogdanfinn/fhttp"
	"github.com/ntakezo/rogojin/_examples/workflows/example/common"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/workflows"
)

// StaticContext is the immutable input the user supplies when creating the task.
type StaticContext struct {
	ProductURL string
	Size       string
	Profile    Profile
}

type Profile struct {
	Email   string
	Name    string
	Address string
}

// RunningContext is the mutable state a workflow accumulates as it advances
// through states, plus its side effects (proxy lease, HTTP client) and the bus
// it uses to coordinate with other tasks.
type RunningContext struct {
	proxies *proxies.Manager
	taskID  string
	lease   *proxies.Lease
	client  *http.Client
	bus     comms.Bus

	queueCookie string
	variantID   string
	csrfToken   string
	cartID      string
}

// Context is the receiver shared across every state. static holds user input by
// value (immutable per task); running is a pointer because states mutate it.
type Context struct {
	static  StaticContext
	running *RunningContext
}

// NewContext builds a fresh context for one task, holding the module's proxy
// manager for lazy lease acquisition plus the bus for inter-task coordination.
func NewContext(input StaticContext, deps workflows.Deps, manager *proxies.Manager) *Context {
	return &Context{
		static: input,
		running: &RunningContext{
			proxies: manager,
			taskID:  deps.TaskID,
			bus:     deps.Bus,
		},
	}
}

// client leases a proxy and builds the client on first use, so a recovered
// task acquires its own lease no matter which state it resumes at.
func (c *Context) client(ctx context.Context) (*http.Client, error) {
	if c.running.client != nil {
		return c.running.client, nil
	}

	lease, err := c.running.proxies.Acquire(ctx, c.running.taskID)
	if err != nil {
		return nil, fmt.Errorf("acquire proxy: %w", err)
	}
	client, err := common.NewClient(lease.Proxy().URL)
	if err != nil {
		lease.Release(false)
		return nil, err
	}
	fmt.Printf("  task %s leased proxy %s (%s)\n", c.running.taskID, lease.Proxy().ID, lease.Proxy().URL)

	c.running.lease = lease
	c.running.client = client
	return client, nil
}

// Teardown releases the task's proxy lease, reporting success on the absence of a
// run error.
func (c *Context) Teardown(ctx context.Context, status workflows.Status, runErr error) error {
	if c.running.lease == nil {
		return nil
	}
	return c.running.lease.Release(runErr == nil)
}

// origin returns the scheme://host of rawURL, the site root the cart and checkout
// endpoints hang off.
func origin(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	return u.Scheme + "://" + u.Host, nil
}

// snapshot is the JSON shape persisted for recovery: the immutable input plus the
// durable running fields. Side effects (lease, client, bus) are reconstructed
// on restore, not serialized.
type snapshot struct {
	Static      StaticContext `json:"static"`
	QueueCookie string        `json:"queueCookie"`
	VariantID   string        `json:"variantID"`
	CSRFToken   string        `json:"csrfToken"`
	CartID      string        `json:"cartID"`
}

// Snapshot serializes the durable context to JSON for checkpointing. It must be
// valid as the entry of the state it is taken before.
func (c *Context) Snapshot() ([]byte, error) {
	return json.Marshal(snapshot{
		Static:      c.static,
		QueueCookie: c.running.queueCookie,
		VariantID:   c.running.variantID,
		CSRFToken:   c.running.csrfToken,
		CartID:      c.running.cartID,
	})
}

// RestoreContext rebuilds a context from a JSON snapshot, restoring the durable
// running fields; the lease and client are re-acquired lazily on first use.
func RestoreContext(deps workflows.Deps, blob []byte, manager *proxies.Manager) (*Context, error) {
	var s snapshot
	if err := json.Unmarshal(blob, &s); err != nil {
		return nil, err
	}
	c := NewContext(s.Static, deps, manager)
	c.running.queueCookie = s.QueueCookie
	c.running.variantID = s.VariantID
	c.running.csrfToken = s.CSRFToken
	c.running.cartID = s.CartID
	return c, nil
}
