package states

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	http "github.com/bogdanfinn/fhttp"
	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/requests"
	"github.com/ntakezo/rogojin/_examples/workflows/example/common"
	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/workflows"
)

// The resource kinds this workflow leases under. A kind is just the key its
// placement is filed on the task record; the framework defines none, so this is
// the one place the two managers and the task service must agree.
const (
	ProxyKind   = "proxy"
	AccountKind = "account"
)

// StaticContext is the immutable input the user supplies when creating the task.
// Where the task leases its proxy and its site login from is placement, not
// input: it lives on the task record and arrives through Deps.
type StaticContext struct {
	ProductURL string
	Size       string
}

// Profile is this workflow's account shape. The accounts module stores it as
// opaque JSON, so another workflow's accounts can look nothing like this one's.
type Profile struct {
	Email   string `json:"email"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

// RunningContext is the mutable state a workflow accumulates as it advances
// through states, plus its side effects (proxy lease, HTTP client) and the bus
// it uses to coordinate with other tasks.
type RunningContext struct {
	proxies      *proxies.Manager
	assignment   proxies.Assignment
	lease        *proxies.Lease
	accounts     *accounts.Manager
	account      accounts.Assignment
	accountLease *accounts.Lease
	client       *http.Client
	bus          comms.Bus

	queueCookie string
	variantID   string
	csrfToken   string
	cartID      string
	order       requests.SubmitCheckoutResponse
}

// Context is the receiver shared across every state. static holds user input by
// value (immutable per task); running is a pointer because states mutate it.
type Context struct {
	static  StaticContext
	running *RunningContext
}

// NewContext builds a fresh context for one task, holding the module's proxy and
// account managers for lazy lease acquisition plus the bus for inter-task
// coordination.
func NewContext(input StaticContext, deps workflows.Deps, manager *proxies.Manager, accountManager *accounts.Manager) *Context {
	return &Context{
		static: input,
		running: &RunningContext{
			proxies: manager,
			// Deps carries one placement per resource kind, keyed by whatever
			// this program calls each manager. The whole placement travels
			// together: the group to rotate within, or the one member pinned
			// inside it. No branching here.
			assignment: proxies.Assignment{
				TaskID:     deps.TaskID,
				GroupID:    deps.Assignments[ProxyKind].GroupID,
				ResourceID: deps.Assignments[ProxyKind].ResourceID,
			},
			accounts: accountManager,
			account: accounts.Assignment{
				TaskID:     deps.TaskID,
				GroupID:    deps.Assignments[AccountKind].GroupID,
				ResourceID: deps.Assignments[AccountKind].ResourceID,
			},
			bus: deps.Bus,
		},
	}
}

// profile locks a site account on first use and decodes the fields this
// workflow needs. Locking rather than acquiring is the point: a task that got
// halfway through a checkout as one persona must come back as the same one, and
// the lock outlives both the lease and the process.
func (c *Context) profile(ctx context.Context) (Profile, error) {
	if c.running.accountLease == nil {
		if c.running.account.GroupID == "" {
			return Profile{}, fmt.Errorf("task %s has no account group assigned", c.running.account.TaskID)
		}
		lease, err := c.running.accounts.Lock(ctx, c.running.account)
		if err != nil {
			return Profile{}, fmt.Errorf("lock account: %w", err)
		}
		c.running.accountLease = lease
		fmt.Printf("  task %s locked account %s\n", c.running.account.TaskID, lease.Resource().ID)
	}
	return accounts.Bind[Profile](c.running.accountLease.Resource())
}

// client leases a proxy and builds the client on first use, so a recovered
// task acquires its own lease no matter which state it resumes at.
func (c *Context) client(ctx context.Context) (*http.Client, error) {
	if c.running.client != nil {
		return c.running.client, nil
	}

	if c.running.assignment.GroupID == "" {
		return nil, fmt.Errorf("task %s has no proxy group assigned", c.running.assignment.TaskID)
	}
	lease, err := c.running.proxies.Acquire(ctx, c.running.assignment)
	if err != nil {
		return nil, fmt.Errorf("acquire proxy: %w", err)
	}
	client, err := common.NewClient(lease.Resource().Attrs.URL)
	if err != nil {
		lease.Release(false)
		return nil, err
	}
	fmt.Printf("  task %s leased proxy %s (%s)\n", c.running.assignment.TaskID, lease.Resource().ID, lease.Resource().Attrs.URL)

	c.running.lease = lease
	c.running.client = client
	return client, nil
}

// Teardown releases both of the task's leases, reporting success on the absence
// of a run error. The account's durable lock survives on purpose: it is the
// task's identity, and only deleting the task gives it back.
func (c *Context) Teardown(ctx context.Context, status workflows.Status, runErr error) error {
	var released []error
	if c.running.lease != nil {
		released = append(released, c.running.lease.Release(runErr == nil))
	}
	if c.running.accountLease != nil {
		released = append(released, c.running.accountLease.Release(runErr == nil))
	}
	return errors.Join(released...)
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

// Output returns the placed order as JSON, the task's final result. It is set by
// SubmitCheckout, the terminal state, and read by the engine on clean completion;
// before then the order is its zero value.
func (c *Context) Output() ([]byte, error) {
	return json.Marshal(c.running.order)
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
func RestoreContext(deps workflows.Deps, blob []byte, manager *proxies.Manager, accountManager *accounts.Manager) (*Context, error) {
	var s snapshot
	if err := json.Unmarshal(blob, &s); err != nil {
		return nil, err
	}
	c := NewContext(s.Static, deps, manager, accountManager)
	c.running.queueCookie = s.QueueCookie
	c.running.variantID = s.VariantID
	c.running.csrfToken = s.CSRFToken
	c.running.cartID = s.CartID
	return c, nil
}
