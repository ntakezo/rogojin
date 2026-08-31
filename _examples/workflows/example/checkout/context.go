package example_checkout

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	http "github.com/bogdanfinn/fhttp"
	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/requests"
	"github.com/ntakezo/rogojin/_examples/workflows/example/common"
	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/workflows"
)

// VerificationSender is the address the site's sign-in mail comes from —
// the workflow's choice of sender filter, not the framework's.
const VerificationSender = "no-reply@site.example"

// Input is the immutable input the user supplies when creating the
// task. The module records it in the snapshot envelope and rebuilds a
// recovered context from it. Where the task leases its proxy and its site
// login from is placement, not input: it lives on the task record and arrives
// through the embedded workflows.Base.
type Input struct {
	ProductURL string `json:"productURL"`
	Size       string `json:"size"`
}

// Profile is this workflow's account shape. The accounts module stores it as
// opaque JSON, so another workflow's accounts can look nothing like this one's.
type Profile struct {
	Email   string `json:"email"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

// Order is the workflow's output: the confirmed order SubmitCheckout placed,
// declared as the module's output via Returns[Order] — so a task created
// through tasks.Create hands it back decoded from Start and on Output.
type Order = requests.SubmitCheckoutResponse

// Durable is the state the workflow accumulates as it advances, partitioned by
// the state that writes each section — so a later state reading an earlier
// one's output names the dependency (d.Homepage.CSRFToken). It is registered
// with Persist, so the snapshot envelope carries it and recovery restores it;
// no mirror struct, no field copying.
type Durable struct {
	Homepage HomepageState `json:"homepage"`
	Queue    QueueState    `json:"queue"`
	Login    LoginState    `json:"login"`
	Cart     CartState     `json:"cart"`
}

// HomepageState is what GetHomepage extracts from the product page.
type HomepageState struct {
	VariantID string `json:"variantID"`
	CSRFToken string `json:"csrfToken"`
}

// QueueState is the shared queue cookie WaitInQueue minted or adopted.
type QueueState struct {
	Cookie string `json:"cookie"`
}

// LoginState is the verification link Login parsed from the site's mail.
type LoginState struct {
	VerifyURL string `json:"verifyURL"`
}

// CartState is the cart AddToCart created.
type CartState struct {
	ID string `json:"id"`
}

// resources holds the per-run side effects: leases, the HTTP client, and the
// inbox subscription. They are acquired lazily on first use and rebuilt the
// same way after recovery — never serialized.
type resources struct {
	lease        *proxies.Lease
	accountLease *accounts.Lease
	client       *http.Client
	inbox        email.Subscription
}

// Context is the receiver shared across every state: the framework machinery
// (Base: deps, effect log, snapshot envelope), the module's managers, the
// immutable input, the durable state, and the ephemeral resources — each in
// its own zone.
type Context struct {
	workflows.Base

	proxies  *proxies.Manager
	accounts *accounts.Manager
	email    *email.Manager

	in  Input
	d   Durable
	res resources

	// order is the placed order Do returns from the submit's effect record —
	// on a fresh run and on a recovered re-entry alike — held only for Output.
	order Order
}

// NewContext builds a context for one task, holding the module's proxy,
// account, and email managers for lazy acquisition. The module binds Base
// afterward; Persist registers the durable state with the snapshot envelope.
func NewContext(in Input, proxyManager *proxies.Manager, accountManager *accounts.Manager, emailManager *email.Manager) *Context {
	c := &Context{in: in, proxies: proxyManager, accounts: accountManager, email: emailManager}
	c.Persist(&c.d)
	return c
}

// profile locks a site account on first use and decodes the fields this
// workflow needs. Locking rather than acquiring is the point: a task that got
// halfway through a checkout as one persona must come back as the same one, and
// the lock outlives both the lease and the process.
func (c *Context) profile(ctx context.Context) (Profile, error) {
	if c.res.accountLease == nil {
		placement := c.Assignment(accounts.Kind)
		if placement.GroupID == "" {
			return Profile{}, fmt.Errorf("task %s has no account group assigned", c.TaskID())
		}
		lease, err := c.accounts.Lock(ctx, accounts.Assignment{
			TaskID: c.TaskID(), GroupID: placement.GroupID, ResourceID: placement.ResourceID,
		})
		if err != nil {
			return Profile{}, fmt.Errorf("lock account: %w", err)
		}
		c.res.accountLease = lease
		fmt.Printf("  task %s locked account %s\n", c.TaskID(), lease.Resource().ID)
	}
	return accounts.Bind[Profile](c.res.accountLease.Resource())
}

// inbox subscribes to the account's forwarding inbox on first use, so a
// recovered task re-subscribes no matter which state it resumes in. The
// route runs through the locked account — its own EmailID, else its
// group's ref — and the backfill window since re-reads mail the server
// still holds, which is what makes a resumed task lossless.
func (c *Context) inbox(ctx context.Context, since time.Time) (email.Subscription, error) {
	if c.res.inbox != nil {
		return c.res.inbox, nil
	}
	if _, err := c.profile(ctx); err != nil { // the account lock is the route to the inbox
		return nil, err
	}
	lease := c.res.accountLease
	id := accounts.ForwardingEmail(lease.Resource(), lease.Group())
	if id == "" {
		return nil, fmt.Errorf("account %s has no forwarding inbox attached", lease.Resource().ID)
	}
	sub, err := c.email.Listen(ctx, c.TaskID(), id,
		email.FromSender(VerificationSender), email.WithBackfill(since))
	if err != nil {
		return nil, fmt.Errorf("listen to inbox %s: %w", id, err)
	}
	fmt.Printf("  task %s listening on forwarding inbox %s\n", c.TaskID(), id)
	c.res.inbox = sub
	return sub, nil
}

// client leases a proxy and builds the client on first use, so a recovered
// task acquires its own lease no matter which state it resumes at.
func (c *Context) client(ctx context.Context) (*http.Client, error) {
	if c.res.client != nil {
		return c.res.client, nil
	}

	placement := c.Assignment(proxies.Kind)
	if placement.GroupID == "" {
		return nil, fmt.Errorf("task %s has no proxy group assigned", c.TaskID())
	}
	lease, err := c.proxies.Acquire(ctx, proxies.Assignment{
		TaskID: c.TaskID(), GroupID: placement.GroupID, ResourceID: placement.ResourceID,
	})
	if err != nil {
		return nil, fmt.Errorf("acquire proxy: %w", err)
	}
	client, err := common.NewClient(lease.Resource().URL)
	if err != nil {
		lease.ReleaseOutcome(ctx, false)
		return nil, err
	}
	fmt.Printf("  task %s leased proxy %s (%s)\n", c.TaskID(), lease.Resource().ID, lease.Resource().URL)

	// The jar is a side effect, rebuilt empty on recovery — a restored queue
	// cookie only survives the restart if it is re-installed here.
	if c.d.Queue.Cookie != "" {
		if err := common.SetCookies(client, c.in.ProductURL,
			&http.Cookie{Name: "queue", Value: c.d.Queue.Cookie}); err != nil {
			lease.ReleaseOutcome(ctx, false)
			return nil, err
		}
	}

	c.res.lease = lease
	c.res.client = client
	return client, nil
}

// Teardown closes the inbox subscription and releases both of the task's
// leases; the proxy's takes the outcome its selection learns from, success
// being the absence of a run error. The account's durable lock survives on
// purpose: it is the task's identity, and only deleting the task gives it
// back.
func (c *Context) Teardown(ctx context.Context, status workflows.Status, runErr error) error {
	var released []error
	if c.res.inbox != nil {
		released = append(released, c.res.inbox.Close())
	}
	if c.res.lease != nil {
		released = append(released, c.res.lease.ReleaseOutcome(ctx, runErr == nil))
	}
	if c.res.accountLease != nil {
		c.res.accountLease.Release()
	}
	return errors.Join(released...)
}

// Output returns the placed order as JSON, the task's final result. It is set
// by SubmitCheckout, the terminal state, and read by the engine on clean
// completion; before then the order is its zero value.
func (c *Context) Output() ([]byte, error) {
	return json.Marshal(c.order)
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
