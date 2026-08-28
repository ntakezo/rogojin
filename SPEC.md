# email — spec

A new top-level package that gives tasks the ability to listen to a
consumer's email inbox over IMAP and post-process incoming mail. Two vendors
are supported: Gmail and Outlook.

`email` is deliberately not a leasing resource. Listening to an inbox is not
exclusive, so there is nothing to acquire, lock, rotate, or score — and there
are no email groups either: an email is one flat row in an inventory. One
IMAP listener exists per inbox, established lazily when the first task asks
for it, and every listening task receives every matching message — delivery
is fan-out, never first-taker-wins.

An email reaches a task through its **account**, not through task placement.
An account, or an account group, is attached to an email at creation time —
the account carries the foreign key. Formally this is a **forwarding
inbox**: one inbox commonly receives mail for many personas (a catch-all, a
plus-addressed pool, a group-wide shared box). The attached email's address
may happen to equal the address inside the account's own payload; that
coincidence is not modeled.

The workflow decides everything downstream: which sender to listen for, and
how to parse whatever arrives — if it listens at all.

## Vocabulary

- **Email** — one stored row: an address, plus *optionally* the vendor and
  credentials needed to open its inbox. An address-only email is usable as
  data (form fill, registration); only an email with inbox credentials can
  be listened to.
- **Forwarding inbox** — the role an email plays when attached to an account
  or account group: the place that account's mail actually lands.
- **Message** — one piece of mail that arrived in an inbox.
- **Listener** — the single IMAP connection + IDLE loop serving one inbox.
- **Subscription** — one task's live feed from one inbox, optionally
  filtered by sender.

## Package layout

```
email/
  email.go      Email, Inbox, Message, Repository, RefKey, errors
  manager.go    Manager: inventory, subscriptions, fan-out
  listener.go   per-inbox IMAP loop: dial, select, backfill, IDLE, reconnect
  vendors.go    Vendor table: endpoints and auth mechanisms for gmail/outlook
persistence/
  emailsqlite/  SQLite adapter for email.Repository (store name "email")
```

`email` does not import `leasing`, `tasks`, or `workflows`. It is imported
by the consumer's workflow code and by its own persistence adapter. It
imports the IMAP client library directly — the IMAP engine is the package's
reason to exist, not an adapter concern.

Proposed dependencies (new to go.mod):

- `github.com/emersion/go-imap/v2` — IMAP client with IDLE support
- `github.com/emersion/go-message` — MIME parsing
- `github.com/emersion/go-sasl` — XOAUTH2 for Outlook (and optionally Gmail)

## Core types

```go
// Email is one row of the email inventory: an address, and optionally the
// inbox behind it. Inbox is nil for an address-only email, which can be
// read but not listened to.
type Email struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	Inbox     *Inbox    `json:"inbox,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// Inbox is the listenable half of an email: where it lives and how to get
// in. LastUID and UIDValidity are the listener's durable cursor; consumers
// never set them.
type Inbox struct {
	Vendor      Vendor `json:"vendor"`
	Auth        Auth   `json:"auth"`
	LastUID     uint32 `json:"lastUID"`
	UIDValidity uint32 `json:"uidValidity"`
}

type Vendor string

const (
	Gmail   Vendor = "gmail"   // imap.gmail.com:993 — app password or XOAUTH2
	Outlook Vendor = "outlook" // outlook.office365.com:993 — XOAUTH2 only
)

// Auth holds inbox credentials. Kind selects the mechanism: "password"
// (app password over SASL PLAIN; Gmail only) or "oauth2" (XOAUTH2; both
// vendors). OAuth fields carry what's needed to mint access tokens from
// the refresh token; the listener refreshes transparently.
type Auth struct {
	Kind         string `json:"kind"`
	Username     string `json:"username,omitempty"` // defaults to Address
	Password     string `json:"password,omitempty"`
	ClientID     string `json:"clientID,omitempty"`
	ClientSecret string `json:"clientSecret,omitempty"`
	RefreshToken string `json:"refreshToken,omitempty"`
	TokenURL     string `json:"tokenURL,omitempty"` // defaults per vendor
}

// Message is one piece of mail as delivered to subscribers. From is the
// addr-spec of the first From header address, lowercased.
type Message struct {
	EmailID   string
	UID       uint32
	MessageID string
	From      string
	To        []string
	Subject   string
	Date      time.Time
	Text      string
	HTML      string
}

// RefKey is the leasing ref key under which an account or account group
// names its forwarding inbox: Refs[email.RefKey] = emailID.
const RefKey = "email"
```

Why a typed struct instead of `json.RawMessage`: the package itself must
dial IMAP from these fields, so the schema is owned here, not by the
consumer.

## The attachment: refs on leasing resources

The foreign key lives on the account, and accounts are pure type aliases of
`leasing.Resource[json.RawMessage]` — so the carrying surface is added to
`leasing`, kept exactly as ignorant as `Attrs` already is:

```go
// leasing.Resource[T] gains:
	Refs map[string]string `json:"refs,omitempty"`

// leasing.Group gains the same field.
```

**Refs are carried, never read.** Leasing persists and returns them and
attaches no meaning — the same contract `Attrs` has. The key/value
convention (`"email"` → email ID) belongs to the consumer and the `email`
package. Cards and proxies inherit the field and simply leave it empty.
This keeps the leasing boundary intact: no email concept enters the
mechanism layer.

Attachment happens at creation time through existing signatures — both
already take the full struct:

```go
mgr.CreateGroup(ctx, accounts.Group{ID: "pool-a",
	Refs: map[string]string{email.RefKey: "inbox-1"}})   // group-wide forwarding inbox

mgr.Add(ctx, accounts.Account{ID: "acct-7", GroupID: "pool-a",
	Refs: map[string]string{email.RefKey: "inbox-2"},    // per-account override
	Attrs: payload})
```

Resolution is per-key, account wins, group falls back — the same shape task
placement already uses for resource groups. To make the group side reachable
from a running task, `leasing.Lease[T]` gains one read accessor:

```go
// Group returns the group of the leased resource as of acquisition.
func (l *Lease[T]) Group() Group
```

so the workflow resolves with no new lookup surface on the manager:

```go
id := lease.Resource().Refs[email.RefKey]
if id == "" {
	id = lease.Group().Refs[email.RefKey]
}
```

Ripple: `accountsqlite`, `cardsqlite`, and `proxysqlite` each gain an
append-only migration adding a `refs TEXT NOT NULL DEFAULT ''` (JSON)
column to both their resource and group tables, so refs survive a restart
for every kind. Deleting an email does **not** scrub refs pointing at it;
a dangling ref fails loud at `Listen` with `ErrEmailNotFound` (open
question below).

## Repository port

```go
type Repository interface {
	List(ctx context.Context) ([]Email, error)
	Save(ctx context.Context, e Email) error
	Delete(ctx context.Context, id string) error
}
```

Same contract style as `leasing.Repository`: `List` in deterministic order,
`Save` is an upsert that never rewrites `CreatedAt`, `Delete` is a no-op on
absent rows. The listener persists its cursor by calling `Save` with updated
`Inbox.LastUID`/`Inbox.UIDValidity`.

## Manager

```go
func NewManager(ctx context.Context, repo Repository, opts ...Option) (*Manager, error)

type Option func(*Manager)
func WithDropHandler(fn func(emailID, taskID string, dropped uint64)) Option
func WithListenerErrorHandler(fn func(emailID string, err error)) Option

// Inventory.
func (m *Manager) Add(ctx context.Context, e Email) error
func (m *Manager) Delete(ctx context.Context, id string) error
func (m *Manager) Get(ctx context.Context, id string) (Email, error)

// Listening.
func (m *Manager) Listen(ctx context.Context, taskID, emailID string, opts ...ListenOption) (Subscription, error)

// Close stops every listener and closes every subscription. For process
// shutdown; not part of the task path.
func (m *Manager) Close() error
```

`NewManager` loads the inventory into memory (leasing-style: persist first,
mutate memory second on every write). `Add` validates ID and address
presence, and — when an inbox is present — vendor membership and auth shape
for the vendor. `Get` exists because address-only use is real: a workflow
filling a form needs the address of its forwarding inbox without listening.
`Delete` refuses with `ErrEmailInUse` while any subscription covers the
target, reporting the listening task IDs — report, don't act, as leasing
does.

### Listen

```go
type ListenOption func(*listenConfig)

// FromSender restricts the subscription to messages whose From addr-spec
// equals address, case-insensitively. Default: all mail. The workflow, not
// the framework, decides this — and owns all parsing of what arrives.
func FromSender(address string) ListenOption

// WithBackfill re-delivers messages already in the inbox with an IMAP date
// on or after since, fetched from the server on subscribe. This is how a
// recovered task catches mail that arrived while it was down.
func WithBackfill(since time.Time) ListenOption

// WithBuffer sets the subscription's channel capacity (default 64).
func WithBuffer(n int) ListenOption

type Subscription interface {
	C() <-chan Message
	Close() error
}
```

`Listen` fails fast: `ErrEmailNotFound` for an unknown ID (including a
dangling ref), `ErrNoInbox` for an address-only email.

## Listener lifecycle

One listener per email ID, held in a manager registry
(`map[string]*listener` under the manager mutex), ref-counted by
subscription.

- **Lazily established.** The first `Listen` covering an inbox dials it.
  Subsequent subscriptions attach to the running listener — many tasks,
  many accounts, one connection. Dial happens outside the manager lock;
  concurrent first-listens are coalesced so exactly one connection results.
- **Loop.** Dial vendor endpoint (TLS :993) → authenticate → SELECT INBOX →
  compare UIDVALIDITY (mismatch resets the cursor to the current last UID —
  history renumbered, don't replay the whole mailbox) → fetch UIDs >
  `LastUID` → enter IDLE. On IDLE wake: fetch new UIDs, parse each into a
  `Message`, fan out, then persist the advanced cursor via `repo.Save`.
- **Cursor after fan-out.** The cursor advances only after delivery is
  attempted, so a crash between fetch and persist re-delivers on restart:
  at-least-once across restarts. Subscribers that care must deduplicate by
  `MessageID`.
- **Reconnect.** On any connection error: exponential backoff (1s doubling
  to 2m, ±jitter), re-dial, resume from the persisted cursor. Errors are
  reported through `WithListenerErrorHandler`; the loop never gives up while
  subscribers remain.
- **Teardown.** The last `Close` covering an inbox logs out and removes the
  listener from the registry. `Close` removes the subscription from the
  fan-out set under the manager lock before closing its channel, so a
  concurrent delivery can never send on a closed channel (same discipline
  as `comms`).

## Fan-out semantics

Every message is offered to **every** subscription attached to its inbox
whose sender filter matches — never only the first to receive it. A
forwarding inbox shared by a whole account group means many tasks on one
listener; each decides for itself what a message means.

Delivery per subscriber is a non-blocking send into that subscriber's
buffered channel; a full buffer drops the message for that subscriber only
and increments a per-subscription drop counter surfaced via
`WithDropHandler`. A slow task cannot stall the IMAP loop or its peers.
Order is per-inbox publish order. A dropped or missed message is recoverable
by design: the mail still exists server-side, and `WithBackfill` re-fetches
it — the IMAP server is the replay log, so `email` persists no message
bodies, only the cursor.

Why not `comms.Bus`: the bus has no subscriber-presence signal (needed to
lazily start and stop listeners), unfiltered topics, and untyped channels.
The internal registry borrows its concurrency discipline instead.

## Task and workflow integration

Email takes no part in task placement. There is no `"email"` kind, no
`WithResourceGroup`, no entry in `Deps.Assignments`, and no
`tasks.WithResource` registration — nothing to unlock, no stale locks to
release. The path to an inbox runs through the account the task already
locks:

```go
// inbox returns the running subscription, establishing it on first use so
// a recovered task re-subscribes no matter which state it resumes in.
func (c *Context) inbox(ctx context.Context) (email.Subscription, error) {
	if c.running.inbox != nil {
		return c.running.inbox, nil
	}
	lease, err := c.account(ctx) // the existing durable account lock
	if err != nil {
		return nil, err
	}
	id := lease.Resource().Refs[email.RefKey]
	if id == "" {
		id = lease.Group().Refs[email.RefKey]
	}
	if id == "" {
		return nil, fmt.Errorf("account %s has no forwarding inbox attached", lease.Resource().ID)
	}
	sub, err := c.static.Email.Listen(ctx, c.static.Deps.TaskID, id,
		email.FromSender("no-reply@store.example"), // the workflow's choice
		email.WithBackfill(c.running.startedAt))
	if err != nil {
		return nil, err
	}
	c.running.inbox = sub
	return sub, nil
}
```

The sender filter and the parsing of any message that arrives are entirely
workflow-defined; a workflow that never calls `inbox` pays nothing. The
subscription is a side effect, not a durable fact — closed in `Teardown`,
reconstructed on restore, never snapshotted; `WithBackfill(startedAt)` (with
`startedAt` a snapshotted field) is what makes resumption lossless.

## Persistence: `persistence/emailsqlite`

Standard adapter shape: `NewSQLite(dsn)`, `SetMaxOpenConns(1)`,
`sqlitemigrate.Run(db, "email", migrations)` (store name `"email"`, stable
forever), RFC3339Nano UTC times, `List ORDER BY id`, upserts that never
overwrite `created_at`.

Migration 1 — one table, no groups, no links:

```sql
CREATE TABLE IF NOT EXISTS emails (
  id TEXT PRIMARY KEY,
  address TEXT NOT NULL DEFAULT '',
  vendor TEXT NOT NULL DEFAULT '',         -- '' = address-only, no inbox
  auth TEXT NOT NULL DEFAULT '',           -- Auth as JSON
  last_uid INTEGER NOT NULL DEFAULT 0,
  uid_validity INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);
```

An empty `vendor` marks an address-only email; the adapter maps it to
`Inbox == nil`. The account-side `refs` columns land in `accountsqlite`
(and siblings) as part of the leasing change, not here.

Credentials are stored in plaintext, as proxy URLs and account payloads
already are; encryption-at-rest is out of scope (open question below).

## Errors

```go
var (
	ErrEmailNotFound = errors.New("email: email not found")
	ErrNoInbox       = errors.New("email: email has no inbox")
	ErrEmailInUse    = errors.New("email: email has active listeners")
	ErrVendorUnknown = errors.New("email: unknown vendor")
)
```

Each sentinel gets the leasing-style doc explaining why it fails rather than
blocks. Wrapped errors follow house style:
`fmt.Errorf("dial inbox %s: %w", id, err)`.

## Testing

White-box, same package, no assertion library. The IMAP boundary is an
unexported seam so the loop is testable without a server:

```go
// mailbox is what the listener loop needs from an IMAP session; the real
// implementation wraps go-imap, tests substitute a scripted fake.
type mailbox interface {
	Select(ctx context.Context) (uidValidity uint32, err error)
	FetchSince(ctx context.Context, uid uint32) ([]Message, error)
	Idle(ctx context.Context) error // returns when new mail may exist
	Close() error
}
```

injected via an unexported `withDialer(func(Email) (mailbox, error))` option.

Guarantee-shaped tests:

- `TestEveryListeningTaskReceivesTheMessage` — the headline: N subscribers,
  one message, N deliveries.
- `TestSenderFilterIsPerSubscription` — filtered and unfiltered subscribers
  coexist on one inbox.
- `TestListenerIsEstablishedLazilyAndOnlyOnce` — no dial before first
  Listen; concurrent first-listens coalesce to one connection.
- `TestLastCloseTearsDownTheListener` — refcount reaches zero, connection
  closes, registry entry gone.
- `TestCursorSurvivesRestart` — manager restart resumes from persisted
  `LastUID`; UIDVALIDITY change resets instead of replaying.
- `TestAddressOnlyEmailRefusesToListen` — `ErrNoInbox`, but `Get` works.
- `TestDeleteRefusesWhileTasksAreListening` — names the listening tasks.
- `TestSlowSubscriberDropsWithoutStallingPeers`
- In `leasing`: `TestRefsTravelOpaquely` — refs on resources and groups
  round-trip through the repository untouched and unread;
  `TestLeaseExposesItsGroup`.
- In the example workflow (or `accounts` docs test): account ref wins,
  group ref falls back — the resolution idiom.
- `emailsqlite`: round-trip of Auth JSON and nil-Inbox mapping, cursor
  upsert preserving `created_at`, migration ledger under store `"email"`
  sharing a file with other stores.

CI additions: none beyond the new packages riding `go test -race ./...`.

## Out of scope / open questions

1. **Dangling refs** — deleting an email leaves account/group refs pointing
   at it; `Listen` fails loud with `ErrEmailNotFound`. Scrubbing would put
   email knowledge inside leasing or accounts; left to the operator for now.
2. **Credential encryption at rest** — plaintext today, consistent with the
   other stores; revisit as its own effort across all stores.
3. **OAuth token acquisition** — the package refreshes access tokens from a
   stored refresh token; the interactive consent flow that produces the
   refresh token is the consumer's problem (CLI helper is a possible
   follow-up).
4. **Folders** — INBOX only for now; a folder option is a compatible later
   addition.
5. **Richer filters** — subject/regex filters are compatible additions to
   `ListenOption`; sender-only matches the stated requirement.
6. **Scaffolder** — no `rogojin new` flag for email in this pass; templates
   are untouched.
7. **Message persistence** — deliberately none. The IMAP server is the
   durable log; `WithBackfill` is the replay mechanism. Revisit only if a
   vendor's retention becomes a problem.
