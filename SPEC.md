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
the account carries the foreign key, as a first-class typed field. Formally
this is a **forwarding inbox**: one inbox commonly receives mail for many
personas (a catch-all, a plus-addressed pool, a group-wide shared box). The
attached email's address may happen to equal the address inside the
account's own payload; that coincidence is not modeled.

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
  email.go      Email, Inbox, Message, Repository, errors
  manager.go    Manager: inventory, subscriptions, fan-out, delete guard
  listener.go   per-inbox IMAP loop: dial, select, backfill, IDLE, reconnect
  vendors.go    Vendor table: endpoints and auth mechanisms for gmail/outlook
persistence/
  emailsqlite/  SQLite adapter for email.Repository (store name "email")
```

`email` imports no domain package — not `accounts`, `leasing`, `tasks`, or
`workflows`. Like `leasing`, it is pure mechanism: an inbox inventory and a
fan-out listening engine, ignorant of who listens and why. **`accounts`
imports `email`** — the account model is what knows about forwarding
inboxes, so the policy edge points downward: `accounts` → `leasing` and
`accounts` → `email`. `email` imports the IMAP client library directly —
the IMAP engine is the package's reason to exist, not an adapter concern.

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
```

Why a typed struct instead of `json.RawMessage`: the package itself must
dial IMAP from these fields, so the schema is owned here, not by the
consumer.

## Consolidating the concrete account model

The framework today guarantees fields at exactly one place per kind: the
generic `leasing.Resource[T]` carries `ID`, `GroupID`, `OwnerID`,
timestamps, and outcome counters; anything kind-specific lives in `Attrs T`.
Proxies already use a typed `Attrs{URL}`; accounts and cards use raw JSON
and therefore have **no concrete model** — nowhere to guarantee an
account-specific field.

The consolidation: **each kind's concrete model is its typed `Attrs`
struct.** Accounts move from `json.RawMessage` to a typed payload that
guarantees the forwarding email ID while keeping the consumer's payload
opaque inside it:

```go
// accounts package

// Attrs is the concrete account model: the fields the framework
// guarantees on every account, wrapped around the consumer's own payload.
type Attrs struct {
	// EmailID names this account's forwarding inbox in the email
	// inventory. Empty inherits the account group's, if any.
	EmailID string `json:"emailID,omitempty"`
	// Fields is the consumer's opaque payload, exactly as before.
	Fields json.RawMessage `json:"fields,omitempty"`
}

type Account = leasing.Resource[Attrs]

// Bind decodes the consumer half of the account — Attrs.Fields — into F.
func Bind[F any](a Account) (F, error)

// EmailRef is the group-ref key under which an account group names its
// forwarding inbox: Group.Refs[EmailRef] = emailID.
const EmailRef = "email"

// ForwardingEmail resolves the effective forwarding inbox of an account in
// its group: the account's own EmailID wherever set, the group's ref
// otherwise. Empty means no inbox is attached at either level.
func ForwardingEmail(a Account, g Group) string

// EmailDeleteGuard adapts this manager into the referential check
// email.Manager.Delete consults; see the delete-guard section.
func EmailDeleteGuard(m *Manager) email.DeleteGuard
```

Accounts stay pure aliases — no wrapper managers return. Cards keep raw
JSON until they earn a guaranteed field; the pattern is now established.

### Group-level attachment: refs on leasing groups

`leasing.Group` has no payload, so the group side needs minimal carriage:

```go
// leasing.Group gains:
	Refs map[string]string `json:"refs,omitempty"`
```

**Refs are carried, never read** — the same contract `Attrs` has on
resources. Leasing persists and returns them and attaches no meaning; the
`accounts.EmailRef` convention belongs to accounts. Card and proxy groups
inherit the field and leave it empty. No email concept enters the mechanism
layer.

Attachment happens at creation time through existing signatures — both
already take the full struct:

```go
mgr.CreateGroup(ctx, accounts.Group{ID: "pool-a",
	Refs: map[string]string{accounts.EmailRef: "inbox-1"}}) // group-wide forwarding inbox

mgr.Add(ctx, accounts.Account{ID: "acct-7", GroupID: "pool-a",
	Attrs: accounts.Attrs{EmailID: "inbox-2", Fields: payload}}) // per-account override
```

### New leasing read surface

Resolution and the delete guard both need facts leasing already owns — who
holds a live lease, who holds a durable lock — filtered by meaning leasing
doesn't have. Three small read accessors, all generic, none aware of email:

```go
// Group returns the group of the leased resource as of acquisition.
func (l *Lease[T]) Group() Group

// Held reports the assignment of every resource currently held by a live
// lease (acquire or lock mode) whose resource and group satisfy pred.
func (m *Manager[T]) Held(pred func(Resource[T], Group) bool) []Assignment

// Locked reports the assignment of every durably locked resource (OwnerID
// set), running or not, whose resource and group satisfy pred.
func (m *Manager[T]) Locked(pred func(Resource[T], Group) bool) []Assignment
```

The caller supplies the predicate; leasing supplies the facts. This is the
same inversion `tasks.ResourceManager` already uses, pointed the other way.

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

// DeleteGuard is the referential check Delete consults: given an email ID,
// the task IDs of accounts held by a live lease (held) and of accounts
// bound only by a durable lock (locked) whose effective forwarding inbox
// is that email. email carries the hook; accounts supplies the canonical
// implementation. Without a guard, Delete checks only active subscriptions.
type DeleteGuard func(emailID string) (held, locked []string)

func WithDeleteGuard(g DeleteGuard) Option

// Inventory.
func (m *Manager) Add(ctx context.Context, e Email) error
func (m *Manager) Delete(ctx context.Context, id string) (stranded []string, err error)
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

### Delete mirrors the leasing delete policy

The same policy leasing institutes for deleting resources under running
tasks, applied to the account→email edge:

- **Refuse while actively used.** `Delete` returns `ErrEmailInUse`, naming
  the task IDs, when any subscription covers the email **or** a referencing
  account is held by a live lease. An account leased or locked to a running
  task cannot have its email deleted out from under it.
- **Report what it strands.** Accounts bound only by an idle durable lock
  (a suspended or failed task that will resume as that persona) don't block
  the delete — leasing's own `Delete` unbinds rather than blocks there —
  but their task IDs come back as `stranded`, so the caller decides, just
  as leasing reports unbound task IDs rather than acting.

`email` cannot see accounts (the import points the other way), so the check
arrives as an injected `DeleteGuard` — and `accounts`, which owns the model
and the resolution, exports its canonical construction:

```go
// accounts package (imports email)

// EmailDeleteGuard returns the referential check email.Manager.Delete
// consults: which tasks hold, or durably lock, an account whose effective
// forwarding inbox is the email in question.
func EmailDeleteGuard(m *Manager) email.DeleteGuard
```

Consumer main wires the two managers together — the same composition point
where `tasks.WithResource` lives:

```go
emailMgr, _ := email.NewManager(ctx, emailRepo,
	email.WithDeleteGuard(accounts.EmailDeleteGuard(accountMgr)))
```

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
dangling reference), `ErrNoInbox` for an address-only email.

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
	id := accounts.ForwardingEmail(lease.Resource(), lease.Group())
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

## Persistence

### `persistence/emailsqlite`

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
`Inbox == nil`. Credentials are stored in plaintext, as proxy URLs and
account payloads already are; encryption-at-rest is out of scope (open
question below).

### Ripple in the existing adapters

- `accountsqlite`: migration adds `email_id TEXT NOT NULL DEFAULT ''` to
  `accounts` — a real queryable column, since it's a guaranteed field. The
  existing `fields` column now holds `Attrs.Fields`; existing rows read
  back unchanged (`email_id` defaults empty). The adapter's port becomes
  `leasing.Repository[accounts.Attrs]`.
- `accountsqlite`, `cardsqlite`, `proxysqlite`: migration adds
  `refs TEXT NOT NULL DEFAULT ''` (JSON) to each group table, so
  `Group.Refs` survive a restart for every kind.

All append-only, per the migration ledger's rules.

## Errors

```go
var (
	ErrEmailNotFound = errors.New("email: email not found")
	ErrNoInbox       = errors.New("email: email has no inbox")
	ErrEmailInUse    = errors.New("email: email in use by running tasks")
	ErrVendorUnknown = errors.New("email: unknown vendor")
)
```

Each sentinel gets the leasing-style doc explaining why it fails rather than
blocks. `ErrEmailInUse` covers both active subscriptions and live leases on
referencing accounts. Wrapped errors follow house style:
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
- `TestDeleteRefusesWhileAReferencingAccountIsHeld` — a guard reporting a
  live lease blocks and names the task; idle lock comes back as `stranded`
  (fake guard in `email`; the real one is covered in `accounts` by
  `TestEmailDeleteGuardSeesHeldAndLockedAccounts`).
- `TestDeleteRefusesWhileTasksAreListening` — names the listening tasks.
- `TestSlowSubscriberDropsWithoutStallingPeers`
- In `accounts`: `TestForwardingEmailPrefersTheAccountOverItsGroup`,
  `TestFieldsTravelOpaquelyInsideAttrs` (Bind still decodes only the
  consumer half).
- In `leasing`: `TestGroupRefsTravelOpaquely`, `TestLeaseExposesItsGroup`,
  `TestHeldAndLockedReportOnlyMatchingAssignments`.
- `emailsqlite`: round-trip of Auth JSON and nil-Inbox mapping, cursor
  upsert preserving `created_at`, migration ledger under store `"email"`
  sharing a file with other stores.

CI additions: none beyond the new packages riding `go test -race ./...`.

## Implementation order

1. `leasing`: `Group.Refs`, `Lease.Group()`, `Held`/`Locked` — generic,
   email-free, tested where they live.
2. `email` + `emailsqlite`: the package itself — no domain imports, so it
   lands standalone with a fake guard in tests.
3. `accounts`: typed `Attrs`, `Bind` over `Attrs.Fields`, `EmailRef`,
   `ForwardingEmail`, `EmailDeleteGuard`; `accountsqlite` migration and
   port change; update the example workflow's `Bind` usage.
4. `cardsqlite`/`proxysqlite`: group `refs` migrations.
5. Wire the guard and the `inbox` idiom into `_examples`.

## Out of scope / open questions

1. **Dangling references** — deleting an email that only idle-locked
   accounts point at strands them by design (reported, not blocked), and
   unheld accounts' references aren't scrubbed either way; `Listen` fails
   loud with `ErrEmailNotFound` on resume. Scrubbing would put email
   knowledge inside leasing or accounts.
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
8. **Cards' concrete model** — cards stay raw JSON until they earn a
   guaranteed field; when they do, they follow the same typed-`Attrs`
   consolidation.
