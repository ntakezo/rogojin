# email — spec

A new top-level package that gives tasks the ability to listen to a consumer's
email inbox over IMAP and post-process incoming mail. Two vendors are
supported: Gmail and Outlook.

`email` is a resource package with a twist. Like `cards` and `proxies`, it has
a persistence layer holding many instances, groups, and participation in task
placement (`Deps.Assignments["email"]`). Unlike them, **there is no leasing**:
listening to an inbox is not exclusive, so there is nothing to acquire, lock,
or rotate. One IMAP listener exists per inbox, established lazily when the
first task asks for it, and every listening task receives every matching
message — delivery is fan-out, never first-taker-wins.

## Vocabulary

- **Email** — the stored resource: an address plus the vendor and credentials
  needed to reach its inbox. What users add, group, and link accounts to.
- **Message** — one piece of mail that arrived in an inbox.
- **Listener** — the single IMAP connection + IDLE loop serving one inbox.
- **Subscription** — one task's live feed of messages from one or more
  inboxes, optionally filtered by sender.

## Package layout

```
email/
  email.go      Email, Group, Message, Assignment, Repository, errors
  manager.go    Manager: inventory, links, subscriptions, fan-out
  listener.go   per-inbox IMAP loop: dial, select, backfill, IDLE, reconnect
  vendors.go    Vendor table: endpoints and auth mechanisms for gmail/outlook
persistence/
  emailsqlite/  SQLite adapter for email.Repository (store name "email")
```

`email` does not import `leasing`, `tasks`, or `workflows`. Like the other
resource packages it is imported *by* the consumer's workflow code and by its
own persistence adapter. It does import the IMAP client library directly —
the IMAP engine is the package's reason to exist, not an adapter concern.

Proposed dependencies (new to go.mod):

- `github.com/emersion/go-imap/v2` — IMAP client with IDLE support
- `github.com/emersion/go-message` — MIME parsing
- `github.com/emersion/go-sasl` — XOAUTH2 for Outlook (and optionally Gmail)

## Core types

```go
// Email is one inbox a consumer has stored: an address, the vendor that
// hosts it, and the credentials needed to open an IMAP session against it.
// LastUID and UIDValidity are the listener's durable cursor; consumers
// never set them.
type Email struct {
	ID          string    `json:"id"`
	GroupID     string    `json:"groupId"`
	Address     string    `json:"address"`
	Vendor      Vendor    `json:"vendor"`
	Auth        Auth      `json:"auth"`
	LastUID     uint32    `json:"lastUID"`
	UIDValidity uint32    `json:"uidValidity"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
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

type Group struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

const GlobalGroup = "global"

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

// Link binds an account (a persona from the accounts package) to the email
// whose inbox it uses. One account has at most one email; many accounts may
// share one email.
type Link struct {
	AccountID string
	EmailID   string
	CreatedAt time.Time
}
```

Unlike `leasing.Resource[T]` there is no `OwnerID`, `MaxHolders`,
`Successes`, or `Failures` — listening is non-exclusive and has no outcome
to score.

Why a typed `Attrs`-equivalent instead of `json.RawMessage`: the package
itself must dial IMAP from these fields, so the schema is owned here, not by
the consumer. Consumer-defined extras don't belong on the credential record.

## Repository port

```go
type Repository interface {
	List(ctx context.Context) ([]Email, error)
	Save(ctx context.Context, e Email) error
	Delete(ctx context.Context, id string) error

	ListGroups(ctx context.Context) ([]Group, error)
	SaveGroup(ctx context.Context, g Group) error
	DeleteGroup(ctx context.Context, id string) error

	ListLinks(ctx context.Context) ([]Link, error)
	SaveLink(ctx context.Context, l Link) error
	DeleteLink(ctx context.Context, accountID string) error
}
```

Same contract style as `leasing.Repository`: `List` in deterministic order,
`Save` is an upsert that never rewrites `CreatedAt`, `Delete` is a no-op on
absent rows. The listener persists its cursor by calling `Save` with updated
`LastUID`/`UIDValidity`.

## Manager

```go
func NewManager(ctx context.Context, repo Repository, opts ...Option) (*Manager, error)

type Option func(*Manager)
func WithDropHandler(fn func(emailID, taskID string, dropped uint64)) Option
func WithListenerErrorHandler(fn func(emailID string, err error)) Option

// Inventory — mirrors the leasing manager's shape.
func (m *Manager) Add(ctx context.Context, e Email) error
func (m *Manager) Delete(ctx context.Context, id string) error
func (m *Manager) CreateGroup(ctx context.Context, id string) error
func (m *Manager) DeleteGroup(ctx context.Context, id string) error

// Links.
func (m *Manager) LinkAccount(ctx context.Context, accountID, emailID string) error
func (m *Manager) UnlinkAccount(ctx context.Context, accountID string) error
func (m *Manager) EmailForAccount(ctx context.Context, accountID string) (Email, error)

// Listening.
func (m *Manager) Listen(ctx context.Context, a Assignment, opts ...ListenOption) (Subscription, error)

// Close stops every listener and closes every subscription. For process
// shutdown; not part of the task path.
func (m *Manager) Close() error
```

`NewManager` loads emails, groups, and links into memory (leasing-style:
persist first, mutate memory second on every write). `Add` validates ID
presence, vendor membership, auth shape for the vendor, and group existence;
it defaults `GroupID` to `"global"`. `Delete` and `DeleteGroup` refuse with
`ErrEmailInUse`/`ErrGroupInUse` while any subscription covers the target,
reporting the listening task IDs in the error — same philosophy as leasing:
report, don't act. `Delete` also removes links pointing at the deleted email.

### Listen

```go
// Assignment names who is listening and to what. EmailID selects one inbox;
// when empty, GroupID selects every inbox in the group, present and future
// membership evaluated at Listen time.
type Assignment struct {
	TaskID  string
	GroupID string
	EmailID string
}

type ListenOption func(*listenConfig)

// FromSender restricts the subscription to messages whose From addr-spec
// equals address, case-insensitively. Default: all mail.
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

Resolution: `EmailID` set → that inbox (must exist, and belong to `GroupID`
when both are set); otherwise `GroupID` → all inboxes currently in the group,
one subscription spanning all of them. Empty group → `ErrNoEmails`.

## Listener lifecycle

One listener per email ID, held in a manager registry
(`map[string]*listener` under the manager mutex), ref-counted by
subscription.

- **Lazily established.** The first `Listen` covering an inbox dials it.
  Subsequent subscriptions attach to the running listener. Dial happens
  outside the manager lock; concurrent first-listens are coalesced so
  exactly one connection results.
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
whose sender filter matches — never only the first to receive it. What each
task does with the message is its own business.

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

`email` participates in placement exactly like other kinds — the workflow
declares a kind string and the consumer routes it at task creation:

```go
// states/context.go (consumer code)
const EmailKind = "email"

// main.go
task, _ := svc.CreateTask(ctx, checkout.Name, input,
	tasks.WithResourceGroup(states.EmailKind, email.GlobalGroup))
```

Because there is nothing to unlock and no stale locks to release, **no
`tasks.WithResource` registration exists for email** — the manager does not
implement `tasks.ResourceManager`, and doesn't need to. `Deps.Assignments`
carries the placement; the workflow holds `*email.Manager` in its static
context and subscribes lazily on first use, mirroring the lease idiom:

```go
// inbox returns the running subscription, establishing it on first use so
// a recovered task re-subscribes no matter which state it resumes in.
func (c *Context) inbox(ctx context.Context) (email.Subscription, error) {
	if c.running.inbox != nil {
		return c.running.inbox, nil
	}
	a, ok := c.static.Deps.Assignments[EmailKind]
	if !ok {
		return nil, fmt.Errorf("task %s has no email group assigned", c.static.Deps.TaskID)
	}
	sub, err := c.static.Email.Listen(ctx, email.Assignment{
		TaskID:  c.static.Deps.TaskID,
		GroupID: a.GroupID,
		EmailID: a.ResourceID,
	}, email.FromSender("no-reply@store.example"),
		email.WithBackfill(c.running.startedAt))
	if err != nil {
		return nil, err
	}
	c.running.inbox = sub
	return sub, nil
}
```

`Teardown` closes the subscription alongside the leases. The subscription is
a side effect, not a durable fact — it is reconstructed on restore, never
snapshotted, and `WithBackfill(startedAt)` (with `startedAt` a snapshotted
field) is what makes resumption lossless.

The account-linked path composes with the lock idiom: a task that locked an
account resolves its inbox with `EmailForAccount(ctx, lease.Resource().ID)`
and listens with `EmailID` pinned. Links are the first cross-resource edge in
the codebase; they live in `email`'s store, keyed by account ID, because
`accounts` payloads are opaque and must stay that way.

## Persistence: `persistence/emailsqlite`

Standard adapter shape: `NewSQLite(dsn)`, `SetMaxOpenConns(1)`,
`sqlitemigrate.Run(db, "email", migrations)` (store name `"email"`, stable
forever), RFC3339Nano UTC times, `List* ORDER BY` primary key, upserts that
never overwrite `created_at`.

Migration 1:

```sql
CREATE TABLE IF NOT EXISTS emails (
  id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL DEFAULT 'global',
  address TEXT NOT NULL DEFAULT '',
  vendor TEXT NOT NULL DEFAULT '',
  auth TEXT NOT NULL DEFAULT '',           -- Auth as JSON
  last_uid INTEGER NOT NULL DEFAULT 0,
  uid_validity INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS email_groups (
  id TEXT PRIMARY KEY,
  created_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS email_links (
  account_id TEXT PRIMARY KEY,
  email_id TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT ''
);
```

Credentials are stored in plaintext, as proxy URLs and account payloads
already are; encryption-at-rest is out of scope here (open question below).

## Errors

```go
var (
	ErrEmailNotFound    = errors.New("email: email not found")
	ErrGroupNotFound    = errors.New("email: group not found")
	ErrNoEmails         = errors.New("email: no emails in group")
	ErrEmailInUse       = errors.New("email: email has active listeners")
	ErrGroupInUse       = errors.New("email: group has active listeners")
	ErrAccountNotLinked = errors.New("email: account has no linked email")
	ErrVendorUnknown    = errors.New("email: unknown vendor")
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
- `TestGroupListenCoversEveryInboxInTheGroup`
- `TestLinkedAccountResolvesItsInbox` / `TestUnlinkedAccountFailsLoud`
- `TestDeleteRefusesWhileTasksAreListening` — names the listening tasks.
- `TestSlowSubscriberDropsWithoutStallingPeers`
- `emailsqlite`: round-trip of Auth JSON, cursor upsert preserving
  `created_at`, migration ledger under store `"email"` sharing a file with
  other stores.

CI additions: none beyond the new packages riding `go test -race ./...`.

## Out of scope / open questions

1. **Credential encryption at rest** — plaintext today, consistent with the
   other stores; revisit as its own effort across all stores.
2. **OAuth token acquisition** — the package refreshes access tokens from a
   stored refresh token; the interactive consent flow that produces the
   refresh token is the consumer's problem (CLI helper is a possible
   follow-up).
3. **Folders** — INBOX only for now; a `Folder` field on Assignment is a
   compatible later addition.
4. **Richer filters** — subject/regex filters are compatible additions to
   `ListenOption`; sender-only matches the stated requirement.
5. **Scaffolder** — no `rogojin new` flag for email in this pass; templates
   are untouched.
6. **Message persistence** — deliberately none. The IMAP server is the
   durable log; `WithBackfill` is the replay mechanism. Revisit only if a
   vendor's retention becomes a problem.
