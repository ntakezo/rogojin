// Package email gives tasks the ability to listen to a consumer's email
// inbox over IMAP and post-process what arrives. It is mechanism, not
// policy: an inventory of inboxes and a fan-out listening engine, ignorant
// of who listens and why. The account model that decides which task gets
// which inbox lives above it, in the accounts package.
//
// Email is deliberately not a leasing resource. Listening is not exclusive,
// so there is nothing to acquire, lock, rotate, or score — and there are no
// email groups: an email is one flat row. One IMAP listener exists per
// inbox — enforced across processes by a store claim, so two nodes sharing
// a store never open duplicate sessions or fight over the cursor —
// established lazily when the first task asks for it, and every listening
// task receives every matching message. Delivery is fan-out, never
// first-taker-wins; what each task does with a message is its own business.
//
// No message bodies are persisted — the IMAP server is the durable log, and
// WithBackfill is the replay mechanism. Only the listener's cursor is
// stored. Credentials are stored verbatim, in the clear: they are only as
// protected as the Repository behind them.
package email

import (
	"context"
	"errors"
	"time"
)

// An Email is one row of the email inventory: an address, and optionally
// the inbox behind it. Inbox is nil for an address-only email, which can be
// read but not listened to.
type Email struct {
	ID        string    `json:"id"`
	Address   string    `json:"address"`
	Inbox     *Inbox    `json:"inbox,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// An Inbox is the listenable half of an email: where it lives and how to
// get in. LastUID and UIDValidity are the listener's durable cursor;
// consumers never set them.
type Inbox struct {
	Vendor      Vendor `json:"vendor"`
	Auth        Auth   `json:"auth"`
	LastUID     uint32 `json:"lastUID"`
	UIDValidity uint32 `json:"uidValidity"`
}

// A Vendor names a supported inbox host and fixes its IMAP endpoint and the
// authentication mechanisms it accepts.
type Vendor string

const (
	// Gmail is imap.gmail.com:993; app passwords and OAuth both work.
	Gmail Vendor = "gmail"
	// Outlook is outlook.office365.com:993; only OAuth works — Microsoft
	// retired basic authentication for IMAP.
	Outlook Vendor = "outlook"
)

// Auth mechanisms Kind selects between.
const (
	// AuthPassword authenticates with Username and Password (an app
	// password) over LOGIN. Gmail only.
	AuthPassword = "password"
	// AuthOAuth2 authenticates over XOAUTH2, minting access tokens from
	// RefreshToken transparently. Both vendors.
	AuthOAuth2 = "oauth2"
)

// Auth holds inbox credentials. Kind selects the mechanism; the OAuth
// fields carry what is needed to mint access tokens from the refresh token
// — the interactive consent flow that produced the refresh token is the
// consumer's concern.
type Auth struct {
	Kind         string `json:"kind"`
	Username     string `json:"username,omitempty"` // defaults to the email's Address
	Password     string `json:"password,omitempty"`
	ClientID     string `json:"clientID,omitempty"`
	ClientSecret string `json:"clientSecret,omitempty"`
	RefreshToken string `json:"refreshToken,omitempty"`
	TokenURL     string `json:"tokenURL,omitempty"` // defaults per vendor
}

// A Message is one piece of mail as delivered to subscribers. From is the
// addr-spec of the first From header address, lowercased — the form
// FromSender filters against. Subscribers needing exactly-once semantics
// deduplicate by MessageID: delivery is at-least-once across restarts.
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

// Repository is the persistence port for the email inventory, and the
// authority on which node runs each inbox's listener. List returns rows in
// stable id order; Save is an upsert that never rewrites CreatedAt; Delete
// is a no-op on absent rows.
//
// The listener claim is store-internal state, deliberately absent from the
// Email model: Save can neither read nor overwrite it, so no inventory
// write ever unseats a live listener. Expiry always compares against the
// store's own clock, never a node's, so clock skew between nodes cannot
// decide ownership. The cursor, by contrast, is model state (Inbox.LastUID,
// Inbox.UIDValidity): Save writes it as given — that is how inboxes are
// seeded — but a live listener advances it only through AdvanceCursor, and
// racing Saves against a live listener is operator misuse.
type Repository interface {
	List(ctx context.Context) ([]Email, error)
	Save(ctx context.Context, e Email) error
	Delete(ctx context.Context, id string) error

	// ClaimListener grants node the exclusive right to run the email's
	// inbox listener: it succeeds iff the claim is unheld, already node's,
	// or expired, and fails with ErrListenerHeld while another node's claim
	// is live. Claim bookkeeping does not touch UpdatedAt — a heartbeat is
	// not an inventory change.
	ClaimListener(ctx context.Context, emailID, node string, ttl time.Duration) error
	// RenewListener extends the claim iff node holds it — even past expiry,
	// so long as no other node has taken it: a late but unusurped renewal
	// wins. ErrListenerHeld reports the claim held elsewhere or not at all.
	RenewListener(ctx context.Context, emailID, node string, ttl time.Duration) error
	// ReleaseListener clears the claim iff node holds it, and is a silent
	// no-op otherwise: releasing after being usurped is a normal shutdown
	// path, not an error.
	ReleaseListener(ctx context.Context, emailID, node string) error
	// AdvanceCursor moves the cursor iff node holds the listener claim and
	// the move is forward: the same uidValidity with a higher lastUID, or a
	// changed uidValidity — a reset, the mailbox renumbered. A same-validity
	// move that is not forward is a silent no-op (a late duplicate write,
	// not an error), so the cursor never moves backward and a message range
	// is never replayed by a lagging writer.
	AdvanceCursor(ctx context.Context, emailID, node string, uidValidity, lastUID uint32) error
}

// ErrEmailNotFound is returned when an operation names an email the manager
// does not know — deleted while a referencing task was down, or never
// added. A dangling account reference surfaces here, at Listen, rather than
// being silently skipped: the fallback is the consumer's call to make.
var ErrEmailNotFound = errors.New("email not found")

// ErrNoInbox is returned by Listen for an address-only email. The address
// is data; without credentials there is no inbox to open, and failing loud
// beats waiting on mail that can never arrive.
var ErrNoInbox = errors.New("email has no inbox")

// ErrEmailInUse is returned by Delete while any task is subscribed to the
// email or holds a live lease on an account forwarding to it. The
// subscription or lease is the fact of use, and it ending is what frees the
// email for deletion — idle durable locks merely report as stranded.
var ErrEmailInUse = errors.New("email in use by running tasks")

// ErrVendorUnknown is returned by Add for an inbox naming a vendor this
// package has no endpoint for.
var ErrVendorUnknown = errors.New("unknown vendor")

// ErrListenerHeld reports a live listener claim by another node: the inbox
// is being listened to elsewhere. Surfaced by Listen when another node holds
// the inbox, and by the claim methods of the Repository.
var ErrListenerHeld = errors.New("inbox listener held by another node")
