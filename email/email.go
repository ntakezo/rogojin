// Package email gives tasks the ability to listen to a consumer's email
// inbox over IMAP and post-process what arrives. It is mechanism, not
// policy: an inventory of inboxes and a fan-out listening engine, ignorant
// of who listens and why. The account model that decides which task gets
// which inbox lives above it, in the accounts package.
//
// Email is deliberately not a leasing resource. Listening is not exclusive,
// so there is nothing to acquire, lock, rotate, or score — and there are no
// email groups: an email is one flat row. One IMAP listener exists per
// inbox, established lazily when the first task asks for it, and every
// listening task receives every matching message. Delivery is fan-out,
// never first-taker-wins; what each task does with a message is its own
// business.
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

// Repository is the persistence port: a dumb durable store of the email
// inventory. List returns rows in stable id order; Save is an upsert that
// never rewrites CreatedAt; Delete is a no-op on absent rows. The listener
// persists its cursor through Save.
type Repository interface {
	List(ctx context.Context) ([]Email, error)
	Save(ctx context.Context, e Email) error
	Delete(ctx context.Context, id string) error
}

// A DeleteGuard is the referential check Delete consults: given an email
// ID, the task IDs of accounts held by a live lease (held) and of accounts
// bound only by a durable lock (locked) whose effective forwarding inbox is
// that email. This package carries the hook; accounts supplies the
// canonical implementation. Without a guard, Delete checks only active
// subscriptions.
type DeleteGuard func(emailID string) (held, locked []string)

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
