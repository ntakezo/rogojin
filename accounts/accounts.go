// Package accounts allocates site accounts to tasks. It is the leasing
// package specialized to one resource kind, and it owns the concrete
// account model: Attrs carries the fields the framework guarantees on every
// account — today the forwarding email — wrapped around the consumer's own
// payload, which travels as opaque JSON so a new workflow needs no schema
// change; Bind decodes it into a struct at the point of use.
//
// The types here are aliases, not wrappers: an Account is a leasing.Resource,
// the Manager is a leasing.Manager, and every behavior — groups, holder caps,
// durable locks, pins, lease-guarded deletes — is documented there. Accounts
// register no strategy of their own: an account is a specific identity on a
// specific site, so which one a task gets matters far less than that no two
// tasks get the same one at once, and round robin is exactly that.
//
// Accounts are also where the email package's inboxes meet tasks: an
// account or its group names a forwarding inbox, ForwardingEmail resolves
// which one is in effect, and a manager built WithEmail keeps a referenced
// email from being deleted out from under a running task.
//
// Fields are stored verbatim, in the clear: credentials are only as
// protected as the Repository behind them. An implementation that encrypts
// on the way down and decrypts on the way up is transparent to everything
// above. Treat a leased account's Attrs as read-only — the pool shares the
// backing bytes.
package accounts

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/leasing"
)

// GlobalGroup is the namespace accounts land in when added without a group;
// a non-global group is normally one target site.
const GlobalGroup = leasing.GlobalGroup

// UnlimitedHolders lifts an account's holder cap — but an account is one
// identity, and more than one task inside it at a time is usually a bug, not a
// throughput win.
const UnlimitedHolders = leasing.UnlimitedHolders

// EmailRef is the group-ref key under which an account group names its
// forwarding inbox: Group.Refs[EmailRef] = the email's ID. The account-level
// counterpart is the typed Attrs.EmailID.
const EmailRef = "email"

// Attrs is the concrete account model: the fields the framework guarantees
// on every account, wrapped around the consumer's own payload.
type Attrs struct {
	// EmailID names this account's forwarding inbox in the email
	// inventory. Empty inherits the account group's, if any.
	EmailID string `json:"emailID,omitempty"`
	// Fields is the consumer's opaque payload; this package never reads it.
	Fields json.RawMessage `json:"fields,omitempty"`
}

// An Account is the durable record of one account on a target site.
type Account = leasing.Resource[Attrs]

// A Group is a durable named subset of the accounts — usually one target site.
type Group = leasing.Group

// Repository is the persistence port: a dumb durable store of accounts and
// their groups, and the only place credentials can be protected.
type Repository = leasing.Repository[Attrs]

// An Assignment is the placement a task leases under; the pin names an
// account, and pinning is the common case here — "this task is this persona".
type Assignment = leasing.Assignment

// A Manager allocates accounts to tasks. See leasing.Manager.
type Manager = leasing.Manager[Attrs]

// A Lease is a live hold on one account. Release it exactly once when done.
type Lease = leasing.Lease[Attrs]

// An Option configures what NewManager wires up around the leasing core.
type Option func(*config)

type config struct {
	email *email.Manager
}

// WithEmail hands the manager the email inventory its accounts forward to.
// It closes the referential loop: the email manager learns to refuse
// deleting an inbox while an account forwarding to it is held by a live
// lease, and to report the tasks an idle durable lock would strand.
func WithEmail(m *email.Manager) Option {
	if m == nil {
		panic("accounts: WithEmail requires a manager")
	}
	return func(c *config) { c.email = m }
}

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent. Given WithEmail, it also installs the account
// side of the email delete policy — see WithEmail.
func NewManager(ctx context.Context, repo Repository, opts ...Option) (*Manager, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	m, err := leasing.NewManager(ctx, repo)
	if err != nil {
		return nil, err
	}
	if cfg.email != nil {
		cfg.email.GuardDeletes(func(emailID string) (held, locked []string) {
			if emailID == "" {
				return nil, nil
			}
			ref := func(a Account, g Group) bool { return ForwardingEmail(a, g) == emailID }
			return taskIDs(m.Held(ref)), taskIDs(m.Locked(ref))
		})
	}
	return m, nil
}

// Bind decodes the consumer half of the account — Attrs.Fields — into F. An
// account with no fields yields the zero F rather than an error, so a
// workflow that needs none can ignore them entirely.
func Bind[F any](a Account) (F, error) {
	var fields F
	if len(a.Attrs.Fields) == 0 {
		return fields, nil
	}
	if err := json.Unmarshal(a.Attrs.Fields, &fields); err != nil {
		return fields, fmt.Errorf("decode fields of account %s: %w", a.ID, err)
	}
	return fields, nil
}

// ForwardingEmail resolves the effective forwarding inbox of an account in
// its group: the account's own EmailID wherever set, the group's EmailRef
// otherwise. Empty means no inbox is attached at either level — the same
// own-then-group shape task placement resolves by.
func ForwardingEmail(a Account, g Group) string {
	if a.Attrs.EmailID != "" {
		return a.Attrs.EmailID
	}
	return g.Refs[EmailRef]
}

// taskIDs flattens assignments to their task ids, deduplicated in report
// order.
func taskIDs(assignments []leasing.Assignment) []string {
	seen := make(map[string]struct{}, len(assignments))
	ids := make([]string, 0, len(assignments))
	for _, a := range assignments {
		if _, dup := seen[a.TaskID]; dup {
			continue
		}
		seen[a.TaskID] = struct{}{}
		ids = append(ids, a.TaskID)
	}
	return ids
}
