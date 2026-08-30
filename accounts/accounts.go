// Package accounts allocates site accounts to tasks. It is the leasing
// package specialized to one model: an Account embeds leasing.Resource — the
// pool, group, and lock fields every leasable kind shares — and adds
// what the framework guarantees on every account, today the forwarding
// email, plus the consumer's own payload. That payload travels as opaque
// JSON so a new workflow needs no schema change; Bind decodes it into a
// struct at the point of use.
//
// Every leasing behavior — groups, holder caps, durable locks, pins,
// lease-guarded deletes — is documented in the leasing package; the Manager
// here is that manager over this model. Accounts register no strategy of
// their own: an account is a specific identity on a specific site, so which
// one a task gets matters far less than that no two tasks get the same one
// at once, and round robin is exactly that.
//
// Accounts are also where the email package's inboxes meet tasks: an
// account or its group names a forwarding inbox, ForwardingEmail resolves
// which one is in effect, and a manager built WithEmail keeps a referenced
// email from being deleted out from under a running task.
//
// Fields are stored verbatim, in the clear: credentials are only as
// protected as the Repository behind them. An implementation that encrypts
// on the way down and decrypts on the way up is transparent to everything
// above. Treat a leased account's Fields as read-only — the pool shares the
// backing bytes.
package accounts

import (
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/leasing"
)

// Kind is the resource kind accounts register with the task manager under —
// the key a task's account placement is filed on. See proxies.Kind.
const Kind leasing.Kind = "account"

// GlobalGroup is the namespace accounts land in when added without a group;
// a non-global group is normally one target site.
const GlobalGroup = leasing.GlobalGroup

// UnlimitedHolders lifts an account's holder cap — but an account is one
// identity, and more than one task inside it at a time is usually a bug, not a
// throughput win.
const UnlimitedHolders = leasing.UnlimitedHolders

// EmailRef is the group-ref key under which an account group names its
// forwarding inbox: Group.Refs[EmailRef] = the email's ID. The account-level
// counterpart is the typed Account.EmailID.
const EmailRef = "email"

// An Account is the durable record of one account on a target site: the
// leasing core, the fields the framework guarantees on every account, and
// the consumer's own payload.
type Account struct {
	leasing.Resource
	// EmailID names this account's forwarding inbox in the email
	// inventory. Empty inherits the account group's, if any.
	EmailID string `json:"emailID,omitempty"`
	// Fields is the consumer's opaque payload; this package never reads it.
	Fields json.RawMessage `json:"fields,omitempty"`
}

// A Group is a durable named subset of the accounts — usually one target site.
type Group = leasing.Group

// Repository is the persistence port: a dumb durable store of accounts and
// their groups, and the only place credentials can be protected.
type Repository = leasing.Repository[Account]

// An Assignment is the placement a task leases under; the pin names an
// account, and pinning is the common case here — "this task is this persona".
type Assignment = leasing.Assignment

// Bind decodes the consumer half of the account — Fields — into F. An
// account with no fields yields the zero F rather than an error, so a
// workflow that needs none can ignore them entirely.
func Bind[F any](a Account) (F, error) {
	var fields F
	if len(a.Fields) == 0 {
		return fields, nil
	}
	if err := json.Unmarshal(a.Fields, &fields); err != nil {
		return fields, fmt.Errorf("decode fields of account %s: %w", a.ID, err)
	}
	return fields, nil
}

// ForwardingEmail resolves the effective forwarding inbox of an account in
// its group: the account's own EmailID wherever set, the group's EmailRef
// otherwise. Empty means no inbox is attached at either level — the same
// own-then-group shape task placement resolves by.
func ForwardingEmail(a Account, g Group) string {
	if a.EmailID != "" {
		return a.EmailID
	}
	return g.Refs[EmailRef]
}
