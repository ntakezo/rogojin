// Package accounts allocates site accounts to tasks. It is the leasing package
// specialized to one resource kind: the payload is the account's fields —
// email, password, names, whatever the target site asks for — travelling as
// opaque JSON, so a new workflow needs no schema change; Bind decodes them
// into a struct at the point of use.
//
// The types here are aliases, not wrappers: an Account is a leasing.Resource,
// the Manager is a leasing.Manager, and every behavior — groups, holder caps,
// durable locks, pins, lease-guarded deletes — is documented there. Accounts
// register no strategy of their own: an account is a specific identity on a
// specific site, so which one a task gets matters far less than that no two
// tasks get the same one at once, and round robin is exactly that.
//
// Fields are stored verbatim, in the clear: credentials are only as protected
// as the Repository behind them. An implementation that encrypts Attrs on the
// way down and decrypts it on the way up is transparent to everything above.
// Treat a leased account's Attrs as read-only — the pool shares the backing
// bytes.
package accounts

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/leasing"
)

// GlobalGroup is the namespace accounts land in when added without a group;
// a non-global group is normally one target site.
const GlobalGroup = leasing.GlobalGroup

// UnlimitedHolders lifts an account's holder cap — but an account is one
// identity, and more than one task inside it at a time is usually a bug, not a
// throughput win.
const UnlimitedHolders = leasing.UnlimitedHolders

// An Account is the durable record of one account on a target site; its
// workflow-defined fields live in Attrs as JSON this package never reads.
type Account = leasing.Resource[json.RawMessage]

// A Group is a durable named subset of the accounts — usually one target site.
type Group = leasing.Group

// Repository is the persistence port: a dumb durable store of accounts and
// their groups, and the only place credentials can be protected.
type Repository = leasing.Repository[json.RawMessage]

// An Assignment is the placement a task leases under; the pin names an
// account, and pinning is the common case here — "this task is this persona".
type Assignment = leasing.Assignment

// A Manager allocates accounts to tasks. See leasing.Manager.
type Manager = leasing.Manager[json.RawMessage]

// A Lease is a live hold on one account. Release it exactly once when done.
type Lease = leasing.Lease[json.RawMessage]

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent.
func NewManager(ctx context.Context, repo Repository) (*Manager, error) {
	return leasing.NewManager(ctx, repo)
}

// Bind decodes an account's workflow-defined fields into F. An account with no
// fields yields the zero F rather than an error, so a workflow that needs none
// can ignore them entirely.
func Bind[F any](a Account) (F, error) {
	var fields F
	if len(a.Attrs) == 0 {
		return fields, nil
	}
	if err := json.Unmarshal(a.Attrs, &fields); err != nil {
		return fields, fmt.Errorf("decode fields of account %s: %w", a.ID, err)
	}
	return fields, nil
}
