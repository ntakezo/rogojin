// Package cards allocates payment instruments to tasks. It is the leasing
// package specialized to one resource kind: the payload is the card's fields —
// number, expiry, CVV, billing address, whatever the checkout asks for —
// travelling as opaque JSON, so a new workflow needs no schema change; Bind
// decodes them into a struct at the point of use.
//
// The types here are aliases, not wrappers: a Card is a leasing.Resource, the
// Manager is a leasing.Manager, and every behavior — groups, holder caps,
// durable locks, pins, lease-guarded deletes — is documented there. Like
// accounts and unlike proxies, cards register no strategy of their own: a card
// is one instrument with one balance and one name on it, so which one a task
// gets matters far less than that no two tasks charge the same one at once,
// and round robin is exactly that.
//
// Fields are stored verbatim, in the clear: card data is only as protected as
// the Repository behind it. An implementation that encrypts Attrs on the way
// down and decrypts it on the way up is transparent to everything above. Treat
// a leased card's Attrs as read-only — the pool shares the backing bytes.
package cards

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/leasing"
)

// GlobalGroup is the namespace cards land in when added without a group; a
// non-global group is usually one funding source, BIN, or allotment.
const GlobalGroup = leasing.GlobalGroup

// UnlimitedHolders lifts a card's holder cap — but a card is one instrument,
// and two tasks charging it at once is usually a bug, not a throughput win.
const UnlimitedHolders = leasing.UnlimitedHolders

// A Card is the durable record of one payment instrument; its
// workflow-defined fields live in Attrs as JSON this package never reads.
type Card = leasing.Resource[json.RawMessage]

// A Group is a durable named subset of the cards — usually one funding source.
type Group = leasing.Group

// Repository is the persistence port: a dumb durable store of cards and their
// groups, and the only place card data can be protected.
type Repository = leasing.Repository[json.RawMessage]

// An Assignment is the placement a task leases under; the pin names a card.
type Assignment = leasing.Assignment

// A Manager allocates cards to tasks. See leasing.Manager.
type Manager = leasing.Manager[json.RawMessage]

// A Lease is a live hold on one card. Release it exactly once when done.
type Lease = leasing.Lease[json.RawMessage]

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent.
func NewManager(ctx context.Context, repo Repository) (*Manager, error) {
	return leasing.NewManager(ctx, repo)
}

// Bind decodes a card's workflow-defined fields into F. A card with no fields
// yields the zero F rather than an error, so a workflow that needs none can
// ignore them entirely.
func Bind[F any](c Card) (F, error) {
	var fields F
	if len(c.Attrs) == 0 {
		return fields, nil
	}
	if err := json.Unmarshal(c.Attrs, &fields); err != nil {
		return fields, fmt.Errorf("decode fields of card %s: %w", c.ID, err)
	}
	return fields, nil
}
