// Package payments allocates payment instruments to tasks. It is the leasing
// package specialized to one model: a Payment embeds leasing.Resource — the
// pool, group, and lock fields every leasable kind shares — and
// adds the payment's fields: number, expiry, CVV, billing address, whatever the
// checkout asks for, travelling as opaque JSON so a new workflow needs no
// schema change. Bind decodes them into a struct at the point of use.
//
// Every leasing behavior — groups, holder caps, durable locks, pins,
// lease-guarded deletes — is documented in the leasing package; the Manager
// here is that manager over this model. Like accounts and unlike proxies,
// payments register no strategy of their own: a payment is one instrument with one
// balance and one name on it, so which one a task gets matters far less than
// that no two tasks charge the same one at once, and round robin is exactly
// that.
//
// Fields are stored verbatim, in the clear: payment data is only as protected as
// the Repository behind it. An implementation that encrypts Fields on the way
// down and decrypts them on the way up is transparent to everything above.
// Treat a leased payment's Fields as read-only — the pool shares the backing
// bytes.
package payments

import (
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/leasing"
)

// Kind is the resource kind payments register with the task manager under —
// the key a task's payment placement is filed on. See proxies.Kind.
const Kind leasing.Kind = "payment"

// GlobalGroup is the namespace payments land in when added without a group; a
// non-global group is usually one funding source, BIN, or allotment.
const GlobalGroup = leasing.GlobalGroup

// UnlimitedHolders lifts a payment's holder cap — but a payment is one instrument,
// and two tasks charging it at once is usually a bug, not a throughput win.
const UnlimitedHolders = leasing.UnlimitedHolders

// A Payment is the durable record of one payment instrument: the leasing core
// and the workflow-defined fields, JSON this package never reads.
type Payment struct {
	leasing.Resource
	// Fields is the workflow's payload; this package never reads it.
	Fields json.RawMessage `json:"fields,omitempty"`
}

// A Group is a durable named subset of the payments — usually one funding source.
type Group = leasing.Group

// Repository is the persistence port: a dumb durable store of payments and their
// groups, and the only place payment data can be protected.
type Repository = leasing.Repository[Payment]

// An Assignment is the placement a task leases under; the pin names a payment.
type Assignment = leasing.Assignment

// Bind decodes a payment's workflow-defined fields into F. A payment with no fields
// yields the zero F rather than an error, so a workflow that needs none can
// ignore them entirely.
func Bind[F any](c Payment) (F, error) {
	var fields F
	if len(c.Fields) == 0 {
		return fields, nil
	}
	if err := json.Unmarshal(c.Fields, &fields); err != nil {
		return fields, fmt.Errorf("decode fields of payment %s: %w", c.ID, err)
	}
	return fields, nil
}
