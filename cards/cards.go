// Package cards allocates payment instruments to tasks. It is the leasing
// package specialized to one resource kind: a consumer-provided Repository
// stores the cards and their groups durably while the Manager owns all live
// acquisition state, handing out unlocked cards within a group and honoring
// durable task-to-card locks.
//
// What a card *is* belongs to the workflow. Its fields — number, expiry, CVV,
// billing address, whatever the checkout asks for — travel as opaque JSON, so a
// new workflow needs no schema change; Bind decodes them into a struct at the
// point of use.
//
// This package never reads those fields and never protects them. Encryption at
// rest is the Repository implementer's job: a store that seals Card.Fields on
// the way down and opens it on the way up is transparent to everything above,
// and the shipped SQLite adapter does neither.
//
// Like accounts and unlike proxies, cards have no selection strategy to
// configure. Members of a group are handed out in turn and there is nothing to
// tune: a card is one instrument with one balance and one name on it, so which
// one a task gets matters far less than that no two tasks charge the same one
// at once.
package cards

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/leasing"
)

// GlobalGroup is the namespace cards land in when added without a group. The
// manager guarantees it exists; it cannot be deleted.
const GlobalGroup = leasing.GlobalGroup

// UnlimitedHolders marks a holder policy with no cap: any number of concurrent
// leases is tolerated.
const UnlimitedHolders = leasing.UnlimitedHolders

// A Card is the durable record of one payment instrument. GroupID names the
// group it is handed out from (GlobalGroup when empty), which is normally the
// pool it was issued for — a BIN, a funding source, the set allotted to one
// drop. OwnerID is the durable lock: the task this card is bound to, or "" while
// it returns to the pool between runs. MaxHolders is the card's own holder
// policy: 0 inherits its group's policy, 1 or more caps concurrent leases,
// UnlimitedHolders lifts the cap — but two tasks inside one card at a time is a
// duplicate charge waiting to happen, not a throughput win. Successes and
// Failures are the lease outcomes, which is how a declining card becomes
// visible.
//
// Fields carries whatever the checkout needs to pay, as JSON this package never
// reads. It is stored verbatim: card data is only as protected as the
// Repository behind it.
type Card struct {
	ID         string          `json:"id"`
	GroupID    string          `json:"groupId"`
	OwnerID    string          `json:"ownerId"`
	MaxHolders int             `json:"maxHolders"`
	Successes  uint64          `json:"successes"`
	Failures   uint64          `json:"failures"`
	Fields     json.RawMessage `json:"fields,omitempty"`
	CreatedAt  time.Time       `json:"createdAt"`
	UpdatedAt  time.Time       `json:"updatedAt"`
}

// Bind decodes a card's workflow-defined fields into F. A card with no fields
// yields the zero F rather than an error, so a workflow that needs none can
// ignore them entirely.
func Bind[F any](c Card) (F, error) {
	var fields F
	if len(c.Fields) == 0 {
		return fields, nil
	}
	if err := json.Unmarshal(c.Fields, &fields); err != nil {
		return fields, fmt.Errorf("decode fields of card %s: %w", c.ID, err)
	}
	return fields, nil
}

// A Group is a durable named subset of the cards that leases are handed out
// within — usually one funding source, BIN, or allotment. MaxHolders is the
// default holder policy for members that set none: 0 means the default of 1, 1
// or more caps concurrent leases per card, and UnlimitedHolders lifts the cap. A
// member card's own MaxHolders overrides it.
type Group struct {
	ID         string    `json:"id"`
	MaxHolders int       `json:"maxHolders"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// Repository is the persistence port: a dumb durable store of cards and their
// groups. It tracks no leases — the Manager owns live state and stamps
// UpdatedAt before every write it makes.
//
// It is also the only place card data can be protected. An implementation that
// encrypts Card.Fields on the way down and decrypts it on the way up is
// transparent to everything above; the shipped SQLite adapter does neither.
type Repository interface {
	List(ctx context.Context) ([]Card, error)
	Save(ctx context.Context, card Card) error
	Delete(ctx context.Context, id string) error
	ListGroups(ctx context.Context) ([]Group, error)
	SaveGroup(ctx context.Context, group Group) error
	DeleteGroup(ctx context.Context, id string) error
}

// An Assignment is the placement a task leases under: the group it draws from
// (GlobalGroup when empty) and, when pinned, the single member of that group it
// must use. Pinning is the common case here — a checkout that has to settle on
// the card it was quoted against — and bypasses rotation entirely. The pinned
// card must belong to GroupID; a mismatch is a misconfiguration and is
// reported, not silently resolved either way.
type Assignment struct {
	TaskID  string
	GroupID string
	CardID  string
}

// A Decision is what a DeletionPolicy tells the Manager to do with the task a
// deleted card was locked to.
type Decision = leasing.Decision

const (
	// Reassign locks the task to a fresh card from the same group. For cards
	// this is rarely what you want: a checkout half-submitted against one
	// instrument does not usually survive being handed another.
	Reassign = leasing.Reassign
	// Unbind leaves the task lockless; it draws from the pool on its next
	// acquire.
	Unbind = leasing.Unbind
	// Fail unbinds the task and surfaces ErrTaskOrphaned to the deleter.
	Fail = leasing.Fail
)

// DeletionPolicy is the port a module implements to decide the fate of a task
// whose locked card is deleted.
//
// It runs while the Manager holds its lock, so it must decide from what it is
// handed. It must not call back into the Manager, nor into a task service whose
// own deletions release cards: that service holds its registry lock across the
// release, so reaching it from here inverts the lock order and can deadlock
// both. Record the decision and act on it after the delete returns.
//
// It is handed the deleted card, fields and all. A policy that logs what a task
// lost should say which card by ID, not by number.
type DeletionPolicy interface {
	OnCardDeleted(ctx context.Context, taskID string, deleted Card) Decision
}

// UsagePolicy is the port a module implements to report which live tasks a
// deletion would disrupt. DeleteCard and DeleteGroup consult it and refuse while
// any of them is running, since taking a card away mid-checkout strands the run
// inside it — possibly between authorization and capture. Wire the task service
// here.
//
// "Running" means actively advancing, not merely started: a suspended task is
// parked between states. That is the escape hatch a refusal points at — suspend
// or kill the task, then delete.
//
// It must not call back into the Manager, for the lock-ordering reason
// DeletionPolicy documents.
type UsagePolicy = leasing.UsagePolicy

// Usage adapts plain functions to UsagePolicy. It exists for the wiring order a
// consumer usually has — the manager is built first, the task service that
// answers the questions second — so each func can close over the service and
// resolve it lazily. A nil field reports nothing running, switching off that
// half of the guard.
//
// With cards acquired by the workflow rather than assigned on the task record,
// TaskRunning alone is the whole guard worth wiring: a card is only ever reached
// through a lease or a lock, both of which the Manager already sees.
// RunningInGroup and PinnedToCard answer questions that only a task store
// carrying card placements can.
type Usage struct {
	RunningInGroup func(ctx context.Context, cardGroupID string) ([]string, error)
	TaskRunning    func(ctx context.Context, taskID string) (bool, error)
	PinnedToCard   func(ctx context.Context, cardID string) ([]string, error)
}

// RunningTasks calls u.RunningInGroup, or reports none when it is nil.
func (u Usage) RunningTasks(ctx context.Context, cardGroupID string) ([]string, error) {
	if u.RunningInGroup == nil {
		return nil, nil
	}
	return u.RunningInGroup(ctx, cardGroupID)
}

// TaskIsRunning calls u.TaskRunning, or reports false when it is nil.
func (u Usage) TaskIsRunning(ctx context.Context, taskID string) (bool, error) {
	if u.TaskRunning == nil {
		return false, nil
	}
	return u.TaskRunning(ctx, taskID)
}

// PinnedTasks calls u.PinnedToCard, or reports none when it is nil.
func (u Usage) PinnedTasks(ctx context.Context, cardID string) ([]string, error) {
	if u.PinnedToCard == nil {
		return nil, nil
	}
	return u.PinnedToCard(ctx, cardID)
}

// An Impact is what deleting a card, or a whole group, would cost the tasks
// linked to it. Running names the tasks the deletion is refused for; Pinned
// names the resumable tasks that would keep their assignment and be unable to
// run under it until they are reassigned. Pinned is a warning, not a refusal —
// retiring a card is a deliberate act, and what it costs is the deleter's call.
type Impact = leasing.Impact

// ErrNoCards is returned by acquires when the group has no cards at all. It
// fails rather than waiting for one to be added: an assignment to a group that
// could be satisfied by a card freeing is worth blocking on, but an empty group
// is a misconfiguration far more often than a swap in progress, and blocking on
// it would hide the mistake as a hang.
var ErrNoCards = leasing.ErrNoResources

// ErrGroupNotFound is returned when an operation names a group the manager does
// not know.
var ErrGroupNotFound = leasing.ErrGroupNotFound

// ErrGroupInUse is returned by DeleteGroup when a running task leases from the
// group, or when a member is locked to or held by one. Suspend or kill the task
// first.
var ErrGroupInUse = leasing.ErrGroupInUse

// ErrCardInUse is returned by DeleteCard when a running task holds a lease on
// the card, is locked to it, or leases from its group. Suspend or kill the task
// first.
var ErrCardInUse = leasing.ErrResourceInUse

// ErrCardNotFound is returned when an assignment pins a card the manager does
// not know — deleted while the task was down, or never added. It is the signal a
// recovered task's fallback policy acts on: draw another, or refuse to run at
// all.
var ErrCardNotFound = leasing.ErrResourceNotFound

// ErrCardNotInGroup is returned when an assignment pins a card that is not a
// member of the group it names. Left to resolve itself either way it would
// quietly charge the wrong pool's instruments, or drop the task off its pin.
var ErrCardNotInGroup = leasing.ErrResourceNotInGroup

// ErrCardLocked is returned when an assignment pins a card another task holds a
// durable lock on. Nothing frees it on its own — unlike a lease, which the
// acquire loop waits out — so this fails rather than blocking on a condition
// that will not arrive.
var ErrCardLocked = leasing.ErrResourceLocked

// ErrPinConflict is returned when a task's durable lock names one card and its
// assignment pins another. A lease must never drop a durable lock as a side
// effect, so the fix is to reassign the task — see Manager.ReleaseStaleLock.
var ErrPinConflict = leasing.ErrPinConflict

// ErrTaskOrphaned is returned when a deletion's policy decides Fail, so the
// deleter can kill or quarantine the named task.
var ErrTaskOrphaned = leasing.ErrTaskOrphaned

// Errors here are the leasing package's, shared with every other resource kind
// built on it. errors.Is answers "was there nothing to hand out", not "was it a
// card or a proxy" — which the manager you called already tells you.

// toResource and fromResource translate between this package's Card and the
// leasing core's resource record. Fields is cloned in both directions: the core
// copies resources freely, and a shared backing array would let one holder's
// edit reach the pool.
func toResource(c Card) leasing.Resource[json.RawMessage] {
	return leasing.Resource[json.RawMessage]{
		ID:         c.ID,
		GroupID:    c.GroupID,
		OwnerID:    c.OwnerID,
		MaxHolders: c.MaxHolders,
		Successes:  c.Successes,
		Failures:   c.Failures,
		Attrs:      cloneFields(c.Fields),
		CreatedAt:  c.CreatedAt,
		UpdatedAt:  c.UpdatedAt,
	}
}

func fromResource(r leasing.Resource[json.RawMessage]) Card {
	return Card{
		ID:         r.ID,
		GroupID:    r.GroupID,
		OwnerID:    r.OwnerID,
		MaxHolders: r.MaxHolders,
		Successes:  r.Successes,
		Failures:   r.Failures,
		Fields:     cloneFields(r.Attrs),
		CreatedAt:  r.CreatedAt,
		UpdatedAt:  r.UpdatedAt,
	}
}

func cloneFields(fields json.RawMessage) json.RawMessage {
	if fields == nil {
		return nil
	}
	cloned := make(json.RawMessage, len(fields))
	copy(cloned, fields)
	return cloned
}

// toGroup and fromGroup translate groups. The core carries a strategy name per
// group; this package pins every one of them to the same rotation and does not
// expose the knob, so the name is filled in on the way down and dropped on the
// way up.
func toGroup(g Group) leasing.Group {
	return leasing.Group{
		ID:         g.ID,
		Strategy:   rotation,
		MaxHolders: g.MaxHolders,
		CreatedAt:  g.CreatedAt,
		UpdatedAt:  g.UpdatedAt,
	}
}

func fromGroup(g leasing.Group) Group {
	return Group{
		ID:         g.ID,
		MaxHolders: g.MaxHolders,
		CreatedAt:  g.CreatedAt,
		UpdatedAt:  g.UpdatedAt,
	}
}
