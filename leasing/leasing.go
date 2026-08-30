// Package leasing allocates pooled resources to tasks. A consumer-provided
// Repository stores the pool and its groups durably while the Manager owns all
// live acquisition state, rotating unlocked resources through per-group
// selection strategies and honoring durable task-to-resource locks.
//
// Resource is the model every leasable kind shares: a proxy, an account, a
// card is a struct that embeds it and adds its own fields — a URL, a
// forwarding inbox, card data — around the leasing core. The Manager is
// generic over that model. It reads and writes only the embedded Resource;
// the rest of the model is the consumer's, copied and persisted but never
// inspected here. Everything in this package is about who holds what, and
// for how long.
//
// This package is mechanism, not policy. It guards its pool with the two facts
// it owns outright — who holds a live lease, who holds a durable lock — and
// asks nothing of any other layer. What a task is, whether one is running, and
// what to do about a task whose lock a deletion released are its callers'
// concerns; deletions report what they unbound and leave the response to the
// caller.
package leasing

import (
	"context"
	"errors"
	"time"
)

// GlobalGroup is the namespace resources land in when added without a group.
// The manager guarantees it exists; it cannot be deleted.
const GlobalGroup = "global"

// A Kind names one resource kind: one leasing manager and the pool it guards.
// Each resource package publishes its own as a typed constant (proxies.Kind,
// accounts.Kind, payments.Kind) — the key placements, registrations, and a
// workflow's manager lookups are all filed under, so every layer agrees on
// the name by construction.
type Kind string

// UnlimitedHolders marks a holder policy with no cap: any number of concurrent
// leases is tolerated.
const UnlimitedHolders = -1

// StrategyRoundRobin names the strategy every Manager installs and defaults
// to: a group naming no strategy rotates round robin.
const StrategyRoundRobin = "roundrobin"

// A Resource is the leasing core of one leasable thing, embedded by every
// model this package manages. GroupID names the group it rotates in
// (GlobalGroup when empty). OwnerID is the durable lock: the task this
// resource is bound to, or "" while it rotates in the pool. MaxHolders is the
// resource's holder cap — the one home that policy has: 0 means the default of
// 1, more caps concurrent leases, UnlimitedHolders lifts the cap.
//
// A model embeds it by value and adds its own fields alongside:
//
//	type Proxy struct {
//		leasing.Resource
//		URL string
//	}
//
// The embedded fields promote, so a Proxy's ID is p.ID; JSON encodes them
// flat, at the same level as the model's own.
type Resource struct {
	ID         string    `json:"id"`
	GroupID    string    `json:"groupId"`
	OwnerID    string    `json:"ownerId"`
	MaxHolders int       `json:"maxHolders"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// core returns the leasing record itself. It is the one method Leasable asks
// for, and it is unexported on purpose: a type in another package can only
// carry it by embedding Resource, so satisfying the constraint and embedding
// the model are the same act.
func (r *Resource) core() *Resource { return r }

// Leasable is the constraint a Manager's model satisfies: a pointer to a
// struct that embeds Resource. The pointer is what lets the manager write the
// leasing fields — lock owner, group, timestamps — of a model it
// otherwise never looks inside. Consumers never name it; it is inferred at
// every call site, and spelled out only where a type alias fixes the model:
//
//	type Manager = leasing.Manager[Proxy, *Proxy]
type Leasable[R any] interface {
	*R
	core() *Resource
}

// A Group is a durable named subset of the pool that leases rotate within.
// Strategy names the selection algorithm its members rotate through
// (StrategyRoundRobin when empty); each group runs its own strategy instance.
// Refs are opaque references to things outside this package, keyed by a
// consumer-chosen name — like a model's own fields, they are carried and
// persisted but never read here.
type Group struct {
	ID        string            `json:"id"`
	Strategy  string            `json:"strategy"`
	Refs      map[string]string `json:"refs,omitempty"`
	CreatedAt time.Time         `json:"createdAt"`
	UpdatedAt time.Time         `json:"updatedAt"`
}

// Repository is the persistence port: a dumb durable store of one model's
// records and their groups. It tracks no leases — the Manager owns live state
// and stamps UpdatedAt before every write it makes.
type Repository[R any] interface {
	List(ctx context.Context) ([]R, error)
	Save(ctx context.Context, record R) error
	Delete(ctx context.Context, id string) error
	ListGroups(ctx context.Context) ([]Group, error)
	SaveGroup(ctx context.Context, group Group) error
	DeleteGroup(ctx context.Context, id string) error
}

// An Assignment is the placement a task leases under: the group it rotates
// within (GlobalGroup when empty) and, when pinned, the single member of that
// group it must use. A pin narrows selection to one candidate and bypasses the
// group's strategy entirely — it is the same relation a group expresses, minus
// the rotation. The pinned resource must belong to GroupID; a mismatch is a
// misconfiguration and is reported, not silently resolved either way.
type Assignment struct {
	TaskID     string
	GroupID    string
	ResourceID string
}

// Selection is the strategy port: pick one record from the currently-
// acquirable candidates. Stateful strategies guard their own state.
type Selection[R any] interface {
	Select(candidates []R) (R, error)
}

// A StrategyFactory builds one Selection instance. The Manager invokes it once
// per group, so every group carries its own strategy state (cursor, sampler).
type StrategyFactory[R any] func() Selection[R]

// ErrNoResources is returned by acquires when the group has no resources at
// all. It fails rather than waiting for one to be added: an assignment to a
// group that could be satisfied by a resource freeing is worth blocking on, but
// an empty group is a misconfiguration far more often than a swap in progress,
// and blocking on it would hide the mistake as a hang.
var ErrNoResources = errors.New("no resources available")

// ErrGroupNotFound is returned when an operation names a group the manager
// does not know.
var ErrGroupNotFound = errors.New("group not found")

// ErrGroupInUse is returned by DeleteGroup while any live lease is held on a
// member. A lease is the fact of use: whoever holds it has the resource wired
// into work in flight, and tearing it out from under them is the one mistake a
// delete cannot take back. The lease being released — however its holder gets
// there — is what frees the group.
var ErrGroupInUse = errors.New("group has a live lease on a member")

// ErrResourceInUse is returned by Delete while any live lease is held on the
// resource. See ErrGroupInUse.
var ErrResourceInUse = errors.New("resource has a live lease")

// ErrResourceNotFound is returned when an assignment pins a resource the
// manager does not know — deleted while the task was down, or never added. It
// is the signal a recovered task's fallback policy acts on: run without one,
// rotate the group instead, or refuse to run at all.
var ErrResourceNotFound = errors.New("pinned resource not found")

// ErrResourceNotInGroup is returned when an assignment pins a resource that is
// not a member of the group it names. Left to resolve itself either way it
// would quietly move a task off its assigned pool, or off its pin.
var ErrResourceNotInGroup = errors.New("pinned resource is not in the assigned group")

// ErrResourceLocked is returned when an assignment pins a resource another task
// holds a durable lock on. Nothing frees it on its own — unlike a lease, which
// the acquire loop waits out — so this fails rather than blocking on a
// condition that will not arrive.
var ErrResourceLocked = errors.New("pinned resource is locked to another task")

// ErrPinConflict is returned when a task's durable lock names one resource and
// its assignment pins another. A lease must never drop a durable lock as a side
// effect, so the fix is to reassign the task — see Manager.ReleaseStaleLock.
var ErrPinConflict = errors.New("locked resource conflicts with pinned resource")
