// Package leasing allocates pooled resources to tasks. A consumer-provided
// Repository is the authority on the pool, its groups, and every live
// acquisition fact — holds, locks, rotation cursors — while the Manager
// rotates unlocked resources through per-group selection strategies over
// caches of it, proving each grant against the store. Several managers over
// one store therefore agree on who holds what by construction, which is what
// lets a deployment run more than one process.
//
// Resource is the model every leasable kind shares: a proxy, an account, a
// card is a struct that embeds it and adds its own fields — a URL, a
// forwarding inbox, card data — around the leasing core. The Manager is
// generic over that model. It reads and writes only the embedded Resource;
// the rest of the model is the consumer's, copied and persisted but never
// inspected here. Everything in this package is about who holds what, and
// for how long.
//
// This package is mechanism, not policy. It guards its pool with the two
// facts the store owns outright — who holds a live lease, who holds a durable
// lock — and asks nothing of any other layer. What a task is, whether one is
// running, and what to do about a task whose lock a deletion released are its
// callers' concerns; deletions report what they unbound and leave the
// response to the caller.
package leasing

import (
	"context"
	"errors"
	"fmt"
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
//
// A kind is one or more ASCII letters, digits, '-', or '_'. The charset is a
// contract, not a style rule: stores file placements under the kind as a JSON
// key and edit them in place through it as a JSON path, where a '.' or '[' has
// meaning of its own and would misfile the placement instead of storing it.
// Validate is the rule; registration and the durable write paths all enforce
// it, so a bad kind fails at the first door rather than corrupting mid-run.
type Kind string

// Validate reports whether k is a legal kind name: non-empty ASCII letters,
// digits, '-', or '_'. See Kind for why the charset is this tight. Consumer
// packages defining their own kind can check the constant once in a test.
func (k Kind) Validate() error {
	if k == "" {
		return errors.New("kind is empty")
	}
	for _, r := range k {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
		default:
			return fmt.Errorf("kind %q contains %q; kinds are ASCII letters, digits, '-', and '_'", string(k), r)
		}
	}
	return nil
}

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
	ID         string `json:"id"`
	GroupID    string `json:"groupId"`
	OwnerID    string `json:"ownerId"`
	MaxHolders int    `json:"maxHolders"`
	// Version is the record's write generation, bumped by the store on every
	// successful Save, ClaimLock, and ReleaseLock. Save is conditional on it,
	// so a writer holding a stale copy loses with ErrStale instead of
	// silently overwriting a concurrent write.
	Version   int64     `json:"version"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
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

// A Hold is one task's durable stake in one resource: the store row that is
// the authority on who leases what. Count is the re-entrant lease depth;
// ExpiresAt is renewed by the holder's heartbeat, and expiry frees the
// capacity — a hold whose holder stopped renewing no longer counts against
// the cap, and once expired it cannot be revived, only re-acquired.
type Hold struct {
	ResourceID string
	TaskID     string
	Count      int
	ExpiresAt  time.Time
}

// Repository is the persistence port: the durable store of one model's
// records, their groups, and the coordination facts shared across processes —
// holds, locks, and counters. The store is the authority on all three; a
// Manager's maps are caches of it. Expiry is always measured against the
// store's own clock, so the clocks of the nodes contending never decide
// capacity.
type Repository[R any] interface {
	List(ctx context.Context) ([]R, error)
	// Save writes the record conditionally on its Version and returns the
	// stored version. Version 0 creates (ErrStale if the id exists — a
	// creation race is a real conflict, not an upsert); Version N replaces
	// iff the stored version is N, preserving CreatedAt; any mismatch, or a
	// row deleted under the writer, is ErrStale.
	Save(ctx context.Context, record R) (int64, error)
	Delete(ctx context.Context, id string) error
	ListGroups(ctx context.Context) ([]Group, error)
	SaveGroup(ctx context.Context, group Group) error
	DeleteGroup(ctx context.Context, id string) error

	// Acquire takes or re-enters a hold: a new task slot is granted iff the
	// count of distinct tasks with unexpired holds stays within cap
	// (cap <= 0 is unlimited — callers pass the resolved policy), returning
	// ErrCapacity when full; the task's own live hold re-enters (Count+1)
	// regardless of cap, and its expired one starts over at Count 1. Either
	// way the lease is refreshed to ttl from the store's now.
	Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (Hold, error)
	// ReleaseHold decrements the task's hold, removing it at zero; no hold
	// is a no-op.
	ReleaseHold(ctx context.Context, resourceID, taskID string) error
	// RenewHolds extends every unexpired hold the task has — the heartbeat
	// primitive. An expired hold is not revived: its capacity may already be
	// promised elsewhere, so the holder must re-acquire.
	RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error
	// ListHolds returns every hold row, expired ones included — the reader
	// filters — ordered by resource then task.
	ListHolds(ctx context.Context) ([]Hold, error)

	// ClaimLock durably binds the resource to the task: OwnerID is set iff
	// it is "" or already the task's, ErrLockHeld otherwise. Locks carry no
	// expiry on purpose: a lock is owned by a task, not a process — a dead
	// node's tasks are recovered elsewhere and their locks legitimately
	// follow, and a detached task heartbeats nowhere yet keeps its bindings.
	// Releasing a dead task's locks is its manager's job, not the clock's.
	ClaimLock(ctx context.Context, resourceID, taskID string) error
	// ReleaseLock clears the lock iff the task owns it; otherwise a no-op.
	ReleaseLock(ctx context.Context, resourceID, taskID string) error

	// Increment atomically adds delta to the named counter under scope — a
	// resource or group id — and returns the new value; a missing counter
	// starts at 0. One primitive serves outcome stats and rotation cursors.
	Increment(ctx context.Context, scope, name string, delta int64) (int64, error)
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

// ErrCapacity is returned by Repository.Acquire when every task slot within
// the cap is taken by an unexpired hold. It is the store-side refusal the
// acquire loop waits out, the way it waits out a full holders map.
var ErrCapacity = errors.New("resource at capacity")

// ErrLockHeld is returned by Repository.ClaimLock when another task owns the
// lock. Nothing frees a lock but its owner's manager, so callers report it
// rather than wait on it.
var ErrLockHeld = errors.New("resource locked by another task")

// ErrStale is returned by Repository.Save when the conditional write lost:
// the caller's Version is behind the store's, the id it meant to create
// exists, or the row it meant to replace is gone. The store's copy won; the
// caller re-reads and decides again.
var ErrStale = errors.New("stale resource write")
