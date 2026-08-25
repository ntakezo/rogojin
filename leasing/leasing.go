// Package leasing allocates pooled resources to tasks. It is the layer the
// proxies and accounts modules are built on: a consumer-provided Repository
// stores the pool and its groups durably while the Manager owns all live
// acquisition state, rotating unlocked resources through per-group selection
// strategies and honoring durable task-to-resource locks.
//
// The resource kind is a type parameter carrying whatever payload the module
// needs — a proxy's URL, an account's credentials. This package never inspects
// it; everything here is about who holds what, and for how long.
package leasing

import (
	"context"
	"errors"
	"time"
)

// GlobalGroup is the namespace resources land in when added without a group.
// The manager guarantees it exists; it cannot be deleted.
const GlobalGroup = "global"

// UnlimitedHolders marks a holder policy with no cap: any number of concurrent
// leases is tolerated.
const UnlimitedHolders = -1

// StrategyRoundRobin names the one strategy this package installs on its own,
// for a Config that configures none.
const StrategyRoundRobin = "roundrobin"

// A Resource is the durable record of one leasable thing. GroupID names the
// group it rotates in (GlobalGroup when empty). OwnerID is the durable lock:
// the task this resource is bound to, or "" while it rotates in the pool.
// MaxHolders is the resource's own holder policy: 0 inherits its group's
// policy, 1 or more caps concurrent leases, UnlimitedHolders lifts the cap.
// Successes and Failures are the lease outcomes selection strategies learn
// from. Attrs is the module's payload, which this package only ever copies.
type Resource[T any] struct {
	ID         string    `json:"id"`
	GroupID    string    `json:"groupId"`
	OwnerID    string    `json:"ownerId"`
	MaxHolders int       `json:"maxHolders"`
	Successes  uint64    `json:"successes"`
	Failures   uint64    `json:"failures"`
	Attrs      T         `json:"attrs"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// A Group is a durable named subset of the pool that leases rotate within.
// Strategy names the selection algorithm its members rotate through (the
// manager's default when empty); each group runs its own strategy instance.
// MaxHolders is the default holder policy for members that set none: 0 means
// the default of 1, 1 or more caps concurrent leases per resource, and
// UnlimitedHolders lifts the cap. A member's own MaxHolders overrides it.
type Group struct {
	ID         string    `json:"id"`
	Strategy   string    `json:"strategy"`
	MaxHolders int       `json:"maxHolders"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// Repository is the persistence port: a dumb durable store of resources and
// their groups. It tracks no leases — the Manager owns live state and stamps
// UpdatedAt before every write it makes.
type Repository[T any] interface {
	List(ctx context.Context) ([]Resource[T], error)
	Save(ctx context.Context, resource Resource[T]) error
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

// Selection is the strategy port: pick one resource from the currently-
// acquirable candidates. Stateful strategies guard their own state.
type Selection[T any] interface {
	Select(candidates []Resource[T]) (Resource[T], error)
}

// A StrategyFactory builds one Selection instance. The Manager invokes it once
// per group, so every group carries its own strategy state (cursor, sampler).
type StrategyFactory[T any] func() Selection[T]

// A Decision is what a DeletionPolicy tells the Manager to do with the task a
// deleted resource was locked to.
type Decision int

const (
	// Reassign locks the task to a freshly selected resource from the same group.
	Reassign Decision = iota
	// Unbind leaves the task lockless; it rotates the pool on its next acquire.
	Unbind
	// Fail unbinds the task and surfaces ErrTaskOrphaned to the deleter.
	Fail
)

// DeletionPolicy is the port a module implements to decide the fate of a task
// whose locked resource is deleted.
//
// It runs while the Manager holds its lock, so it must decide from what it is
// handed. It must not call back into the Manager, nor into a task service
// whose own deletions release resources: that service holds its registry lock
// across the release, so reaching it from here inverts the lock order and can
// deadlock both. Record the decision and act on it after the delete returns.
type DeletionPolicy[T any] interface {
	OnDeleted(ctx context.Context, taskID string, deleted Resource[T]) Decision
}

// UsagePolicy is the port a module implements to report which live tasks a
// deletion would disrupt. Delete and DeleteGroup consult it and refuse while
// any of them is running, since tearing a resource out from under a live run
// strands its in-flight requests. Wire the task service here.
//
// "Running" means actively advancing, not merely started: a suspended task is
// parked between states, so its resources are editable. That is the escape
// hatch a refusal points at — suspend or kill the task, then delete.
//
// It must not call back into the Manager. The Manager asks it without holding
// its own lock for the same reason a DeletionPolicy must not reach the task
// service: that service holds its registry lock across the Unlock it calls
// back into, so taking both locks in either order can deadlock.
type UsagePolicy interface {
	// RunningTasks returns the ids of every running task leasing from groupID.
	RunningTasks(ctx context.Context, groupID string) ([]string, error)
	// TaskIsRunning reports whether the named task is running. It answers for
	// tasks bound to or leasing a single doomed resource, which may run against
	// some other group entirely — a durable lock outranks the group a task is
	// assigned, so the group question alone would miss them.
	TaskIsRunning(ctx context.Context, taskID string) (bool, error)
	// PinnedTasks returns the ids of every task pinned to resourceID that could
	// still run, whether or not it is running now. A pin lives on the task
	// record, in a store this package does not own, so this is the one link the
	// Manager cannot discover for itself — a durable lock it can see, a pin it
	// cannot. Deleting a pinned resource is allowed; DeletionImpact reports it
	// so a deliberate deletion can be weighed first.
	PinnedTasks(ctx context.Context, resourceID string) ([]string, error)
}

// Usage adapts plain functions to UsagePolicy. It exists for the wiring order a
// consumer usually has — the manager is built first, the task service that
// answers the questions second — so each func can close over the service and
// resolve it lazily. A nil field reports nothing running, switching off that
// half of the guard.
type Usage struct {
	RunningInGroup   func(ctx context.Context, groupID string) ([]string, error)
	TaskRunning      func(ctx context.Context, taskID string) (bool, error)
	PinnedToResource func(ctx context.Context, resourceID string) ([]string, error)
}

// RunningTasks calls u.RunningInGroup, or reports none when it is nil.
func (u Usage) RunningTasks(ctx context.Context, groupID string) ([]string, error) {
	if u.RunningInGroup == nil {
		return nil, nil
	}
	return u.RunningInGroup(ctx, groupID)
}

// TaskIsRunning calls u.TaskRunning, or reports false when it is nil.
func (u Usage) TaskIsRunning(ctx context.Context, taskID string) (bool, error) {
	if u.TaskRunning == nil {
		return false, nil
	}
	return u.TaskRunning(ctx, taskID)
}

// PinnedTasks calls u.PinnedToResource, or reports none when it is nil.
func (u Usage) PinnedTasks(ctx context.Context, resourceID string) ([]string, error) {
	if u.PinnedToResource == nil {
		return nil, nil
	}
	return u.PinnedToResource(ctx, resourceID)
}

// An Impact is what deleting a resource, or a whole group, would cost the tasks
// linked to it. Running names the tasks the deletion is refused for; Pinned
// names the resumable tasks that would keep their assignment and be unable to
// run under it until they are reassigned. Pinned is a warning, not a refusal —
// deleting a resource is a deliberate act, and what it costs is the deleter's
// call.
type Impact struct {
	Running []string
	Pinned  []string
}

// Empty reports whether the deletion would disturb no task at all.
func (i Impact) Empty() bool {
	return len(i.Running) == 0 && len(i.Pinned) == 0
}

// ErrNoResources is returned by acquires when the group has no resources at
// all. It fails rather than waiting for one to be added: an assignment to a
// group that could be satisfied by a resource freeing is worth blocking on, but
// an empty group is a misconfiguration far more often than a swap in progress,
// and blocking on it would hide the mistake as a hang.
var ErrNoResources = errors.New("no resources available")

// ErrGroupNotFound is returned when an operation names a group the manager
// does not know.
var ErrGroupNotFound = errors.New("group not found")

// ErrGroupInUse is returned by DeleteGroup when a running task leases from the
// group, or when a member is locked to or held by one. Suspend or kill the
// task first.
var ErrGroupInUse = errors.New("group in use by a running task")

// ErrResourceInUse is returned by Delete when a running task holds a lease on
// the resource, is locked to it, or leases from its group. Suspend or kill the
// task first.
var ErrResourceInUse = errors.New("resource in use by a running task")

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

// ErrTaskOrphaned is returned when a deletion's policy decides Fail, so the
// deleter can kill or quarantine the named task.
var ErrTaskOrphaned = errors.New("task orphaned by resource deletion")
