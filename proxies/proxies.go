// Package proxies allocates proxies to tasks. A consumer-provided Repository
// stores the pool and its groups durably while the Manager owns all live
// acquisition state, rotating unlocked proxies through per-group selection
// strategies and honoring durable task-to-proxy locks.
package proxies

import (
	"context"
	"errors"
	"time"
)

// GlobalGroup is the namespace proxies land in when added without a group.
// The manager guarantees it exists; it cannot be deleted.
const GlobalGroup = "global"

// UnlimitedHolders marks a holder policy with no cap: any number of concurrent
// leases is tolerated.
const UnlimitedHolders = -1

// Built-in selection strategy names a Group may reference.
const (
	StrategyRoundRobin = "roundrobin"
	StrategyBayesian   = "bayesian"
)

// A Proxy is the durable record of one proxy. GroupID names the group it
// rotates in (GlobalGroup when empty). OwnerID is the durable lock: the task
// this proxy is bound to, or "" while it rotates in the pool. MaxHolders is
// the proxy's own holder policy: 0 inherits its group's policy, 1 or more caps
// concurrent leases, UnlimitedHolders lifts the cap. Successes and Failures
// are the lease outcomes selection strategies learn from.
type Proxy struct {
	ID         string    `json:"id"`
	URL        string    `json:"url"`
	GroupID    string    `json:"groupId"`
	OwnerID    string    `json:"ownerId"`
	MaxHolders int       `json:"maxHolders"`
	Successes  uint64    `json:"successes"`
	Failures   uint64    `json:"failures"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// A Group is a durable named subset of the pool that leases rotate within.
// Strategy names the selection algorithm its members rotate through
// (StrategyRoundRobin when empty); each group runs its own strategy instance.
// MaxHolders is the default holder policy for members that set none: 0 means
// the default of 1, 1 or more caps concurrent leases per proxy, and
// UnlimitedHolders lifts the cap. A member proxy's own MaxHolders overrides it.
type Group struct {
	ID         string    `json:"id"`
	Strategy   string    `json:"strategy"`
	MaxHolders int       `json:"maxHolders"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
}

// Repository is the persistence port: a dumb durable store of proxies and
// their groups. It tracks no leases — the Manager owns live state and stamps
// UpdatedAt before every write it makes.
type Repository interface {
	List(ctx context.Context) ([]Proxy, error)
	Save(ctx context.Context, proxy Proxy) error
	Delete(ctx context.Context, id string) error
	ListGroups(ctx context.Context) ([]Group, error)
	SaveGroup(ctx context.Context, group Group) error
	DeleteGroup(ctx context.Context, id string) error
}

// An Assignment is the placement a task leases under: the group it rotates
// within (GlobalGroup when empty) and, when pinned, the single member of that
// group it must use. A pin narrows selection to one candidate and bypasses the
// group's strategy entirely — it is the same relation a group expresses, minus
// the rotation. The pinned proxy must belong to GroupID; a mismatch is a
// misconfiguration and is reported, not silently resolved either way.
type Assignment struct {
	TaskID  string
	GroupID string
	ProxyID string
}

// Selection is the strategy port: pick one proxy from the currently-acquirable
// candidates. Stateful strategies guard their own state.
type Selection interface {
	Select(candidates []Proxy) (Proxy, error)
}

// A StrategyFactory builds one Selection instance. The Manager invokes it once
// per group, so every group carries its own strategy state (cursor, sampler).
type StrategyFactory func() Selection

// A Decision is what a DeletionPolicy tells the Manager to do with the task a
// deleted proxy was locked to.
type Decision int

const (
	// Reassign locks the task to a freshly selected proxy from the same group.
	Reassign Decision = iota
	// Unbind leaves the task lockless; it rotates the pool on its next acquire.
	Unbind
	// Fail unbinds the task and surfaces ErrTaskOrphaned to the deleter.
	Fail
)

// DeletionPolicy is the port a module implements to decide the fate of a task
// whose locked proxy is deleted.
//
// It runs while the Manager holds its lock, so it must decide from what it is
// handed. It must not call back into the Manager, nor into a task service
// whose own deletions release proxies: that service holds its registry lock
// across the release, so reaching it from here inverts the lock order and can
// deadlock both. Record the decision and act on it after the delete returns.
type DeletionPolicy interface {
	OnProxyDeleted(ctx context.Context, taskID string, deleted Proxy) Decision
}

// UsagePolicy is the port a module implements to report which live tasks a
// deletion would disrupt. DeleteProxy and DeleteGroup consult it and refuse
// while any of them is running, since tearing a proxy out from under a live
// run strands its in-flight requests. Wire the task service here.
//
// "Running" means actively advancing, not merely started: a suspended task is
// parked between states, so its proxies are editable. That is the escape hatch
// a refusal points at — suspend or kill the task, then delete.
//
// It must not call back into the Manager. The Manager asks it without holding
// its own lock for the same reason a DeletionPolicy must not reach the task
// service: that service holds its registry lock across the Unlock it calls
// back into, so taking both locks in either order can deadlock.
type UsagePolicy interface {
	// RunningTasks returns the ids of every running task leasing from
	// proxyGroupID.
	RunningTasks(ctx context.Context, proxyGroupID string) ([]string, error)
	// TaskIsRunning reports whether the named task is running. It answers for
	// tasks bound to or leasing a single doomed proxy, which may run against
	// some other group entirely — a durable lock outranks the group a task is
	// assigned, so the group question alone would miss them.
	TaskIsRunning(ctx context.Context, taskID string) (bool, error)
	// PinnedTasks returns the ids of every task pinned to proxyID that could
	// still run, whether or not it is running now. A pin lives on the task
	// record, in a store this package does not own, so this is the one link the
	// Manager cannot discover for itself — a durable lock it can see, a pin it
	// cannot. Deleting a pinned proxy is allowed; DeletionImpact reports it so a
	// deliberate deletion can be weighed first.
	PinnedTasks(ctx context.Context, proxyID string) ([]string, error)
}

// Usage adapts plain functions to UsagePolicy. It exists for the wiring order a
// consumer usually has — the manager is built first, the task service that
// answers the questions second — so each func can close over the service and
// resolve it lazily. A nil field reports nothing running, switching off that
// half of the guard.
type Usage struct {
	RunningInGroup func(ctx context.Context, proxyGroupID string) ([]string, error)
	TaskRunning    func(ctx context.Context, taskID string) (bool, error)
	PinnedToProxy  func(ctx context.Context, proxyID string) ([]string, error)
}

// RunningTasks calls u.RunningInGroup, or reports none when it is nil.
func (u Usage) RunningTasks(ctx context.Context, proxyGroupID string) ([]string, error) {
	if u.RunningInGroup == nil {
		return nil, nil
	}
	return u.RunningInGroup(ctx, proxyGroupID)
}

// TaskIsRunning calls u.TaskRunning, or reports false when it is nil.
func (u Usage) TaskIsRunning(ctx context.Context, taskID string) (bool, error) {
	if u.TaskRunning == nil {
		return false, nil
	}
	return u.TaskRunning(ctx, taskID)
}

// PinnedTasks calls u.PinnedToProxy, or reports none when it is nil.
func (u Usage) PinnedTasks(ctx context.Context, proxyID string) ([]string, error) {
	if u.PinnedToProxy == nil {
		return nil, nil
	}
	return u.PinnedToProxy(ctx, proxyID)
}

// ErrNoProxies is returned by acquires when the group has no proxies at all.
// It fails rather than waiting for one to be added: an assignment to a group
// that could be satisfied by a proxy freeing is worth blocking on, but an empty
// group is a misconfiguration far more often than a swap in progress, and
// blocking on it would hide the mistake as a hang.
var ErrNoProxies = errors.New("no proxies available")

// ErrGroupNotFound is returned when an operation names a group the manager
// does not know.
var ErrGroupNotFound = errors.New("proxy group not found")

// ErrGroupInUse is returned by DeleteGroup when a running task leases from the
// group, or when a member is locked to or held by one. Suspend or kill the
// task first.
var ErrGroupInUse = errors.New("proxy group in use by a running task")

// ErrProxyInUse is returned by DeleteProxy when a running task holds a lease on
// the proxy, is locked to it, or leases from its group. Suspend or kill the
// task first.
var ErrProxyInUse = errors.New("proxy in use by a running task")

// ErrProxyNotFound is returned when an assignment pins a proxy the manager does
// not know — deleted while the task was down, or never added. It is the signal
// a recovered task's fallback policy acts on: run proxyless, rotate the group
// instead, or refuse to run at all.
var ErrProxyNotFound = errors.New("pinned proxy not found")

// ErrProxyNotInGroup is returned when an assignment pins a proxy that is not a
// member of the group it names. Left to resolve itself either way it would
// quietly move a task off its assigned pool, or off its pin.
var ErrProxyNotInGroup = errors.New("pinned proxy is not in the assigned group")

// ErrProxyLocked is returned when an assignment pins a proxy another task holds
// a durable lock on. Nothing frees it on its own — unlike a lease, which the
// acquire loop waits out — so this fails rather than blocking on a condition
// that will not arrive.
var ErrProxyLocked = errors.New("pinned proxy is locked to another task")

// ErrPinConflict is returned when a task's durable lock names one proxy and its
// assignment pins another. A lease must never drop a durable lock as a side
// effect, so the fix is to reassign the task — see Manager.ReleaseStaleLock and
// the task service's AssignProxy, which release the stale lock deliberately.
var ErrPinConflict = errors.New("locked proxy conflicts with pinned proxy")

// ErrTaskOrphaned is returned when a deletion's policy decides Fail, so the
// deleter can kill or quarantine the named task.
var ErrTaskOrphaned = errors.New("task orphaned by proxy deletion")
