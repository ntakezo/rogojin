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

// UsagePolicy is the port a module implements to report which live tasks lease
// from a proxy group. DeleteGroup consults it and refuses while any task is
// running, since tearing the pool out from under a live run would strand its
// in-flight requests. Wire the task service here. It must not call back into
// the Manager.
type UsagePolicy interface {
	RunningTasks(ctx context.Context, proxyGroupID string) ([]string, error)
}

// UsageFunc adapts a function to UsagePolicy. It exists for the wiring order a
// consumer usually has — the manager is built first, the task service that
// answers the question second — so the func can close over the service and
// resolve it lazily.
type UsageFunc func(ctx context.Context, proxyGroupID string) ([]string, error)

// RunningTasks calls f.
func (f UsageFunc) RunningTasks(ctx context.Context, proxyGroupID string) ([]string, error) {
	return f(ctx, proxyGroupID)
}

// ErrNoProxies is returned by acquires when the group has no proxies.
var ErrNoProxies = errors.New("no proxies available")

// ErrGroupNotFound is returned when an operation names a group the manager
// does not know.
var ErrGroupNotFound = errors.New("proxy group not found")

// ErrGroupInUse is returned by DeleteGroup when a running task leases from the
// group. Suspend or kill the task first.
var ErrGroupInUse = errors.New("proxy group in use by a running task")

// ErrTaskOrphaned is returned when a deletion's policy decides Fail, so the
// deleter can kill or quarantine the named task.
var ErrTaskOrphaned = errors.New("task orphaned by proxy deletion")
