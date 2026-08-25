package tasks

import (
	"context"
	"fmt"
	"time"
)

// GlobalGroup is the task group tasks land in when created without one. It
// exists implicitly — it needs no stored record, resolves to no resource group
// of any kind until one is saved for it, and cannot be deleted.
const GlobalGroup = "global"

// A Group is a durable named collection of tasks. ResourceGroups names, per
// resource kind, the group its members lease from; a kind absent from the map,
// or mapped to "", means members lease none of that kind. A member task's own
// assignment for a kind overrides it. This package owns no resource pool and
// does not validate the names — an unknown one surfaces as an error when a
// member first tries to lease.
type Group struct {
	ID             string            `json:"id"`
	ResourceGroups map[string]string `json:"resourceGroups,omitempty"`
	CreatedAt      time.Time         `json:"createdAt"`
	UpdatedAt      time.Time         `json:"updatedAt"`
}

// createConfig collects the optional placement of a new task.
type createConfig struct {
	groupID     string
	assignments map[string]Assignment
}

// assign records an option's placement for one kind, merging it into whatever
// an earlier option set for that kind.
func (c *createConfig) assign(kind string, apply func(*Assignment)) {
	if c.assignments == nil {
		c.assignments = make(map[string]Assignment)
	}
	a := c.assignments[kind]
	apply(&a)
	c.assignments[kind] = a
}

// A CreateOption places a new task in a task group, assigns the resource group
// of one kind it leases from, or pins it to one resource within that group.
type CreateOption func(*createConfig)

// InGroup places the task in the named task group instead of the global one.
func InGroup(groupID string) CreateOption {
	return func(c *createConfig) { c.groupID = groupID }
}

// WithResourceGroup assigns the task its own group of the named resource kind
// — "proxy", "account", or whatever a consumer calls its manager — overriding
// its task group's assignment for that kind.
func WithResourceGroup(kind, groupID string) CreateOption {
	return func(c *createConfig) {
		c.assign(kind, func(a *Assignment) { a.GroupID = &groupID })
	}
}

// WithPin pins the task to one resource of the kind, which must belong to the
// group the task resolves to for that kind. A pinned task never rotates: every
// lease it takes of that kind is that resource. This package owns no resource
// pool and cannot check the pin, so a resource that does not exist, or is in
// another group, surfaces as an error at the task's first lease.
func WithPin(kind, resourceID string) CreateOption {
	return func(c *createConfig) {
		c.assign(kind, func(a *Assignment) { a.ResourceID = &resourceID })
	}
}

// Without makes the task lease no resource of the kind even if its task group
// assigns one, clearing any pin along with it.
func Without(kind string) CreateOption {
	return func(c *createConfig) {
		none := ""
		c.assign(kind, func(a *Assignment) { a.GroupID, a.ResourceID = &none, &none })
	}
}

// A ReleaseFunc frees external state bound to a task when it is deleted.
// A leasing manager's durable locks are handled by WithResource; this is the
// general hook, for whatever else a task owns that no resource kind describes.
//
// It runs while the service holds its task registry lock, so it must not call
// back into the Service — the lock is not reentrant and the call would hang
// forever — nor block on anything that might.
type ReleaseFunc func(ctx context.Context, taskID string) error

// A StaleLockFunc releases the durable lock a task's new placement no longer
// fits — a lock naming another resource, or a group the locked one is not in.
// Nothing else will ever drop it: a lease must not, since that would let a
// routine acquire quietly undo a deliberate binding. The strings it receives
// are the resolved placement for its kind, "" for none.
//
// Wire each manager's stale-lock release through WithResource; proxies and
// accounts each ship an adapter that fits. Like ReleaseFunc it runs while the
// service holds its task registry lock, so it must not call back into the
// Service.
type StaleLockFunc func(ctx context.Context, taskID, groupID, resourceID string) error

// An Assignment is a task's stored placement for one resource kind: the group
// it leases from and the resource it is pinned to within that group. A nil
// GroupID inherits the task group's assignment for the kind; an empty one
// leases none. A nil or empty ResourceID leaves the task unpinned, rotating
// its group.
type Assignment struct {
	GroupID    *string `json:"groupId"`
	ResourceID *string `json:"resourceId"`
}

// A ServiceOption configures a Service at construction.
type ServiceOption func(*service)

// WithTaskReleaser runs release before every task deletion, single or
// cascaded; a release error aborts that deletion. It runs alongside every
// registered resource's unlock, not instead of it.
func WithTaskReleaser(release ReleaseFunc) ServiceOption {
	return func(s *service) { s.release = release }
}

// WithResource registers one resource kind's leasing manager, which is the
// whole of that wiring: unlock frees a deleted task's durable lock, and stale
// drops a repointed one's when its new placement no longer fits. Both come off
// the manager —
//
//	tasks.WithResource("proxy", manager.Unlock, proxies.StaleLockReleaser(manager))
//
// Register every kind whose locks outlive the process. A kind left unregistered
// still places tasks, but nothing ever frees its locks: deleting a task strands
// the resource it held, and repointing one leaves it leasing what it was moved
// off, since a binding outranks the group. Either func may be nil to wire only
// the half a manager needs.
//
// It panics on a kind registered twice, which could only unlock it twice.
func WithResource(kind string, unlock ReleaseFunc, stale StaleLockFunc) ServiceOption {
	return func(s *service) {
		for _, r := range s.resources {
			if r.kind == kind {
				panic(fmt.Sprintf("tasks: resource kind %q registered twice", kind))
			}
		}
		s.resources = append(s.resources, resource{kind: kind, unlock: unlock, stale: stale})
	}
}

// a resource is one registered kind's lock surface, kept in registration order
// so a failure names its kinds the same way every run.
type resource struct {
	kind   string
	unlock ReleaseFunc
	stale  StaleLockFunc
}

// A Guard adapts a Service to the usage policy a leasing manager consults
// before deleting a resource or a group, scoped to one resource kind. It is the
// whole of that wiring:
//
//	proxies.WithUsagePolicy(tasks.NewGuard(&svc, "proxy"))
//
// Every question a manager asks is either about its own kind or about a task,
// and the kind is fixed when the guard is built — so there is nothing left for
// a consumer to decide, and nothing to get wrong per manager.
//
// It holds a *Service rather than a Service because of the wiring order a
// consumer cannot avoid: the manager is built first, the service that answers
// its questions second. The guard reads through the pointer at call time, by
// which point the service exists.
type Guard struct {
	svc  *Service
	kind string
}

// NewGuard returns the usage guard a manager of kind consults, answering from
// svc. It panics if svc is nil: there is no later moment at which a nil pointer
// becomes answerable.
func NewGuard(svc *Service, kind string) Guard {
	if svc == nil {
		panic("tasks: NewGuard needs a non-nil *Service to read at call time")
	}
	return Guard{svc: svc, kind: kind}
}

// resolve returns the service, panicking if it was never assigned. Answering
// "nothing is running" instead would let a deletion tear a pool out from under
// a live run, so a wiring mistake fails loudly rather than degrading into a
// silent wrong answer.
func (g Guard) resolve() Service {
	svc := *g.svc
	if svc == nil {
		panic(fmt.Sprintf("tasks: the %s usage guard was consulted before its Service was assigned", g.kind))
	}
	return svc
}

// RunningTasks reports the tasks actively running against groupID under the
// guard's kind.
func (g Guard) RunningTasks(ctx context.Context, groupID string) ([]string, error) {
	return g.resolve().RunningTasks(ctx, g.kind, groupID)
}

// TaskIsRunning reports whether the named task is actively running. The
// question is kind-agnostic, so the guard passes it straight through.
func (g Guard) TaskIsRunning(ctx context.Context, taskID string) (bool, error) {
	return g.resolve().TaskIsRunning(ctx, taskID)
}

// PinnedTasks reports the tasks pinned to resourceID under the guard's kind.
func (g Guard) PinnedTasks(ctx context.Context, resourceID string) ([]string, error) {
	return g.resolve().PinnedTasks(ctx, g.kind, resourceID)
}
