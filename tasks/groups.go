package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/leasing"
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

// ResourceManager is the lock surface this service drives per registered kind:
// Unlock frees a deleted task's durable lock, and ReleaseStaleLock drops a
// repointed task's when its new placement no longer fits. Every
// *leasing.Manager[T] satisfies it.
//
// Both calls run while the service holds its task registry lock. That is safe
// because a leasing manager never calls back into this package — it decides
// everything from the leases and locks it owns.
type ResourceManager interface {
	Unlock(ctx context.Context, taskID string) error
	ReleaseStaleLock(ctx context.Context, a leasing.Assignment) error
}

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

// WithResource registers one resource kind's leasing manager, which is the
// whole of that wiring:
//
//	tasks.WithResource("proxy", manager)
//
// Deleting a task then unlocks the kind, and repointing one drops only the
// lock its new placement no longer fits. Register every kind whose locks
// outlive the process. A kind left unregistered still places tasks, but
// nothing ever frees its locks: deleting a task strands the resource it held,
// and repointing one leaves it leasing what it was moved off, since a binding
// outranks the group.
//
// It panics on a nil manager and on a kind registered twice, which could only
// unlock it twice.
func WithResource(kind string, manager ResourceManager) ServiceOption {
	return func(s *service) {
		if manager == nil {
			panic(fmt.Sprintf("tasks: resource kind %q registered with a nil manager", kind))
		}
		for _, r := range s.resources {
			if r.kind == kind {
				panic(fmt.Sprintf("tasks: resource kind %q registered twice", kind))
			}
		}
		s.resources = append(s.resources, resource{kind: kind, manager: manager})
	}
}

// a resource is one registered kind's manager, kept in registration order so a
// failure names its kinds the same way every run.
type resource struct {
	kind    string
	manager ResourceManager
}
