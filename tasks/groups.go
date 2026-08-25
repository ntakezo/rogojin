package tasks

import (
	"context"
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

// A ReleaseFunc frees external resources bound to a task when it is deleted —
// durable leasing locks above all, since they outlive the program. Wire every
// manager's Unlock here so deleting a task or cascading a group delete never
// strands a locked resource.
//
// It runs while the service holds its task registry lock, so it must not call
// back into the Service — the lock is not reentrant and the call would hang
// forever — nor block on anything that might.
type ReleaseFunc func(ctx context.Context, taskID string) error

// An Assignment is a task's stored placement for one resource kind: the group
// it leases from and the resource it is pinned to within that group. A nil
// GroupID inherits the task group's assignment for the kind; an empty one
// leases none. A nil or empty ResourceID leaves the task unpinned, rotating
// its group.
type Assignment struct {
	GroupID    *string `json:"groupId"`
	ResourceID *string `json:"resourceId"`
}

// A ReassignFunc releases external state a task's placement change invalidates
// — a durable lock the new placement no longer fits above all, since nothing
// else will ever drop it. Wire each manager's ReleaseStaleLock here, routed on
// kind. The strings it receives are the resolved placement, "" for none.
//
// Like ReleaseFunc it runs while the service holds its task registry lock, so
// it must not call back into the Service.
type ReassignFunc func(ctx context.Context, taskID, kind, groupID, resourceID string) error

// A ServiceOption configures a Service at construction.
type ServiceOption func(*service)

// WithTaskReleaser runs release before every task deletion, single or
// cascaded; a release error aborts that deletion.
func WithTaskReleaser(release ReleaseFunc) ServiceOption {
	return func(s *service) { s.release = release }
}

// WithTaskReassigner runs reassign after every AssignResource, so a durable
// lock the new placement no longer fits is released. Without one,
// AssignResource still repoints the record, but a locked task keeps leasing
// its old resource — a binding outranks the group, and nothing would ever
// clear it.
func WithTaskReassigner(reassign ReassignFunc) ServiceOption {
	return func(s *service) { s.reassign = reassign }
}
