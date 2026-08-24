package tasks

import (
	"context"
	"time"
)

// GlobalGroup is the task group tasks land in when created without one. It
// exists implicitly — it needs no stored record, resolves to no proxy group
// until one is saved for it, and cannot be deleted.
const GlobalGroup = "global"

// A Group is a durable named collection of tasks. ProxyGroupID names the
// proxy group its members lease from; "" means members run without proxies. A
// member task's own assignment overrides it. This package owns no proxy pool
// and does not validate the name — an unknown one surfaces as an error when a
// member first tries to lease.
type Group struct {
	ID           string    `json:"id"`
	ProxyGroupID string    `json:"proxyGroupId"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

// createConfig collects the optional placement of a new task.
type createConfig struct {
	groupID      string
	proxyGroupID *string // nil inherits the task group's assignment
}

// A CreateOption places a new task in a group or assigns its proxy group.
type CreateOption func(*createConfig)

// InGroup places the task in the named task group instead of the global one.
func InGroup(groupID string) CreateOption {
	return func(c *createConfig) { c.groupID = groupID }
}

// WithProxyGroup assigns the task its own proxy group, overriding its task
// group's assignment.
func WithProxyGroup(proxyGroupID string) CreateOption {
	return func(c *createConfig) { c.proxyGroupID = &proxyGroupID }
}

// WithoutProxies makes the task run proxyless even if its task group has a
// proxy group assigned.
func WithoutProxies() CreateOption {
	return func(c *createConfig) {
		none := ""
		c.proxyGroupID = &none
	}
}

// A ReleaseFunc frees external resources bound to a task when it is deleted —
// durable proxy locks above all, since they outlive the program. Wire
// proxies.Manager.Unlock here so deleting a task or cascading a group delete
// never strands a locked proxy.
//
// It runs while the service holds its task registry lock, so it must not call
// back into the Service — the lock is not reentrant and the call would hang
// forever — nor block on anything that might.
type ReleaseFunc func(ctx context.Context, taskID string) error

// A ServiceOption configures a Service at construction.
type ServiceOption func(*service)

// WithTaskReleaser runs release before every task deletion, single or
// cascaded; a release error aborts that deletion.
func WithTaskReleaser(release ReleaseFunc) ServiceOption {
	return func(s *service) { s.release = release }
}
