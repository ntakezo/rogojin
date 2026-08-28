// Package proxies allocates proxies to tasks. It is the leasing package
// specialized to one resource kind: the payload is a proxy's URL, and a
// Thompson-sampling selection strategy is installed alongside the default
// round robin, learning from the success/failure outcomes leases record.
//
// The types here are aliases, not wrappers: a Proxy is a leasing.Resource, the
// Manager is a leasing.Manager, and every behavior — groups, holder caps,
// durable locks, pins, lease-guarded deletes — is documented there.
package proxies

import (
	"context"

	"github.com/ntakezo/rogojin/leasing"
)

// GlobalGroup is the namespace proxies land in when added without a group.
const GlobalGroup = leasing.GlobalGroup

// UnlimitedHolders lifts a proxy's holder cap: any number of concurrent
// leases is tolerated.
const UnlimitedHolders = leasing.UnlimitedHolders

// Built-in selection strategy names a Group may reference.
const (
	StrategyRoundRobin = leasing.StrategyRoundRobin
	StrategyBayesian   = "bayesian"
)

// Attrs is the proxy payload the leasing core carries opaquely.
type Attrs struct {
	URL string `json:"url"`
}

// A Proxy is the durable record of one proxy; its URL lives in Attrs.
type Proxy = leasing.Resource[Attrs]

// A Group is a durable named subset of the pool that leases rotate within.
type Group = leasing.Group

// Repository is the persistence port: a dumb durable store of proxies and
// their groups.
type Repository = leasing.Repository[Attrs]

// An Assignment is the placement a task leases under; the pin names a proxy.
type Assignment = leasing.Assignment

// A Manager allocates proxies to tasks. See leasing.Manager.
type Manager = leasing.Manager[Attrs]

// A Lease is a live hold on one proxy. Release it exactly once when done.
type Lease = leasing.Lease[Attrs]

// Selection is the strategy port: pick one proxy from the currently-acquirable
// candidates.
type Selection = leasing.Selection[Attrs]

// A StrategyFactory builds one Selection instance per group.
type StrategyFactory = leasing.StrategyFactory[Attrs]

// An Option configures the Manager at construction; see leasing.WithStrategy.
type Option = leasing.Option[Attrs]

// WithStrategy registers a custom selection strategy under name.
func WithStrategy(name string, factory StrategyFactory) Option {
	return leasing.WithStrategy(name, factory)
}

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent. The bayesian strategy is installed alongside the
// built-in round robin before opts apply, so a WithStrategy under its name
// overrides it.
func NewManager(ctx context.Context, repo Repository, opts ...Option) (*Manager, error) {
	installed := append([]Option{
		WithStrategy(StrategyBayesian, func() Selection { return NewBayesian() }),
	}, opts...)
	return leasing.NewManager(ctx, repo, installed...)
}
