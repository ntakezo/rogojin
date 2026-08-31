package proxies

import (
	"context"
	"sync"

	"github.com/ntakezo/rogojin/leasing"
)

// Built-in selection strategy names a Group may reference.
const (
	StrategyRoundRobin = leasing.StrategyRoundRobin
	StrategyBayesian   = "bayesian"
)

// A Manager allocates proxies to tasks. It is the leasing manager over Proxy
// with one addition: the leases it hands out can take an outcome on release,
// the data point the bayesian strategy learns from. Everything else — groups,
// holder caps, durable locks, pins, lease-guarded deletes — promotes from
// leasing.Manager and is documented there.
type Manager struct {
	*leasing.Manager[Proxy, *Proxy]
}

// Selection is the strategy port: pick one proxy from the currently-acquirable
// candidates.
type Selection = leasing.Selection[Proxy]

// A StrategyFactory builds one Selection instance per group.
type StrategyFactory = leasing.StrategyFactory[Proxy]

// An Option configures the Manager at construction; see leasing.WithStrategy.
type Option = leasing.Option[Proxy, *Proxy]

// WithStrategy registers a custom selection strategy under name.
func WithStrategy(name string, factory StrategyFactory) Option {
	return leasing.WithStrategy(name, factory)
}

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent. Seed groups before resources — see
// leasing.NewManager. The bayesian strategy is installed alongside the
// built-in round robin before opts apply, so a WithStrategy under its name
// overrides it.
func NewManager(ctx context.Context, repo Repository, opts ...Option) (*Manager, error) {
	installed := append([]Option{
		WithStrategy(StrategyBayesian, func() Selection { return NewBayesian() }),
	}, opts...)
	m, err := leasing.NewManager(ctx, repo, installed...)
	if err != nil {
		return nil, err
	}
	return &Manager{Manager: m}, nil
}

// Acquire leases a proxy under a. See leasing.Manager.Acquire.
func (m *Manager) Acquire(ctx context.Context, a Assignment) (*Lease, error) {
	l, err := m.Manager.Acquire(ctx, a)
	if err != nil {
		return nil, err
	}
	return &Lease{Lease: l, manager: m}, nil
}

// Lock durably binds a.TaskID to a proxy and leases it. See
// leasing.Manager.Lock.
func (m *Manager) Lock(ctx context.Context, a Assignment) (*Lease, error) {
	l, err := m.Manager.Lock(ctx, a)
	if err != nil {
		return nil, err
	}
	return &Lease{Lease: l, manager: m}, nil
}

// A Lease is a live hold on one proxy. Release it exactly once when done —
// through ReleaseOutcome where how it went is known, or the plain Release
// every lease kind shares.
type Lease struct {
	*leasing.Lease[Proxy, *Proxy]
	manager *Manager
	once    sync.Once
}

// ReleaseOutcome tallies the outcome on the proxy — persisted, so the
// bayesian strategy's history survives a restart — and then frees it. The
// tally goes first, while the lease still guards the proxy from deletion; the
// hold is freed even if the tally fails to persist, since a lease must never
// leak over a store error. Only the first of ReleaseOutcome and Release acts;
// later calls return nil.
func (l *Lease) ReleaseOutcome(ctx context.Context, success bool) error {
	var err error
	l.once.Do(func() {
		err = l.manager.Update(ctx, l.Resource().ID, func(p *Proxy) {
			if success {
				p.Successes++
			} else {
				p.Failures++
			}
		})
		l.Lease.Release()
	})
	return err
}

// Release frees the proxy without recording an outcome — the uniform release
// every lease kind shares, for teardown paths where how it went is unknown.
// Prefer ReleaseOutcome where there is an outcome: it is the data point the
// bayesian strategy learns from. Only the first of the two acts.
func (l *Lease) Release() {
	l.once.Do(func() { l.Lease.Release() })
}
