// Package proxies allocates proxies to tasks. It is the leasing package
// specialized to one model: a Proxy embeds leasing.Resource — the pool,
// group, and lock fields every leasable kind shares — and adds its
// URL and the success/failure counts it alone keeps. A Thompson-sampling
// selection strategy is installed alongside the default round robin,
// learning from those counts — which is why a proxy lease, unlike any
// other, is released with an outcome.
//
// Every leasing behavior — groups, holder caps, durable locks, pins,
// lease-guarded deletes — is documented in the leasing package; the Manager
// here is that manager over this model, plus the outcome-taking Lease.
package proxies

import (
	"github.com/ntakezo/rogojin/leasing"
)

// Kind is the resource kind proxies register with the task manager under —
// the key a task's proxy placement is filed on. It is the one name the
// manager, the task service, and a workflow reading its Deps must agree on,
// so it lives here and nowhere else.
const Kind leasing.Kind = "proxy"

// GlobalGroup is the namespace proxies land in when added without a group.
const GlobalGroup = leasing.GlobalGroup

// UnlimitedHolders lifts a proxy's holder cap: any number of concurrent
// leases is tolerated.
const UnlimitedHolders = leasing.UnlimitedHolders

// A Proxy is the durable record of one proxy: the leasing core, its URL, and
// the lease outcomes it has seen. Successes and Failures are what the
// bayesian strategy ranks by; a Lease's Release tallies them, so every
// release is a data point and the counts survive restarts with the row.
type Proxy struct {
	leasing.Resource
	URL       string `json:"url"`
	Successes uint64 `json:"successes"`
	Failures  uint64 `json:"failures"`
}

// A Group is a durable named subset of the pool that leases rotate within.
type Group = leasing.Group

// Repository is the persistence port: a dumb durable store of proxies and
// their groups.
type Repository = leasing.Repository[Proxy]

// An Assignment is the placement a task leases under; the pin names a proxy.
type Assignment = leasing.Assignment
