// Package proxies allocates proxies to tasks. A consumer-provided Repository
// stores the pool durably while the Manager owns all live acquisition state,
// rotating unlocked proxies through a pluggable selection strategy and honoring
// durable task-to-proxy locks.
package proxies

import (
	"context"
	"errors"
)

// A Proxy is the durable record of one proxy. OwnerID is the durable lock: the
// task this proxy is bound to, or "" while it rotates in the pool. Successes
// and Failures are the lease outcomes selection strategies learn from.
type Proxy struct {
	ID        string
	URL       string
	OwnerID   string
	Successes uint64
	Failures  uint64
}

// Repository is the persistence port: a dumb durable store of proxies. It
// tracks no leases — the Manager owns live state.
type Repository interface {
	List(ctx context.Context) ([]Proxy, error)
	Save(ctx context.Context, proxy Proxy) error
	Delete(ctx context.Context, id string) error
}

// Selection is the strategy port: pick one proxy from the currently-acquirable
// candidates. Stateful strategies guard their own state.
type Selection interface {
	Select(candidates []Proxy) (Proxy, error)
}

// Exclusivity is the maximum number of concurrent leases per rotating proxy.
type Exclusivity struct {
	maxHolders int
}

// Exclusive allows one lease per proxy at a time.
func Exclusive() Exclusivity {
	return Exclusivity{maxHolders: 1}
}

// Capped allows up to n concurrent leases per proxy.
func Capped(n int) Exclusivity {
	return Exclusivity{maxHolders: n}
}

// A Decision is what a DeletionPolicy tells the Manager to do with the task a
// deleted proxy was locked to.
type Decision int

const (
	// Reassign locks the task to a freshly selected proxy.
	Reassign Decision = iota
	// Unbind leaves the task lockless; it rotates the pool on its next acquire.
	Unbind
	// Fail unbinds the task and surfaces ErrTaskOrphaned to the deleter.
	Fail
)

// DeletionPolicy is the port a module implements to decide the fate of a task
// whose locked proxy is deleted. It must not call back into the Manager.
type DeletionPolicy interface {
	OnProxyDeleted(ctx context.Context, taskID string, deleted Proxy) Decision
}

// ErrNoProxies is returned by acquires when the pool is empty.
var ErrNoProxies = errors.New("no proxies available")

// ErrTaskOrphaned is returned by DeleteProxy when the policy decides Fail, so
// the deleter can kill or quarantine the named task.
var ErrTaskOrphaned = errors.New("task orphaned by proxy deletion")
