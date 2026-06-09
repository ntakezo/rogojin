package proxies

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// A Manager allocates proxies to tasks: locked proxies go only to their owner,
// unlocked ones rotate through the selection strategy under the exclusivity
// cap. It owns all live lease state; the Repository only stores bytes.
// A Manager is safe for concurrent use.
type Manager struct {
	repo   Repository
	sel    Selection
	excl   Exclusivity
	policy DeletionPolicy

	mu       sync.Mutex
	cond     *sync.Cond
	pool     map[string]Proxy
	order    []string          // stable candidate order for selection
	holders  map[string]int    // live lease count per proxy
	bindings map[string]string // taskID -> locked proxy ID
}

// NewManager loads the pool from the repository; the pool is fixed for the
// manager's lifetime apart from DeleteProxy.
func NewManager(ctx context.Context, repo Repository, sel Selection, excl Exclusivity, policy DeletionPolicy) (*Manager, error) {
	if repo == nil || sel == nil {
		return nil, errors.New("repository and selection are required")
	}
	if excl.maxHolders < 1 {
		return nil, errors.New("exclusivity capacity must be at least 1")
	}

	listed, err := repo.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("load proxy pool: %w", err)
	}

	m := &Manager{
		repo:     repo,
		sel:      sel,
		excl:     excl,
		policy:   policy,
		pool:     make(map[string]Proxy, len(listed)),
		holders:  make(map[string]int),
		bindings: make(map[string]string),
	}
	m.cond = sync.NewCond(&m.mu)

	for _, p := range listed {
		if _, dup := m.pool[p.ID]; dup {
			return nil, fmt.Errorf("duplicate proxy id %s", p.ID)
		}
		m.pool[p.ID] = p
		m.order = append(m.order, p.ID)
		if p.OwnerID != "" {
			if prior, bound := m.bindings[p.OwnerID]; bound {
				return nil, fmt.Errorf("task %s locked to multiple proxies (%s, %s)", p.OwnerID, prior, p.ID)
			}
			m.bindings[p.OwnerID] = p.ID
		}
	}
	return m, nil
}

// Acquire leases a proxy for taskID: its locked proxy if it has one, otherwise
// one rotated from the unlocked pool. It blocks until a proxy frees or ctx is
// done; an empty pool fails immediately with ErrNoProxies.
func (m *Manager) Acquire(ctx context.Context, taskID string) (*Lease, error) {
	return m.acquire(ctx, taskID, false)
}

// Lock durably binds taskID to a proxy (selecting one if unbound, idempotent)
// and leases it. The binding outlives the lease and the manager until Unlock
// or DeleteProxy; no other task can ever acquire the proxy.
func (m *Manager) Lock(ctx context.Context, taskID string) (*Lease, error) {
	return m.acquire(ctx, taskID, true)
}

// acquire is the shared blocking loop behind Acquire and Lock. A bound task
// only ever leases its own proxy, one lease at a time; an unbound task rotates
// the unlocked pool, durably binding the pick first when lock is set.
func (m *Manager) acquire(ctx context.Context, taskID string, lock bool) (*Lease, error) {
	// cond.Wait cannot watch ctx, so a watcher wakes the loop on cancellation.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			m.mu.Lock()
			m.cond.Broadcast()
			m.mu.Unlock()
		case <-stop:
		}
	}()

	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if len(m.pool) == 0 {
			return nil, ErrNoProxies
		}

		if id, bound := m.bindings[taskID]; bound {
			// a locked proxy is exclusive to its owner: one lease at a time.
			if m.holders[id] == 0 {
				m.holders[id]++
				return &Lease{manager: m, proxy: m.pool[id]}, nil
			}
		} else if p, found, err := m.selectUnlocked(); err != nil {
			return nil, err
		} else if found {
			if lock {
				p.OwnerID = taskID
				if err := m.repo.Save(ctx, p); err != nil {
					return nil, fmt.Errorf("persist lock: %w", err)
				}
				m.pool[p.ID] = p
				m.bindings[taskID] = p.ID
			}
			m.holders[p.ID]++
			return &Lease{manager: m, proxy: p}, nil
		}

		m.cond.Wait()
	}
}

// selectUnlocked picks an unlocked, under-capacity proxy via the selection
// strategy; found is false when there are no candidates. Callers hold m.mu.
func (m *Manager) selectUnlocked() (Proxy, bool, error) {
	candidates := make([]Proxy, 0, len(m.order))
	for _, id := range m.order {
		p := m.pool[id]
		if p.OwnerID == "" && m.holders[id] < m.excl.maxHolders {
			candidates = append(candidates, p)
		}
	}
	if len(candidates) == 0 {
		return Proxy{}, false, nil
	}

	p, err := m.sel.Select(candidates)
	if err != nil {
		return Proxy{}, false, fmt.Errorf("selection: %w", err)
	}
	live, ok := m.pool[p.ID]
	if !ok {
		return Proxy{}, false, fmt.Errorf("selection returned unknown proxy %s", p.ID)
	}
	return live, true, nil
}

// Unlock removes taskID's durable lock, returning its proxy to the rotating
// pool. It is a no-op if taskID has no locked proxy.
func (m *Manager) Unlock(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	id, bound := m.bindings[taskID]
	if !bound {
		return nil
	}

	p := m.pool[id]
	p.OwnerID = ""
	if err := m.repo.Save(ctx, p); err != nil {
		return fmt.Errorf("persist unlock: %w", err)
	}
	m.pool[id] = p
	delete(m.bindings, taskID)
	m.cond.Broadcast()
	return nil
}

// DeleteProxy removes a proxy from the pool and the repository. Deleting a
// locked proxy runs the deletion policy and executes its decision; a Fail
// decision returns ErrTaskOrphaned naming the task so the deleter can act.
func (m *Manager) DeleteProxy(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p, ok := m.pool[id]
	if !ok {
		return m.repo.Delete(ctx, id)
	}
	if p.OwnerID == "" {
		return m.remove(ctx, p)
	}

	if m.policy == nil {
		return fmt.Errorf("proxy %s is locked to task %s and no deletion policy is set", id, p.OwnerID)
	}
	switch decision := m.policy.OnProxyDeleted(ctx, p.OwnerID, p); decision {
	case Reassign:
		next, found, err := m.selectUnlocked()
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("reassign task %s: %w", p.OwnerID, ErrNoProxies)
		}
		next.OwnerID = p.OwnerID
		if err := m.repo.Save(ctx, next); err != nil {
			return fmt.Errorf("persist reassign: %w", err)
		}
		m.pool[next.ID] = next
		m.bindings[p.OwnerID] = next.ID
		return m.remove(ctx, p)
	case Unbind:
		delete(m.bindings, p.OwnerID)
		return m.remove(ctx, p)
	case Fail:
		delete(m.bindings, p.OwnerID)
		if err := m.remove(ctx, p); err != nil {
			return err
		}
		return fmt.Errorf("%w: %s", ErrTaskOrphaned, p.OwnerID)
	default:
		return fmt.Errorf("unknown deletion decision %d", decision)
	}
}

// remove deletes p from the live pool and the repository, waking waiters.
// Callers hold m.mu.
func (m *Manager) remove(ctx context.Context, p Proxy) error {
	delete(m.pool, p.ID)
	delete(m.holders, p.ID)
	for i, id := range m.order {
		if id == p.ID {
			m.order = append(m.order[:i], m.order[i+1:]...)
			break
		}
	}
	m.cond.Broadcast()
	return m.repo.Delete(ctx, p.ID)
}

// Lease is a live hold on one proxy. Release it exactly once when done.
type Lease struct {
	manager *Manager
	proxy   Proxy
	once    sync.Once
}

// Proxy returns the leased proxy as of acquisition.
func (l *Lease) Proxy() Proxy {
	return l.proxy
}

// Release frees the proxy, records the outcome the bayesian strategy learns
// from, and persists it. Only the first call acts; later calls return nil.
func (l *Lease) Release(success bool) error {
	var err error
	l.once.Do(func() { err = l.manager.release(l.proxy.ID, success) })
	return err
}

// release updates stats, frees the holder slot, wakes waiters, then persists
// the stats with a background context so they land even when the caller's
// context is gone.
func (m *Manager) release(id string, success bool) error {
	m.mu.Lock()
	p, ok := m.pool[id]
	if ok {
		if success {
			p.Successes++
		} else {
			p.Failures++
		}
		m.pool[id] = p
	}
	if m.holders[id] > 0 {
		m.holders[id]--
	}
	m.cond.Broadcast()
	m.mu.Unlock()

	if !ok {
		return nil // proxy was deleted while leased; nothing to persist
	}
	return m.repo.Save(context.Background(), p)
}
