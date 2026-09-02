package memory

import (
	"context"
	"sync"

	"github.com/ntakezo/rogojin/proxies"
)

// Proxies is the proxies.Repository: one record per proxy, with the URL and
// the success and failure counts the bayesian strategy learns from.
type Proxies struct {
	mu      sync.Mutex
	records map[string]proxies.Proxy
	groups  groupStore
}

var _ proxies.Repository = (*Proxies)(nil)

// NewProxies builds an empty in-memory proxies store.
func NewProxies() proxies.Repository {
	return &Proxies{records: make(map[string]proxies.Proxy), groups: newGroupStore()}
}

// List returns every stored proxy in stable id order, so the manager's pool
// order is deterministic.
func (s *Proxies) List(ctx context.Context) ([]proxies.Proxy, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	listed := make([]proxies.Proxy, 0, len(s.records))
	for _, id := range sortedIDs(s.records) {
		listed = append(listed, s.records[id])
	}
	return listed, nil
}

// Save upserts the proxy's record: URL, group, holder policy, lock owner,
// stats, and UpdatedAt. CreatedAt is written on insert and never
// overwritten, so a lock or a stat update cannot revise it.
func (s *Proxies) Save(ctx context.Context, p proxies.Proxy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p.CreatedAt, p.UpdatedAt = storeTime(p.CreatedAt), storeTime(p.UpdatedAt)
	if kept, ok := s.records[p.ID]; ok {
		p.CreatedAt = kept.CreatedAt
	}
	s.records[p.ID] = p
	return nil
}

// Delete removes the proxy's record; absent ids are a no-op.
func (s *Proxies) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	return nil
}

// ListGroups returns every stored proxy group in stable id order.
func (s *Proxies) ListGroups(ctx context.Context) ([]proxies.Group, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.groups.list(), nil
}

// SaveGroup upserts the group's record; CreatedAt is written on insert and
// never overwritten.
func (s *Proxies) SaveGroup(ctx context.Context, g proxies.Group) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups.save(g)
	return nil
}

// DeleteGroup removes the group's record; absent ids are a no-op. Member
// proxies are the manager's to delete — the store cascades nothing.
func (s *Proxies) DeleteGroup(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groups.delete(id)
	return nil
}
