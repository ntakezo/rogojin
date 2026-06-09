package proxies

import (
	"errors"
	"sync"
)

// RoundRobin selects candidates in turn, spreading load evenly by advancing a
// cursor on every selection. It is safe for concurrent use.
type RoundRobin struct {
	mu   sync.Mutex
	next int
}

// NewRoundRobin returns a RoundRobin with its cursor at the first candidate.
func NewRoundRobin() *RoundRobin {
	return &RoundRobin{}
}

// Select returns the candidate at the cursor and advances it.
func (r *RoundRobin) Select(candidates []Proxy) (Proxy, error) {
	if len(candidates) == 0 {
		return Proxy{}, errors.New("no candidates")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := candidates[r.next%len(candidates)]
	r.next++
	return p, nil
}
