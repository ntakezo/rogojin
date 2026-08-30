package leasing

import (
	"errors"
	"sync"
)

// RoundRobin selects candidates in turn, spreading load evenly by advancing a
// cursor on every selection. It reads nothing off the records, so it needs no
// constraint on R. It is safe for concurrent use.
type RoundRobin[R any] struct {
	mu   sync.Mutex
	next int
}

// NewRoundRobin returns a RoundRobin with its cursor at the first candidate.
func NewRoundRobin[R any]() *RoundRobin[R] {
	return &RoundRobin[R]{}
}

// Select returns the candidate at the cursor and advances it.
func (r *RoundRobin[R]) Select(candidates []R) (R, error) {
	if len(candidates) == 0 {
		return zero[R](), errors.New("no candidates")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := candidates[r.next%len(candidates)]
	r.next++
	return p, nil
}
