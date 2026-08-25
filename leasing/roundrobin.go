package leasing

import (
	"errors"
	"sync"
)

// RoundRobin selects candidates in turn, spreading load evenly by advancing a
// cursor on every selection. It is safe for concurrent use.
type RoundRobin[T any] struct {
	mu   sync.Mutex
	next int
}

// NewRoundRobin returns a RoundRobin with its cursor at the first candidate.
func NewRoundRobin[T any]() *RoundRobin[T] {
	return &RoundRobin[T]{}
}

// Select returns the candidate at the cursor and advances it.
func (r *RoundRobin[T]) Select(candidates []Resource[T]) (Resource[T], error) {
	if len(candidates) == 0 {
		return zero[T](), errors.New("no candidates")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	p := candidates[r.next%len(candidates)]
	r.next++
	return p, nil
}
