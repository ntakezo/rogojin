package cards

import (
	"context"

	"github.com/ntakezo/rogojin/leasing"
)

// A Manager allocates cards to tasks. See leasing.Manager.
type Manager = leasing.Manager[Card, *Card]

// A Lease is a live hold on one card. Release it exactly once when done.
type Lease = leasing.Lease[Card, *Card]

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent.
func NewManager(ctx context.Context, repo Repository) (*Manager, error) {
	return leasing.NewManager[Card](ctx, repo)
}
