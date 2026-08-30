package payments

import (
	"context"

	"github.com/ntakezo/rogojin/leasing"
)

// A Manager allocates payments to tasks. See leasing.Manager.
type Manager = leasing.Manager[Payment, *Payment]

// A Lease is a live hold on one payment. Release it exactly once when done.
type Lease = leasing.Lease[Payment, *Payment]

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent.
func NewManager(ctx context.Context, repo Repository) (*Manager, error) {
	return leasing.NewManager[Payment](ctx, repo)
}
