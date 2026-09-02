package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/payments"
)

// Payments is the payments.Repository: one row per payment, its
// checkout-defined fields in a JSON text column, stored in the clear — see
// the package doc and the sqlite counterpart for the vault caveat.
type Payments struct {
	db *sql.DB
}

// NewPayments builds the payments store on db, bringing its tables up to the
// current schema.
func NewPayments(db *DB) (payments.Repository, error) {
	if err := migrate(db.db, "payments", paymentMigrations); err != nil {
		return nil, err
	}
	return &Payments{db: db.db}, nil
}

// paymentMigrations is the ordered schema history of the payments store —
// a fresh baseline, since this adapter was born on the current contract.
var paymentMigrations = []migration{
	{
		Name: "create payments table",
		SQL: `CREATE TABLE payments (
			id          TEXT PRIMARY KEY,
			group_id    TEXT NOT NULL DEFAULT 'global',
			owner_id    TEXT NOT NULL DEFAULT '',
			max_holders BIGINT NOT NULL DEFAULT 0,
			version     BIGINT NOT NULL DEFAULT 0,
			fields      TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
	{Name: "create payment_groups table", SQL: groupsSchema("payment_groups")},
	{Name: "create payment_holds table", SQL: holdsSchema("payment_holds")},
	{Name: "create payment_counters table", SQL: countersSchema("payment_counters")},
}

// paymentTables wires the shared leasing mechanics to this store's tables.
var paymentTables = leaseTables{noun: "payment", records: "payments", holds: "payment_holds", counters: "payment_counters"}

// List returns every stored payment in stable id order, so the manager's pool
// order is deterministic.
func (s *Payments) List(ctx context.Context) ([]payments.Payment, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, group_id, owner_id, max_holders, version, fields, created_at, updated_at
		 FROM payments ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list payments: %w", err)
	}
	defer rows.Close()

	listed := make([]payments.Payment, 0)
	for rows.Next() {
		var c payments.Payment
		var fields, created, updated string
		if err := rows.Scan(&c.ID, &c.GroupID, &c.OwnerID, &c.MaxHolders, &c.Version, &fields, &created, &updated); err != nil {
			return nil, fmt.Errorf("list payments: %w", err)
		}
		c.Fields = parseFields(fields)
		if c.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list payments: %w", err)
		}
		if c.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list payments: %w", err)
		}
		listed = append(listed, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list payments: %w", err)
	}
	return listed, nil
}

// Save writes the payment's record conditionally on its Version — see
// leasing.Repository. created_at is written on insert and never overwritten.
func (s *Payments) Save(ctx context.Context, c payments.Payment) (int64, error) {
	fields, err := formatFields(c.Fields)
	if err != nil {
		return 0, fmt.Errorf("save payment %s: %w", c.ID, err)
	}
	return saveVersioned(paymentTables, c.ID, c.Version,
		func(next int64) (sql.Result, error) {
			return s.db.ExecContext(ctx,
				`INSERT INTO payments (id, group_id, owner_id, max_holders, version, fields, created_at, updated_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING`,
				c.ID, c.GroupID, c.OwnerID, c.MaxHolders, next, fields,
				formatTime(c.CreatedAt), formatTime(c.UpdatedAt))
		},
		func(next int64) (sql.Result, error) {
			return s.db.ExecContext(ctx,
				`UPDATE payments SET group_id = $1, owner_id = $2, max_holders = $3, version = $4,
				 fields = $5, updated_at = $6 WHERE id = $7 AND version = $8`,
				c.GroupID, c.OwnerID, c.MaxHolders, next, fields, formatTime(c.UpdatedAt), c.ID, c.Version)
		})
}

// Delete removes the payment's record and its holds; absent rows are a no-op.
func (s *Payments) Delete(ctx context.Context, id string) error {
	return deleteWithHolds(ctx, s.db, paymentTables, id)
}

// Acquire takes or re-enters a hold on the payment under cap — see
// leasing.Repository.
func (s *Payments) Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	return acquireHold(ctx, s.db, paymentTables, resourceID, taskID, cap, ttl)
}

// ReleaseHold decrements the task's hold, removing it at zero.
func (s *Payments) ReleaseHold(ctx context.Context, resourceID, taskID string) error {
	return releaseHold(ctx, s.db, paymentTables, resourceID, taskID)
}

// RenewHolds extends every unexpired hold the task has.
func (s *Payments) RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	return renewHolds(ctx, s.db, paymentTables, taskID, ttl)
}

// ListHolds returns every hold row, expired ones included.
func (s *Payments) ListHolds(ctx context.Context) ([]leasing.Hold, error) {
	return listHolds(ctx, s.db, paymentTables)
}

// ClaimLock binds the payment to the task iff unlocked or already its own.
func (s *Payments) ClaimLock(ctx context.Context, resourceID, taskID string) error {
	return claimLock(ctx, s.db, paymentTables, resourceID, taskID)
}

// ReleaseLock clears the lock iff the task owns it.
func (s *Payments) ReleaseLock(ctx context.Context, resourceID, taskID string) error {
	return releaseLock(ctx, s.db, paymentTables, resourceID, taskID)
}

// Increment atomically adds delta to the counter under (scope, name).
func (s *Payments) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return incrementCounter(ctx, s.db, paymentTables, scope, name, delta)
}

// ListGroups returns every stored payment group in stable id order.
func (s *Payments) ListGroups(ctx context.Context) ([]payments.Group, error) {
	return listGroups(ctx, s.db, paymentTables, "payment_groups")
}

// SaveGroup upserts the group's record, preserving created_at.
func (s *Payments) SaveGroup(ctx context.Context, g payments.Group) error {
	return saveGroup(ctx, s.db, paymentTables, "payment_groups", g)
}

// DeleteGroup removes the group's record; absent rows are a no-op.
func (s *Payments) DeleteGroup(ctx context.Context, id string) error {
	return dropGroup(ctx, s.db, paymentTables, "payment_groups", id)
}
