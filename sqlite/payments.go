package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/payments"
)

// Payments is the payments.Repository: one row per payment, its checkout-defined
// fields — number, expiry, CVV, billing address — in a JSON text column so
// the schema knows nothing about what any workflow's payments contain.
//
// They are stored in the clear. This is the plain-file default, not a vault:
// a database holding real payment data belongs behind a store that seals
// payments.Payment.Fields on the way down and opens them on the way up, which the
// Repository port is shaped to allow. Wrap this one, or write your own.
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

// paymentMigrations is the ordered schema history of the payments store. Append new
// steps to the end; never edit or reorder shipped ones: the ledger records
// which of them have already run on existing databases by position.
var paymentMigrations = []migration{
	{
		Name: "create payments table",
		SQL: `CREATE TABLE payments (
			id          TEXT PRIMARY KEY,
			group_id    TEXT NOT NULL DEFAULT 'global',
			owner_id    TEXT NOT NULL DEFAULT '',
			max_holders INTEGER NOT NULL DEFAULT 0,
			version     INTEGER NOT NULL DEFAULT 0,
			fields      TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "create payment_groups table",
		SQL: `CREATE TABLE payment_groups (
			id         TEXT PRIMARY KEY,
			strategy   TEXT NOT NULL DEFAULT '',
			refs       TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "create payment_holds table",
		SQL:  holdsSchema("payment_holds"),
	},
	{
		Name: "create payment_counters table",
		SQL:  countersSchema("payment_counters"),
	},
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
// leasing.Repository. created_at is written on insert and never overwritten,
// so a later lock cannot revise it.
func (s *Payments) Save(ctx context.Context, c payments.Payment) (int64, error) {
	fields, err := formatFields(c.Fields)
	if err != nil {
		return 0, fmt.Errorf("save payment %s: %w", c.ID, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("save payment %s: %w", c.ID, err)
	}
	defer tx.Rollback()

	insert, next, err := versionGate(ctx, tx, paymentTables, c.ID, c.Version)
	if err != nil {
		return 0, err
	}
	if insert {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO payments (id, group_id, owner_id, max_holders, version, fields, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			c.ID, c.GroupID, c.OwnerID, c.MaxHolders, next, fields,
			formatTime(c.CreatedAt), formatTime(c.UpdatedAt))
	} else {
		_, err = tx.ExecContext(ctx,
			`UPDATE payments SET group_id = ?, owner_id = ?, max_holders = ?, version = ?,
			 fields = ?, updated_at = ? WHERE id = ?`,
			c.GroupID, c.OwnerID, c.MaxHolders, next, fields, formatTime(c.UpdatedAt), c.ID)
	}
	if err != nil {
		return 0, fmt.Errorf("save payment %s: %w", c.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("save payment %s: %w", c.ID, err)
	}
	return next, nil
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
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, strategy, refs, created_at, updated_at FROM payment_groups ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list payment groups: %w", err)
	}
	defer rows.Close()

	listed := make([]payments.Group, 0)
	for rows.Next() {
		var g payments.Group
		var refs, created, updated string
		if err := rows.Scan(&g.ID, &g.Strategy, &refs, &created, &updated); err != nil {
			return nil, fmt.Errorf("list payment groups: %w", err)
		}
		if g.Refs, err = parseMap[string](refs); err != nil {
			return nil, fmt.Errorf("list payment groups: decode refs of %s: %w", g.ID, err)
		}
		if g.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list payment groups: %w", err)
		}
		if g.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list payment groups: %w", err)
		}
		listed = append(listed, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list payment groups: %w", err)
	}
	return listed, nil
}

// SaveGroup upserts the group's record. created_at is written on insert and
// never overwritten: when a group was created is not something a later save
// gets to revise.
func (s *Payments) SaveGroup(ctx context.Context, g payments.Group) error {
	refs, err := formatMap(g.Refs)
	if err != nil {
		return fmt.Errorf("save payment group %s: encode refs: %w", g.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO payment_groups (id, strategy, refs, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET strategy = excluded.strategy,
		 refs = excluded.refs, updated_at = excluded.updated_at`,
		g.ID, g.Strategy, refs, formatTime(g.CreatedAt), formatTime(g.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save payment group %s: %w", g.ID, err)
	}
	return nil
}

// DeleteGroup removes the group's record; absent rows are a no-op. Member
// payments are the manager's to delete — the store cascades nothing.
func (s *Payments) DeleteGroup(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM payment_groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete payment group %s: %w", id, err)
	}
	return nil
}
