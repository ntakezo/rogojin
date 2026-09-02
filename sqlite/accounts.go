package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/leasing"
)

// Accounts is the accounts.Repository: one row per account, its workflow's
// own fields in a JSON text column so the schema knows nothing about what any
// workflow's accounts contain and never needs a migration when a new one asks
// for different ones. Credentials are stored in the clear.
type Accounts struct {
	db *sql.DB
}

// NewAccounts builds the accounts store on db, bringing its tables up to the
// current schema.
func NewAccounts(db *DB) (accounts.Repository, error) {
	if err := migrate(db.db, "accounts", accountMigrations); err != nil {
		return nil, err
	}
	return &Accounts{db: db.db}, nil
}

// accountMigrations is the ordered schema history of the accounts store.
// Append new steps to the end; never edit or reorder shipped ones: the ledger
// records which of them have already run on existing databases by position.
var accountMigrations = []migration{
	{
		Name: "create accounts table",
		SQL: `CREATE TABLE accounts (
			id          TEXT PRIMARY KEY,
			group_id    TEXT NOT NULL DEFAULT 'global',
			owner_id    TEXT NOT NULL DEFAULT '',
			max_holders INTEGER NOT NULL DEFAULT 0,
			email_id    TEXT NOT NULL DEFAULT '',
			fields      TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "create account_groups table",
		SQL: `CREATE TABLE account_groups (
			id         TEXT PRIMARY KEY,
			strategy   TEXT NOT NULL DEFAULT '',
			refs       TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "add version column",
		SQL:  `ALTER TABLE accounts ADD COLUMN version INTEGER NOT NULL DEFAULT 0`,
	},
	{
		Name: "create account_holds table",
		SQL:  holdsSchema("account_holds"),
	},
	{
		Name: "create account_counters table",
		SQL:  countersSchema("account_counters"),
	},
}

// accountTables wires the shared leasing mechanics to this store's tables.
var accountTables = leaseTables{noun: "account", records: "accounts", holds: "account_holds", counters: "account_counters"}

// List returns every stored account in stable id order, so the manager's pool
// order is deterministic.
func (s *Accounts) List(ctx context.Context) ([]accounts.Account, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, group_id, owner_id, max_holders, version, email_id, fields, created_at, updated_at
		 FROM accounts ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	defer rows.Close()

	listed := make([]accounts.Account, 0)
	for rows.Next() {
		var a accounts.Account
		var fields, created, updated string
		if err := rows.Scan(&a.ID, &a.GroupID, &a.OwnerID, &a.MaxHolders, &a.Version, &a.EmailID, &fields, &created, &updated); err != nil {
			return nil, fmt.Errorf("list accounts: %w", err)
		}
		a.Fields = parseFields(fields)
		if a.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list accounts: %w", err)
		}
		if a.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list accounts: %w", err)
		}
		listed = append(listed, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	return listed, nil
}

// Save writes the account's record conditionally on its Version — see
// leasing.Repository. created_at is written on insert and never overwritten,
// so a later lock cannot revise it.
func (s *Accounts) Save(ctx context.Context, a accounts.Account) (int64, error) {
	fields, err := formatFields(a.Fields)
	if err != nil {
		return 0, fmt.Errorf("save account %s: %w", a.ID, err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("save account %s: %w", a.ID, err)
	}
	defer tx.Rollback()

	insert, next, err := versionGate(ctx, tx, accountTables, a.ID, a.Version)
	if err != nil {
		return 0, err
	}
	if insert {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO accounts (id, group_id, owner_id, max_holders, version, email_id, fields, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			a.ID, a.GroupID, a.OwnerID, a.MaxHolders, next,
			a.EmailID, fields, formatTime(a.CreatedAt), formatTime(a.UpdatedAt))
	} else {
		_, err = tx.ExecContext(ctx,
			`UPDATE accounts SET group_id = ?, owner_id = ?, max_holders = ?, version = ?,
			 email_id = ?, fields = ?, updated_at = ? WHERE id = ?`,
			a.GroupID, a.OwnerID, a.MaxHolders, next, a.EmailID, fields,
			formatTime(a.UpdatedAt), a.ID)
	}
	if err != nil {
		return 0, fmt.Errorf("save account %s: %w", a.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("save account %s: %w", a.ID, err)
	}
	return next, nil
}

// Acquire takes or re-enters a hold on the account under cap — see
// leasing.Repository.
func (s *Accounts) Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	return acquireHold(ctx, s.db, accountTables, resourceID, taskID, cap, ttl)
}

// ReleaseHold decrements the task's hold, removing it at zero.
func (s *Accounts) ReleaseHold(ctx context.Context, resourceID, taskID string) error {
	return releaseHold(ctx, s.db, accountTables, resourceID, taskID)
}

// RenewHolds extends every unexpired hold the task has.
func (s *Accounts) RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	return renewHolds(ctx, s.db, accountTables, taskID, ttl)
}

// ListHolds returns every hold row, expired ones included.
func (s *Accounts) ListHolds(ctx context.Context) ([]leasing.Hold, error) {
	return listHolds(ctx, s.db, accountTables)
}

// ClaimLock binds the account to the task iff unlocked or already its own.
func (s *Accounts) ClaimLock(ctx context.Context, resourceID, taskID string) error {
	return claimLock(ctx, s.db, accountTables, resourceID, taskID)
}

// ReleaseLock clears the lock iff the task owns it.
func (s *Accounts) ReleaseLock(ctx context.Context, resourceID, taskID string) error {
	return releaseLock(ctx, s.db, accountTables, resourceID, taskID)
}

// Increment atomically adds delta to the counter under (scope, name).
func (s *Accounts) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return incrementCounter(ctx, s.db, accountTables, scope, name, delta)
}

// Delete removes the account's record and its holds; absent rows are a no-op.
func (s *Accounts) Delete(ctx context.Context, id string) error {
	return deleteWithHolds(ctx, s.db, accountTables, id)
}

// ListGroups returns every stored account group in stable id order.
func (s *Accounts) ListGroups(ctx context.Context) ([]accounts.Group, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, strategy, refs, created_at, updated_at FROM account_groups ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list account groups: %w", err)
	}
	defer rows.Close()

	listed := make([]accounts.Group, 0)
	for rows.Next() {
		var g accounts.Group
		var refs, created, updated string
		if err := rows.Scan(&g.ID, &g.Strategy, &refs, &created, &updated); err != nil {
			return nil, fmt.Errorf("list account groups: %w", err)
		}
		if g.Refs, err = parseMap[string](refs); err != nil {
			return nil, fmt.Errorf("list account groups: decode refs of %s: %w", g.ID, err)
		}
		if g.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list account groups: %w", err)
		}
		if g.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list account groups: %w", err)
		}
		listed = append(listed, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list account groups: %w", err)
	}
	return listed, nil
}

// SaveGroup upserts the group's record. created_at is written on insert and
// never overwritten: when a group was created is not something a later save
// gets to revise.
func (s *Accounts) SaveGroup(ctx context.Context, g accounts.Group) error {
	refs, err := formatMap(g.Refs)
	if err != nil {
		return fmt.Errorf("save account group %s: encode refs: %w", g.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO account_groups (id, strategy, refs, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET strategy = excluded.strategy,
		 refs = excluded.refs, updated_at = excluded.updated_at`,
		g.ID, g.Strategy, refs, formatTime(g.CreatedAt), formatTime(g.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save account group %s: %w", g.ID, err)
	}
	return nil
}

// DeleteGroup removes the group's record; absent rows are a no-op. Member
// accounts are the manager's to delete — the store cascades nothing.
func (s *Accounts) DeleteGroup(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM account_groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete account group %s: %w", id, err)
	}
	return nil
}
