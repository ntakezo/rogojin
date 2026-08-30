package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ntakezo/rogojin/accounts"
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
		SQL: `CREATE TABLE IF NOT EXISTS accounts (
			id          TEXT PRIMARY KEY,
			group_id    TEXT NOT NULL DEFAULT 'global',
			owner_id    TEXT NOT NULL DEFAULT '',
			max_holders INTEGER NOT NULL DEFAULT 0,
			successes   INTEGER NOT NULL DEFAULT 0,
			failures    INTEGER NOT NULL DEFAULT 0,
			fields      TEXT NOT NULL DEFAULT '',
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "create account_groups table",
		SQL: `CREATE TABLE IF NOT EXISTS account_groups (
			id          TEXT PRIMARY KEY,
			max_holders INTEGER NOT NULL DEFAULT 0,
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "add strategy column to account_groups",
		SQL:  `ALTER TABLE account_groups ADD COLUMN strategy TEXT NOT NULL DEFAULT ''`,
	},
	{
		Name: "add email_id column to accounts",
		SQL:  `ALTER TABLE accounts ADD COLUMN email_id TEXT NOT NULL DEFAULT ''`,
	},
	{
		Name: "add refs column to account_groups",
		SQL:  `ALTER TABLE account_groups ADD COLUMN refs TEXT NOT NULL DEFAULT ''`,
	},
}

// List returns every stored account in stable id order, so the manager's pool
// order is deterministic. The successes and failures columns are legacy —
// accounts keep no lease outcomes — and are left unread.
func (s *Accounts) List(ctx context.Context) ([]accounts.Account, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, group_id, owner_id, max_holders, email_id, fields, created_at, updated_at
		 FROM accounts ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	defer rows.Close()

	listed := make([]accounts.Account, 0)
	for rows.Next() {
		var a accounts.Account
		var fields, created, updated string
		if err := rows.Scan(&a.ID, &a.GroupID, &a.OwnerID, &a.MaxHolders, &a.EmailID, &fields, &created, &updated); err != nil {
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

// Save upserts the account's record: group, holder policy, lock owner, the
// forwarding email, fields, and updated_at. created_at is written on
// insert and never overwritten, so a later lock cannot revise it.
func (s *Accounts) Save(ctx context.Context, a accounts.Account) error {
	fields, err := formatFields(a.Fields)
	if err != nil {
		return fmt.Errorf("save account %s: %w", a.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO accounts (id, group_id, owner_id, max_holders, email_id, fields, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET group_id = excluded.group_id,
		 owner_id = excluded.owner_id, max_holders = excluded.max_holders,
		 email_id = excluded.email_id, fields = excluded.fields,
		 updated_at = excluded.updated_at`,
		a.ID, a.GroupID, a.OwnerID, a.MaxHolders,
		a.EmailID, fields, formatTime(a.CreatedAt), formatTime(a.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save account %s: %w", a.ID, err)
	}
	return nil
}

// Delete removes the account's record; absent rows are a no-op.
func (s *Accounts) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM accounts WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete account %s: %w", id, err)
	}
	return nil
}

// ListGroups returns every stored account group in stable id order. The
// max_holders column is legacy — holder policy lives on the account — and is
// left unread.
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
