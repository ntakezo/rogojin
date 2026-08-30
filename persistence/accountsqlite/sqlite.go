// Package accountsqlite provides a file-backed, durable implementation of the
// accounts.Repository port. A consumer that does not want to write its own
// account store can inject SQLite.
//
// Account fields are stored as one JSON text column, so the schema knows
// nothing about what any workflow's accounts contain and never needs a
// migration when a new workflow asks for different ones. They are stored in the
// clear: wrap this store to encrypt them if the database is not trusted.
package accountsqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/persistence/sqlitemigrate"
)

// SQLite is a durable accounts.Repository backed by a single SQLite database file.
type SQLite struct {
	db *sql.DB
}

// NewSQLite opens (creating if absent) the database at dsn and ensures the schema exists.
func NewSQLite(dsn string) (*SQLite, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite serializes writes per file; one connection avoids "database is locked" under concurrent saves.
	db.SetMaxOpenConns(1)

	if err := ensureSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return &SQLite{db: db}, nil
}

// ensureSchema brings the database up to the latest schema version, applying any
// migrations it has not yet seen.
func ensureSchema(db *sql.DB) error {
	return sqlitemigrate.Run(db, "accounts", migrations)
}

// migrations is the ordered schema history of the accounts store. Append new
// steps to the end; never edit or reorder shipped ones: the ledger records
// which of them have already run on existing databases by position.
var migrations = []sqlitemigrate.Migration{
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

// Close closes the underlying database.
func (s *SQLite) Close() error {
	return s.db.Close()
}

// List returns every stored account in stable id order, so the manager's pool
// order is deterministic. The successes and failures columns are legacy —
// accounts keep no lease outcomes — and are left unread.
func (s *SQLite) List(ctx context.Context) ([]accounts.Account, error) {
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
func (s *SQLite) Save(ctx context.Context, a accounts.Account) error {
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
func (s *SQLite) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM accounts WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete account %s: %w", id, err)
	}
	return nil
}

// ListGroups returns every stored account group in stable id order. The
// max_holders column is legacy — holder policy lives on the account — and is
// left unread.
func (s *SQLite) ListGroups(ctx context.Context) ([]accounts.Group, error) {
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
		if g.Refs, err = parseRefs(refs); err != nil {
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
func (s *SQLite) SaveGroup(ctx context.Context, g accounts.Group) error {
	refs, err := formatRefs(g.Refs)
	if err != nil {
		return fmt.Errorf("save account group %s: %w", g.ID, err)
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
func (s *SQLite) DeleteGroup(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM account_groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete account group %s: %w", id, err)
	}
	return nil
}

// formatFields stores the workflow's fields as JSON text, rejecting a payload
// that is not valid JSON: a column that round-trips garbage would surface it as
// a decode failure inside a run, far from the write that caused it. Absent
// fields store as "".
func formatFields(fields json.RawMessage) (string, error) {
	if len(fields) == 0 {
		return "", nil
	}
	if !json.Valid(fields) {
		return "", fmt.Errorf("fields are not valid JSON")
	}
	return string(fields), nil
}

// parseFields is the inverse of formatFields.
func parseFields(fields string) json.RawMessage {
	if fields == "" {
		return nil
	}
	return json.RawMessage(fields)
}

// formatRefs stores a group's refs as JSON text; no refs store as "" so
// pre-refs rows keep round-tripping.
func formatRefs(refs map[string]string) (string, error) {
	if len(refs) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(refs)
	if err != nil {
		return "", fmt.Errorf("encode refs: %w", err)
	}
	return string(encoded), nil
}

// parseRefs is the inverse of formatRefs.
func parseRefs(refs string) (map[string]string, error) {
	if refs == "" {
		return nil, nil
	}
	decoded := make(map[string]string)
	if err := json.Unmarshal([]byte(refs), &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

// formatTime stores timestamps as RFC3339Nano UTC text; the zero time stores
// as "" so pre-timestamp rows keep round-tripping.
func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// parseTime is the inverse of formatTime.
func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, s)
}
