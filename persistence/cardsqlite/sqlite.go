// Package cardsqlite provides a file-backed, durable implementation of the
// cards.Repository port. A consumer that does not want to write its own card
// store can inject SQLite.
//
// Card fields are stored as one JSON text column, so the schema knows nothing
// about what any workflow's cards contain — number, expiry, CVV, billing
// address — and never needs a migration when a new checkout asks for different
// ones.
//
// They are stored in the clear. This adapter is the plain-file default, not a
// vault: a database holding real card data belongs behind a store that seals
// cards.Card.Fields on the way down and opens it on the way up, which the
// Repository port is shaped to allow. Wrap this one, or write your own.
package cardsqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ntakezo/rogojin/cards"
	"github.com/ntakezo/rogojin/persistence/sqlitemigrate"
)

// SQLite is a durable cards.Repository backed by a single SQLite database file.
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
	return sqlitemigrate.Run(db, "cards", migrations)
}

// migrations is the ordered schema history of the cards store. Append new steps
// to the end; never edit or reorder shipped ones: the ledger records which of
// them have already run on existing databases by position.
var migrations = []sqlitemigrate.Migration{
	{
		Name: "create cards table",
		SQL: `CREATE TABLE IF NOT EXISTS cards (
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
		Name: "create card_groups table",
		SQL: `CREATE TABLE IF NOT EXISTS card_groups (
			id          TEXT PRIMARY KEY,
			max_holders INTEGER NOT NULL DEFAULT 0,
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
}

// Close closes the underlying database.
func (s *SQLite) Close() error {
	return s.db.Close()
}

// List returns every stored card in stable id order, so the manager's pool order
// is deterministic.
func (s *SQLite) List(ctx context.Context) ([]cards.Card, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, group_id, owner_id, max_holders, successes, failures, fields, created_at, updated_at
		 FROM cards ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list cards: %w", err)
	}
	defer rows.Close()

	listed := make([]cards.Card, 0)
	for rows.Next() {
		var c cards.Card
		var fields, created, updated string
		if err := rows.Scan(&c.ID, &c.GroupID, &c.OwnerID, &c.MaxHolders, &c.Successes, &c.Failures, &fields, &created, &updated); err != nil {
			return nil, fmt.Errorf("list cards: %w", err)
		}
		c.Fields = parseFields(fields)
		if c.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list cards: %w", err)
		}
		if c.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list cards: %w", err)
		}
		listed = append(listed, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list cards: %w", err)
	}
	return listed, nil
}

// Save upserts the card's record: group, holder policy, lock owner, fields,
// stats, and updated_at. created_at is written on insert and never overwritten,
// so a lock or a stat update cannot revise it.
func (s *SQLite) Save(ctx context.Context, c cards.Card) error {
	fields, err := formatFields(c.Fields)
	if err != nil {
		return fmt.Errorf("save card %s: %w", c.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO cards (id, group_id, owner_id, max_holders, successes, failures, fields, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET group_id = excluded.group_id,
		 owner_id = excluded.owner_id, max_holders = excluded.max_holders,
		 successes = excluded.successes, failures = excluded.failures,
		 fields = excluded.fields, updated_at = excluded.updated_at`,
		c.ID, c.GroupID, c.OwnerID, c.MaxHolders, c.Successes, c.Failures, fields,
		formatTime(c.CreatedAt), formatTime(c.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save card %s: %w", c.ID, err)
	}
	return nil
}

// Delete removes the card's record; absent rows are a no-op.
func (s *SQLite) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM cards WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete card %s: %w", id, err)
	}
	return nil
}

// ListGroups returns every stored card group in stable id order.
func (s *SQLite) ListGroups(ctx context.Context) ([]cards.Group, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, max_holders, created_at, updated_at FROM card_groups ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list card groups: %w", err)
	}
	defer rows.Close()

	listed := make([]cards.Group, 0)
	for rows.Next() {
		var g cards.Group
		var created, updated string
		if err := rows.Scan(&g.ID, &g.MaxHolders, &created, &updated); err != nil {
			return nil, fmt.Errorf("list card groups: %w", err)
		}
		if g.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list card groups: %w", err)
		}
		if g.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list card groups: %w", err)
		}
		listed = append(listed, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list card groups: %w", err)
	}
	return listed, nil
}

// SaveGroup upserts the group's record. created_at is written on insert and
// never overwritten: when a group was created is not something a later save gets
// to revise.
func (s *SQLite) SaveGroup(ctx context.Context, g cards.Group) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO card_groups (id, max_holders, created_at, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET max_holders = excluded.max_holders,
		 updated_at = excluded.updated_at`,
		g.ID, g.MaxHolders, formatTime(g.CreatedAt), formatTime(g.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save card group %s: %w", g.ID, err)
	}
	return nil
}

// DeleteGroup removes the group's record; absent rows are a no-op. Member cards
// are the manager's to delete — the store cascades nothing.
func (s *SQLite) DeleteGroup(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM card_groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete card group %s: %w", id, err)
	}
	return nil
}

// formatFields stores the workflow's fields as JSON text, rejecting a payload
// that is not valid JSON: a column that round-trips garbage would surface it as
// a decode failure inside a run, far from the write that caused it — and for a
// card, at the last state before payment. Absent fields store as "".
//
// A store that encrypts wraps this one and hands down ciphertext of its own
// shape; this validation is about what the plain adapter is asked to hold.
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

// formatTime stores timestamps as RFC3339Nano UTC text; the zero time stores as
// "" so pre-timestamp rows keep round-tripping.
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
