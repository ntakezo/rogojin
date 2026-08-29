// Package emailsqlite provides a file-backed, durable implementation of the
// email.Repository port. A consumer that does not want to write its own
// email store can inject SQLite.
//
// Inbox credentials are stored as one JSON text column, in the clear: wrap
// this store to encrypt them if the database is not trusted. An empty
// vendor column marks an address-only email — one with no inbox at all.
package emailsqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/persistence/sqlitemigrate"
)

// SQLite is a durable email.Repository backed by a single SQLite database file.
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
	return sqlitemigrate.Run(db, "email", migrations)
}

// migrations is the ordered schema history of the email store. Append new
// steps to the end; never edit or reorder shipped ones: the ledger records
// which of them have already run on existing databases by position.
var migrations = []sqlitemigrate.Migration{
	{
		Name: "create emails table",
		SQL: `CREATE TABLE IF NOT EXISTS emails (
			id           TEXT PRIMARY KEY,
			address      TEXT NOT NULL DEFAULT '',
			vendor       TEXT NOT NULL DEFAULT '',
			auth         TEXT NOT NULL DEFAULT '',
			last_uid     INTEGER NOT NULL DEFAULT 0,
			uid_validity INTEGER NOT NULL DEFAULT 0,
			created_at   TEXT NOT NULL DEFAULT '',
			updated_at   TEXT NOT NULL DEFAULT ''
		)`,
	},
}

// Close closes the underlying database.
func (s *SQLite) Close() error {
	return s.db.Close()
}

// List returns every stored email in stable id order, so the manager's
// inventory order is deterministic.
func (s *SQLite) List(ctx context.Context) ([]email.Email, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, address, vendor, auth, last_uid, uid_validity, created_at, updated_at
		 FROM emails ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list emails: %w", err)
	}
	defer rows.Close()

	listed := make([]email.Email, 0)
	for rows.Next() {
		var e email.Email
		var vendor, auth, created, updated string
		var lastUID, uidValidity uint32
		if err := rows.Scan(&e.ID, &e.Address, &vendor, &auth, &lastUID, &uidValidity, &created, &updated); err != nil {
			return nil, fmt.Errorf("list emails: %w", err)
		}
		if e.Inbox, err = parseInbox(e.ID, vendor, auth, lastUID, uidValidity); err != nil {
			return nil, fmt.Errorf("list emails: %w", err)
		}
		if e.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list emails: %w", err)
		}
		if e.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list emails: %w", err)
		}
		listed = append(listed, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list emails: %w", err)
	}
	return listed, nil
}

// Save upserts the email's record: address, inbox credentials, cursor, and
// updated_at. created_at is written on insert and never overwritten, so a
// cursor advance cannot revise it.
func (s *SQLite) Save(ctx context.Context, e email.Email) error {
	vendor, auth, lastUID, uidValidity, err := formatInbox(e.Inbox)
	if err != nil {
		return fmt.Errorf("save email %s: %w", e.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO emails (id, address, vendor, auth, last_uid, uid_validity, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET address = excluded.address,
		 vendor = excluded.vendor, auth = excluded.auth,
		 last_uid = excluded.last_uid, uid_validity = excluded.uid_validity,
		 updated_at = excluded.updated_at`,
		e.ID, e.Address, vendor, auth, lastUID, uidValidity,
		formatTime(e.CreatedAt), formatTime(e.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save email %s: %w", e.ID, err)
	}
	return nil
}

// Delete removes the email's record; absent rows are a no-op.
func (s *SQLite) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM emails WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete email %s: %w", id, err)
	}
	return nil
}

// formatInbox flattens the optional inbox into its columns; a nil inbox
// stores an empty vendor, the address-only marker.
func formatInbox(in *email.Inbox) (vendor, auth string, lastUID, uidValidity uint32, err error) {
	if in == nil {
		return "", "", 0, 0, nil
	}
	encoded, err := json.Marshal(in.Auth)
	if err != nil {
		return "", "", 0, 0, fmt.Errorf("encode auth: %w", err)
	}
	return string(in.Vendor), string(encoded), in.LastUID, in.UIDValidity, nil
}

// parseInbox is the inverse of formatInbox.
func parseInbox(id, vendor, auth string, lastUID, uidValidity uint32) (*email.Inbox, error) {
	if vendor == "" {
		return nil, nil
	}
	in := &email.Inbox{Vendor: email.Vendor(vendor), LastUID: lastUID, UIDValidity: uidValidity}
	if auth != "" {
		if err := json.Unmarshal([]byte(auth), &in.Auth); err != nil {
			return nil, fmt.Errorf("decode auth of email %s: %w", id, err)
		}
	}
	return in, nil
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
