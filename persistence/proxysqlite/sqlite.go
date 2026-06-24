// Package proxysqlite provides a file-backed, durable implementation of the
// proxies.Repository port. A consumer that does not want to write its own
// proxy store can inject SQLite.
package proxysqlite

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ntakezo/rogojin/persistence/internal/sqlitemigrate"
	"github.com/ntakezo/rogojin/proxies"
)

// SQLite is a durable proxies.Repository backed by a single SQLite database file.
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
	return sqlitemigrate.Run(db, migrations)
}

// migrations is the ordered schema history of the proxies store. Append new steps
// to the end; never edit or reorder shipped ones, since PRAGMA user_version pins
// how many have already run on existing databases.
var migrations = []sqlitemigrate.Migration{
	{
		Name: "create proxies table",
		SQL: `CREATE TABLE IF NOT EXISTS proxies (
			id        TEXT PRIMARY KEY,
			url       TEXT NOT NULL DEFAULT '',
			owner_id  TEXT NOT NULL DEFAULT '',
			successes INTEGER NOT NULL DEFAULT 0,
			failures  INTEGER NOT NULL DEFAULT 0
		)`,
	},
}

// Close closes the underlying database.
func (s *SQLite) Close() error {
	return s.db.Close()
}

// List returns every stored proxy in stable id order, so the manager's pool
// order is deterministic.
func (s *SQLite) List(ctx context.Context) ([]proxies.Proxy, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, url, owner_id, successes, failures FROM proxies ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list proxies: %w", err)
	}
	defer rows.Close()

	listed := make([]proxies.Proxy, 0)
	for rows.Next() {
		var p proxies.Proxy
		if err := rows.Scan(&p.ID, &p.URL, &p.OwnerID, &p.Successes, &p.Failures); err != nil {
			return nil, fmt.Errorf("list proxies: %w", err)
		}
		listed = append(listed, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list proxies: %w", err)
	}
	return listed, nil
}

// Save upserts the proxy's full record: url, lock owner, and stats.
func (s *SQLite) Save(ctx context.Context, p proxies.Proxy) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO proxies (id, url, owner_id, successes, failures) VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET url = excluded.url, owner_id = excluded.owner_id,
		 successes = excluded.successes, failures = excluded.failures`,
		p.ID, p.URL, p.OwnerID, p.Successes, p.Failures)
	if err != nil {
		return fmt.Errorf("save proxy %s: %w", p.ID, err)
	}
	return nil
}

// Delete removes the proxy's record; absent rows are a no-op.
func (s *SQLite) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM proxies WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete proxy %s: %w", id, err)
	}
	return nil
}
