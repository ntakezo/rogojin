// Package proxysqlite provides a file-backed, durable implementation of the
// proxies.Repository port. A consumer that does not want to write its own
// proxy store can inject SQLite.
package proxysqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ntakezo/rogojin/persistence/sqlitemigrate"
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
	{
		Name: "add group_id column placing existing proxies in the global group",
		SQL:  `ALTER TABLE proxies ADD COLUMN group_id TEXT NOT NULL DEFAULT 'global'`,
	},
	{
		Name: "add max_holders column for per-proxy holder policies",
		SQL:  `ALTER TABLE proxies ADD COLUMN max_holders INTEGER NOT NULL DEFAULT 0`,
	},
	{
		Name: "add created_at column to proxies",
		SQL:  `ALTER TABLE proxies ADD COLUMN created_at TEXT NOT NULL DEFAULT ''`,
	},
	{
		Name: "add updated_at column to proxies",
		SQL:  `ALTER TABLE proxies ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''`,
	},
	{
		Name: "create proxy_groups table",
		SQL: `CREATE TABLE IF NOT EXISTS proxy_groups (
			id          TEXT PRIMARY KEY,
			strategy    TEXT NOT NULL DEFAULT '',
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

// List returns every stored proxy in stable id order, so the manager's pool
// order is deterministic.
func (s *SQLite) List(ctx context.Context) ([]proxies.Proxy, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, url, group_id, owner_id, max_holders, successes, failures, created_at, updated_at
		 FROM proxies ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list proxies: %w", err)
	}
	defer rows.Close()

	listed := make([]proxies.Proxy, 0)
	for rows.Next() {
		var p proxies.Proxy
		var created, updated string
		if err := rows.Scan(&p.ID, &p.URL, &p.GroupID, &p.OwnerID, &p.MaxHolders, &p.Successes, &p.Failures, &created, &updated); err != nil {
			return nil, fmt.Errorf("list proxies: %w", err)
		}
		if p.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list proxies: %w", err)
		}
		if p.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list proxies: %w", err)
		}
		listed = append(listed, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list proxies: %w", err)
	}
	return listed, nil
}

// Save upserts the proxy's record: url, group, holder policy, lock owner,
// stats, and updated_at. created_at is written on insert and never
// overwritten, so a lock or a stat update cannot revise it.
func (s *SQLite) Save(ctx context.Context, p proxies.Proxy) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO proxies (id, url, group_id, owner_id, max_holders, successes, failures, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET url = excluded.url, group_id = excluded.group_id,
		 owner_id = excluded.owner_id, max_holders = excluded.max_holders,
		 successes = excluded.successes, failures = excluded.failures,
		 updated_at = excluded.updated_at`,
		p.ID, p.URL, p.GroupID, p.OwnerID, p.MaxHolders, p.Successes, p.Failures,
		formatTime(p.CreatedAt), formatTime(p.UpdatedAt))
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

// ListGroups returns every stored proxy group in stable id order.
func (s *SQLite) ListGroups(ctx context.Context) ([]proxies.Group, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, strategy, max_holders, created_at, updated_at FROM proxy_groups ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list proxy groups: %w", err)
	}
	defer rows.Close()

	listed := make([]proxies.Group, 0)
	for rows.Next() {
		var g proxies.Group
		var created, updated string
		if err := rows.Scan(&g.ID, &g.Strategy, &g.MaxHolders, &created, &updated); err != nil {
			return nil, fmt.Errorf("list proxy groups: %w", err)
		}
		if g.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list proxy groups: %w", err)
		}
		if g.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list proxy groups: %w", err)
		}
		listed = append(listed, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list proxy groups: %w", err)
	}
	return listed, nil
}

// SaveGroup upserts the group's record. created_at is written on insert and
// never overwritten: when a group was created is not something a later save
// gets to revise.
func (s *SQLite) SaveGroup(ctx context.Context, g proxies.Group) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO proxy_groups (id, strategy, max_holders, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET strategy = excluded.strategy,
		 max_holders = excluded.max_holders, updated_at = excluded.updated_at`,
		g.ID, g.Strategy, g.MaxHolders, formatTime(g.CreatedAt), formatTime(g.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save proxy group %s: %w", g.ID, err)
	}
	return nil
}

// DeleteGroup removes the group's record; absent rows are a no-op. Member
// proxies are the manager's to delete — the store cascades nothing.
func (s *SQLite) DeleteGroup(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM proxy_groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete proxy group %s: %w", id, err)
	}
	return nil
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
