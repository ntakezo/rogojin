package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/ntakezo/rogojin/proxies"
)

// Proxies is the proxies.Repository: one row per proxy, with the URL and the
// success and failure counts the bayesian strategy learns from.
type Proxies struct {
	db *sql.DB
}

// NewProxies builds the proxies store on db, bringing its tables up to the
// current schema.
func NewProxies(db *DB) (proxies.Repository, error) {
	if err := migrate(db.db, "proxies", proxyMigrations); err != nil {
		return nil, err
	}
	return &Proxies{db: db.db}, nil
}

// proxyMigrations is the ordered schema history of the proxies store. Append
// new steps to the end; never edit or reorder shipped ones: the ledger records
// which of them have already run on existing databases by position.
var proxyMigrations = []migration{
	{
		Name: "create proxies table",
		SQL: `CREATE TABLE proxies (
			id          TEXT PRIMARY KEY,
			url         TEXT NOT NULL DEFAULT '',
			group_id    TEXT NOT NULL DEFAULT 'global',
			owner_id    TEXT NOT NULL DEFAULT '',
			max_holders INTEGER NOT NULL DEFAULT 0,
			successes   INTEGER NOT NULL DEFAULT 0,
			failures    INTEGER NOT NULL DEFAULT 0,
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "create proxy_groups table",
		SQL: `CREATE TABLE proxy_groups (
			id         TEXT PRIMARY KEY,
			strategy   TEXT NOT NULL DEFAULT '',
			refs       TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL DEFAULT ''
		)`,
	},
}

// List returns every stored proxy in stable id order, so the manager's pool
// order is deterministic.
func (s *Proxies) List(ctx context.Context) ([]proxies.Proxy, error) {
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

// Save upserts the proxy's record: URL, group, holder policy, lock owner,
// stats, and updated_at. created_at is written on insert and never
// overwritten, so a lock or a stat update cannot revise it.
func (s *Proxies) Save(ctx context.Context, p proxies.Proxy) error {
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
func (s *Proxies) Delete(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM proxies WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete proxy %s: %w", id, err)
	}
	return nil
}

// ListGroups returns every stored proxy group in stable id order.
func (s *Proxies) ListGroups(ctx context.Context) ([]proxies.Group, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, strategy, refs, created_at, updated_at FROM proxy_groups ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list proxy groups: %w", err)
	}
	defer rows.Close()

	listed := make([]proxies.Group, 0)
	for rows.Next() {
		var g proxies.Group
		var refs, created, updated string
		if err := rows.Scan(&g.ID, &g.Strategy, &refs, &created, &updated); err != nil {
			return nil, fmt.Errorf("list proxy groups: %w", err)
		}
		if g.Refs, err = parseMap[string](refs); err != nil {
			return nil, fmt.Errorf("list proxy groups: decode refs of %s: %w", g.ID, err)
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
func (s *Proxies) SaveGroup(ctx context.Context, g proxies.Group) error {
	refs, err := formatMap(g.Refs)
	if err != nil {
		return fmt.Errorf("save proxy group %s: encode refs: %w", g.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO proxy_groups (id, strategy, refs, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET strategy = excluded.strategy,
		 refs = excluded.refs, updated_at = excluded.updated_at`,
		g.ID, g.Strategy, refs, formatTime(g.CreatedAt), formatTime(g.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save proxy group %s: %w", g.ID, err)
	}
	return nil
}

// DeleteGroup removes the group's record; absent rows are a no-op. Member
// proxies are the manager's to delete — the store cascades nothing.
func (s *Proxies) DeleteGroup(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM proxy_groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete proxy group %s: %w", id, err)
	}
	return nil
}
