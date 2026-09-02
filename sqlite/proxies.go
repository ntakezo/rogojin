package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/leasing"
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
	{
		Name: "add version column",
		SQL:  `ALTER TABLE proxies ADD COLUMN version INTEGER NOT NULL DEFAULT 0`,
	},
	{
		Name: "create proxy_holds table",
		SQL:  holdsSchema("proxy_holds"),
	},
	{
		Name: "create proxy_counters table",
		SQL:  countersSchema("proxy_counters"),
	},
	// Outcome stats move from row columns into the counters table, where an
	// increment is atomic and outcomes reported by several processes land
	// whole. The legacy columns are frozen at their backfilled values — Save
	// no longer writes them, List no longer reads them — and disappear when
	// the histories collapse to their next baseline.
	{
		Name: "backfill successes counters",
		SQL: `INSERT INTO proxy_counters (scope, name, value)
			SELECT id, 'successes', successes FROM proxies WHERE successes > 0`,
	},
	{
		Name: "backfill failures counters",
		SQL: `INSERT INTO proxy_counters (scope, name, value)
			SELECT id, 'failures', failures FROM proxies WHERE failures > 0`,
	},
}

// proxyTables wires the shared leasing mechanics to this store's tables.
var proxyTables = leaseTables{noun: "proxy", records: "proxies", holds: "proxy_holds", counters: "proxy_counters"}

// List returns every stored proxy in stable id order, so the manager's pool
// order is deterministic. Successes and Failures are projected from the
// counters table — the durable truth ReleaseOutcome increments — not from the
// frozen legacy columns.
func (s *Proxies) List(ctx context.Context) ([]proxies.Proxy, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT p.id, p.url, p.group_id, p.owner_id, p.max_holders, p.version,
		        COALESCE(cs.value, 0), COALESCE(cf.value, 0), p.created_at, p.updated_at
		 FROM proxies p
		 LEFT JOIN proxy_counters cs ON cs.scope = p.id AND cs.name = 'successes'
		 LEFT JOIN proxy_counters cf ON cf.scope = p.id AND cf.name = 'failures'
		 ORDER BY p.id`)
	if err != nil {
		return nil, fmt.Errorf("list proxies: %w", err)
	}
	defer rows.Close()

	listed := make([]proxies.Proxy, 0)
	for rows.Next() {
		var p proxies.Proxy
		var created, updated string
		if err := rows.Scan(&p.ID, &p.URL, &p.GroupID, &p.OwnerID, &p.MaxHolders, &p.Version, &p.Successes, &p.Failures, &created, &updated); err != nil {
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

// Save writes the proxy's record conditionally on its Version — see
// leasing.Repository. created_at is written on insert and never overwritten,
// so a lock cannot revise it. The record's Successes and Failures are not
// written at all: their durable truth is the counters table, and a re-saved
// record clobbering a concurrent increment is exactly the lost update the
// counters exist to end.
func (s *Proxies) Save(ctx context.Context, p proxies.Proxy) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("save proxy %s: %w", p.ID, err)
	}
	defer tx.Rollback()

	insert, next, err := versionGate(ctx, tx, proxyTables, p.ID, p.Version)
	if err != nil {
		return 0, err
	}
	if insert {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO proxies (id, url, group_id, owner_id, max_holders, version, created_at, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			p.ID, p.URL, p.GroupID, p.OwnerID, p.MaxHolders, next,
			formatTime(p.CreatedAt), formatTime(p.UpdatedAt))
	} else {
		_, err = tx.ExecContext(ctx,
			`UPDATE proxies SET url = ?, group_id = ?, owner_id = ?, max_holders = ?, version = ?,
			 updated_at = ? WHERE id = ?`,
			p.URL, p.GroupID, p.OwnerID, p.MaxHolders, next,
			formatTime(p.UpdatedAt), p.ID)
	}
	if err != nil {
		return 0, fmt.Errorf("save proxy %s: %w", p.ID, err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("save proxy %s: %w", p.ID, err)
	}
	return next, nil
}

// Acquire takes or re-enters a hold on the proxy under cap — see
// leasing.Repository.
func (s *Proxies) Acquire(ctx context.Context, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	return acquireHold(ctx, s.db, proxyTables, resourceID, taskID, cap, ttl)
}

// ReleaseHold decrements the task's hold, removing it at zero.
func (s *Proxies) ReleaseHold(ctx context.Context, resourceID, taskID string) error {
	return releaseHold(ctx, s.db, proxyTables, resourceID, taskID)
}

// RenewHolds extends every unexpired hold the task has.
func (s *Proxies) RenewHolds(ctx context.Context, taskID string, ttl time.Duration) error {
	return renewHolds(ctx, s.db, proxyTables, taskID, ttl)
}

// ListHolds returns every hold row, expired ones included.
func (s *Proxies) ListHolds(ctx context.Context) ([]leasing.Hold, error) {
	return listHolds(ctx, s.db, proxyTables)
}

// ClaimLock binds the proxy to the task iff unlocked or already its own.
func (s *Proxies) ClaimLock(ctx context.Context, resourceID, taskID string) error {
	return claimLock(ctx, s.db, proxyTables, resourceID, taskID)
}

// ReleaseLock clears the lock iff the task owns it.
func (s *Proxies) ReleaseLock(ctx context.Context, resourceID, taskID string) error {
	return releaseLock(ctx, s.db, proxyTables, resourceID, taskID)
}

// Increment atomically adds delta to the counter under (scope, name).
func (s *Proxies) Increment(ctx context.Context, scope, name string, delta int64) (int64, error) {
	return incrementCounter(ctx, s.db, proxyTables, scope, name, delta)
}

// Delete removes the proxy's record and its holds; absent rows are a no-op.
func (s *Proxies) Delete(ctx context.Context, id string) error {
	return deleteWithHolds(ctx, s.db, proxyTables, id)
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
