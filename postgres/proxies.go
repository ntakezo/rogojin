package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/proxies"
)

// Proxies is the proxies.Repository: one row per proxy with its URL; the
// success and failure counts the bayesian strategy learns from live in the
// counters table, where increments from every node land whole.
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

// proxyMigrations is the ordered schema history of the proxies store — a
// fresh baseline with no legacy stat columns: outcome stats were counters
// before this adapter existed.
var proxyMigrations = []migration{
	{
		Name: "create proxies table",
		SQL: `CREATE TABLE proxies (
			id          TEXT PRIMARY KEY,
			url         TEXT NOT NULL DEFAULT '',
			group_id    TEXT NOT NULL DEFAULT 'global',
			owner_id    TEXT NOT NULL DEFAULT '',
			max_holders BIGINT NOT NULL DEFAULT 0,
			version     BIGINT NOT NULL DEFAULT 0,
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
		)`,
	},
	{Name: "create proxy_groups table", SQL: groupsSchema("proxy_groups")},
	{Name: "create proxy_holds table", SQL: holdsSchema("proxy_holds")},
	{Name: "create proxy_counters table", SQL: countersSchema("proxy_counters")},
}

// proxyTables wires the shared leasing mechanics to this store's tables.
var proxyTables = leaseTables{noun: "proxy", records: "proxies", holds: "proxy_holds", counters: "proxy_counters"}

// List returns every stored proxy in stable id order. Successes and Failures
// are projected from the counters table — the durable truth ReleaseOutcome
// increments.
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
// leasing.Repository. The record's Successes and Failures are not written at
// all: their durable truth is the counters table, and a re-saved record
// clobbering a concurrent increment is exactly the lost update the counters
// exist to end.
func (s *Proxies) Save(ctx context.Context, p proxies.Proxy) (int64, error) {
	return saveVersioned(proxyTables, p.ID, p.Version,
		func(next int64) (sql.Result, error) {
			return s.db.ExecContext(ctx,
				`INSERT INTO proxies (id, url, group_id, owner_id, max_holders, version, created_at, updated_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (id) DO NOTHING`,
				p.ID, p.URL, p.GroupID, p.OwnerID, p.MaxHolders, next,
				formatTime(p.CreatedAt), formatTime(p.UpdatedAt))
		},
		func(next int64) (sql.Result, error) {
			return s.db.ExecContext(ctx,
				`UPDATE proxies SET url = $1, group_id = $2, owner_id = $3, max_holders = $4, version = $5,
				 updated_at = $6 WHERE id = $7 AND version = $8`,
				p.URL, p.GroupID, p.OwnerID, p.MaxHolders, next,
				formatTime(p.UpdatedAt), p.ID, p.Version)
		})
}

// Delete removes the proxy's record and its holds; absent rows are a no-op.
func (s *Proxies) Delete(ctx context.Context, id string) error {
	return deleteWithHolds(ctx, s.db, proxyTables, id)
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

// ListGroups returns every stored proxy group in stable id order.
func (s *Proxies) ListGroups(ctx context.Context) ([]proxies.Group, error) {
	return listGroups(ctx, s.db, proxyTables, "proxy_groups")
}

// SaveGroup upserts the group's record, preserving created_at.
func (s *Proxies) SaveGroup(ctx context.Context, g proxies.Group) error {
	return saveGroup(ctx, s.db, proxyTables, "proxy_groups", g)
}

// DeleteGroup removes the group's record; absent rows are a no-op.
func (s *Proxies) DeleteGroup(ctx context.Context, id string) error {
	return dropGroup(ctx, s.db, proxyTables, "proxy_groups", id)
}
