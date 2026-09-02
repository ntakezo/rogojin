package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/leasing"
)

// leaseTables names one leasing-shaped store's tables, so the hold, lock,
// counter, and versioning mechanics are written once and shared by payments,
// proxies, and accounts. The names are package constants, never caller
// input, which is what makes splicing them into SQL safe.
type leaseTables struct {
	noun     string
	records  string
	holds    string
	counters string
}

// holdsSchema and countersSchema render the shared table shapes for a store's
// baseline migration. expires_at is TIMESTAMPTZ and every predicate over it
// uses the server's now(): the store clock decides expiry, so the clocks of
// contending nodes never do.
func holdsSchema(table string) string {
	return `CREATE TABLE ` + table + ` (
		resource_id TEXT NOT NULL,
		task_id     TEXT NOT NULL,
		count       BIGINT NOT NULL,
		expires_at  TIMESTAMPTZ NOT NULL,
		PRIMARY KEY (resource_id, task_id)
	)`
}

func countersSchema(table string) string {
	return `CREATE TABLE ` + table + ` (
		scope TEXT NOT NULL,
		name  TEXT NOT NULL,
		value BIGINT NOT NULL,
		PRIMARY KEY (scope, name)
	)`
}

// ttlExpr is the SQL for "ttl from now, by the server's clock"; it takes the
// TTL as whole milliseconds.
const ttlExpr = `now() + ($%d * interval '1 millisecond')`

// saveVersioned runs the conditional write Save promises: version 0 is a
// create that loses to any existing row, version N replaces iff the stored
// version is still N. Each arm is one conditional statement, so two nodes
// racing a save resolve in the server — no read-then-write window — and the
// loser's zero-row result maps to ErrStale whether the row was taken, moved
// on, or deleted under it.
func saveVersioned(t leaseTables, id string, version int64, insert, update func(next int64) (sql.Result, error)) (int64, error) {
	write, next := update, version+1
	if version == 0 {
		write, next = insert, 1
	}
	res, err := write(next)
	if err != nil {
		return 0, fmt.Errorf("save %s %s: %w", t.noun, id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("save %s %s: %w", t.noun, id, err)
	}
	if n == 0 {
		return 0, fmt.Errorf("save %s %s: %w", t.noun, id, leasing.ErrStale)
	}
	return next, nil
}

// acquireHold takes or re-enters a hold inside one transaction serialized per
// resource by an advisory lock — a row lock cannot serialize acquirers of a
// resource that has no record row, and the store does not police existence.
// Expired rows are pruned on this write path, the cap is measured over what
// remains, and a resource locked by another task refuses with ErrLockHeld.
func acquireHold(ctx context.Context, db *sql.DB, t leaseTables, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	fail := func(err error) (leasing.Hold, error) {
		return leasing.Hold{}, fmt.Errorf("acquire %s %s for task %s: %w", t.noun, resourceID, taskID, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fail(err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, t.holds+"/"+resourceID); err != nil {
		return fail(err)
	}

	var owner string
	err = tx.QueryRowContext(ctx, `SELECT owner_id FROM `+t.records+` WHERE id = $1`, resourceID).Scan(&owner)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fail(err)
	}
	if owner != "" && owner != taskID {
		return fail(leasing.ErrLockHeld)
	}

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM `+t.holds+` WHERE resource_id = $1 AND expires_at <= now()`, resourceID); err != nil {
		return fail(err)
	}

	var count int
	err = tx.QueryRowContext(ctx,
		`SELECT count FROM `+t.holds+` WHERE resource_id = $1 AND task_id = $2`,
		resourceID, taskID).Scan(&count)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fail(err)
	}
	if count == 0 {
		var others int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM `+t.holds+` WHERE resource_id = $1 AND task_id <> $2`,
			resourceID, taskID).Scan(&others); err != nil {
			return fail(err)
		}
		if cap > 0 && others >= cap {
			return fail(leasing.ErrCapacity)
		}
	}

	var expires time.Time
	if err := tx.QueryRowContext(ctx,
		`INSERT INTO `+t.holds+` (resource_id, task_id, count, expires_at)
		 VALUES ($1, $2, $3, `+fmt.Sprintf(ttlExpr, 4)+`)
		 ON CONFLICT (resource_id, task_id) DO UPDATE SET count = EXCLUDED.count, expires_at = EXCLUDED.expires_at
		 RETURNING expires_at`,
		resourceID, taskID, count+1, ttl.Milliseconds()).Scan(&expires); err != nil {
		return fail(err)
	}
	if err := tx.Commit(); err != nil {
		return fail(err)
	}
	return leasing.Hold{ResourceID: resourceID, TaskID: taskID, Count: count + 1, ExpiresAt: expires.UTC()}, nil
}

// releaseHold decrements the task's hold, removing the row at zero; no row is
// a no-op.
func releaseHold(ctx context.Context, db *sql.DB, t leaseTables, resourceID, taskID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("release %s hold %s: %w", t.noun, resourceID, err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`UPDATE `+t.holds+` SET count = count - 1 WHERE resource_id = $1 AND task_id = $2`,
		resourceID, taskID); err != nil {
		return fmt.Errorf("release %s hold %s: %w", t.noun, resourceID, err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM `+t.holds+` WHERE resource_id = $1 AND task_id = $2 AND count <= 0`,
		resourceID, taskID); err != nil {
		return fmt.Errorf("release %s hold %s: %w", t.noun, resourceID, err)
	}
	return tx.Commit()
}

// renewHolds extends every unexpired hold the task has; expired rows stay
// expired, since their capacity may already be promised elsewhere.
func renewHolds(ctx context.Context, db *sql.DB, t leaseTables, taskID string, ttl time.Duration) error {
	_, err := db.ExecContext(ctx,
		`UPDATE `+t.holds+` SET expires_at = `+fmt.Sprintf(ttlExpr, 2)+` WHERE task_id = $1 AND expires_at > now()`,
		taskID, ttl.Milliseconds())
	if err != nil {
		return fmt.Errorf("renew %s holds of task %s: %w", t.noun, taskID, err)
	}
	return nil
}

// listHolds returns every hold row, expired ones included, ordered by
// resource then task.
func listHolds(ctx context.Context, db *sql.DB, t leaseTables) ([]leasing.Hold, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT resource_id, task_id, count, expires_at FROM `+t.holds+` ORDER BY resource_id, task_id`)
	if err != nil {
		return nil, fmt.Errorf("list %s holds: %w", t.noun, err)
	}
	defer rows.Close()

	listed := make([]leasing.Hold, 0)
	for rows.Next() {
		var h leasing.Hold
		var expires time.Time
		if err := rows.Scan(&h.ResourceID, &h.TaskID, &h.Count, &expires); err != nil {
			return nil, fmt.Errorf("list %s holds: %w", t.noun, err)
		}
		h.ExpiresAt = expires.UTC()
		listed = append(listed, h)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list %s holds: %w", t.noun, err)
	}
	return listed, nil
}

// claimLock binds the resource to the task iff unlocked or already its own,
// in one conditional update.
func claimLock(ctx context.Context, db *sql.DB, t leaseTables, resourceID, taskID string) error {
	res, err := db.ExecContext(ctx,
		`UPDATE `+t.records+` SET owner_id = $1, version = version + 1, updated_at = $2
		 WHERE id = $3 AND owner_id IN ('', $1)`,
		taskID, formatTime(time.Now()), resourceID)
	if err != nil {
		return fmt.Errorf("claim lock on %s %s: %w", t.noun, resourceID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("claim lock on %s %s: %w", t.noun, resourceID, err)
	} else if n > 0 {
		return nil
	}
	var owner string
	err = db.QueryRowContext(ctx, `SELECT owner_id FROM `+t.records+` WHERE id = $1`, resourceID).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("claim lock on %s %s: resource not found", t.noun, resourceID)
	}
	if err != nil {
		return fmt.Errorf("claim lock on %s %s: %w", t.noun, resourceID, err)
	}
	return fmt.Errorf("claim lock on %s %s for task %s: %w", t.noun, resourceID, taskID, leasing.ErrLockHeld)
}

// releaseLock clears the lock iff the task owns it; otherwise a no-op.
func releaseLock(ctx context.Context, db *sql.DB, t leaseTables, resourceID, taskID string) error {
	_, err := db.ExecContext(ctx,
		`UPDATE `+t.records+` SET owner_id = '', version = version + 1, updated_at = $1
		 WHERE id = $2 AND owner_id = $3`,
		formatTime(time.Now()), resourceID, taskID)
	if err != nil {
		return fmt.Errorf("release lock on %s %s: %w", t.noun, resourceID, err)
	}
	return nil
}

// incrementCounter adds delta to the counter under (scope, name), creating it
// at 0, and returns the new value in the same statement.
func incrementCounter(ctx context.Context, db *sql.DB, t leaseTables, scope, name string, delta int64) (int64, error) {
	var value int64
	err := db.QueryRowContext(ctx,
		`INSERT INTO `+t.counters+` (scope, name, value) VALUES ($1, $2, $3)
		 ON CONFLICT (scope, name) DO UPDATE SET value = `+t.counters+`.value + EXCLUDED.value
		 RETURNING value`,
		scope, name, delta).Scan(&value)
	if err != nil {
		return 0, fmt.Errorf("increment %s counter %s/%s: %w", t.noun, scope, name, err)
	}
	return value, nil
}

// deleteWithHolds removes the record and its hold rows in one transaction, so
// a deleted resource cannot leave holds that block a later re-add.
func deleteWithHolds(ctx context.Context, db *sql.DB, t leaseTables, id string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete %s %s: %w", t.noun, id, err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+t.records+` WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete %s %s: %w", t.noun, id, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+t.holds+` WHERE resource_id = $1`, id); err != nil {
		return fmt.Errorf("delete %s %s: %w", t.noun, id, err)
	}
	return tx.Commit()
}

// groupsSchema renders the shared group table shape for a store's baseline
// migration.
func groupsSchema(table string) string {
	return `CREATE TABLE ` + table + ` (
		id         TEXT PRIMARY KEY,
		strategy   TEXT NOT NULL DEFAULT '',
		refs       TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL DEFAULT '',
		updated_at TEXT NOT NULL DEFAULT ''
	)`
}

// listGroups, saveGroup, and dropGroup are the group half every
// leasing-shaped store shares: stable-order listing, upsert preserving
// created_at, no-op deletes. The three models alias leasing.Group, so one
// concrete implementation serves them all.
func listGroups(ctx context.Context, db *sql.DB, t leaseTables, table string) ([]leasing.Group, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT id, strategy, refs, created_at, updated_at FROM `+table+` ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list %s groups: %w", t.noun, err)
	}
	defer rows.Close()

	listed := make([]leasing.Group, 0)
	for rows.Next() {
		var g leasing.Group
		var refs, created, updated string
		if err := rows.Scan(&g.ID, &g.Strategy, &refs, &created, &updated); err != nil {
			return nil, fmt.Errorf("list %s groups: %w", t.noun, err)
		}
		if g.Refs, err = parseMap[string](refs); err != nil {
			return nil, fmt.Errorf("list %s groups: decode refs of %s: %w", t.noun, g.ID, err)
		}
		if g.CreatedAt, err = parseTime(created); err != nil {
			return nil, fmt.Errorf("list %s groups: %w", t.noun, err)
		}
		if g.UpdatedAt, err = parseTime(updated); err != nil {
			return nil, fmt.Errorf("list %s groups: %w", t.noun, err)
		}
		listed = append(listed, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list %s groups: %w", t.noun, err)
	}
	return listed, nil
}

func saveGroup(ctx context.Context, db *sql.DB, t leaseTables, table string, g leasing.Group) error {
	refs, err := formatMap(g.Refs)
	if err != nil {
		return fmt.Errorf("save %s group %s: encode refs: %w", t.noun, g.ID, err)
	}
	_, err = db.ExecContext(ctx,
		`INSERT INTO `+table+` (id, strategy, refs, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO UPDATE SET strategy = EXCLUDED.strategy,
		 refs = EXCLUDED.refs, updated_at = EXCLUDED.updated_at`,
		g.ID, g.Strategy, refs, formatTime(g.CreatedAt), formatTime(g.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save %s group %s: %w", t.noun, g.ID, err)
	}
	return nil
}

func dropGroup(ctx context.Context, db *sql.DB, t leaseTables, table, id string) error {
	if _, err := db.ExecContext(ctx, `DELETE FROM `+table+` WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete %s group %s: %w", t.noun, id, err)
	}
	return nil
}
