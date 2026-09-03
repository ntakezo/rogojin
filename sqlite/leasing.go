package sqlite

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
// migration list. expires_at is INTEGER unix milliseconds, not the text
// timestamps records use: it exists to be compared, and RFC3339Nano's
// variable-width fractions do not compare correctly as text. Expiry is always
// measured against this adapter's own clock — the store decides, so the
// clocks of contending nodes never do.
func holdsSchema(table string) string {
	return `CREATE TABLE ` + table + ` (
		resource_id TEXT NOT NULL,
		task_id     TEXT NOT NULL,
		count       INTEGER NOT NULL,
		expires_at  INTEGER NOT NULL,
		PRIMARY KEY (resource_id, task_id)
	)`
}

func countersSchema(table string) string {
	return `CREATE TABLE ` + table + ` (
		scope TEXT NOT NULL,
		name  TEXT NOT NULL,
		value INTEGER NOT NULL,
		PRIMARY KEY (scope, name)
	)`
}

// unixMillis and millisTime are the expires_at column encoding.
func unixMillis(t time.Time) int64 { return t.UnixMilli() }
func millisTime(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}

// versionGate reads the stored version inside the caller's transaction and
// decides the conditional write: create (version 0, no row), replace (version
// N matching), or refuse with leasing.ErrStale. It returns whether to insert
// and the version the write must land at.
func versionGate(ctx context.Context, tx *sql.Tx, t leaseTables, id string, version int64) (insert bool, next int64, err error) {
	var stored int64
	err = tx.QueryRowContext(ctx, `SELECT version FROM `+t.records+` WHERE id = ?`, id).Scan(&stored)
	missing := errors.Is(err, sql.ErrNoRows)
	if err != nil && !missing {
		return false, 0, fmt.Errorf("read %s %s version: %w", t.noun, id, err)
	}
	switch {
	case version == 0 && missing:
		return true, 1, nil
	case version == 0, missing, stored != version:
		return false, 0, fmt.Errorf("save %s %s: %w", t.noun, id, leasing.ErrStale)
	default:
		return false, version + 1, nil
	}
}

// acquireHold takes or re-enters a hold inside one immediate transaction:
// expired rows of the resource are pruned on this write path — a superseded
// hold must not be revivable by its owner's next heartbeat — then the cap is
// measured over what remains. A resource locked by another task refuses with
// ErrLockHeld — a manager's cache may not know about the lock yet, so the
// store is what says no; a resource with no record reads as unlocked, since
// the store does not police existence.
func acquireHold(ctx context.Context, db *sql.DB, t leaseTables, resourceID, taskID string, cap int, ttl time.Duration) (leasing.Hold, error) {
	fail := func(err error) (leasing.Hold, error) {
		return leasing.Hold{}, fmt.Errorf("acquire %s %s for task %s: %w", t.noun, resourceID, taskID, err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fail(err)
	}
	defer tx.Rollback()

	var owner string
	err = tx.QueryRowContext(ctx, `SELECT owner_id FROM `+t.records+` WHERE id = ?`, resourceID).Scan(&owner)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fail(err)
	}
	if owner != "" && owner != taskID {
		return fail(leasing.ErrLockHeld)
	}

	now := time.Now()
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM `+t.holds+` WHERE resource_id = ? AND expires_at <= ?`,
		resourceID, unixMillis(now)); err != nil {
		return fail(err)
	}

	var count int
	err = tx.QueryRowContext(ctx,
		`SELECT count FROM `+t.holds+` WHERE resource_id = ? AND task_id = ?`,
		resourceID, taskID).Scan(&count)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fail(err)
	}
	if count == 0 {
		var others int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM `+t.holds+` WHERE resource_id = ? AND task_id <> ?`,
			resourceID, taskID).Scan(&others); err != nil {
			return fail(err)
		}
		if cap > 0 && others >= cap {
			return fail(leasing.ErrCapacity)
		}
	}

	hold := leasing.Hold{ResourceID: resourceID, TaskID: taskID, Count: count + 1, ExpiresAt: now.Add(ttl)}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO `+t.holds+` (resource_id, task_id, count, expires_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(resource_id, task_id) DO UPDATE SET count = excluded.count, expires_at = excluded.expires_at`,
		resourceID, taskID, hold.Count, unixMillis(hold.ExpiresAt)); err != nil {
		return fail(err)
	}
	if err := tx.Commit(); err != nil {
		return fail(err)
	}
	return hold, nil
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
		`UPDATE `+t.holds+` SET count = count - 1 WHERE resource_id = ? AND task_id = ?`,
		resourceID, taskID); err != nil {
		return fmt.Errorf("release %s hold %s: %w", t.noun, resourceID, err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM `+t.holds+` WHERE resource_id = ? AND task_id = ? AND count <= 0`,
		resourceID, taskID); err != nil {
		return fmt.Errorf("release %s hold %s: %w", t.noun, resourceID, err)
	}
	return tx.Commit()
}

// renewHolds extends every unexpired hold the task has; expired rows stay
// expired, since their capacity may already be promised elsewhere.
func renewHolds(ctx context.Context, db *sql.DB, t leaseTables, taskID string, ttl time.Duration) error {
	now := time.Now()
	_, err := db.ExecContext(ctx,
		`UPDATE `+t.holds+` SET expires_at = ? WHERE task_id = ? AND expires_at > ?`,
		unixMillis(now.Add(ttl)), taskID, unixMillis(now))
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
		var expires int64
		if err := rows.Scan(&h.ResourceID, &h.TaskID, &h.Count, &expires); err != nil {
			return nil, fmt.Errorf("list %s holds: %w", t.noun, err)
		}
		h.ExpiresAt = millisTime(expires)
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
		`UPDATE `+t.records+` SET owner_id = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND owner_id IN ('', ?)`,
		taskID, formatTime(time.Now().UTC()), resourceID, taskID)
	if err != nil {
		return fmt.Errorf("claim lock on %s %s: %w", t.noun, resourceID, err)
	}
	if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("claim lock on %s %s: %w", t.noun, resourceID, err)
	} else if n > 0 {
		return nil
	}
	var owner string
	err = db.QueryRowContext(ctx, `SELECT owner_id FROM `+t.records+` WHERE id = ?`, resourceID).Scan(&owner)
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
		`UPDATE `+t.records+` SET owner_id = '', version = version + 1, updated_at = ?
		 WHERE id = ? AND owner_id = ?`,
		formatTime(time.Now().UTC()), resourceID, taskID)
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
		`INSERT INTO `+t.counters+` (scope, name, value) VALUES (?, ?, ?)
		 ON CONFLICT(scope, name) DO UPDATE SET value = value + excluded.value
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
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+t.records+` WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete %s %s: %w", t.noun, id, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+t.holds+` WHERE resource_id = ?`, id); err != nil {
		return fmt.Errorf("delete %s %s: %w", t.noun, id, err)
	}
	return tx.Commit()
}
