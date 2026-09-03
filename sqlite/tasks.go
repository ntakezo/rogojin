package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/tasks"
	"github.com/ntakezo/rogojin/workflows"
)

// ErrTaskNotFound is returned when no record exists for the id. It is the
// port-wide tasks.ErrTaskNotFound, kept under its old name here.
var ErrTaskNotFound = tasks.ErrTaskNotFound

// Tasks is the tasks.Repository: one row per task carrying its placement,
// last checkpoint, and terminal outcome, and one per task group.
type Tasks struct {
	db *sql.DB
}

// NewTasks builds the tasks store on db, bringing its tables up to the
// current schema.
func NewTasks(db *DB) (tasks.Repository, error) {
	if err := migrate(db.db, "tasks", taskMigrations); err != nil {
		return nil, err
	}
	return &Tasks{db: db.db}, nil
}

// taskMigrations is the ordered schema history of the tasks store. Append new
// steps to the end; never edit or reorder shipped ones: the ledger records
// which of them have already run on existing databases by position.
var taskMigrations = []migration{
	{
		Name: "create tasks table",
		SQL: `CREATE TABLE tasks (
			id               TEXT PRIMARY KEY,
			workflow_id      TEXT NOT NULL,
			group_id         TEXT NOT NULL DEFAULT 'global',
			assignments      TEXT NOT NULL DEFAULT '',
			state            TEXT NOT NULL DEFAULT '',
			status           TEXT NOT NULL DEFAULT '',
			snapshot         BLOB,
			output           BLOB,
			version          INTEGER NOT NULL DEFAULT 0,
			owner_node       TEXT NOT NULL DEFAULT '',
			lease_expires_at INTEGER NOT NULL DEFAULT 0,
			created_at       TEXT NOT NULL DEFAULT '',
			updated_at       TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "create task_groups table",
		SQL: `CREATE TABLE task_groups (
			id              TEXT PRIMARY KEY,
			resource_groups TEXT NOT NULL DEFAULT '',
			created_at      TEXT NOT NULL DEFAULT '',
			updated_at      TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "create task_effects table",
		SQL: `CREATE TABLE task_effects (
			task_id    TEXT NOT NULL,
			key        TEXT NOT NULL,
			result     BLOB,
			created_at TEXT NOT NULL,
			PRIMARY KEY (task_id, key)
		)`,
	},
}

// The lease expiry is stored as unix milliseconds, not RFC3339Nano text like
// the other timestamps: it is a comparison column — every claim predicate
// orders it against "now" in SQL — and variable-width fractional-second text
// does not compare correctly. 0 is the zero time (unclaimed). The claim
// predicates compare against the adapter's own clock: the store is the
// authority on liveness, and for sqlite the process clock is the store clock,
// so node clock skew never decides ownership.

// formatMillis renders a timestamp for the lease column; the zero time
// stores as 0.
func formatMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

// parseMillis is the inverse of formatMillis.
func parseMillis(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// CreateTask inserts a fresh task row from its record: workflow, placement,
// and timestamps, with no checkpoint yet.
func (s *Tasks) CreateTask(ctx context.Context, rec tasks.Task) error {
	assignments, err := encodeAssignments(rec.Assignments)
	if err != nil {
		return fmt.Errorf("create task %s: %w", rec.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tasks (id, workflow_id, group_id, assignments, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		rec.ID, rec.WorkflowID, rec.GroupID, assignments, formatTime(rec.CreatedAt), formatTime(rec.UpdatedAt))
	if err != nil {
		return fmt.Errorf("create task %s: %w", rec.ID, err)
	}
	return nil
}

// ClaimTask atomically takes ownership for node iff the task is unclaimed,
// already node's, or leased past expiry, returning the claimed record with
// its new version; ErrClaimHeld reports a live claim by another node. One
// conditional UPDATE is the whole race: whichever node's statement runs
// first wins, and the loser's matches no row.
func (s *Tasks) ClaimTask(ctx context.Context, id, node string, ttl time.Duration) (tasks.Task, error) {
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET owner_node = ?, lease_expires_at = ?, version = version + 1, updated_at = ?
		 WHERE id = ? AND (owner_node = '' OR owner_node = ? OR lease_expires_at < ?)`,
		node, formatMillis(now.Add(ttl)), formatTime(now), id, node, formatMillis(now))
	if err != nil {
		return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, err)
	}
	if n == 0 {
		if _, err := s.RecoverTask(ctx, id); err != nil {
			return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, tasks.ErrTaskNotFound)
		}
		return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, tasks.ErrClaimHeld)
	}
	rec, err := s.RecoverTask(ctx, id)
	if err != nil {
		return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, err)
	}
	return rec, nil
}

// RenewClaim extends the lease iff node still owns the claim, expired or
// not, without bumping the version — renewal moves only the lease clock, so
// it never invalidates the owner's own in-flight conditional writes.
// ErrStale reports the claim gone or another node's.
func (s *Tasks) RenewClaim(ctx context.Context, id, node string, ttl time.Duration) error {
	if node == "" {
		return fmt.Errorf("renew claim %s: %w", id, tasks.ErrStale)
	}
	now := time.Now().UTC()
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET lease_expires_at = ?, updated_at = ? WHERE id = ? AND owner_node = ?`,
		formatMillis(now.Add(ttl)), formatTime(now), id, node)
	if err != nil {
		return fmt.Errorf("renew claim %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("renew claim %s: %w", id, err)
	}
	if n == 0 {
		if _, err := s.RecoverTask(ctx, id); err != nil {
			return fmt.Errorf("renew claim %s: %w", id, tasks.ErrTaskNotFound)
		}
		return fmt.Errorf("renew claim %s: %w", id, tasks.ErrStale)
	}
	return nil
}

// ReleaseClaim clears the claim iff node owns it, silently a no-op
// otherwise: a release racing its own usurpation is a shutdown path, not an
// error.
func (s *Tasks) ReleaseClaim(ctx context.Context, id, node string) error {
	if node == "" {
		return nil
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET owner_node = '', lease_expires_at = 0, version = version + 1, updated_at = ?
		 WHERE id = ? AND owner_node = ?`,
		formatTime(time.Now().UTC()), id, node)
	if err != nil {
		return fmt.Errorf("release claim %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("release claim %s: %w", id, err)
	}
	if n == 0 {
		if _, err := s.RecoverTask(ctx, id); err != nil {
			return fmt.Errorf("release claim %s: %w", id, tasks.ErrTaskNotFound)
		}
	}
	return nil
}

// SaveCheckpoint overwrites the task's last-checkpointed status, state, and
// snapshot iff version matches and node owns the claim, bumping and
// returning the version; ErrStale reports the write lost. It fails with
// ErrTaskNotFound if no record exists: a checkpoint that lands nowhere must
// not report durability the store does not have.
func (s *Tasks) SaveCheckpoint(ctx context.Context, id string, version int64, node, status, state string, snapshot []byte) (int64, error) {
	return s.conditionalWrite(ctx, "save checkpoint", id, version, node,
		`status = ?, state = ?, snapshot = ?`, status, state, snapshot)
}

// SaveAssignment repoints a task's placement for one kind, leaving every other
// kind and the rest of the record untouched. A nil field stores JSON null: no
// group assignment of its own, or no pin. json_set edits the stored object in
// place, so one kind is rewritten without reading the others back first — which
// is why the kind is validated here even though the manager already refuses bad
// ones: it becomes a JSON path, and a '.' or '[' in it would misfile the
// placement instead of storing it.
func (s *Tasks) SaveAssignment(ctx context.Context, id string, kind leasing.Kind, a tasks.Assignment) error {
	if err := kind.Validate(); err != nil {
		return fmt.Errorf("assign placement of task %s: %w", id, err)
	}
	encoded, err := json.Marshal(a)
	if err != nil {
		return fmt.Errorf("assign %s placement of task %s: %w", kind, id, err)
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks
		 SET assignments = json_set(COALESCE(NULLIF(assignments, ''), '{}'), '$.' || ?, json(?)),
		     updated_at = ?
		 WHERE id = ?`,
		kind, string(encoded), formatTime(time.Now().UTC()), id)
	if err != nil {
		return fmt.Errorf("assign %s placement of task %s: %w", kind, id, err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("assign %s placement of task %s: %w", kind, id, err)
	}
	if affected == 0 {
		return fmt.Errorf("assign %s placement: task %s does not exist", kind, id)
	}
	return nil
}

// MarkTerminal stamps the terminal outcome and the run's output under
// SaveCheckpoint's conditionality, additionally clearing the claim — a
// finished task is nobody's to run. State and snapshot stay intact as a
// valid resume entry. output is nil for runs that produce no result or did
// not complete cleanly. It fails with ErrTaskNotFound if no record exists.
func (s *Tasks) MarkTerminal(ctx context.Context, id string, version int64, node, outcome string, output []byte) (int64, error) {
	return s.conditionalWrite(ctx, "mark terminal", id, version, node,
		`status = ?, output = ?, owner_node = '', lease_expires_at = 0`, outcome, output)
}

// conditionalWrite runs one guarded UPDATE — version and ownership in the
// predicate — bumping the version and returning it via RETURNING, so the
// write and the version read are one statement even with another process on
// the file. A write that matched no row is ErrStale if the record exists and
// ErrTaskNotFound if it does not.
func (s *Tasks) conditionalWrite(ctx context.Context, op, id string, version int64, node, sets string, args ...any) (int64, error) {
	query := `UPDATE tasks SET ` + sets + `, version = version + 1, updated_at = ?
	 WHERE id = ? AND version = ? AND owner_node = ? RETURNING version`
	args = append(args, formatTime(time.Now().UTC()), id, version, node)

	var v int64
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := s.RecoverTask(ctx, id); err != nil {
			return 0, fmt.Errorf("%s %s: %w", op, id, tasks.ErrTaskNotFound)
		}
		return 0, fmt.Errorf("%s %s: %w", op, id, tasks.ErrStale)
	}
	if err != nil {
		return 0, fmt.Errorf("%s %s: %w", op, id, err)
	}
	return v, nil
}

// taskColumns is the SELECT list scanTask reads, shared so every read path
// stays in step with the schema.
const taskColumns = `id, workflow_id, group_id, assignments, state, status, snapshot, output, created_at, updated_at, version, owner_node, lease_expires_at`

// RecoverTask returns the record for id, or ErrTaskNotFound if none exists.
func (s *Tasks) RecoverTask(ctx context.Context, id string) (tasks.Task, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE id = ?`, id)
	rec, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return tasks.Task{}, fmt.Errorf("recover task %s: %w", id, ErrTaskNotFound)
	}
	if err != nil {
		return tasks.Task{}, fmt.Errorf("recover task %s: %w", id, err)
	}
	return rec, nil
}

// RecoverAll returns every persisted record, terminal ones included.
func (s *Tasks) RecoverAll(ctx context.Context) ([]tasks.Task, error) {
	return s.listTasks(ctx, "recover all", `SELECT `+taskColumns+` FROM tasks`)
}

// ListClaimable returns the non-terminal tasks whose claim is free for any
// taker — unclaimed or leased past expiry, by the store's clock.
func (s *Tasks) ListClaimable(ctx context.Context) ([]tasks.Task, error) {
	return s.listTasks(ctx, "list claimable",
		`SELECT `+taskColumns+` FROM tasks WHERE status != ? AND status != ? AND (owner_node = '' OR lease_expires_at < ?)`,
		string(workflows.StatusDone), string(workflows.StatusKilled), formatMillis(time.Now().UTC()))
}

// listTasks runs one listing query and scans every row.
func (s *Tasks) listTasks(ctx context.Context, op, query string, args ...any) ([]tasks.Task, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer rows.Close()

	records := make([]tasks.Task, 0)
	for rows.Next() {
		rec, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return records, nil
}

// DeleteTask removes the task's record and its recorded effects; absent rows
// are a no-op. Effects go first: task ids are never reused, so a crash
// between the deletes leaves either an intact task or nothing referencing
// the gone one, never orphaned effects a future task could read.
func (s *Tasks) DeleteTask(ctx context.Context, id string) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM task_effects WHERE task_id = ?`, id); err != nil {
		return fmt.Errorf("delete task %s: %w", id, err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM tasks WHERE id = ?`, id); err != nil {
		return fmt.Errorf("delete task %s: %w", id, err)
	}
	return nil
}

// RecordEffect stores result under (taskID, key) if no record exists, and
// returns the stored result either way; first reports whether this call
// created it. The insert-or-ignore and the read-back are two statements, but
// the primary key makes the pair correct under any interleaving: whoever
// inserted, the read returns the one recorded result.
func (s *Tasks) RecordEffect(ctx context.Context, taskID, key string, result []byte) ([]byte, bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO task_effects (task_id, key, result, created_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(task_id, key) DO NOTHING`,
		taskID, key, result, formatTime(time.Now().UTC()))
	if err != nil {
		return nil, false, fmt.Errorf("record effect %s/%s: %w", taskID, key, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, false, fmt.Errorf("record effect %s/%s: %w", taskID, key, err)
	}
	if n == 1 {
		return result, true, nil
	}
	var stored []byte
	if err := s.db.QueryRowContext(ctx,
		`SELECT result FROM task_effects WHERE task_id = ? AND key = ?`, taskID, key).Scan(&stored); err != nil {
		return nil, false, fmt.Errorf("record effect %s/%s: %w", taskID, key, err)
	}
	return stored, false, nil
}

// ListEffects returns every effect recorded for the task, keyed by effect key.
func (s *Tasks) ListEffects(ctx context.Context, taskID string) (map[string][]byte, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT key, result FROM task_effects WHERE task_id = ?`, taskID)
	if err != nil {
		return nil, fmt.Errorf("list effects %s: %w", taskID, err)
	}
	defer rows.Close()

	effects := make(map[string][]byte)
	for rows.Next() {
		var key string
		var result []byte
		if err := rows.Scan(&key, &result); err != nil {
			return nil, fmt.Errorf("list effects %s: %w", taskID, err)
		}
		effects[key] = result
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list effects %s: %w", taskID, err)
	}
	return effects, nil
}

// SaveGroup upserts the task group's record. created_at is written on insert
// and never overwritten: when a group was created is not something a later
// save gets to revise.
func (s *Tasks) SaveGroup(ctx context.Context, g tasks.Group) error {
	resourceGroups, err := formatMap(g.ResourceGroups)
	if err != nil {
		return fmt.Errorf("save task group %s: encode resource groups: %w", g.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO task_groups (id, resource_groups, created_at, updated_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET resource_groups = excluded.resource_groups,
		 updated_at = excluded.updated_at`,
		g.ID, resourceGroups, formatTime(g.CreatedAt), formatTime(g.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save task group %s: %w", g.ID, err)
	}
	return nil
}

// GetGroup returns the group and whether a record exists for the id.
func (s *Tasks) GetGroup(ctx context.Context, id string) (tasks.Group, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, resource_groups, created_at, updated_at FROM task_groups WHERE id = ?`, id)
	g, err := scanTaskGroup(row)
	if errors.Is(err, sql.ErrNoRows) {
		return tasks.Group{}, false, nil
	}
	if err != nil {
		return tasks.Group{}, false, fmt.Errorf("get task group %s: %w", id, err)
	}
	return g, true, nil
}

// ListGroups returns every stored task group in stable id order.
func (s *Tasks) ListGroups(ctx context.Context) ([]tasks.Group, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, resource_groups, created_at, updated_at FROM task_groups ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list task groups: %w", err)
	}
	defer rows.Close()

	listed := make([]tasks.Group, 0)
	for rows.Next() {
		g, err := scanTaskGroup(rows)
		if err != nil {
			return nil, fmt.Errorf("list task groups: %w", err)
		}
		listed = append(listed, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list task groups: %w", err)
	}
	return listed, nil
}

// DeleteGroup removes the group's record; absent rows are a no-op. Member
// tasks are the task manager's to delete — the store cascades nothing.
func (s *Tasks) DeleteGroup(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM task_groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete task group %s: %w", id, err)
	}
	return nil
}

// TasksInGroup returns the ids of every task in the group, in stable id order.
func (s *Tasks) TasksInGroup(ctx context.Context, groupID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM tasks WHERE group_id = ? ORDER BY id`, groupID)
	if err != nil {
		return nil, fmt.Errorf("tasks in group %s: %w", groupID, err)
	}
	defer rows.Close()

	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("tasks in group %s: %w", groupID, err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("tasks in group %s: %w", groupID, err)
	}
	return ids, nil
}

// scanTask reads one row (in taskColumns order) into a tasks.Task.
func scanTask(row scanner) (tasks.Task, error) {
	var rec tasks.Task
	var assignments, created, updated string
	var leaseMillis int64
	if err := row.Scan(&rec.ID, &rec.WorkflowID, &rec.GroupID, &assignments, &rec.State, &rec.Status, &rec.Snapshot, &rec.Output, &created, &updated, &rec.Version, &rec.OwnerNode, &leaseMillis); err != nil {
		return tasks.Task{}, err
	}
	rec.LeaseExpiresAt = parseMillis(leaseMillis)
	var err error
	if rec.Assignments, err = decodeAssignments(assignments); err != nil {
		return tasks.Task{}, err
	}
	if rec.CreatedAt, err = parseTime(created); err != nil {
		return tasks.Task{}, err
	}
	if rec.UpdatedAt, err = parseTime(updated); err != nil {
		return tasks.Task{}, err
	}
	return rec, nil
}

// scanTaskGroup reads one row into a tasks.Group.
func scanTaskGroup(row scanner) (tasks.Group, error) {
	var g tasks.Group
	var resourceGroups, created, updated string
	if err := row.Scan(&g.ID, &resourceGroups, &created, &updated); err != nil {
		return tasks.Group{}, err
	}
	var err error
	if g.ResourceGroups, err = parseMap[leasing.Kind](resourceGroups); err != nil {
		return tasks.Group{}, fmt.Errorf("decode resource groups: %w", err)
	}
	if g.CreatedAt, err = parseTime(created); err != nil {
		return tasks.Group{}, err
	}
	if g.UpdatedAt, err = parseTime(updated); err != nil {
		return tasks.Group{}, err
	}
	return g, nil
}

// encodeAssignments renders a record's per-kind placement for its column. An
// empty map stores as the column default, so a task placed nowhere reads back
// alike whether it was written here or predates the column.
func encodeAssignments(a map[leasing.Kind]tasks.Assignment) (string, error) {
	if len(a) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(a)
	if err != nil {
		return "", fmt.Errorf("encode assignments: %w", err)
	}
	return string(encoded), nil
}

// decodeAssignments is the inverse of encodeAssignments. A JSON null field
// decodes to a nil *string, which is what carries "inherit" back out of the
// store intact.
func decodeAssignments(raw string) (map[leasing.Kind]tasks.Assignment, error) {
	if raw == "" {
		return nil, nil
	}
	var a map[leasing.Kind]tasks.Assignment
	if err := json.Unmarshal([]byte(raw), &a); err != nil {
		return nil, fmt.Errorf("decode assignments: %w", err)
	}
	return a, nil
}
