package postgres

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

// Tasks is the tasks.Repository: one row per task carrying its placement,
// claim, last checkpoint, and terminal outcome; one per task group; one per
// recorded effect.
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

// taskMigrations is the ordered schema history of the tasks store — a fresh
// baseline, since this adapter was born on the current contract.
// lease_expires_at is TIMESTAMPTZ, NULL when unclaimed, and every claim
// predicate compares it against the server's now(): the store clock decides
// liveness, so the clocks of contending nodes never do.
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
			snapshot         BYTEA,
			output           BYTEA,
			version          BIGINT NOT NULL DEFAULT 0,
			owner_node       TEXT NOT NULL DEFAULT '',
			lease_expires_at TIMESTAMPTZ,
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
			result     BYTEA,
			created_at TEXT NOT NULL,
			PRIMARY KEY (task_id, key)
		)`,
	},
}

// CreateTask inserts a fresh task row from its record: workflow, placement,
// and timestamps, with no checkpoint and no claim yet.
func (s *Tasks) CreateTask(ctx context.Context, rec tasks.Task) error {
	assignments, err := encodeAssignments(rec.Assignments)
	if err != nil {
		return fmt.Errorf("create task %s: %w", rec.ID, err)
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO tasks (id, workflow_id, group_id, assignments, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		rec.ID, rec.WorkflowID, rec.GroupID, assignments, formatTime(rec.CreatedAt), formatTime(rec.UpdatedAt))
	if err != nil {
		return fmt.Errorf("create task %s: %w", rec.ID, err)
	}
	return nil
}

// ClaimTask atomically takes ownership for node iff the task is unclaimed,
// already node's, or leased past expiry, returning the claimed record with
// its new version; ErrClaimHeld reports a live claim by another node. The
// conditional UPDATE and the read-back are one statement via RETURNING, so
// contending nodes resolve in the server.
func (s *Tasks) ClaimTask(ctx context.Context, id, node string, ttl time.Duration) (tasks.Task, error) {
	row := s.db.QueryRowContext(ctx,
		`UPDATE tasks SET owner_node = $1, lease_expires_at = `+fmt.Sprintf(ttlExpr, 2)+`,
		 version = version + 1, updated_at = $3
		 WHERE id = $4 AND (owner_node = '' OR owner_node = $1 OR lease_expires_at < now())
		 RETURNING `+taskColumns,
		node, ttl.Milliseconds(), formatTime(time.Now()), id)
	rec, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		if _, err := s.RecoverTask(ctx, id); err != nil {
			return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, tasks.ErrTaskNotFound)
		}
		return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, tasks.ErrClaimHeld)
	}
	if err != nil {
		return tasks.Task{}, fmt.Errorf("claim task %s: %w", id, err)
	}
	return rec, nil
}

// RenewClaim extends the lease iff node still owns the claim, expired or not,
// without bumping the version — renewal moves only the lease clock, so it
// never invalidates the owner's own in-flight conditional writes.
func (s *Tasks) RenewClaim(ctx context.Context, id, node string, ttl time.Duration) error {
	if node == "" {
		return fmt.Errorf("renew claim %s: %w", id, tasks.ErrStale)
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET lease_expires_at = `+fmt.Sprintf(ttlExpr, 1)+`, updated_at = $2
		 WHERE id = $3 AND owner_node = $4`,
		ttl.Milliseconds(), formatTime(time.Now()), id, node)
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
		`UPDATE tasks SET owner_node = '', lease_expires_at = NULL, version = version + 1, updated_at = $1
		 WHERE id = $2 AND owner_node = $3`,
		formatTime(time.Now()), id, node)
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
// returning the version; ErrStale reports the write lost.
func (s *Tasks) SaveCheckpoint(ctx context.Context, id string, version int64, node, status, state string, snapshot []byte) (int64, error) {
	return s.conditionalWrite(ctx, "save checkpoint", id, version, node,
		`status = $1, state = $2, snapshot = $3`, status, state, snapshot)
}

// MarkTerminal stamps the terminal outcome and the run's output under
// SaveCheckpoint's conditionality, additionally clearing the claim — a
// finished task is nobody's to run.
func (s *Tasks) MarkTerminal(ctx context.Context, id string, version int64, node, outcome string, output []byte) (int64, error) {
	return s.conditionalWrite(ctx, "mark terminal", id, version, node,
		`status = $1, output = $2, owner_node = '', lease_expires_at = NULL`, outcome, output)
}

// conditionalWrite runs one guarded UPDATE — version and ownership in the
// predicate — bumping the version and returning it via RETURNING. The sets
// clause numbers its placeholders from $1; the guard args follow. A write
// that matched no row is ErrStale if the record exists and ErrTaskNotFound
// if it does not.
func (s *Tasks) conditionalWrite(ctx context.Context, op, id string, version int64, node, sets string, args ...any) (int64, error) {
	n := len(args)
	query := fmt.Sprintf(`UPDATE tasks SET %s, version = version + 1, updated_at = $%d
	 WHERE id = $%d AND version = $%d AND owner_node = $%d RETURNING version`, sets, n+1, n+2, n+3, n+4)
	args = append(args, formatTime(time.Now()), id, version, node)

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

// SaveAssignment repoints a task's placement for one kind, leaving every
// other kind and the rest of the record untouched. The jsonb merge rewrites
// one key in place; the kind is validated here even though the manager
// already refuses bad ones, since it becomes a JSON object key.
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
		 SET assignments = (COALESCE(NULLIF(assignments, ''), '{}')::jsonb || jsonb_build_object($1::text, $2::jsonb))::text,
		     updated_at = $3
		 WHERE id = $4`,
		string(kind), string(encoded), formatTime(time.Now()), id)
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

// taskColumns is the SELECT list scanTask reads, shared so every read path
// stays in step with the schema.
const taskColumns = `id, workflow_id, group_id, assignments, state, status, snapshot, output, created_at, updated_at, version, owner_node, lease_expires_at`

// RecoverTask returns the record for id, or ErrTaskNotFound if none exists.
func (s *Tasks) RecoverTask(ctx context.Context, id string) (tasks.Task, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+taskColumns+` FROM tasks WHERE id = $1`, id)
	rec, err := scanTask(row)
	if errors.Is(err, sql.ErrNoRows) {
		return tasks.Task{}, fmt.Errorf("recover task %s: %w", id, tasks.ErrTaskNotFound)
	}
	if err != nil {
		return tasks.Task{}, fmt.Errorf("recover task %s: %w", id, err)
	}
	return rec, nil
}

// RecoverAll returns every persisted record, terminal ones included, in
// stable id order.
func (s *Tasks) RecoverAll(ctx context.Context) ([]tasks.Task, error) {
	return s.listTasks(ctx, "recover all", `SELECT `+taskColumns+` FROM tasks ORDER BY id`)
}

// ListClaimable returns the non-terminal tasks whose claim is free for any
// taker — unclaimed or leased past expiry, by the server's clock.
func (s *Tasks) ListClaimable(ctx context.Context) ([]tasks.Task, error) {
	return s.listTasks(ctx, "list claimable",
		`SELECT `+taskColumns+` FROM tasks
		 WHERE status != $1 AND status != $2 AND (owner_node = '' OR lease_expires_at < now())
		 ORDER BY id`,
		string(workflows.StatusDone), string(workflows.StatusKilled))
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
// are a no-op. One transaction: task ids are never reused, and the pair
// leaving together keeps a future reader from meeting orphaned effects.
func (s *Tasks) DeleteTask(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("delete task %s: %w", id, err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM task_effects WHERE task_id = $1`, id); err != nil {
		return fmt.Errorf("delete task %s: %w", id, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM tasks WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete task %s: %w", id, err)
	}
	return tx.Commit()
}

// RecordEffect stores result under (taskID, key) if no record exists, and
// returns the stored result either way; first reports whether this call
// created it. ON CONFLICT DO NOTHING waits out a racing insert, so the
// read-back after a conflict always sees the one recorded result.
func (s *Tasks) RecordEffect(ctx context.Context, taskID, key string, result []byte) ([]byte, bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO task_effects (task_id, key, result, created_at) VALUES ($1, $2, $3, $4)
		 ON CONFLICT (task_id, key) DO NOTHING`,
		taskID, key, result, formatTime(time.Now()))
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
		`SELECT result FROM task_effects WHERE task_id = $1 AND key = $2`, taskID, key).Scan(&stored); err != nil {
		return nil, false, fmt.Errorf("record effect %s/%s: %w", taskID, key, err)
	}
	return stored, false, nil
}

// ListEffects returns every effect recorded for the task, keyed by effect key.
func (s *Tasks) ListEffects(ctx context.Context, taskID string) (map[string][]byte, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT key, result FROM task_effects WHERE task_id = $1`, taskID)
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
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (id) DO UPDATE SET resource_groups = EXCLUDED.resource_groups,
		 updated_at = EXCLUDED.updated_at`,
		g.ID, resourceGroups, formatTime(g.CreatedAt), formatTime(g.UpdatedAt))
	if err != nil {
		return fmt.Errorf("save task group %s: %w", g.ID, err)
	}
	return nil
}

// GetGroup returns the group and whether a record exists for the id.
func (s *Tasks) GetGroup(ctx context.Context, id string) (tasks.Group, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, resource_groups, created_at, updated_at FROM task_groups WHERE id = $1`, id)
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
	_, err := s.db.ExecContext(ctx, `DELETE FROM task_groups WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete task group %s: %w", id, err)
	}
	return nil
}

// TasksInGroup returns the ids of every task in the group, in stable id order.
func (s *Tasks) TasksInGroup(ctx context.Context, groupID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM tasks WHERE group_id = $1 ORDER BY id`, groupID)
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
	var lease sql.NullTime
	if err := row.Scan(&rec.ID, &rec.WorkflowID, &rec.GroupID, &assignments, &rec.State, &rec.Status, &rec.Snapshot, &rec.Output, &created, &updated, &rec.Version, &rec.OwnerNode, &lease); err != nil {
		return tasks.Task{}, err
	}
	rec.LeaseExpiresAt = nullExpiry(lease)
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
// alike whether it was written here or seeded by another adapter.
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
