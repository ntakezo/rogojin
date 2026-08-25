// Package tasksqlite provides a file-backed, durable implementation of the
// tasks.Repository port. A consumer that does not want to write its own byte
// store can inject SQLite.
package tasksqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ntakezo/rogojin/persistence/sqlitemigrate"
	"github.com/ntakezo/rogojin/tasks"
)

// ErrNotFound is returned when no record exists for the id, so the service can
// tell a missing task apart from a store failure.
var ErrNotFound = errors.New("task not found")

// SQLite is a durable tasks.Repository backed by a single SQLite database file.
type SQLite struct {
	db *sql.DB
}

// NewSQLite opens (creating if absent) the database at dsn and ensures the schema exists.
func NewSQLite(dsn string) (*SQLite, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// SQLite serializes writes per file; one connection avoids "database is locked" under concurrent checkpoints.
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

// migrations is the ordered schema history of the tasks store. Append new steps
// to the end; never edit or reorder shipped ones, since PRAGMA user_version pins
// how many have already run on existing databases.
//
// The proxy_group_id and proxy_id columns are dead once the assignments backfill
// runs, but stay: dropping a column already shipped in someone's database buys
// nothing worth the risk.
var migrations = []sqlitemigrate.Migration{
	{
		Name: "create tasks table",
		SQL: `CREATE TABLE IF NOT EXISTS tasks (
			id          TEXT PRIMARY KEY,
			workflow_id TEXT NOT NULL,
			state       TEXT NOT NULL DEFAULT '',
			status      TEXT NOT NULL DEFAULT '',
			snapshot    BLOB
		)`,
	},
	{
		Name: "add output column for task results",
		SQL:  `ALTER TABLE tasks ADD COLUMN output BLOB`,
	},
	{
		Name: "add group_id column placing existing tasks in the global group",
		SQL:  `ALTER TABLE tasks ADD COLUMN group_id TEXT NOT NULL DEFAULT 'global'`,
	},
	{
		Name: "add nullable proxy_group_id column for per-task assignments",
		SQL:  `ALTER TABLE tasks ADD COLUMN proxy_group_id TEXT`,
	},
	{
		Name: "add created_at column to tasks",
		SQL:  `ALTER TABLE tasks ADD COLUMN created_at TEXT NOT NULL DEFAULT ''`,
	},
	{
		Name: "add updated_at column to tasks",
		SQL:  `ALTER TABLE tasks ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''`,
	},
	{
		Name: "add nullable proxy_id column pinning a task to one proxy",
		SQL:  `ALTER TABLE tasks ADD COLUMN proxy_id TEXT`,
	},
	{
		Name: "create task_groups table",
		SQL: `CREATE TABLE IF NOT EXISTS task_groups (
			id             TEXT PRIMARY KEY,
			proxy_group_id TEXT NOT NULL DEFAULT '',
			created_at     TEXT NOT NULL DEFAULT '',
			updated_at     TEXT NOT NULL DEFAULT ''
		)`,
	},
	{
		Name: "add assignments column generalizing placement to any resource kind",
		SQL:  `ALTER TABLE tasks ADD COLUMN assignments TEXT NOT NULL DEFAULT ''`,
	},
	{
		// SQL NULL survives as JSON null and reads back as a nil *string, which is
		// what keeps the tri-state: inherit, explicitly none, or explicitly named.
		Name: "backfill task assignments from the proxy columns",
		SQL: `UPDATE tasks
			SET assignments = json_object('proxy',
				json_object('groupId', proxy_group_id, 'resourceId', proxy_id))
			WHERE proxy_group_id IS NOT NULL OR proxy_id IS NOT NULL`,
	},
	{
		Name: "add resource_groups column generalizing task group assignment",
		SQL:  `ALTER TABLE task_groups ADD COLUMN resource_groups TEXT NOT NULL DEFAULT ''`,
	},
	{
		Name: "backfill task group resource groups from the proxy column",
		SQL: `UPDATE task_groups
			SET resource_groups = json_object('proxy', proxy_group_id)
			WHERE proxy_group_id != ''`,
	},
}

// Close closes the underlying database.
func (s *SQLite) Close() error {
	return s.db.Close()
}

// CreateTask inserts a fresh task row from its record: workflow, placement,
// and timestamps, with no checkpoint yet.
func (s *SQLite) CreateTask(ctx context.Context, rec tasks.Record) error {
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

// SaveCheckpoint overwrites the task's last-checkpointed status, state, and
// snapshot, refreshing updated_at. It fails with ErrNotFound if no record
// exists: a checkpoint that lands nowhere must not report durability the
// store does not have.
func (s *SQLite) SaveCheckpoint(ctx context.Context, id, status, state string, snapshot []byte) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET status = ?, state = ?, snapshot = ?, updated_at = ? WHERE id = ?`,
		status, state, snapshot, formatTime(time.Now().UTC()), id)
	if err != nil {
		return fmt.Errorf("save checkpoint %s: %w", id, err)
	}
	return errRowMissing("save checkpoint", id, res)
}

// SaveAssignment repoints a task's placement for one kind, leaving every other
// kind and the rest of the record untouched. A nil field stores JSON null: no
// group assignment of its own, or no pin. json_set edits the stored object in
// place, so one kind is rewritten without reading the others back first.
func (s *SQLite) SaveAssignment(ctx context.Context, id string, kind string, a tasks.Assignment) error {
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

// MarkTerminal stamps the terminal outcome and the run's output, refreshing
// updated_at and leaving state and snapshot intact as a valid resume entry.
// output is nil for runs that produce no result or did not complete cleanly.
// It fails with ErrNotFound if no record exists.
func (s *SQLite) MarkTerminal(ctx context.Context, id, outcome string, output []byte) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET status = ?, output = ?, updated_at = ? WHERE id = ?`,
		outcome, output, formatTime(time.Now().UTC()), id)
	if err != nil {
		return fmt.Errorf("mark terminal %s: %w", id, err)
	}
	return errRowMissing("mark terminal", id, res)
}

// errRowMissing turns an update that matched no row into ErrNotFound, so a
// write to a deleted task fails loud instead of silently landing nowhere.
func errRowMissing(op, id string, res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s %s: %w", op, id, err)
	}
	if n == 0 {
		return fmt.Errorf("%s %s: %w", op, id, ErrNotFound)
	}
	return nil
}

// RecoverTask returns the record for id, or ErrNotFound if none exists.
func (s *SQLite) RecoverTask(ctx context.Context, id string) (tasks.Record, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, workflow_id, group_id, assignments, state, status, snapshot, output, created_at, updated_at
		 FROM tasks WHERE id = ?`, id)
	rec, err := scanRecord(row)
	if errors.Is(err, sql.ErrNoRows) {
		return tasks.Record{}, fmt.Errorf("recover task %s: %w", id, ErrNotFound)
	}
	if err != nil {
		return tasks.Record{}, fmt.Errorf("recover task %s: %w", id, err)
	}
	return rec, nil
}

// RecoverAll returns every persisted record, terminal ones included.
func (s *SQLite) RecoverAll(ctx context.Context) ([]tasks.Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, workflow_id, group_id, assignments, state, status, snapshot, output, created_at, updated_at
		 FROM tasks`)
	if err != nil {
		return nil, fmt.Errorf("recover all: %w", err)
	}
	defer rows.Close()

	records := make([]tasks.Record, 0)
	for rows.Next() {
		rec, err := scanRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("recover all: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("recover all: %w", err)
	}
	return records, nil
}

// DeleteTask removes the task's record; absent rows are a no-op.
func (s *SQLite) DeleteTask(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM tasks WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete task %s: %w", id, err)
	}
	return nil
}

// SaveGroup upserts the task group's record. created_at is written on insert
// and never overwritten: when a group was created is not something a later
// save gets to revise.
func (s *SQLite) SaveGroup(ctx context.Context, g tasks.Group) error {
	resourceGroups, err := encodeResourceGroups(g.ResourceGroups)
	if err != nil {
		return fmt.Errorf("save task group %s: %w", g.ID, err)
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
func (s *SQLite) GetGroup(ctx context.Context, id string) (tasks.Group, bool, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, resource_groups, created_at, updated_at FROM task_groups WHERE id = ?`, id)
	g, err := scanGroup(row)
	if errors.Is(err, sql.ErrNoRows) {
		return tasks.Group{}, false, nil
	}
	if err != nil {
		return tasks.Group{}, false, fmt.Errorf("get task group %s: %w", id, err)
	}
	return g, true, nil
}

// ListGroups returns every stored task group in stable id order.
func (s *SQLite) ListGroups(ctx context.Context) ([]tasks.Group, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, resource_groups, created_at, updated_at FROM task_groups ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list task groups: %w", err)
	}
	defer rows.Close()

	listed := make([]tasks.Group, 0)
	for rows.Next() {
		g, err := scanGroup(rows)
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
// tasks are the service's to delete — the store cascades nothing.
func (s *SQLite) DeleteGroup(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM task_groups WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete task group %s: %w", id, err)
	}
	return nil
}

// TasksInGroup returns the ids of every task in the group, in stable id order.
func (s *SQLite) TasksInGroup(ctx context.Context, groupID string) ([]string, error) {
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

// TasksPinnedTo returns every task record pinned to resourceID for the kind. It
// reads whole records because the caller decides which of them could still run,
// a rule this store does not own.
//
// The NULLIF is load-bearing: json_extract rejects the empty string as
// malformed, and a row that predates the assignments column and had no proxy
// placement to backfill still carries the column default.
func (s *SQLite) TasksPinnedTo(ctx context.Context, kind, resourceID string) ([]tasks.Record, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, workflow_id, group_id, assignments, state, status, snapshot, output, created_at, updated_at
		 FROM tasks
		 WHERE json_extract(NULLIF(assignments, ''), '$.' || ? || '.resourceId') = ?
		 ORDER BY id`, kind, resourceID)
	if err != nil {
		return nil, fmt.Errorf("list tasks pinned to %s %s: %w", kind, resourceID, err)
	}
	defer rows.Close()

	records := make([]tasks.Record, 0)
	for rows.Next() {
		record, err := scanRecord(rows)
		if err != nil {
			return nil, fmt.Errorf("list tasks pinned to %s %s: %w", kind, resourceID, err)
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

// scanner is the read surface shared by sql.Row and sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// scanRecord reads one row into a tasks.Record.
func scanRecord(row scanner) (tasks.Record, error) {
	var rec tasks.Record
	var assignments, created, updated string
	if err := row.Scan(&rec.ID, &rec.WorkflowID, &rec.GroupID, &assignments, &rec.State, &rec.Status, &rec.Snapshot, &rec.Output, &created, &updated); err != nil {
		return tasks.Record{}, err
	}
	var err error
	if rec.Assignments, err = decodeAssignments(assignments); err != nil {
		return tasks.Record{}, err
	}
	if rec.CreatedAt, err = parseTime(created); err != nil {
		return tasks.Record{}, err
	}
	if rec.UpdatedAt, err = parseTime(updated); err != nil {
		return tasks.Record{}, err
	}
	return rec, nil
}

// scanGroup reads one row into a tasks.Group.
func scanGroup(row scanner) (tasks.Group, error) {
	var g tasks.Group
	var resourceGroups, created, updated string
	if err := row.Scan(&g.ID, &resourceGroups, &created, &updated); err != nil {
		return tasks.Group{}, err
	}
	var err error
	if g.ResourceGroups, err = decodeResourceGroups(resourceGroups); err != nil {
		return tasks.Group{}, err
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
func encodeAssignments(a map[string]tasks.Assignment) (string, error) {
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
func decodeAssignments(raw string) (map[string]tasks.Assignment, error) {
	if raw == "" {
		return nil, nil
	}
	var a map[string]tasks.Assignment
	if err := json.Unmarshal([]byte(raw), &a); err != nil {
		return nil, fmt.Errorf("decode assignments: %w", err)
	}
	return a, nil
}

// encodeResourceGroups renders a task group's per-kind assignment for its
// column, empty storing as the column default.
func encodeResourceGroups(g map[string]string) (string, error) {
	if len(g) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(g)
	if err != nil {
		return "", fmt.Errorf("encode resource groups: %w", err)
	}
	return string(encoded), nil
}

// decodeResourceGroups is the inverse of encodeResourceGroups.
func decodeResourceGroups(raw string) (map[string]string, error) {
	if raw == "" {
		return nil, nil
	}
	var g map[string]string
	if err := json.Unmarshal([]byte(raw), &g); err != nil {
		return nil, fmt.Errorf("decode resource groups: %w", err)
	}
	return g, nil
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
