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
			id          TEXT PRIMARY KEY,
			workflow_id TEXT NOT NULL,
			group_id    TEXT NOT NULL DEFAULT 'global',
			assignments TEXT NOT NULL DEFAULT '',
			state       TEXT NOT NULL DEFAULT '',
			status      TEXT NOT NULL DEFAULT '',
			snapshot    BLOB,
			output      BLOB,
			created_at  TEXT NOT NULL DEFAULT '',
			updated_at  TEXT NOT NULL DEFAULT ''
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

// SaveCheckpoint overwrites the task's last-checkpointed status, state, and
// snapshot, refreshing updated_at. It fails with ErrTaskNotFound if no record
// exists: a checkpoint that lands nowhere must not report durability the
// store does not have.
func (s *Tasks) SaveCheckpoint(ctx context.Context, id, status, state string, snapshot []byte) error {
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

// MarkTerminal stamps the terminal outcome and the run's output, refreshing
// updated_at and leaving state and snapshot intact as a valid resume entry.
// output is nil for runs that produce no result or did not complete cleanly.
// It fails with ErrTaskNotFound if no record exists.
func (s *Tasks) MarkTerminal(ctx context.Context, id, outcome string, output []byte) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET status = ?, output = ?, updated_at = ? WHERE id = ?`,
		outcome, output, formatTime(time.Now().UTC()), id)
	if err != nil {
		return fmt.Errorf("mark terminal %s: %w", id, err)
	}
	return errRowMissing("mark terminal", id, res)
}

// errRowMissing turns an update that matched no row into ErrTaskNotFound, so
// a write to a deleted task fails loud instead of silently landing nowhere.
func errRowMissing(op, id string, res sql.Result) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s %s: %w", op, id, err)
	}
	if n == 0 {
		return fmt.Errorf("%s %s: %w", op, id, ErrTaskNotFound)
	}
	return nil
}

// RecoverTask returns the record for id, or ErrTaskNotFound if none exists.
func (s *Tasks) RecoverTask(ctx context.Context, id string) (tasks.Task, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, workflow_id, group_id, assignments, state, status, snapshot, output, created_at, updated_at
		 FROM tasks WHERE id = ?`, id)
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
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, workflow_id, group_id, assignments, state, status, snapshot, output, created_at, updated_at
		 FROM tasks`)
	if err != nil {
		return nil, fmt.Errorf("recover all: %w", err)
	}
	defer rows.Close()

	records := make([]tasks.Task, 0)
	for rows.Next() {
		rec, err := scanTask(rows)
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
func (s *Tasks) DeleteTask(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM tasks WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("delete task %s: %w", id, err)
	}
	return nil
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

// scanTask reads one row into a tasks.Task.
func scanTask(row scanner) (tasks.Task, error) {
	var rec tasks.Task
	var assignments, created, updated string
	if err := row.Scan(&rec.ID, &rec.WorkflowID, &rec.GroupID, &assignments, &rec.State, &rec.Status, &rec.Snapshot, &rec.Output, &created, &updated); err != nil {
		return tasks.Task{}, err
	}
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
