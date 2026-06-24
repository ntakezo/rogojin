// Package tasksqlite provides a file-backed, durable implementation of the
// tasks.Repository port. A consumer that does not want to write its own byte
// store can inject SQLite.
package tasksqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
	"github.com/ntakezo/rogojin/persistence/internal/sqlitemigrate"
	"github.com/ntakezo/rogojin/tasks"
)

// ErrNotFound is returned by RecoverTask when no record exists for the id, so the
// service can tell a missing task apart from a store failure.
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
}

// Close closes the underlying database.
func (s *SQLite) Close() error {
	return s.db.Close()
}

// CreateTask inserts a fresh task row with its workflow and no checkpoint yet.
func (s *SQLite) CreateTask(ctx context.Context, id, workflowID string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO tasks (id, workflow_id) VALUES (?, ?)`, id, workflowID)
	if err != nil {
		return fmt.Errorf("create task %s: %w", id, err)
	}
	return nil
}

// SaveCheckpoint overwrites the task's last-checkpointed status, state, and snapshot.
func (s *SQLite) SaveCheckpoint(ctx context.Context, id, status, state string, snapshot []byte) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE tasks SET status = ?, state = ?, snapshot = ? WHERE id = ?`,
		status, state, snapshot, id)
	if err != nil {
		return fmt.Errorf("save checkpoint %s: %w", id, err)
	}
	return nil
}

// MarkTerminal stamps the terminal outcome and the run's output, leaving state
// and snapshot intact as a valid resume entry. output is nil for runs that
// produce no result or did not complete cleanly.
func (s *SQLite) MarkTerminal(ctx context.Context, id, outcome string, output []byte) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tasks SET status = ?, output = ? WHERE id = ?`, outcome, output, id)
	if err != nil {
		return fmt.Errorf("mark terminal %s: %w", id, err)
	}
	return nil
}

// RecoverTask returns the record for id, or ErrNotFound if none exists.
func (s *SQLite) RecoverTask(ctx context.Context, id string) (tasks.Record, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, workflow_id, state, status, snapshot, output FROM tasks WHERE id = ?`, id)
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
		`SELECT id, workflow_id, state, status, snapshot, output FROM tasks`)
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

// scanner is the read surface shared by sql.Row and sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// scanRecord reads one row into a tasks.Record.
func scanRecord(row scanner) (tasks.Record, error) {
	var rec tasks.Record
	if err := row.Scan(&rec.ID, &rec.WorkflowID, &rec.State, &rec.Status, &rec.Snapshot, &rec.Output); err != nil {
		return tasks.Record{}, err
	}
	return rec, nil
}
