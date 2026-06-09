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

// ensureSchema creates the tasks table if it does not already exist.
func ensureSchema(db *sql.DB) error {
	const schema = `CREATE TABLE IF NOT EXISTS tasks (
		id          TEXT PRIMARY KEY,
		workflow_id TEXT NOT NULL,
		state       TEXT NOT NULL DEFAULT '',
		status      TEXT NOT NULL DEFAULT '',
		snapshot    BLOB
	)`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
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

// MarkTerminal stamps the terminal outcome, leaving state and snapshot intact as a valid resume entry.
func (s *SQLite) MarkTerminal(ctx context.Context, id, outcome string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE tasks SET status = ? WHERE id = ?`, outcome, id)
	if err != nil {
		return fmt.Errorf("mark terminal %s: %w", id, err)
	}
	return nil
}

// RecoverTask returns the record for id, or ErrNotFound if none exists.
func (s *SQLite) RecoverTask(ctx context.Context, id string) (tasks.Record, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, workflow_id, state, status, snapshot FROM tasks WHERE id = ?`, id)
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
		`SELECT id, workflow_id, state, status, snapshot FROM tasks`)
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
	if err := row.Scan(&rec.ID, &rec.WorkflowID, &rec.State, &rec.Status, &rec.Snapshot); err != nil {
		return tasks.Record{}, err
	}
	return rec, nil
}
