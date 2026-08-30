// Package sqlite is the SQLite persistence layer: one file-backed
// implementation of every repository port the framework defines — accounts,
// cards, proxies, email, and tasks — built on one open database.
//
// Open opens the database once, on a single connection, and each store is
// constructed on it: NewAccounts, NewCards, NewProxies, NewEmails, NewTasks.
// The stores share the file and the connection but nothing else. Each records
// its schema migrations under its own name in a shared ledger, so any subset
// of them can live in one file, each advancing its own history without
// seeing the others' — see migrate.go for why that ledger exists.
//
// What every store has in common is here: the connection, the timestamp and
// JSON column encodings, and the migration runner. What each one stores, and
// how, is in its own file. Everything is stored in the clear: credentials,
// card data, and inbox passwords are only as protected as the file. A store
// that seals a model's fields on the way down and opens them on the way up
// is transparent to everything above, and is what a database holding real
// secrets belongs behind.
package sqlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// A DB is one open SQLite database, shared by every store built on it.
type DB struct {
	db *sql.DB
}

// Open opens (creating if absent) the database at dsn. It holds a single
// connection: SQLite serializes writes per file, and one connection is what
// keeps concurrent saves, checkpoints, and lock writes from meeting
// "database is locked" instead of queueing.
func Open(dsn string) (*DB, error) {
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	return &DB{db: db}, nil
}

// Close closes the database. Every store built on it is closed with it.
func (d *DB) Close() error {
	return d.db.Close()
}

// scanner is the read surface shared by sql.Row and sql.Rows, so one scan
// function serves a lookup and a listing alike.
type scanner interface {
	Scan(dest ...any) error
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

// formatFields stores an opaque JSON payload as text, rejecting one that is
// not valid JSON: a column that round-trips garbage would surface it as a
// decode failure inside a run, far from the write that caused it. Absent
// fields store as "".
func formatFields(fields json.RawMessage) (string, error) {
	if len(fields) == 0 {
		return "", nil
	}
	if !json.Valid(fields) {
		return "", fmt.Errorf("fields are not valid JSON")
	}
	return string(fields), nil
}

// parseFields is the inverse of formatFields.
func parseFields(fields string) json.RawMessage {
	if fields == "" {
		return nil
	}
	return json.RawMessage(fields)
}

// formatMap stores a string-keyed map — a group's refs, a task group's
// per-kind resource groups — as JSON text; an empty map stores as "" so rows
// written before the column existed keep round-tripping.
func formatMap[K ~string](m map[K]string) (string, error) {
	if len(m) == 0 {
		return "", nil
	}
	encoded, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

// parseMap is the inverse of formatMap. The key type cannot be inferred from
// the raw text, so call sites name it.
func parseMap[K ~string](raw string) (map[K]string, error) {
	if raw == "" {
		return nil, nil
	}
	decoded := make(map[K]string)
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}
