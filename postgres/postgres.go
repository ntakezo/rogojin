// Package postgres is the PostgreSQL persistence layer: one server-backed
// implementation of every repository port the framework defines — accounts,
// payments, proxies, email, and tasks — built on one open database. It is the
// adapter for running several nodes over one store: every claim, hold, lock,
// and counter the ports define lands as a conditional statement the server
// serializes, so contending processes meet in the database, not in a mutex.
//
// Open opens the database once, and each store is constructed on it:
// NewAccounts, NewPayments, NewProxies, NewEmails, NewTasks. The stores share
// the connection pool and the migration ledger but nothing else, exactly as
// the sqlite package arranges its file.
//
// Time is the server's. Every lease and claim expiry is a TIMESTAMPTZ
// compared against the server's now(), so the store clock decides liveness
// and the clocks of contending nodes never do — the literal reading of the
// ports' "expiry compares against the store's clock". Record timestamps
// (created_at, updated_at) stay RFC3339Nano text like sqlite's: they are
// display fields, never compared in SQL, and text keeps their nanosecond
// round-trip exact where TIMESTAMPTZ would truncate to microseconds.
//
// Everything is stored in the clear: credentials, card data, and inbox
// passwords are only as protected as the database. A store that seals a
// model's fields on the way down and opens them on the way up is transparent
// to everything above, and is what a database holding real secrets belongs
// behind.
package postgres

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// A DB is one open PostgreSQL database, shared by every store built on it.
type DB struct {
	db *sql.DB
}

// Open opens a pool on the postgres:// (or key=value) dsn. Connections are
// capped low: every statement here is short, and a runaway pool would let one
// process starve the server the whole fleet shares.
func Open(dsn string) (*DB, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(16)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return &DB{db: db}, nil
}

// Close closes the pool. Every store built on it is closed with it.
func (d *DB) Close() error {
	return d.db.Close()
}

// scanner is the read surface shared by sql.Row and sql.Rows, so one scan
// function serves a lookup and a listing alike.
type scanner interface {
	Scan(dest ...any) error
}

// formatTime stores timestamps as RFC3339Nano UTC text; the zero time stores
// as "" so absent timestamps keep round-tripping.
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

// nullExpiry reads an expiry column, where NULL is the zero time — unclaimed.
func nullExpiry(t sql.NullTime) time.Time {
	if !t.Valid {
		return time.Time{}
	}
	return t.Time.UTC()
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

// formatMap stores a string-keyed map as JSON text; an empty map stores as ""
// so the round trip matches the other adapters'.
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
