package sqlitemigrate

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// openDB opens a fresh temp-file database with the single-connection setting the
// real repositories use, so tests exercise migrations the way production does.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "m.db")
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

func version(t *testing.T, db *sql.DB) int {
	t.Helper()
	v, err := userVersion(db)
	if err != nil {
		t.Fatalf("userVersion: %v", err)
	}
	return v
}

// twoSteps is a representative history: create a table, then add a column — the
// same shape as the tasks store's real output migration.
var twoSteps = []Migration{
	{Name: "create t", SQL: `CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)`},
	{Name: "add col", SQL: `ALTER TABLE t ADD COLUMN extra TEXT`},
}

// TestRunAppliesAllOnFreshDatabase verifies a fresh database runs every migration
// in order and ends stamped at the latest version, so a new install lands on the
// current schema in a single open.
func TestRunAppliesAllOnFreshDatabase(t *testing.T) {
	db := openDB(t)
	if err := Run(db, twoSteps); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := version(t, db); got != 2 {
		t.Fatalf("user_version = %d, want 2", got)
	}
	// The column the second migration added must exist and be writable.
	if _, err := db.Exec(`INSERT INTO t (id, extra) VALUES ('a', 'b')`); err != nil {
		t.Fatalf("insert into migrated table: %v", err)
	}
}

// TestRunIsIdempotent verifies re-running the same migrations is a no-op: the
// second Run must not re-apply the ALTER (which would fail as a duplicate
// column), proving applied steps are skipped rather than repeated — the property
// that makes it safe to call on every open.
func TestRunIsIdempotent(t *testing.T) {
	db := openDB(t)
	if err := Run(db, twoSteps); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := Run(db, twoSteps); err != nil {
		t.Fatalf("second Run must be a no-op, got: %v", err)
	}
	if got := version(t, db); got != 2 {
		t.Fatalf("user_version = %d, want 2", got)
	}
}

// TestRunResumesFromPartialVersion verifies only migrations past the recorded
// version run, so an upgrade applies exactly the new steps and never re-touches
// already-applied ones.
func TestRunResumesFromPartialVersion(t *testing.T) {
	db := openDB(t)
	// Simulate a database already at version 1 (first migration applied).
	if _, err := db.Exec(twoSteps[0].SQL); err != nil {
		t.Fatalf("seed table: %v", err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 1`); err != nil {
		t.Fatalf("seed version: %v", err)
	}

	if err := Run(db, twoSteps); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := version(t, db); got != 2 {
		t.Fatalf("user_version = %d, want 2", got)
	}
}

// TestRunRollsBackFailedMigration verifies a failing migration leaves the version
// unchanged and its effects rolled back, so a botched step can be fixed and
// retried from a clean point instead of leaving a half-applied schema.
func TestRunRollsBackFailedMigration(t *testing.T) {
	db := openDB(t)
	steps := []Migration{
		{Name: "good", SQL: `CREATE TABLE good (id TEXT)`},
		{Name: "bad", SQL: `CREATE TABLE good (id TEXT)`}, // re-creating the table fails
	}
	if err := Run(db, steps); err == nil {
		t.Fatal("Run: want error from duplicate table, got nil")
	}
	// The first migration committed at version 1; the failed second did not advance it.
	if got := version(t, db); got != 1 {
		t.Fatalf("user_version = %d, want 1 (failed step must not advance the version)", got)
	}
}

// TestRunRejectsNewerDatabase verifies a database stamped beyond the known
// migrations is refused rather than silently operated on, because older code must
// not run against a schema from a future version (Rule 12 — fail loud).
func TestRunRejectsNewerDatabase(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(`PRAGMA user_version = 5`); err != nil {
		t.Fatalf("seed version: %v", err)
	}
	if err := Run(db, twoSteps); err == nil {
		t.Fatal("Run: want error for a newer database, got nil")
	}
}
