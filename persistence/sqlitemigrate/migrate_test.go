package sqlitemigrate

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// openDB opens a fresh temp-file database with the single-connection setting the
// real repositories use, so tests exercise migrations the way production does.
func openDB(t *testing.T) *sql.DB {
	t.Helper()
	return openAt(t, filepath.Join(t.TempDir(), "m.db"))
}

// openAt opens a named database, so a test can point two stores at one file or
// close and reopen the same one.
func openAt(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// recorded reports how many migrations the ledger holds for the store.
func recorded(t *testing.T, db *sql.DB, store string) int {
	t.Helper()
	n, err := applied(db, store)
	if err != nil {
		t.Fatalf("applied(%s): %v", store, err)
	}
	return n
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

// otherSteps is a second store's unrelated history, for the shared-file tests.
var otherSteps = []Migration{
	{Name: "create u", SQL: `CREATE TABLE IF NOT EXISTS u (id TEXT PRIMARY KEY)`},
}

// TestRunAppliesAllOnFreshDatabase verifies a fresh database runs every migration
// in order and ends recorded at the latest version, so a new install lands on the
// current schema in a single open.
func TestRunAppliesAllOnFreshDatabase(t *testing.T) {
	db := openDB(t)
	if err := Run(db, "main", twoSteps); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := recorded(t, db, "main"); got != 2 {
		t.Fatalf("recorded = %d, want 2", got)
	}
	// The column the second migration added must exist and be writable.
	if _, err := db.Exec(`INSERT INTO t (id, extra) VALUES ('a', 'b')`); err != nil {
		t.Fatalf("insert into migrated table: %v", err)
	}
}

// TestLedgerRecordsNames verifies the ledger reads as a history rather than a
// counter: each row carries the step's name and when it ran, which is what makes
// a database's schema state legible without diffing tables.
func TestLedgerRecordsNames(t *testing.T) {
	db := openDB(t)
	if err := Run(db, "main", twoSteps); err != nil {
		t.Fatalf("Run: %v", err)
	}

	rows, err := db.Query(`SELECT version, name, applied_at FROM schema_migrations WHERE store = 'main' ORDER BY version`)
	if err != nil {
		t.Fatalf("query ledger: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var version int
		var name, at string
		if err := rows.Scan(&version, &name, &at); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if at == "" {
			t.Fatalf("migration %d recorded with no applied_at", version)
		}
		names = append(names, name)
	}
	if len(names) != 2 || names[0] != "create t" || names[1] != "add col" {
		t.Fatalf("ledger names = %v, want the two step names in order", names)
	}
}

// TestRunIsIdempotent verifies re-running the same migrations is a no-op: the
// second Run must not re-apply the ALTER (which would fail as a duplicate
// column), proving applied steps are skipped rather than repeated — the property
// that makes it safe to call on every open.
func TestRunIsIdempotent(t *testing.T) {
	db := openDB(t)
	if err := Run(db, "main", twoSteps); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := Run(db, "main", twoSteps); err != nil {
		t.Fatalf("second Run must be a no-op, got: %v", err)
	}
	if got := recorded(t, db, "main"); got != 2 {
		t.Fatalf("recorded = %d, want 2", got)
	}
}

// TestRunResumesFromPartialHistory verifies only migrations past the recorded
// ones run, so an upgrade applies exactly the new steps and never re-touches
// already-applied ones.
func TestRunResumesFromPartialHistory(t *testing.T) {
	db := openDB(t)
	if err := Run(db, "main", twoSteps[:1]); err != nil {
		t.Fatalf("Run of the first step: %v", err)
	}
	if got := recorded(t, db, "main"); got != 1 {
		t.Fatalf("recorded = %d, want 1", got)
	}

	// The upgrade ships a second step; only that one may run, since re-running
	// the ALTER would fail as a duplicate column.
	if err := Run(db, "main", twoSteps); err != nil {
		t.Fatalf("Run after the upgrade: %v", err)
	}
	if got := recorded(t, db, "main"); got != 2 {
		t.Fatalf("recorded = %d, want 2", got)
	}
}

// TestRunRollsBackFailedMigration verifies a failing migration leaves the ledger
// unchanged and its effects rolled back, so a botched step can be fixed and
// retried from a clean point instead of leaving a half-applied schema.
func TestRunRollsBackFailedMigration(t *testing.T) {
	db := openDB(t)
	steps := []Migration{
		{Name: "good", SQL: `CREATE TABLE good (id TEXT)`},
		{Name: "bad", SQL: `CREATE TABLE good (id TEXT)`}, // re-creating the table fails
	}
	if err := Run(db, "main", steps); err == nil {
		t.Fatal("Run: want error from duplicate table, got nil")
	}
	// The first migration committed at version 1; the failed second did not record.
	if got := recorded(t, db, "main"); got != 1 {
		t.Fatalf("recorded = %d, want 1 (a failed step must not record)", got)
	}
}

// TestRunRejectsNewerDatabase verifies a store recorded beyond the migrations it
// knows is refused rather than silently operated on, because older code must not
// run against a schema from a future version.
func TestRunRejectsNewerDatabase(t *testing.T) {
	db := openDB(t)
	if err := Run(db, "main", twoSteps); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Older code, shipping only the first of the two steps it later grew.
	err := Run(db, "main", twoSteps[:1])
	if err == nil {
		t.Fatal("Run: want error for a newer database, got nil")
	}
	if !strings.Contains(err.Error(), "newer than") {
		t.Fatalf("err = %v, want it to name the version mismatch", err)
	}
}

// TestRunRejectsGappedHistory verifies a ledger missing a step in the middle is
// refused. Resuming from a plain count would treat the hole as applied and skip
// it forever, so the gap has to stop the open instead.
func TestRunRejectsGappedHistory(t *testing.T) {
	db := openDB(t)
	if err := Run(db, "main", twoSteps); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE store = 'main' AND version = 1`); err != nil {
		t.Fatalf("punch a hole: %v", err)
	}

	err := Run(db, "main", twoSteps)
	if err == nil {
		t.Fatal("Run: want error for a gapped history, got nil")
	}
	if !strings.Contains(err.Error(), "gap") {
		t.Fatalf("err = %v, want it to name the gap", err)
	}
}

// TestStoresShareOneFile verifies the whole point of the ledger: two stores
// pointed at one database file each run their own migrations and neither reads
// the other's progress as its own. Under the old per-file counter the second
// store found it already advanced and created no tables at all.
func TestStoresShareOneFile(t *testing.T) {
	db := openDB(t)
	if err := Run(db, "first", twoSteps); err != nil {
		t.Fatalf("first store: %v", err)
	}
	if err := Run(db, "second", otherSteps); err != nil {
		t.Fatalf("second store: %v", err)
	}

	if got := recorded(t, db, "first"); got != 2 {
		t.Fatalf("first store recorded = %d, want 2", got)
	}
	if got := recorded(t, db, "second"); got != 1 {
		t.Fatalf("second store recorded = %d, want 1", got)
	}
	// Both stores' tables exist and are writable in the one file.
	if _, err := db.Exec(`INSERT INTO t (id, extra) VALUES ('a', 'b')`); err != nil {
		t.Fatalf("first store's table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO u (id) VALUES ('a')`); err != nil {
		t.Fatalf("second store's table: %v", err)
	}
}

// TestSharedFileUpgradesIndependently verifies a shared file lets one store grow
// its schema without disturbing the other's recorded history.
func TestSharedFileUpgradesIndependently(t *testing.T) {
	db := openDB(t)
	if err := Run(db, "first", twoSteps[:1]); err != nil {
		t.Fatalf("first store: %v", err)
	}
	if err := Run(db, "second", otherSteps); err != nil {
		t.Fatalf("second store: %v", err)
	}
	if err := Run(db, "first", twoSteps); err != nil {
		t.Fatalf("first store upgrade: %v", err)
	}

	if got := recorded(t, db, "first"); got != 2 {
		t.Fatalf("first store recorded = %d, want 2", got)
	}
	if got := recorded(t, db, "second"); got != 1 {
		t.Fatalf("second store recorded = %d, want 1 (untouched by the other's upgrade)", got)
	}
}

// TestAdoptsPreLedgerDatabase verifies an existing database — one written when
// progress lived in PRAGMA user_version — is carried onto the ledger rather than
// re-migrated. Re-running its ALTER would fail as a duplicate column, so a clean
// open is the proof the counter was honored.
func TestAdoptsPreLedgerDatabase(t *testing.T) {
	db := openDB(t)
	// Hand-build the pre-ledger state: both steps applied, counter at 2, no ledger.
	for _, m := range twoSteps {
		if _, err := db.Exec(m.SQL); err != nil {
			t.Fatalf("seed %s: %v", m.Name, err)
		}
	}
	if _, err := db.Exec(`PRAGMA user_version = 2`); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	if err := Run(db, "main", twoSteps); err != nil {
		t.Fatalf("Run on a pre-ledger database: %v", err)
	}
	if got := recorded(t, db, "main"); got != 2 {
		t.Fatalf("recorded = %d, want the counter's 2 carried over", got)
	}
	// The counter is cleared, so a second store joining this file later starts
	// from nothing rather than inheriting a count that was never about it.
	if got := version(t, db); got != 0 {
		t.Fatalf("user_version = %d, want it cleared after adoption", got)
	}
	if err := Run(db, "second", otherSteps); err != nil {
		t.Fatalf("second store joining an adopted file: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO u (id) VALUES ('a')`); err != nil {
		t.Fatalf("second store's table after adoption: %v", err)
	}
}

// TestAdoptsPartiallyMigratedDatabase verifies adoption carries a counter that
// stops short of the current schema, and that the remaining steps then run.
func TestAdoptsPartiallyMigratedDatabase(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(twoSteps[0].SQL); err != nil {
		t.Fatalf("seed table: %v", err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 1`); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	if err := Run(db, "main", twoSteps); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := recorded(t, db, "main"); got != 2 {
		t.Fatalf("recorded = %d, want 2", got)
	}
	if _, err := db.Exec(`INSERT INTO t (id, extra) VALUES ('a', 'b')`); err != nil {
		t.Fatalf("the pending step did not run: %v", err)
	}
}

// TestAdoptionRefusesAForeignCounter verifies a pre-ledger file whose counter
// exceeds the opening store's whole history is refused — the counter came from
// some other store — and that the refusal writes nothing. Leaving the counter
// intact is the point: the store that actually owns the file must still be able
// to adopt it afterwards.
func TestAdoptionRefusesAForeignCounter(t *testing.T) {
	db := openDB(t)
	if _, err := db.Exec(`PRAGMA user_version = 8`); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	if err := Run(db, "main", twoSteps); err == nil {
		t.Fatal("Run: want a foreign counter to be refused, got nil")
	}
	if got := version(t, db); got != 8 {
		t.Fatalf("user_version = %d, want the refused counter left at 8", got)
	}
	if got := recorded(t, db, "main"); got != 0 {
		t.Fatalf("recorded = %d, want nothing written by a refused adoption", got)
	}

	// The store that owns the file — eight migrations deep — still adopts it.
	owner := make([]Migration, 8)
	for i := range owner {
		owner[i] = Migration{Name: "step", SQL: `SELECT 1`}
	}
	if err := Run(db, "owner", owner); err != nil {
		t.Fatalf("the owning store must still adopt: %v", err)
	}
	if got := recorded(t, db, "owner"); got != 8 {
		t.Fatalf("owner recorded = %d, want 8", got)
	}
}

// TestRunRequiresAStoreName verifies the key every row is filed under cannot be
// empty, since an unnamed store would collide with the next one.
func TestRunRequiresAStoreName(t *testing.T) {
	db := openDB(t)
	if err := Run(db, "", twoSteps); err == nil {
		t.Fatal("Run: want an empty store name to be refused, got nil")
	}
}

// TestLedgerSurvivesReopen verifies the history is durable: a reopened file
// applies nothing and keeps what it recorded.
func TestLedgerSurvivesReopen(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "m.db")
	first := openAt(t, dsn)
	if err := Run(first, "main", twoSteps); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second := openAt(t, dsn)
	if err := Run(second, "main", twoSteps); err != nil {
		t.Fatalf("Run after reopen: %v", err)
	}
	if got := recorded(t, second, "main"); got != 2 {
		t.Fatalf("recorded = %d, want 2", got)
	}
}
