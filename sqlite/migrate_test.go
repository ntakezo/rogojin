package sqlite

import (
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// openRawDB opens a fresh temp-file database on a bare *sql.DB, with the
// connection settings Open uses, so tests exercise migrations the way production does.
func openRawDB(t *testing.T) *sql.DB {
	t.Helper()
	return openRawAt(t, filepath.Join(t.TempDir(), "m.db"))
}

// openRawAt opens a named database, so a test can point two stores at one file or
// close and reopen the same one.
func openRawAt(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", withSettings(dsn))
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

// twoSteps is a representative history: create a table, then add a column — the
// shape a store's history takes as it grows past its baseline.
var twoSteps = []migration{
	{Name: "create t", SQL: `CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY)`},
	{Name: "add col", SQL: `ALTER TABLE t ADD COLUMN extra TEXT`},
}

// otherSteps is a second store's unrelated history, for the shared-file tests.
var otherSteps = []migration{
	{Name: "create u", SQL: `CREATE TABLE IF NOT EXISTS u (id TEXT PRIMARY KEY)`},
}

// TestRunAppliesAllOnFreshDatabase verifies a fresh database runs every migration
// in order and ends recorded at the latest version, so a new install lands on the
// current schema in a single open.
func TestRunAppliesAllOnFreshDatabase(t *testing.T) {
	db := openRawDB(t)
	if err := migrate(db, "main", twoSteps); err != nil {
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
	db := openRawDB(t)
	if err := migrate(db, "main", twoSteps); err != nil {
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
	db := openRawDB(t)
	if err := migrate(db, "main", twoSteps); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := migrate(db, "main", twoSteps); err != nil {
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
	db := openRawDB(t)
	if err := migrate(db, "main", twoSteps[:1]); err != nil {
		t.Fatalf("Run of the first step: %v", err)
	}
	if got := recorded(t, db, "main"); got != 1 {
		t.Fatalf("recorded = %d, want 1", got)
	}

	// The upgrade ships a second step; only that one may run, since re-running
	// the ALTER would fail as a duplicate column.
	if err := migrate(db, "main", twoSteps); err != nil {
		t.Fatalf("Run after the upgrade: %v", err)
	}
	if got := recorded(t, db, "main"); got != 2 {
		t.Fatalf("recorded = %d, want 2", got)
	}
}

// TestRunRollsBackFailedmigration verifies a failing migration rolls back the
// whole sweep — its own effects and every step applied before it in the same
// call — so a botched upgrade leaves the file at the last release's schema
// instead of partway between two.
func TestRunRollsBackFailedmigration(t *testing.T) {
	db := openRawDB(t)
	steps := []migration{
		{Name: "good", SQL: `CREATE TABLE good (id TEXT)`},
		{Name: "bad", SQL: `CREATE TABLE good (id TEXT)`}, // re-creating the table fails
	}
	if err := migrate(db, "main", steps); err == nil {
		t.Fatal("Run: want error from duplicate table, got nil")
	}
	// On a fresh database the rollback takes everything with it — the ledger
	// included — so the file looks never opened.
	var tables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'`).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 0 {
		t.Fatalf("tables = %d, want none left by a failed sweep", tables)
	}

	// A sweep whose steps all succeed still lands whole.
	if err := migrate(db, "main", steps[:1]); err != nil {
		t.Fatalf("Run after the fix: %v", err)
	}
	if got := recorded(t, db, "main"); got != 1 {
		t.Fatalf("recorded = %d, want 1", got)
	}
}

// TestConcurrentOpensMigrateOnce verifies two handles on one file racing the
// same migration list serialize instead of colliding: exactly one applies each
// step, the other finds it recorded, and both return clean — the rolling-deploy
// case, where two processes open the database at the same moment.
func TestConcurrentOpensMigrateOnce(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "m.db")
	handles := []*sql.DB{openRawAt(t, dsn), openRawAt(t, dsn)}

	errs := make([]error, len(handles))
	var wg sync.WaitGroup
	for i, db := range handles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = migrate(db, "main", twoSteps)
		}()
	}
	wg.Wait()

	// Both racers return clean; a loser that re-ran the ALTER would have
	// surfaced "duplicate column" here.
	for i, err := range errs {
		if err != nil {
			t.Fatalf("racer %d: %v", i, err)
		}
	}
	if got := recorded(t, handles[0], "main"); got != 2 {
		t.Fatalf("recorded = %d, want 2", got)
	}
}

// TestRunRejectsNewerDatabase verifies a store recorded beyond the migrations it
// knows is refused rather than silently operated on, because older code must not
// run against a schema from a future version.
func TestRunRejectsNewerDatabase(t *testing.T) {
	db := openRawDB(t)
	if err := migrate(db, "main", twoSteps); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Older code, shipping only the first of the two steps it later grew.
	err := migrate(db, "main", twoSteps[:1])
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
	db := openRawDB(t)
	if err := migrate(db, "main", twoSteps); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE store = 'main' AND version = 1`); err != nil {
		t.Fatalf("punch a hole: %v", err)
	}

	err := migrate(db, "main", twoSteps)
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
	db := openRawDB(t)
	if err := migrate(db, "first", twoSteps); err != nil {
		t.Fatalf("first store: %v", err)
	}
	if err := migrate(db, "second", otherSteps); err != nil {
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
	db := openRawDB(t)
	if err := migrate(db, "first", twoSteps[:1]); err != nil {
		t.Fatalf("first store: %v", err)
	}
	if err := migrate(db, "second", otherSteps); err != nil {
		t.Fatalf("second store: %v", err)
	}
	if err := migrate(db, "first", twoSteps); err != nil {
		t.Fatalf("first store upgrade: %v", err)
	}

	if got := recorded(t, db, "first"); got != 2 {
		t.Fatalf("first store recorded = %d, want 2", got)
	}
	if got := recorded(t, db, "second"); got != 1 {
		t.Fatalf("second store recorded = %d, want 1 (untouched by the other's upgrade)", got)
	}
}

// TestRejectsPreLedgerDatabase verifies a database carrying the retired
// file-header counter is refused before anything is written. The migration
// lists that counter described no longer ship, so there is no upgrade for such
// a file — only quiet mismatches — and the refusal has to leave it untouched
// for whoever wants to salvage it by hand.
func TestRejectsPreLedgerDatabase(t *testing.T) {
	db := openRawDB(t)
	if _, err := db.Exec(`PRAGMA user_version = 2`); err != nil {
		t.Fatalf("seed counter: %v", err)
	}

	err := migrate(db, "main", twoSteps)
	if err == nil {
		t.Fatal("Run: want a pre-ledger database to be refused, got nil")
	}
	if !strings.Contains(err.Error(), "recreate") {
		t.Fatalf("err = %v, want it to say the file must be recreated", err)
	}
	// The refusal writes nothing: no ledger, no tables, counter untouched.
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || v != 2 {
		t.Fatalf("user_version = %d (err %v), want the counter left at 2", v, err)
	}
	var tables int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'`).Scan(&tables); err != nil {
		t.Fatalf("count tables: %v", err)
	}
	if tables != 0 {
		t.Fatalf("tables = %d, want none created by a refused open", tables)
	}
}

// TestRunRequiresAStoreName verifies the key every row is filed under cannot be
// empty, since an unnamed store would collide with the next one.
func TestRunRequiresAStoreName(t *testing.T) {
	db := openRawDB(t)
	if err := migrate(db, "", twoSteps); err == nil {
		t.Fatal("Run: want an empty store name to be refused, got nil")
	}
}

// TestLedgerSurvivesReopen verifies the history is durable: a reopened file
// applies nothing and keeps what it recorded.
func TestLedgerSurvivesReopen(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "m.db")
	first := openRawAt(t, dsn)
	if err := migrate(first, "main", twoSteps); err != nil {
		t.Fatalf("first Run: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second := openRawAt(t, dsn)
	if err := migrate(second, "main", twoSteps); err != nil {
		t.Fatalf("Run after reopen: %v", err)
	}
	if got := recorded(t, second, "main"); got != 2 {
		t.Fatalf("recorded = %d, want 2", got)
	}
}
