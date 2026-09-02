package sqlite

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// A migration is one step of a store's schema history: the SQL to run and a
// name recorded alongside it, so the ledger reads as a history rather than a
// column of integers.
type migration struct {
	Name string
	SQL  string
}

// ledger is the table every store records its applied migrations in. It is
// shared across stores and keyed by store name, which is what lets one file
// hold several independent histories.
//
// SQLite has a built-in counter for this job, PRAGMA user_version, but it is
// one integer in the file header and so can only ever describe one migration
// list per file. Pointing a second store at the same file under that scheme
// made it read the first store's count as its own and skip its own tables.
const ledger = `CREATE TABLE IF NOT EXISTS schema_migrations (
	store      TEXT NOT NULL,
	version    INTEGER NOT NULL,
	name       TEXT NOT NULL,
	applied_at TEXT NOT NULL,
	PRIMARY KEY (store, version)
)`

// migrate applies every migration of store not yet recorded in the ledger,
// in order, all inside one transaction. It is safe to call on every open:
// already applied steps are skipped, so a database on the current schema is
// untouched.
//
// The transaction begins immediate (see connSettings), so reading the
// recorded history and applying what is missing is one atomic step: two
// processes opening the same file concurrently serialize at BEGIN, and the
// loser finds the winner's rows recorded instead of re-running its steps
// into "duplicate column" errors. It also makes an upgrade all-or-nothing —
// a failing step rolls back every step of the sweep, leaving the file at
// the last release's schema rather than partway between two.
//
// store names the caller's migration list ("tasks", "proxies"). It is
// recorded with every row and is the only thing separating one store's
// history from another's in a shared file, so it must be stable across
// releases — renaming it presents an already-migrated store as a fresh one.
//
// The ledger is the sole authority on progress. A file carrying a nonzero
// PRAGMA user_version was written before the ledger existed, by a schema this
// release no longer describes, and is refused outright — see rejectPreLedger.
func migrate(db *sql.DB, store string, migrations []migration) error {
	if store == "" {
		return errors.New("sqlite: store name is required")
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("sqlite: begin migration: %w", err)
	}
	defer tx.Rollback()

	if err := rejectPreLedger(tx); err != nil {
		return err
	}
	if _, err := tx.Exec(ledger); err != nil {
		return fmt.Errorf("sqlite: create ledger: %w", err)
	}

	current, err := applied(tx, store)
	if err != nil {
		return err
	}
	if current > len(migrations) {
		return fmt.Errorf("sqlite: store %q is at version %d, newer than the %d known migrations", store, current, len(migrations))
	}
	for i := current; i < len(migrations); i++ {
		version := i + 1
		if err := apply(tx, store, migrations[i], version); err != nil {
			return fmt.Errorf("sqlite: %s migration %d (%s): %w", store, version, migrations[i].Name, err)
		}
	}
	return tx.Commit()
}

// querier is the read surface shared by sql.DB and sql.Tx, so the history
// helpers serve the migration transaction and a caller inspecting a database
// outside one alike.
type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
	QueryRow(query string, args ...any) *sql.Row
}

// rejectPreLedger refuses a database whose file-header counter is set. Before
// the ledger, progress lived in PRAGMA user_version, and the migration lists
// of that era were retired when the histories were collapsed to their current
// baselines — there is no sequence of known steps that upgrades such a file,
// only ways to quietly mismatch it. Nothing under the ledger scheme ever sets
// the counter, so a nonzero value can only mean a pre-ledger file, and the
// honest response is to stop before writing anything and ask for a fresh one.
func rejectPreLedger(db querier) error {
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return fmt.Errorf("sqlite: read user_version: %w", err)
	}
	if v != 0 {
		return fmt.Errorf("sqlite: the database predates the migration ledger (user_version = %d) and this release cannot upgrade it; recreate the database file", v)
	}
	return nil
}

// applied reports how many of the store's migrations the ledger records,
// refusing a history with a hole in it. Recorded versions are a prefix of the
// list — 1, 2, 3 — so a missing one in the middle means the ledger was edited
// or a step was undone by hand, and resuming from the count would skip whatever
// is missing forever.
func applied(db querier, store string) (int, error) {
	rows, err := db.Query(`SELECT version FROM schema_migrations WHERE store = ? ORDER BY version`, store)
	if err != nil {
		return 0, fmt.Errorf("sqlite: read %s history: %w", store, err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return 0, fmt.Errorf("sqlite: read %s history: %w", store, err)
		}
		count++
		if version != count {
			return 0, fmt.Errorf("sqlite: store %q records migration %d with %d missing; the history has a gap", store, version, count)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("sqlite: read %s history: %w", store, err)
	}
	return count, nil
}

// apply runs one migration and records it, inside the sweep's transaction.
func apply(tx *sql.Tx, store string, m migration, version int) error {
	if _, err := tx.Exec(m.SQL); err != nil {
		return err
	}
	_, err := tx.Exec(
		`INSERT INTO schema_migrations (store, version, name, applied_at) VALUES (?, ?, ?, ?)`,
		store, version, m.Name, now())
	return err
}

// now stamps ledger rows the way the stores stamp their own timestamps.
func now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
