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
// in order, each in its own transaction. It is safe to call on every open:
// already applied steps are skipped, so a database on the current schema is
// untouched.
//
// store names the caller's migration list ("tasks", "proxies"). It is
// recorded with every row and is the only thing separating one store's
// history from another's in a shared file, so it must be stable across
// releases — renaming it presents an already-migrated store as a fresh one.
//
// Databases written before the ledger are adopted on their next open, taking
// the old counter as that store's own progress — see adopt.
func migrate(db *sql.DB, store string, migrations []migration) error {
	if store == "" {
		return errors.New("sqlite: store name is required")
	}
	if _, err := db.Exec(ledger); err != nil {
		return fmt.Errorf("sqlite: create ledger: %w", err)
	}
	if err := adopt(db, store, migrations); err != nil {
		return err
	}

	current, err := applied(db, store)
	if err != nil {
		return err
	}
	if current > len(migrations) {
		return fmt.Errorf("sqlite: store %q is at version %d, newer than the %d known migrations", store, current, len(migrations))
	}
	for i := current; i < len(migrations); i++ {
		version := i + 1
		if err := apply(db, store, migrations[i], version); err != nil {
			return fmt.Errorf("sqlite: %s migration %d (%s): %w", store, version, migrations[i].Name, err)
		}
	}
	return nil
}

// adopt moves a database written before the ledger onto it. Such a database
// carries its progress in PRAGMA user_version, which is per-file and so could
// only ever describe one store: the store opening it is therefore the store
// that wrote it, and the counter's worth of migrations is recorded as its own.
//
// The counter is then cleared, so adoption happens exactly once and a second
// store joining the file afterwards starts from nothing rather than inheriting
// a count that was never about it. An empty ledger is the signal — once any
// store has recorded a row, the file is under the new scheme and the counter is
// no longer consulted.
//
// A counter larger than the adopting store's whole history did not come from
// this store, so adoption refuses it and writes nothing: the file is left as it
// was found, for the store that owns it to adopt properly. A counter that fits
// is taken at face value, which is the one thing adoption cannot verify: a
// pre-ledger file was only ever reachable by the single store that wrote it,
// so the store opening it is that store. Pointing a different one at an
// un-adopted legacy file breaks the assumption — it is refused outright when
// the counter exceeds that store's history, and otherwise fails when the
// migrations it then runs meet tables it did not create.
func adopt(db *sql.DB, store string, migrations []migration) error {
	var rows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&rows); err != nil {
		return fmt.Errorf("sqlite: read ledger: %w", err)
	}
	if rows > 0 {
		return nil
	}
	legacy, err := userVersion(db)
	if err != nil || legacy == 0 {
		return err
	}
	if legacy > len(migrations) {
		return fmt.Errorf("sqlite: cannot adopt %q: the database counts %d applied migrations but %s knows only %d, so the counter belongs to another store", store, legacy, store, len(migrations))
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("sqlite: adopt %s: %w", store, err)
	}
	defer tx.Rollback()

	for version := 1; version <= legacy; version++ {
		if _, err := tx.Exec(
			`INSERT INTO schema_migrations (store, version, name, applied_at) VALUES (?, ?, ?, ?)`,
			store, version, migrations[version-1].Name, now()); err != nil {
			return fmt.Errorf("sqlite: adopt %s: %w", store, err)
		}
	}
	if _, err := tx.Exec(`PRAGMA user_version = 0`); err != nil {
		return fmt.Errorf("sqlite: adopt %s: %w", store, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: adopt %s: %w", store, err)
	}
	return nil
}

// applied reports how many of the store's migrations the ledger records,
// refusing a history with a hole in it. Recorded versions are a prefix of the
// list — 1, 2, 3 — so a missing one in the middle means the ledger was edited
// or a step was undone by hand, and resuming from the count would skip whatever
// is missing forever.
func applied(db *sql.DB, store string) (int, error) {
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

// userVersion reads the pre-ledger counter from the SQLite file header. It is
// consulted only by adopt.
func userVersion(db *sql.DB) (int, error) {
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return 0, fmt.Errorf("sqlite: read user_version: %w", err)
	}
	return v, nil
}

// apply runs one migration and records it in the same transaction, so a step
// that fails leaves neither its effects nor its ledger row behind.
func apply(db *sql.DB, store string, m migration, version int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(m.SQL); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`INSERT INTO schema_migrations (store, version, name, applied_at) VALUES (?, ?, ?, ?)`,
		store, version, m.Name, now()); err != nil {
		return err
	}
	return tx.Commit()
}

// now stamps ledger rows the way the stores stamp their own timestamps.
func now() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
