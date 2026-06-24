// Package sqlitemigrate runs ordered, versioned schema migrations for the
// file-backed SQLite repositories. It uses SQLite's built-in PRAGMA user_version
// as the applied-version counter, so no bookkeeping table or external dependency
// is needed. Each repository declares its schema history as an ordered slice of
// Migrations; Run brings a database from its current version up to the latest.
package sqlitemigrate

import (
	"database/sql"
	"fmt"
)

// Migration is one step in a repository's schema history. SQL is applied in a
// transaction and should be a single, additive change. Once a Migration has
// shipped, never edit or reorder it: user_version pins how many steps have run,
// so rewriting history would silently skip or misapply steps on existing files.
type Migration struct {
	Name string
	SQL  string
}

// Run applies every migration the database has not yet recorded in its PRAGMA
// user_version, in order, each in its own transaction that also advances
// user_version — so an interrupted upgrade leaves the database at a clean,
// resumable version rather than half-applied. It is safe to call on every open:
// a database already at the latest version is left untouched. It fails loudly if
// the database reports a version newer than the known migrations, since older
// code must not operate a schema it does not understand.
func Run(db *sql.DB, migrations []Migration) error {
	current, err := userVersion(db)
	if err != nil {
		return err
	}
	if current > len(migrations) {
		return fmt.Errorf("sqlitemigrate: database at version %d is newer than the %d known migrations", current, len(migrations))
	}
	for i := current; i < len(migrations); i++ {
		version := i + 1
		if err := apply(db, migrations[i], version); err != nil {
			return fmt.Errorf("sqlitemigrate: migration %d (%s): %w", version, migrations[i].Name, err)
		}
	}
	return nil
}

// userVersion reads the applied-migration counter from the SQLite file header. A
// database that has never been migrated reports 0.
func userVersion(db *sql.DB) (int, error) {
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return 0, fmt.Errorf("sqlitemigrate: read user_version: %w", err)
	}
	return v, nil
}

// apply runs one migration and stamps the new version atomically in the same
// transaction, so the schema change and the version bump commit or roll back
// together. PRAGMA does not accept bound parameters, so version is interpolated
// directly; it is an integer the runner derives from the migration index, never
// caller input.
func apply(db *sql.DB, m Migration, version int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // no-op once committed; rolls back a failed step

	if _, err := tx.Exec(m.SQL); err != nil {
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
		return err
	}
	return tx.Commit()
}
