package sqlitemigrate

import (
	"database/sql"
	"fmt"
)

type Migration struct {
	Name string
	SQL  string
}

// Run applies every migration not yet recorded in PRAGMA user_version, in order, each in its own transaction.
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

// userVersion reads the applied-migration counter from the SQLite file header.
func userVersion(db *sql.DB) (int, error) {
	var v int
	if err := db.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return 0, fmt.Errorf("sqlitemigrate: read user_version: %w", err)
	}
	return v, nil
}

// apply runs one migration and stamps the new version atomically in the same transaction.
func apply(db *sql.DB, m Migration, version int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(m.SQL); err != nil {
		return err
	}
	if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
		return err
	}
	return tx.Commit()
}
