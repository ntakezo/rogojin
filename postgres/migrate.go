package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// A migration is one step of a store's schema history: the SQL to run and a
// name recorded alongside it. The postgres histories are fresh baselines —
// this adapter was born on the current port contract, so each store ships one
// create step rather than replaying another adapter's ALTER trail.
type migration struct {
	Name string
	SQL  string
}

// ledger is the table every store records its applied migrations in, shared
// across stores and keyed by store name — the same arrangement as sqlite's,
// scoped to whatever schema the connection's search_path selects. There is no
// pre-ledger era to reject here: postgres databases never carried the retired
// user_version counter, so any database without a ledger is simply fresh.
const ledger = `CREATE TABLE IF NOT EXISTS schema_migrations (
	store      TEXT NOT NULL,
	version    INTEGER NOT NULL,
	name       TEXT NOT NULL,
	applied_at TEXT NOT NULL,
	PRIMARY KEY (store, version)
)`

// migrate applies every migration of store not yet recorded in the ledger, in
// order, all inside one transaction. The transaction takes a cluster-wide
// advisory lock before touching anything — several processes booting against
// one database serialize there, and the loser finds the winner's rows
// recorded — and a failing step rolls the whole sweep back.
func migrate(db *sql.DB, store string, migrations []migration) error {
	if store == "" {
		return errors.New("postgres: store name is required")
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("postgres: begin migration: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtextextended('rogojin_migrations', 0))`); err != nil {
		return fmt.Errorf("postgres: take migration lock: %w", err)
	}
	if _, err := tx.Exec(ledger); err != nil {
		return fmt.Errorf("postgres: create ledger: %w", err)
	}

	current, err := applied(tx, store)
	if err != nil {
		return err
	}
	if current > len(migrations) {
		return fmt.Errorf("postgres: store %q is at version %d, newer than the %d known migrations", store, current, len(migrations))
	}
	for i := current; i < len(migrations); i++ {
		version := i + 1
		if err := apply(tx, store, migrations[i], version); err != nil {
			return fmt.Errorf("postgres: %s migration %d (%s): %w", store, version, migrations[i].Name, err)
		}
	}
	return tx.Commit()
}

// applied reports how many of the store's migrations the ledger records,
// refusing a history with a hole in it — a missing middle version means the
// ledger was edited, and resuming from the count would skip it forever.
func applied(tx *sql.Tx, store string) (int, error) {
	rows, err := tx.Query(`SELECT version FROM schema_migrations WHERE store = $1 ORDER BY version`, store)
	if err != nil {
		return 0, fmt.Errorf("postgres: read %s history: %w", store, err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			return 0, fmt.Errorf("postgres: read %s history: %w", store, err)
		}
		count++
		if version != count {
			return 0, fmt.Errorf("postgres: store %q records migration %d with %d missing; the history has a gap", store, version, count)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("postgres: read %s history: %w", store, err)
	}
	return count, nil
}

// apply runs one migration and records it, inside the sweep's transaction.
func apply(tx *sql.Tx, store string, m migration, version int) error {
	if _, err := tx.Exec(m.SQL); err != nil {
		return err
	}
	_, err := tx.Exec(
		`INSERT INTO schema_migrations (store, version, name, applied_at) VALUES ($1, $2, $3, $4)`,
		store, version, m.Name, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
