package postgres

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
)

// dsnEnv names the server the suite runs against; without it every postgres
// test skips, so the suite costs nothing where no server is available.
const dsnEnv = "ROGOJIN_POSTGRES_DSN"

// openTestDB opens the stores on a schema of their own — created fresh,
// selected via search_path, dropped with the test — the postgres equivalent
// of sqlite's temp files, so parallel tests never meet each other's tables.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	base := os.Getenv(dsnEnv)
	if base == "" {
		t.Skipf("%s not set; postgres suite skipped", dsnEnv)
	}

	raw := make([]byte, 6)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("random schema name: %v", err)
	}
	schema := "rogojin_test_" + hex.EncodeToString(raw)

	admin, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		admin.Close()
		t.Fatalf("create schema %s: %v", schema, err)
	}

	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	db, err := Open(base + sep + "search_path=" + schema)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		db.Close()
		if _, err := admin.Exec(fmt.Sprintf(`DROP SCHEMA %s CASCADE`, schema)); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
		admin.Close()
	})
	return db
}
