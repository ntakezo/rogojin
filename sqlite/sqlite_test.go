package sqlite

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

// openTestDB opens a fresh temp-file database, closed when the test ends.
func openTestDB(t *testing.T) *DB {
	t.Helper()
	return openAt(t, filepath.Join(t.TempDir(), "rogojin.db"))
}

// openAt opens the database at dsn, closed when the test ends, so a test can
// reopen a file or point several stores at one.
func openAt(t *testing.T, dsn string) *DB {
	t.Helper()
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open %s: %v", dsn, err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}
