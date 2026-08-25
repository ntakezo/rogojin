package accountsqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/accounts"
)

// satisfiesRepositoryPort fails to compile if SQLite drifts from the persistence port it exists to implement.
var _ accounts.Repository = (*SQLite)(nil)

// newTestRepo opens a SQLite repository backed by a fresh temp-file database.
func newTestRepo(t *testing.T) *SQLite {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "accounts.db")
	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { repo.Close() })
	return repo
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// TestSaveListRoundTrip verifies every field — the lock owner, the stats, and
// the workflow's own JSON — survives storage, because lock reclamation and
// login both read them back verbatim.
func TestSaveListRoundTrip(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	locked := accounts.Account{
		ID:      "a1",
		GroupID: "site",
		OwnerID: "t1",
		Fields: mustJSON(t, map[string]string{
			"email":    "buyer@example.com",
			"password": "hunter2",
		}),
		Successes: 3,
		Failures:  2,
	}
	free := accounts.Account{ID: "a2", GroupID: "site", MaxHolders: 2}
	if err := repo.Save(ctx, locked); err != nil {
		t.Fatalf("save locked: %v", err)
	}
	if err := repo.Save(ctx, free); err != nil {
		t.Fatalf("save free: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("got %d accounts, want 2", len(listed))
	}
	if listed[0].ID != "a1" || listed[1].ID != "a2" {
		t.Fatalf("order = %s, %s; want a1, a2", listed[0].ID, listed[1].ID)
	}
	if got := listed[0]; got.OwnerID != "t1" || got.Successes != 3 || got.Failures != 2 || got.GroupID != "site" {
		t.Fatalf("a1 round-trip: got %+v", got)
	}
	if string(listed[0].Fields) != string(locked.Fields) {
		t.Fatalf("fields = %s, want %s", listed[0].Fields, locked.Fields)
	}
	if listed[1].MaxHolders != 2 {
		t.Fatalf("a2 max holders = %d, want 2", listed[1].MaxHolders)
	}
	if listed[1].Fields != nil {
		t.Fatalf("a2 fields = %s, want none", listed[1].Fields)
	}
}

// TestFieldsAreOpaqueToTheSchema verifies the point of the JSON column: two
// workflows with unrelated account shapes share one table and one migration
// history.
func TestFieldsAreOpaqueToTheSchema(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	type checkout struct {
		Email string `json:"email"`
		Card  string `json:"card"`
	}
	type forum struct {
		Handle string   `json:"handle"`
		Token  string   `json:"token"`
		Boards []string `json:"boards"`
	}
	wantCheckout := checkout{Email: "buyer@example.com", Card: "4111"}
	wantForum := forum{Handle: "ada", Token: "t0k", Boards: []string{"a", "b"}}

	if err := repo.Save(ctx, accounts.Account{ID: "a1", Fields: mustJSON(t, wantCheckout)}); err != nil {
		t.Fatalf("save checkout account: %v", err)
	}
	if err := repo.Save(ctx, accounts.Account{ID: "a2", Fields: mustJSON(t, wantForum)}); err != nil {
		t.Fatalf("save forum account: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	gotCheckout, err := accounts.Bind[checkout](listed[0])
	if err != nil {
		t.Fatalf("bind checkout: %v", err)
	}
	if gotCheckout != wantCheckout {
		t.Fatalf("checkout = %+v, want %+v", gotCheckout, wantCheckout)
	}
	gotForum, err := accounts.Bind[forum](listed[1])
	if err != nil {
		t.Fatalf("bind forum: %v", err)
	}
	if gotForum.Handle != wantForum.Handle || gotForum.Token != wantForum.Token || len(gotForum.Boards) != 2 {
		t.Fatalf("forum = %+v, want %+v", gotForum, wantForum)
	}
}

// TestSaveRejectsFieldsThatAreNotJSON verifies a bad payload is refused at the
// write, not discovered as a decode failure inside a later run.
func TestSaveRejectsFieldsThatAreNotJSON(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.Save(ctx, accounts.Account{ID: "a1", Fields: json.RawMessage("not json")}); err == nil {
		t.Fatal("expected invalid JSON fields to be refused")
	}
	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("refused save still stored %d accounts", len(listed))
	}
}

// TestSavePreservesCreatedAt verifies a lock, an unlock, or a stat update does
// not get to revise when the account was added.
func TestSavePreservesCreatedAt(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	if err := repo.Save(ctx, accounts.Account{ID: "a1", CreatedAt: created, UpdatedAt: created}); err != nil {
		t.Fatalf("first save: %v", err)
	}

	updated := time.Now().UTC().Truncate(time.Millisecond)
	if err := repo.Save(ctx, accounts.Account{ID: "a1", OwnerID: "t1", CreatedAt: updated, UpdatedAt: updated}); err != nil {
		t.Fatalf("second save: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if !listed[0].CreatedAt.Equal(created) {
		t.Fatalf("created_at = %s, want the original %s", listed[0].CreatedAt, created)
	}
	if !listed[0].UpdatedAt.Equal(updated) {
		t.Fatalf("updated_at = %s, want the refreshed %s", listed[0].UpdatedAt, updated)
	}
}

// TestDeleteIsIdempotent verifies deleting an absent row is not an error, so a
// manager cleaning up after a partial failure can retry.
func TestDeleteIsIdempotent(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.Save(ctx, accounts.Account{ID: "a1"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := repo.Delete(ctx, "a1"); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := repo.Delete(ctx, "a1"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("got %d accounts, want none", len(listed))
	}
}

// TestGroupRoundTrip verifies groups persist without a strategy column: the one
// knob proxies carry has no meaning here, so the schema does not reserve space
// for it.
func TestGroupRoundTrip(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	created := time.Now().UTC().Truncate(time.Millisecond)
	want := accounts.Group{ID: "site", MaxHolders: 2, CreatedAt: created, UpdatedAt: created}
	if err := repo.SaveGroup(ctx, want); err != nil {
		t.Fatalf("save group: %v", err)
	}

	listed, err := repo.ListGroups(ctx)
	if err != nil {
		t.Fatalf("list groups: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("got %d groups, want 1", len(listed))
	}
	if listed[0].ID != want.ID || listed[0].MaxHolders != want.MaxHolders {
		t.Fatalf("group round-trip: got %+v, want %+v", listed[0], want)
	}

	if err := repo.DeleteGroup(ctx, "site"); err != nil {
		t.Fatalf("delete group: %v", err)
	}
	if listed, err = repo.ListGroups(ctx); err != nil || len(listed) != 0 {
		t.Fatalf("after delete: %v, %v", listed, err)
	}
}

// TestAdoptsAPreLedgerDatabase verifies the upgrade path for a database written
// before migrations were recorded in a ledger, when progress lived in the
// file-header counter. Its rows must survive and its schema must not be
// re-migrated, since the store is opened by the same code on every start.
func TestAdoptsAPreLedgerDatabase(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "accounts.db")
	ctx := context.Background()

	// Hand-build what the old code left behind: both tables, the counter at 2,
	// and no ledger.
	raw, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	for _, m := range migrations {
		if _, err := raw.Exec(m.SQL); err != nil {
			t.Fatalf("seed %s: %v", m.Name, err)
		}
	}
	if _, err := raw.Exec(`INSERT INTO accounts (id, group_id) VALUES ('a1', 'site')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	if _, err := raw.Exec(`PRAGMA user_version = 2`); err != nil {
		t.Fatalf("seed counter: %v", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("open a pre-ledger database: %v", err)
	}
	defer repo.Close()

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != "a1" {
		t.Fatalf("got %+v, want the row the old database held", listed)
	}
	if err := repo.Save(ctx, accounts.Account{ID: "a2"}); err != nil {
		t.Fatalf("save after adoption: %v", err)
	}
}

// TestSchemaReopensCleanly verifies the migration counter holds: a second open
// of the same file applies nothing and loses nothing.
func TestSchemaReopensCleanly(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "accounts.db")
	ctx := context.Background()

	first, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := first.Save(ctx, accounts.Account{ID: "a1", Fields: mustJSON(t, map[string]string{"email": "a@b.c"})}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	second, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer second.Close()

	listed, err := second.List(ctx)
	if err != nil {
		t.Fatalf("list after reopen: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != "a1" {
		t.Fatalf("got %+v, want the stored account", listed)
	}
}
