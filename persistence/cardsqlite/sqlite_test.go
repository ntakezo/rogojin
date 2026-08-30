package cardsqlite

import (
	"context"
	"encoding/json"
	"github.com/ntakezo/rogojin/leasing"
	"path/filepath"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/cards"
	"github.com/ntakezo/rogojin/persistence/accountsqlite"
)

// satisfiesRepositoryPort fails to compile if SQLite drifts from the persistence port it exists to implement.
var _ cards.Repository = (*SQLite)(nil)

// newTestRepo opens a SQLite repository backed by a fresh temp-file database.
func newTestRepo(t *testing.T) *SQLite {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "cards.db")
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

// TestSaveListRoundTrip verifies every field — the lock owner and
// the workflow's own JSON — survives storage, because lock reclamation and
// payment both read them back verbatim.
func TestSaveListRoundTrip(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	locked := cards.Card{
		Resource: leasing.Resource{ID: "c1", GroupID: "bin", OwnerID: "t1"},
		Fields: mustJSON(t, map[string]string{
			"number": "4111111111111111",
			"expiry": "12/29",
			"cvv":    "737",
		}),
	}
	free := cards.Card{Resource: leasing.Resource{ID: "c2", GroupID: "bin", MaxHolders: 2}}
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
		t.Fatalf("got %d cards, want 2", len(listed))
	}
	if listed[0].ID != "c1" || listed[1].ID != "c2" {
		t.Fatalf("order = %s, %s; want c1, c2", listed[0].ID, listed[1].ID)
	}
	if got := listed[0]; got.OwnerID != "t1" || got.GroupID != "bin" {
		t.Fatalf("c1 round-trip: got %+v", got)
	}
	if string(listed[0].Fields) != string(locked.Fields) {
		t.Fatalf("fields = %s, want %s", listed[0].Fields, locked.Fields)
	}
	if listed[1].MaxHolders != 2 {
		t.Fatalf("c2 max holders = %d, want 2", listed[1].MaxHolders)
	}
	if listed[1].Fields != nil {
		t.Fatalf("c2 fields = %s, want none", listed[1].Fields)
	}
}

// TestFieldsAreOpaqueToTheSchema verifies the point of the JSON column: a raw
// PAN, a gateway token, and a wrapped ciphertext are all just text here, so a
// store that encrypts and one that does not share the same table and the same
// migration history.
func TestFieldsAreOpaqueToTheSchema(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	type raw struct {
		Number string `json:"number"`
		Expiry string `json:"expiry"`
		CVV    string `json:"cvv"`
	}
	type tokenized struct {
		Token   string            `json:"token"`
		Gateway string            `json:"gateway"`
		Billing map[string]string `json:"billing"`
	}
	wantRaw := raw{Number: "4111111111111111", Expiry: "12/29", CVV: "737"}
	wantTokenized := tokenized{
		Token:   "tok_abc",
		Gateway: "acquirer",
		Billing: map[string]string{"zip": "10001", "country": "US"},
	}

	if err := repo.Save(ctx, cards.Card{Resource: leasing.Resource{ID: "c1"}, Fields: mustJSON(t, wantRaw)}); err != nil {
		t.Fatalf("save raw card: %v", err)
	}
	if err := repo.Save(ctx, cards.Card{Resource: leasing.Resource{ID: "c2"}, Fields: mustJSON(t, wantTokenized)}); err != nil {
		t.Fatalf("save tokenized card: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	gotRaw, err := cards.Bind[raw](listed[0])
	if err != nil {
		t.Fatalf("bind raw: %v", err)
	}
	if gotRaw != wantRaw {
		t.Fatalf("raw = %+v, want %+v", gotRaw, wantRaw)
	}
	gotTokenized, err := cards.Bind[tokenized](listed[1])
	if err != nil {
		t.Fatalf("bind tokenized: %v", err)
	}
	if gotTokenized.Token != wantTokenized.Token || gotTokenized.Billing["zip"] != "10001" {
		t.Fatalf("tokenized = %+v, want %+v", gotTokenized, wantTokenized)
	}
}

// TestSaveRejectsFieldsThatAreNotJSON verifies a bad payload is refused at the
// write, not discovered as a decode failure inside a later run — which for a
// card is at the last state before payment.
func TestSaveRejectsFieldsThatAreNotJSON(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.Save(ctx, cards.Card{Resource: leasing.Resource{ID: "c1"}, Fields: json.RawMessage("not json")}); err == nil {
		t.Fatal("expected invalid JSON fields to be refused")
	}
	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("refused save still stored %d cards", len(listed))
	}
}

// TestSavePreservesCreatedAt verifies a lock, an unlock, or a stat update does
// not get to revise when the card was added.
func TestSavePreservesCreatedAt(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	if err := repo.Save(ctx, cards.Card{Resource: leasing.Resource{ID: "c1", CreatedAt: created, UpdatedAt: created}}); err != nil {
		t.Fatalf("first save: %v", err)
	}

	updated := time.Now().UTC().Truncate(time.Millisecond)
	if err := repo.Save(ctx, cards.Card{Resource: leasing.Resource{ID: "c1", OwnerID: "t1", CreatedAt: updated, UpdatedAt: updated}}); err != nil {
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

	if err := repo.Save(ctx, cards.Card{Resource: leasing.Resource{ID: "c1"}}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := repo.Delete(ctx, "c1"); err != nil {
		t.Fatalf("first delete: %v", err)
	}
	if err := repo.Delete(ctx, "c1"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("got %d cards, want none", len(listed))
	}
}

// TestGroupRoundTrip verifies groups persist with their strategy — normally
// the empty string, resolving to round robin — and their timestamps.
func TestGroupRoundTrip(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	created := time.Now().UTC().Truncate(time.Millisecond)
	want := cards.Group{ID: "bin", CreatedAt: created, UpdatedAt: created}
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
	if listed[0].ID != want.ID || listed[0].Strategy != "" || !listed[0].CreatedAt.Equal(created) {
		t.Fatalf("group round-trip: got %+v, want %+v", listed[0], want)
	}

	if err := repo.DeleteGroup(ctx, "bin"); err != nil {
		t.Fatalf("delete group: %v", err)
	}
	if listed, err = repo.ListGroups(ctx); err != nil || len(listed) != 0 {
		t.Fatalf("after delete: %v, %v", listed, err)
	}
}

// TestCardsAndAccountsShareOneFile verifies two stores can be pointed at one
// database file. Each records its migrations under its own name, so neither
// reads the other's progress as its own — which under the per-file counter left
// the second store with no tables at all.
func TestCardsAndAccountsShareOneFile(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "rogojin.db")
	ctx := context.Background()

	accountStore, err := accountsqlite.NewSQLite(dsn)
	if err != nil {
		t.Fatalf("open accounts: %v", err)
	}
	defer accountStore.Close()
	cardStore, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("open cards on the same file: %v", err)
	}
	defer cardStore.Close()

	if err := accountStore.Save(ctx, accounts.Account{Resource: leasing.Resource{ID: "a1", GroupID: "site"}}); err != nil {
		t.Fatalf("save account: %v", err)
	}
	if err := cardStore.Save(ctx, cards.Card{Resource: leasing.Resource{ID: "c1", GroupID: "bin"}}); err != nil {
		t.Fatalf("save card: %v", err)
	}

	listedAccounts, err := accountStore.List(ctx)
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	if len(listedAccounts) != 1 || listedAccounts[0].ID != "a1" {
		t.Fatalf("accounts = %+v, want the stored account", listedAccounts)
	}
	listedCards, err := cardStore.List(ctx)
	if err != nil {
		t.Fatalf("list cards: %v", err)
	}
	if len(listedCards) != 1 || listedCards[0].ID != "c1" {
		t.Fatalf("cards = %+v, want the stored card", listedCards)
	}
}

// TestSchemaReopensCleanly verifies the migration counter holds: a second open
// of the same file applies nothing and loses nothing.
func TestSchemaReopensCleanly(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "cards.db")
	ctx := context.Background()

	first, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := first.Save(ctx, cards.Card{Resource: leasing.Resource{ID: "c1"}, Fields: mustJSON(t, map[string]string{"number": "4111111111111111"})}); err != nil {
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
	if len(listed) != 1 || listed[0].ID != "c1" {
		t.Fatalf("got %+v, want the stored card", listed)
	}
}
