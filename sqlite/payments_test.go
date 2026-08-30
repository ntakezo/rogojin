package sqlite

import (
	"context"
	"encoding/json"
	"github.com/ntakezo/rogojin/leasing"
	"path/filepath"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/payments"
)

// satisfiesRepositoryPort fails to compile if Payments drifts from the persistence port it exists to implement.
var _ payments.Repository = (*Payments)(nil)

// newTestPayments opens the payments store on a fresh temp-file database.
func newTestPayments(t *testing.T) payments.Repository {
	t.Helper()
	repo, err := NewPayments(openTestDB(t))
	if err != nil {
		t.Fatalf("NewPayments: %v", err)
	}
	return repo
}

// TestPaymentsSaveListRoundTrip verifies every field — the lock owner and
// the workflow's own JSON — survives storage, because lock reclamation and
// payment both read them back verbatim.
func TestPaymentsSaveListRoundTrip(t *testing.T) {
	repo := newTestPayments(t)
	ctx := context.Background()

	locked := payments.Payment{
		Resource: leasing.Resource{ID: "c1", GroupID: "bin", OwnerID: "t1"},
		Fields: mustJSON(t, map[string]string{
			"number": "4111111111111111",
			"expiry": "12/29",
			"cvv":    "737",
		}),
	}
	free := payments.Payment{Resource: leasing.Resource{ID: "c2", GroupID: "bin", MaxHolders: 2}}
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
		t.Fatalf("got %d payments, want 2", len(listed))
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

// TestPaymentsFieldsAreOpaqueToTheSchema verifies the point of the JSON column: a raw
// PAN, a gateway token, and a wrapped ciphertext are all just text here, so a
// store that encrypts and one that does not share the same table and the same
// migration history.
func TestPaymentsFieldsAreOpaqueToTheSchema(t *testing.T) {
	repo := newTestPayments(t)
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

	if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1"}, Fields: mustJSON(t, wantRaw)}); err != nil {
		t.Fatalf("save raw payment: %v", err)
	}
	if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c2"}, Fields: mustJSON(t, wantTokenized)}); err != nil {
		t.Fatalf("save tokenized payment: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	gotRaw, err := payments.Bind[raw](listed[0])
	if err != nil {
		t.Fatalf("bind raw: %v", err)
	}
	if gotRaw != wantRaw {
		t.Fatalf("raw = %+v, want %+v", gotRaw, wantRaw)
	}
	gotTokenized, err := payments.Bind[tokenized](listed[1])
	if err != nil {
		t.Fatalf("bind tokenized: %v", err)
	}
	if gotTokenized.Token != wantTokenized.Token || gotTokenized.Billing["zip"] != "10001" {
		t.Fatalf("tokenized = %+v, want %+v", gotTokenized, wantTokenized)
	}
}

// TestPaymentsSaveRejectsFieldsThatAreNotJSON verifies a bad payload is refused at the
// write, not discovered as a decode failure inside a later run — which for a
// payment is at the last state before payment.
func TestPaymentsSaveRejectsFieldsThatAreNotJSON(t *testing.T) {
	repo := newTestPayments(t)
	ctx := context.Background()

	if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1"}, Fields: json.RawMessage("not json")}); err == nil {
		t.Fatal("expected invalid JSON fields to be refused")
	}
	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 0 {
		t.Fatalf("refused save still stored %d payments", len(listed))
	}
}

// TestPaymentsSavePreservesCreatedAt verifies a lock, an unlock, or a stat update does
// not get to revise when the payment was added.
func TestPaymentsSavePreservesCreatedAt(t *testing.T) {
	repo := newTestPayments(t)
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1", CreatedAt: created, UpdatedAt: created}}); err != nil {
		t.Fatalf("first save: %v", err)
	}

	updated := time.Now().UTC().Truncate(time.Millisecond)
	if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1", OwnerID: "t1", CreatedAt: updated, UpdatedAt: updated}}); err != nil {
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

// TestPaymentsDeleteIsIdempotent verifies deleting an absent row is not an error, so a
// manager cleaning up after a partial failure can retry.
func TestPaymentsDeleteIsIdempotent(t *testing.T) {
	repo := newTestPayments(t)
	ctx := context.Background()

	if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1"}}); err != nil {
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
		t.Fatalf("got %d payments, want none", len(listed))
	}
}

// TestPaymentsGroupRoundTrip verifies groups persist with their strategy — normally
// the empty string, resolving to round robin — and their timestamps.
func TestPaymentsGroupRoundTrip(t *testing.T) {
	repo := newTestPayments(t)
	ctx := context.Background()

	created := time.Now().UTC().Truncate(time.Millisecond)
	want := payments.Group{ID: "bin", CreatedAt: created, UpdatedAt: created}
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

// TestPaymentsAndAccountsShareOneDatabase verifies two stores build on one open
// database. Each records its migrations under its own name, so neither reads
// the other's progress as its own — which under the per-file counter left the
// second store with no tables at all.
func TestPaymentsAndAccountsShareOneDatabase(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	accountStore, err := NewAccounts(db)
	if err != nil {
		t.Fatalf("open accounts: %v", err)
	}
	paymentStore, err := NewPayments(db)
	if err != nil {
		t.Fatalf("open payments on the same database: %v", err)
	}

	if err := accountStore.Save(ctx, accounts.Account{Resource: leasing.Resource{ID: "a1", GroupID: "site"}}); err != nil {
		t.Fatalf("save account: %v", err)
	}
	if err := paymentStore.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1", GroupID: "bin"}}); err != nil {
		t.Fatalf("save payment: %v", err)
	}

	listedAccounts, err := accountStore.List(ctx)
	if err != nil {
		t.Fatalf("list accounts: %v", err)
	}
	if len(listedAccounts) != 1 || listedAccounts[0].ID != "a1" {
		t.Fatalf("accounts = %+v, want the stored account", listedAccounts)
	}
	listedPayments, err := paymentStore.List(ctx)
	if err != nil {
		t.Fatalf("list payments: %v", err)
	}
	if len(listedPayments) != 1 || listedPayments[0].ID != "c1" {
		t.Fatalf("payments = %+v, want the stored payment", listedPayments)
	}
}

// TestPaymentsSchemaReopensCleanly verifies the migration counter holds: a second open
// of the same file applies nothing and loses nothing.
func TestPaymentsSchemaReopensCleanly(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "payments.db")
	ctx := context.Background()

	firstDB := openAt(t, dsn)
	first, err := NewPayments(firstDB)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	if err := first.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1"}, Fields: mustJSON(t, map[string]string{"number": "4111111111111111"})}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	secondDB := openAt(t, dsn)
	second, err := NewPayments(secondDB)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	defer secondDB.Close()

	listed, err := second.List(ctx)
	if err != nil {
		t.Fatalf("list after reopen: %v", err)
	}
	if len(listed) != 1 || listed[0].ID != "c1" {
		t.Fatalf("got %+v, want the stored payment", listed)
	}
}
