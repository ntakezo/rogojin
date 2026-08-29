package emailsqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/persistence/accountsqlite"
)

// satisfiesRepositoryPort fails to compile if SQLite drifts from the persistence port it exists to implement.
var _ email.Repository = (*SQLite)(nil)

// newTestRepo opens a SQLite repository backed by a fresh temp-file database.
func newTestRepo(t *testing.T) *SQLite {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "email.db")
	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { repo.Close() })
	return repo
}

// TestSaveListRoundTrip verifies every field — the address, the credentials,
// and the cursor — survives storage verbatim, because the next listener
// session authenticates and resumes from exactly what it reads back.
func TestSaveListRoundTrip(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	withInbox := email.Email{
		ID:      "e1",
		Address: "drop@example.com",
		Inbox: &email.Inbox{
			Vendor: email.Gmail,
			Auth: email.Auth{
				Kind:     email.AuthPassword,
				Username: "drop@example.com",
				Password: "app-pass",
			},
			LastUID:     41,
			UIDValidity: 7,
		},
		CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC),
	}
	addressOnly := email.Email{ID: "e2", Address: "plus-tag@example.com"}
	for _, e := range []email.Email{withInbox, addressOnly} {
		if err := repo.Save(ctx, e); err != nil {
			t.Fatalf("save %s: %v", e.ID, err)
		}
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(listed) != 2 {
		t.Fatalf("listed %d emails, want 2", len(listed))
	}
	got := listed[0]
	if got.Address != withInbox.Address || got.Inbox == nil {
		t.Fatalf("e1 = %+v, want its inbox back", got)
	}
	if *got.Inbox != *withInbox.Inbox {
		t.Fatalf("inbox = %+v, want %+v", *got.Inbox, *withInbox.Inbox)
	}
	if !got.CreatedAt.Equal(withInbox.CreatedAt) || !got.UpdatedAt.Equal(withInbox.UpdatedAt) {
		t.Fatalf("times = %v/%v, want the stored ones", got.CreatedAt, got.UpdatedAt)
	}
	if listed[1].Inbox != nil {
		t.Fatalf("e2 inbox = %+v, want nil for an address-only email", listed[1].Inbox)
	}
}

// TestCursorUpsertPreservesCreatedAt verifies the cursor advance a listener
// makes on every batch cannot revise when the email was created.
func TestCursorUpsertPreservesCreatedAt(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	created := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	e := email.Email{
		ID:      "e1",
		Address: "drop@example.com",
		Inbox:   &email.Inbox{Vendor: email.Gmail, Auth: email.Auth{Kind: email.AuthPassword, Password: "p"}},
	}
	e.CreatedAt, e.UpdatedAt = created, created
	if err := repo.Save(ctx, e); err != nil {
		t.Fatalf("save: %v", err)
	}

	e.Inbox.LastUID, e.Inbox.UIDValidity = 99, 3
	e.CreatedAt = time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC) // must not win
	e.UpdatedAt = time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	if err := repo.Save(ctx, e); err != nil {
		t.Fatalf("resave: %v", err)
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	got := listed[0]
	if got.Inbox.LastUID != 99 || got.Inbox.UIDValidity != 3 {
		t.Fatalf("cursor = %d/%d, want 99/3", got.Inbox.LastUID, got.Inbox.UIDValidity)
	}
	if !got.CreatedAt.Equal(created) {
		t.Fatalf("created_at = %v, want the original %v", got.CreatedAt, created)
	}
}

// TestDeleteIsIdempotent verifies absent rows are a no-op, matching the
// repository contract every store here shares.
func TestDeleteIsIdempotent(t *testing.T) {
	repo := newTestRepo(t)
	ctx := context.Background()

	if err := repo.Save(ctx, email.Email{ID: "e1", Address: "a@example.com"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := repo.Delete(ctx, "e1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := repo.Delete(ctx, "e1"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	listed, err := repo.List(ctx)
	if err != nil || len(listed) != 0 {
		t.Fatalf("list = %v, %v; want empty", listed, err)
	}
}

// TestSharesADatabaseFileWithOtherStores verifies the "email" migration
// ledger coexists with another store's in one file, because consumers point
// every adapter at a single database.
func TestSharesADatabaseFileWithOtherStores(t *testing.T) {
	dsn := filepath.Join(t.TempDir(), "shared.db")
	other, err := accountsqlite.NewSQLite(dsn)
	if err != nil {
		t.Fatalf("accounts store: %v", err)
	}
	defer other.Close()

	repo, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("email store in the same file: %v", err)
	}
	defer repo.Close()

	if err := repo.Save(context.Background(), email.Email{ID: "e1", Address: "a@example.com"}); err != nil {
		t.Fatalf("save: %v", err)
	}
	reopened, err := NewSQLite(dsn)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	listed, err := reopened.List(context.Background())
	if err != nil || len(listed) != 1 {
		t.Fatalf("list after reopen = %v, %v; want the saved email", listed, err)
	}
}
