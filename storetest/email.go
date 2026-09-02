package storetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/email"
)

// Emails exercises the email.Repository contract against the store the
// factory opens.
func Emails(t *testing.T, open func(t *testing.T) email.Repository) {
	ctx := context.Background()

	// An email round-trips whole — address, inbox credentials, cursor —
	// in stable id order, and an address-only email reads back with no
	// inbox.
	t.Run("SaveListRoundTrip", func(t *testing.T) {
		repo := open(t)
		now := time.Now().UTC().Truncate(time.Millisecond)
		withInbox := email.Email{
			ID: "e1", Address: "orders@example.com", CreatedAt: now, UpdatedAt: now,
			Inbox: &email.Inbox{
				Vendor:      email.Gmail,
				Auth:        email.Auth{Kind: email.AuthPassword, Password: "app-password"},
				LastUID:     42,
				UIDValidity: 7,
			},
		}
		addressOnly := email.Email{ID: "e0", Address: "alias@example.com", CreatedAt: now, UpdatedAt: now}
		for _, e := range []email.Email{withInbox, addressOnly} {
			if err := repo.Save(ctx, e); err != nil {
				t.Fatalf("Save %s: %v", e.ID, err)
			}
		}

		listed, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(listed) != 2 || listed[0].ID != "e0" || listed[1].ID != "e1" {
			t.Fatalf("got %+v, want e0 then e1", listed)
		}
		if listed[0].Inbox != nil {
			t.Fatalf("address-only email read back an inbox: %+v", listed[0].Inbox)
		}
		got := listed[1]
		if got.Address != "orders@example.com" || got.Inbox == nil {
			t.Fatalf("record = %+v", got)
		}
		if got.Inbox.Vendor != email.Gmail || got.Inbox.Auth.Password != "app-password" {
			t.Fatalf("inbox = %+v", got.Inbox)
		}
		if got.Inbox.LastUID != 42 || got.Inbox.UIDValidity != 7 {
			t.Fatalf("cursor = %d/%d, want 42/7", got.Inbox.LastUID, got.Inbox.UIDValidity)
		}
		if !got.CreatedAt.Equal(now) || !got.UpdatedAt.Equal(now) {
			t.Fatalf("timestamps = %v/%v, want %v", got.CreatedAt, got.UpdatedAt, now)
		}
	})

	// An inbox without a vendor stores as no inbox at all — the
	// address-only marker the columns produce.
	t.Run("VendorlessInboxStoresAsNone", func(t *testing.T) {
		repo := open(t)
		e := email.Email{ID: "e1", Address: "a@example.com", Inbox: &email.Inbox{LastUID: 9}}
		if err := repo.Save(ctx, e); err != nil {
			t.Fatalf("Save: %v", err)
		}
		listed, _ := repo.List(ctx)
		if listed[0].Inbox != nil {
			t.Fatalf("vendor-less inbox survived storage: %+v", listed[0].Inbox)
		}
	})

	// A cursor advance rewrites the record without revising when it was
	// created.
	t.Run("CursorUpsertPreservesCreatedAt", func(t *testing.T) {
		repo := open(t)
		created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
		e := email.Email{
			ID: "e1", Address: "orders@example.com", CreatedAt: created, UpdatedAt: created,
			Inbox: &email.Inbox{Vendor: email.Gmail, Auth: email.Auth{Kind: email.AuthPassword, Password: "p"}},
		}
		if err := repo.Save(ctx, e); err != nil {
			t.Fatalf("Save: %v", err)
		}

		later := time.Now().UTC().Truncate(time.Millisecond)
		e.Inbox.LastUID, e.Inbox.UIDValidity = 99, 3
		e.CreatedAt, e.UpdatedAt = later, later
		if err := repo.Save(ctx, e); err != nil {
			t.Fatalf("cursor Save: %v", err)
		}

		listed, _ := repo.List(ctx)
		got := listed[0]
		if got.Inbox.LastUID != 99 || got.Inbox.UIDValidity != 3 {
			t.Fatalf("cursor = %d/%d, want 99/3", got.Inbox.LastUID, got.Inbox.UIDValidity)
		}
		if !got.CreatedAt.Equal(created) {
			t.Fatalf("CreatedAt = %v, want the original %v", got.CreatedAt, created)
		}
		if !got.UpdatedAt.Equal(later) {
			t.Fatalf("UpdatedAt = %v, want %v", got.UpdatedAt, later)
		}
	})

	// Deletes remove the record and absent ids are a no-op.
	t.Run("DeleteIsIdempotent", func(t *testing.T) {
		repo := open(t)
		if err := repo.Save(ctx, email.Email{ID: "e1", Address: "a@example.com"}); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if err := repo.Delete(ctx, "e1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if err := repo.Delete(ctx, "e1"); err != nil {
			t.Fatalf("second Delete: %v", err)
		}
		if listed, _ := repo.List(ctx); len(listed) != 0 {
			t.Fatalf("record survived delete: %+v", listed)
		}
	})

	// The caller's inbox pointer and the store's never share memory.
	t.Run("InboxDoesNotAlias", func(t *testing.T) {
		repo := open(t)
		in := &email.Inbox{Vendor: email.Gmail, Auth: email.Auth{Kind: email.AuthPassword, Password: "p"}}
		if err := repo.Save(ctx, email.Email{ID: "e1", Address: "a@example.com", Inbox: in}); err != nil {
			t.Fatalf("Save: %v", err)
		}
		in.LastUID = 1000

		listed, _ := repo.List(ctx)
		if listed[0].Inbox.LastUID != 0 {
			t.Fatalf("saved inbox mutated through the caller: %+v", listed[0].Inbox)
		}
		listed[0].Inbox.Auth.Password = "leaked"
		again, _ := repo.List(ctx)
		if again[0].Inbox.Auth.Password != "p" {
			t.Fatalf("listed inbox shares memory with the store: %+v", again[0].Inbox)
		}
	})

	// Concurrent writers on distinct ids interleave without losing
	// records. Listener-claim and cursor races join this section as the
	// port grows those primitives.
	t.Run("ConcurrentUse", func(t *testing.T) {
		repo := open(t)
		ids := []string{"c1", "c2", "c3", "c4"}
		var wg sync.WaitGroup
		for _, id := range ids {
			wg.Go(func() {
				if err := repo.Save(ctx, email.Email{ID: id, Address: id + "@example.com"}); err != nil {
					t.Errorf("Save %s: %v", id, err)
					return
				}
				if _, err := repo.List(ctx); err != nil {
					t.Errorf("List: %v", err)
				}
			})
		}
		wg.Wait()

		listed, err := repo.List(ctx)
		if err != nil || len(listed) != len(ids) {
			t.Fatalf("List = %d records, err %v; want all %d", len(listed), err, len(ids))
		}
	})
}
