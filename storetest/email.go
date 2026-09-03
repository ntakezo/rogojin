package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

	// The listener claim admits one node at a time: first taker wins,
	// re-claiming your own succeeds, a live claim refuses others, an
	// expired one is taken over, and renewal keeps a claim alive — even a
	// renewal arriving after expiry, so long as no one else took it.
	t.Run("ListenerClaim", func(t *testing.T) {
		repo := open(t)
		seed := func(id string) {
			if err := repo.Save(ctx, email.Email{ID: id, Address: id + "@example.com",
				Inbox: &email.Inbox{Vendor: email.Gmail, Auth: email.Auth{Kind: email.AuthPassword, Password: "p"}}}); err != nil {
				t.Fatalf("Save %s: %v", id, err)
			}
		}
		seed("e1")

		if err := repo.ClaimListener(ctx, "missing", "n1", time.Minute); !errors.Is(err, email.ErrEmailNotFound) {
			t.Fatalf("claim of a missing email = %v, want ErrEmailNotFound", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n1", time.Minute); err != nil {
			t.Fatalf("first claim: %v", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n1", time.Minute); err != nil {
			t.Fatalf("re-claim by the holder: %v", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n2", time.Minute); !errors.Is(err, email.ErrListenerHeld) {
			t.Fatalf("claim over a live one = %v, want ErrListenerHeld", err)
		}
		if err := repo.RenewListener(ctx, "e1", "n2", time.Minute); !errors.Is(err, email.ErrListenerHeld) {
			t.Fatalf("renew by a non-holder = %v, want ErrListenerHeld", err)
		}
		if err := repo.ReleaseListener(ctx, "e1", "n2"); err != nil {
			t.Fatalf("release by a non-holder must no-op, got %v", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n2", time.Minute); !errors.Is(err, email.ErrListenerHeld) {
			t.Fatalf("the non-holder's release must not free the claim, got %v", err)
		}
		if err := repo.ReleaseListener(ctx, "e1", "n1"); err != nil {
			t.Fatalf("release by the holder: %v", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n2", time.Minute); err != nil {
			t.Fatalf("claim after release: %v", err)
		}
		if err := repo.RenewListener(ctx, "e1", "n2", time.Minute); err != nil {
			t.Fatalf("renew by the holder: %v", err)
		}

		// Renewal on an unclaimed inbox refuses: there is nothing to extend.
		seed("e2")
		if err := repo.RenewListener(ctx, "e2", "n1", time.Minute); !errors.Is(err, email.ErrListenerHeld) {
			t.Fatalf("renew of an unclaimed inbox = %v, want ErrListenerHeld", err)
		}
		if err := repo.RenewListener(ctx, "missing", "n1", time.Minute); !errors.Is(err, email.ErrEmailNotFound) {
			t.Fatalf("renew of a missing email = %v, want ErrEmailNotFound", err)
		}
		if err := repo.ReleaseListener(ctx, "missing", "n1"); !errors.Is(err, email.ErrEmailNotFound) {
			t.Fatalf("release of a missing email = %v, want ErrEmailNotFound", err)
		}
	})

	// An expired claim is another node's for the taking, and a renewal that
	// arrives late but unusurped still wins.
	t.Run("ListenerClaimExpiry", func(t *testing.T) {
		repo := open(t)
		if err := repo.Save(ctx, email.Email{ID: "e1", Address: "a@example.com"}); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n1", 20*time.Millisecond); err != nil {
			t.Fatalf("claim: %v", err)
		}
		time.Sleep(40 * time.Millisecond)
		if err := repo.RenewListener(ctx, "e1", "n1", time.Minute); err != nil {
			t.Fatalf("late unusurped renewal must win, got %v", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n2", time.Minute); !errors.Is(err, email.ErrListenerHeld) {
			t.Fatalf("the renewed claim must hold, got %v", err)
		}

		if err := repo.Save(ctx, email.Email{ID: "e2", Address: "b@example.com"}); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if err := repo.ClaimListener(ctx, "e2", "n1", 20*time.Millisecond); err != nil {
			t.Fatalf("claim: %v", err)
		}
		time.Sleep(40 * time.Millisecond)
		if err := repo.ClaimListener(ctx, "e2", "n2", time.Minute); err != nil {
			t.Fatalf("takeover of an expired claim: %v", err)
		}
		if err := repo.RenewListener(ctx, "e2", "n1", time.Minute); !errors.Is(err, email.ErrListenerHeld) {
			t.Fatalf("renew after usurpation = %v, want ErrListenerHeld", err)
		}
		if err := repo.ReleaseListener(ctx, "e2", "n1"); err != nil {
			t.Fatalf("release after usurpation must no-op, got %v", err)
		}
		if err := repo.RenewListener(ctx, "e2", "n2", time.Minute); err != nil {
			t.Fatalf("the usurper must keep its claim, got %v", err)
		}
	})

	// Racing claims admit exactly one node.
	t.Run("ListenerClaimRace", func(t *testing.T) {
		repo := open(t)
		if err := repo.Save(ctx, email.Email{ID: "e1", Address: "a@example.com"}); err != nil {
			t.Fatalf("Save: %v", err)
		}
		var wg sync.WaitGroup
		var won atomic.Int32
		for i := range 4 {
			node := fmt.Sprintf("n%d", i)
			wg.Go(func() {
				err := repo.ClaimListener(ctx, "e1", node, time.Minute)
				switch {
				case err == nil:
					won.Add(1)
				case !errors.Is(err, email.ErrListenerHeld):
					t.Errorf("claim by %s: %v", node, err)
				}
			})
		}
		wg.Wait()
		if won.Load() != 1 {
			t.Fatalf("%d nodes won the claim race, want exactly 1", won.Load())
		}
	})

	// The claim is store-internal: an inventory Save can neither clear it
	// nor stamp UpdatedAt through it — a heartbeat is not an inventory
	// change.
	t.Run("SaveCannotClobberAClaim", func(t *testing.T) {
		repo := open(t)
		created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
		e := email.Email{ID: "e1", Address: "a@example.com", CreatedAt: created, UpdatedAt: created}
		if err := repo.Save(ctx, e); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n1", time.Minute); err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err := repo.Save(ctx, e); err != nil {
			t.Fatalf("re-Save: %v", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n2", time.Minute); !errors.Is(err, email.ErrListenerHeld) {
			t.Fatalf("the claim must survive a Save, got %v", err)
		}
		listed, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if !listed[0].UpdatedAt.Equal(created) {
			t.Fatalf("UpdatedAt = %v; claiming must not stamp it", listed[0].UpdatedAt)
		}
	})

	// The cursor moves only under the holder's hand and only forward; a
	// changed validity is the reset that may move it back.
	t.Run("AdvanceCursor", func(t *testing.T) {
		repo := open(t)
		e := email.Email{ID: "e1", Address: "a@example.com",
			Inbox: &email.Inbox{Vendor: email.Gmail, Auth: email.Auth{Kind: email.AuthPassword, Password: "p"}, LastUID: 10, UIDValidity: 1}}
		if err := repo.Save(ctx, e); err != nil {
			t.Fatalf("Save: %v", err)
		}

		if err := repo.AdvanceCursor(ctx, "missing", "n1", 1, 11); !errors.Is(err, email.ErrEmailNotFound) {
			t.Fatalf("advance of a missing email = %v, want ErrEmailNotFound", err)
		}
		if err := repo.AdvanceCursor(ctx, "e1", "n1", 1, 11); !errors.Is(err, email.ErrListenerHeld) {
			t.Fatalf("advance without the claim = %v, want ErrListenerHeld", err)
		}
		if err := repo.ClaimListener(ctx, "e1", "n1", time.Minute); err != nil {
			t.Fatalf("claim: %v", err)
		}
		if err := repo.AdvanceCursor(ctx, "e1", "n2", 1, 11); !errors.Is(err, email.ErrListenerHeld) {
			t.Fatalf("advance by a non-holder = %v, want ErrListenerHeld", err)
		}

		cursor := func() (uint32, uint32, time.Time) {
			t.Helper()
			listed, err := repo.List(ctx)
			if err != nil || len(listed) == 0 || listed[0].Inbox == nil {
				t.Fatalf("List = %+v, err %v", listed, err)
			}
			return listed[0].Inbox.UIDValidity, listed[0].Inbox.LastUID, listed[0].UpdatedAt
		}

		if err := repo.AdvanceCursor(ctx, "e1", "n1", 1, 42); err != nil {
			t.Fatalf("forward advance: %v", err)
		}
		validity, uid, stamped := cursor()
		if validity != 1 || uid != 42 {
			t.Fatalf("cursor = %d/%d, want 1/42", validity, uid)
		}

		// Late duplicates — equal or lower UID under the same validity —
		// no-op silently and leave the record untouched.
		for _, stale := range []uint32{42, 7} {
			if err := repo.AdvanceCursor(ctx, "e1", "n1", 1, stale); err != nil {
				t.Fatalf("stale advance to %d: %v", stale, err)
			}
		}
		if validity, uid, after := cursor(); validity != 1 || uid != 42 || !after.Equal(stamped) {
			t.Fatalf("cursor = %d/%d stamped %v; a stale advance must change nothing (was 1/42 %v)",
				validity, uid, after, stamped)
		}

		// A renumbered mailbox resets: the new validity lands even with a
		// lower UID.
		if err := repo.AdvanceCursor(ctx, "e1", "n1", 2, 3); err != nil {
			t.Fatalf("validity reset: %v", err)
		}
		if validity, uid, _ := cursor(); validity != 2 || uid != 3 {
			t.Fatalf("cursor = %d/%d, want the reset 2/3", validity, uid)
		}
	})

	// Concurrent writers on distinct ids interleave without losing
	// records.
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
