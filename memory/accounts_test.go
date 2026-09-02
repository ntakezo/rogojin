package memory

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/leasing"
)

// TestAccountsSaveListRoundTrip verifies an account round-trips whole —
// group, policy, lock owner, forwarding email, opaque fields — and lists in
// stable id order.
func TestAccountsSaveListRoundTrip(t *testing.T) {
	repo := NewAccounts()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, id := range []string{"buyer-2", "buyer-1"} {
		a := accounts.Account{
			Resource: leasing.Resource{ID: id, GroupID: "shoppers", OwnerID: "t1", MaxHolders: 1, CreatedAt: now, UpdatedAt: now},
			EmailID:  "inbox-1",
			Fields:   json.RawMessage(`{"username":"u","password":"p"}`),
		}
		if err := repo.Save(ctx, a); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 2 || listed[0].ID != "buyer-1" || listed[1].ID != "buyer-2" {
		t.Fatalf("got %+v, want buyer-1 then buyer-2", listed)
	}
	got := listed[0]
	if got.GroupID != "shoppers" || got.OwnerID != "t1" || got.EmailID != "inbox-1" {
		t.Fatalf("record = %+v", got)
	}
	if string(got.Fields) != `{"username":"u","password":"p"}` {
		t.Fatalf("fields = %s, want them verbatim", got.Fields)
	}
	if !got.CreatedAt.Equal(now) {
		t.Fatalf("CreatedAt = %v, want %v", got.CreatedAt, now)
	}
}

// TestAccountsSaveRejectsFieldsThatAreNotJSON verifies garbage is refused at
// the write.
func TestAccountsSaveRejectsFieldsThatAreNotJSON(t *testing.T) {
	repo := NewAccounts()
	err := repo.Save(context.Background(), accounts.Account{
		Resource: leasing.Resource{ID: "a1"},
		Fields:   json.RawMessage(`not json`),
	})
	if err == nil {
		t.Fatal("Save: want invalid JSON refused, got nil")
	}
	if listed, _ := repo.List(context.Background()); len(listed) != 0 {
		t.Fatalf("refused save still stored %d records", len(listed))
	}
}

// TestAccountsSavePreservesCreatedAt verifies an upsert never revises when
// the record was created, while everything else lands.
func TestAccountsSavePreservesCreatedAt(t *testing.T) {
	repo := NewAccounts()
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	a := accounts.Account{Resource: leasing.Resource{ID: "a1", CreatedAt: created, UpdatedAt: created}}
	if err := repo.Save(ctx, a); err != nil {
		t.Fatalf("Save: %v", err)
	}

	later := time.Now().UTC().Truncate(time.Millisecond)
	a.OwnerID, a.EmailID, a.CreatedAt, a.UpdatedAt = "t1", "inbox-1", later, later
	if err := repo.Save(ctx, a); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	listed, _ := repo.List(ctx)
	if !listed[0].CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt = %v, want the original %v", listed[0].CreatedAt, created)
	}
	if listed[0].OwnerID != "t1" || listed[0].EmailID != "inbox-1" {
		t.Fatalf("upsert did not land: %+v", listed[0])
	}
}

// TestAccountsDeleteIsIdempotent verifies deletes remove the record and
// absent ids are a no-op.
func TestAccountsDeleteIsIdempotent(t *testing.T) {
	repo := NewAccounts()
	ctx := context.Background()

	if err := repo.Save(ctx, accounts.Account{Resource: leasing.Resource{ID: "a1"}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.Delete(ctx, "a1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := repo.Delete(ctx, "a1"); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if listed, _ := repo.List(ctx); len(listed) != 0 {
		t.Fatalf("record survived delete: %+v", listed)
	}
}

// TestAccountsGroupRoundTrip verifies groups round-trip and preserve
// CreatedAt across upserts.
func TestAccountsGroupRoundTrip(t *testing.T) {
	repo := NewAccounts()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	g := accounts.Group{ID: "shoppers", Strategy: "roundrobin", Refs: map[string]string{"site": "example"}, CreatedAt: now, UpdatedAt: now}
	if err := repo.SaveGroup(ctx, g); err != nil {
		t.Fatalf("SaveGroup: %v", err)
	}

	g.CreatedAt = now.Add(time.Hour)
	if err := repo.SaveGroup(ctx, g); err != nil {
		t.Fatalf("second SaveGroup: %v", err)
	}

	listed, err := repo.ListGroups(ctx)
	if err != nil || len(listed) != 1 {
		t.Fatalf("ListGroups = %d, err %v; want 1, nil", len(listed), err)
	}
	if listed[0].Refs["site"] != "example" || !listed[0].CreatedAt.Equal(now) {
		t.Fatalf("group = %+v, want refs kept and CreatedAt %v", listed[0], now)
	}

	if err := repo.DeleteGroup(ctx, "shoppers"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	if listed, _ := repo.ListGroups(ctx); len(listed) != 0 {
		t.Fatalf("group survived delete: %+v", listed)
	}
}
