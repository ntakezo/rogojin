package storetest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/leasing"
)

// Accounts exercises the accounts.Repository contract: the shared leasing
// record behavior plus the model's own columns — the forwarding email and
// the opaque fields payload.
func Accounts(t *testing.T, open func(t *testing.T) accounts.Repository) {
	ctx := context.Background()

	t.Run("Leasing", func(t *testing.T) {
		Leasing(t, open,
			func(id string) accounts.Account {
				return accounts.Account{Resource: leasing.Resource{ID: id}}
			},
			func(a *accounts.Account) *leasing.Resource { return &a.Resource })
	})

	// The forwarding email and any JSON shape of fields survive storage;
	// consumers read fields back through accounts.Bind.
	t.Run("ModelFieldsRoundTrip", func(t *testing.T) {
		repo := open(t)
		type checkout struct {
			Email string `json:"email"`
			Card  string `json:"card"`
		}
		want := checkout{Email: "buyer@example.com", Card: "4111"}
		a := accounts.Account{Resource: leasing.Resource{ID: "a1"}, EmailID: "inbox-1", Fields: mustJSON(t, want)}
		if _, err := repo.Save(ctx, a); err != nil {
			t.Fatalf("Save: %v", err)
		}

		listed, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if listed[0].EmailID != "inbox-1" {
			t.Fatalf("EmailID = %q, want inbox-1", listed[0].EmailID)
		}
		got, err := accounts.Bind[checkout](listed[0])
		if err != nil {
			t.Fatalf("Bind: %v", err)
		}
		if got != want {
			t.Fatalf("fields = %+v, want %+v", got, want)
		}
	})

	// Garbage fields are refused at the write, and the refusal stores
	// nothing.
	t.Run("FieldsMustBeJSON", func(t *testing.T) {
		repo := open(t)
		_, err := repo.Save(ctx, accounts.Account{
			Resource: leasing.Resource{ID: "a1"},
			Fields:   json.RawMessage(`not json`),
		})
		if err == nil {
			t.Fatal("Save: want invalid JSON refused, got nil")
		}
		if listed, _ := repo.List(ctx); len(listed) != 0 {
			t.Fatalf("refused save still stored %d records", len(listed))
		}
	})
}
