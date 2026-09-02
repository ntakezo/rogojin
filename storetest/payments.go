package storetest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/payments"
)

// Payments exercises the payments.Repository contract: the shared leasing
// record behavior plus the model's own column, the opaque fields payload.
func Payments(t *testing.T, open func(t *testing.T) payments.Repository) {
	ctx := context.Background()

	t.Run("Leasing", func(t *testing.T) {
		Leasing(t, open,
			func(id string) payments.Payment {
				return payments.Payment{Resource: leasing.Resource{ID: id}}
			},
			func(p *payments.Payment) *leasing.Resource { return &p.Resource })
	})

	// Any JSON shape stores verbatim — lock reclamation and payment both
	// read it back — and an absent payload reads back nil.
	t.Run("FieldsAreOpaque", func(t *testing.T) {
		repo := open(t)
		deep := mustJSON(t, map[string]any{"billing": map[string]string{"zip": "10001"}, "tokens": []int{1, 2}})
		if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1"}, Fields: deep}); err != nil {
			t.Fatalf("Save: %v", err)
		}
		if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c2"}}); err != nil {
			t.Fatalf("Save without fields: %v", err)
		}

		listed, _ := repo.List(ctx)
		if string(listed[0].Fields) != string(deep) {
			t.Fatalf("fields = %s, want %s verbatim", listed[0].Fields, deep)
		}
		if listed[1].Fields != nil {
			t.Fatalf("absent fields = %s, want nil", listed[1].Fields)
		}
	})

	// Garbage is refused at the write, not surfaced as a decode failure
	// inside a later run, and the refusal stores nothing.
	t.Run("FieldsMustBeJSON", func(t *testing.T) {
		repo := open(t)
		err := repo.Save(ctx, payments.Payment{
			Resource: leasing.Resource{ID: "c1"},
			Fields:   json.RawMessage(`{"broken`),
		})
		if err == nil {
			t.Fatal("Save: want invalid JSON refused, got nil")
		}
		if listed, _ := repo.List(ctx); len(listed) != 0 {
			t.Fatalf("refused save still stored %d records", len(listed))
		}
	})

	// The caller's fields buffer and the store's never alias.
	t.Run("FieldsDoNotAlias", func(t *testing.T) {
		repo := open(t)
		fields := mustJSON(t, map[string]string{"number": "4111"})
		if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1"}, Fields: fields}); err != nil {
			t.Fatalf("Save: %v", err)
		}
		fields[2] = 'X'

		listed, _ := repo.List(ctx)
		var decoded map[string]string
		if err := json.Unmarshal(listed[0].Fields, &decoded); err != nil || decoded["number"] != "4111" {
			t.Fatalf("stored fields mutated through the caller: %s (err %v)", listed[0].Fields, err)
		}

		listed[0].Fields[2] = 'Y'
		again, _ := repo.List(ctx)
		if err := json.Unmarshal(again[0].Fields, &decoded); err != nil || decoded["number"] != "4111" {
			t.Fatalf("listed fields share memory with the store: %s (err %v)", again[0].Fields, err)
		}
	})
}
