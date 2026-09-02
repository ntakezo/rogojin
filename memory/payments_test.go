package memory

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/payments"
)

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

// TestPaymentsSaveListRoundTrip verifies a payment round-trips whole — group,
// policy, lock owner, opaque fields — and lists in stable id order.
func TestPaymentsSaveListRoundTrip(t *testing.T) {
	repo := NewPayments()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	for _, id := range []string{"visa-2", "visa-1"} {
		c := payments.Payment{
			Resource: leasing.Resource{ID: id, GroupID: "cards", OwnerID: "t1", MaxHolders: 1, CreatedAt: now, UpdatedAt: now},
			Fields:   mustJSON(t, map[string]string{"number": "4111", "cvv": "123"}),
		}
		if err := repo.Save(ctx, c); err != nil {
			t.Fatalf("Save %s: %v", id, err)
		}
	}

	listed, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listed) != 2 || listed[0].ID != "visa-1" || listed[1].ID != "visa-2" {
		t.Fatalf("got %+v, want visa-1 then visa-2", listed)
	}
	got := listed[0]
	if got.GroupID != "cards" || got.OwnerID != "t1" || got.MaxHolders != 1 {
		t.Fatalf("record = %+v", got)
	}
	var fields map[string]string
	if err := json.Unmarshal(got.Fields, &fields); err != nil || fields["number"] != "4111" {
		t.Fatalf("fields = %s (err %v), want number 4111", got.Fields, err)
	}
	if !got.CreatedAt.Equal(now) || !got.UpdatedAt.Equal(now) {
		t.Fatalf("timestamps = %v/%v, want %v", got.CreatedAt, got.UpdatedAt, now)
	}
}

// TestPaymentsFieldsAreOpaque verifies any JSON shape stores verbatim and an
// absent payload reads back nil.
func TestPaymentsFieldsAreOpaque(t *testing.T) {
	repo := NewPayments()
	ctx := context.Background()

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
}

// TestPaymentsSaveRejectsFieldsThatAreNotJSON verifies garbage is refused at
// the write, not surfaced as a decode failure later.
func TestPaymentsSaveRejectsFieldsThatAreNotJSON(t *testing.T) {
	repo := NewPayments()
	err := repo.Save(context.Background(), payments.Payment{
		Resource: leasing.Resource{ID: "c1"},
		Fields:   json.RawMessage(`{"broken`),
	})
	if err == nil {
		t.Fatal("Save: want invalid JSON refused, got nil")
	}
	if listed, _ := repo.List(context.Background()); len(listed) != 0 {
		t.Fatalf("refused save still stored %d records", len(listed))
	}
}

// TestPaymentsSavePreservesCreatedAt verifies an upsert never revises when
// the record was created.
func TestPaymentsSavePreservesCreatedAt(t *testing.T) {
	repo := NewPayments()
	ctx := context.Background()

	created := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	c := payments.Payment{Resource: leasing.Resource{ID: "c1", CreatedAt: created, UpdatedAt: created}}
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("Save: %v", err)
	}

	later := time.Now().UTC().Truncate(time.Millisecond)
	c.OwnerID, c.CreatedAt, c.UpdatedAt = "t1", later, later
	if err := repo.Save(ctx, c); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	listed, _ := repo.List(ctx)
	if !listed[0].CreatedAt.Equal(created) {
		t.Fatalf("CreatedAt = %v, want the original %v", listed[0].CreatedAt, created)
	}
	if !listed[0].UpdatedAt.Equal(later) || listed[0].OwnerID != "t1" {
		t.Fatalf("upsert did not land: %+v", listed[0])
	}
}

// TestPaymentsDeleteIsIdempotent verifies deletes remove the record and
// absent ids are a no-op.
func TestPaymentsDeleteIsIdempotent(t *testing.T) {
	repo := NewPayments()
	ctx := context.Background()

	if err := repo.Save(ctx, payments.Payment{Resource: leasing.Resource{ID: "c1"}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.Delete(ctx, "c1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := repo.Delete(ctx, "c1"); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	if listed, _ := repo.List(ctx); len(listed) != 0 {
		t.Fatalf("record survived delete: %+v", listed)
	}
}

// TestPaymentsGroupRoundTrip verifies groups round-trip with strategy and
// refs, preserve CreatedAt across upserts, and delete idempotently.
func TestPaymentsGroupRoundTrip(t *testing.T) {
	repo := NewPayments()
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Millisecond)
	g := payments.Group{ID: "cards", Strategy: "roundrobin", Refs: map[string]string{"note": "usd"}, CreatedAt: now, UpdatedAt: now}
	if err := repo.SaveGroup(ctx, g); err != nil {
		t.Fatalf("SaveGroup: %v", err)
	}

	g.Strategy, g.CreatedAt = "other", now.Add(time.Hour)
	if err := repo.SaveGroup(ctx, g); err != nil {
		t.Fatalf("second SaveGroup: %v", err)
	}

	listed, err := repo.ListGroups(ctx)
	if err != nil || len(listed) != 1 {
		t.Fatalf("ListGroups = %d, err %v; want 1, nil", len(listed), err)
	}
	got := listed[0]
	if got.Strategy != "other" || got.Refs["note"] != "usd" || !got.CreatedAt.Equal(now) {
		t.Fatalf("group = %+v, want other/usd with CreatedAt %v kept", got, now)
	}

	if err := repo.DeleteGroup(ctx, "cards"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	if err := repo.DeleteGroup(ctx, "cards"); err != nil {
		t.Fatalf("second DeleteGroup: %v", err)
	}
	if listed, _ := repo.ListGroups(ctx); len(listed) != 0 {
		t.Fatalf("group survived delete: %+v", listed)
	}
}

// TestPaymentsCopiesAtTheBoundary verifies the caller's fields buffer and the
// store's never alias.
func TestPaymentsCopiesAtTheBoundary(t *testing.T) {
	repo := NewPayments()
	ctx := context.Background()

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
}
