package cards

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/ntakezo/rogojin/leasing"
)

// These tests cover what this package adds to the leasing core: the opaque
// JSON payload and Bind. The leasing behavior itself — groups, caps, locks,
// pins, deletes — is covered where it lives.

// fakeRepo is a minimal in-memory Repository.
type fakeRepo struct {
	mu      sync.Mutex
	order   []string
	records map[string]Card
	groups  map[string]Group
}

func newFakeRepo(seed ...Card) *fakeRepo {
	r := &fakeRepo{records: map[string]Card{}, groups: map[string]Group{}}
	for _, c := range seed {
		r.records[c.ID] = c
		r.order = append(r.order, c.ID)
	}
	return r
}

func (r *fakeRepo) List(ctx context.Context) ([]Card, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Card, 0, len(r.order))
	for _, id := range r.order {
		if c, ok := r.records[id]; ok {
			out = append(out, c)
		}
	}
	return out, nil
}

func (r *fakeRepo) Save(ctx context.Context, c Card) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.records[c.ID]; !ok {
		r.order = append(r.order, c.ID)
	}
	r.records[c.ID] = c
	return nil
}

func (r *fakeRepo) Delete(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, id)
	return nil
}

func (r *fakeRepo) ListGroups(ctx context.Context) ([]Group, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Group, 0, len(r.groups))
	for _, g := range r.groups {
		out = append(out, g)
	}
	return out, nil
}

func (r *fakeRepo) SaveGroup(ctx context.Context, g Group) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.groups[g.ID] = g
	return nil
}

func (r *fakeRepo) DeleteGroup(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.groups, id)
	return nil
}

// instrument is a workflow-shaped view of a card's fields, standing in for
// whatever a real checkout defines.
type instrument struct {
	Number string `json:"number"`
	CVV    string `json:"cvv"`
}

// TestFieldsTravelOpaquelyAndBindDecodes verifies the payload this package
// exists for: fields pass through the manager untouched, and Bind decodes them
// into the workflow's own shape at the point of use.
func TestFieldsTravelOpaquelyAndBindDecodes(t *testing.T) {
	raw := json.RawMessage(`{"number":"4111111111111111","cvv":"123"}`)
	repo := newFakeRepo(Card{ID: "c1", Attrs: raw})
	ctx := context.Background()

	m, err := NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	got, err := Bind[instrument](lease.Resource())
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if got.Number != "4111111111111111" || got.CVV != "123" {
		t.Fatalf("bound = %+v, want the seeded fields", got)
	}
	if err := lease.Release(true); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if string(repo.records["c1"].Attrs) != string(raw) {
		t.Fatalf("persisted fields = %s, want untouched", repo.records["c1"].Attrs)
	}
}

// TestBindToleratesCardsWithoutFields verifies a card with no fields binds to
// the zero value rather than erroring, and garbage reports the card it
// belongs to.
func TestBindToleratesCardsWithoutFields(t *testing.T) {
	got, err := Bind[instrument](Card{ID: "bare"})
	if err != nil {
		t.Fatalf("Bind of empty fields: %v", err)
	}
	if got != (instrument{}) {
		t.Fatalf("bound = %+v, want the zero instrument", got)
	}

	if _, err := Bind[instrument](Card{ID: "c9", Attrs: json.RawMessage(`{"number":`)}); err == nil {
		t.Fatal("expected an error for malformed fields")
	}
}

// TestCardsRotateWithNoConfiguration verifies the strategy-less default: two
// tasks drawing from the pool get different cards, with nothing configured but
// the repository.
func TestCardsRotateWithNoConfiguration(t *testing.T) {
	repo := newFakeRepo(Card{ID: "c1"}, Card{ID: "c2"})
	ctx := context.Background()

	m, err := NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	first, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	second, err := m.Acquire(ctx, Assignment{TaskID: "t2"})
	if err != nil {
		t.Fatalf("second Acquire: %v", err)
	}
	if first.Resource().ID == second.Resource().ID {
		t.Fatalf("both tasks got %s, want one each", first.Resource().ID)
	}
}

// TestManagerSatisfiesTasksContract verifies the alias really is the leasing
// manager: the methods a task service drives are present without adapters.
func TestManagerSatisfiesTasksContract(t *testing.T) {
	m, err := NewManager(context.Background(), newFakeRepo())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	var _ interface {
		Unlock(ctx context.Context, taskID string) error
		ReleaseStaleLock(ctx context.Context, a leasing.Assignment) error
	} = m
}
