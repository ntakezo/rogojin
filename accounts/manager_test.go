package accounts

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
	records map[string]Account
	groups  map[string]Group
}

func newFakeRepo(seed ...Account) *fakeRepo {
	r := &fakeRepo{records: map[string]Account{}, groups: map[string]Group{}}
	for _, a := range seed {
		r.records[a.ID] = a
		r.order = append(r.order, a.ID)
	}
	return r
}

func (r *fakeRepo) List(ctx context.Context) ([]Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Account, 0, len(r.order))
	for _, id := range r.order {
		if a, ok := r.records[id]; ok {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *fakeRepo) Save(ctx context.Context, a Account) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.records[a.ID]; !ok {
		r.order = append(r.order, a.ID)
	}
	r.records[a.ID] = a
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

// profile is a workflow-shaped view of an account's fields, standing in for
// whatever a real workflow defines.
type profile struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// TestFieldsTravelOpaquelyAndBindDecodes verifies the payload this package
// exists for: fields pass through the manager untouched, and Bind decodes them
// into the workflow's own shape at the point of use.
func TestFieldsTravelOpaquelyAndBindDecodes(t *testing.T) {
	raw := json.RawMessage(`{"email":"a@b.c","password":"hunter2"}`)
	repo := newFakeRepo(Account{ID: "a1", Attrs: raw})
	ctx := context.Background()

	m, err := NewManager(ctx, repo)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	lease, err := m.Lock(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("Lock: %v", err)
	}
	got, err := Bind[profile](lease.Resource())
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if got.Email != "a@b.c" || got.Password != "hunter2" {
		t.Fatalf("bound = %+v, want the seeded fields", got)
	}
	if err := lease.Release(true); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if string(repo.records["a1"].Attrs) != string(raw) {
		t.Fatalf("persisted fields = %s, want untouched", repo.records["a1"].Attrs)
	}
}

// TestBindToleratesAccountsWithoutFields verifies an account with no fields
// binds to the zero value rather than erroring, and garbage reports the
// account it belongs to.
func TestBindToleratesAccountsWithoutFields(t *testing.T) {
	got, err := Bind[profile](Account{ID: "bare"})
	if err != nil {
		t.Fatalf("Bind of empty fields: %v", err)
	}
	if got != (profile{}) {
		t.Fatalf("bound = %+v, want the zero profile", got)
	}

	if _, err := Bind[profile](Account{ID: "a9", Attrs: json.RawMessage(`{"email":`)}); err == nil {
		t.Fatal("expected an error for malformed fields")
	}
}

// TestAccountsRotateWithNoConfiguration verifies the strategy-less default:
// two tasks drawing from the pool get different accounts, with nothing
// configured but the repository.
func TestAccountsRotateWithNoConfiguration(t *testing.T) {
	repo := newFakeRepo(Account{ID: "a1"}, Account{ID: "a2"})
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
