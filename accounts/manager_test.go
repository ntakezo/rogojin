package accounts

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ntakezo/rogojin/leasing"
)

// The leasing behaviour this package inherits — pooling, groups, holder caps,
// durable locks, pins, deletion policies, the usage guard — is covered once in
// the leasing package, against the code that implements it. What is tested here
// is what accounts add: the workflow-defined JSON payload, the absent rotation
// knob, and the translation between this package's shapes and the core's.

// fakeRepo is an in-memory Repository recording what the manager stores, so a
// test can assert the consumer's store is handed account-shaped records.
type fakeRepo struct {
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
	out := make([]Account, 0, len(r.order))
	for _, id := range r.order {
		if a, ok := r.records[id]; ok {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *fakeRepo) Save(ctx context.Context, a Account) error {
	if _, ok := r.records[a.ID]; !ok {
		r.order = append(r.order, a.ID)
	}
	r.records[a.ID] = a
	return nil
}

func (r *fakeRepo) Delete(ctx context.Context, id string) error {
	delete(r.records, id)
	return nil
}

func (r *fakeRepo) ListGroups(ctx context.Context) ([]Group, error) {
	out := make([]Group, 0, len(r.groups))
	for _, g := range r.groups {
		out = append(out, g)
	}
	return out, nil
}

func (r *fakeRepo) SaveGroup(ctx context.Context, g Group) error {
	r.groups[g.ID] = g
	return nil
}

func (r *fakeRepo) DeleteGroup(ctx context.Context, id string) error {
	delete(r.groups, id)
	return nil
}

type creds struct {
	Email     string `json:"email"`
	Password  string `json:"password"`
	FirstName string `json:"firstName"`
}

func fields(t *testing.T, c creds) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal creds: %v", err)
	}
	return raw
}

func newTestManager(t *testing.T, repo Repository, policy DeletionPolicy, opts ...ManagerOption) *Manager {
	t.Helper()
	m, err := NewManager(context.Background(), repo, policy, opts...)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

// TestFieldsRoundTripThroughTheStore verifies the whole point of the JSON
// payload: a workflow defines its own shape, the store never learns it, and
// Bind hands it back typed on the far side of a restart.
func TestFieldsRoundTripThroughTheStore(t *testing.T) {
	want := creds{Email: "buyer@example.com", Password: "hunter2", FirstName: "Ada"}
	repo := newFakeRepo()
	m := newTestManager(t, repo, nil)
	ctx := context.Background()

	if err := m.AddAccount(ctx, Account{ID: "a1", Fields: fields(t, want)}); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	if stored := repo.records["a1"]; string(stored.Fields) != string(fields(t, want)) {
		t.Fatalf("stored fields = %s, want the JSON handed in", stored.Fields)
	}

	restarted := newTestManager(t, repo, nil)
	lease, err := restarted.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	got, err := Bind[creds](lease.Account())
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if got != want {
		t.Fatalf("fields = %+v, want %+v", got, want)
	}
}

// TestBindToleratesAccountsWithoutFields verifies a workflow that needs no
// credentials is not forced to store an empty object, and that a payload that
// is not what the caller expects is reported rather than silently zeroed.
func TestBindToleratesAccountsWithoutFields(t *testing.T) {
	got, err := Bind[creds](Account{ID: "a1"})
	if err != nil {
		t.Fatalf("Bind on an account with no fields: %v", err)
	}
	if got != (creds{}) {
		t.Fatalf("fields = %+v, want the zero value", got)
	}

	if _, err := Bind[creds](Account{ID: "a1", Fields: json.RawMessage(`["not","an","object"]`)}); err == nil {
		t.Fatal("expected a mismatched payload to be reported")
	}
}

// TestLeasedFieldsAreACopy verifies a holder editing the credentials it was
// handed cannot corrupt the pool every other task draws from. The core copies
// resources by value, so without an explicit clone the backing array is shared.
func TestLeasedFieldsAreACopy(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo, nil)
	ctx := context.Background()

	original := fields(t, creds{Email: "buyer@example.com"})
	if err := m.AddAccount(ctx, Account{ID: "a1", Fields: original}); err != nil {
		t.Fatalf("AddAccount: %v", err)
	}
	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	leased := lease.Account().Fields
	leased[0] = 'X'
	if err := lease.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}

	// The next holder is the proof: if the edit had reached the pool, this is
	// where it would come back out.
	next, err := m.Acquire(ctx, Assignment{TaskID: "t2"})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if string(next.Account().Fields) != string(original) {
		t.Fatalf("pool fields = %s, want %s", next.Account().Fields, original)
	}
	if stored := repo.records["a1"]; string(stored.Fields) != string(original) {
		t.Fatalf("stored fields = %s, want %s", stored.Fields, original)
	}
}

// TestGroupsCarryNoStrategy verifies the one way accounts differ from proxies:
// there is no rotation knob on the API or in the store, and the manager still
// rotates fairly without one — including after a restart, where the group comes
// back with no strategy recorded and must resolve one anyway.
func TestGroupsCarryNoStrategy(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo, nil)
	ctx := context.Background()

	if err := m.CreateGroup(ctx, Group{ID: "site", MaxHolders: 1}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	for _, id := range []string{"a1", "a2"} {
		if err := m.AddAccount(ctx, Account{ID: id, GroupID: "site"}); err != nil {
			t.Fatalf("AddAccount %s: %v", id, err)
		}
	}
	if _, ok := repo.groups["site"]; !ok {
		t.Fatal("the group was not persisted")
	}

	restarted := newTestManager(t, repo, nil)
	first, err := restarted.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "site"})
	if err != nil {
		t.Fatalf("first acquire after restart: %v", err)
	}
	second, err := restarted.Acquire(ctx, Assignment{TaskID: "t2", GroupID: "site"})
	if err != nil {
		t.Fatalf("second acquire after restart: %v", err)
	}
	if first.Account().ID == second.Account().ID {
		t.Fatalf("both tasks got %s, want rotation", first.Account().ID)
	}
}

// recordingPolicy captures the account it is handed, so a test can prove the
// policy port speaks this package's shape rather than the core's.
type recordingPolicy struct {
	deleted Account
	taskID  string
}

func (p *recordingPolicy) OnAccountDeleted(ctx context.Context, taskID string, deleted Account) Decision {
	p.taskID = taskID
	p.deleted = deleted
	return Unbind
}

// TestDeletionPolicySeesAnAccount verifies the port a consumer implements is
// handed an Account with its fields, not the core's record — a policy deciding
// the fate of an orphaned task usually wants to say which login it lost.
func TestDeletionPolicySeesAnAccount(t *testing.T) {
	want := creds{Email: "buyer@example.com"}
	repo := newFakeRepo(Account{ID: "a1", GroupID: GlobalGroup, OwnerID: "t1", Fields: fields(t, want)})
	policy := &recordingPolicy{}
	ctx := context.Background()
	m := newTestManager(t, repo, policy, WithUsagePolicy(Usage{}))

	if err := m.DeleteAccount(ctx, "a1"); err != nil {
		t.Fatalf("DeleteAccount: %v", err)
	}
	if policy.taskID != "t1" {
		t.Fatalf("policy asked about %q, want t1", policy.taskID)
	}
	if policy.deleted.ID != "a1" {
		t.Fatalf("policy saw %q, want a1", policy.deleted.ID)
	}
	got, err := Bind[creds](policy.deleted)
	if err != nil {
		t.Fatalf("Bind on what the policy saw: %v", err)
	}
	if got != want {
		t.Fatalf("policy saw fields %+v, want %+v", got, want)
	}
}

// TestAssignmentPinTravelsAsTheAccountID verifies this package's AccountID
// reaches the core as its ResourceID: the two structs are spelled differently,
// and a pin dropped in translation would look like ordinary rotation — which
// for accounts means running as the wrong person.
func TestAssignmentPinTravelsAsTheAccountID(t *testing.T) {
	repo := newFakeRepo(
		Account{ID: "a1", GroupID: GlobalGroup},
		Account{ID: "a2", GroupID: GlobalGroup},
	)
	ctx := context.Background()
	m := newTestManager(t, repo, nil)

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1", AccountID: "a2"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Account().ID != "a2" {
		t.Fatalf("leased %s, want the pinned a2", lease.Account().ID)
	}
	if err := m.CheckAssignment(Assignment{TaskID: "t1", AccountID: "a2"}); err != nil {
		t.Fatalf("CheckAssignment on a good pin: %v", err)
	}
	if err := m.CheckAssignment(Assignment{TaskID: "t1", AccountID: "ghost"}); !errors.Is(err, ErrAccountNotFound) {
		t.Fatalf("err = %v, want ErrAccountNotFound", err)
	}
}

// TestSentinelsAliasTheCore verifies every error this package documents is the
// core's, so errors.Is answers the same question either side of the boundary.
func TestSentinelsAliasTheCore(t *testing.T) {
	for _, pair := range []struct {
		name  string
		here  error
		there error
	}{
		{"ErrNoAccounts", ErrNoAccounts, leasing.ErrNoResources},
		{"ErrGroupNotFound", ErrGroupNotFound, leasing.ErrGroupNotFound},
		{"ErrGroupInUse", ErrGroupInUse, leasing.ErrGroupInUse},
		{"ErrAccountInUse", ErrAccountInUse, leasing.ErrResourceInUse},
		{"ErrAccountNotFound", ErrAccountNotFound, leasing.ErrResourceNotFound},
		{"ErrAccountNotInGroup", ErrAccountNotInGroup, leasing.ErrResourceNotInGroup},
		{"ErrAccountLocked", ErrAccountLocked, leasing.ErrResourceLocked},
		{"ErrPinConflict", ErrPinConflict, leasing.ErrPinConflict},
		{"ErrTaskOrphaned", ErrTaskOrphaned, leasing.ErrTaskOrphaned},
	} {
		if pair.here != pair.there {
			t.Fatalf("%s is not the core sentinel", pair.name)
		}
	}
}

// TestUsageWiresEachQuestionToItsOwnFunc verifies the three closures land on the
// three questions the guard asks, and that a nil one reports nothing rather
// than panicking. That last part is the wiring accounts actually ship with: with
// no account placement on the task record, TaskRunning is the only one a
// consumer can answer.
func TestUsageWiresEachQuestionToItsOwnFunc(t *testing.T) {
	repo := newFakeRepo(Account{ID: "a1", GroupID: GlobalGroup})
	ctx := context.Background()
	asked := map[string]string{}
	m := newTestManager(t, repo, nil, WithUsagePolicy(Usage{
		RunningInGroup: func(ctx context.Context, accountGroupID string) ([]string, error) {
			asked["group"] = accountGroupID
			return nil, nil
		},
		PinnedToAccount: func(ctx context.Context, accountID string) ([]string, error) {
			asked["pinned"] = accountID
			return []string{"t9"}, nil
		},
	}))

	impact, err := m.DeletionImpact(ctx, "a1")
	if err != nil {
		t.Fatalf("DeletionImpact: %v", err)
	}
	if asked["group"] != GlobalGroup {
		t.Fatalf("RunningInGroup asked about %q, want the global group", asked["group"])
	}
	if asked["pinned"] != "a1" {
		t.Fatalf("PinnedToAccount asked about %q, want a1", asked["pinned"])
	}
	if len(impact.Pinned) != 1 || impact.Pinned[0] != "t9" {
		t.Fatalf("pinned = %v, want [t9]", impact.Pinned)
	}
	// TaskRunning was left nil, and the guard neither panicked nor refused.
	if len(impact.Running) != 0 {
		t.Fatalf("running = %v, want none", impact.Running)
	}
	if err := m.DeleteAccount(ctx, "a1"); err != nil {
		t.Fatalf("a pin is a warning, not a refusal: %v", err)
	}
}
