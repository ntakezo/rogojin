package cards

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
)

// The leasing behaviour this package inherits — pooling, groups, holder caps,
// durable locks, pins, deletion policies, the usage guard — is covered once in
// the leasing package, against the code that implements it. What is tested here
// is what cards add: the workflow-defined JSON payload, the absent rotation
// knob, and the translation between this package's shapes and the core's.

// fakeRepo is an in-memory Repository recording what the manager stores, so a
// test can assert the consumer's store is handed card-shaped records.
type fakeRepo struct {
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
	out := make([]Card, 0, len(r.order))
	for _, id := range r.order {
		if c, ok := r.records[id]; ok {
			out = append(out, c)
		}
	}
	return out, nil
}

func (r *fakeRepo) Save(ctx context.Context, c Card) error {
	if _, ok := r.records[c.ID]; !ok {
		r.order = append(r.order, c.ID)
	}
	r.records[c.ID] = c
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

type payment struct {
	Number string `json:"number"`
	Expiry string `json:"expiry"`
	CVV    string `json:"cvv"`
}

func fields(t *testing.T, p payment) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal payment: %v", err)
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
// payload: a workflow defines its own shape, the store never learns it, and Bind
// hands it back typed on the far side of a restart.
func TestFieldsRoundTripThroughTheStore(t *testing.T) {
	want := payment{Number: "4111111111111111", Expiry: "12/29", CVV: "737"}
	repo := newFakeRepo()
	m := newTestManager(t, repo, nil)
	ctx := context.Background()

	if err := m.AddCard(ctx, Card{ID: "c1", Fields: fields(t, want)}); err != nil {
		t.Fatalf("AddCard: %v", err)
	}
	if stored := repo.records["c1"]; string(stored.Fields) != string(fields(t, want)) {
		t.Fatalf("stored fields = %s, want the JSON handed in", stored.Fields)
	}

	restarted := newTestManager(t, repo, nil)
	lease, err := restarted.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	got, err := Bind[payment](lease.Card())
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if got != want {
		t.Fatalf("fields = %+v, want %+v", got, want)
	}
}

// TestBindToleratesCardsWithoutFields verifies a workflow that carries its
// payment data elsewhere — a stored token, a wallet handle — is not forced to
// store an empty object, and that a payload that is not what the caller expects
// is reported rather than silently zeroed.
func TestBindToleratesCardsWithoutFields(t *testing.T) {
	got, err := Bind[payment](Card{ID: "c1"})
	if err != nil {
		t.Fatalf("Bind on a card with no fields: %v", err)
	}
	if got != (payment{}) {
		t.Fatalf("fields = %+v, want the zero value", got)
	}

	if _, err := Bind[payment](Card{ID: "c1", Fields: json.RawMessage(`["not","an","object"]`)}); err == nil {
		t.Fatal("expected a mismatched payload to be reported")
	}
}

// TestLeasedFieldsAreACopy verifies a holder editing the card it was handed
// cannot corrupt the pool every other task draws from. The core copies resources
// by value, so without an explicit clone the backing array is shared.
func TestLeasedFieldsAreACopy(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo, nil)
	ctx := context.Background()

	original := fields(t, payment{Number: "4111111111111111"})
	if err := m.AddCard(ctx, Card{ID: "c1", Fields: original}); err != nil {
		t.Fatalf("AddCard: %v", err)
	}
	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	leased := lease.Card().Fields
	leased[0] = 'X'
	if err := lease.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}

	// The next holder is the proof: if the edit had reached the pool, this is
	// where it would come back out — as an unpayable card.
	next, err := m.Acquire(ctx, Assignment{TaskID: "t2"})
	if err != nil {
		t.Fatalf("second acquire: %v", err)
	}
	if string(next.Card().Fields) != string(original) {
		t.Fatalf("pool fields = %s, want %s", next.Card().Fields, original)
	}
	if stored := repo.records["c1"]; string(stored.Fields) != string(original) {
		t.Fatalf("stored fields = %s, want %s", stored.Fields, original)
	}
}

// TestGroupsCarryNoStrategy verifies the one way cards differ from proxies:
// there is no rotation knob on the API or in the store, and the manager still
// rotates fairly without one — including after a restart, where the group comes
// back with no strategy recorded and must resolve one anyway.
func TestGroupsCarryNoStrategy(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo, nil)
	ctx := context.Background()

	if err := m.CreateGroup(ctx, Group{ID: "bin", MaxHolders: 1}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	for _, id := range []string{"c1", "c2"} {
		if err := m.AddCard(ctx, Card{ID: id, GroupID: "bin"}); err != nil {
			t.Fatalf("AddCard %s: %v", id, err)
		}
	}
	if _, ok := repo.groups["bin"]; !ok {
		t.Fatal("the group was not persisted")
	}

	restarted := newTestManager(t, repo, nil)
	first, err := restarted.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "bin"})
	if err != nil {
		t.Fatalf("first acquire after restart: %v", err)
	}
	second, err := restarted.Acquire(ctx, Assignment{TaskID: "t2", GroupID: "bin"})
	if err != nil {
		t.Fatalf("second acquire after restart: %v", err)
	}
	if first.Card().ID == second.Card().ID {
		t.Fatalf("both tasks got %s, want rotation", first.Card().ID)
	}
}

// TestDefaultHolderCapIsOne verifies the guarantee that matters most for a
// payment instrument: with no holder policy set, a second task waits rather
// than checking out against a card another task is already charging. The
// contrast is the proof — the same card with MaxHolders 2 hands out twice
// under the same deadline, so what expires below is the cap and not the clock.
func TestDefaultHolderCapIsOne(t *testing.T) {
	ctx := context.Background()
	m := newTestManager(t, newFakeRepo(Card{ID: "c1", GroupID: GlobalGroup}), nil)

	held, err := m.Acquire(ctx, Assignment{TaskID: "t1"})
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// The pool's one card is taken and its effective cap is 1, so this acquire
	// has nothing to hand out and waits; the deadline it runs out is what
	// proves it blocked rather than doubling up.
	waiting, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := m.Acquire(waiting, Assignment{TaskID: "t2"}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire = %v, want it to have blocked on the held card", err)
	}

	if err := held.Release(true); err != nil {
		t.Fatalf("release: %v", err)
	}
	freed, err := m.Acquire(ctx, Assignment{TaskID: "t2"})
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if freed.Card().ID != "c1" {
		t.Fatalf("leased %s, want the freed c1", freed.Card().ID)
	}

	// A card that explicitly tolerates two holders is handed out twice, well
	// inside the same deadline the capped one ran out.
	shared := newTestManager(t, newFakeRepo(Card{ID: "c2", GroupID: GlobalGroup, MaxHolders: 2}), nil)
	if _, err := shared.Acquire(ctx, Assignment{TaskID: "t3"}); err != nil {
		t.Fatalf("first acquire of a shared card: %v", err)
	}
	both, cancelBoth := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelBoth()
	if _, err := shared.Acquire(both, Assignment{TaskID: "t4"}); err != nil {
		t.Fatalf("second acquire of a shared card: %v", err)
	}
}

// recordingPolicy captures the card it is handed, so a test can prove the policy
// port speaks this package's shape rather than the core's.
type recordingPolicy struct {
	deleted Card
	taskID  string
}

func (p *recordingPolicy) OnCardDeleted(ctx context.Context, taskID string, deleted Card) Decision {
	p.taskID = taskID
	p.deleted = deleted
	return Unbind
}

// TestDeletionPolicySeesACard verifies the port a consumer implements is handed
// a Card with its fields, not the core's record — a policy deciding the fate of
// an orphaned checkout usually wants to say which instrument it lost.
func TestDeletionPolicySeesACard(t *testing.T) {
	want := payment{Number: "4111111111111111"}
	repo := newFakeRepo(Card{ID: "c1", GroupID: GlobalGroup, OwnerID: "t1", Fields: fields(t, want)})
	policy := &recordingPolicy{}
	ctx := context.Background()
	m := newTestManager(t, repo, policy, WithUsagePolicy(Usage{}))

	if err := m.DeleteCard(ctx, "c1"); err != nil {
		t.Fatalf("DeleteCard: %v", err)
	}
	if policy.taskID != "t1" {
		t.Fatalf("policy asked about %q, want t1", policy.taskID)
	}
	if policy.deleted.ID != "c1" {
		t.Fatalf("policy saw %q, want c1", policy.deleted.ID)
	}
	got, err := Bind[payment](policy.deleted)
	if err != nil {
		t.Fatalf("Bind on what the policy saw: %v", err)
	}
	if got != want {
		t.Fatalf("policy saw fields %+v, want %+v", got, want)
	}
}

// TestAssignmentPinTravelsAsTheCardID verifies this package's CardID reaches the
// core as its ResourceID: the two structs are spelled differently, and a pin
// dropped in translation would look like ordinary rotation — which for cards
// means charging the wrong one.
func TestAssignmentPinTravelsAsTheCardID(t *testing.T) {
	repo := newFakeRepo(
		Card{ID: "c1", GroupID: GlobalGroup},
		Card{ID: "c2", GroupID: GlobalGroup},
	)
	ctx := context.Background()
	m := newTestManager(t, repo, nil)

	lease, err := m.Acquire(ctx, Assignment{TaskID: "t1", CardID: "c2"})
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if lease.Card().ID != "c2" {
		t.Fatalf("leased %s, want the pinned c2", lease.Card().ID)
	}
	if err := m.CheckAssignment(Assignment{TaskID: "t1", CardID: "c2"}); err != nil {
		t.Fatalf("CheckAssignment on a good pin: %v", err)
	}
	if err := m.CheckAssignment(Assignment{TaskID: "t1", CardID: "ghost"}); !errors.Is(err, ErrCardNotFound) {
		t.Fatalf("err = %v, want ErrCardNotFound", err)
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
		{"ErrNoCards", ErrNoCards, leasing.ErrNoResources},
		{"ErrGroupNotFound", ErrGroupNotFound, leasing.ErrGroupNotFound},
		{"ErrGroupInUse", ErrGroupInUse, leasing.ErrGroupInUse},
		{"ErrCardInUse", ErrCardInUse, leasing.ErrResourceInUse},
		{"ErrCardNotFound", ErrCardNotFound, leasing.ErrResourceNotFound},
		{"ErrCardNotInGroup", ErrCardNotInGroup, leasing.ErrResourceNotInGroup},
		{"ErrCardLocked", ErrCardLocked, leasing.ErrResourceLocked},
		{"ErrPinConflict", ErrPinConflict, leasing.ErrPinConflict},
		{"ErrTaskOrphaned", ErrTaskOrphaned, leasing.ErrTaskOrphaned},
	} {
		if pair.here != pair.there {
			t.Fatalf("%s is not the core sentinel", pair.name)
		}
	}
}

// TestUsageWiresEachQuestionToItsOwnFunc verifies the three closures land on the
// three questions the guard asks, and that a nil one reports nothing rather than
// panicking. That last part is the wiring cards actually ship with: with no card
// placement on the task record, TaskRunning is the only one a consumer can
// answer.
func TestUsageWiresEachQuestionToItsOwnFunc(t *testing.T) {
	repo := newFakeRepo(Card{ID: "c1", GroupID: GlobalGroup})
	ctx := context.Background()
	asked := map[string]string{}
	m := newTestManager(t, repo, nil, WithUsagePolicy(Usage{
		RunningInGroup: func(ctx context.Context, cardGroupID string) ([]string, error) {
			asked["group"] = cardGroupID
			return nil, nil
		},
		PinnedToCard: func(ctx context.Context, cardID string) ([]string, error) {
			asked["pinned"] = cardID
			return []string{"t9"}, nil
		},
	}))

	impact, err := m.DeletionImpact(ctx, "c1")
	if err != nil {
		t.Fatalf("DeletionImpact: %v", err)
	}
	if asked["group"] != GlobalGroup {
		t.Fatalf("RunningInGroup asked about %q, want the global group", asked["group"])
	}
	if asked["pinned"] != "c1" {
		t.Fatalf("PinnedToCard asked about %q, want c1", asked["pinned"])
	}
	if len(impact.Pinned) != 1 || impact.Pinned[0] != "t9" {
		t.Fatalf("pinned = %v, want [t9]", impact.Pinned)
	}
	// TaskRunning was left nil, and the guard neither panicked nor refused.
	if len(impact.Running) != 0 {
		t.Fatalf("running = %v, want none", impact.Running)
	}
	if err := m.DeleteCard(ctx, "c1"); err != nil {
		t.Fatalf("a pin is a warning, not a refusal: %v", err)
	}
}
