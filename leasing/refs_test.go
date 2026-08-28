package leasing

import (
	"context"
	"testing"
)

// TestGroupRefsTravelOpaquely verifies group refs round-trip through the
// repository and a manager restart untouched and unread, because consumers
// hang cross-resource references (like a forwarding inbox) on them and the
// mechanism layer must carry those bytes the way it carries Attrs.
func TestGroupRefsTravelOpaquely(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo)
	ctx := context.Background()

	refs := map[string]string{"email": "inbox-1", "other": "x"}
	if err := m.CreateGroup(ctx, Group{ID: "g1", Refs: refs}); err != nil {
		t.Fatalf("create group: %v", err)
	}

	restarted := rebuildManager(t, repo)
	if err := restarted.Add(ctx, res{ID: "p1", GroupID: "g1"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	l, err := restarted.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "g1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer l.Release(true)
	got := l.Group().Refs
	if got["email"] != "inbox-1" || got["other"] != "x" {
		t.Fatalf("refs after restart = %v, want %v", got, refs)
	}
}

// TestLeaseExposesItsGroup verifies a lease reports the group it was acquired
// under, because holders resolving group-level refs must not need a second
// lookup surface on the manager.
func TestLeaseExposesItsGroup(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo)
	ctx := context.Background()

	if err := m.CreateGroup(ctx, Group{ID: "g1", Refs: map[string]string{"email": "inbox-1"}}); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := m.Add(ctx, res{ID: "p1", GroupID: "g1"}); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Both acquisition paths must carry the group: rotation and the durable
	// lock's bound fast path.
	l, err := m.Lock(ctx, Assignment{TaskID: "t1", GroupID: "g1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	if l.Group().ID != "g1" || l.Group().Refs["email"] != "inbox-1" {
		t.Fatalf("lock lease group = %+v, want g1 with email ref", l.Group())
	}
	l.Release(true)

	relock, err := m.Acquire(ctx, Assignment{TaskID: "t1", GroupID: "g1"})
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	defer relock.Release(true)
	if relock.Group().ID != "g1" || relock.Group().Refs["email"] != "inbox-1" {
		t.Fatalf("bound lease group = %+v, want g1 with email ref", relock.Group())
	}
}

// TestHeldAndLockedReportOnlyMatchingAssignments verifies the two fact
// reports: Held names live leases, Locked names durable locks running or not,
// and both apply the caller's predicate over resource and group — the queries
// a referential delete guard is built from.
func TestHeldAndLockedReportOnlyMatchingAssignments(t *testing.T) {
	repo := newFakeRepo()
	m := newTestManager(t, repo)
	ctx := context.Background()

	if err := m.CreateGroup(ctx, Group{ID: "g1", Refs: map[string]string{"email": "inbox-1"}}); err != nil {
		t.Fatalf("create group: %v", err)
	}
	seed := []res{
		{ID: "p1", GroupID: "g1"},
		{ID: "p2", GroupID: "g1"},
		{ID: "p3", Attrs: payload{region: "elsewhere"}},
	}
	for _, p := range seed {
		if err := m.Add(ctx, p); err != nil {
			t.Fatalf("add %s: %v", p.ID, err)
		}
	}

	inG1 := func(p res, g Group) bool { return g.Refs["email"] == "inbox-1" }

	if got := m.Held(inG1); len(got) != 0 {
		t.Fatalf("Held before any lease = %v, want none", got)
	}

	held, err := m.Acquire(ctx, Assignment{TaskID: "t-live", GroupID: "g1"})
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	locked, err := m.Lock(ctx, Assignment{TaskID: "t-locked", GroupID: "g1"})
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	outside, err := m.Acquire(ctx, Assignment{TaskID: "t-outside"})
	if err != nil {
		t.Fatalf("acquire outside: %v", err)
	}
	defer outside.Release(true)

	// t-locked releases its lease but keeps the durable lock: an idle lock
	// must appear in Locked and vanish from Held.
	locked.Release(true)

	gotHeld := m.Held(inG1)
	if len(gotHeld) != 1 || gotHeld[0].TaskID != "t-live" {
		t.Fatalf("Held = %v, want only t-live", gotHeld)
	}
	gotLocked := m.Locked(inG1)
	if len(gotLocked) != 1 || gotLocked[0].TaskID != "t-locked" {
		t.Fatalf("Locked = %v, want only t-locked", gotLocked)
	}

	held.Release(true)
	if got := m.Held(inG1); len(got) != 0 {
		t.Fatalf("Held after release = %v, want none", got)
	}
	if got := m.Locked(inG1); len(got) != 1 {
		t.Fatalf("Locked after release = %v, want the idle lock to remain", got)
	}
}
