package tasks

import (
	"context"
	"testing"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/leasing"
)

// badKinds are names outside leasing.Kind's charset. Each becomes a JSON key
// and a JSON path in the store, so every door kinds enter through must refuse
// them before anything durable is written.
var badKinds = []leasing.Kind{"x.y", "a[0]", "", `q"o`}

// TestWithResourceRefusesInvalidKind verifies registration panics on a kind
// the charset refuses — registration is wiring, and bad wiring should fail at
// construction, the way a nil manager already does.
func TestWithResourceRefusesInvalidKind(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatalf("WithResource accepted an invalid kind; want panic")
		}
	}()
	mustManager(t, newMemStore(), comms.NewBus(), WithResource("x.y", &fakeManager{}))
}

// TestInvalidKindRefusedAtEveryDurableDoor verifies each write path a kind
// reaches durable state through — task creation options, reassignment, and
// group resource maps — fails loudly on a name the charset refuses, so a
// misfiled placement can never be stored.
func TestInvalidKindRefusedAtEveryDurableDoor(t *testing.T) {
	svc, _, wf := newDepsManager(t)
	ctx := context.Background()

	for _, kind := range badKinds {
		if _, err := svc.CreateTask(ctx, wf.ID(), nil, WithResourceGroup(kind, "g")); err == nil {
			t.Errorf("CreateTask with kind %q = nil, want an error", kind)
		}
		if err := svc.CreateGroup(ctx, Group{ID: "grp", ResourceGroups: map[leasing.Kind]string{kind: "g"}}); err == nil {
			t.Errorf("CreateGroup with kind %q = nil, want an error", kind)
		}
	}

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	group := "g"
	for _, kind := range badKinds {
		if err := svc.AssignResource(ctx, task.ID, kind, Assignment{GroupID: &group}); err == nil {
			t.Errorf("AssignResource with kind %q = nil, want an error", kind)
		}
	}

	if err := svc.CreateGroup(ctx, Group{ID: "grp"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	for _, kind := range badKinds {
		err := svc.UpdateGroup(ctx, "grp", func(g *Group) {
			g.ResourceGroups = map[leasing.Kind]string{kind: "g"}
		})
		if err == nil {
			t.Errorf("UpdateGroup with kind %q = nil, want an error", kind)
		}
	}

	// The valid counterparts still pass the same doors.
	if _, err := svc.CreateTask(ctx, wf.ID(), nil, WithResourceGroup("proxy", "g")); err != nil {
		t.Fatalf("CreateTask with a valid kind: %v", err)
	}
	if err := svc.UpdateGroup(ctx, "grp", func(g *Group) {
		g.ResourceGroups = map[leasing.Kind]string{"proxy": "g"}
	}); err != nil {
		t.Fatalf("UpdateGroup with a valid kind: %v", err)
	}
}
