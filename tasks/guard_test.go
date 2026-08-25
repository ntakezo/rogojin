package tasks

import (
	"context"
	"strings"
	"testing"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/proxies"
)

// A Guard is never declared to implement anything: it satisfies each manager's
// usage policy structurally, which is what lets this package stay ignorant of
// leasing. That makes the coupling invisible, so these assertions are what
// notice if either port changes shape.
var (
	guardedService Service
	_              proxies.UsagePolicy  = NewGuard(&guardedService, proxyKind)
	_              accounts.UsagePolicy = NewGuard(&guardedService, accountKind)
)

// recordingService captures what a guard asked it. The embedded Service is nil,
// so reaching past these three methods panics rather than passing quietly —
// a guard must consult exactly the usage policy's surface.
type recordingService struct {
	Service
	kinds  []string
	groups []string
	pins   []string
	tasks  []string
}

func (r *recordingService) RunningTasks(ctx context.Context, kind, groupID string) ([]string, error) {
	r.kinds = append(r.kinds, kind)
	r.groups = append(r.groups, groupID)
	return []string{"t1"}, nil
}

func (r *recordingService) PinnedTasks(ctx context.Context, kind, resourceID string) ([]string, error) {
	r.kinds = append(r.kinds, kind)
	r.pins = append(r.pins, resourceID)
	return []string{"t2"}, nil
}

func (r *recordingService) TaskIsRunning(ctx context.Context, taskID string) (bool, error) {
	r.tasks = append(r.tasks, taskID)
	return true, nil
}

// TestGuardScopesEveryQuestionToItsKind verifies the kind is fixed when the
// guard is built and applied to every question that takes one. That is the
// guard's whole job: a manager asks about a group or a resource by name, and
// two managers may use the same name for unrelated pools.
func TestGuardScopesEveryQuestionToItsKind(t *testing.T) {
	rec := &recordingService{}
	var svc Service = rec
	guard := NewGuard(&svc, accountKind)
	ctx := context.Background()

	if got, err := guard.RunningTasks(ctx, "shared"); err != nil || len(got) != 1 {
		t.Fatalf("RunningTasks = %v, %v; want the service's answer", got, err)
	}
	if got, err := guard.PinnedTasks(ctx, "r1"); err != nil || len(got) != 1 {
		t.Fatalf("PinnedTasks = %v, %v; want the service's answer", got, err)
	}
	if got, err := guard.TaskIsRunning(ctx, "t3"); err != nil || !got {
		t.Fatalf("TaskIsRunning = %v, %v; want true", got, err)
	}

	for _, kind := range rec.kinds {
		if kind != accountKind {
			t.Fatalf("asked under kind %q, want every question under %q", kind, accountKind)
		}
	}
	if len(rec.kinds) != 2 {
		t.Fatalf("kinded questions = %d, want 2 (running and pinned)", len(rec.kinds))
	}
	if len(rec.groups) != 1 || rec.groups[0] != "shared" {
		t.Fatalf("groups asked = %v, want [shared]", rec.groups)
	}
	if len(rec.pins) != 1 || rec.pins[0] != "r1" {
		t.Fatalf("pins asked = %v, want [r1]", rec.pins)
	}
	// A task either runs or it does not; the kind has no bearing on it.
	if len(rec.tasks) != 1 || rec.tasks[0] != "t3" {
		t.Fatalf("tasks asked = %v, want [t3]", rec.tasks)
	}
}

// TestGuardReadsTheServiceAtCallTime verifies the guard resolves through its
// pointer on every call, not at construction. That is the point of taking one:
// a manager is built before the service that answers for it exists.
func TestGuardReadsTheServiceAtCallTime(t *testing.T) {
	var svc Service
	guard := NewGuard(&svc, proxyKind)

	rec := &recordingService{}
	svc = rec

	if _, err := guard.RunningTasks(context.Background(), "residential"); err != nil {
		t.Fatalf("RunningTasks: %v", err)
	}
	if len(rec.groups) != 1 || rec.groups[0] != "residential" {
		t.Fatalf("groups asked = %v, want [residential]: the guard did not resolve the assigned service", rec.groups)
	}
}

// TestGuardPanicsBeforeItsServiceIsAssigned verifies an unwired guard fails
// loudly, and says what went wrong. Reporting "nothing is running" would be
// worse than a crash: it lets a deletion tear a pool out from under a live run,
// and the damage surfaces far from the wiring mistake that caused it.
//
// The message is the assertion, not the panic. Reaching a nil Service would
// crash on the method call regardless — as a bare nil dereference naming
// nothing, from inside a manager's delete, pointing at no line the consumer
// wrote. Naming the kind and the cause is the whole difference.
func TestGuardPanicsBeforeItsServiceIsAssigned(t *testing.T) {
	var svc Service
	guard := NewGuard(&svc, proxyKind)
	ctx := context.Background()

	for name, ask := range map[string]func(){
		"RunningTasks":  func() { guard.RunningTasks(ctx, "residential") },
		"PinnedTasks":   func() { guard.PinnedTasks(ctx, "p1") },
		"TaskIsRunning": func() { guard.TaskIsRunning(ctx, "t1") },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				raised := recover()
				if raised == nil {
					t.Fatal("no panic: an unwired guard must not answer")
				}
				message, ok := raised.(string)
				if !ok {
					t.Fatalf("panicked with %T (%v), want a message naming the mistake", raised, raised)
				}
				if !strings.Contains(message, proxyKind) || !strings.Contains(message, "Service was assigned") {
					t.Fatalf("panic message = %q, want the kind and the unassigned service named", message)
				}
			}()
			ask()
		})
	}
}

// TestNewGuardRejectsANilPointer verifies the one mistake no later moment can
// repair fails at the wiring line rather than at the first deletion.
func TestNewGuardRejectsANilPointer(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic: a nil *Service can never become answerable")
		}
	}()
	NewGuard(nil, proxyKind)
}
