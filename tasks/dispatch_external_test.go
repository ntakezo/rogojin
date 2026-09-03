// Dispatch is exercised here from outside the package, over the real memory
// adapter, because its whole subject is a task created by one manager
// beginning on another — the record, not the creating process, carries
// everything a run needs.
package tasks_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/memory"
	"github.com/ntakezo/rogojin/tasks"
	"github.com/ntakezo/rogojin/workflows"
)

const proxyKind leasing.Kind = "proxy"

// dispatchInput is the typed input a dispatched run must be rebuilt from.
type dispatchInput struct {
	Order string `json:"order"`
}

// dispatchReport is what a dispatched instance observed at run time: the
// input it was rebuilt from and the placement it resolved — the two things a
// record must carry faithfully across nodes.
type dispatchReport struct {
	input      dispatchInput
	assignment workflows.Assignment
}

type dispatchInstance struct {
	workflows.Base
	in     dispatchInput
	report chan<- dispatchReport
}

func (i *dispatchInstance) Graph() workflows.Graph {
	return workflows.NewGraph("work", workflows.On("work", i.work))
}

func (i *dispatchInstance) work(ctx context.Context) (*workflows.State, error) {
	i.report <- dispatchReport{input: i.in, assignment: i.Assignment(proxyKind)}
	return nil, nil
}

// dispatchModule builds the one-state workflow whose run reports what it saw.
func dispatchModule(report chan<- dispatchReport) *workflows.Module[dispatchInput] {
	return workflows.NewModule("dispatch", func(in dispatchInput, deps workflows.Deps) (workflows.Instance, error) {
		return &dispatchInstance{in: in, report: report}, nil
	})
}

// dispatchNode builds a manager on repo under the given node id, registered
// for the dispatch workflow and closed with the test.
func dispatchNode(t *testing.T, repo tasks.Repository, report chan<- dispatchReport, id string, opts ...tasks.Option) tasks.Manager {
	t.Helper()
	m, err := tasks.NewManager(context.Background(), repo, comms.NewBus(), append([]tasks.Option{tasks.WithNode(id)}, opts...)...)
	if err != nil {
		t.Fatalf("NewManager %s: %v", id, err)
	}
	t.Cleanup(func() { m.Close() })
	if err := m.RegisterWorkflow("dispatch", dispatchModule(report)); err != nil {
		t.Fatalf("RegisterWorkflow %s: %v", id, err)
	}
	return m
}

// TestDispatchRunsCreatedTaskOnAnotherNode verifies the dispatch path whole:
// node A creates a task — with input and a resource-group placement — and
// never starts it; node B recovers the bare record and Start runs it fresh,
// with the input decoded from the record and the placement resolved, to a
// durable done.
func TestDispatchRunsCreatedTaskOnAnotherNode(t *testing.T) {
	repo := memory.NewTasks()
	report := make(chan dispatchReport, 2)
	a := dispatchNode(t, repo, report, "node-a")
	b := dispatchNode(t, repo, report, "node-b")

	created, err := a.CreateTask(context.Background(), "dispatch", dispatchInput{Order: "999"},
		tasks.WithResourceGroup(proxyKind, "residential"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	task, err := b.RecoverTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if _, err := task.Start(context.Background()); err != nil {
		t.Fatalf("dispatched Start: %v", err)
	}

	saw := <-report
	if saw.input != (dispatchInput{Order: "999"}) {
		t.Fatalf("dispatched input = %+v, want order 999", saw.input)
	}
	if saw.assignment != (workflows.Assignment{GroupID: "residential"}) {
		t.Fatalf("dispatched placement = %+v, want group residential", saw.assignment)
	}

	rec, err := repo.RecoverTask(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("RecoverTask record: %v", err)
	}
	if rec.Status != string(workflows.StatusDone) {
		t.Fatalf("status = %q, want done stamped by the dispatched run", rec.Status)
	}
	if rec.OwnerNode != "" {
		t.Fatalf("owner = %q, want the claim cleared with the terminal stamp", rec.OwnerNode)
	}
}

// TestSweepDispatchesCreatedTask verifies the sweep is the dispatcher: node A
// creates a task and does nothing else, and node B's recovery sweep — with no
// Start call anywhere — claims it and runs it to done.
func TestSweepDispatchesCreatedTask(t *testing.T) {
	repo := memory.NewTasks()
	report := make(chan dispatchReport, 2)
	a := dispatchNode(t, repo, report, "node-a")
	created, err := a.CreateTask(context.Background(), "dispatch", dispatchInput{Order: "42"})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	dispatchNode(t, repo, report, "node-b",
		tasks.WithRecoverySweep(10*time.Millisecond, func(taskID string, err error) {
			t.Logf("sweep %s: %v", taskID, err)
		}))

	eventually(t, "the sweep to dispatch the task", func() bool {
		rec, err := repo.RecoverTask(context.Background(), created.ID)
		return err == nil && rec.Status == string(workflows.StatusDone)
	})
	if saw := <-report; saw.input != (dispatchInput{Order: "42"}) {
		t.Fatalf("dispatched input = %+v, want order 42", saw.input)
	}
}

// TestDispatchRefusesInputlessRecord verifies the pre-input shape still fails
// loud: a never-checkpointed record carrying no input — seeded straight into
// the store, the way an old adapter would have written it — refuses with
// ErrNoCheckpoint and releases its claim rather than running from nothing.
func TestDispatchRefusesInputlessRecord(t *testing.T) {
	repo := memory.NewTasks()
	report := make(chan dispatchReport, 1)
	now := time.Now().UTC()
	if err := repo.CreateTask(context.Background(), tasks.Task{
		ID: "legacy", WorkflowID: "dispatch", GroupID: tasks.GlobalGroup,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	b := dispatchNode(t, repo, report, "node-b")
	task, err := b.RecoverTask(context.Background(), "legacy")
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if _, err := task.Start(context.Background()); !errors.Is(err, tasks.ErrNoCheckpoint) {
		t.Fatalf("Start err = %v, want ErrNoCheckpoint", err)
	}
	rec, _ := repo.RecoverTask(context.Background(), "legacy")
	if rec.OwnerNode != "" {
		t.Fatalf("owner = %q, want the refusal's claim released", rec.OwnerNode)
	}
}
