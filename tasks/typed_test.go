package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/workflows"
)

// typedResult is the output shape the typed fixtures produce and decode.
type typedResult struct {
	OrderID string `json:"orderID"`
}

// typedCtx completes in one state and outputs a typedResult; with fail set the
// run errors instead, producing no output.
type typedCtx struct {
	workflows.Base
	fail bool
}

func (c *typedCtx) Graph() workflows.Graph {
	return workflows.NewGraph(s1,
		workflows.On(s1, func(ctx context.Context) (*workflows.State, error) {
			if c.fail {
				return nil, errors.New("run failed")
			}
			return nil, nil
		}),
	)
}

func (c *typedCtx) Output() ([]byte, error) {
	return json.Marshal(typedResult{OrderID: "order-1"})
}

// typedTestModule declares typedResult as its output, the contract Create
// consumes.
func typedTestModule(fail bool) *workflows.OutModule[struct{}, typedResult] {
	return workflows.NewModule("typed", func(in struct{}, deps workflows.Deps) (workflows.Instance, error) {
		return &typedCtx{fail: fail}, nil
	}).Returns[typedResult]()
}

// TestCreateStartDecodesOutput verifies the typed path end to end: Create
// infers In and Out from the module, Start decodes the harvested output and
// stores it on the handle, and the raw bytes stay readable underneath.
func TestCreateStartDecodesOutput(t *testing.T) {
	ctx := context.Background()
	svc := mustManager(t, nil, comms.NewBus())
	wf := typedTestModule(false)
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	task, err := Create(ctx, svc, wf, struct{}{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	out, err := task.Start(ctx)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if out.OrderID != "order-1" {
		t.Fatalf("output = %+v, want orderID order-1", out)
	}
	if task.Output.OrderID != "order-1" {
		t.Fatalf("handle Output = %+v, want the decoded order", task.Output)
	}
	if len(task.Task.Output) == 0 {
		t.Fatal("raw output bytes not readable under the typed field")
	}
}

// TestTypedStartFailedRunZeroOutput verifies a failed run surfaces its error
// with the zero Out — no partial decode.
func TestTypedStartFailedRunZeroOutput(t *testing.T) {
	ctx := context.Background()
	svc := mustManager(t, nil, comms.NewBus())
	wf := typedTestModule(true)
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	task, err := Create(ctx, svc, wf, struct{}{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	out, err := task.Start(ctx)
	if err == nil {
		t.Fatal("Start on a failing run: want error")
	}
	if out != (typedResult{}) || task.Output != (typedResult{}) {
		t.Fatalf("output = %+v / %+v, want zero", out, task.Output)
	}
}

// TestAsDecodesPersistedOutput verifies As types a task obtained outside
// Create, decoding the output its record already carries.
func TestAsDecodesPersistedOutput(t *testing.T) {
	ctx := context.Background()
	svc := mustManager(t, nil, comms.NewBus())
	wf := typedTestModule(false)
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if _, err := task.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	typed, err := As[typedResult](task)
	if err != nil {
		t.Fatalf("As: %v", err)
	}
	if typed.Output.OrderID != "order-1" {
		t.Fatalf("As Output = %+v, want the persisted order", typed.Output)
	}
	if _, err := As[int](task); err == nil {
		t.Fatal("As with a mismatched Out: want a decode error")
	}
}

// TestTypedDeclaredOutputMismatchSurfaces verifies a module whose declared Out
// does not match what its Outputter marshals fails loudly at decode.
func TestTypedDeclaredOutputMismatchSurfaces(t *testing.T) {
	ctx := context.Background()
	svc := mustManager(t, nil, comms.NewBus())
	wf := workflows.NewModule("mismatched", func(in struct{}, deps workflows.Deps) (workflows.Instance, error) {
		return &typedCtx{}, nil // outputs an object, not an int
	}).Returns[int]()
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	task, err := Create(ctx, svc, wf, struct{}{})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := task.Start(ctx); err == nil {
		t.Fatal("Start decoding an object into int: want error")
	}
}
