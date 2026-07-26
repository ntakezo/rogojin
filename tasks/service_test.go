package tasks

import (
	"context"
	"errors"
	"testing"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/workflows"
)

// TestNilRepositoryRunsInMemoryLifecycle verifies a Service built with a nil
// repository supports the full in-memory lifecycle — create, start to completion,
// and delete — without ever dereferencing a store. This is the purely in-memory
// use case: no durability is wanted, so a nil repository must be a valid choice
// rather than a panic.
func TestNilRepositoryRunsInMemoryLifecycle(t *testing.T) {
	var log []workflows.State
	svc := NewService(nil, comms.NewBus())
	wf := &testWorkflow{log: &log}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	task, err := svc.CreateTask(context.Background(), wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask with nil repository: %v", err)
	}

	if _, err := task.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if task.Status() != workflows.StatusDone {
		t.Fatalf("status = %q, want done", task.Status())
	}

	if err := svc.DeleteTask(context.Background(), task.ID()); err != nil {
		t.Fatalf("DeleteTask with nil repository: %v", err)
	}
}

// TestNilRepositoryRecoverAllIsEmpty verifies recovery is a harmless no-op with no
// durable store: there are no persisted tasks to rehydrate, so a startup recovery
// loop stays safe in in-memory mode rather than crashing on a nil store.
func TestNilRepositoryRecoverAllIsEmpty(t *testing.T) {
	svc := NewService(nil, comms.NewBus())

	recovered, err := svc.RecoverAll(context.Background())
	if err != nil {
		t.Fatalf("RecoverAll with nil repository: %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("recovered %d tasks, want 0 (nothing persisted)", len(recovered))
	}
}

// TestNilRepositoryRecoverTaskErrors verifies recovering a specific task fails
// loudly with a nil repository: there is nothing durable to rehydrate from, and
// silently returning a zero task would hide that.
func TestNilRepositoryRecoverTaskErrors(t *testing.T) {
	svc := NewService(nil, comms.NewBus())

	if _, err := svc.RecoverTask(context.Background(), "missing"); err == nil {
		t.Fatal("RecoverTask with nil repository: want error, got nil")
	}
}

// recordStore is a fakeStore that recovers a canned record, standing in for a
// repository holding one persisted task.
type recordStore struct {
	fakeStore
	record Record
}

func (r *recordStore) RecoverTask(ctx context.Context, id string) (Record, error) {
	return r.record, nil
}

// TestRecoverNeverCheckpointedTaskFailsLoud verifies a task that was created
// but never checkpointed recovers into a task whose Start fails with
// ErrNoCheckpoint. Its input was never persisted, so nothing can actually
// resume; a cryptic state error — or silently rerunning — would both lie about
// what recovery can do. Durability begins at the first checkpoint.
func TestRecoverNeverCheckpointedTaskFailsLoud(t *testing.T) {
	var log []workflows.State
	store := &recordStore{record: Record{ID: "t1", WorkflowID: "test"}}
	svc := NewService(store, comms.NewBus())
	wf := &testWorkflow{log: &log}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	task, err := svc.RecoverTask(context.Background(), "t1")
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}

	if _, err := task.Start(context.Background()); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("Start err = %v, want ErrNoCheckpoint", err)
	}
	if len(log) != 0 {
		t.Fatalf("no states should have run, got %v", log)
	}
}
