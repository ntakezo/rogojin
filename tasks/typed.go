package tasks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/workflows"
)

// Typed is a task handle whose output carries the workflow's declared type.
// It embeds the task — every other field and method reads through unchanged —
// and shadows the raw Output bytes and untyped Start with decoded versions.
// Create builds one from a module that declares its output via Returns; As
// types a task obtained some other way.
type Typed[Out any] struct {
	*Task
	// Output is the decoded workflow output: the zero Out until Start
	// completes cleanly, or until As wraps a task whose record already
	// carries one. The raw bytes stay at Task.Output.
	Output Out
}

// Create is Manager.CreateTask against a module that declares its output
// type, returning a typed handle: input is compile-time checked against the
// module's In, and Start and Output carry its Out — both inferred from the
// module, so the call site names no types:
//
//	task, err := tasks.Create(ctx, manager, checkoutModule, input)
//	order, err := task.Start(ctx)
//
// The module must already be registered under its ID. The typing rides on
// this function rather than on Manager: Manager is an interface, and
// interface methods cannot declare type parameters.
func Create[In, Out any](ctx context.Context, m Manager, wf *workflows.OutModule[In, Out], input In, opts ...CreateOption) (*Typed[Out], error) {
	task, err := m.CreateTask(ctx, wf.ID(), input, opts...)
	if err != nil {
		return nil, err
	}
	return &Typed[Out]{Task: task}, nil
}

// As types an existing task's output as Out — for tasks obtained outside
// Create, such as recovery — decoding whatever output the record already
// carries. Out must decode what the workflow's Outputter marshals; the
// pairing is the caller's claim, and a mismatch fails here.
func As[Out any](t *Task) (*Typed[Out], error) {
	out, err := decodeOutput[Out](t.Output)
	if err != nil {
		return nil, err
	}
	return &Typed[Out]{Task: t, Output: out}, nil
}

// Start runs the task like Task.Start, decodes the harvested output, and
// both stores it in Output and returns it. A run that yields no output — the
// workflow implements no Outputter, or the run errors or is killed — reads
// as the zero Out; a mismatch between Out and what the Outputter marshaled
// fails loudly. A run whose terminal stamp fails to persist returns its
// decoded output alongside the error.
func (t *Typed[Out]) Start(ctx context.Context) (Out, error) {
	blob, err := t.Task.Start(ctx)
	out, derr := decodeOutput[Out](blob)
	t.Output = out
	if err != nil {
		// the run's error outranks a decode complaint over whatever it left
		return out, err
	}
	return out, derr
}

// decodeOutput unmarshals a harvested output blob, reading absence as the
// zero Out.
func decodeOutput[Out any](blob []byte) (Out, error) {
	var out Out
	if len(blob) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(blob, &out); err != nil {
		return out, fmt.Errorf("decode output: %w", err)
	}
	return out, nil
}
