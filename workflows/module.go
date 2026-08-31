package workflows

import (
	"encoding/json"
	"fmt"

	"github.com/ntakezo/rogojin/leasing"
)

// A Module is the standard Workflow implementation: typed input validation,
// instance construction, and snapshot recovery, all derived from one typed
// build function. The instance build returns must embed Base — the module
// binds the task's deps into it on construction and restores its envelope on
// recovery, which is what lets the instance skip hand-written snapshot and
// restore plumbing.
//
// Configuration chains off the constructor:
//
//	workflows.NewModule(Name, build).
//		WithResources(wire).
//		WithValidate(check)
type Module[In any] struct {
	id        string
	build     func(In, Deps) (Instance, error)
	validate  func(In) error
	resources func(map[leasing.Kind]any) error
}

// NewModule builds a module registered under id that constructs instances with
// build. In must round-trip through JSON: the module records it in the
// snapshot envelope and rebuilds recovered instances from it.
func NewModule[In any](id string, build func(In, Deps) (Instance, error)) *Module[In] {
	return &Module[In]{id: id, build: build}
}

// WithResources registers the wiring RegisterWorkflow calls with every manager
// on the task manager; extract the kinds the workflow leases through with
// Resource, so a wiring hole fails the registration at boot.
func (m *Module[In]) WithResources(fn func(map[leasing.Kind]any) error) *Module[In] {
	m.resources = fn
	return m
}

// WithValidate adds input validation beyond the type check, run at task
// creation.
func (m *Module[In]) WithValidate(fn func(In) error) *Module[In] {
	m.validate = fn
	return m
}

// ID returns the module's registered workflow ID.
func (m *Module[In]) ID() string { return m.id }

// assertInput types the caller's input. nil reads as the zero In, for
// workflows that take no input.
func (m *Module[In]) assertInput(input any) (In, error) {
	var zero In
	if input == nil {
		return zero, nil
	}
	in, ok := input.(In)
	if !ok {
		return zero, fmt.Errorf("%s: expected %T, got %T", m.id, zero, input)
	}
	return in, nil
}

// ValidateInput checks the input is an In, then runs the WithValidate hook.
func (m *Module[In]) ValidateInput(input any) error {
	in, err := m.assertInput(input)
	if err != nil {
		return err
	}
	if m.validate != nil {
		return m.validate(in)
	}
	return nil
}

// NewInstance builds a per-task instance from input and binds its Base to deps.
func (m *Module[In]) NewInstance(input any, deps Deps) (Instance, error) {
	in, err := m.assertInput(input)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return nil, fmt.Errorf("%s: marshal input: %w", m.id, err)
	}
	inst, err := m.build(in, deps)
	if err != nil {
		return nil, err
	}
	b, err := m.baseOf(inst)
	if err != nil {
		return nil, err
	}
	b.bind(deps, raw)
	return inst, nil
}

// RestoreInstance decodes the snapshot envelope, rebuilds the instance from
// its recorded input, and restores the effect log and durable state into it.
func (m *Module[In]) RestoreInstance(deps Deps, snapshot []byte) (Instance, error) {
	var env envelope
	if err := json.Unmarshal(snapshot, &env); err != nil {
		return nil, fmt.Errorf("%s: decode snapshot: %w", m.id, err)
	}
	var in In
	if len(env.Input) > 0 {
		if err := json.Unmarshal(env.Input, &in); err != nil {
			return nil, fmt.Errorf("%s: decode input: %w", m.id, err)
		}
	}
	inst, err := m.build(in, deps)
	if err != nil {
		return nil, err
	}
	b, err := m.baseOf(inst)
	if err != nil {
		return nil, err
	}
	b.bind(deps, env.Input)
	if err := b.restore(env); err != nil {
		return nil, fmt.Errorf("%s: %w", m.id, err)
	}
	return inst, nil
}

// UseResources implements ResourceReceiver: it runs the WithResources wiring,
// or accepts any registration when none was configured.
func (m *Module[In]) UseResources(managers map[leasing.Kind]any) error {
	if m.resources == nil {
		return nil
	}
	return m.resources(managers)
}

// baseOf finds the instance's embedded Base, refusing instances that lack one:
// the module cannot bind or restore what it cannot reach.
func (m *Module[In]) baseOf(inst Instance) (*Base, error) {
	hb, ok := inst.(hasBase)
	if !ok {
		return nil, fmt.Errorf("%s: instance %T must embed workflows.Base", m.id, inst)
	}
	return hb.base(), nil
}

// An OutModule is a Module that additionally declares its output type — the
// shape the instances' Outputter marshals. Out is a compile-time contract,
// unused at runtime: tasks.Create consumes it to hand back a task handle
// whose Start and Output carry the declared type, with no type arguments at
// the call site.
type OutModule[In, Out any] struct {
	*Module[In]
}

// Returns declares the module's output type:
//
//	workflows.NewModule(Name, build).
//		WithResources(wire).
//		Returns[Order]()
//
// Declare it last: WithResources and WithValidate chain on the plain module.
func (m *Module[In]) Returns[Out any]() *OutModule[In, Out] {
	return &OutModule[In, Out]{Module: m}
}

// Resource extracts the manager registered under kind as a T, reporting the
// missing or mistyped registration otherwise — the typed accessor for a
// WithResources hook.
func Resource[T any](managers map[leasing.Kind]any, kind leasing.Kind) (T, error) {
	v, ok := managers[kind].(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("no %T registered under kind %q", zero, kind)
	}
	return v, nil
}
