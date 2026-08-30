// Package workflows defines the workflow programming model: the Workflow and
// Instance ports a module implements, the State graph it runs, and the
// durability hooks (Snapshotter, PersistableWorkflow) used to checkpoint and
// recover a running workflow. The runtime that drives the model lives in the
// tasks package; this package is types only.
package workflows

import (
	"context"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/leasing"
)

// A State names one node in a workflow's graph.
type State string

// A StateHandler executes one state and returns the next state to enter, or
// nil when the workflow is complete.
type StateHandler func(ctx context.Context) (*State, error)

// Next returns a pointer to s, for a StateHandler to return as its next state.
func Next(s State) *State {
	return &s
}

// States maps each state in a graph to its handler.
type States map[State]StateHandler

// A Graph is a workflow's state machine: an initial state plus a handler for
// each state.
type Graph struct {
	initialState State
	states       States
}

// NewGraph builds a graph from its initial state and the handler for each state.
func NewGraph(initial State, states States) Graph {
	return Graph{initialState: initial, states: states}
}

// Initial returns the graph's entry state, where a fresh run begins.
func (g Graph) Initial() State {
	return g.initialState
}

// Handler returns the handler registered for s, or nil if s is not in the graph.
func (g Graph) Handler(s State) StateHandler {
	return g.states[s]
}

// A Workflow is a module the task service can run: it validates task input
// and builds a per-task Instance from it.
type Workflow interface {
	ID() string
	ValidateInput(input any) error
	NewInstance(input any, deps Deps) (Instance, error)
}

// An Instance is a live, per-task workflow exposing the graph the engine runs.
type Instance interface {
	Graph() Graph
}

// Snapshotter is the opt-in durability capability. Snapshot marshals the
// instance's durable state to an opaque blob, which the engine persists before
// entering each state. A snapshot must be valid as the entry of the state it
// is taken before, since recovery resumes there.
type Snapshotter interface {
	Snapshot() ([]byte, error)
}

// Teardowner is the opt-in cleanup capability for releasing resources acquired
// during a run. The engine calls Teardown exactly once when a started run
// exits, with the terminal status and the run's error (nil on clean
// completion). It receives a background context so a kill's cancellation
// cannot block cleanup.
type Teardowner interface {
	Teardown(ctx context.Context, status Status, runErr error) error
}

// Outputter is the opt-in result capability: the inverse of input injection via
// NewInstance. Output marshals the instance's final result to an opaque blob,
// which the engine returns from the task's Start and persists on the terminal
// stamp — but only when a run completes cleanly. A run that is killed or errors
// produces no output. Instances that yield no result simply do not implement it.
type Outputter interface {
	Output() ([]byte, error)
}

// PersistableWorkflow rebuilds a full instance from a snapshot — the inverse
// of Snapshotter. Only workflows that support recovery implement it.
type PersistableWorkflow interface {
	Workflow
	RestoreInstance(deps Deps, snapshot []byte) (Instance, error)
}

// ResourceReceiver is the opt-in wiring capability. RegisterWorkflow calls
// UseResources exactly once, before the workflow is available for task
// creation, with every manager registered on the task manager under its kind —
// concretely typed behind any. The workflow asserts the kinds it leases
// through back to their concrete types and rejects the registration on a
// missing or mistyped one, so a wiring hole is a boot error, not a mid-run
// one. Receiving managers here rather than at construction guarantees the
// manager a workflow leases from is the same instance the task manager
// unlocks through — two instances over one store silently strand locks.
type ResourceReceiver interface {
	UseResources(managers map[leasing.Kind]any) error
}

// Status is a task's lifecycle status and the durable outcome stamped when a
// run exits. The zero value means not started.
type Status string

const (
	StatusNotStarted Status = ""
	StatusRunning    Status = "running"
	StatusSuspended  Status = "suspended"
	StatusKilled     Status = "killed"
	// StatusFailed stamps a run that exited with an error. It is not terminal:
	// the last good checkpoint stands, so the task can be recovered and retried.
	StatusFailed Status = "failed"
	StatusDone   Status = "done"
)

// Terminal reports whether the status is an end-of-life outcome (done or
// killed). A failed run is not terminal; it remains recoverable.
func (s Status) Terminal() bool {
	return s == StatusDone || s == StatusKilled
}

// Deps is the per-task set of ports the framework injects when building a
// workflow instance.
type Deps struct {
	TaskID string
	Bus    comms.Bus
	// Assignments is the task's resolved placement per resource kind, keyed by
	// the kind each resource package publishes (proxies.Kind, accounts.Kind). A
	// kind the task has no placement for is absent, which reads as the zero
	// Assignment — so a lookup needs no branching.
	Assignments map[leasing.Kind]Assignment
}

// An Assignment is a task's resolved placement for one resource kind: the
// group its leases draw from, and the single member of that group it is pinned
// to. Inheritance from the task group is already applied, so both fields are
// final: "" means no group, or no pin. Pass both to that kind's manager
// verbatim; it needs no branching here.
type Assignment struct {
	GroupID    string
	ResourceID string
}
