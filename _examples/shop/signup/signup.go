// Package signup is the signup workflow of the shop domain. It
// validates input and builds per-task instances; the automation lives in the
// states subpackage, and the HTTP in shop/requests, shared with
// the domain's other workflows.
package signup

import (
	"fmt"

	"github.com/ntakezo/rogojin/_examples/shop/signup/states"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/workflows"
)

// Name is the ID the workflow registers under, qualified by its domain so
// another domain may hold a signup of its own.
const Name = "shop/signup"

// Config is what the domain hands this workflow when it registers it. It is a
// struct so a new dependency is a new field rather than a changed signature.
type Config struct {
	Proxies *proxies.Manager
}

// workflow is the registered module. It is built once and shared across tasks;
// per-task state lives on the instance it builds in NewInstance.
type workflow struct {
	cfg Config
}

// New builds the workflow module.
func New(cfg Config) workflows.Workflow { return workflow{cfg: cfg} }

func (w workflow) ID() string { return Name }

// ValidateInput ensures the caller passed a states.Input.
func (w workflow) ValidateInput(input any) error {
	if _, ok := input.(states.Input); !ok {
		return fmt.Errorf("shop/signup: expected states.Input, got %T", input)
	}
	return nil
}

// NewInstance builds a fresh, per-task instance bound to input and deps.
func (w workflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	in, ok := input.(states.Input)
	if !ok {
		return nil, fmt.Errorf("shop/signup: expected states.Input, got %T", input)
	}
	return states.NewContext(in, deps, w.cfg.Proxies), nil
}

// RestoreInstance rebuilds an instance from a snapshot so a task can resume
// after a crash, restart, or suspend.
func (w workflow) RestoreInstance(deps workflows.Deps, snapshot []byte) (workflows.Instance, error) {
	return states.RestoreContext(deps, snapshot, w.cfg.Proxies)
}
