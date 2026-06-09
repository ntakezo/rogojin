package example_checkout

import (
	"fmt"

	"github.com/ntakezo/rogojin/_examples/workflows/example/checkout/states"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/workflows"
)

const Name = "example-checkout"

// workflow is the registered module. It validates input and builds a fresh,
// per-task graph so every task owns its running state and side effects. The
// proxy manager is injected at construction; instances lease from it per run.
type workflow struct {
	proxies *proxies.Manager
}

func New(manager *proxies.Manager) workflows.Workflow {
	return workflow{proxies: manager}
}

func (w workflow) ID() string {
	return Name
}

// ValidateInput ensures the caller passed a checkout StaticContext.
func (w workflow) ValidateInput(input any) error {
	if _, ok := input.(states.StaticContext); !ok {
		return fmt.Errorf("checkout: expected states.StaticContext, got %T", input)
	}
	return nil
}

// NewInstance builds a per-task instance bound to a new context derived from input and deps.
func (w workflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	static, ok := input.(states.StaticContext)
	if !ok {
		return nil, fmt.Errorf("checkout: expected states.StaticContext, got %T", input)
	}
	return states.NewContext(static, deps, w.proxies), nil
}

// RestoreInstance rebuilds a context from a JSON snapshot for recovery.
func (w workflow) RestoreInstance(deps workflows.Deps, snapshot []byte) (workflows.Instance, error) {
	c, err := states.RestoreContext(deps, snapshot, w.proxies)
	if err != nil {
		return nil, err
	}
	return c, nil
}
