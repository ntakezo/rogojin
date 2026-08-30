package example_checkout

import (
	"fmt"

	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/workflows"
)

const Name = "example-checkout"

// workflow is the registered module. It validates input and builds a fresh,
// per-task graph so every task owns its running state and side effects. The
// proxy and account managers arrive through UseResources at registration —
// from the task manager's own registry, so the manager a task leases through
// is the same instance the task manager unlocks through. Only the email
// manager is injected at construction.
type workflow struct {
	proxies  *proxies.Manager
	accounts *accounts.Manager
	email    *email.Manager
}

func New(emailManager *email.Manager) workflows.Workflow {
	return &workflow{email: emailManager}
}

func (w *workflow) ID() string {
	return Name
}

// UseResources receives the task manager's registered managers and asserts
// the kinds this workflow leases through, refusing the registration when one
// is missing or of another type.
func (w *workflow) UseResources(managers map[leasing.Kind]any) error {
	px, ok := managers[proxies.Kind].(*proxies.Manager)
	if !ok {
		return fmt.Errorf("checkout: needs a *proxies.Manager registered under kind %q", proxies.Kind)
	}
	ac, ok := managers[accounts.Kind].(*accounts.Manager)
	if !ok {
		return fmt.Errorf("checkout: needs an *accounts.Manager registered under kind %q", accounts.Kind)
	}
	w.proxies, w.accounts = px, ac
	return nil
}

// ValidateInput ensures the caller passed a checkout StaticContext.
func (w *workflow) ValidateInput(input any) error {
	if _, ok := input.(StaticContext); !ok {
		return fmt.Errorf("checkout: expected StaticContext, got %T", input)
	}
	return nil
}

// NewInstance builds a per-task instance bound to a new context derived from input and deps.
func (w *workflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	static, ok := input.(StaticContext)
	if !ok {
		return nil, fmt.Errorf("checkout: expected StaticContext, got %T", input)
	}
	return NewContext(static, deps, w.proxies, w.accounts, w.email), nil
}

// RestoreInstance rebuilds a context from a JSON snapshot for recovery.
func (w *workflow) RestoreInstance(deps workflows.Deps, snapshot []byte) (workflows.Instance, error) {
	c, err := RestoreContext(deps, snapshot, w.proxies, w.accounts, w.email)
	if err != nil {
		return nil, err
	}
	return c, nil
}
