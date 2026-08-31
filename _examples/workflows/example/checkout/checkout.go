package example_checkout

import (
	"github.com/ntakezo/rogojin/accounts"
	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/workflows"
)

const Name = "example-checkout"

// New builds the registered module. The module derives validation,
// construction, and snapshot recovery from the build function, and declares
// Order as its output, so tasks.Create returns a handle typed to it. The
// proxy and account managers arrive through WithResources at registration —
// from the task manager's own registry, so the manager a task leases through
// is the same instance the task manager unlocks through. Only the email
// manager is injected at construction.
func New(emailManager *email.Manager) *workflows.OutModule[Input, Order] {
	var (
		proxyManager   *proxies.Manager
		accountManager *accounts.Manager
	)
	return workflows.NewModule(Name,
		func(in Input, deps workflows.Deps) (workflows.Instance, error) {
			return NewContext(in, proxyManager, accountManager, emailManager), nil
		}).
		WithResources(func(managers map[leasing.Kind]any) error {
			var err error
			if proxyManager, err = workflows.Resource[*proxies.Manager](managers, proxies.Kind); err != nil {
				return err
			}
			accountManager, err = workflows.Resource[*accounts.Manager](managers, accounts.Kind)
			return err
		}).
		Returns[Order]()
}
