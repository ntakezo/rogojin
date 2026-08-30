package accounts

import (
	"context"

	"github.com/ntakezo/rogojin/email"
	"github.com/ntakezo/rogojin/leasing"
)

// A Manager allocates accounts to tasks. See leasing.Manager.
type Manager = leasing.Manager[Account, *Account]

// A Lease is a live hold on one account. Release it exactly once when done.
type Lease = leasing.Lease[Account, *Account]

// An Option configures what NewManager wires up around the leasing core.
type Option func(*config)

type config struct {
	email *email.Manager
}

// WithEmail hands the manager the email inventory its accounts forward to.
// It closes the referential loop: the email manager learns to refuse
// deleting an inbox while an account forwarding to it is held by a live
// lease, and to report the tasks an idle durable lock would strand.
func WithEmail(m *email.Manager) Option {
	if m == nil {
		panic("accounts: WithEmail requires a manager")
	}
	return func(c *config) { c.email = m }
}

// NewManager loads the groups and pool from the repository, persisting the
// global group if absent. Given WithEmail, it also installs the account
// side of the email delete policy — see WithEmail.
func NewManager(ctx context.Context, repo Repository, opts ...Option) (*Manager, error) {
	var cfg config
	for _, opt := range opts {
		opt(&cfg)
	}
	m, err := leasing.NewManager(ctx, repo)
	if err != nil {
		return nil, err
	}
	if cfg.email != nil {
		cfg.email.GuardDeletes(func(emailID string) (held, locked []string) {
			if emailID == "" {
				return nil, nil
			}
			ref := func(a Account, g Group) bool { return ForwardingEmail(a, g) == emailID }
			return taskIDs(m.Held(ref)), taskIDs(m.Locked(ref))
		})
	}

	return m, nil
}

// taskIDs flattens assignments to their task ids, deduplicated in report
// order.
func taskIDs(assignments []leasing.Assignment) []string {
	seen := make(map[string]struct{}, len(assignments))
	ids := make([]string, 0, len(assignments))
	for _, a := range assignments {
		if _, dup := seen[a.TaskID]; dup {
			continue
		}
		seen[a.TaskID] = struct{}{}
		ids = append(ids, a.TaskID)
	}
	return ids
}
