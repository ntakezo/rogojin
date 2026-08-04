package states

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/cookiejar"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/proxies"
	"github.com/ntakezo/rogojin/workflows"
)

// Input is the immutable per-task input the caller supplies at CreateTask. Add
// the fields your workflow needs; they are held by value and never mutated.
type Input struct{}

// Output is what the workflow produces, set by the terminal state and marshaled
// on clean completion. Add the fields your workflow returns.
type Output struct{}

// running is the state the workflow accumulates as it advances, plus its side
// effects. It is a pointer on Context so handlers mutate one shared copy.
type running struct {
	proxies *proxies.Manager
	taskID  string
	lease   *proxies.Lease
	client  *http.Client
	bus     comms.Bus

	output Output
}

// Context is the receiver shared across every state. input is immutable per
// task; r carries the evolving state and side effects.
type Context struct {
	input Input
	r     *running
}

// NewContext builds a fresh context for one task.
func NewContext(input Input, deps workflows.Deps, manager *proxies.Manager) *Context {
	return &Context{
		input: input,
		r: &running{
			proxies: manager,
			taskID:  deps.TaskID,
			bus:     deps.Bus,
		},
	}
}

// Output marshals the workflow's Output to JSON. The engine reads it only on
// clean completion; a run that is killed or errors produces none.
func (c *Context) Output() ([]byte, error) {
	return json.Marshal(c.r.output)
}

// client builds the task's HTTP client on first use, backed by an isolated
// cookie jar so each task carries its own session. The proxy is leased here
// too, so a recovered task acquires its own wherever in the graph it resumes.
func (c *Context) client(ctx context.Context) (*http.Client, error) {
	if c.r.client != nil {
		return c.r.client, nil
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("new cookie jar: %w", err)
	}
	lease, err := c.r.proxies.Acquire(ctx, c.r.taskID)
	if err != nil {
		return nil, fmt.Errorf("acquire proxy: %w", err)
	}
	proxyURL, err := url.Parse(lease.Proxy().URL)
	if err != nil {
		lease.Release(false)
		return nil, fmt.Errorf("parse proxy url: %w", err)
	}
	c.r.lease = lease
	c.r.client = &http.Client{Jar: jar, Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	return c.r.client, nil
}

// Teardown releases the task's proxy lease exactly once when the run exits,
// reporting success on the absence of a run error.
func (c *Context) Teardown(ctx context.Context, status workflows.Status, runErr error) error {
	if c.r.lease == nil {
		return nil
	}
	return c.r.lease.Release(runErr == nil)
}

// snapshot is the JSON persisted for recovery: every value a resumed state
// needs, and nothing else. Side effects are rebuilt on restore, not serialized.
type snapshot struct {
	Input Input `json:"input"`
}

// Snapshot serializes the durable context. The engine calls it before entering
// each state, so it must be valid as the entry of the state it is taken before.
func (c *Context) Snapshot() ([]byte, error) {
	return json.Marshal(snapshot{Input: c.input})
}

// RestoreContext rebuilds a context from a JSON snapshot; the lease and client
// are re-acquired lazily on first use.
func RestoreContext(deps workflows.Deps, blob []byte, manager *proxies.Manager) (*Context, error) {
	var s snapshot
	if err := json.Unmarshal(blob, &s); err != nil {
		return nil, err
	}
	return NewContext(s.Input, deps, manager), nil
}
