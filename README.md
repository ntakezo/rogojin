<div class="title-block" style="text-align: center;" align="center">

# Rogojin — durable web automation workflows

[![Go Reference](https://pkg.go.dev/badge/github.com/ntakezo/rogojin.svg)](https://pkg.go.dev/github.com/ntakezo/rogojin)
[![GitHub License](https://img.shields.io/github/license/ntakezo/rogojin)](./LICENSE)

**[Introduction](#introduction) &nbsp;&nbsp;&bull;&nbsp;&nbsp;**
**[Getting Started](#getting-started) &nbsp;&nbsp;&bull;&nbsp;&nbsp;**
**[Contributing](#contributing) &nbsp;&nbsp;&bull;&nbsp;&nbsp;**
**[API Reference](https://pkg.go.dev/github.com/ntakezo/rogojin)**

</div>

## Introduction

Rogojin is a Go framework for building durable, resumable automation workflows against websites.

A state is just a method:

```go
func (r *run) fetch(ctx context.Context) (*workflows.State, error) {
	// fetch the page, stash the result on r...
	return workflows.Next(process), nil
}
```

Built-in modules cover the unglamorous parts of site automation:

- **Durability** — every transition checkpointed; suspend, resume, kill, and recover at state boundaries. An external side effect a re-run must not repeat rides in a durable effect log (`workflows.Do`), so a retried or recovered state replays a submitted order instead of resubmitting it.
- **Policy** — retries with backoff and timeouts are declared per state on the graph (`workflows.On(state, handler, workflows.Retry(3, workflows.ExpBackoff(…)))`), never implemented inside handlers.
- **Typed results** — a module declares its output once (`.Returns[Order]()`), and `tasks.Create` returns a handle whose `Start` and `Output` carry a decoded `Order` — input and output types both inferred, no type arguments at the call site.
- **Proxies** — per-task leasing with round-robin or Thompson-sampling selection, per-proxy and per-group holder caps, and sticky locks that outlive the process.
- **Accounts** — the same leasing, groups, holder caps, and sticky locks for site logins. What an account _is_ belongs to the workflow: its fields travel as JSON, so a new workflow needs no schema change.
- **Payments** — the same leasing again, for the payment instruments a checkout settles against: one holder at a time by default, so no two tasks charge the same instrument at once, and a sticky lock so a resumed checkout comes back to the instrument it started on. Payment data travels as JSON the library never reads; encrypting it at rest is the store's job.
- **Groups** — named sets of proxies, of accounts, of payments, and of tasks. A task carries one assignment per resource kind and inherits its task group's for any kind it names none of, so a pool is assigned once and every member follows; each proxy group rotates through its own selection strategy. When seeding a repository directly, save groups before the resources that name them — a resource pointing at a group the store doesn't hold fails the manager's construction.
- **Comms** — typed pub/sub bus for inter-task coordination.
- **Persistence** — a small byte-store interface; SQLite adapters ship in the box, swap in anything else. Each adapter records its migrations under its own name, so they can share one database file or take one each.

## Getting Started

### Install

Requires Go 1.27+ (typed task results use generic methods).

```sh
go get github.com/ntakezo/rogojin
```

### Scaffold a workflow

Generate a runnable skeleton from inside your module:

```sh
go install github.com/ntakezo/rogojin/cmd/rogojin@latest
rogojin new checkout
go run ./checkout/cmd/run
```

`rogojin new <name>` emits a full workflow package — proxy leasing, site accounts, payment instruments, inbox listening, crash recovery — wired onto a SQLite-backed task service. Flags subtract the pieces you don't need (`--no-proxy`, `--no-accounts`, `--no-payments`, `--no-email`, `--no-durable`), and `--repo memory` swaps SQLite for nil repositories so everything runs in memory.

### Run the example

A complete workflow — proxy leasing, durable snapshots, recovery, and inter-task coordination against a canned site:

```sh
cd _examples && go run ./workflows/example
```

### Learn the API

The [API reference](https://pkg.go.dev/github.com/ntakezo/rogojin) documents each package: `workflows` (the programming model), `tasks` (the runtime), `comms`, `proxies`, `accounts`, `payments`, `email`, and `sqlite`. `proxies`, `accounts`, and `payments` are all thin layers over `leasing`, which owns the pooling, grouping, and locking they share — build a fourth resource kind on it the same way.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for how to build, test, and propose changes.

## License

[MIT](./LICENSE)
