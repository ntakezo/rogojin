<div align="center">

# rogojin

rogojin is an end-to-end web automation framework in Go, written to be written by agents.

<h3>

[API Reference](https://pkg.go.dev/github.com/ntakezo/rogojin) | [Contributing](./CONTRIBUTING.md) | [Example](_examples/workflows/example)

</h3>

[![Go Reference](https://pkg.go.dev/badge/github.com/ntakezo/rogojin.svg)](https://pkg.go.dev/github.com/ntakezo/rogojin)
[![Tests](https://github.com/ntakezo/rogojin/actions/workflows/test.yml/badge.svg)](https://github.com/ntakezo/rogojin/actions/workflows/test.yml)
[![GitHub License](https://img.shields.io/github/license/ntakezo/rogojin)](./LICENSE)

</div>

---

The portal to the internet is built for humans. Websites exclude bots from that interface — but they don't have to be excluded. rogojin gives you the tools to act as a bot while presenting as a human in a browser.

## Use cases

- **Ecommerce automation (ACO)** — carting and checkout under contention
- **Scraping past the login** — targets whose data sits behind a session, a multi-step flow, or a queue, rather than one page a stateless crawler can hit.
- **Account lifecycle** — signup, verification, warming: waiting on a mailed link or code is a state the task resumes from, not a sleep it burns a process on.
- **Monitors and drops** — long-lived watchers that survive restarts and hand off to a checkout the moment something lands.

---

## Installation

Requires Go 1.27+; the SQLite adapter needs cgo. The Postgres adapter
(`postgres.Open` on a `postgres://` DSN, pure Go) is the store several nodes
share when one process is not enough.

```sh
go get github.com/ntakezo/rogojin
go install github.com/ntakezo/rogojin/cmd/rogojin@latest
```

## Getting started

From inside the module that will own the workflow:

```sh
rogojin new checkout
go run ./checkout/cmd/run
```

That is a running task, not a skeleton — edit it into the site you actually mean. `rogojin help` lists the flags that subtract what you don't need.

To watch the whole thing work first:

```sh
cd _examples && go run ./workflows/example
```

## Deployment profiles

One codebase, three ways to run it — the difference is only which repository the managers open:

- **Embedded** (`--repo memory`) — everything in one process, nothing survives it. Experiments and tests.
- **Single node** (`--repo sqlite`, the default) — one file holds every store; tasks, locks, and inventory survive restarts.
- **Fleet** (`--repo postgres`) — N processes over one database. The store is the authority: task claims decide who runs what, leases and locks decide who holds what, and the effect log makes a side effect happen once fleet-wide, so two nodes can't run the same task or charge the same instrument. Kill a node and its work is claimable the moment its lease lapses — `_examples/workflows/fleet` is that story in one file, runnable on a shared sqlite file. For tasks on different nodes to hear each other, add the [`bus/redis`](./bus/redis) sub-module: a `comms.Bus` and `comms.Notifier` over Redis pub/sub, so topic messages and capacity wakeups cross nodes the way they already cross goroutines. Workflow code doesn't change — the typed `comms.Topic` layer reads identically over either transport.

Pre-1.0 note: this release collapsed the schema histories; a database file from an earlier release is refused at open — recreate it.

## Documentation

The [API reference](https://pkg.go.dev/github.com/ntakezo/rogojin) is the documentation — start with `workflows` for the model, `tasks` for the runtime. Every package stands alone and carries a doc comment saying what it owns and why.

## FAQ

### Why not just use a browser agent?

Inference and browsers at scale are costly. If speed or cost is a concern, request-based automation is often the answer. rogojin also lends itself to generating request-based bots on top of an existing browser fleet, or to hybrid approaches.

### Can I use a browser agent with rogojin?

Yes. We provide primitives and tooling for request-based automation, but the medium you drive an automation with is deliberately left to you — the task orchestration, identity primitives, and session management are yours either way.

### Isn't maintaining request-based bots laborious?

Before agents it was, in labor. Now you can build and maintain several platforms at once.

## Versioning

[SemVer](https://semver.org), pre-1.0: minor releases may break API, patch releases won't.

## Contributing

Read [CONTRIBUTING.md](./CONTRIBUTING.md) first; [`good first issue`](https://github.com/ntakezo/rogojin/labels/good%20first%20issue) is scoped to be approachable. Short version: no speculative code, no unrelated churn, a failing test for every behavior change, and `internal/scaffold` templates updated in the same PR as the surface they reproduce. Writing a PR with an agent is fine; shipping a diff you haven't read is not.

```sh
gofmt -l .          # must print nothing
go vet ./...
go test -race ./...
```

## License

[MIT](./LICENSE)
