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

## FAQ

### Whats it for?

The portal to the internet is built for humans. Bots are excluded from that interface by websites -- but they don't have to be. Rogojin gives you all the tools to perform actions as a bot while presenting yourself as a human in a browser.

usecases:

- ecommerce automation(ACO)
- web scraping

### Why not just use a browser agent?

inference and browsers at scale are costly. if speed and/or cost is a concern, request based automation is often the answer. moreover, rogojin lends itself nicely to generating request based bots on top of your existing browser fleet or hybrid workflow approaches.

### Can i use a browser agent with Rogojin?

yes. although we provide many primitives and tooling for request based automation, we intentionally leave it up to the user to decide what medium they prefer to drive an automation with. You can still benefit from the task orchestration, identity primitives, session management and more.

## Isn't maintaining request based bots laborious?

Before agents this task was very costly in labor. Now with agents you can build and maintain multiple platforms at once.

---

## Installation

Requires Go 1.27+; the SQLite adapter needs cgo.

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

## Documentation

The [API reference](https://pkg.go.dev/github.com/ntakezo/rogojin) is the documentation — start with `workflows` for the model, `tasks` for the runtime. Every package stands alone and carries a doc comment saying what it owns and why.

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
