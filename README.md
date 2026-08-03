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
You write a workflow as a graph of small states; Rogojin checkpoints progress before every state, so a crash, restart, or deliberate suspend never loses a session — the task resumes exactly where it left off.

A state is just a method — no DSL, nothing generated at runtime. Return the next state to advance, `nil` to complete:

```go
func (r *run) fetch(ctx context.Context) (*workflows.State, error) {
	// fetch the page, stash the result on r...
	return workflows.Next(process), nil
}
```

Built-in modules cover the unglamorous parts of site automation:

- **Durability** — every transition checkpointed; suspend, resume, kill, and recover at state boundaries.
- **Proxies** — per-task leasing with round-robin or Thompson-sampling selection, concurrency caps, and sticky locks.
- **Comms** — typed pub/sub bus for inter-task coordination.
- **Persistence** — a small byte-store interface; SQLite adapters ship in the box, swap in anything else.

## Getting Started

### Install

Requires Go 1.25+.

```sh
go get github.com/ntakezo/rogojin
```

### Scaffold a workflow

Generate a runnable skeleton from inside your module:

```sh
go install github.com/ntakezo/rogojin/cmd/rogojin@latest
rogojin new checkout
go mod tidy
go run ./checkout/cmd/run
```

`rogojin new <name>` emits a full workflow package wired onto a SQLite-backed task service; flags like `--no-proxy`, `--no-durable`, and `--no-task-persistence` subtract the pieces you don't need.

Each request the workflow makes is one file under `<name>/requests/`, holding a pure function a state calls. `new` writes the first; add the rest one at a time, as you find them:

```sh
rogojin request checkout add-to-cart
rogojin request checkout submit-checkout
```

Neither command overwrites a file, so a request you have edited survives a repeated call. Pass `--force` to replace one deliberately.

Point `request` at a [powhttp](https://powhttp.com) capture and it writes the exchange as it actually happened — same URL, same header and pseudo-header order, same body bytes:

```sh
rogojin request checkout add-to-cart --entry 01KYWPK8JH135M795ME2G8ACZQ
```

`--session` selects the capture session (default `active`) and `--powhttp` the data API address (default `$POWHTTP_BASE_URL`, then `http://localhost:7777`).

A JSON request body is typed too, as its own `XBody` type reached through the request's `Body` field, with fields in the order the capture sent them — so setting a value is a struct field rather than an edit inside a string literal. Because only `Body` is marshaled, a URL or header value you add alongside it cannot end up in the payload. Bodies of any other kind keep their captured bytes verbatim.

The response is typed the same way, in the same file — a JSON reply becomes the struct the state unmarshals into:

```go
type SearchResponse struct {
	Products []struct {
		Name     string `json:"name"`
		ImageURL string `json:"imageUrl"`
	} `json:"products"`
	TotalResults int64 `json:"totalResults"`
}
```

A response with no shape to infer — HTML, an image, a body that does not match its content type — gets an empty struct and a comment saying why.

Every request value arrives as the captured literal, including the ones that only held for that session: cookies, tokens, nonces. Rewrite those into `Request` fields before the workflow depends on them. What the capture buys you is that the static half — the URL, the header and pseudo-header order, the body bytes, the response shape — is right the first time.

Generated requests are built on [fhttp](https://github.com/bogdanfinn/fhttp), which sends headers and HTTP/2 pseudo-headers in the order you give it. `go mod tidy` resolves it to the version rogojin pins and tests its templates against; run `go get github.com/bogdanfinn/fhttp@latest` in your module to move ahead of that.

### Run the example

A complete workflow — proxy leasing, durable snapshots, recovery, and inter-task coordination against a canned site:

```sh
cd _examples && go run ./workflows/example
```

### Learn the API

The [API reference](https://pkg.go.dev/github.com/ntakezo/rogojin) documents each package: `workflows` (the programming model), `tasks` (the runtime), `comms`, `proxies`, and `persistence`.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for how to build, test, and propose changes.

## License

[MIT](./LICENSE)
