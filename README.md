# <img src="assets/rogojin.jpg" alt="Logo" height="48"> Rogojin

Rogojin is a Go framework for building durable, resumable automation workflows against websites. You write a workflow as a graph of small states; Rogojin runs it as a task that checkpoints its progress before every state, so a crash, restart, or deliberate suspend never loses a session — the task picks up exactly where it left off. Built-in modules cover the unglamorous parts of site automation: proxy rotation and scoring, inter-task coordination, and pluggable persistence.

## Why Rogojin

- **Durable by default.** Every state transition is checkpointed. Kill the process mid-run and recover the task at the next unprocessed state, with its accumulated context intact. If a checkpoint cannot be written, the run stops _before_ the next state executes — work is never performed without its resume point persisted first.
- **Workflows are plain Go.** A state is just a method: `func(ctx context.Context) (*workflows.State, error)`. Return the next state to advance, `nil` to complete. No DSL, no code generation.
- **Lifecycle control.** Start, suspend, resume, and kill tasks at state boundaries. A suspended task persists durably and recovers paused, exactly where it stopped.
- **Proxy management included.** Lease proxies per task with pluggable selection (round-robin, or Thompson-sampling that learns which proxies succeed), per-proxy concurrency caps, and durable task-to-proxy locks for sticky sessions.
- **Bring your own storage.** Persistence is a small interface (a dumb byte store). SQLite implementations ship in the box; swap in anything else by implementing the repository.

## Installation

Requires **Go 1.25+**.

```sh
go get github.com/ntakezo/rogojin
```

The core framework is pure Go. The optional SQLite persistence adapters (`persistence/tasksqlite`, `persistence/proxysqlite`) use [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3), which needs cgo: build with `CGO_ENABLED=1` and a C compiler installed. If you implement your own `Repository`, there is no cgo requirement.

## Quick start

A workflow is a type that validates its input and builds a per-task instance; the instance exposes a graph of states. Below, a two-state workflow fetches a page and processes it.

```go
package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/persistence/tasksqlite"
	"github.com/ntakezo/rogojin/tasks"
	"github.com/ntakezo/rogojin/workflows"
)

// Each state's name lives next to its handler.
const (
	fetch   workflows.State = "fetch"
	process workflows.State = "process"
)

// scrape is the workflow module: it validates input and builds instances.
type scrape struct{}

func (scrape) ID() string { return "scrape" }

func (scrape) ValidateInput(input any) error {
	if _, ok := input.(string); !ok {
		return fmt.Errorf("expected a URL string, got %T", input)
	}
	return nil
}

func (scrape) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	return &run{url: input.(string)}, nil
}

// run is one live task: its fields are the state shared across handlers.
type run struct {
	url  string
	body string
}

func (r *run) Graph() workflows.Graph {
	return workflows.NewGraph(fetch, workflows.States{
		fetch:   r.fetch,
		process: r.process,
	})
}

func (r *run) fetch(ctx context.Context) (*workflows.State, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return nil, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	r.body = string(body)
	return workflows.Next(process), nil
}

func (r *run) process(ctx context.Context) (*workflows.State, error) {
	fmt.Printf("fetched %d bytes from %s\n", len(r.body), r.url)
	return nil, nil // a nil next state completes the task
}

func main() {
	ctx := context.Background()

	repo, err := tasksqlite.NewSQLite("tasks.db")
	if err != nil {
		log.Fatal(err)
	}
	defer repo.Close()

	svc := tasks.NewService(repo, comms.NewBus())
	if err := svc.RegisterWorkflow("scrape", scrape{}); err != nil {
		log.Fatal(err)
	}

	task, err := svc.CreateTask(ctx, "scrape", "https://example.com")
	if err != nil {
		log.Fatal(err)
	}
	if err := task.Start(ctx); err != nil {
		log.Fatal(err)
	}
}
```

For a complete workflow — proxy leasing, durable snapshots, recovery, and inter-task coordination against a real (canned) site — see [`_examples/workflows/example`](./_examples/workflows/example):

```sh
cd _examples && go run ./workflows/example
```

## Concepts

### Workflows: graphs of states

A workflow module implements `workflows.Workflow`:

```go
type Workflow interface {
	ID() string
	ValidateInput(input any) error
	NewInstance(input any, deps Deps) (Instance, error)
}
```

`NewInstance` builds a fresh, per-task instance — typically a context struct whose fields carry state between handlers and whose methods are the handlers. The instance returns its graph: an initial state plus a handler per state. Handlers decide control flow at runtime; a state can branch to different next states, loop, or finish by returning `nil`.

`workflows.Deps` is what the framework injects per task: the task's ID and the comms bus. Module-level dependencies (a proxy manager, an API client) are injected into your workflow's constructor instead, so each module declares exactly what it needs.

### Tasks: the runtime

`tasks.NewService(repo, bus)` gives you the task service. Pass a nil `repo` for purely in-memory tasks — they run without checkpoints or recovery. Register workflows once, then create tasks from registered workflow IDs:

```go
task, _ := svc.CreateTask(ctx, "scrape", input)
task.Start(ctx)    // runs the graph synchronously until done, error, or kill
task.Suspend()     // parks the task at the next state boundary, durably
task.Resume()      // continues a suspended task
task.Kill()        // cancels immediately, interrupting the in-flight state
task.Status()      // "", running, suspended, killed, done
```

Suspend, resume, and kill are honored at state boundaries (kill also cancels the in-flight state's context). The terminal outcome (`done` or `killed`) is stamped on the task's durable record; records are never deleted by the framework — cleanup is yours via `svc.DeleteTask`.

### Durability: checkpoints and recovery

Durability is opt-in per workflow. Implement two more methods and your tasks survive anything:

```go
// On the instance: serialize the durable context. Called before every state.
func (r *run) Snapshot() ([]byte, error)

// On the workflow: rebuild an instance from a snapshot.
func (w scrape) RestoreInstance(deps workflows.Deps, snapshot []byte) (workflows.Instance, error)
```

Before entering each state, the engine snapshots the instance and saves it with the state about to run. After a crash or restart:

```go
recovered, _ := svc.RecoverTask(ctx, id) // or svc.RecoverAll(ctx)
recovered.Status()                       // how it was left: running, suspended, done...
recovered.Start(ctx)                     // resumes at the next unprocessed state
```

The contract: a recovered task re-enters the state whose checkpoint was last written, with the snapshot taken at that boundary. States that completed are not re-run. If a checkpoint cannot be written, the run exits with the error before executing the next state — the last successful checkpoint stands, and the task remains resumable from it.

Side effects that should not be serialized (HTTP clients, leases) are re-acquired lazily after restore; implement the optional `Teardowner` interface to release them exactly once when a run exits, whatever the outcome:

```go
func (r *run) Teardown(ctx context.Context, status workflows.Status, runErr error) error
```

### Comms: coordinating tasks

Tasks coordinate over a topic-based pub/sub bus. The `Topic[T]` wrapper gives compile-time type safety on the publish side:

```go
cookies := comms.NewTopic[string](bus, "queue-cookie")

cookies.Emit(ctx, cookie)          // publish
sub, _ := cookies.On(ctx)          // subscribe; payloads assert back to string
defer sub.Close()
cookie := (<-sub.C()).(string)
```

Delivery is at-most-once and never blocks the publisher: each subscriber has a fixed buffer, and a full buffer drops the message for that subscriber only. Subscriptions are live — they see only messages published after they were created. The in-memory bus ships in the box; `comms.Bus` is an interface, so a networked transport can be swapped in without touching workflow code.

### Proxies: leasing, scoring, locking

The `proxies.Manager` allocates proxies to tasks from a durable pool:

```go
manager, _ := proxies.NewManager(ctx, repo, proxies.NewRoundRobin(), proxies.Exclusive(), nil)

lease, _ := manager.Acquire(ctx, taskID) // blocks until a proxy frees up
defer lease.Release(success)             // records the outcome for scoring
```

- **Selection** is a strategy port. `NewRoundRobin()` spreads load evenly; `NewBayesian()` Thompson-samples each proxy's success/failure history, exploiting proven proxies while still exploring uncertain ones. Implement `Select(candidates []Proxy) (Proxy, error)` for your own.
- **Exclusivity** caps concurrent leases per proxy: `Exclusive()` for one at a time, `Capped(n)` for up to n.
- **Locks** make sessions sticky: `manager.Lock(ctx, taskID)` durably binds a task to its proxy across leases, restarts, and recoveries until `Unlock`. A `DeletionPolicy` decides what happens to a task whose locked proxy is deleted (reassign, unbind, or fail loudly).

### Persistence: bring your own store

Both the task service and the proxy manager persist through small repository interfaces — dumb byte stores with no business logic:

```go
// tasks.Repository
CreateTask, SaveCheckpoint, MarkTerminal, RecoverTask, RecoverAll, DeleteTask

// proxies.Repository
List, Save, Delete
```

SQLite implementations are provided (`persistence/tasksqlite`, `persistence/proxysqlite`), each a single-file database with the schema auto-created. Any store that can read and write these records works: Postgres, Redis, a JSON file, an in-memory map for tests (the example ships one).

## Project layout

| Package       | What it is                                                                            |
| ------------- | ------------------------------------------------------------------------------------- |
| `workflows`   | The programming model: `Workflow`, `Instance`, `Graph`, durability hooks. Types only. |
| `tasks`       | The runtime: task service, lifecycle, checkpointing engine, recovery.                 |
| `comms`       | Inter-task pub/sub: the `Bus` port, in-memory implementation, typed topics.           |
| `proxies`     | Proxy pool manager: leasing, selection strategies, locks, deletion policy.            |
| `persistence` | SQLite adapters for the `tasks` and `proxies` repository ports.                       |
| `_examples`   | A runnable end-to-end checkout workflow against a canned site.                        |

## Versioning

Rogojin follows [Semantic Versioning 2.0.0](https://semver.org), released as Go module tags of the form `vMAJOR.MINOR.PATCH`.

- **v0.x (current):** the API is still settling. Minor versions (`v0.5.0` → `v0.6.0`) may contain breaking changes, called out in the release notes; patch versions (`v0.5.0` → `v0.5.1`) are fixes only.
- **v1.0.0 and beyond:** the API is a compatibility promise. Breaking changes require a new major version, which per the [Go modules convention](https://go.dev/blog/v2-go-modules) lives at a new import path (`github.com/ntakezo/rogojin/v2`).

A release is an annotated tag on `main` (`git tag -a v0.1.0 -m "v0.1.0" && git push origin v0.1.0`); the Go module proxy picks it up from there. Untagged commits on `main` are not releases and carry no stability promise.

## License

[MIT](./LICENSE)
