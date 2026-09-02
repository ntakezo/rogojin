---
name: rogojin-new
description: Bootstrap a rogojin workflow package with the rogojin CLI by mapping the use case onto scaffold flags. Use when the user wants to scaffold, bootstrap, generate, or start a new workflow, bot, or automation project on rogojin, or asks which rogojin new flags fit their use case.
---

# Scaffold a rogojin workflow

`rogojin new <name>` generates a runnable workflow package. The default scaffold includes **everything**; flags subtract features. Your job is to translate the user's use case into the right flag set, run the CLI, and orient them in what came out.

## 1. Map the use case to flags

Work through these questions — from what the user already said where possible, asking only what you cannot infer:

| Question | If yes | If no |
|---|---|---|
| Does each task need its own egress proxy? (scraping, sneakers, anything rate-limited or fingerprinted) | keep | `--no-proxy` |
| Does the flow act *as someone* on a site — log in, hold a session, keep an identity across a run? | keep | `--no-accounts` |
| Does it pay for anything — checkout, subscription, top-up? | keep | `--no-payments` |
| Does it wait on mail — verification links, OTP codes, order confirmations? | keep | `--no-email` |
| Should inventory (proxies, accounts, …) and task history survive the process? | `--repo sqlite` (default) | `--repo memory` |
| Will several processes share the work — a fleet over one database? | `--repo postgres` | `--repo sqlite` (default) |
| Must a crashed task resume mid-run instead of restarting? | keep (durable) | `--no-durable` |

Constraints and defaults:

- **Durable requires a repository.** `--repo memory --no-durable` go together; the CLI rejects durable-in-memory with an error saying so.
- **Postgres is the fleet profile.** The generated main takes `-db` (a DSN for postgres, a file path for sqlite) and reads `ROGOJIN_NODE` for a stable node name; every process pointed at one database coordinates through it.
- **Email and accounts are independent.** Together, the workflow reaches its inbox through the locked account's forwarding email; email alone listens on an inbox named in the task's `Input.EmailID`.
- **Quick experiment / throwaway:** `--repo memory --no-durable`, and subtract whatever the experiment doesn't touch.
- **Not sure?** Keep the feature. Subtracting later means deleting generated code you can see; adding later means re-scaffolding.

## 2. Run it

From the root of the Go module that will own the workflow (a `go.mod` must exist at or above the working directory — `go mod init` first if not):

```sh
go run github.com/ntakezo/rogojin/cmd/rogojin@latest new <name> [flags]
```

Inside the rogojin repo itself, use `go run ./cmd/rogojin new <name> [flags]`.

The name becomes the package and directory (lowercased, non-identifier characters dropped), so prefer something short like `checkout` or `dropmonitor`. The CLI refuses to overwrite existing files — a re-scaffold needs a fresh name or a cleaned directory.

## 3. Orient the user in what came out

Generated layout (under `<pkg>/`):

- `<pkg>.go` — the module declaration; rarely edited.
- `states/context.go` — `Input`, `Result`, the durable state, and the lazy resource helpers (`client`, `profile`, `payment`, `inbox`). Shape `Input`/`Result` first.
- `states/graph.go`, `states/fetch.go`, `states/process.go` — the state graph and two starter states. This is where the actual automation gets written: rename/add states here. `fetch` calls a request function and stores the body; `process` reports it.
- `requests/get_page.go` — the target's request functions, one per file, each a pure function of `(ctx, client, typed request struct)` returning the raw `*http.Response`, with an exported response struct for the caller to decode into. Add one file per endpoint here.
- `common/client.go` — `NewClient(proxyURL)`, the one seam every state builds its HTTP client through (stdlib `net/http`; swap in a fingerprinting client here). Shared request plumbing lives here.
- `cmd/run/main.go` — wiring plus one task run. In memory mode it seeds placeholder inventory (`proxy-1`, `account-1`, …) — point the user at those to replace. In sqlite mode inventory persists in `<pkg>.db`; it starts empty, so real proxies/accounts/payments/inboxes must be added once.

Verify with `go vet ./...`, then `go run ./<pkg>/cmd/run` (expect a fetch of example.com to succeed; with seeded placeholder proxies/inboxes the run will fail at that resource until real ones are supplied — say so rather than debugging it).
