# Contributing to Rogojin

Thank you for your interest in contributing — contributions of all kinds are welcome: bug reports, documentation fixes, new selection strategies, persistence adapters, and improvements to the core engine.

New to the project (or to open source)? You're explicitly welcome here. Issues labeled [`good first issue`](https://github.com/ntakezo/rogojin/labels/good%20first%20issue) are scoped to be approachable without deep knowledge of the codebase, and issues labeled [`help wanted`](https://github.com/ntakezo/rogojin/labels/help%20wanted) are where a hand is most useful. If you're unsure where to start, open an issue and say so — happy to help you find something that fits.

## Getting started

1. Fork and clone the repository.
2. Install [Go 1.25+](https://go.dev/dl/). The SQLite adapter tests need cgo, so make sure a C compiler is available (`CGO_ENABLED=1` is Go's default).
3. Verify your setup:

   ```sh
   go test -race ./...
   ```

4. Run the end-to-end example to see the framework working:

   ```sh
   cd _examples && go run ./workflows/example
   ```

The [README](./README.md) explains the architecture; the package doc comments (`go doc ./tasks`, `go doc ./workflows`, ...) are the API reference.

## Reporting bugs and proposing features

- **Bugs:** open an issue with a minimal reproduction — ideally a failing test. Include your Go version and OS.
- **Features:** open an issue describing the use case before writing code, especially for anything that adds API surface. Rogojin deliberately stays minimal: features land when a real consumer needs them, not speculatively. A short discussion first saves you from building something that won't be merged.
- **Documentation:** unclear docs are bugs too. Issues labeled `documentation` are a great way to make a first contribution.

## Making changes

1. Create a branch from `main`.
2. Make your change, following the [conventions](./CONVENTIONS.md). In short:
   - Document the what and why, not the how, using [Go doc comment conventions](https://go.dev/doc/comment).
   - Keep changes surgical — don't reformat or refactor code unrelated to your change.
   - No speculative code: build what the change needs, nothing ahead of it.
3. Add or update tests. Tests should encode *why* the behavior matters, not just what the code does — see the existing tests for the style. Business-logic changes need a test that fails without them.
4. Run the checks locally before pushing:

   ```sh
   gofmt -l .          # must print nothing
   go vet ./...
   go test -race ./...
   ```

5. Open a pull request against `main`. Describe what the change does and why; link the issue it addresses. CI runs the race-enabled test suite on every PR, and merging requires it to pass.

Small, focused PRs are reviewed quickly. If your change grew beyond one concern, split it.

## Changing the scaffolder

The `rogojin new` CLI (`cmd/rogojin`) renders a runnable workflow from the templates in `internal/scaffold/templates`. Those templates **reproduce framework surface** — the `workflows.Workflow` and `Instance` interfaces, the opt-in capabilities (`Snapshotter`, `Outputter`, `Teardowner`), and the wiring in `main` (`tasks.NewService`, `proxies.NewManager`, the SQLite constructors). This makes the scaffolder a maintenance surface that many feature changes touch indirectly:

- **If you change surface the templates reproduce, update the templates in the same PR.** Renaming a type, adding an interface method, or changing a constructor signature will leave the generated code stale. `TestGeneratedCodeCompiles` renders every flag combination and runs `go vet` against the real packages, so a change that breaks the templates fails CI — but it only catches *compile* breakage. Conceptual drift (the example adopts a better pattern the templates don't) is not caught; keep the templates and `_examples` in step.
- **A new opt-in capability gets a `--no-` flag, not unconditional code.** The existing flags (`--no-durable`, `--no-output`, `--no-proxy`, and the two persistence flags) each gate one feature so a generated tree never carries code it cannot use. Match that: gate the feature behind a flag, add it to the `validCombos` matrix in the test, and reject any incoherent combination in `Options.Validate` rather than emit dead wiring.
- **Keep generated code hand-written quality.** `Render` runs every file through `go/format`, so template conditionals can be coarse without leaving whitespace scars — write the templates for readability and let the format pass clean up.

## Versioning and releases

Rogojin follows [Semantic Versioning](https://semver.org); see the [README's versioning section](./README.md#versioning). Maintainers cut releases by tagging `main` — contributors never need to touch version numbers.

## Code of Conduct

This project follows the [Contributor Covenant](./CODE_OF_CONDUCT.md). By participating, you agree to uphold it.
