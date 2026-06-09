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

## Versioning and releases

Rogojin follows [Semantic Versioning](https://semver.org); see the [README's versioning section](./README.md#versioning). Maintainers cut releases by tagging `main` — contributors never need to touch version numbers.

## Code of Conduct

This project follows the [Contributor Covenant](./CODE_OF_CONDUCT.md). By participating, you agree to uphold it.
