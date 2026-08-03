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
- **A third-party import in a template is a dependency of this module.** The generated `requests` package imports `fhttp`, so `TestGeneratedCodeCompiles` can only resolve it if this repo's `go.mod` and `go.sum` carry it — the temp modules it builds require whatever version we pin. Nothing in rogojin imports fhttp, so a blank import in `scaffold_test.go` anchors it against `go mod tidy`, and `generatedDeps` in that test lists every such module. Adding another third-party import to a template means adding it to both.

### The requests layer

Generated workflows split HTTP into two packages: `requests` holds one pure function per request — it takes a client and a typed request struct, fires exactly one call, and returns the raw response — while `states` owns the decode, the context mutation, and the next-state decision. Keep that split; a state that builds its own `*http.Request` inline is the thing this layout exists to prevent.

`rogojin request --force` replaces exactly one request file and nothing around it — it is not a re-scaffold, and it will not create the workflow the request belongs to. Keep that scope: regenerating a request whose capture has changed must never put the states that call it out of sync silently, so anything broader than one file belongs in a different verb.

Every request function has the same signature, without exception, so a state calls any of them the same way:

```go
func X(ctx context.Context, client *http.Client, r XRequest) (*http.Response, error)
```

Three rules make that format mechanical rather than conventional. `XRequest` is the single input type. A structured body is always `Body XBody`, a named type in the same file, and it is the only thing marshaled. And **`XRequest` never carries a wire tag** — that is what makes a URL or header field added to it structurally incapable of reaching the payload, rather than relying on someone remembering `json:"-"`. `TestRequestStructCarriesNoWireTags` enforces it.

A request with no structured body has no `Body` field: a form or sensor body keeps its captured bytes as a literal, and turning an exact literal into a field a caller can get wrong buys nothing. So the contents of `XRequest` vary; the signature and the rules do not.

Two commands write into that package, from two templates. `rogojin new` renders `templates/requests/fetch.go.tmpl` as part of the whole-tree render driven by `Options.outputs()`, and it carries the package doc. `rogojin request` renders `templates/requests/request.go.tmpl` on its own, through `RenderRequest`/`WriteRequest`, and is deliberately outside that map — it adds a single file to a workflow that already exists and is meant to be run once per request. Keep the second template free of a package comment, and keep both in step: a change to how a request is written belongs in both, or the two vintages diverge inside one directory.

Keeping those imports current is not cosmetic: the templates name no version, so a fresh consumer's module resolution picks the floor this repo pins, and **whatever rogojin requires is the version every scaffolded workflow gets**. Three things hold that in place — Dependabot bumps both modules weekly, `TestGeneratedDepsMatchExamples` fails if the root and `_examples` pins drift apart, and the `canary` workflow builds the scaffolder and the examples against the newest release so upstream breakage surfaces on release rather than at the next bump. Bumps stay human-merged: a transport change that alters TLS or HTTP/2 behaviour compiles cleanly and fails in production instead.

Generated requests write headers with `Header.Append`, never `Set` plus a manual `HeaderOrderKey`. Two reasons, both found the hard way. `Append` records each name in the send order as it writes it, so the written order *is* the sent order; a hand-built `HeaderOrderKey` has to be **lowercase**, because fhttp looks the slice up with `strings.ToLower` — a canonical-cased slice matches nothing and the headers silently fall back to alphabetical. `Append` also keeps repeated fields, which matters because HTTP/2 splits a long cookie into several `cookie` fields that `Set` would collapse into one.

`PHeaderOrderKey` is still set explicitly, and is load-bearing beyond fingerprinting: fhttp emits HTTP/2 pseudo-headers unordered without it, and some servers route such a request straight to a 404.

### Generating from a capture

`internal/capture` reads recorded exchanges; `rogojin request --entry` renders one through `templates/requests/captured.go.tmpl`. The adapter normalizes nothing — header order, repeated fields, and body bytes come through as captured, because each is a fingerprinting signal and a value that looks wrong is a value the client really sent. The single exception is `Content-Length`, which the transport derives from the body it is handed; a stale literal would contradict it.

Adding another capture source means another file in `internal/capture` producing a `capture.Entry`. The renderer sees only that type, so nothing in `internal/scaffold` should learn where an entry came from.

### Typing a request body

A JSON request body types the `XRequest` struct and is marshaled back out of it, the way `_examples`' `submit_checkout.go` does by hand. Request and response bodies are both identified by their content type and both lean on the same inference, so `bodyTypers` in `internal/scaffold/body.go` is one dispatch table whose entries declare both directions side by side: `{matches, request, response}`. **Supporting another media type means appending one entry**; nothing else in the renderer knows what a payload looks like. Anything nothing handles keeps its captured bytes — a literal for a request, which is always exact, and an untyped `struct{}` for a response.

A request body differs from a response in one way that drives its render: **it goes back on the wire**, so two things a response never has to care about become correctness issues.

`encoding/json` writes struct fields in declaration order — so a request body is inferred with `jsontype.Options.KeepOrder`, which declares fields in the order the capture carried its keys, at every level, merging order across array elements. Without that, a captured `{"challengeSessions":…,"layoutName":…}` goes out reordered. For the same reason a request body never collapses to a `map` however wide it is: `encoding/json` sorts a map's keys.

The generated function encodes with `SetEscapeHTML(false)` rather than calling `json.Marshal`. Marshal escapes `<`, `>` and `&` to `\u00xx`, which grew one real captured body from 4762 to 5312 bytes. With escaping off, three real captured bodies round-trip byte for byte.

A body that types to an empty struct falls back to its literal: an unbindable key would otherwise leave a struct that marshals to `{}` and silently sends a different body than the one captured.

### Typing a response

The exported `XResponse` type in a captured request is inferred from the captured response body, so a state can unmarshal into it without anyone reading the payload by hand. What "typed" means depends on the media type — a JSON object is a struct, an HTML document has no shape at all — which is what the `bodyTypers` table above dispatches on; a response additionally falls back to an untyped `struct{}` for anything nothing handles, because servers do mislabel bodies.

The inference itself lives in `internal/jsontype`, which owns what go-jsonstruct used to provide: one token walk over the document records both the JSON kinds observed at every position *and* the order each object's keys first appeared in, then renders the bare type expression directly — no generated file to parse back apart. Array elements are *merged* rather than sampled, so an endpoint that omits a key on some elements still contributes it, and a key that was absent from some observations earns an `omitempty` and, for objects, a pointer. A generated response says which fields the endpoint actually guarantees. RFC 3339 strings become `time.Time`, which is why an inferred `Type` reports the imports it needs and the template has a conditional import block. Behaviors that came from real captures, each pinned by a test:

- **Unbindable JSON keys.** The empty key, and keys holding a character the tag grammar reserves, get no field — a tag like `json:""` or `json:",omitempty"` would silently bind under the field's own name instead — and contribute nothing, imports included.
- **Colliding names.** Two keys reducing to one identifier would emit the same field twice and not compile, so the later one takes a numeric suffix.
- **Dictionaries.** With `Options.DictionaryFields` set, an object of at least that many same-shaped keys becomes a `map[string]T` (the scaffold sets 24 for responses, never for request bodies). A capture used in testing returned a translation payload of 222 keys; a struct that wide misses every key the capture happened not to contain and cannot be looked up by a value known only at run time.

Field naming runs the key's components together and capitalizes initialisms whole, so a key gives `CartID` and `ImageURL` rather than `Cart_Id`. Without `KeepOrder`, fields sort by JSON key, not by the Go name derived from it.

Anything that does not parse as the media type it claims falls back to the empty placeholder rather than guessing, as does a body holding more than one JSON document. This is not defensive coding — servers really do mislabel bodies, and a font served as `application/x-www-form-urlencoded` turned up in the first capture this was tested against.

Values are interpolated through the template's `quote` function (`strconv.Quote`), never raw — a captured header full of quotes and backslashes is normal, and must not be able to break out of the generated source. Bodies go through `goStringLiteral`, which prefers a backtick literal for readability but falls back to quoting, since a raw literal silently discards carriage returns and cannot contain a backtick.

## Versioning and releases

Rogojin follows [Semantic Versioning](https://semver.org); see the [README's versioning section](./README.md#versioning). Maintainers cut releases by tagging `main` — contributors never need to touch version numbers.

## Code of Conduct

This project follows the [Contributor Covenant](./CODE_OF_CONDUCT.md). By participating, you agree to uphold it.
