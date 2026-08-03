// Package scaffold renders a runnable rogojin workflow package from a small set
// of embedded templates. The feature flags on Options gate the durability hooks,
// the output hook, proxy leasing, and the persistence wiring in the generated
// main, so a generated tree always compiles and never carries code it cannot use.
//
// The templates reproduce framework surface (the workflows interfaces, the opt-in
// capabilities, the service and manager constructors), so they drift when that
// surface changes. TestGeneratedCodeCompiles renders every flag combination and
// vets it against the real packages to catch compile-level drift; see the
// scaffolder section of CONTRIBUTING.md for the maintenance contract.
package scaffold

import (
	"bufio"
	"bytes"
	"embed"
	"fmt"
	"go/format"
	"go/token"
	"maps"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"text/template"
	"unicode"
	"unicode/utf8"

	"github.com/ntakezo/rogojin/internal/capture"
)

//go:embed templates
var templatesFS embed.FS

// Options configures one scaffold render. Name is the raw workflow ID; Package
// is derived from it as a valid Go identifier for the package and directory.
type Options struct {
	Name    string
	Package string

	// Durable emits Snapshot/RestoreContext/RestoreInstance for crash recovery.
	Durable bool
	// Output emits the Outputter implementation: a Result type the terminal
	// state fills and Output marshals on clean completion.
	Output bool
	// Proxy emits per-task proxy leasing and the Teardown that releases it.
	Proxy bool
	// TaskPersist wires a SQLite task repository in main; false uses a nil
	// (in-memory) repository.
	TaskPersist bool
	// ProxyPersist wires a SQLite proxy repository in main; false emits an
	// in-memory one. Meaningful only when Proxy is set.
	ProxyPersist bool
}

// templateData is what the templates see. ModulePath is the consuming module's
// path, resolved from its go.mod so generated imports point at the user's code.
type templateData struct {
	Options
	ModulePath string
}

// outputs maps each embedded template to the path it renders to, relative to the
// destination root. Paths use the package name as their leading segment.
func (o Options) outputs() map[string]string {
	return map[string]string{
		"templates/workflow.go.tmpl":       path.Join(o.Package, o.Package+".go"),
		"templates/states/context.go.tmpl": path.Join(o.Package, "states", "context.go"),
		"templates/states/graph.go.tmpl":   path.Join(o.Package, "states", "graph.go"),
		"templates/states/fetch.go.tmpl":   path.Join(o.Package, "states", "fetch.go"),
		"templates/states/process.go.tmpl": path.Join(o.Package, "states", "process.go"),
		"templates/requests/fetch.go.tmpl": path.Join(o.Package, "requests", "fetch.go"),
		"templates/cmd/run/main.go.tmpl":   path.Join(o.Package, "cmd", "run", "main.go"),
	}
}

// Validate rejects flag combinations that would generate code which lies about
// what it does, surfacing the conflict rather than emitting dead wiring.
func (o Options) Validate() error {
	if o.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if o.Package == "" {
		return fmt.Errorf("workflow name %q has no valid package identifier", o.Name)
	}
	// A keyword fails compilation and "main" cannot be imported; both would
	// otherwise surface as a confusing format error over the rendered source.
	if token.IsKeyword(o.Package) || o.Package == "main" {
		return fmt.Errorf("workflow name %q yields package %q, which is not usable as an importable package name — choose another name", o.Name, o.Package)
	}
	if o.Durable && !o.TaskPersist {
		return fmt.Errorf("a durable workflow needs task persistence: a nil task repository never writes the snapshots the durability hooks produce — pass --no-durable too")
	}
	if o.ProxyPersist && !o.Proxy {
		return fmt.Errorf("proxy persistence requires a proxy pool: set Proxy, or clear ProxyPersist")
	}
	return nil
}

// Render returns every generated file as relative-path -> formatted Go source.
// A render that produces invalid Go fails here, at format, before anything is
// written.
func Render(modulePath string, o Options) (map[string]string, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	data := templateData{Options: o, ModulePath: modulePath}

	files := make(map[string]string)
	for tmplPath, outPath := range o.outputs() {
		raw, err := templatesFS.ReadFile(tmplPath)
		if err != nil {
			return nil, fmt.Errorf("read template %s: %w", tmplPath, err)
		}
		tmpl, err := template.New(path.Base(tmplPath)).Parse(string(raw))
		if err != nil {
			return nil, fmt.Errorf("parse template %s: %w", tmplPath, err)
		}
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			return nil, fmt.Errorf("execute template %s: %w", tmplPath, err)
		}
		formatted, err := format.Source(buf.Bytes())
		if err != nil {
			return nil, fmt.Errorf("format %s: %w\n--- rendered ---\n%s", outPath, err, buf.String())
		}
		files[outPath] = string(formatted)
	}
	return files, nil
}

// Write renders the workflow and writes it under destRoot, refusing to clobber
// any file that already exists so a mistaken re-run never overwrites edits.
func Write(destRoot, modulePath string, o Options) ([]string, error) {
	files, err := Render(modulePath, o)
	if err != nil {
		return nil, err
	}

	for rel := range files {
		full := filepath.Join(destRoot, rel)
		if _, err := os.Stat(full); err == nil {
			return nil, fmt.Errorf("refusing to overwrite existing file: %s", full)
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat %s: %w", full, err)
		}
	}

	written := make([]string, 0, len(files))
	for rel, content := range files {
		full := filepath.Join(destRoot, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return nil, fmt.Errorf("mkdir for %s: %w", full, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			return nil, fmt.Errorf("write %s: %w", full, err)
		}
		written = append(written, rel)
	}
	return written, nil
}

// ModulePath reads the module path from the go.mod at or above dir, so generated
// imports resolve inside the consuming module.
func ModulePath(dir string) (string, error) {
	gomod, err := findGoMod(dir)
	if err != nil {
		return "", err
	}
	f, err := os.Open(gomod)
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if rest, ok := strings.CutPrefix(line, "module "); ok {
			return strings.TrimSpace(rest), nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("no module directive in %s", gomod)
}

// findGoMod walks up from dir to find the nearest go.mod.
func findGoMod(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(abs, "go.mod")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return "", fmt.Errorf("no go.mod found at or above %s; run inside a Go module", dir)
		}
		abs = parent
	}
}

// RequestOptions configures one request render: the workflow package the file
// lands in, plus the identifiers derived from the request's raw name.
type RequestOptions struct {
	Workflow string
	Name     string

	// Package is the workflow's directory and package identifier, Ident the
	// exported Go name the request's func and types are built from, and File
	// the snake_case base name of the file itself.
	Package string
	Ident   string
	File    string

	// Entry, when set, renders the request from a captured exchange instead of
	// the stub: the URL, the header order, and the body bytes come from the wire.
	Entry *capture.Entry
}

// capturedView is what the captured template renders from. Everything the
// template cannot decide — the method constant, the body literal's quoting — is
// reduced to a Go source fragment here, so the template only interpolates.
type capturedView struct {
	Ident   string
	Source  string
	Method  string
	URL     string
	Headers []capture.Header
	Pseudo  []string
	Body    string

	// BodyType is the Go type the request's Body field declares, typed from a
	// captured JSON body and marshaled back to it. Anything else leaves the
	// request struct empty and sends Body, the captured bytes, as a literal.
	BodyType  string
	BodyTyped bool

	// ResponseType is the Go type the response unmarshals into, and Inferred
	// says whether it came from the captured body or is the empty placeholder.
	// Media names the body's type when it does not, so the reader knows why.
	ResponseType     string
	ResponseInferred bool
	ResponseMedia    string

	// Imports are the standard library packages the rendered file needs,
	// sorted. They depend on what was inferred: a timestamp pulls in time, a
	// typed body pulls in bytes and encoding/json, a literal one pulls in strings.
	Imports []string
}

// methods maps the HTTP methods net/http names to their constants, so generated
// code reads like hand-written code. Anything else renders as a plain literal.
var methods = map[string]string{
	http.MethodGet:     "http.MethodGet",
	http.MethodHead:    "http.MethodHead",
	http.MethodPost:    "http.MethodPost",
	http.MethodPut:     "http.MethodPut",
	http.MethodPatch:   "http.MethodPatch",
	http.MethodDelete:  "http.MethodDelete",
	http.MethodConnect: "http.MethodConnect",
	http.MethodOptions: "http.MethodOptions",
	http.MethodTrace:   "http.MethodTrace",
}

// newCapturedView reduces a captured entry to the template's view of it.
func newCapturedView(ident string, e capture.Entry) capturedView {
	method, ok := methods[strings.ToUpper(e.Method)]
	if !ok {
		method = strconv.Quote(e.Method)
	}
	source := "a captured request"
	if e.ID != "" {
		source = "powhttp entry " + e.ID
	}
	response, responseTyped := responseType(e.Response)
	var media string
	if e.Response != nil {
		media = e.Response.MediaType
	}

	view := capturedView{
		Ident:            ident,
		Source:           source,
		Method:           method,
		URL:              e.URL,
		Headers:          e.Headers,
		Pseudo:           e.Pseudo,
		ResponseType:     response.goType,
		ResponseInferred: responseTyped,
		ResponseMedia:    media,
	}

	imports := map[string]bool{"context": true}
	for _, path := range response.imports {
		imports[path] = true
	}

	// A typed body is marshaled from the request struct; an untyped one is sent
	// as the captured bytes, which is exact for a body no typer understands.
	if body, ok := requestBodyType(e); ok {
		view.BodyType, view.BodyTyped = body.goType, true
		imports["bytes"], imports["encoding/json"] = true, true
		for _, path := range body.imports {
			imports[path] = true
		}
	} else if len(e.Body) > 0 {
		view.Body = goStringLiteral(e.Body)
		imports["strings"] = true
	}

	view.Imports = slices.Sorted(maps.Keys(imports))
	return view
}

// goStringLiteral renders body as a Go string literal, preferring a raw literal
// for readability. Raw literals drop carriage returns and cannot hold a
// backtick, so anything containing either — or any other byte a raw literal
// would not survive — is quoted instead, which is byte-exact for any input.
func goStringLiteral(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	if utf8.Valid(body) && !bytes.ContainsAny(body, "`\r") {
		printable := true
		for _, r := range string(body) {
			if r != '\n' && r != '\t' && unicode.IsControl(r) {
				printable = false
				break
			}
		}
		if printable {
			return "`" + string(body) + "`"
		}
	}
	return strconv.Quote(string(body))
}

// NewRequestOptions derives the render options for a request from the raw
// workflow and request names as the user typed them.
func NewRequestOptions(workflow, name string) RequestOptions {
	return RequestOptions{
		Workflow: workflow,
		Name:     name,
		Package:  PackageName(workflow),
		Ident:    RequestIdent(name),
		File:     RequestFile(name),
	}
}

// Validate rejects names that yield no usable package or Go identifier, which
// would otherwise surface later as a confusing format error over the source.
func (o RequestOptions) Validate() error {
	if o.Workflow == "" {
		return fmt.Errorf("workflow name is required")
	}
	if o.Name == "" {
		return fmt.Errorf("request name is required")
	}
	if o.Package == "" {
		return fmt.Errorf("workflow name %q has no valid package identifier", o.Workflow)
	}
	if o.Ident == "" {
		return fmt.Errorf("request name %q yields no usable Go identifier — it must contain a letter, and cannot start with a digit", o.Name)
	}
	return nil
}

// RenderRequest returns the request's path relative to the destination root and
// its formatted source. A captured entry renders the exchange as it happened;
// without one the request is a stub. Invalid Go fails here, at format, before
// any write.
func RenderRequest(o RequestOptions) (string, string, error) {
	if err := o.Validate(); err != nil {
		return "", "", err
	}
	tmplPath := "templates/requests/request.go.tmpl"
	var data any = o
	if o.Entry != nil {
		tmplPath = "templates/requests/captured.go.tmpl"
		data = newCapturedView(o.Ident, *o.Entry)
	}

	raw, err := templatesFS.ReadFile(tmplPath)
	if err != nil {
		return "", "", fmt.Errorf("read template %s: %w", tmplPath, err)
	}
	// quote renders a captured value as a Go literal, so a header holding a
	// quote or a backslash cannot break out of the generated source.
	tmpl, err := template.New(path.Base(tmplPath)).
		Funcs(template.FuncMap{"quote": strconv.Quote}).
		Parse(string(raw))
	if err != nil {
		return "", "", fmt.Errorf("parse template %s: %w", tmplPath, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", "", fmt.Errorf("execute template %s: %w", tmplPath, err)
	}
	outPath := path.Join(o.Package, "requests", o.File+".go")
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return "", "", fmt.Errorf("format %s: %w\n--- rendered ---\n%s", outPath, err, buf.String())
	}
	return outPath, string(formatted), nil
}

// WriteRequest renders one request into an existing workflow under destRoot, so
// it can be called once per request as a workflow grows. It refuses to overwrite
// unless force is set, leaving an edited request untouched on a repeated run,
// and reports whether it replaced one so the caller can say so.
func WriteRequest(destRoot string, o RequestOptions, force bool) (rel string, overwrote bool, err error) {
	rel, source, err := RenderRequest(o)
	if err != nil {
		return "", false, err
	}

	// A request belongs to a workflow: without one there is no package for it
	// to compile into, and a bare requests/ directory would never build. Force
	// overwrites a request, it does not conjure the workflow around it.
	workflowDir := filepath.Join(destRoot, o.Package)
	if _, err := os.Stat(workflowDir); os.IsNotExist(err) {
		return "", false, fmt.Errorf("no workflow package %q at %s — run `rogojin new %s` first", o.Package, workflowDir, o.Workflow)
	} else if err != nil {
		return "", false, fmt.Errorf("stat %s: %w", workflowDir, err)
	}

	full := filepath.Join(destRoot, rel)
	switch _, err := os.Stat(full); {
	case err == nil:
		if !force {
			return "", false, fmt.Errorf("refusing to overwrite existing file: %s — pass --force to replace it", full)
		}
		overwrote = true
	case !os.IsNotExist(err):
		return "", false, fmt.Errorf("stat %s: %w", full, err)
	}

	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return "", false, fmt.Errorf("mkdir for %s: %w", full, err)
	}
	if err := os.WriteFile(full, []byte(source), 0o644); err != nil {
		return "", false, fmt.Errorf("write %s: %w", full, err)
	}
	return rel, overwrote, nil
}

// RequestIdent derives the exported Go identifier for a request from its raw
// name, so "add-to-cart" and "addToCart" both yield "AddToCart". It returns the
// empty string when no identifier can be built: one must contain a letter and
// cannot start with a digit.
func RequestIdent(name string) string {
	var b strings.Builder
	for _, word := range splitWords(name) {
		r := []rune(word)
		b.WriteString(strings.ToUpper(string(r[0])))
		b.WriteString(string(r[1:]))
	}
	out := b.String()
	if out == "" || (out[0] >= '0' && out[0] <= '9') {
		return ""
	}
	return out
}

// RequestFile derives a request's snake_case file base name, so "add-to-cart"
// and "addToCart" both yield "add_to_cart".
func RequestFile(name string) string {
	words := splitWords(name)
	for i, word := range words {
		words[i] = strings.ToLower(word)
	}
	return strings.Join(words, "_")
}

// splitWords breaks a raw name into words on any run of non-alphanumeric
// characters and on lower-to-upper transitions, so kebab, snake, and camel
// spellings of one name all split the same way.
func splitWords(name string) []string {
	var words []string
	var cur []rune
	var prev rune
	for _, r := range name {
		alnum := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		switch {
		case !alnum:
			if len(cur) > 0 {
				words = append(words, string(cur))
				cur = nil
			}
		default:
			if len(cur) > 0 && prev >= 'a' && prev <= 'z' && r >= 'A' && r <= 'Z' {
				words = append(words, string(cur))
				cur = nil
			}
			cur = append(cur, r)
		}
		prev = r
	}
	if len(cur) > 0 {
		words = append(words, string(cur))
	}
	return words
}

// PackageName derives a valid Go package identifier from a workflow name: it
// lowercases and drops every character that is not a letter, digit, or
// underscore, then ensures the result does not start with a digit. It returns
// the empty string when nothing usable remains.
func PackageName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if b.Len() == 0 {
				continue // a package identifier cannot start with a digit
			}
			b.WriteRune(r)
		}
	}
	return b.String()
}
