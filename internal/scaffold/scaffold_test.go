package scaffold

import (
	"bufio"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ntakezo/rogojin/internal/capture"

	// The generated requests package imports these, so TestGeneratedCodeCompiles
	// needs them in this module's go.mod and go.sum to resolve. No rogojin
	// package imports either, so without these anchors `go mod tidy` drops them
	// and the temp modules the test builds stop compiling.
	_ "github.com/bogdanfinn/fhttp"
	_ "github.com/justhyped/OrderedForm"
)

// generatedDeps are the third-party modules the templates emit imports for.
// The temp module in TestGeneratedCodeCompiles requires them at whatever
// version this repo pins, so the two can never drift apart.
var generatedDeps = []string{"github.com/bogdanfinn/fhttp", "github.com/justhyped/OrderedForm"}

// TestPackageName pins the identifier derivation: lowercased, non-ident
// characters dropped, never leading with a digit.
func TestPackageName(t *testing.T) {
	cases := map[string]string{
		"checkout":      "checkout",
		"Checkout":      "checkout",
		"checkout-flow": "checkoutflow",
		"my_workflow":   "my_workflow",
		"2fast":         "fast",
		"123":           "",
		"":              "",
	}
	for in, want := range cases {
		if got := PackageName(in); got != want {
			t.Errorf("PackageName(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestRequestNaming pins the two derivations a request name feeds: the exported
// identifier its func and types are built from, and its file's base name. Kebab,
// snake, and camel spellings of one name must land on the same pair.
func TestRequestNaming(t *testing.T) {
	cases := []struct{ in, ident, file string }{
		{"add-to-cart", "AddToCart", "add_to_cart"},
		{"add_to_cart", "AddToCart", "add_to_cart"},
		{"addToCart", "AddToCart", "add_to_cart"},
		{"AddToCart", "AddToCart", "add_to_cart"},
		{"add to cart", "AddToCart", "add_to_cart"},
		{"submit", "Submit", "submit"},
		{"get CSRF", "GetCSRF", "get_csrf"},
		{"checkout2", "Checkout2", "checkout2"},
		{"2fast", "", "2fast"},
		{"---", "", ""},
		{"", "", ""},
	}
	for _, c := range cases {
		if got := RequestIdent(c.in); got != c.ident {
			t.Errorf("RequestIdent(%q) = %q, want %q", c.in, got, c.ident)
		}
		if got := RequestFile(c.in); got != c.file {
			t.Errorf("RequestFile(%q) = %q, want %q", c.in, got, c.file)
		}
	}
}

// TestRequestOptionsValidate guards the names that cannot produce compiling
// code, so they fail with a usable message rather than at format over the source.
func TestRequestOptionsValidate(t *testing.T) {
	valid := NewRequestOptions("shop", "add-to-cart")
	if err := valid.Validate(); err != nil {
		t.Errorf("Validate rejected a usable request: %v", err)
	}
	for _, c := range []struct{ domain, name string }{
		{"", "add-to-cart"},
		{"shop", ""},
		{"123", "add-to-cart"},
		{"shop", "2fast"},
		{"shop", "---"},
	} {
		if err := NewRequestOptions(c.domain, c.name).Validate(); err == nil {
			t.Errorf("Validate accepted domain %q request %q, want rejection", c.domain, c.name)
		}
	}
}

// TestWriteRequestIsAdditive is the point of the verb: repeated calls each add a
// file, an existing request is never clobbered, and a request without a workflow
// to live in is refused rather than left orphaned.
func TestWriteRequestIsAdditive(t *testing.T) {
	dir := t.TempDir()

	if _, _, err := WriteRequest(dir, NewRequestOptions("shop", "add-to-cart"), false); err == nil {
		t.Error("WriteRequest wrote into a module with no shop domain, want refusal")
	}
	if _, _, err := WriteRequest(dir, NewRequestOptions("shop", "add-to-cart"), true); err == nil {
		t.Error("force wrote into a module with no shop domain: force replaces a request, it does not create the domain")
	}

	base := NewOptions("shop", "checkout")
	base.Output, base.TaskPersist = true, true
	if _, _, err := WriteWorkflow(dir, "example.com/consumer", base); err != nil {
		t.Fatalf("WriteWorkflow: %v", err)
	}

	for _, name := range []string{"add-to-cart", "submit-checkout"} {
		rel, overwrote, err := WriteRequest(dir, NewRequestOptions("shop", name), false)
		if err != nil {
			t.Fatalf("WriteRequest(%q): %v", name, err)
		}
		if overwrote {
			t.Errorf("WriteRequest(%q) reported an overwrite on a fresh file", name)
		}
		if want := filepath.Join("shop", "requests", RequestFile(name)+".go"); rel != want {
			t.Errorf("WriteRequest(%q) wrote %s, want %s", name, rel, want)
		}
	}

	// Only the two added above: a scaffolded workflow starts with no requests,
	// and the directory is created by the first one written into it.
	entries, err := os.ReadDir(filepath.Join(dir, "shop", "requests"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Errorf("requests/ holds %d files, want 2", len(entries))
	}

	// A hand edit is what the refusal exists to protect, and what --force is for
	// discarding. The camel spelling resolves to the same file, so it collides.
	edited := filepath.Join(dir, "shop", "requests", "add_to_cart.go")
	generated, err := os.ReadFile(edited)
	if err != nil {
		t.Fatal(err)
	}
	byHand := append(generated, "\n// hand-written since generation\n"...)
	if err := os.WriteFile(edited, byHand, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, _, err := WriteRequest(dir, NewRequestOptions("shop", "addToCart"), false); err == nil {
		t.Error("WriteRequest overwrote an existing request, want refusal")
	}
	kept, err := os.ReadFile(edited)
	if err != nil {
		t.Fatal(err)
	}
	if string(kept) != string(byHand) {
		t.Error("a refused WriteRequest still modified the existing request")
	}

	rel, overwrote, err := WriteRequest(dir, NewRequestOptions("shop", "addToCart"), true)
	if err != nil {
		t.Fatalf("forced WriteRequest: %v", err)
	}
	if !overwrote {
		t.Error("forced WriteRequest over an existing request did not report an overwrite")
	}
	if want := filepath.Join("shop", "requests", "add_to_cart.go"); rel != want {
		t.Errorf("forced WriteRequest wrote %s, want %s", rel, want)
	}
	replaced, err := os.ReadFile(edited)
	if err != nil {
		t.Fatal(err)
	}
	if string(replaced) != string(generated) {
		t.Error("force did not restore the request to freshly generated source")
	}

	// Force replaces one request; it must not disturb its neighbours.
	entries, err = os.ReadDir(filepath.Join(dir, "shop", "requests"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Errorf("requests/ holds %d files after a forced write, want 2", len(entries))
	}
}

// capturedEntry is a small captured request covering what rendering has to get
// right: pseudo-headers, repeated fields, the headers a client writes for
// itself, and values full of characters that would break out of a Go literal if
// they were interpolated raw.
func capturedEntry() capture.Entry {
	return capture.Entry{
		ID:     "01TEST",
		Source: "powhttp entry 01TEST",
		URL:    "https://example.com/cart?size=M&color=red",
		Method: http.MethodPost,
		Pseudo: []string{":method", ":authority", ":scheme", ":path"},
		Headers: []capture.Header{
			{Name: "content-length", Value: "23"},
			{Name: "sec-ch-ua", Value: `"Not;A=Brand";v="8", "Chromium";v="150"`},
			{Name: "content-type", Value: "application/x-www-form-urlencoded"},
			{Name: "x-weird", Value: "back\\slash\ttab\"quote"},
			{Name: "cookie", Value: "a=1"},
			{Name: "cookie", Value: "b=2"},
		},
		Body: []byte("variantID=99&quantity=1"),
		Response: &capture.Response{
			StatusCode: 200,
			MediaType:  "application/json",
			Body:       []byte(`{"cartID":"c1","itemCount":2}`),
		},
	}
}

// TestRenderCapturedRequest is the fidelity contract: everything the capture saw
// reaches the source in the order it saw it, and every value survives quoting.
func TestRenderCapturedRequest(t *testing.T) {
	entry := capturedEntry()
	opts := NewRequestOptions("shop", "add-to-cart")
	opts.Entry = &entry

	rel, source, err := RenderRequest(opts)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if want := filepath.Join("shop", "requests", "add_to_cart.go"); rel != want {
		t.Errorf("rendered to %s, want %s", rel, want)
	}

	for _, want := range []string{
		`http.MethodPost`,
		`"https://example.com/cart?size=M&color=red"`,
		// The form body is typed, so its values are fields rather than a literal.
		`form.Set("variantID", r.Body.VariantID)`,
		`form.Set("quantity", r.Body.Quantity)`,
		`headers.Add("sec-ch-ua", "\"Not;A=Brand\";v=\"8\", \"Chromium\";v=\"150\"")`,
		`headers.Add("x-weird", "back\\slash\ttab\"quote")`,
		`http.HeaderOrderKey:  {"content-length", "sec-ch-ua", "content-type", "x-weird", "cookie"}`,
		`http.PHeaderOrderKey: {":method", ":authority", ":scheme", ":path"}`,
		`req.Header = headers`,
		"Generated from powhttp entry 01TEST",
		// The response is typed from the captured body, in the same file.
		"type AddToCartResponse struct {",
		"CartID    string `json:\"cartID\"`",
		"ItemCount int64  `json:\"itemCount\"`",
	} {
		if !strings.Contains(source, want) {
			t.Errorf("rendered source is missing:\n  %s\n--- got ---\n%s", want, source)
		}
	}

	// The client writes these for itself. A literal would contradict the
	// transport's Content-Length and be appended to by the cookie jar, so each
	// holds its place in the order and nothing more.
	for _, unwanted := range []string{`headers.Add("content-length"`, `headers.Add("cookie"`} {
		if strings.Contains(source, unwanted) {
			t.Errorf("rendered a literal for a header the client owns:\n  %s\n--- got ---\n%s", unwanted, source)
		}
	}

	// The order key is what fhttp sends by, so the written values have to appear
	// in the order the capture carried them.
	var added []string
	for _, line := range strings.Split(source, "\n") {
		if _, name, ok := strings.Cut(strings.TrimSpace(line), `headers.Add("`); ok {
			added = append(added, name[:strings.Index(name, `"`)])
		}
	}
	want := []string{"sec-ch-ua", "content-type", "x-weird"}
	if strings.Join(added, ",") != strings.Join(want, ",") {
		t.Errorf("added %v, want %v", added, want)
	}
}

// TestRenderCapturedRequestKeepsUntypedBodyLiteral covers the path a payload no
// typer understands takes: the captured bytes go out verbatim, which is exact
// where a struct would be a guess.
func TestRenderCapturedRequestKeepsUntypedBodyLiteral(t *testing.T) {
	entry := capturedEntry()
	entry.Headers = []capture.Header{{Name: "content-type", Value: "text/plain"}}
	entry.Body = []byte("sensor=a;b;c&checksum=91f2")

	opts := NewRequestOptions("shop", "post-sensor")
	opts.Entry = &entry
	_, source, err := RenderRequest(opts)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	for _, want := range []string{
		// Body is reserved even here, where nothing filled it: the payload is
		// the captured literal below, so the field stays nil.
		"Body any",
		"body := `sensor=a;b;c&checksum=91f2`",
		"strings.NewReader(body)",
	} {
		if !strings.Contains(source, want) {
			t.Errorf("rendered source is missing:\n  %s\n--- got ---\n%s", want, source)
		}
	}
}

// TestRenderCapturedRequestWithoutBody keeps a bodyless request from dragging in
// an unused strings import, which would not compile.
func TestRenderCapturedRequestWithoutBody(t *testing.T) {
	entry := capturedEntry()
	entry.Body = nil
	entry.Method = http.MethodGet
	opts := NewRequestOptions("shop", "get-cart")
	opts.Entry = &entry

	_, source, err := RenderRequest(opts)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}
	if strings.Contains(source, `"strings"`) {
		t.Error("a request with no body imported strings")
	}
	if !strings.Contains(source, "http.MethodGet, \"https://example.com/cart?size=M&color=red\", nil)") {
		t.Errorf("expected a nil body argument, got:\n%s", source)
	}
}

// TestGoStringLiteral pins when a body renders raw for readability and when it
// must be quoted: raw literals silently drop carriage returns and cannot hold a
// backtick, so a multipart or binary body has to take the quoted path.
func TestGoStringLiteral(t *testing.T) {
	cases := []struct{ name, in, want string }{
		{"empty", "", ""},
		{"form", "a=1&b=2", "`a=1&b=2`"},
		{"json multiline", "{\n  \"a\": 1\n}", "`{\n  \"a\": 1\n}`"},
		{"backtick", "a=`b`", `"a=` + "`" + `b` + "`" + `"`},
		{"carriage return", "a\r\nb", `"a\r\nb"`},
		{"control byte", "a\x00b", `"a\x00b"`},
		{"invalid utf8", "a\xffb", `"a\xffb"`},
	}
	for _, c := range cases {
		if got := goStringLiteral([]byte(c.in)); got != c.want {
			t.Errorf("%s: goStringLiteral(%q) = %s, want %s", c.name, c.in, got, c.want)
		}
	}
}

// TestCapturedMethodRendersAsConstant keeps generated code reading like
// hand-written code, while still carrying a method net/http does not name.
func TestCapturedMethodRendersAsConstant(t *testing.T) {
	for method, want := range map[string]string{
		"GET":    "http.MethodGet",
		"post":   "http.MethodPost",
		"PATCH":  "http.MethodPatch",
		"REPORT": `"REPORT"`,
	} {
		entry := capturedEntry()
		entry.Method = method
		if got := newCapturedView("X", entry).Method; got != want {
			t.Errorf("method %q rendered as %s, want %s", method, got, want)
		}
	}
}

// TestValidateRejectsIncoherentCombos guards the two combinations that would
// otherwise generate code that lies about what it does. These are the whole
// reason Validate exists, so they must fail loudly rather than render.
func TestValidateRejectsIncoherentCombos(t *testing.T) {
	durableWithoutTaskPersist := Options{Name: "x", Package: "x", Durable: true, TaskPersist: false}
	if err := durableWithoutTaskPersist.Validate(); err == nil {
		t.Error("durable + no task persistence should be rejected: snapshots would never be written")
	}

	proxyPersistWithoutProxy := Options{Name: "x", Package: "x", Proxy: false, ProxyPersist: true}
	if err := proxyPersistWithoutProxy.Validate(); err == nil {
		t.Error("proxy persistence without a proxy pool should be rejected")
	}
}

// TestValidateRejectsUnusablePackageNames guards names whose derived package
// identifier cannot actually be used: Go keywords fail compilation and "main"
// cannot be imported. Without the guard these die later as a cryptic format
// error over the full rendered source.
func TestValidateRejectsUnusablePackageNames(t *testing.T) {
	for _, name := range []string{"type", "select", "func", "main", "Go!"} {
		o := Options{Name: name, Package: PackageName(name), TaskPersist: true}
		if err := o.Validate(); err == nil {
			t.Errorf("Validate accepted name %q (package %q), want rejection", name, o.Package)
		}
	}
}

// validCombos enumerates every flag combination that survives normalization and
// Validate, so the compile test covers the whole feature matrix.
func validCombos() []Options {
	var out []Options
	for _, durable := range []bool{true, false} {
		for _, output := range []bool{true, false} {
			for _, proxy := range []bool{true, false} {
				for _, taskPersist := range []bool{true, false} {
					for _, proxyPersist := range []bool{true, false} {
						o := NewOptions("shop", "sample")
						o.Durable, o.Output = durable, output
						o.Proxy, o.TaskPersist, o.ProxyPersist = proxy, taskPersist, proxyPersist
						if !o.Proxy {
							o.ProxyPersist = false // mirror CLI normalization
						}
						if o.Validate() != nil {
							continue
						}
						out = append(out, o)
					}
				}
			}
		}
	}
	return dedupe(out)
}

func dedupe(in []Options) []Options {
	seen := make(map[Options]bool)
	var out []Options
	for _, o := range in {
		if seen[o] {
			continue
		}
		seen[o] = true
		out = append(out, o)
	}
	return out
}

// TestRenderProducesValidGo renders every valid combo and asserts it formats —
// format.Source inside Render rejects syntactically invalid output, so a passing
// render is a real (fast) syntax guarantee across the matrix.
func TestRenderProducesValidGo(t *testing.T) {
	for _, o := range validCombos() {
		t.Run(comboName(o), func(t *testing.T) {
			files, err := Render("example.com/consumer", o, true)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			if len(files) == 0 {
				t.Fatal("Render produced no files")
			}
		})
	}
}

// TestGeneratedCodeCompiles is the contract: every valid combo must produce a
// tree that actually type-checks against the real rogojin packages. It writes
// each scaffold into a throwaway module that replaces rogojin with this checkout,
// then runs `go vet ./...` — which compiles every package (failing on any compile
// error) without linking binaries, so it stays light on disk. Skipped under
// -short because it shells out to the toolchain (and needs cgo for the SQLite
// adapters).
func TestGeneratedCodeCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compile test in short mode")
	}
	for _, o := range validCombos() {
		t.Run(comboName(o), func(t *testing.T) {
			dir := t.TempDir()

			if _, _, err := WriteWorkflow(dir, "example.com/consumer", o); err != nil {
				t.Fatalf("WriteWorkflow: %v", err)
			}
			// A workflow has no graph until a state derives one, so the tree is
			// only a workflows.Instance once the first state lands — which puts
			// the state template under the same compile check as the rest.
			state := NewStateOptions(o.Domain, o.Name, "fetch")
			state.ModulePath = "example.com/consumer"
			if _, _, err := WriteState(dir, state, false, ""); err != nil {
				t.Fatalf("WriteState: %v", err)
			}
			writeConsumerModule(t, dir)
			vetModule(t, dir)
		})
	}
}

// TestAddedWorkflowsCompile extends the contract to a domain that grows: the
// registration is re-derived around each workflow, and the entrypoint written
// with the first one is not — so a second workflow must leave it building. That
// is what hands Register a Configs struct rather than a parameter per workflow.
func TestAddedWorkflowsCompile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compile test in short mode")
	}
	dir := t.TempDir()

	for _, name := range []string{"checkout", "signup"} {
		o := NewOptions("shop", name)
		o.Durable, o.Output = true, true
		o.Proxy, o.TaskPersist, o.ProxyPersist = true, true, true
		if _, _, err := WriteWorkflow(dir, "example.com/consumer", o); err != nil {
			t.Fatalf("WriteWorkflow(%q): %v", name, err)
		}
		// A workflow is only a workflows.Instance once a state derives its graph.
		state := NewStateOptions("shop", name, "fetch")
		state.ModulePath = "example.com/consumer"
		if _, _, err := WriteState(dir, state, false, ""); err != nil {
			t.Fatalf("WriteState(%q): %v", name, err)
		}
	}

	writeConsumerModule(t, dir)
	vetModule(t, dir)
}

// TestAddedRequestsCompile extends the contract to the request verb: requests
// added one at a time must type-check alongside the workflow they were added to,
// however many of them there are.
func TestAddedRequestsCompile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compile test in short mode")
	}
	dir := t.TempDir()

	base := NewOptions("shop", "checkout")
	base.Output, base.TaskPersist = true, true
	if _, _, err := WriteWorkflow(dir, "example.com/consumer", base); err != nil {
		t.Fatalf("WriteWorkflow: %v", err)
	}
	// A workflow is only a workflows.Instance once a state derives its graph.
	state := NewStateOptions("shop", "checkout", "fetch")
	state.ModulePath = "example.com/consumer"
	if _, _, err := WriteState(dir, state, false, ""); err != nil {
		t.Fatalf("WriteState: %v", err)
	}
	for _, name := range []string{"add-to-cart", "submit_checkout", "getCSRF"} {
		if _, _, err := WriteRequest(dir, NewRequestOptions("shop", name), false); err != nil {
			t.Fatalf("WriteRequest(%q): %v", name, err)
		}
	}

	// A captured request renders from a different template, so it earns its own
	// place in the compile contract — with and without a body, since the body is
	// what decides whether the strings import belongs.
	withBody := capturedEntry()
	captured := NewRequestOptions("shop", "captured-post")
	captured.Entry = &withBody
	if _, _, err := WriteRequest(dir, captured, false); err != nil {
		t.Fatalf("WriteRequest(captured): %v", err)
	}

	noBody := capturedEntry()
	noBody.Body = nil
	noBody.Method = http.MethodGet
	bodyless := NewRequestOptions("shop", "captured-get")
	bodyless.Entry = &noBody
	if _, _, err := WriteRequest(dir, bodyless, false); err != nil {
		t.Fatalf("WriteRequest(captured, no body): %v", err)
	}

	// A JSON body types the request struct and marshals back out of it, which
	// is a third shape again — and the one that imports bytes and encoding/json.
	jsonBody := capturedEntry()
	jsonBody.Headers = []capture.Header{{Name: "content-type", Value: "application/json"}}
	jsonBody.Body = []byte(`{"zebra":"z","nested":{"when":"2028-07-10T00:00:00-04:00"}}`)
	typedBody := NewRequestOptions("shop", "captured-json")
	typedBody.Entry = &jsonBody
	if _, _, err := WriteRequest(dir, typedBody, false); err != nil {
		t.Fatalf("WriteRequest(captured, json body): %v", err)
	}

	writeConsumerModule(t, dir)
	vetModule(t, dir)
}

// writeConsumerModule gives a generated tree the go.mod and go.sum it needs to
// build: this checkout replaced in, plus every module the templates import, at
// the version this repo pins.
func writeConsumerModule(t *testing.T, dir string) {
	t.Helper()
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	goSum, err := os.ReadFile(filepath.Join(repoRoot, "go.sum"))
	if err != nil {
		t.Fatalf("read repo go.sum: %v", err)
	}

	var requires strings.Builder
	for _, dep := range generatedDeps {
		version, err := requiredVersion(filepath.Join(repoRoot, "go.mod"), dep)
		if err != nil {
			t.Fatalf("%v — the templates import it, so this module must require it", err)
		}
		fmt.Fprintf(&requires, "\t%s %s\n", dep, version)
	}

	gomod := fmt.Sprintf(`module example.com/consumer

go 1.25.0

require (
	github.com/ntakezo/rogojin v0.0.0
%s)

replace github.com/ntakezo/rogojin => %s
`, requires.String(), repoRoot)
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "go.sum"), goSum, 0o644); err != nil {
		t.Fatal(err)
	}
}

// vetModule compiles every package in dir without linking binaries, so a compile
// error fails the test while the run stays light on disk.
func vetModule(t *testing.T, dir string) {
	t.Helper()
	cmd := exec.Command("go", "vet", "./...")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOPROXY=off")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go vet failed: %v\n%s", err, out)
	}
}

// TestGeneratedDepsMatchExamples pins the two modules to the same version of
// every dependency the templates emit imports for. They require those modules
// independently, so nothing but this stops them drifting — and once they drift,
// the compile test proves the templates against one version while _examples,
// the reference the templates are kept in step with, demonstrates another.
func TestGeneratedDepsMatchExamples(t *testing.T) {
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}

	for _, dep := range generatedDeps {
		root, err := requiredVersion(filepath.Join(repoRoot, "go.mod"), dep)
		if err != nil {
			t.Errorf("%v — the templates import it, so this module must require it", err)
			continue
		}
		examples, err := requiredVersion(filepath.Join(repoRoot, "_examples", "go.mod"), dep)
		if err != nil {
			t.Errorf("%v — the templates import it, so the examples must demonstrate it", err)
			continue
		}
		if root != examples {
			t.Errorf("%s: root pins %s, _examples pins %s — bump both together", dep, root, examples)
		}
	}
}

// requiredVersion returns the version gomod requires modulePath at, scanning the
// require lines directly so the test needs no module-file parser.
func requiredVersion(gomod, modulePath string) (string, error) {
	f, err := os.Open(gomod)
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		fields := strings.Fields(strings.TrimPrefix(strings.TrimSpace(scanner.Text()), "require "))
		if len(fields) >= 2 && fields[0] == modulePath {
			return fields[1], nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%s requires no %s", gomod, modulePath)
}

func comboName(o Options) string {
	return fmt.Sprintf("durable=%t_output=%t_proxy=%t_taskpersist=%t_proxypersist=%t",
		o.Durable, o.Output, o.Proxy, o.TaskPersist, o.ProxyPersist)
}
