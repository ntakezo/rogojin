package scaffold

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stateSource is a minimal state file: the constant and the handler the scan
// has to pair.
func stateSource(constName, handler string) string {
	return `package states

import (
	"context"

	"github.com/ntakezo/rogojin/workflows"
)

const ` + constName + ` workflows.State = "` + constName + `"

func (c *Context) ` + handler + `(ctx context.Context) (*workflows.State, error) {
	return nil, nil
}
`
}

// TestScanStatesPairsConstantsWithHandlers is the convention the derived graph
// rests on: a state named by getHomepage is run by (*Context).GetHomepage, and
// the pair may be found however the files are arranged.
func TestScanStatesPairsConstantsWithHandlers(t *testing.T) {
	got, err := scanStates(map[string]string{
		"submit_checkout.go": stateSource("submitCheckout", "SubmitCheckout"),
		"get_homepage.go":    stateSource("getHomepage", "GetHomepage"),
		"add_to_cart.go":     stateSource("addToCart", "AddToCart"),
	})
	if err != nil {
		t.Fatalf("scanStates: %v", err)
	}

	// Sorted, so a graph rewritten after an unrelated edit does not churn.
	var order []string
	for _, s := range got {
		order = append(order, s.Const)
	}
	if want := "addToCart,getHomepage,submitCheckout"; strings.Join(order, ",") != want {
		t.Errorf("states = %v, want %s", order, want)
	}
	for _, s := range got {
		if s.Handler != strings.ToUpper(s.Const[:1])+s.Const[1:] {
			t.Errorf("state %s paired with handler %s", s.Const, s.Handler)
		}
	}
}

// TestScanStatesRejectsHalfAPair is why the scan is safe to derive from: a
// state left out of the graph fails at run time, far from the file that caused
// it, so neither half may go missing quietly.
func TestScanStatesRejectsHalfAPair(t *testing.T) {
	cases := map[string]map[string]string{
		"constant with no handler": {
			"orphan.go": `package states

import "github.com/ntakezo/rogojin/workflows"

const orphan workflows.State = "orphan"
`},
		"handler with no constant": {
			"lonely.go": `package states

import (
	"context"

	"github.com/ntakezo/rogojin/workflows"
)

func (c *Context) Lonely(ctx context.Context) (*workflows.State, error) { return nil, nil }
`},
		"same state twice": {
			"a.go": stateSource("dup", "Dup"),
			"b.go": `package states

import "github.com/ntakezo/rogojin/workflows"

const dup workflows.State = "other"
`},
		"unparseable": {"broken.go": "package states\nfunc ("},
	}
	for name, sources := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := scanStates(sources); err == nil {
				t.Error("scanStates accepted a states package it should have refused")
			}
		})
	}
}

// TestScanStatesIgnoresNonHandlers keeps the other methods on Context — and any
// constant that is not a state — from being mistaken for one.
func TestScanStatesIgnoresNonHandlers(t *testing.T) {
	got, err := scanStates(map[string]string{
		"process.go": stateSource("process", "Process"),
		"context.go": `package states

import (
	"context"

	http "github.com/bogdanfinn/fhttp"
	"github.com/ntakezo/rogojin/workflows"
)

const queueTopic = "queue-cookie"

func (c *Context) client(ctx context.Context) (*http.Client, error) { return nil, nil }

func (c *Context) Output() ([]byte, error) { return nil, nil }

func (c *Context) Teardown(ctx context.Context, status workflows.Status, runErr error) error {
	return nil
}
`,
	})
	if err != nil {
		t.Fatalf("scanStates: %v", err)
	}
	if len(got) != 1 || got[0].Const != "process" {
		t.Errorf("scanned %v, want only the process state", got)
	}
}

// writeStates lays a states package on disk and returns the destination root.
func writeStates(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "shop", "checkout", "states")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, source := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// TestWriteGraphPicksUpAHandWrittenState is the property the derived graph
// exists for: a state that no command generated is still wired, so the graph
// tracks the package rather than a record of which commands were run.
func TestWriteGraphPicksUpAHandWrittenState(t *testing.T) {
	root := writeStates(t, map[string]string{
		"process.go":       stateSource("process", "Process"),
		"wait_in_queue.go": stateSource("waitInQueue", "WaitInQueue"),
	})
	if _, err := WriteGraph(root, "shop", "checkout", "process"); err != nil {
		t.Fatalf("WriteGraph: %v", err)
	}

	graph := filepath.Join(root, "shop", "checkout", "states", "graph.go")
	source, err := os.ReadFile(graph)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"workflows.NewGraph(process, workflows.States{",
		"process:     c.Process,",
		"waitInQueue: c.WaitInQueue,",
	} {
		if !strings.Contains(string(source), want) {
			t.Errorf("derived graph is missing:\n  %s\n--- got ---\n%s", want, source)
		}
	}

	// A state removed from the package leaves the graph on the next derive: the
	// file is a view of what is there, not a log of what was added.
	if err := os.Remove(filepath.Join(root, "shop", "checkout", "states", "process.go")); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteGraph(root, "shop", "checkout", "waitInQueue"); err != nil {
		t.Fatalf("WriteGraph after removal: %v", err)
	}
	if source, err = os.ReadFile(graph); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(source), "c.Process") {
		t.Errorf("a deleted state survived the rewrite:\n%s", source)
	}
}

// TestWriteGraphKeepsTheEntryState pins that adding a state never moves where a
// run begins: the entry is read back out of the graph being replaced.
func TestWriteGraphKeepsTheEntryState(t *testing.T) {
	root := writeStates(t, map[string]string{"get_homepage.go": stateSource("getHomepage", "GetHomepage")})
	if _, err := WriteGraph(root, "shop", "checkout", ""); err != nil {
		t.Fatalf("WriteGraph: %v", err)
	}

	// A second state arrives; the first must still be the entry, unasked.
	dir := filepath.Join(root, "shop", "checkout", "states")
	if err := os.WriteFile(filepath.Join(dir, "add_to_cart.go"), []byte(stateSource("addToCart", "AddToCart")), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := WriteGraph(root, "shop", "checkout", ""); err != nil {
		t.Fatalf("WriteGraph: %v", err)
	}
	source, err := os.ReadFile(filepath.Join(dir, "graph.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "NewGraph(getHomepage,") {
		t.Errorf("the entry state moved when a state was added:\n%s", source)
	}

	// Asked explicitly, it moves — and only to a state that exists.
	if _, err := WriteGraph(root, "shop", "checkout", "addToCart"); err != nil {
		t.Fatalf("WriteGraph: %v", err)
	}
	if source, err = os.ReadFile(filepath.Join(dir, "graph.go")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "NewGraph(addToCart,") {
		t.Errorf("--initial did not move the entry state:\n%s", source)
	}
	if _, err := WriteGraph(root, "shop", "checkout", "noSuchState"); err == nil {
		t.Error("WriteGraph accepted an entry state that is not declared")
	}
}

// TestWriteGraphNeedsAnEntryItCanName covers the one case the rewrite cannot
// answer for itself: several states and no graph to read the entry back from.
func TestWriteGraphNeedsAnEntryItCanName(t *testing.T) {
	root := writeStates(t, map[string]string{
		"get_homepage.go": stateSource("getHomepage", "GetHomepage"),
		"add_to_cart.go":  stateSource("addToCart", "AddToCart"),
	})
	_, err := WriteGraph(root, "shop", "checkout", "")
	if err == nil {
		t.Fatal("WriteGraph guessed an entry state, want an error naming --initial")
	}
	if !strings.Contains(err.Error(), "--initial") {
		t.Errorf("error does not say how to fix it: %v", err)
	}

	// One state needs no answer: it is the only place a run could begin.
	only := writeStates(t, map[string]string{"process.go": stateSource("process", "Process")})
	if _, err := WriteGraph(only, "shop", "checkout", ""); err != nil {
		t.Errorf("WriteGraph asked which of one state to start at: %v", err)
	}
}

// TestRenderStateShapes pins what a generated state carries: the request call
// and the next-state return, and nothing else to unpick.
func TestRenderStateShapes(t *testing.T) {
	opts := NewStateOptions("shop", "checkout", "get-homepage")
	opts.ModulePath = "example.com/consumer"
	opts.Request = "GetHomepage"
	opts.Next = "addToCart"
	opts.Decode = true

	rel, source, err := RenderState(opts)
	if err != nil {
		t.Fatalf("RenderState: %v", err)
	}
	if want := filepath.Join("shop", "checkout", "states", "get_homepage.go"); rel != want {
		t.Errorf("rendered to %s, want %s", rel, want)
	}
	for _, want := range []string{
		`const getHomepage workflows.State = "get-homepage"`,
		"func (c *Context) GetHomepage(ctx context.Context) (*workflows.State, error) {",
		"requests.GetHomepage(ctx, client, requests.GetHomepageRequest{})",
		"var body requests.GetHomepageResponse",
		"return workflows.Next(addToCart), nil",
		`"example.com/consumer/shop/requests"`,
	} {
		if !strings.Contains(source, want) {
			t.Errorf("rendered state is missing:\n  %s\n--- got ---\n%s", want, source)
		}
	}

	// No request and no next: a terminal state that calls nothing must not drag
	// in the requests import, which would not compile.
	bare := NewStateOptions("shop", "checkout", "wait-in-queue")
	bare.ModulePath = "example.com/consumer"
	_, source, err = RenderState(bare)
	if err != nil {
		t.Fatalf("RenderState: %v", err)
	}
	if strings.Contains(source, "requests") || strings.Contains(source, "encoding/json") {
		t.Errorf("a state that calls nothing imported the requests layer:\n%s", source)
	}
	if !strings.Contains(source, "return nil, nil") {
		t.Errorf("a state with no next is not terminal:\n%s", source)
	}
}

// requestSource is a generated request reduced to what the state scan reads:
// the function and the type its reply unmarshals into.
func requestSource(ident, response string) string {
	return `package requests

import (
	"context"

	http "github.com/bogdanfinn/fhttp"
)

type ` + ident + `Request struct{ Body any }

type ` + ident + `Response ` + response + `

func ` + ident + `(ctx context.Context, client *http.Client, r ` + ident + `Request) (*http.Response, error) {
	return nil, nil
}
`
}

// writeRequests lays a workflow with a requests package on disk.
func writeRequests(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for _, dir := range []string{filepath.Join("checkout", "states"), "requests"} {
		if err := os.MkdirAll(filepath.Join(root, "shop", dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, source := range files {
		if err := os.WriteFile(filepath.Join(root, "shop", "requests", name), []byte(source), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// TestStateDecodesOnlyWhatWasTyped is the reason the request is read rather than
// assumed: a response typed from nothing is an empty struct, and decoding an
// HTML page into one fails on the first byte, every run.
func TestStateDecodesOnlyWhatWasTyped(t *testing.T) {
	root := writeRequests(t, map[string]string{
		"get_homepage.go": requestSource("GetHomepage", "struct{}"),
		"get_product.go":  requestSource("GetProduct", "struct {\n\tVariantID string `json:\"variantID\"`\n}"),
		"list_sizes.go":   requestSource("ListSizes", "[]string"),
	})

	cases := map[string]bool{"GetHomepage": false, "GetProduct": true, "ListSizes": true}
	for ident, want := range cases {
		opts := NewStateOptions("shop", "checkout", ident)
		opts.ModulePath, opts.Request = "example.com/consumer", ident
		if _, _, err := WriteState(root, opts, false, ""); err != nil {
			t.Fatalf("WriteState(%q): %v", ident, err)
		}
		source, err := os.ReadFile(filepath.Join(root, "shop", "checkout", "states", RequestFile(ident)+".go"))
		if err != nil {
			t.Fatal(err)
		}
		decodes := strings.Contains(string(source), "json.NewDecoder(res.Body).Decode(&body)")
		if decodes != want {
			t.Errorf("%s decodes = %v, want %v:\n%s", ident, decodes, want, source)
		}
		// An unused import does not compile, so the two must move together.
		if imports := strings.Contains(string(source), `"encoding/json"`); imports != want {
			t.Errorf("%s imports encoding/json = %v, want %v", ident, imports, want)
		}
	}
}

// TestStateNeedsTheRequestToExist keeps a state from naming a function that was
// never generated, which would leave a tree that does not build.
func TestStateNeedsTheRequestToExist(t *testing.T) {
	root := writeRequests(t, map[string]string{"get_product.go": requestSource("GetProduct", "struct{}")})

	opts := NewStateOptions("shop", "checkout", "get-homepage")
	opts.ModulePath, opts.Request = "example.com/consumer", "GetHomepage"
	_, _, err := WriteState(root, opts, false, "")
	if err == nil {
		t.Fatal("WriteState accepted a request that does not exist")
	}
	if !strings.Contains(err.Error(), "rogojin request") {
		t.Errorf("error does not say how to fix it: %v", err)
	}
}

// TestStateConst pins the constant derivation, which the pairing depends on:
// kebab, snake, and camel spellings of one name land on one identifier.
func TestStateConst(t *testing.T) {
	cases := map[string]string{
		"get-homepage": "getHomepage",
		"get_homepage": "getHomepage",
		"getHomepage":  "getHomepage",
		"process":      "process",
		"123":          "",
		"":             "",
	}
	for in, want := range cases {
		if got := StateConst(in); got != want {
			t.Errorf("StateConst(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestWriteStateRequiresAWorkflow keeps a state from being orphaned in a
// directory with no Context to hang its handler off.
func TestWriteStateRequiresAWorkflow(t *testing.T) {
	opts := NewStateOptions("shop", "checkout", "get-homepage")
	opts.ModulePath = "example.com/consumer"
	if _, _, err := WriteState(t.TempDir(), opts, false, ""); err == nil {
		t.Error("WriteState wrote into a module with no checkout workflow, want refusal")
	}
}
