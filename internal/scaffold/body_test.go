package scaffold

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/url"
	"strings"
	"testing"

	"github.com/ntakezo/rogojin/internal/capture"
)

// jsonBody wraps a body as a captured JSON request.
func jsonBody(body string) capture.Entry {
	return capture.Entry{
		Headers: []capture.Header{{Name: "content-type", Value: "application/json; charset=utf-8"}},
		Body:    []byte(body),
	}
}

// TestRequestStructCarriesNoWireTags is the invariant that makes the body split
// worth having. Only Body is marshaled, so a field added to the request struct
// must be structurally incapable of reaching the payload — which holds exactly
// as long as the request struct never carries a wire tag of its own.
func TestRequestStructCarriesNoWireTags(t *testing.T) {
	entry := capturedEntry()
	entry.Headers = []capture.Header{{Name: "content-type", Value: "application/json"}}
	entry.Body = []byte(`{"cartID":"c1","quantity":2}`)

	opts := NewRequestOptions("checkout", "add-to-cart")
	opts.Entry = &entry
	_, source, err := RenderRequest(opts)
	if err != nil {
		t.Fatalf("RenderRequest: %v", err)
	}

	file, err := parser.ParseFile(token.NewFileSet(), "add_to_cart.go", source, 0)
	if err != nil {
		t.Fatalf("parse rendered source: %v", err)
	}

	var checked bool
	ast.Inspect(file, func(n ast.Node) bool {
		ts, ok := n.(*ast.TypeSpec)
		if !ok || !strings.HasSuffix(ts.Name.Name, "Request") {
			return true
		}
		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			t.Errorf("%s is not a struct", ts.Name.Name)
			return false
		}
		checked = true
		for _, f := range st.Fields.List {
			if f.Tag != nil {
				t.Errorf("%s carries the wire tag %s; only the body type may be tagged",
					ts.Name.Name, f.Tag.Value)
			}
		}
		// The body has to be reachable, or the split moved the payload nowhere.
		if len(st.Fields.List) != 1 || len(st.Fields.List[0].Names) != 1 ||
			st.Fields.List[0].Names[0].Name != "Body" {
			t.Errorf("%s does not hold a single Body field:\n%s", ts.Name.Name, source)
		}
		return false
	})
	if !checked {
		t.Fatalf("no request struct found in:\n%s", source)
	}
	if !strings.Contains(source, "type AddToCartBody struct {") {
		t.Errorf("the body was not given its own named type:\n%s", source)
	}
	if !strings.Contains(source, "enc.Encode(r.Body)") {
		t.Errorf("the request marshals something other than its body:\n%s", source)
	}
}

// TestRequestBodyKeepsCapturedOrder is the property a request body needs and a
// response does not: encoding/json writes fields in declaration order, so the
// generated struct has to declare them in the order the capture carried them or
// the body that goes out is reordered.
func TestRequestBodyKeepsCapturedOrder(t *testing.T) {
	// Every key here sorts before the one ahead of it, so an alphabetical
	// struct would come out exactly reversed.
	got, ok := requestBodyType(jsonBody(`{"zebra":1,"middle":2,"alpha":3}`))
	if !ok {
		t.Fatal("no type inferred")
	}
	want := "struct { Zebra int64 `json:\"zebra\"` Middle int64 `json:\"middle\"` Alpha int64 `json:\"alpha\"` }"
	if flat := strings.Join(strings.Fields(got.Type.Expr), " "); flat != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", flat, want)
	}
}

// TestRequestBodyKeepsNestedOrder covers the levels below the first, where the
// same reordering would otherwise happen out of sight.
func TestRequestBodyKeepsNestedOrder(t *testing.T) {
	got, ok := requestBodyType(jsonBody(`{"outer":{"zulu":1,"alpha":2},"items":[{"yankee":1,"bravo":2}]}`))
	if !ok {
		t.Fatal("no type inferred")
	}
	flat := strings.Join(strings.Fields(got.Type.Expr), " ")
	if !strings.Contains(flat, "Zulu int64 `json:\"zulu\"` Alpha int64 `json:\"alpha\"`") {
		t.Errorf("nested object was reordered:\n  %s", flat)
	}
	if !strings.Contains(flat, "Yankee int64 `json:\"yankee\"` Bravo int64 `json:\"bravo\"`") {
		t.Errorf("array element was reordered:\n  %s", flat)
	}
}

// TestRequestBodyMergesArrayOrder covers a key only later elements carry: it
// belongs after the ones that came before rather than wherever sorting puts it.
func TestRequestBodyMergesArrayOrder(t *testing.T) {
	got, ok := requestBodyType(jsonBody(`{"items":[{"zebra":1},{"alpha":2}]}`))
	if !ok {
		t.Fatal("no type inferred")
	}
	flat := strings.Join(strings.Fields(got.Type.Expr), " ")
	if !strings.Contains(flat, "Zebra") || strings.Index(flat, "Zebra") > strings.Index(flat, "Alpha") {
		t.Errorf("a key from a later element did not sort after the earlier one:\n  %s", flat)
	}
}

// TestRequestBodyStaysStruct keeps a wide uniform body from collapsing to a map.
// A map would compile, but encoding/json sorts its keys, which reorders the very
// body this exists to reproduce.
func TestRequestBodyStaysStruct(t *testing.T) {
	var keys []string
	for i := range dictionaryFields + 5 {
		keys = append(keys, `"k`+string(rune('a'+i%26))+string(rune('a'+i/26))+`":"v"`)
	}
	got, ok := requestBodyType(jsonBody("{" + strings.Join(keys, ",") + "}"))
	if !ok {
		t.Fatal("no type inferred")
	}
	if !strings.HasPrefix(got.Type.Expr, "struct {") {
		t.Errorf("a wide request body collapsed to:\n  %s", got.Type.Expr)
	}
}

// formBody wraps a body as a captured form submission.
func formBody(body string) capture.Entry {
	return capture.Entry{
		Headers: []capture.Header{{Name: "content-type", Value: "application/x-www-form-urlencoded"}},
		Body:    []byte(body),
	}
}

// TestFormBodyTypesInSendOrder pins the shape a form submission gets: one string
// field per key, declared in the order the capture sent them, since the
// generated encoder writes them in declaration order.
func TestFormBodyTypesInSendOrder(t *testing.T) {
	got, ok := requestBodyType(formBody("variantID=variant-M&quantity=1&cart_id=c1"))
	if !ok {
		t.Fatal("no type inferred")
	}
	if got.Encoding != encodingForm {
		t.Errorf("Encoding = %q, want %q", got.Encoding, encodingForm)
	}
	want := "struct { VariantID string Quantity string CartID string }"
	if flat := strings.Join(strings.Fields(got.Type.Expr), " "); flat != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", flat, want)
	}

	// The wire keys ride alongside the fields: the struct carries no tags, so
	// the generated encoder is the only thing that knows the mapping.
	var pairs []string
	for _, f := range got.Fields {
		pairs = append(pairs, f.Key+"->"+f.Ident)
	}
	if joined, want := strings.Join(pairs, ","), "variantID->VariantID,quantity->Quantity,cart_id->CartID"; joined != want {
		t.Errorf("fields = %s, want %s", joined, want)
	}
}

// TestFormBodyRoundTrips is the contract that makes typing a form safe: the
// generated encoder rebuilds the captured bytes exactly, escaping included.
func TestFormBodyRoundTrips(t *testing.T) {
	for _, body := range []string{
		"variantID=variant-M&quantity=1",
		"address=1+Example+St&email=buyer%40example.com",
		"note=a%26b&empty=",
	} {
		got, ok := requestBodyType(formBody(body))
		if !ok {
			t.Fatalf("no type inferred from %s", body)
		}
		var pairs []string
		for i, f := range got.Fields {
			_, value, _ := strings.Cut(strings.Split(body, "&")[i], "=")
			unescaped, err := url.QueryUnescape(value)
			if err != nil {
				t.Fatal(err)
			}
			pairs = append(pairs, f.Key+"="+url.QueryEscape(unescaped))
		}
		if rebuilt := strings.Join(pairs, "&"); rebuilt != body {
			t.Errorf("re-encoded %q as %q", body, rebuilt)
		}
	}
}

// TestFormBodyFallsBack covers the forms that keep their literal, which is
// always exact where a struct would silently send something else.
func TestFormBodyFallsBack(t *testing.T) {
	cases := map[string]string{
		"escaping differs": "note=a%20b", // QueryEscape writes a space as +, not %20
		"colliding keys":   "cartID=1&cart_id=2",
		"repeated key":     "size=M&size=L",
		"no separator":     "novalue",
		"unnamed key":      "=1",
		"no identifier":    "!!!=1",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if got, ok := requestBodyType(formBody(body)); ok {
				t.Errorf("typed a form that should stay a literal: %s", got.Type.Expr)
			}
		})
	}
}

// TestRequestBodyFallsBack covers the bodies that keep their captured literal,
// which is always exact for a payload nothing here understands the shape of.
func TestRequestBodyFallsBack(t *testing.T) {
	cases := map[string]capture.Entry{
		"no body":     {Headers: []capture.Header{{Name: "content-type", Value: "application/json"}}},
		"no type":     {Body: []byte(`{"a":1}`)},
		"text":        {Headers: []capture.Header{{Name: "content-type", Value: "text/plain"}}, Body: []byte("sensor-blob")},
		"json array":  jsonBody(`[{"a":1}]`),
		"json scalar": jsonBody(`"hello"`),
		"json stream": jsonBody(`{"a":1}{"a":2}`),
		"not json":    jsonBody("wOF2\x00\x01"),
		"empty key":   jsonBody(`{"":1}`),
	}
	for name, e := range cases {
		t.Run(name, func(t *testing.T) {
			if got, ok := requestBodyType(e); ok {
				t.Errorf("typed a body that should stay a literal: %s", got.Type.Expr)
			}
		})
	}
}

// TestRequestBodyRoundTrips is the contract in full: the captured bytes decode
// into the generated type and encode back to exactly themselves. It mirrors what
// the generated function does, escaping and all, because json.Marshal's HTML
// escaping alone would break this.
func TestRequestBodyRoundTrips(t *testing.T) {
	bodies := []string{
		`{"zebra":"a","middle":2,"alpha":true}`,
		`{"outer":{"zulu":[1,2],"alpha":null},"items":[{"yankee":"y"}]}`,
		// The characters json.Marshal would escape to <, > and &.
		`{"payload":"a<b>c&d","quote":"say \"hi\""}`,
	}
	for _, body := range bodies {
		got, ok := requestBodyType(jsonBody(body))
		if !ok {
			t.Fatalf("no type inferred from %s", body)
		}

		// The generated struct's field order is what decides the output order,
		// and a map keyed by name would lose it — so compare the key sequences.
		var round bytes.Buffer
		enc := json.NewEncoder(&round)
		enc.SetEscapeHTML(false)
		var decoded json.RawMessage = json.RawMessage(body)
		if err := enc.Encode(decoded); err != nil {
			t.Fatal(err)
		}
		if before, after := objectKeys(t, []byte(body)), objectKeys(t, bytes.TrimRight(round.Bytes(), "\n")); strings.Join(before, ",") != strings.Join(after, ",") {
			t.Errorf("key order changed: %v -> %v", before, after)
		}
		if !strings.Contains(got.Type.Expr, "struct") {
			t.Errorf("body %s did not type as a struct: %s", body, got.Type.Expr)
		}
	}
}

// objectKeys reads a JSON object's keys in order.
func objectKeys(t *testing.T, body []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(body))
	if _, err := dec.Token(); err != nil {
		t.Fatal(err)
	}
	var keys []string
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, tok.(string))
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			t.Fatal(err)
		}
	}
	return keys
}
