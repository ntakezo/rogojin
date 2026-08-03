package scaffold

import (
	"fmt"
	"go/format"
	"strings"
	"testing"

	"github.com/ntakezo/rogojin/internal/capture"
)

// jsonResponse wraps a body as a captured JSON reply.
func jsonResponse(body string) *capture.Response {
	return &capture.Response{StatusCode: 200, MediaType: "application/json", Body: []byte(body)}
}

// typeOf renders the inferred type with its whitespace collapsed, so assertions
// read as one line and do not pin gofmt's column alignment. It fails the test if
// the type does not compile, which is the property every case shares.
func typeOf(t *testing.T, body string) string {
	t.Helper()
	res, ok := responseType(jsonResponse(body))
	if !ok {
		t.Fatalf("no type inferred from %s", body)
	}
	if _, err := format.Source([]byte("package p\ntype T " + res.Expr)); err != nil {
		t.Fatalf("inferred type does not format: %v\n%s", err, res.Expr)
	}
	return strings.Join(strings.Fields(res.Expr), " ")
}

// TestResponseTypeFromJSON pins the inference: the Go type each JSON value maps
// to, and the name each key ends up under. Fields come out sorted, so the order
// here is alphabetical rather than the payload's.
func TestResponseTypeFromJSON(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"empty object", `{}`, "struct{}"},
		{"scalar kinds", `{"ok":true,"count":3,"price":1.5,"name":"x","missing":null}`,
			"struct { Count int64 `json:\"count\"` Missing any `json:\"missing\"` Name string `json:\"name\"` OK bool `json:\"ok\"` Price float64 `json:\"price\"` }"},
		// Fields sort by their JSON key, not by the Go name derived from it.
		{"initialisms run together", `{"cartID":"c","imageUrl":"u","csrf_token":"t"}`,
			"struct { CartID string `json:\"cartID\"` CSRFToken string `json:\"csrf_token\"` ImageURL string `json:\"imageUrl\"` }"},
		{"nested object", `{"order":{"id":"o1"}}`,
			"struct { Order struct { ID string `json:\"id\"` } `json:\"order\"` }"},
		{"top level array", `[{"id":1}]`, "[]struct { ID int64 `json:\"id\"` }"},
		{"top level scalar", `"hello"`, "string"},
		{"digit leading key", `{"2fa":true}`, "struct { F2fa bool `json:\"2fa\"` }"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := typeOf(t, c.body); got != c.want {
				t.Errorf("got:\n  %s\nwant:\n  %s", got, c.want)
			}
		})
	}
}

// TestResponseTypeTracksOptionality is what a typed response buys over reading
// the payload by hand: a key some elements omit is marked optional rather than
// looking guaranteed, and a sometimes-absent object becomes a pointer the caller
// has to check.
func TestResponseTypeTracksOptionality(t *testing.T) {
	got := typeOf(t, `[{"always":1,"sometimes":"x"},{"always":2}]`)
	for _, want := range []string{
		"Always int64 `json:\"always\"`",
		"Sometimes string `json:\"sometimes,omitempty\"`",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in:\n  %s", want, got)
		}
	}

	nested := typeOf(t, `[{"a":{"b":1}},{"c":2}]`)
	if !strings.Contains(nested, "A *struct") {
		t.Errorf("a sometimes-absent object was not a pointer:\n  %s", nested)
	}
}

// TestResponseTypeMergesArrayElements keeps an endpoint that varies its elements
// from being typed off the first one alone.
func TestResponseTypeMergesArrayElements(t *testing.T) {
	got := typeOf(t, `[{"a":1},{"b":"x"},{"a":1.5}]`)
	if !strings.Contains(got, "A float64") {
		t.Errorf("a field that was whole then fractional did not widen:\n  %s", got)
	}
	if !strings.Contains(got, "B string") {
		t.Errorf("a field only later elements carried was dropped:\n  %s", got)
	}
}

// TestResponseTypeWidensConflicts keeps a payload whose values disagree from
// producing source that cannot hold them.
func TestResponseTypeWidensConflicts(t *testing.T) {
	if got := typeOf(t, `[{"v":1},{"v":"one"}]`); !strings.Contains(got, "V any") {
		t.Errorf("conflicting field did not widen to any:\n  %s", got)
	}
}

// TestResponseTypeUsesMapForDictionaries covers the objects whose keys are data
// rather than field names. A struct hundreds of fields wide cannot be looked up
// by a key known only at run time, and misses every key the capture happened not
// to contain.
func TestResponseTypeUsesMapForDictionaries(t *testing.T) {
	var keys []string
	for i := range dictionaryFields {
		keys = append(keys, fmt.Sprintf("%q:%q", fmt.Sprintf("key.%d", i), "v"))
	}
	if got := typeOf(t, "{"+strings.Join(keys, ",")+"}"); got != "map[string]string" {
		t.Errorf("wide uniform object typed as:\n  %s\nwant map[string]string", got)
	}

	// One key short of the threshold is still a record.
	if got := typeOf(t, "{"+strings.Join(keys[:dictionaryFields-1], ",")+"}"); !strings.HasPrefix(got, "struct {") {
		t.Errorf("object below the threshold typed as:\n  %s\nwant a struct", got)
	}

	// Mixed value types are a record however many keys there are.
	mixed := append([]string(nil), keys...)
	mixed[0] = `"key.0":123`
	if got := typeOf(t, "{"+strings.Join(mixed, ",")+"}"); !strings.HasPrefix(got, "struct {") {
		t.Errorf("wide object with mixed values typed as:\n  %s\nwant a struct", got)
	}
}

// TestResponseTypeInfersTimestamps covers the imports an inferred type drags in:
// an RFC 3339 string becomes a time.Time, and the request file has to import
// time to declare one.
func TestResponseTypeInfersTimestamps(t *testing.T) {
	res, ok := responseType(jsonResponse(`{"startDate":"2028-07-10T00:00:00-04:00"}`))
	if !ok {
		t.Fatal("no type inferred")
	}
	if !strings.Contains(res.Expr, "time.Time") {
		t.Errorf("timestamp was not typed:\n  %s", res.Expr)
	}
	if strings.Join(res.Imports, ",") != "time" {
		t.Errorf("imports = %v, want [time]", res.Imports)
	}
}

// TestResponseTypeFallsBack covers every reason a response has no inferred
// shape. A mislabeled body matters most: servers really do serve fonts as
// x-www-form-urlencoded, and guessing at one must not produce broken source.
func TestResponseTypeFallsBack(t *testing.T) {
	cases := map[string]*capture.Response{
		"no response":     nil,
		"empty body":      {MediaType: "application/json"},
		"html":            {MediaType: "text/html", Body: []byte("<!DOCTYPE html><p>hi")},
		"binary":          {MediaType: "image/png", Body: []byte{0x89, 'P', 'N', 'G'}},
		"unknown type":    {MediaType: "", Body: []byte(`{"a":1}`)},
		"mislabeled json": {MediaType: "application/json", Body: []byte("wOF2\x00\x01not json")},
		"json stream":     {MediaType: "application/json", Body: []byte(`{"a":1}{"a":2}`)},
		"json null":       {MediaType: "application/json", Body: []byte(`null`)},
		"truncated":       {MediaType: "application/json", Body: []byte(`{"a":`)},
	}
	for name, r := range cases {
		t.Run(name, func(t *testing.T) {
			res, ok := responseType(r)
			if ok {
				t.Errorf("inferred a shape where none exists: %s", res.Expr)
			}
			if res.Expr != "struct{}" {
				t.Errorf("fallback type = %s, want struct{}", res.Expr)
			}
			if res.Imports != nil {
				t.Errorf("fallback wants imports %v", res.Imports)
			}
		})
	}
}

// TestResponseTypeAcceptsJSONSuffix covers the structured-suffix media types
// that carry most real API payloads.
func TestResponseTypeAcceptsJSONSuffix(t *testing.T) {
	for _, media := range []string{"application/json", "text/json", "application/problem+json", "application/speculationrules+json"} {
		r := &capture.Response{MediaType: media, Body: []byte(`{"a":1}`)}
		if _, ok := responseType(r); !ok {
			t.Errorf("media type %q was not typed as JSON", media)
		}
	}
}

// TestFieldIdentCollision keeps two keys that reduce to one identifier from
// declaring the same field twice, which would not compile. The generator does
// not dedupe, so this is entirely on us.
func TestFieldIdentCollision(t *testing.T) {
	got := typeOf(t, `{"cart_id":"a","cartId":"b","cart-id":"c"}`)
	for _, want := range []string{"CartID string", "CartID2 string", "CartID3 string"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in:\n  %s", want, got)
		}
	}
}

// TestFieldIdentSkipsUnnameableKeys covers the empty key, which is legal JSON
// and appeared in a real capture: it cannot be bound by a tag, so it has no
// field, and the keys around it must survive.
func TestFieldIdentSkipsUnnameableKeys(t *testing.T) {
	got := typeOf(t, `{"":"skipped","kept":"yes"}`)
	want := "struct { Kept string `json:\"kept\"` }"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}
}
