package jsontype_test

import (
	"fmt"
	"go/format"
	"strings"
	"testing"

	"github.com/ntakezo/rogojin/internal/jsontype"
)

// inferred runs Infer and returns the expression with its whitespace
// collapsed, so assertions read as one line. It fails the test if the
// expression does not compile, which is the property every case shares.
func inferred(t *testing.T, body string, opts jsontype.Options) string {
	t.Helper()
	got, ok := jsontype.Infer([]byte(body), opts)
	if !ok {
		t.Fatalf("no type inferred from %s", body)
	}
	if _, err := format.Source([]byte("package p\ntype T " + got.Expr)); err != nil {
		t.Fatalf("inferred type does not format: %v\n%s", err, got.Expr)
	}
	return strings.Join(strings.Fields(got.Expr), " ")
}

// TestInferPinsTypes pins the inference: the Go type each JSON value maps to,
// and the name each key ends up under. Without KeepOrder fields sort by their
// JSON key, so the order here is alphabetical rather than the payload's.
func TestInferPinsTypes(t *testing.T) {
	cases := []struct{ name, body, want string }{
		{"empty object", `{}`, "struct{}"},
		{"scalar kinds", `{"ok":true,"count":3,"price":1.5,"name":"x","missing":null}`,
			"struct { Count int64 `json:\"count\"` Missing any `json:\"missing\"` Name string `json:\"name\"` OK bool `json:\"ok\"` Price float64 `json:\"price\"` }"},
		{"initialisms run together", `{"cartID":"c","imageUrl":"u","csrf_token":"t"}`,
			"struct { CartID string `json:\"cartID\"` CSRFToken string `json:\"csrf_token\"` ImageURL string `json:\"imageUrl\"` }"},
		{"nested object", `{"order":{"id":"o1"}}`,
			"struct { Order struct { ID string `json:\"id\"` } `json:\"order\"` }"},
		{"top level array", `[{"id":1}]`, "[]struct { ID int64 `json:\"id\"` }"},
		{"top level scalar", `"hello"`, "string"},
		{"null learns nothing", `null`, "any"},
		{"empty array", `[]`, "[]any"},
		{"nullable field", `[{"a":"x"},{"a":null}]`, "[]struct { A *string `json:\"a\"` }"},
		{"digit leading key", `{"2fa":true}`, "struct { F2fa bool `json:\"2fa\"` }"},
		{"unnameable key binds placeholder", `{"日本語":1}`, "struct { Field int64 `json:\"日本語\"` }"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := inferred(t, c.body, jsontype.Options{}); got != c.want {
				t.Errorf("got:\n  %s\nwant:\n  %s", got, c.want)
			}
		})
	}
}

// TestInferTracksOptionality is what a typed payload buys over reading it by
// hand: a key some elements omit is marked optional rather than looking
// guaranteed, and a sometimes-absent object becomes a pointer the caller has
// to check.
func TestInferTracksOptionality(t *testing.T) {
	got := inferred(t, `[{"always":1,"sometimes":"x"},{"always":2}]`, jsontype.Options{})
	for _, want := range []string{
		"Always int64 `json:\"always\"`",
		"Sometimes string `json:\"sometimes,omitempty\"`",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in:\n  %s", want, got)
		}
	}

	nested := inferred(t, `[{"a":{"b":1}},{"c":2}]`, jsontype.Options{})
	if !strings.Contains(nested, "A *struct") {
		t.Errorf("a sometimes-absent object was not a pointer:\n  %s", nested)
	}

	// A field that was ever empty on the wire never earns omitempty: the tag
	// would drop it, and the type would round-trip to a different body.
	empty := inferred(t, `[{"a":""},{"b":1}]`, jsontype.Options{})
	if strings.Contains(empty, "a,omitempty") {
		t.Errorf("a sometimes-empty field earned omitempty:\n  %s", empty)
	}
}

// TestInferMergesAndWidens keeps a payload that varies from being typed off
// its first element alone, and one whose values disagree from producing source
// that cannot hold them.
func TestInferMergesAndWidens(t *testing.T) {
	got := inferred(t, `[{"a":1},{"b":"x"},{"a":1.5}]`, jsontype.Options{})
	if !strings.Contains(got, "A float64") {
		t.Errorf("a field that was whole then fractional did not widen:\n  %s", got)
	}
	if !strings.Contains(got, "B string") {
		t.Errorf("a field only later elements carried was dropped:\n  %s", got)
	}

	if got := inferred(t, `[{"v":1},{"v":"one"}]`, jsontype.Options{}); !strings.Contains(got, "V any") {
		t.Errorf("conflicting field did not widen to any:\n  %s", got)
	}
}

// TestInferKeepsOrder covers the property a marshaled type needs: fields
// declared in the order the document carried its keys, at every level, with a
// key only later array elements carried landing after the ones before it.
func TestInferKeepsOrder(t *testing.T) {
	got := inferred(t, `{"zebra":1,"middle":2,"alpha":3}`, jsontype.Options{KeepOrder: true})
	want := "struct { Zebra int64 `json:\"zebra\"` Middle int64 `json:\"middle\"` Alpha int64 `json:\"alpha\"` }"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}

	nested := inferred(t, `{"outer":{"zulu":1,"alpha":2},"items":[{"yankee":1,"bravo":2},{"delta":3}]}`,
		jsontype.Options{KeepOrder: true})
	for _, want := range []string{
		"Zulu int64 `json:\"zulu\"` Alpha int64 `json:\"alpha\"`",
		"Yankee int64 `json:\"yankee,omitempty\"` Bravo int64 `json:\"bravo,omitempty\"` Delta int64 `json:\"delta,omitempty\"`",
	} {
		if !strings.Contains(nested, want) {
			t.Errorf("missing %s in:\n  %s", want, nested)
		}
	}
}

// TestInferDictionaries covers the option's contract: a wide uniform object
// collapses to a map at the threshold and not below it, mixed values stay a
// record however many keys there are, and the zero value never collapses.
func TestInferDictionaries(t *testing.T) {
	const threshold = 24
	var keys []string
	for i := range threshold {
		keys = append(keys, fmt.Sprintf("%q:%q", fmt.Sprintf("key.%d", i), "v"))
	}
	wide := "{" + strings.Join(keys, ",") + "}"
	opts := jsontype.Options{DictionaryFields: threshold}

	if got := inferred(t, wide, opts); got != "map[string]string" {
		t.Errorf("wide uniform object typed as:\n  %s\nwant map[string]string", got)
	}
	if got := inferred(t, "{"+strings.Join(keys[:threshold-1], ",")+"}", opts); !strings.HasPrefix(got, "struct {") {
		t.Errorf("object below the threshold typed as:\n  %s\nwant a struct", got)
	}

	mixed := append([]string(nil), keys...)
	mixed[0] = `"key.0":123`
	if got := inferred(t, "{"+strings.Join(mixed, ",")+"}", opts); !strings.HasPrefix(got, "struct {") {
		t.Errorf("wide object with mixed values typed as:\n  %s\nwant a struct", got)
	}

	if got := inferred(t, wide, jsontype.Options{}); !strings.HasPrefix(got, "struct {") {
		t.Errorf("object collapsed with no threshold set:\n  %s\nwant a struct", got)
	}
}

// TestInferTimestamps covers the imports an inferred type drags in: an
// RFC 3339 string becomes a time.Time, and the declaring file has to import
// time to compile one.
func TestInferTimestamps(t *testing.T) {
	got, ok := jsontype.Infer([]byte(`{"startDate":"2028-07-10T00:00:00-04:00"}`), jsontype.Options{})
	if !ok {
		t.Fatal("no type inferred")
	}
	if !strings.Contains(got.Expr, "time.Time") {
		t.Errorf("timestamp was not typed:\n  %s", got.Expr)
	}
	if strings.Join(got.Imports, ",") != "time" {
		t.Errorf("imports = %v, want [time]", got.Imports)
	}

	plain, ok := jsontype.Infer([]byte(`{"a":1}`), jsontype.Options{})
	if !ok {
		t.Fatal("no type inferred")
	}
	if plain.Imports != nil {
		t.Errorf("a type needing no imports reported %v", plain.Imports)
	}
}

// TestInferFieldCollisions keeps two keys that reduce to one identifier from
// declaring the same field twice, which would not compile.
func TestInferFieldCollisions(t *testing.T) {
	got := inferred(t, `{"cart_id":"a","cartId":"b","cart-id":"c"}`, jsontype.Options{})
	for _, want := range []string{"CartID string", "CartID2 string", "CartID3 string"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %s in:\n  %s", want, got)
		}
	}
}

// TestInferSkipsUnbindableKeys covers the keys encoding/json cannot bind: the
// empty key, and keys holding a character the tag grammar reserves. They get
// no field, and the keys around them must survive.
func TestInferSkipsUnbindableKeys(t *testing.T) {
	got := inferred(t, `{"":"skipped","a b":1,"q\"q":2,"c,d":3,"kept":"yes"}`, jsontype.Options{})
	want := "struct { Kept string `json:\"kept\"` }"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}

	// An unbindable key stays skipped even when only some elements carry it: a
	// field tagged `json:",omitempty"` would bind its own name, not the key.
	optional := inferred(t, `[{"":"x","kept":1},{"kept":2}]`, jsontype.Options{})
	if strings.Contains(optional, "Field") {
		t.Errorf("a sometimes-present empty key grew a field:\n  %s", optional)
	}

	// A skipped key contributes nothing, imports included: a timestamp under
	// one must not drag in a time import the declared type never uses.
	dropped, ok := jsontype.Infer([]byte(`{"a b":"2028-07-10T00:00:00-04:00","kept":1}`), jsontype.Options{})
	if !ok {
		t.Fatal("no type inferred")
	}
	if dropped.Imports != nil {
		t.Errorf("a dropped field's imports leaked: %v", dropped.Imports)
	}
}

// TestInferMixedArrayShapes covers an array whose elements disagree about
// being containers at all, which must widen rather than fail — under KeepOrder
// too, where the ordering walk sees an array merge into an object.
func TestInferMixedArrayShapes(t *testing.T) {
	got := inferred(t, `{"a":[[1],{"b":2}],"c":3}`, jsontype.Options{KeepOrder: true})
	want := "struct { A []any `json:\"a\"` C int64 `json:\"c\"` }"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}
}

// TestInferRejects covers every input that yields no type at all: bodies that
// are not exactly one JSON document, and a key whose backtick would break out
// of the struct tag's raw literal and render source that is not Go.
func TestInferRejects(t *testing.T) {
	cases := map[string]string{
		"empty":         "",
		"not json":      "wOF2\x00\x01",
		"truncated":     `{"a":`,
		"stream":        `{"a":1}{"a":2}`,
		"trailing junk": `{"a":1}xyz`,
		"backtick key":  "{\"a`b\":1}",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if got, ok := jsontype.Infer([]byte(body), jsontype.Options{}); ok {
				t.Errorf("inferred a type where none exists: %s", got.Expr)
			}
		})
	}
}

// TestSplitWords pins the word splitting the field names — and the scaffolder's
// derived identifiers — are built from.
func TestSplitWords(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"add-to-cart", "add to cart"},
		{"addToCart", "add To Cart"},
		{"add_to_cart", "add to cart"},
		{"cartID", "cart ID"},
		{"", ""},
		{"--", ""},
	}
	for _, c := range cases {
		if got := strings.Join(jsontype.SplitWords(c.in), " "); got != c.want {
			t.Errorf("SplitWords(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
