// Package jsontype infers the Go type a JSON document maps to, from the
// document alone: the struct a captured response unmarshals into, or the one
// a captured request body is marshaled back from.
//
// Inference merges every observation of a value rather than sampling the
// first: array elements are folded together, so a key some elements omit is
// still typed — and earns an omitempty and, for an object, a pointer, which is
// how a generated type says what the payload actually guarantees. Values that
// disagree widen: a number that was whole and then fractional becomes float64,
// anything less reconcilable becomes any. Strings that all parse as RFC 3339
// timestamps become time.Time, which is why an inferred Type reports the
// imports it needs.
package jsontype

import (
	"bytes"
	"encoding/json"
	"go/parser"
	"maps"
	"slices"
	"strings"
)

// Options tunes inference for the use the type is put to.
type Options struct {
	// KeepOrder declares struct fields in the order the document first carried
	// their keys, at every level; otherwise fields sort by key. A type that is
	// marshaled back onto the wire needs the captured order: encoding/json
	// writes fields in declaration order.
	KeepOrder bool

	// DictionaryFields, when positive, collapses an object with at least that
	// many same-typed fields into a map[string]T — an object whose keys are
	// data rather than names. That suits a type that is unmarshaled into, and
	// never one that is marshaled back: encoding/json sorts a map's keys.
	DictionaryFields int
}

// Type is an inferred Go type: the bare type expression to declare, and the
// imports the declaring file needs to compile it — an inferred timestamp pulls
// in time.
type Type struct {
	Expr    string
	Imports []string
}

// Infer types exactly one JSON document. It reports false when body is not a
// single document — a stream would be typed from a fraction of the payload —
// or when the document renders no usable type expression.
func Infer(body []byte, opts Options) (Type, bool) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	root, err := observeDocument(dec, nil)
	if err != nil {
		return Type{}, false
	}
	if dec.More() {
		return Type{}, false
	}

	state := &renderState{opts: opts, imports: make(map[string]bool)}
	expr, _ := root.goType(0, state)
	// A pathological key — a backtick, say, which would end a struct tag's raw
	// literal — can render an expression that is not Go. Refuse it here rather
	// than hand the caller source that cannot format.
	if _, err := parser.ParseExpr(expr); err != nil {
		return Type{}, false
	}
	return Type{Expr: expr, Imports: slices.Sorted(maps.Keys(state.imports))}, true
}

// IsJSONMediaType accepts the JSON family, including the "+json" structured
// suffix that carries most API payloads (application/problem+json and friends).
func IsJSONMediaType(mediaType string) bool {
	return mediaType == "application/json" ||
		mediaType == "text/json" ||
		strings.HasSuffix(mediaType, "+json")
}
