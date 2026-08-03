package scaffold

import (
	"bytes"
	"strings"

	"github.com/ntakezo/rogojin/internal/capture"
	"github.com/ntakezo/rogojin/internal/jsontype"
)

// A bodyTyper types the payloads of one media-type family, in both directions:
// the Go type a captured request body is marshaled back from, and the one a
// captured response unmarshals into. Both bodies are identified the same way,
// by their content type, and both lean on the same inference — a family
// declares the two renders side by side so the directions cannot drift apart.
type bodyTyper struct {
	matches  func(mediaType string) bool
	request  func(body []byte) (jsontype.Type, bool)
	response func(body []byte) (jsontype.Type, bool)
}

// bodyTypers is the dispatch table. Add a media type by adding an entry:
// nothing else in the renderer knows what a payload looks like. A media type
// nothing here handles keeps its captured bytes — a verbatim literal for a
// request, which is always exact, and an untyped struct{} for a response.
var bodyTypers = []bodyTyper{
	{matches: jsontype.IsJSONMediaType, request: jsonRequestType, response: jsonResponseType},
}

// requestBodyType renders the Go type a captured request body is built from,
// reporting false when the body should stay a verbatim literal instead.
func requestBodyType(e capture.Entry) (jsontype.Type, bool) {
	if len(e.Body) == 0 {
		return jsontype.Type{}, false
	}
	mediaType := headerValue(e.Headers, "content-type")
	for _, t := range bodyTypers {
		if !t.matches(mediaType) {
			continue
		}
		if got, ok := t.request(e.Body); ok {
			return got, true
		}
	}
	return jsontype.Type{}, false
}

// responseType renders the Go type a captured response unmarshals into,
// reporting whether a shape was actually inferred. A media type nothing
// handles, and a body that does not parse as its media type claims, both fall
// back to an empty struct rather than guessing — servers do mislabel bodies.
func responseType(r *capture.Response) (jsontype.Type, bool) {
	none := jsontype.Type{Expr: "struct{}"}
	if r == nil || len(r.Body) == 0 {
		return none, false
	}
	for _, t := range bodyTypers {
		if !t.matches(r.MediaType) {
			continue
		}
		if got, ok := t.response(r.Body); ok {
			return got, true
		}
	}
	return none, false
}

// headerValue returns a captured header's value with any parameters stripped,
// so "application/json; charset=utf-8" matches as "application/json". Only the
// request side needs this: a captured response carries its media type already
// parsed.
func headerValue(headers []capture.Header, name string) string {
	for _, h := range headers {
		if !strings.EqualFold(h.Name, name) {
			continue
		}
		value, _, _ := strings.Cut(h.Value, ";")
		return strings.ToLower(strings.TrimSpace(value))
	}
	return ""
}

// jsonRequestType types a JSON request body as the struct that marshals back
// to it. Only an object becomes a type: an array or a scalar cannot also carry
// the fields a caller lifts out of the request, so those keep their literal.
// The struct keeps the captured key order and never collapses to a dictionary
// map: encoding/json writes fields in declaration order but sorts a map's
// keys, so either would reorder the very body this type is meant to reproduce.
func jsonRequestType(body []byte) (jsontype.Type, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return jsontype.Type{}, false
	}
	got, ok := jsontype.Infer(body, jsontype.Options{KeepOrder: true})
	if !ok {
		return jsontype.Type{}, false
	}
	// A struct with nothing in it marshals to {}, which is not the body that was
	// captured — a key Go cannot bind leaves one behind. Keep the literal.
	if flat := strings.Join(strings.Fields(got.Expr), " "); flat == "struct{}" || flat == "struct { }" {
		return jsontype.Type{}, false
	}
	return got, true
}

// dictionaryFields is the number of same-shaped keys past which a response
// object reads as a lookup table rather than a record, and is typed as a map.
// Real payloads carry translation and price maps with hundreds of keys; a
// struct that wide is unusable, misses every key the capture happened not to
// contain, and cannot be looked up by a value known only at run time.
const dictionaryFields = 24

// jsonResponseType infers a Go type from one JSON response body. Inference
// widens what it cannot reconcile, and a body that reduces to bare any — a
// null, say — taught us nothing worth declaring, so it stays untyped.
func jsonResponseType(body []byte) (jsontype.Type, bool) {
	got, ok := jsontype.Infer(body, jsontype.Options{DictionaryFields: dictionaryFields})
	if !ok || got.Expr == "any" {
		return jsontype.Type{}, false
	}
	return got, true
}
