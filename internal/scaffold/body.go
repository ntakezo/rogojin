package scaffold

import (
	"bytes"
	"net/url"
	"strings"

	"github.com/ntakezo/rogojin/internal/capture"
	"github.com/ntakezo/rogojin/internal/jsontype"
)

// requestBody is the render of a captured request body: the Go type it is built
// from, and how the generated function puts that type back on the wire. Fields
// is set only for an encoding the template writes field by field.
type requestBody struct {
	Type     jsontype.Type
	Encoding string
	Fields   []bodyField
}

// bodyField pairs a wire key with the Go field carrying it, in send order.
type bodyField struct {
	Key   string
	Ident string
}

// The encodings a typed request body renders as. Each is a branch in the
// captured template, which is the only thing that knows how to write one.
const (
	encodingJSON = "json"
	encodingForm = "form"
)

// A bodyTyper types the payloads of one media-type family, in both directions:
// the Go type a captured request body is marshaled back from, and the one a
// captured response unmarshals into. Both bodies are identified the same way,
// by their content type, and both lean on the same inference — a family
// declares the two renders side by side so the directions cannot drift apart.
type bodyTyper struct {
	matches  func(mediaType string) bool
	request  func(body []byte) (requestBody, bool)
	response func(body []byte) (jsontype.Type, bool)
}

// bodyTypers is the dispatch table. Add a media type by adding an entry:
// nothing else in the renderer knows what a payload looks like. A media type
// nothing here handles keeps its captured bytes — a verbatim literal for a
// request, which is always exact, and an untyped struct{} for a response.
var bodyTypers = []bodyTyper{
	{matches: jsontype.IsJSONMediaType, request: jsonRequestType, response: jsonResponseType},
	{matches: isFormMediaType, request: formRequestType, response: nil},
}

// requestBodyType renders the Go type a captured request body is built from,
// reporting false when the body should stay a verbatim literal instead.
func requestBodyType(e capture.Entry) (requestBody, bool) {
	if len(e.Body) == 0 {
		return requestBody{}, false
	}
	mediaType := headerValue(e.Headers, "content-type")
	for _, t := range bodyTypers {
		if t.request == nil || !t.matches(mediaType) {
			continue
		}
		if got, ok := t.request(e.Body); ok {
			return got, true
		}
	}
	return requestBody{}, false
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
		if t.response == nil || !t.matches(r.MediaType) {
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
func jsonRequestType(body []byte) (requestBody, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return requestBody{}, false
	}
	got, ok := jsontype.Infer(body, jsontype.Options{KeepOrder: true})
	if !ok {
		return requestBody{}, false
	}
	// A struct with nothing in it marshals to {}, which is not the body that was
	// captured — a key Go cannot bind leaves one behind. Keep the literal.
	if flat := strings.Join(strings.Fields(got.Expr), " "); flat == "struct{}" || flat == "struct { }" {
		return requestBody{}, false
	}
	return requestBody{Type: got, Encoding: encodingJSON}, true
}

// isFormMediaType accepts the one encoding a browser submits a form as.
func isFormMediaType(mediaType string) bool {
	return mediaType == "application/x-www-form-urlencoded"
}

// formRequestType types a urlencoded body as a struct of string fields, one per
// key in send order, that the generated function submits through OrderedForm.
// Every value is a string because the wire has no other type: a captured "1"
// could be a number or the digit, and guessing would change what goes out.
// Keys and values are held decoded, since OrderedForm.Set escapes both.
//
// It types only a body the render reproduces byte for byte. A capture escaped
// differently than url.QueryEscape does, a key that yields no field name or
// collides with another, and a pair with no "=" all keep the literal, which is
// exact where a struct would not be.
func formRequestType(body []byte) (requestBody, bool) {
	var fields []bodyField
	taken := make(map[string]bool)
	for _, pair := range strings.Split(string(body), "&") {
		rawKey, rawValue, ok := strings.Cut(pair, "=")
		if !ok || rawKey == "" {
			return requestBody{}, false
		}
		key, err := url.QueryUnescape(rawKey)
		if err != nil {
			return requestBody{}, false
		}
		if _, err := url.QueryUnescape(rawValue); err != nil {
			return requestBody{}, false
		}
		ident := jsontype.FieldIdent(key)
		if ident == "" || taken[ident] {
			return requestBody{}, false
		}
		taken[ident] = true
		fields = append(fields, bodyField{Key: key, Ident: ident})
	}
	if len(fields) == 0 || !formRoundTrips(body, fields) {
		return requestBody{}, false
	}

	var expr strings.Builder
	expr.WriteString("struct {\n")
	for _, f := range fields {
		expr.WriteString(f.Ident + " string\n")
	}
	expr.WriteString("}")
	return requestBody{Type: jsontype.Type{Expr: expr.String()}, Encoding: encodingForm, Fields: fields}, true
}

// formRoundTrips reports whether OrderedForm rebuilds body byte for byte, which
// is what makes a typed form safe to send in an exact literal's place. It
// mirrors what OrderedForm.Set does — escape the key and the value, join on "&"
// — so a capture it would re-escape differently is caught here rather than on
// the wire.
func formRoundTrips(body []byte, fields []bodyField) bool {
	pairs := strings.Split(string(body), "&")
	rebuilt := make([]string, 0, len(fields))
	for i, f := range fields {
		_, rawValue, _ := strings.Cut(pairs[i], "=")
		value, err := url.QueryUnescape(rawValue)
		if err != nil {
			return false
		}
		rebuilt = append(rebuilt, url.QueryEscape(f.Key)+"="+url.QueryEscape(value))
	}
	return strings.Join(rebuilt, "&") == string(body)
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
