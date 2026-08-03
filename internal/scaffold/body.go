package scaffold

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"reflect"
	"sort"
	"strings"

	jsonstruct "github.com/twpayne/go-jsonstruct/v3"

	"github.com/ntakezo/rogojin/internal/capture"
)

// bodyTyper renders the Go type a captured request body is built from. It
// mirrors the response typers, and differs in one way that drives everything
// here: a request body is marshaled back onto the wire, so the order of its
// fields is part of what was captured.
type bodyTyper struct {
	matches func(mediaType string) bool
	render  func(body []byte) (inferred, bool)
}

// bodyTypers is the dispatch table. A media type nothing here handles keeps its
// captured bytes as a literal, which is always exact.
var bodyTypers = []bodyTyper{
	{matches: isJSONMedia, render: jsonBodyType},
}

// requestBodyType renders the Go type a captured request body is built from,
// reporting false when the body should stay a verbatim literal instead.
func requestBodyType(e capture.Entry) (inferred, bool) {
	if len(e.Body) == 0 {
		return inferred{}, false
	}
	mediaType := headerValue(e.Headers, "content-type")
	for _, t := range bodyTypers {
		if !t.matches(mediaType) {
			continue
		}
		if got, ok := t.render(e.Body); ok {
			return got, true
		}
	}
	return inferred{}, false
}

// headerValue returns a captured header's value with any parameters stripped,
// so "application/json; charset=utf-8" matches as "application/json".
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

// jsonBodyType types a JSON request body as the struct that marshals back to it.
// Only an object becomes a type: an array or a scalar cannot also carry the
// fields a caller lifts out of the request, so those keep their literal.
func jsonBodyType(body []byte) (out inferred, ok bool) {
	defer func() {
		if recover() != nil {
			out, ok = inferred{}, false
		}
	}()

	if !singleJSONDocument(body) {
		return inferred{}, false
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return inferred{}, false
	}
	order, err := captureKeyOrder(body)
	if err != nil {
		return inferred{}, false
	}

	g := jsonstruct.NewGenerator(
		jsonstruct.WithTypeName(inferredTypeName),
		jsonstruct.WithExportNameFunc(exportFieldName),
		jsonstruct.WithIntType("int64"),
		jsonstruct.WithGoFormat(false),
	)
	if err := g.ObserveJSONReader(bytes.NewReader(body)); err != nil {
		return inferred{}, false
	}
	src, err := g.Generate()
	if err != nil {
		return inferred{}, false
	}

	// A request body keeps its captured key order and stays a struct: encoding
	// /json writes fields in declaration order, but sorts a map's keys, so a
	// dictionary here would reorder the body it is meant to reproduce.
	goType, imports, err := inlineType(src, inferredTypeName, inlineOpts{order: order})
	if err != nil || !strings.HasPrefix(goType, "struct") {
		return inferred{}, false
	}
	// A struct with nothing in it marshals to {}, which is not the body that was
	// captured — a key Go cannot bind leaves one behind. Keep the literal.
	if flat := strings.Join(strings.Fields(goType), " "); flat == "struct{}" || flat == "struct { }" {
		return inferred{}, false
	}
	return inferred{goType: goType, imports: imports}, true
}

// keyOrder mirrors a JSON document's shape, recording the order each object's
// keys first appeared in. Arrays merge their elements' orders, so a key only
// later elements carried still lands after the ones before it.
type keyOrder struct {
	keys     []string
	index    map[string]int
	children map[string]*keyOrder
	elem     *keyOrder
}

// captureKeyOrder reads the key order out of a JSON document. It walks tokens
// rather than unmarshaling, because a map would lose the very thing it records.
func captureKeyOrder(body []byte) (*keyOrder, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	return readKeyOrder(dec)
}

func readKeyOrder(dec *json.Decoder) (*keyOrder, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil, nil // a scalar orders nothing
	}

	switch delim {
	case '{':
		order := &keyOrder{index: map[string]int{}, children: map[string]*keyOrder{}}
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, _ := keyTok.(string)
			child, err := readKeyOrder(dec)
			if err != nil {
				return nil, err
			}
			if _, seen := order.index[key]; !seen {
				order.index[key] = len(order.keys)
				order.keys = append(order.keys, key)
			}
			order.children[key] = mergeKeyOrder(order.children[key], child)
		}
		_, err := dec.Token() // closing brace
		return order, err

	case '[':
		var elem *keyOrder
		for dec.More() {
			next, err := readKeyOrder(dec)
			if err != nil {
				return nil, err
			}
			elem = mergeKeyOrder(elem, next)
		}
		_, err := dec.Token() // closing bracket
		return &keyOrder{elem: elem}, err
	}
	return nil, nil
}

// mergeKeyOrder unions two orders, keeping the earlier document's sequence and
// appending whatever the later one introduced.
func mergeKeyOrder(a, b *keyOrder) *keyOrder {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	}
	for _, key := range b.keys {
		if _, seen := a.index[key]; !seen {
			a.index[key] = len(a.keys)
			a.keys = append(a.keys, key)
		}
		a.children[key] = mergeKeyOrder(a.children[key], b.children[key])
	}
	a.elem = mergeKeyOrder(a.elem, b.elem)
	return a
}

// orderAsCaptured sorts a generated type's fields back into the order the
// capture carried them, at every level. The generator sorts alphabetically,
// which would silently reorder a body that is marshaled from these fields.
func orderAsCaptured(node ast.Expr, order *keyOrder) {
	if order == nil {
		return
	}
	switch t := node.(type) {
	case *ast.ArrayType:
		orderAsCaptured(t.Elt, order.elem)
	case *ast.StarExpr:
		orderAsCaptured(t.X, order)
	case *ast.StructType:
		fields := t.Fields.List
		sort.SliceStable(fields, func(i, j int) bool {
			return captureIndex(order, fields[i]) < captureIndex(order, fields[j])
		})
		for _, f := range fields {
			orderAsCaptured(f.Type, order.children[jsonTagName(f)])
		}
	}
}

// captureIndex is a field's position in the captured document. A field the
// capture did not carry sorts last rather than jumping to the front.
func captureIndex(order *keyOrder, field *ast.Field) int {
	if i, ok := order.index[jsonTagName(field)]; ok {
		return i
	}
	return len(order.keys)
}

// jsonTagName is the key a field binds to, which is what ties it back to the
// captured document.
func jsonTagName(field *ast.Field) string {
	if field.Tag == nil {
		return ""
	}
	tag := reflect.StructTag(strings.Trim(field.Tag.Value, "`")).Get("json")
	name, _, _ := strings.Cut(tag, ",")
	return name
}
