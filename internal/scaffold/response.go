package scaffold

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"strconv"
	"strings"

	jsonstruct "github.com/twpayne/go-jsonstruct/v3"

	"github.com/ntakezo/rogojin/internal/capture"
)

// responseTyper renders the Go type a captured response body unmarshals into.
// What "typed" means depends on the media type — a JSON object becomes a struct,
// an HTML document has no shape at all — so each family gets its own, and the
// first whose matches accepts the captured type wins.
type responseTyper struct {
	matches func(mediaType string) bool
	render  func(body []byte) (inferred, bool)
}

// inferred is a response type rendered from a captured body: the Go type
// expression to declare, and the imports the request file needs to compile it.
type inferred struct {
	goType  string
	imports []string
}

// typers is the dispatch table. Add a media type by adding an entry: nothing
// else in the renderer knows what a response looks like.
var typers = []responseTyper{
	{matches: isJSONMedia, render: jsonType},
}

// isJSONMedia accepts the JSON family, including the "+json" structured suffix
// that carries most API payloads (application/problem+json and friends).
func isJSONMedia(mediaType string) bool {
	return mediaType == "application/json" ||
		mediaType == "text/json" ||
		strings.HasSuffix(mediaType, "+json")
}

// responseType renders the Go type a captured response unmarshals into,
// reporting whether a shape was actually inferred. A media type nothing handles,
// and a body that does not parse as its media type claims, both fall back to an
// empty struct rather than guessing — servers do mislabel bodies.
func responseType(r *capture.Response) (inferred, bool) {
	none := inferred{goType: "struct{}"}
	if r == nil || len(r.Body) == 0 {
		return none, false
	}
	for _, t := range typers {
		if !t.matches(r.MediaType) {
			continue
		}
		if got, ok := t.render(r.Body); ok {
			return got, true
		}
	}
	return none, false
}

// inferredTypeName is the placeholder the JSON generator declares, which is
// stripped back off when the type is inlined into the request.
const inferredTypeName = "Response"

// jsonType infers a Go type from one JSON body. go-jsonstruct does the
// inference — including which keys were absent from some elements, which is
// what earns a field its omitempty — and the result is reduced to a bare type
// expression so it can live in the request file beside the function.
func jsonType(body []byte) (out inferred, ok bool) {
	// The generator indexes into an empty slice on some inputs, so a body that
	// trips it degrades to an untyped response instead of killing the command.
	defer func() {
		if recover() != nil {
			out, ok = inferred{}, false
		}
	}()

	if !singleJSONDocument(body) {
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
	goType, imports, err := inlineType(src, inferredTypeName, inlineOpts{dictionaries: true})
	if err != nil || goType == "" || goType == "any" {
		return inferred{}, false
	}
	return inferred{goType: goType, imports: imports}, true
}

// singleJSONDocument reports whether body is exactly one JSON value. A stream
// of them would otherwise be typed from its first alone, which describes a
// fraction of the response and says nothing about it.
func singleJSONDocument(body []byte) bool {
	dec := json.NewDecoder(bytes.NewReader(body))
	var v any
	if err := dec.Decode(&v); err != nil {
		return false
	}
	return !dec.More()
}

// exportFieldName names a struct field after its JSON key. It never returns the
// empty string: the generator would emit an embedded field, and a key with no
// usable identifier is dealt with on the way out instead.
func exportFieldName(key string) string {
	if name := fieldIdent(key); name != "" {
		return name
	}
	return "Field"
}

// inlineOpts tunes how a generated type is reduced for the use it is put to.
type inlineOpts struct {
	// order restores the capture's key order, for a type that is marshaled back
	// onto the wire and whose field order therefore shows up in the request.
	order *keyOrder
	// dictionaries collapses a wide uniform object to a map. That suits a
	// response, and never a request body: encoding/json sorts a map's keys.
	dictionaries bool
}

// inlineType reduces a generated file to the bare type expression it declares,
// so the type can sit in the request file next to the function that uses it
// rather than in one of its own. It also reports the imports that expression
// needs, since an inferred timestamp pulls in time.
func inlineType(src []byte, typeName string, opts inlineOpts) (string, []string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "response.go", src, 0)
	if err != nil {
		return "", nil, err
	}

	var imports []string
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return "", nil, err
		}
		imports = append(imports, path)
	}

	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.TYPE {
			continue
		}
		for _, spec := range gen.Specs {
			ts, ok := spec.(*ast.TypeSpec)
			if !ok || ts.Name.Name != typeName {
				continue
			}
			normalized := normalizeType(ts.Type, fset, opts)
			orderAsCaptured(normalized, opts.order)
			var buf bytes.Buffer
			if err := printer.Fprint(&buf, fset, normalized); err != nil {
				return "", nil, err
			}
			return buf.String(), imports, nil
		}
	}
	return "", nil, fmt.Errorf("generated source declares no type %s", typeName)
}

// normalizeType fixes up an inferred type for use as a response: it drops the
// fields Go cannot bind, keeps two keys that reduce to one name from colliding,
// and turns an object whose keys are data into the map that models it.
func normalizeType(node ast.Expr, fset *token.FileSet, opts inlineOpts) ast.Expr {
	switch t := node.(type) {
	case *ast.ArrayType:
		t.Elt = normalizeType(t.Elt, fset, opts)
	case *ast.StarExpr:
		t.X = normalizeType(t.X, fset, opts)
	case *ast.MapType:
		t.Value = normalizeType(t.Value, fset, opts)
	case *ast.StructType:
		kept := t.Fields.List[:0]
		taken := make(map[string]bool, len(t.Fields.List))
		for _, f := range t.Fields.List {
			// A field tagged with an empty JSON name binds nothing: Go would
			// read it back under the field's own name instead.
			if f.Tag != nil && strings.Contains(f.Tag.Value, `json:""`) {
				continue
			}
			f.Type = normalizeType(f.Type, fset, opts)
			for i, name := range f.Names {
				unique := uniqueFieldName(name.Name, taken)
				taken[unique] = true
				f.Names[i] = ast.NewIdent(unique)
			}
			kept = append(kept, f)
		}
		t.Fields.List = kept
		if opts.dictionaries {
			if m := asDictionary(t, fset); m != nil {
				return m
			}
		}
	}
	return node
}

// dictionaryFields is the number of same-shaped keys past which an object reads
// as a lookup table rather than a record, and is typed as a map. Real payloads
// carry translation and price maps with hundreds of keys; a struct that wide is
// unusable, misses every key the capture happened not to contain, and cannot be
// looked up by a value known only at run time.
const dictionaryFields = 24

// asDictionary returns the map that models a struct whose keys are data, or nil
// when the struct is a record and should stay one.
func asDictionary(st *ast.StructType, fset *token.FileSet) ast.Expr {
	if len(st.Fields.List) < dictionaryFields {
		return nil
	}
	value := st.Fields.List[0].Type
	want := exprString(value, fset)
	for _, f := range st.Fields.List[1:] {
		if exprString(f.Type, fset) != want {
			return nil
		}
	}
	return &ast.MapType{Key: ast.NewIdent("string"), Value: value}
}

// exprString renders a type expression so two of them can be compared.
func exprString(e ast.Expr, fset *token.FileSet) string {
	var b strings.Builder
	if err := printer.Fprint(&b, fset, e); err != nil {
		return ""
	}
	return b.String()
}

// fieldIdent derives an exported Go field name from a JSON key, keeping the
// initialisms Go style capitalizes whole and running the components together as
// Go names do. It returns the empty string for a key that yields no identifier.
func fieldIdent(key string) string {
	var b strings.Builder
	for _, word := range splitWords(key) {
		if initialisms[strings.ToUpper(word)] {
			b.WriteString(strings.ToUpper(word))
			continue
		}
		r := []rune(word)
		b.WriteString(strings.ToUpper(string(r[0])))
		b.WriteString(string(r[1:]))
	}
	out := b.String()
	if out == "" {
		return ""
	}
	if out[0] >= '0' && out[0] <= '9' {
		return "F" + out
	}
	return out
}

// initialisms are the abbreviations Go writes in full caps, so a generated field
// reads the way the same field would if it had been typed by hand.
var initialisms = map[string]bool{
	"API": true, "ASCII": true, "CSRF": true, "CSS": true, "DNS": true, "EOF": true,
	"GUID": true, "HTML": true, "HTTP": true, "HTTPS": true, "ID": true, "IP": true,
	"JSON": true, "JWT": true, "OK": true, "SKU": true, "SLA": true, "SQL": true,
	"SSO": true, "TLS": true, "TTL": true, "UI": true, "UID": true, "URI": true,
	"URL": true, "UTF8": true, "UUID": true, "XML": true,
}

// uniqueFieldName keeps two distinct JSON keys that reduce to one identifier
// from colliding, since a struct cannot declare the same field twice.
func uniqueFieldName(name string, taken map[string]bool) string {
	if !taken[name] {
		return name
	}
	for i := 2; ; i++ {
		candidate := name + strconv.Itoa(i)
		if !taken[candidate] {
			return candidate
		}
	}
}
