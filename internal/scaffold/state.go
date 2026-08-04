package scaffold

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// graphFile is the derived file: it is rewritten from the states package on
// every write, so nothing may be hand-edited into it and it is never scanned.
const graphFile = "graph.go"

// stateDecl is one state found in the states package: the constant naming it,
// the Context method handling it, and the wire name the constant holds.
type stateDecl struct {
	Const   string
	Handler string
	Name    string
}

// graphView is what the graph template renders from.
type graphView struct {
	Initial string
	States  []stateDecl
}

// StateOptions configures one state render. Const, Handler, and File are
// derived from the raw name; Next is the constant of the state to advance to,
// empty for a terminal state, and Request is the identifier of the request the
// state calls, empty for one that calls none.
type StateOptions struct {
	Domain   string
	Workflow string
	Name     string

	DomainPackage string
	Package       string
	Const         string
	Handler       string
	File          string

	ModulePath string
	Next       string
	Request    string

	// Decode says the state unmarshals the reply into the request's response
	// type. It is false when that type was inferred from nothing — an HTML page,
	// or any body no typer understood — where decoding as JSON only ever fails.
	Decode bool
}

// NewStateOptions derives the render options for a state from the raw workflow
// and state names as the user typed them.
func NewStateOptions(domain, workflow, name string) StateOptions {
	return StateOptions{
		Domain:        domain,
		Workflow:      workflow,
		Name:          name,
		DomainPackage: PackageName(domain),
		Package:       PackageName(workflow),
		Const:         StateConst(name),
		Handler:       RequestIdent(name),
		File:          RequestFile(name),
	}
}

// Validate rejects names that yield no usable package or Go identifier, which
// would otherwise surface later as a confusing format error over the source.
func (o StateOptions) Validate() error {
	if o.Domain == "" {
		return fmt.Errorf("domain name is required")
	}
	if o.Workflow == "" {
		return fmt.Errorf("workflow name is required")
	}
	if o.Name == "" {
		return fmt.Errorf("state name is required")
	}
	if o.DomainPackage == "" || o.Package == "" {
		return fmt.Errorf("domain %q or workflow %q has no valid package identifier", o.Domain, o.Workflow)
	}
	if o.Const == "" || o.Handler == "" {
		return fmt.Errorf("state name %q yields no usable Go identifier — it must contain a letter, and cannot start with a digit", o.Name)
	}
	if o.File == graphFile[:len(graphFile)-3] {
		return fmt.Errorf("state name %q would be written to graph.go, which is derived from the other states", o.Name)
	}
	return nil
}

// StateConst derives the unexported constant naming a state, so "add-to-cart"
// and "addToCart" both yield "addToCart". It returns the empty string when no
// identifier can be built.
func StateConst(name string) string {
	ident := RequestIdent(name)
	if ident == "" {
		return ""
	}
	return strings.ToLower(ident[:1]) + ident[1:]
}

// RenderState returns the state's path relative to the destination root and its
// formatted source.
func RenderState(o StateOptions) (string, string, error) {
	if err := o.Validate(); err != nil {
		return "", "", err
	}
	outPath := path.Join(o.DomainPackage, o.Package, "states", o.File+".go")
	source, err := renderTemplate("templates/states/state.go.tmpl", o)
	if err != nil {
		return "", "", fmt.Errorf("%s: %w", outPath, err)
	}
	return outPath, source, nil
}

// WriteState renders one state into an existing workflow and rewrites the graph
// around it, so a state is wired the moment it lands. It refuses to overwrite
// unless force is set, and reports whether it replaced one.
func WriteState(destRoot string, o StateOptions, force bool, initial string) (rel string, overwrote bool, err error) {
	// A state belongs to a workflow: without one there is no Context for the
	// handler to hang off, and no graph to register it in.
	statesDir := filepath.Join(destRoot, o.DomainPackage, o.Package, "states")
	if _, err := os.Stat(statesDir); os.IsNotExist(err) {
		return "", false, fmt.Errorf("no workflow package %q at %s — run `rogojin workflow %s %s` first", o.Package, filepath.Join(destRoot, o.DomainPackage, o.Package), o.Domain, o.Workflow)
	} else if err != nil {
		return "", false, fmt.Errorf("stat %s: %w", statesDir, err)
	}

	// Whether the reply is worth decoding is the request's to answer, so read it
	// off the function being called rather than guessing at the call site.
	if o.Request != "" {
		if o.Decode, err = requestDecodes(filepath.Join(destRoot, o.DomainPackage, requestsDir), o.Request); err != nil {
			return "", false, err
		}
	}

	rel, source, err := RenderState(o)
	if err != nil {
		return "", false, err
	}

	full := filepath.Join(destRoot, rel)
	switch _, err := os.Stat(full); {
	case err == nil:
		if !force {
			return "", false, fmt.Errorf("refusing to overwrite existing file: %s — pass --force to replace it", full)
		}
		overwrote = true
	case !os.IsNotExist(err):
		return "", false, fmt.Errorf("stat %s: %w", full, err)
	}

	if err := os.WriteFile(full, []byte(source), 0o644); err != nil {
		return "", false, fmt.Errorf("write %s: %w", full, err)
	}
	if _, err := WriteGraph(destRoot, o.DomainPackage, o.Package, initial); err != nil {
		return "", false, err
	}
	return rel, overwrote, nil
}

// WriteGraph re-derives the graph from every state in the package and writes it,
// always replacing what was there: the file is generated, and a state added by
// any means — this tool or by hand — belongs in it.
func WriteGraph(destRoot, domainPkg, pkg, initial string) (string, error) {
	dir := filepath.Join(destRoot, domainPkg, pkg, "states")
	states, err := scanStateDir(dir)
	if err != nil {
		return "", err
	}
	if len(states) == 0 {
		return "", fmt.Errorf("no states found in %s — a graph needs at least one", dir)
	}

	rel := path.Join(domainPkg, pkg, "states", graphFile)
	full := filepath.Join(destRoot, rel)
	if initial == "" {
		if initial, err = resolveInitial(full, states); err != nil {
			return "", err
		}
	}
	if !slices.ContainsFunc(states, func(s stateDecl) bool { return s.Const == initial }) {
		return "", fmt.Errorf("initial state %q is not declared in %s", initial, dir)
	}

	source, err := renderTemplate("templates/states/graph.go.tmpl", graphView{Initial: initial, States: states})
	if err != nil {
		return "", fmt.Errorf("%s: %w", rel, err)
	}
	if err := os.WriteFile(full, []byte(source), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", full, err)
	}
	return rel, nil
}

// resolveInitial keeps the graph's entry state across a rewrite by reading it
// back out of the graph that is being replaced. A package with one state needs
// no such answer; one with several and no readable graph has to be told.
func resolveInitial(graphPath string, states []stateDecl) (string, error) {
	if got, err := readInitial(graphPath); err == nil {
		return got, nil
	}
	if len(states) == 1 {
		return states[0].Const, nil
	}
	return "", fmt.Errorf("cannot tell which state %s starts at — pass --initial <state>", graphPath)
}

// readInitial reads the entry state out of an existing graph: the first
// argument of the NewGraph call it returns.
func readInitial(graphPath string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), graphPath, nil, 0)
	if err != nil {
		return "", err
	}
	var initial string
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 || !isSelector(call.Fun, "workflows", "NewGraph") {
			return true
		}
		if ident, ok := call.Args[0].(*ast.Ident); ok {
			initial = ident.Name
		}
		return false
	})
	if initial == "" {
		return "", fmt.Errorf("no workflows.NewGraph call in %s", graphPath)
	}
	return initial, nil
}

// requestDecodes reports whether a state calling ident should unmarshal the
// reply into its response type, by reading the request that was generated. A
// response typed from nothing is an empty struct, and decoding an HTML page — or
// any body no typer understood — into one only ever fails at run time.
//
// A request that is not there fails the write: the state would name a function
// that does not exist, and the tree would not build.
func requestDecodes(dir, ident string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, fmt.Errorf("no requests package at %s — generate %s first with `rogojin request`", dir, ident)
	}

	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			return false, fmt.Errorf("parse %s: %w", filepath.Join(dir, name), err)
		}
		if response, ok := declaredResponse(file, ident); ok {
			// Only an empty struct says nothing was typed. A slice or a map is
			// still JSON, and still worth decoding into.
			shape, isStruct := response.(*ast.StructType)
			return !isStruct || (shape.Fields != nil && len(shape.Fields.List) > 0), nil
		}
	}
	return false, fmt.Errorf("no request %s in %s — generate it first with `rogojin request`", ident, dir)
}

// declaredResponse returns the type identResponse is declared as, if the file
// declares both it and the request function that returns it.
func declaredResponse(file *ast.File, ident string) (ast.Expr, bool) {
	var response ast.Expr
	var hasFunc bool
	for _, decl := range file.Decls {
		switch d := decl.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil && d.Name.Name == ident {
				hasFunc = true
			}
		case *ast.GenDecl:
			for _, spec := range d.Specs {
				if ts, ok := spec.(*ast.TypeSpec); ok && ts.Name.Name == ident+"Response" {
					response = ts.Type
				}
			}
		}
	}
	return response, hasFunc && response != nil
}

// scanStateDir reads every state declared in a states package on disk.
func scanStateDir(dir string) ([]stateDecl, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}
	sources := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") || name == graphFile {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", filepath.Join(dir, name), err)
		}
		sources[name] = string(raw)
	}
	return scanStates(sources)
}

// scanStates finds every state the given sources declare, pairing each state
// constant with the Context method that handles it. The pairing is the
// convention the whole layout rests on: a state named by the constant
// getHomepage is run by (*Context).GetHomepage.
//
// Half a pair is an error rather than a silent omission — a state left out of
// the graph fails at run time, far from the file that caused it.
func scanStates(sources map[string]string) ([]stateDecl, error) {
	consts := make(map[string]stateDecl)
	handlers := make(map[string]string)
	fset := token.NewFileSet()

	for _, name := range slices.Sorted(maps.Keys(sources)) {
		file, err := parser.ParseFile(fset, name, sources[name], 0)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", name, err)
		}
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.FuncDecl:
				if isStateHandler(d) {
					handlers[d.Name.Name] = name
				}
			case *ast.GenDecl:
				for _, got := range stateConsts(d) {
					if prior, ok := consts[got.Const]; ok {
						return nil, fmt.Errorf("state %q is declared twice, as %q and %q", got.Const, prior.Name, got.Name)
					}
					consts[got.Const] = got
				}
			}
		}
	}

	states := make([]stateDecl, 0, len(consts))
	for _, got := range consts {
		if _, ok := handlers[got.Handler]; !ok {
			return nil, fmt.Errorf("state %s is declared but nothing handles it — add `func (c *Context) %s(ctx context.Context) (*workflows.State, error)`", got.Const, got.Handler)
		}
		states = append(states, got)
	}
	for handler, file := range handlers {
		if _, ok := consts[strings.ToLower(handler[:1])+handler[1:]]; !ok {
			return nil, fmt.Errorf("%s declares the handler (*Context).%s but no state names it — add `const %s workflows.State = \"...\"`", file, handler, strings.ToLower(handler[:1])+handler[1:])
		}
	}

	slices.SortFunc(states, func(a, b stateDecl) int { return strings.Compare(a.Const, b.Const) })
	return states, nil
}

// stateConsts reads the workflows.State constants out of one declaration.
func stateConsts(decl *ast.GenDecl) []stateDecl {
	if decl.Tok != token.CONST {
		return nil
	}
	var out []stateDecl
	for _, spec := range decl.Specs {
		value, ok := spec.(*ast.ValueSpec)
		if !ok || !isSelector(value.Type, "workflows", "State") {
			continue
		}
		for i, name := range value.Names {
			if i >= len(value.Values) {
				continue
			}
			lit, ok := value.Values[i].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				continue
			}
			unquoted, err := strconv.Unquote(lit.Value)
			if err != nil {
				continue
			}
			out = append(out, stateDecl{
				Const:   name.Name,
				Handler: strings.ToUpper(name.Name[:1]) + name.Name[1:],
				Name:    unquoted,
			})
		}
	}
	return out
}

// isStateHandler reports whether fn has the exact shape of a state handler:
// func (c *Context) X(ctx context.Context) (*workflows.State, error).
func isStateHandler(fn *ast.FuncDecl) bool {
	if fn.Recv == nil || len(fn.Recv.List) != 1 || !fn.Name.IsExported() {
		return false
	}
	star, ok := fn.Recv.List[0].Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	if ident, ok := star.X.(*ast.Ident); !ok || ident.Name != "Context" {
		return false
	}
	if fn.Type.Params == nil || len(fn.Type.Params.List) != 1 ||
		!isSelector(fn.Type.Params.List[0].Type, "context", "Context") {
		return false
	}
	results := fn.Type.Results
	if results == nil || len(results.List) != 2 {
		return false
	}
	next, ok := results.List[0].Type.(*ast.StarExpr)
	if !ok || !isSelector(next.X, "workflows", "State") {
		return false
	}
	err, ok := results.List[1].Type.(*ast.Ident)
	return ok && err.Name == "error"
}

// isSelector reports whether expr is the qualified identifier pkg.name.
func isSelector(expr ast.Expr, pkg, name string) bool {
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	ident, ok := selector.X.(*ast.Ident)
	return ok && ident.Name == pkg
}
