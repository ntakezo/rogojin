package scaffold

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// requestsDir is the domain-level package every workflow in a domain calls its
// HTTP through, so it is never mistaken for a workflow while scanning.
const requestsDir = "requests"

// workflowDecl is one workflow found in a domain: the package holding it, the
// field its Config sits under in the domain's Configs struct, and whether that
// Config asks for anything — an empty one has no unset state to refuse.
type workflowDecl struct {
	Package string
	Field   string
	Checked bool
}

// registerView is what the registration template renders from. Checked says at
// least one workflow's config is refused when unset, which is what the imports
// the check needs hang off.
type registerView struct {
	Package    string
	Domain     string
	ModulePath string
	Workflows  []workflowDecl
	Checked    bool
}

// WriteWorkflow renders one workflow into a domain, creating the domain around
// it on the first call, and re-derives the registration so the workflow is on
// the service the moment it lands.
func WriteWorkflow(destRoot, modulePath string, o Options) (written []string, createdDomain bool, err error) {
	domainDir := filepath.Join(destRoot, o.DomainPackage)
	switch _, err := os.Stat(domainDir); {
	case os.IsNotExist(err):
		createdDomain = true
	case err != nil:
		return nil, false, fmt.Errorf("stat %s: %w", domainDir, err)
	}

	written, err = Write(destRoot, modulePath, o, createdDomain)
	if err != nil {
		return nil, false, err
	}
	rel, err := WriteRegister(destRoot, modulePath, o.DomainPackage, o.Domain)
	if err != nil {
		return nil, false, err
	}
	return append(written, rel), createdDomain, nil
}

// WriteRegister re-derives the domain's registration from the workflow packages
// beside it and writes it, always replacing what was there: the file is
// generated, and a workflow added by any means belongs on the service.
func WriteRegister(destRoot, modulePath, pkg, domain string) (string, error) {
	dir := filepath.Join(destRoot, pkg)
	workflows, err := scanWorkflows(dir)
	if err != nil {
		return "", err
	}
	if len(workflows) == 0 {
		return "", fmt.Errorf("no workflows found in %s — a domain registers at least one", dir)
	}

	view := registerView{Package: pkg, Domain: domain, ModulePath: modulePath, Workflows: workflows}
	view.Checked = slices.ContainsFunc(workflows, func(w workflowDecl) bool { return w.Checked })

	rel := path.Join(pkg, pkg+".go")
	source, err := renderTemplate("templates/register.go.tmpl", view)
	if err != nil {
		return "", fmt.Errorf("%s: %w", rel, err)
	}
	if err := os.WriteFile(filepath.Join(destRoot, rel), []byte(source), 0o644); err != nil {
		return "", fmt.Errorf("write %s: %w", rel, err)
	}
	return rel, nil
}

// scanWorkflows finds every workflow package in a domain, by the two things a
// registration needs from one: the Name it registers under and the Config it is
// built from. A directory holding neither is not a workflow and is skipped;
// holding one but not the other is an error rather than a silent omission,
// since a workflow left unregistered fails only when a task names it.
func scanWorkflows(dir string) ([]workflowDecl, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", dir, err)
	}

	var found []workflowDecl
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() || name == requestsDir || name == "cmd" || strings.HasPrefix(name, ".") {
			continue
		}
		decl, ok, err := scanWorkflowPackage(filepath.Join(dir, name), name)
		if err != nil {
			return nil, err
		}
		if ok {
			found = append(found, decl)
		}
	}
	slices.SortFunc(found, func(a, b workflowDecl) int { return strings.Compare(a.Package, b.Package) })
	return found, nil
}

// scanWorkflowPackage reads one candidate directory, reporting whether it holds
// a workflow and what the registration must hand it.
func scanWorkflowPackage(dir, pkg string) (workflowDecl, bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return workflowDecl{}, false, fmt.Errorf("read %s: %w", dir, err)
	}

	var hasName, hasConfig, wantsConfig bool
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			return workflowDecl{}, false, fmt.Errorf("parse %s: %w", filepath.Join(dir, name), err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}
			for _, spec := range gen.Specs {
				switch s := spec.(type) {
				case *ast.ValueSpec:
					hasName = hasName || gen.Tok == token.CONST && slices.ContainsFunc(s.Names, func(n *ast.Ident) bool { return n.Name == "Name" })
				case *ast.TypeSpec:
					if s.Name.Name != "Config" {
						continue
					}
					hasConfig = true
					wantsConfig = configHasFields(s.Type)
				}
			}
		}
	}

	switch {
	case !hasName && !hasConfig:
		return workflowDecl{}, false, nil
	case !hasName:
		return workflowDecl{}, false, fmt.Errorf("%s declares a Config but no Name — a workflow registers under one, so add `const Name = %q`", dir, pkg)
	case !hasConfig:
		return workflowDecl{}, false, fmt.Errorf("%s declares a Name but no Config — the domain builds every workflow from one, so add `type Config struct{...}`", dir)
	}
	return workflowDecl{Package: pkg, Field: RequestIdent(pkg), Checked: wantsConfig}, true, nil
}

// configHasFields reports whether a Config asks the domain for anything. An
// empty one is satisfied by its zero value, so the registration has nothing to
// refuse and says nothing about it.
func configHasFields(expr ast.Expr) bool {
	shape, ok := expr.(*ast.StructType)
	return ok && shape.Fields != nil && len(shape.Fields.List) > 0
}

// DomainPosture reads back the choices a domain was created with, so a workflow
// added later inherits them rather than contradicting the entrypoint every
// workflow in the domain is registered from. It reports false when there is no
// domain yet, and the caller's flags stand.
func DomainPosture(destRoot, pkg string) (o Options, ok bool, err error) {
	main := filepath.Join(destRoot, pkg, "cmd", "run", "main.go")
	file, err := parser.ParseFile(token.NewFileSet(), main, nil, parser.ImportsOnly)
	if err != nil {
		if os.IsNotExist(err) {
			return Options{}, false, nil
		}
		return Options{}, false, fmt.Errorf("read the domain's entrypoint at %s: %w", main, err)
	}

	for _, spec := range file.Imports {
		value, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		switch value {
		case "github.com/ntakezo/rogojin/proxies":
			o.Proxy = true
		case "github.com/ntakezo/rogojin/persistence/tasksqlite":
			o.TaskPersist = true
		case "github.com/ntakezo/rogojin/persistence/proxysqlite":
			o.ProxyPersist = true
		}
	}
	return o, true, nil
}
