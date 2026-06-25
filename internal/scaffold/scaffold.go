// Package scaffold renders a runnable rogojin workflow package from a small set
// of embedded templates. The four feature flags on Options gate the durability
// hooks, proxy leasing, and the persistence wiring in the generated main, so a
// generated tree always compiles and never carries code it cannot use.
package scaffold

import (
	"bufio"
	"bytes"
	"embed"
	"fmt"
	"go/format"
	"os"
	"path"
	"path/filepath"
	"strings"
	"text/template"
)

//go:embed templates
var templatesFS embed.FS

// Options configures one scaffold render. Name is the raw workflow ID; Package
// is derived from it as a valid Go identifier for the package and directory.
type Options struct {
	Name    string
	Package string

	// Durable emits Snapshot/RestoreContext/RestoreInstance for crash recovery.
	Durable bool
	// Output emits the Outputter implementation: a Result type the terminal
	// state fills and Output marshals on clean completion.
	Output bool
	// Proxy emits per-task proxy leasing and the Teardown that releases it.
	Proxy bool
	// TaskPersist wires a SQLite task repository in main; false uses a nil
	// (in-memory) repository.
	TaskPersist bool
	// ProxyPersist wires a SQLite proxy repository in main; false emits an
	// in-memory one. Meaningful only when Proxy is set.
	ProxyPersist bool
}

// templateData is what the templates see. ModulePath is the consuming module's
// path, resolved from its go.mod so generated imports point at the user's code.
type templateData struct {
	Options
	ModulePath string
}

// outputs maps each embedded template to the path it renders to, relative to the
// destination root. Paths use the package name as their leading segment.
func (o Options) outputs() map[string]string {
	return map[string]string{
		"templates/workflow.go.tmpl":       path.Join(o.Package, o.Package+".go"),
		"templates/states/context.go.tmpl": path.Join(o.Package, "states", "context.go"),
		"templates/states/graph.go.tmpl":   path.Join(o.Package, "states", "graph.go"),
		"templates/states/fetch.go.tmpl":   path.Join(o.Package, "states", "fetch.go"),
		"templates/states/process.go.tmpl": path.Join(o.Package, "states", "process.go"),
		"templates/cmd/run/main.go.tmpl":   path.Join(o.Package, "cmd", "run", "main.go"),
	}
}

// Validate rejects flag combinations that would generate code which lies about
// what it does, surfacing the conflict rather than emitting dead wiring.
func (o Options) Validate() error {
	if o.Name == "" {
		return fmt.Errorf("workflow name is required")
	}
	if o.Package == "" {
		return fmt.Errorf("workflow name %q has no valid package identifier", o.Name)
	}
	if o.Durable && !o.TaskPersist {
		return fmt.Errorf("a durable workflow needs task persistence: a nil task repository never writes the snapshots the durability hooks produce — pass --no-durable too")
	}
	if o.ProxyPersist && !o.Proxy {
		return fmt.Errorf("proxy persistence requires a proxy pool: set Proxy, or clear ProxyPersist")
	}
	return nil
}

// Render returns every generated file as relative-path -> formatted Go source.
// A render that produces invalid Go fails here, at format, before anything is
// written.
func Render(modulePath string, o Options) (map[string]string, error) {
	if err := o.Validate(); err != nil {
		return nil, err
	}
	data := templateData{Options: o, ModulePath: modulePath}

	files := make(map[string]string)
	for tmplPath, outPath := range o.outputs() {
		raw, err := templatesFS.ReadFile(tmplPath)
		if err != nil {
			return nil, fmt.Errorf("read template %s: %w", tmplPath, err)
		}
		tmpl, err := template.New(path.Base(tmplPath)).Parse(string(raw))
		if err != nil {
			return nil, fmt.Errorf("parse template %s: %w", tmplPath, err)
		}
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, data); err != nil {
			return nil, fmt.Errorf("execute template %s: %w", tmplPath, err)
		}
		formatted, err := format.Source(buf.Bytes())
		if err != nil {
			return nil, fmt.Errorf("format %s: %w\n--- rendered ---\n%s", outPath, err, buf.String())
		}
		files[outPath] = string(formatted)
	}
	return files, nil
}

// Write renders the workflow and writes it under destRoot, refusing to clobber
// any file that already exists so a mistaken re-run never overwrites edits.
func Write(destRoot, modulePath string, o Options) ([]string, error) {
	files, err := Render(modulePath, o)
	if err != nil {
		return nil, err
	}

	for rel := range files {
		full := filepath.Join(destRoot, rel)
		if _, err := os.Stat(full); err == nil {
			return nil, fmt.Errorf("refusing to overwrite existing file: %s", full)
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat %s: %w", full, err)
		}
	}

	written := make([]string, 0, len(files))
	for rel, content := range files {
		full := filepath.Join(destRoot, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			return nil, fmt.Errorf("mkdir for %s: %w", full, err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			return nil, fmt.Errorf("write %s: %w", full, err)
		}
		written = append(written, rel)
	}
	return written, nil
}

// ModulePath reads the module path from the go.mod at or above dir, so generated
// imports resolve inside the consuming module.
func ModulePath(dir string) (string, error) {
	gomod, err := findGoMod(dir)
	if err != nil {
		return "", err
	}
	f, err := os.Open(gomod)
	if err != nil {
		return "", err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if rest, ok := strings.CutPrefix(line, "module "); ok {
			return strings.TrimSpace(rest), nil
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("no module directive in %s", gomod)
}

// findGoMod walks up from dir to find the nearest go.mod.
func findGoMod(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for {
		candidate := filepath.Join(abs, "go.mod")
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return "", fmt.Errorf("no go.mod found at or above %s; run inside a Go module", dir)
		}
		abs = parent
	}
}

// PackageName derives a valid Go package identifier from a workflow name: it
// lowercases and drops every character that is not a letter, digit, or
// underscore, then ensures the result does not start with a digit. It returns
// the empty string when nothing usable remains.
func PackageName(name string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			if b.Len() == 0 {
				continue // a package identifier cannot start with a digit
			}
			b.WriteRune(r)
		}
	}
	return b.String()
}
