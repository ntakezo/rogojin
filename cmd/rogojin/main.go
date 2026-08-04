// Command rogojin scaffolds workflows into a domain — the target they all speak
// to — so several workflows can share one requests package and one entrypoint.
//
// Usage:
//
//	rogojin workflow <domain> <name> [flags]
//	rogojin request <domain> <name> [--entry <id> [--har <file>]]
//	rogojin state <domain> <workflow> <name> [--request <name>] [--next <state>]
//
// Run it from inside the Go module that will own the domain; generated imports
// resolve against that module's path. The first "workflow" call creates the
// domain around it; the rest add to it. Nothing is overwritten without --force,
// which replaces one file and nothing else — except the two derived files, which
// are rewritten whenever what they are derived from changes: the domain's
// registration, from its workflow packages, and each workflow's states/graph.go,
// from its states package.
//
// Flags subtract features. Two shape one workflow, and may differ between the
// workflows of a domain:
//
//	--no-durable             omit the Snapshot/Restore recovery hooks
//	--no-output              omit the Output result hook
//
// Three shape the entrypoint every workflow in a domain registers from, so they
// belong to the domain and are fixed by the workflow that creates it:
//
//	--no-proxy               omit per-task proxy leasing
//	--no-task-persistence    run tasks in memory (nil repo) instead of SQLite
//	--no-proxy-persistence   use an in-memory proxy pool instead of SQLite
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ntakezo/rogojin/internal/capture"
	"github.com/ntakezo/rogojin/internal/scaffold"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "workflow":
		if err := runWorkflow(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "rogojin: "+err.Error())
			os.Exit(1)
		}
	case "request":
		if err := runRequest(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "rogojin: "+err.Error())
			os.Exit(1)
		}
	case "state":
		if err := runState(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "rogojin: "+err.Error())
			os.Exit(1)
		}
	case "-h", "--help", "help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "rogojin: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func runWorkflow(args []string) error {
	fs := flag.NewFlagSet("workflow", flag.ContinueOnError)
	noDurable := fs.Bool("no-durable", false, "omit the Snapshot/Restore recovery hooks")
	noOutput := fs.Bool("no-output", false, "omit the Output result hook")
	noProxy := fs.Bool("no-proxy", false, "omit per-task proxy leasing (domain-level)")
	noTaskPersist := fs.Bool("no-task-persistence", false, "run tasks in memory instead of SQLite (domain-level)")
	noProxyPersist := fs.Bool("no-proxy-persistence", false, "use an in-memory proxy pool instead of SQLite (domain-level)")

	// Pull the names out of the args so flags may appear on either side of them:
	// the flag package stops at the first non-flag, and all our flags are boolean
	// (no values to mistake a name for).
	names, flags := splitPositional(fs, args)
	if err := fs.Parse(flags); err != nil {
		return err
	}
	if len(names) != 2 {
		return fmt.Errorf("usage: rogojin workflow <domain> <name> [flags]")
	}

	opts := scaffold.NewOptions(names[0], names[1])
	opts.Durable, opts.Output = !*noDurable, !*noOutput
	opts.Proxy, opts.TaskPersist, opts.ProxyPersist = !*noProxy, !*noTaskPersist, !*noProxyPersist
	// Proxy persistence is meaningless without a proxy pool, so --no-proxy
	// normalizes it off — stated explicitly alongside it or not.
	if !opts.Proxy {
		opts.ProxyPersist = false
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	modulePath, err := scaffold.ModulePath(cwd)
	if err != nil {
		return err
	}

	// A domain's posture is set by the workflow that creates it and shapes the
	// entrypoint they all register from, so a later workflow inherits it. Saying
	// otherwise is a mistake worth stopping on rather than quietly ignoring.
	posture, exists, err := scaffold.DomainPosture(cwd, opts.DomainPackage)
	if err != nil {
		return err
	}
	if exists {
		if conflict := domainFlagConflict(fs, opts, posture); conflict != "" {
			return fmt.Errorf("domain %q already exists and %s; that choice belongs to the domain, not one workflow — drop the flag", opts.Domain, conflict)
		}
		opts.Proxy, opts.TaskPersist, opts.ProxyPersist = posture.Proxy, posture.TaskPersist, posture.ProxyPersist
		if !opts.TaskPersist {
			opts.Durable = false
		}
	}

	written, created, err := scaffold.WriteWorkflow(cwd, modulePath, opts)
	if err != nil {
		return err
	}

	if created {
		fmt.Printf("created domain %q (package %s)\n", opts.Domain, opts.DomainPackage)
	}
	fmt.Printf("scaffolded workflow %q as %s:\n", opts.ID(), opts.Package)
	for _, rel := range written {
		fmt.Printf("  %s\n", rel)
	}
	// A workflow with no states has no graph, so *Context is not yet an Instance
	// and the tree does not build until the first state lands.
	fmt.Printf("\nadd the first state with: rogojin state %s %s <name> --initial\n", opts.Domain, opts.Name)
	return nil
}

// domainFlagConflict names the domain-level flag the caller passed that the
// existing domain contradicts, or the empty string when none was.
func domainFlagConflict(fs *flag.FlagSet, want, have scaffold.Options) string {
	conflicts := map[string]struct{ passed, matches bool }{
		"no-proxy":             {want.Proxy != have.Proxy, false},
		"no-task-persistence":  {want.TaskPersist != have.TaskPersist, false},
		"no-proxy-persistence": {want.ProxyPersist != have.ProxyPersist, false},
	}
	var named string
	fs.Visit(func(f *flag.Flag) {
		if c, ok := conflicts[f.Name]; ok && c.passed && named == "" {
			named = "--" + f.Name + " contradicts it"
		}
	})
	return named
}

// runRequest adds one request file to a domain, shared by every workflow in it.
// It is additive by design: call it once per request as the domain grows.
func runRequest(args []string) error {
	fs := flag.NewFlagSet("request", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite the request if it already exists")
	entry := fs.String("entry", "", "capture entry id to generate the request from")
	har := fs.String("har", "", "HAR archive holding the entry")
	session := fs.String("session", "active", "powhttp session holding the entry")
	baseURL := fs.String("powhttp", "", "powhttp data API base URL (default $POWHTTP_BASE_URL)")

	names, flags := splitPositional(fs, args)
	if err := fs.Parse(flags); err != nil {
		return err
	}
	if len(names) != 2 {
		return fmt.Errorf("usage: rogojin request <domain> <name> [--entry <id>] [--har <file>] [--force]")
	}
	opts := scaffold.NewRequestOptions(names[0], names[1])

	// Without an entry the request is a stub; with one it is the captured
	// exchange, rendered from what the capture saw on the wire.
	if *entry != "" {
		captured, err := readEntry(*har, *baseURL, *session, *entry)
		if err != nil {
			return err
		}
		opts.Entry = &captured
	} else if *har != "" {
		return fmt.Errorf("--har names a capture to read but --entry says nothing to read from it")
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	written, overwrote, err := scaffold.WriteRequest(cwd, opts, *force)
	if err != nil {
		return err
	}

	verb := "scaffolded"
	if overwrote {
		verb = "overwrote"
	}
	fmt.Printf("%s request %s:\n  %s\n", verb, opts.Ident, written)
	fmt.Printf("\ncall it from a state with: requests.%s(ctx, client, requests.%sRequest{...})\n", opts.Ident, opts.Ident)
	return nil
}

// runState adds one state to an existing workflow and re-derives the graph
// around it. It is additive by design: call it once per state as the graph grows.
func runState(args []string) error {
	fs := flag.NewFlagSet("state", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite the state if it already exists")
	request := fs.String("request", "", "request the state calls, by the name it was generated under")
	next := fs.String("next", "", "state to advance to; omit for a terminal state")
	initial := fs.Bool("initial", false, "make this the state the graph starts at")

	names, flags := splitPositional(fs, args)
	if err := fs.Parse(flags); err != nil {
		return err
	}
	if len(names) != 3 {
		return fmt.Errorf("usage: rogojin state <domain> <workflow> <name> [--request <name>] [--next <state>] [--initial] [--force]")
	}
	opts := scaffold.NewStateOptions(names[0], names[1], names[2])
	if *request != "" {
		opts.Request = scaffold.RequestIdent(*request)
	}
	if *next != "" {
		opts.Next = scaffold.StateConst(*next)
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	if opts.ModulePath, err = scaffold.ModulePath(cwd); err != nil {
		return err
	}

	// An empty entry state leaves the graph's own to stand, which is what keeps
	// adding a state from silently moving where a run begins.
	var entry string
	if *initial {
		entry = opts.Const
	}
	written, overwrote, err := scaffold.WriteState(cwd, opts, *force, entry)
	if err != nil {
		return err
	}

	verb := "scaffolded"
	if overwrote {
		verb = "overwrote"
	}
	fmt.Printf("%s state %s:\n  %s\n", verb, opts.Name, written)
	fmt.Printf("  %s (re-derived)\n", filepath.Join(opts.DomainPackage, opts.Package, "states", "graph.go"))
	// A forward --next names a state that need not exist yet, so say so rather
	// than let the tree fail to build with no hint of which call to make next.
	if opts.Next != "" {
		fmt.Printf("\n%s advances to %s — add it with: rogojin state %s %s <name>\n", opts.Const, opts.Next, opts.Domain, opts.Workflow)
	}
	return nil
}

// readEntry pulls one captured exchange from the source that was named: the HAR
// archive at --har, or a running powhttp proxy when no archive was given.
func readEntry(harPath, baseURL, session, entry string) (capture.Entry, error) {
	if harPath != "" {
		archive, err := capture.ReadHAR(harPath)
		if err != nil {
			return capture.Entry{}, err
		}
		return archive.Entry(entry)
	}
	return capture.NewPowHTTP(baseURL).Entry(context.Background(), session, entry)
}

// splitPositional separates the non-flag tokens from the flags, so the two may
// be interleaved in any order. A flag that takes a value claims the token after
// it, which keeps that value from being mistaken for a positional argument.
func splitPositional(fs *flag.FlagSet, args []string) (positional, flags []string) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			positional = append(positional, arg)
			continue
		}
		flags = append(flags, arg)
		if takesValue(fs, arg) && i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	return positional, flags
}

// takesValue reports whether arg names a defined flag whose value is the token
// that follows it: one that is neither boolean nor already carrying an "=".
func takesValue(fs *flag.FlagSet, arg string) bool {
	name := strings.TrimLeft(arg, "-")
	if strings.Contains(name, "=") {
		return false
	}
	f := fs.Lookup(name)
	if f == nil {
		return false
	}
	boolFlag, ok := f.Value.(interface{ IsBoolFlag() bool })
	return !ok || !boolFlag.IsBoolFlag()
}

func usage() {
	fmt.Fprint(os.Stderr, `rogojin scaffolds workflows into a domain.

Usage:
  rogojin workflow <domain> <name> [flags]
  rogojin request <domain> <name> [--force]
  rogojin state <domain> <workflow> <name> [--force]

Commands:
  workflow  add a workflow to a domain, creating the domain on the first call
  request   add one request, shared by every workflow in the domain
  state     add one state to a workflow, and re-derive its graph

No command overwrites a file, so an edited request or state survives a repeated
call. Pass --force to replace one deliberately. Two files are derived and always
rewritten: the domain's registration, and each workflow's states/graph.go.

Flags (workflow) — these shape one workflow:
  --no-durable             omit the Snapshot/Restore recovery hooks
  --no-output              omit the Output result hook

Flags (workflow) — these shape the domain, and are fixed by its first workflow:
  --no-proxy               omit per-task proxy leasing
  --no-task-persistence    run tasks in memory (nil repo) instead of SQLite
  --no-proxy-persistence   use an in-memory proxy pool instead of SQLite

Flags (request):
  --entry <id>             render from a capture instead of a stub
  --force                  replace the request if it already exists

Reading the entry from a HAR archive:
  --har <file>             the archive; --entry is its _id or 1-based position

Reading the entry from a powhttp proxy:
  --session <id>           the session holding it (default "active")
  --powhttp <url>          data API address (default $POWHTTP_BASE_URL)

Flags (state):
  --request <name>         request the state calls, by its generated name
  --next <state>           state to advance to; omit for a terminal state
  --initial                make this the state the graph starts at
  --force                  replace the state if it already exists
`)
}
