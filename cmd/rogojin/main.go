// Command rogojin scaffolds a runnable workflow package to build off of.
//
// Usage:
//
//	rogojin new <name> [flags]
//	rogojin request <workflow> <name>
//
// Run it from inside the Go module that will own the workflow; generated imports
// resolve against that module's path. "new" writes the workflow once; "request"
// adds a single request function to an existing one and is meant to be called
// again for every request the workflow grows. Neither overwrites a file unless
// "request" is passed --force, which replaces one request and nothing else.
//
// Flags subtract features from the default full scaffold:
//
//	--no-durable             omit the Snapshot/Restore recovery hooks
//	--no-output              omit the Output result hook
//	--no-proxy               omit per-task proxy leasing
//	--no-task-persistence    run tasks in memory (nil repo) instead of SQLite
//	--no-proxy-persistence   use an in-memory proxy pool instead of SQLite
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
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
	case "new":
		if err := runNew(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "rogojin: "+err.Error())
			os.Exit(1)
		}
	case "request":
		if err := runRequest(os.Args[2:]); err != nil {
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

func runNew(args []string) error {
	fs := flag.NewFlagSet("new", flag.ContinueOnError)
	noDurable := fs.Bool("no-durable", false, "omit the Snapshot/Restore recovery hooks")
	noOutput := fs.Bool("no-output", false, "omit the Output result hook")
	noProxy := fs.Bool("no-proxy", false, "omit per-task proxy leasing")
	noTaskPersist := fs.Bool("no-task-persistence", false, "run tasks in memory instead of SQLite")
	noProxyPersist := fs.Bool("no-proxy-persistence", false, "use an in-memory proxy pool instead of SQLite")

	// Pull the workflow name out of the args so flags may appear on either side of
	// it: the flag package stops at the first non-flag, and all our flags are
	// boolean (no values to mistake the name for).
	names, flags := splitPositional(fs, args)
	if err := fs.Parse(flags); err != nil {
		return err
	}
	if len(names) != 1 {
		return fmt.Errorf("usage: rogojin new <name> [flags]")
	}
	name := names[0]

	opts := scaffold.Options{
		Name:         name,
		Package:      scaffold.PackageName(name),
		Durable:      !*noDurable,
		Output:       !*noOutput,
		Proxy:        !*noProxy,
		TaskPersist:  !*noTaskPersist,
		ProxyPersist: !*noProxyPersist,
	}
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

	written, err := scaffold.Write(cwd, modulePath, opts)
	if err != nil {
		return err
	}

	fmt.Printf("scaffolded workflow %q (package %s):\n", opts.Name, opts.Package)
	for _, rel := range written {
		fmt.Printf("  %s\n", rel)
	}
	// The generated requests package imports fhttp, so a module that has not
	// used it before needs a tidy before the tree builds.
	fmt.Printf("\nrun it with: go mod tidy && go run ./%s/cmd/run\n", opts.Package)
	return nil
}

// runRequest adds one request file to an existing workflow. It is additive by
// design: call it once per request as the workflow grows.
func runRequest(args []string) error {
	fs := flag.NewFlagSet("request", flag.ContinueOnError)
	force := fs.Bool("force", false, "overwrite the request if it already exists")
	entry := fs.String("entry", "", "powhttp entry id to generate the request from")
	session := fs.String("session", "active", "powhttp session holding the entry")
	baseURL := fs.String("powhttp", "", "powhttp data API base URL (default $POWHTTP_BASE_URL)")

	names, flags := splitPositional(fs, args)
	if err := fs.Parse(flags); err != nil {
		return err
	}
	if len(names) != 2 {
		return fmt.Errorf("usage: rogojin request <workflow> <name> [--entry <id>] [--force]")
	}
	opts := scaffold.NewRequestOptions(names[0], names[1])

	// Without an entry the request is a stub; with one it is the captured
	// exchange, rendered from what powhttp saw on the wire.
	if *entry != "" {
		captured, err := capture.NewPowHTTP(*baseURL).Entry(context.Background(), *session, *entry)
		if err != nil {
			return err
		}
		opts.Entry = &captured
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
	fmt.Fprint(os.Stderr, `rogojin scaffolds a runnable workflow package.

Usage:
  rogojin new <name> [flags]
  rogojin request <workflow> <name> [--force]

Commands:
  new       scaffold a workflow package, with one request to build off of
  request   add one request to an existing workflow; call it once per request

Neither command overwrites a file, so an edited request survives a repeated
call. Pass --force to replace one deliberately.

Flags (new):
  --no-durable             omit the Snapshot/Restore recovery hooks
  --no-output              omit the Output result hook
  --no-proxy               omit per-task proxy leasing
  --no-task-persistence    run tasks in memory (nil repo) instead of SQLite
  --no-proxy-persistence   use an in-memory proxy pool instead of SQLite

Flags (request):
  --entry <id>             render from a powhttp capture instead of a stub
  --session <id>           powhttp session holding the entry (default "active")
  --powhttp <url>          powhttp data API address (default $POWHTTP_BASE_URL)
  --force                  replace the request if it already exists
`)
}
