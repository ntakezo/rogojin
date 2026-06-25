// Command rogojin scaffolds a runnable workflow package to build off of.
//
// Usage:
//
//	rogojin new <name> [flags]
//
// Run it from inside the Go module that will own the workflow; generated imports
// resolve against that module's path. Flags subtract features from the default
// full scaffold:
//
//	--no-durable             omit the Snapshot/Restore recovery hooks
//	--no-output              omit the Output result hook
//	--no-proxy               omit per-task proxy leasing
//	--no-task-persistence    run tasks in memory (nil repo) instead of SQLite
//	--no-proxy-persistence   use an in-memory proxy pool instead of SQLite
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

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
	name, flags := splitName(args)
	if err := fs.Parse(flags); err != nil {
		return err
	}
	if name == "" || fs.NArg() != 0 {
		return fmt.Errorf("usage: rogojin new <name> [flags]")
	}

	if *noProxy && *noProxyPersist {
		return fmt.Errorf("--no-proxy-persistence is meaningless with --no-proxy: there is no proxy pool to store")
	}

	opts := scaffold.Options{
		Name:         name,
		Package:      scaffold.PackageName(name),
		Durable:      !*noDurable,
		Output:       !*noOutput,
		Proxy:        !*noProxy,
		TaskPersist:  !*noTaskPersist,
		ProxyPersist: !*noProxyPersist,
	}
	// ProxyPersist is only meaningful with a proxy pool. When --no-proxy is set
	// without an explicit --no-proxy-persistence, normalize it off rather than
	// reject; an explicit pairing of the two is still a conflict Validate flags.
	if !opts.Proxy && !*noProxyPersist {
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
	fmt.Printf("\nrun it with: go run ./%s/cmd/run\n", opts.Package)
	return nil
}

// splitName returns the first non-flag token as the workflow name and every
// other token as flags, so the name may sit before or after the flags. It is
// safe because all of new's flags are boolean and never consume a value.
func splitName(args []string) (name string, flags []string) {
	for _, a := range args {
		if name == "" && !strings.HasPrefix(a, "-") {
			name = a
			continue
		}
		flags = append(flags, a)
	}
	return name, flags
}

func usage() {
	fmt.Fprint(os.Stderr, `rogojin scaffolds a runnable workflow package.

Usage:
  rogojin new <name> [flags]

Flags:
  --no-durable             omit the Snapshot/Restore recovery hooks
  --no-output              omit the Output result hook
  --no-proxy               omit per-task proxy leasing
  --no-task-persistence    run tasks in memory (nil repo) instead of SQLite
  --no-proxy-persistence   use an in-memory proxy pool instead of SQLite
`)
}
