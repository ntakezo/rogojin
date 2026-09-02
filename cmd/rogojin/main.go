// Command rogojin scaffolds a runnable workflow package to build off of.
//
// Usage:
//
//	rogojin new <name> [flags]
//
// Run it from inside the Go module that will own the workflow; generated imports
// resolve against that module's path. Flags subtract features from the default
// full scaffold, and one flag picks the repository:
//
//	--no-durable    omit the Snapshot/Restore recovery hooks
//	--no-proxy      omit per-task proxy leasing
//	--no-accounts   omit per-task site-account locking
//	--no-payments   omit per-task payment-instrument leasing
//	--no-email      omit inbox listening
//	--repo          sqlite (default) persists to a file; postgres persists to
//	                a server, which is what several processes share; memory
//	                runs the managers on nil repositories, nothing surviving
//	                the process
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
	noProxy := fs.Bool("no-proxy", false, "omit per-task proxy leasing")
	noAccounts := fs.Bool("no-accounts", false, "omit per-task site-account locking")
	noPayments := fs.Bool("no-payments", false, "omit per-task payment-instrument leasing")
	noEmail := fs.Bool("no-email", false, "omit inbox listening")
	repo := fs.String("repo", "sqlite", "repository behind the managers: sqlite, postgres, or memory")

	// Pull the workflow name out of the args so flags may appear on either side
	// of it: the flag package stops at the first non-flag, and splitName knows
	// which flags consume the token after them.
	name, flags := splitName(args)
	if err := fs.Parse(flags); err != nil {
		return err
	}
	if name == "" || fs.NArg() != 0 {
		return fmt.Errorf("usage: rogojin new <name> [flags]")
	}

	opts := scaffold.Options{
		Name:     name,
		Package:  scaffold.PackageName(name),
		Durable:  !*noDurable,
		Proxy:    !*noProxy,
		Accounts: !*noAccounts,
		Payments: !*noPayments,
		Email:    !*noEmail,
		Repo:     *repo,
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
// other token as flags, so the name may sit before or after the flags. The
// boolean flags never consume a value, and the one value flag, --repo, only
// swallows the next token in its separated form (--repo=memory is
// self-contained) — so the first remaining non-flag token is the name.
func splitName(args []string) (name string, flags []string) {
	for i := 0; i < len(args); i++ {
		a := args[i]
		if name == "" && !strings.HasPrefix(a, "-") {
			name = a
			continue
		}
		flags = append(flags, a)
		if trimmed := strings.TrimLeft(a, "-"); trimmed == "repo" && i+1 < len(args) {
			i++
			flags = append(flags, args[i])
		}
	}
	return name, flags
}

func usage() {
	fmt.Fprint(os.Stderr, `rogojin scaffolds a runnable workflow package.

Usage:
  rogojin new <name> [flags]

Flags:
  --no-durable    omit the Snapshot/Restore recovery hooks
  --no-proxy      omit per-task proxy leasing
  --no-accounts   omit per-task site-account locking
  --no-payments   omit per-task payment-instrument leasing
  --no-email      omit inbox listening
  --repo          sqlite (default) persists to a file; postgres persists to
                  a server, which is what several processes share; memory
                  runs the managers on nil repositories, nothing surviving
                  the process
`)
}
