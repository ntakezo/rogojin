package scaffold

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// TestPackageName pins the identifier derivation: lowercased, non-ident
// characters dropped, never leading with a digit.
func TestPackageName(t *testing.T) {
	cases := map[string]string{
		"checkout":      "checkout",
		"Checkout":      "checkout",
		"checkout-flow": "checkoutflow",
		"my_workflow":   "my_workflow",
		"2fast":         "fast",
		"123":           "",
		"":              "",
	}
	for in, want := range cases {
		if got := PackageName(in); got != want {
			t.Errorf("PackageName(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestValidateRejectsIncoherentCombos guards the one combination that would
// otherwise generate code that lies about what it does — and pins that the
// combos deliberately kept independent stay valid.
func TestValidateRejectsIncoherentCombos(t *testing.T) {
	durableInMemory := Options{Name: "x", Package: "x", Durable: true, Repo: RepoMemory}
	if err := durableInMemory.Validate(); err == nil {
		t.Error("durable + memory repo should be rejected: snapshots would never be written")
	}

	// Email and accounts are independent features: an inbox can be listened to
	// without routing through an account's forwarding reference, and accounts
	// need no inbox at all.
	emailWithoutAccounts := Options{Name: "x", Package: "x", Email: true, Accounts: false, Repo: RepoSQLite}
	if err := emailWithoutAccounts.Validate(); err != nil {
		t.Errorf("email without accounts should be valid, got %v", err)
	}
	accountsWithoutEmail := Options{Name: "x", Package: "x", Accounts: true, Email: false, Repo: RepoSQLite}

	unknownRepo := Options{Name: "x", Package: "x", Repo: "mongodb"}
	if err := unknownRepo.Validate(); err == nil {
		t.Error("an unknown repo name should be rejected")
	}
	if err := accountsWithoutEmail.Validate(); err != nil {
		t.Errorf("accounts without email should be valid, got %v", err)
	}
}

// TestValidateRejectsUnusablePackageNames guards names whose derived package
// identifier cannot actually be used: Go keywords fail compilation and "main"
// cannot be imported. Without the guard these die later as a cryptic format
// error over the full rendered source.
func TestValidateRejectsUnusablePackageNames(t *testing.T) {
	for _, name := range []string{"type", "select", "func", "main", "Go!"} {
		o := Options{Name: name, Package: PackageName(name), Repo: RepoSQLite}
		if err := o.Validate(); err == nil {
			t.Errorf("Validate accepted name %q (package %q), want rejection", name, o.Package)
		}
	}
}

// validCombos enumerates every flag combination that survives Validate, so the
// compile test covers the whole feature matrix.
func validCombos() []Options {
	bools := []bool{true, false}
	var out []Options
	for _, durable := range bools {
		for _, proxy := range bools {
			for _, accounts := range bools {
				for _, payments := range bools {
					for _, mail := range bools {
						for _, repo := range []string{RepoSQLite, RepoPostgres, RepoMemory} {
							o := Options{
								Name:     "sample",
								Package:  "sample",
								Durable:  durable,
								Proxy:    proxy,
								Accounts: accounts,
								Payments: payments,
								Email:    mail,
								Repo:     repo,
							}
							if o.Validate() != nil {
								continue
							}
							out = append(out, o)
						}
					}
				}
			}
		}
	}
	return out
}

// TestRenderProducesValidGo renders every valid combo and asserts it formats —
// format.Source inside Render rejects syntactically invalid output, so a passing
// render is a real (fast) syntax guarantee across the matrix.
func TestRenderProducesValidGo(t *testing.T) {
	for _, o := range validCombos() {
		t.Run(comboName(o), func(t *testing.T) {
			files, err := Render("example.com/consumer", o)
			if err != nil {
				t.Fatalf("Render: %v", err)
			}
			if len(files) == 0 {
				t.Fatal("Render produced no files")
			}
		})
	}
}

// TestGeneratedCodeCompiles is the contract: every valid combo must produce a
// tree that actually type-checks against the real rogojin packages. It writes
// each scaffold into a throwaway module that replaces rogojin with this checkout,
// then runs `go vet ./...` — which compiles every package (failing on any compile
// error) without linking binaries, so it stays light on disk. Skipped under
// -short because it shells out to the toolchain (and needs cgo for the SQLite
// adapters).
func TestGeneratedCodeCompiles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping compile test in short mode")
	}
	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	goSum, err := os.ReadFile(filepath.Join(repoRoot, "go.sum"))
	if err != nil {
		t.Fatalf("read repo go.sum: %v", err)
	}
	rootMod, err := os.ReadFile(filepath.Join(repoRoot, "go.mod"))
	if err != nil {
		t.Fatalf("read repo go.mod: %v", err)
	}
	// The throwaway module restates every requirement of the root module at
	// the root's selected versions. With only the rogojin require, resolving
	// the graph would traverse into each dependency's own go.mod and meet
	// versions the root build never downloads — absent from a cold module
	// cache, and unfetchable under GOPROXY=off.
	requires := requireLines(t, string(rootMod))

	for _, o := range validCombos() {
		t.Run(comboName(o), func(t *testing.T) {
			dir := t.TempDir()

			if _, err := Write(dir, "example.com/consumer", o); err != nil {
				t.Fatalf("Write: %v", err)
			}

			gomod := fmt.Sprintf(`module example.com/consumer

go 1.25.0

require github.com/ntakezo/rogojin v0.0.0

require (
%s)

replace github.com/ntakezo/rogojin => %s
`, requires, repoRoot)
			if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "go.sum"), goSum, 0o644); err != nil {
				t.Fatal(err)
			}

			cmd := exec.Command("go", "vet", "./...")
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "GOFLAGS=-mod=mod", "GOPROXY=off")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("go vet failed for combo %s: %v\n%s", comboName(o), err, out)
			}
		})
	}
}

// requireLines extracts every "path version" requirement from a go.mod, block
// and single-line forms alike, as lines ready for a require block.
func requireLines(t *testing.T, gomod string) string {
	t.Helper()
	var out strings.Builder
	inBlock := false
	for _, line := range strings.Split(gomod, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case trimmed == "require (":
			inBlock = true
		case inBlock && trimmed == ")":
			inBlock = false
		case inBlock && trimmed != "":
			fmt.Fprintf(&out, "\t%s\n", trimmed)
		case strings.HasPrefix(trimmed, "require "):
			fmt.Fprintf(&out, "\t%s\n", strings.TrimPrefix(trimmed, "require "))
		}
	}
	if out.Len() == 0 {
		t.Fatal("no requirements found in the root go.mod")
	}
	return out.String()
}

func comboName(o Options) string {
	return fmt.Sprintf("durable=%t_proxy=%t_accounts=%t_payments=%t_email=%t_repo=%s",
		o.Durable, o.Proxy, o.Accounts, o.Payments, o.Email, o.Repo)
}
