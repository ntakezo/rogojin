package scaffold

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
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
	durableInMemory := Options{Name: "x", Package: "x", Durable: true, Persist: false}
	if err := durableInMemory.Validate(); err == nil {
		t.Error("durable + memory repo should be rejected: snapshots would never be written")
	}

	// Email and accounts are independent features: an inbox can be listened to
	// without routing through an account's forwarding reference, and accounts
	// need no inbox at all.
	emailWithoutAccounts := Options{Name: "x", Package: "x", Email: true, Accounts: false, Persist: true}
	if err := emailWithoutAccounts.Validate(); err != nil {
		t.Errorf("email without accounts should be valid, got %v", err)
	}
	accountsWithoutEmail := Options{Name: "x", Package: "x", Accounts: true, Email: false, Persist: true}
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
		o := Options{Name: name, Package: PackageName(name), Persist: true}
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
						for _, persist := range bools {
							o := Options{
								Name:     "sample",
								Package:  "sample",
								Durable:  durable,
								Proxy:    proxy,
								Accounts: accounts,
								Payments: payments,
								Email:    mail,
								Persist:  persist,
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

	for _, o := range validCombos() {
		t.Run(comboName(o), func(t *testing.T) {
			dir := t.TempDir()

			if _, err := Write(dir, "example.com/consumer", o); err != nil {
				t.Fatalf("Write: %v", err)
			}

			gomod := fmt.Sprintf(`module example.com/consumer

go 1.25.0

require github.com/ntakezo/rogojin v0.0.0

replace github.com/ntakezo/rogojin => %s
`, repoRoot)
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

func comboName(o Options) string {
	return fmt.Sprintf("durable=%t_proxy=%t_accounts=%t_payments=%t_email=%t_persist=%t",
		o.Durable, o.Proxy, o.Accounts, o.Payments, o.Email, o.Persist)
}
