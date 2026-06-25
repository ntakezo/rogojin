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

// TestValidateRejectsIncoherentCombos guards the two combinations that would
// otherwise generate code that lies about what it does. These are the whole
// reason Validate exists, so they must fail loudly rather than render.
func TestValidateRejectsIncoherentCombos(t *testing.T) {
	durableWithoutTaskPersist := Options{Name: "x", Package: "x", Durable: true, TaskPersist: false}
	if err := durableWithoutTaskPersist.Validate(); err == nil {
		t.Error("durable + no task persistence should be rejected: snapshots would never be written")
	}

	proxyPersistWithoutProxy := Options{Name: "x", Package: "x", Proxy: false, ProxyPersist: true}
	if err := proxyPersistWithoutProxy.Validate(); err == nil {
		t.Error("proxy persistence without a proxy pool should be rejected")
	}
}

// validCombos enumerates every flag combination that survives normalization and
// Validate, so the compile test covers the whole feature matrix.
func validCombos() []Options {
	var out []Options
	for _, durable := range []bool{true, false} {
		for _, output := range []bool{true, false} {
			for _, proxy := range []bool{true, false} {
				for _, taskPersist := range []bool{true, false} {
					for _, proxyPersist := range []bool{true, false} {
						o := Options{
							Name:         "sample",
							Package:      "sample",
							Durable:      durable,
							Output:       output,
							Proxy:        proxy,
							TaskPersist:  taskPersist,
							ProxyPersist: proxyPersist,
						}
						if !o.Proxy {
							o.ProxyPersist = false // mirror CLI normalization
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
	return dedupe(out)
}

func dedupe(in []Options) []Options {
	seen := make(map[Options]bool)
	var out []Options
	for _, o := range in {
		if seen[o] {
			continue
		}
		seen[o] = true
		out = append(out, o)
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
	return fmt.Sprintf("durable=%t_output=%t_proxy=%t_taskpersist=%t_proxypersist=%t",
		o.Durable, o.Output, o.Proxy, o.TaskPersist, o.ProxyPersist)
}
