package proxies

import "testing"

// TestBayesianPrefersProvenProxy verifies Thompson sampling overwhelmingly
// picks the proxy with the dominant success history, because exploiting
// known-good proxies is the point of the strategy.
func TestBayesianPrefersProvenProxy(t *testing.T) {
	b := NewBayesian(WithSeed(42))
	good := Proxy{ID: "good", Successes: 100, Failures: 1}
	bad := Proxy{ID: "bad", Successes: 1, Failures: 100}

	counts := map[string]int{}
	for i := range 1000 {
		p, err := b.Select([]Proxy{good, bad})
		if err != nil {
			t.Fatalf("select %d: %v", i, err)
		}
		counts[p.ID]++
	}
	if counts["good"] < 950 {
		t.Fatalf("good selected %d/1000, want >= 950", counts["good"])
	}
}

// TestBayesianExploresUnproven verifies candidates with no history are not
// starved, because exploration is what discovers proxy quality in the first place.
func TestBayesianExploresUnproven(t *testing.T) {
	b := NewBayesian(WithSeed(42))
	a := Proxy{ID: "a"}
	c := Proxy{ID: "c"}

	counts := map[string]int{}
	for i := range 1000 {
		p, err := b.Select([]Proxy{a, c})
		if err != nil {
			t.Fatalf("select %d: %v", i, err)
		}
		counts[p.ID]++
	}
	if counts["a"] < 100 || counts["c"] < 100 {
		t.Fatalf("selection starved a candidate: %v", counts)
	}
}

// TestBayesianNoCandidates verifies an empty candidate set errors instead of
// panicking, because the manager treats a selection error as fatal to the acquire.
func TestBayesianNoCandidates(t *testing.T) {
	if _, err := NewBayesian(WithSeed(42)).Select(nil); err == nil {
		t.Fatal("expected error for no candidates")
	}
}
