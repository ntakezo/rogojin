package proxies

import "testing"

// TestRoundRobinCycles verifies successive selections rotate through the
// candidates in stable order, because spreading load evenly is the strategy's intent.
func TestRoundRobinCycles(t *testing.T) {
	rr := NewRoundRobin()
	pool := []Proxy{{ID: "a"}, {ID: "b"}, {ID: "c"}}

	want := []string{"a", "b", "c", "a", "b", "c"}
	for i, w := range want {
		p, err := rr.Select(pool)
		if err != nil {
			t.Fatalf("select %d: %v", i, err)
		}
		if p.ID != w {
			t.Fatalf("select %d = %s, want %s", i, p.ID, w)
		}
	}
}

// TestRoundRobinNoCandidates verifies an empty candidate set errors instead of
// panicking, because the manager treats a selection error as fatal to the acquire.
func TestRoundRobinNoCandidates(t *testing.T) {
	if _, err := NewRoundRobin().Select(nil); err == nil {
		t.Fatal("expected error for no candidates")
	}
}
