// WithMaxRunning is exercised from outside the package, over the real memory
// adapter, because its subject is the sweep meeting a store full of claimable
// work — the same ground the claims tests stand on. Helpers are this file's
// own: each manager gets its own gate, so a thief's runs are counted apart
// from the zombies that seeded the store.
package tasks_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/memory"
	"github.com/ntakezo/rogojin/tasks"
	"github.com/ntakezo/rogojin/workflows"
)

// capGate observes one manager's runs of the capped workflow: entered
// announces each pass through the work state, release unblocks one, and the
// counters record how many handlers ran at once.
type capGate struct {
	entered chan string
	release chan struct{}
	active  atomic.Int32
	high    atomic.Int32
}

func newCapGate() *capGate {
	return &capGate{entered: make(chan string, 16), release: make(chan struct{})}
}

type capInstance struct {
	workflows.Base
	g *capGate
}

func (i *capInstance) Graph() workflows.Graph {
	return workflows.NewGraph("work",
		workflows.On("work", i.work),
		workflows.On("finish", i.finish),
	)
}

func (i *capInstance) work(ctx context.Context) (*workflows.State, error) {
	cur := i.g.active.Add(1)
	defer i.g.active.Add(-1)
	for {
		high := i.g.high.Load()
		if cur <= high || i.g.high.CompareAndSwap(high, cur) {
			break
		}
	}
	i.g.entered <- "work"
	select {
	case <-i.g.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return workflows.Next("finish"), nil
}

func (i *capInstance) finish(ctx context.Context) (*workflows.State, error) {
	return nil, nil
}

// capModule builds the two-state workflow every max-running test runs,
// wired to the given manager's own gate.
func capModule(g *capGate) *workflows.Module[struct{}] {
	return workflows.NewModule("capped", func(_ struct{}, deps workflows.Deps) (workflows.Instance, error) {
		return &capInstance{g: g}, nil
	})
}

// capNode builds a manager on repo under the given node id, closed with the
// test, with the capped workflow registered against g.
func capNode(t *testing.T, repo tasks.Repository, g *capGate, id string, opts ...tasks.Option) tasks.Manager {
	t.Helper()
	m, err := tasks.NewManager(context.Background(), repo, comms.NewBus(), append([]tasks.Option{tasks.WithNode(id)}, opts...)...)
	if err != nil {
		t.Fatalf("NewManager %s: %v", id, err)
	}
	t.Cleanup(func() { m.Close() })
	if err := m.RegisterWorkflow("capped", capModule(g)); err != nil {
		t.Fatalf("RegisterWorkflow %s: %v", id, err)
	}
	return m
}

// seedClaimable leaves n claimable records in the store the way a crashed
// node would: a seed manager starts n runs that checkpoint into the work
// state and block, then closes, so its claims lapse after ttl with the runs
// still hanging as zombies. Release the gate at the end of the test to let
// them die stale.
func seedClaimable(t *testing.T, repo tasks.Repository, g *capGate, n int, ttl time.Duration) {
	t.Helper()
	seed := capNode(t, repo, g, "seed", tasks.WithLeaseTTL(ttl))
	for range n {
		task, err := seed.CreateTask(context.Background(), "capped", struct{}{})
		if err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
		go task.Start(context.Background())
	}
	for range n {
		<-g.entered
	}
	if err := seed.Close(); err != nil {
		t.Fatalf("Close seed: %v", err)
	}
}

// countDone reports how many stored tasks are done.
func countDone(t *testing.T, repo tasks.Repository) int {
	t.Helper()
	recs, err := repo.RecoverAll(context.Background())
	if err != nil {
		t.Fatalf("RecoverAll: %v", err)
	}
	done := 0
	for _, r := range recs {
		if r.Status == string(workflows.StatusDone) {
			done++
		}
	}
	return done
}

// TestSweepHonorsMaxRunning proves the budget: five claimable tasks, a bound
// of two, and the sweep admits exactly two until a run finishes — then works
// through the rest without the handler concurrency ever exceeding the bound.
func TestSweepHonorsMaxRunning(t *testing.T) {
	repo := memory.NewTasks()
	gSeed := newCapGate()
	t.Cleanup(func() { close(gSeed.release) })
	seedClaimable(t, repo, gSeed, 5, 40*time.Millisecond)

	gThief := newCapGate()
	capNode(t, repo, gThief, "thief",
		tasks.WithLeaseTTL(40*time.Millisecond),
		tasks.WithRecoverySweep(25*time.Millisecond, nil),
		tasks.WithMaxRunning(2))

	<-gThief.entered
	<-gThief.entered
	// Several sweep ticks pass with both slots blocked: a third admission
	// would be the budget failing.
	select {
	case <-gThief.entered:
		t.Fatal("a third run entered under WithMaxRunning(2)")
	case <-time.After(150 * time.Millisecond):
	}

	// Each release frees a slot; the sends self-pace against the sweep
	// admitting the next run, so five of them drain the store.
	for range 5 {
		gThief.release <- struct{}{}
	}
	eventually(t, "all five tasks to finish", func() bool { return countDone(t, repo) == 5 })
	if high := gThief.high.Load(); high > 2 {
		t.Fatalf("handler concurrency reached %d, want at most 2", high)
	}
}

// TestDirectStartIgnoresMaxRunning proves the rule that the bound governs
// only the sweep: three direct Starts on a WithMaxRunning(1) node all run at
// once.
func TestDirectStartIgnoresMaxRunning(t *testing.T) {
	repo := memory.NewTasks()
	g := newCapGate()
	m := capNode(t, repo, g, "solo", tasks.WithMaxRunning(1))

	done := make(chan error, 3)
	for range 3 {
		task, err := m.CreateTask(context.Background(), "capped", struct{}{})
		if err != nil {
			t.Fatalf("CreateTask: %v", err)
		}
		go func() { _, err := task.Start(context.Background()); done <- err }()
	}
	for range 3 {
		<-g.entered
	}
	if high := g.high.Load(); high != 3 {
		t.Fatalf("handler concurrency = %d, want all 3 direct runs at once", high)
	}
	close(g.release)
	for range 3 {
		if err := <-done; err != nil {
			t.Fatalf("direct Start: %v", err)
		}
	}
}

// TestDirectRunsCountAgainstSweepBudget proves the other half of that rule:
// a direct run occupies capacity, so a WithMaxRunning(1) node's sweep leaves
// claimable work in the store until the direct run finishes.
func TestDirectRunsCountAgainstSweepBudget(t *testing.T) {
	repo := memory.NewTasks()
	gSeed := newCapGate()
	t.Cleanup(func() { close(gSeed.release) })
	// A long seed TTL keeps the claimable task out of reach until well after
	// the direct run below has taken the budget.
	seedClaimable(t, repo, gSeed, 1, 100*time.Millisecond)

	g := newCapGate()
	m := capNode(t, repo, g, "busy",
		tasks.WithLeaseTTL(40*time.Millisecond),
		tasks.WithRecoverySweep(25*time.Millisecond, nil),
		tasks.WithMaxRunning(1))

	task, err := m.CreateTask(context.Background(), "capped", struct{}{})
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	direct := make(chan error, 1)
	go func() { _, err := task.Start(context.Background()); direct <- err }()
	<-g.entered

	// The seed's claim lapses inside this window; the sweep must still not
	// start the claimable task while the direct run holds the budget.
	select {
	case <-g.entered:
		t.Fatal("the sweep started a task while a direct run held the budget")
	case <-time.After(250 * time.Millisecond):
	}

	g.release <- struct{}{}
	if err := <-direct; err != nil {
		t.Fatalf("direct Start: %v", err)
	}
	<-g.entered // the swept task, admitted once the direct run freed capacity
	g.release <- struct{}{}
	eventually(t, "both tasks to finish", func() bool { return countDone(t, repo) == 2 })
}
