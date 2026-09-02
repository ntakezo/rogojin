// Claims are exercised here from outside the package, over the real memory
// adapter, because their whole subject is two managers meeting in one store —
// the in-package fakes deliberately have no lease semantics to meet in.
package tasks_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/memory"
	"github.com/ntakezo/rogojin/tasks"
	"github.com/ntakezo/rogojin/workflows"
)

// gate coordinates a gatedModule's runs across the managers sharing it:
// entered announces each pass through the work state, release unblocks the
// blocked ones, and blockFirst traps only the first run in — the crash dummy
// — letting a stealing node's run straight through.
type gate struct {
	entered    chan string
	release    chan struct{}
	blockFirst bool
	entries    atomic.Int32
}

func newGate(blockFirst bool) *gate {
	return &gate{entered: make(chan string, 16), release: make(chan struct{}), blockFirst: blockFirst}
}

type gatedInstance struct {
	workflows.Base
	g *gate
}

func (i *gatedInstance) Graph() workflows.Graph {
	return workflows.NewGraph("work",
		workflows.On("work", i.work),
		workflows.On("finish", i.finish),
	)
}

func (i *gatedInstance) work(ctx context.Context) (*workflows.State, error) {
	n := i.g.entries.Add(1)
	i.g.entered <- "work"
	if !i.g.blockFirst || n == 1 {
		select {
		case <-i.g.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return workflows.Next("finish"), nil
}

func (i *gatedInstance) finish(ctx context.Context) (*workflows.State, error) {
	return nil, nil
}

// gatedModule builds the shared two-state workflow every claims test runs.
func gatedModule(g *gate) *workflows.Module[struct{}] {
	return workflows.NewModule("gated", func(_ struct{}, deps workflows.Deps) (workflows.Instance, error) {
		return &gatedInstance{g: g}, nil
	})
}

// node builds a manager on repo under the given node id, closed with the test.
func node(t *testing.T, repo tasks.Repository, g *gate, id string, opts ...tasks.Option) tasks.Manager {
	t.Helper()
	m, err := tasks.NewManager(context.Background(), repo, comms.NewBus(), append([]tasks.Option{tasks.WithNode(id)}, opts...)...)
	if err != nil {
		t.Fatalf("NewManager %s: %v", id, err)
	}
	t.Cleanup(func() { m.Close() })
	if err := m.RegisterWorkflow("gated", gatedModule(g)); err != nil {
		t.Fatalf("RegisterWorkflow %s: %v", id, err)
	}
	return m
}

// eventually polls cond until it holds or the deadline passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// suspendAndDetach parks a freshly started task at its suspend boundary and
// detaches it, leaving a suspended, unclaimed record behind — the shape a
// task hands between nodes cleanly.
func suspendAndDetach(t *testing.T, g *gate, task *tasks.Task) {
	t.Helper()
	done := make(chan error, 1)
	go func() { _, err := task.Start(context.Background()); done <- err }()
	<-g.entered
	if err := task.Suspend(); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	g.release <- struct{}{}
	// The run parks only after persisting the suspended checkpoint, and
	// Detach refuses until it is parked there.
	eventually(t, "a detachable park", func() bool { return task.Detach() == nil })
	if err := <-done; !errors.Is(err, tasks.ErrDetached) {
		t.Fatalf("detached Start: err = %v, want ErrDetached", err)
	}
}

// TestClaimAdmitsExactlyOneNode has two nodes race Start on the same
// detached task: the store admits one, the other refuses with ErrClaimHeld.
func TestClaimAdmitsExactlyOneNode(t *testing.T) {
	repo := memory.NewTasks()
	g := newGate(false)
	seeder := node(t, repo, g, "seed")
	task, err := seeder.CreateTask(context.Background(), "gated", nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	suspendAndDetach(t, g, task)
	close(g.release) // every later run passes straight through

	a, b := node(t, repo, g, "node-a"), node(t, repo, g, "node-b")
	handles := make([]*tasks.Task, 2)
	for i, m := range []tasks.Manager{a, b} {
		recovered, err := m.RecoverClaimable(context.Background())
		if err != nil || len(recovered) != 1 {
			t.Fatalf("RecoverClaimable: %v, %d tasks", err, len(recovered))
		}
		handles[i] = recovered[0]
	}

	results := make(chan error, 2)
	for _, h := range handles {
		go func(h *tasks.Task) { _, err := h.Start(context.Background()); results <- err }(h)
	}

	var wins, losses int
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			wins++
		case errors.Is(err, tasks.ErrClaimHeld):
			losses++
		default:
			// The winner resumes parked (the checkpoint was suspended), so a
			// nil can only arrive after a Resume — issue one for whichever
			// handle is live and read the second result.
			t.Fatalf("Start: unexpected error %v", err)
		}
		if wins+losses == 1 {
			// The winner resumes parked at the suspended checkpoint; wait for
			// the park before resuming, since Resume before it is a no-op.
			eventually(t, "the winner to park", func() bool {
				for _, h := range handles {
					if h.IsRunning() && h.LiveStatus() == workflows.StatusSuspended {
						h.Resume()
						return true
					}
				}
				return false
			})
		}
	}
	if wins != 1 || losses != 1 {
		t.Fatalf("wins = %d, losses = %d; want exactly one of each", wins, losses)
	}
}

// TestHeartbeatKeepsTheLeaseAlive parks a run in a handler for several lease
// lifetimes and has a rival claim throughout: the heartbeat must keep the
// rival losing the entire time.
func TestHeartbeatKeepsTheLeaseAlive(t *testing.T) {
	repo := memory.NewTasks()
	g := newGate(false)
	m := node(t, repo, g, "node-a", tasks.WithLeaseTTL(60*time.Millisecond))
	task, err := m.CreateTask(context.Background(), "gated", nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	done := make(chan error, 1)
	go func() { _, err := task.Start(context.Background()); done <- err }()
	<-g.entered

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, err := repo.ClaimTask(context.Background(), task.ID, "rival", time.Minute); !errors.Is(err, tasks.ErrClaimHeld) {
			t.Fatalf("rival claim: err = %v, want ErrClaimHeld while the owner heartbeats", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	close(g.release)
	if err := <-done; err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// TestUsurpedRunStopsWithoutStamping closes the owner's manager so its claims
// stop renewing, lets a rival take the task, and then releases the run: its
// next checkpoint must lose with ErrStale, kill the run, and leave the
// rival's record unstamped.
func TestUsurpedRunStopsWithoutStamping(t *testing.T) {
	repo := memory.NewTasks()
	g := newGate(false)
	m := node(t, repo, g, "node-a", tasks.WithLeaseTTL(40*time.Millisecond))
	task, err := m.CreateTask(context.Background(), "gated", nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	done := make(chan error, 1)
	go func() { _, err := task.Start(context.Background()); done <- err }()
	<-g.entered

	// "Crash" the node: the run keeps executing but nothing renews its claim.
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	eventually(t, "the lease to lapse for the rival", func() bool {
		_, err := repo.ClaimTask(context.Background(), task.ID, "rival", time.Minute)
		return err == nil
	})

	close(g.release)
	err = <-done
	if !errors.Is(err, tasks.ErrStale) {
		t.Fatalf("usurped Start: err = %v, want ErrStale", err)
	}
	rec, err := repo.RecoverTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if workflows.Status(rec.Status).Terminal() {
		t.Fatalf("record stamped %q by a usurped run; the rival's outcome was not its to write", rec.Status)
	}
	if rec.OwnerNode != "rival" {
		t.Fatalf("owner = %q, want the rival's claim intact", rec.OwnerNode)
	}
}

// TestDetachReleasesTheClaim verifies a detached run frees the claim
// immediately — the handoff case — rather than leaving the task locked out
// until the lease expires.
func TestDetachReleasesTheClaim(t *testing.T) {
	repo := memory.NewTasks()
	g := newGate(false)
	m := node(t, repo, g, "node-a") // default TTL: expiry alone cannot pass this test
	task, err := m.CreateTask(context.Background(), "gated", nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	suspendAndDetach(t, g, task)

	if _, err := repo.ClaimTask(context.Background(), task.ID, "node-b", time.Minute); err != nil {
		t.Fatalf("claim after detach: %v, want immediate success", err)
	}
}

// TestCloseLeavesRunsAlive verifies Close stops the background work and
// nothing else: a run in flight keeps executing and completes cleanly.
func TestCloseLeavesRunsAlive(t *testing.T) {
	repo := memory.NewTasks()
	g := newGate(false)
	m := node(t, repo, g, "node-a")
	task, err := m.CreateTask(context.Background(), "gated", nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	done := make(chan error, 1)
	go func() { _, err := task.Start(context.Background()); done <- err }()
	<-g.entered

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !task.IsRunning() {
		t.Fatal("Close killed a running task")
	}
	close(g.release)
	if err := <-done; err != nil {
		t.Fatalf("Start after Close: %v", err)
	}
}

// TestSweepStealsOrphanedWork crashes one node mid-run and has a second,
// sweeping node pick the task up after the lease lapses and finish it — the
// work-stealing loop end to end. The crashed node's zombie run is then
// released and must die stale without disturbing the thief's outcome.
func TestSweepStealsOrphanedWork(t *testing.T) {
	repo := memory.NewTasks()
	g := newGate(true) // trap only the first run: the crash dummy
	a := node(t, repo, g, "node-a", tasks.WithLeaseTTL(40*time.Millisecond))
	task, err := a.CreateTask(context.Background(), "gated", nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	done := make(chan error, 1)
	go func() { _, err := task.Start(context.Background()); done <- err }()
	<-g.entered
	if err := a.Close(); err != nil { // the "crash": renewal stops, the run hangs
		t.Fatalf("Close: %v", err)
	}

	var sweepFailures atomic.Int32
	node(t, repo, g, "node-b",
		tasks.WithLeaseTTL(40*time.Millisecond),
		tasks.WithRecoverySweep(25*time.Millisecond, func(taskID string, err error) {
			sweepFailures.Add(1)
			t.Logf("sweep %s: %v", taskID, err)
		}))

	eventually(t, "the thief to finish the task", func() bool {
		rec, err := repo.RecoverTask(context.Background(), task.ID)
		return err == nil && rec.Status == string(workflows.StatusDone)
	})

	// Release the zombie: its next write must lose to the thief's claim or
	// terminal stamp, never overwrite it.
	close(g.release)
	if err := <-done; err == nil {
		t.Fatal("zombie run completed cleanly; want it to lose its writes")
	}
	rec, err := repo.RecoverTask(context.Background(), task.ID)
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	if rec.Status != string(workflows.StatusDone) {
		t.Fatalf("status = %q, want the thief's done to stand", rec.Status)
	}
	if n := sweepFailures.Load(); n != 0 {
		t.Fatalf("sweep reported %d failures; the steal should be quiet", n)
	}
}
