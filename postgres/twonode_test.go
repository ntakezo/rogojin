// The two-node tests are the package's reason to exist, run end to end: real
// managers from the tasks and leasing packages, two per test, meeting in one
// postgres database the way two processes on two machines would.
package postgres

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/payments"
	"github.com/ntakezo/rogojin/tasks"
	"github.com/ntakezo/rogojin/workflows"
)

// gate coordinates a workflow's runs across the managers sharing it: entered
// announces each pass through the work state, release unblocks the blocked
// ones, and blockFirst traps only the first run in — the crash dummy —
// letting a stealing node's run straight through.
type gate struct {
	entered    chan string
	release    chan struct{}
	blockFirst bool
	entries    atomic.Int32
	effects    atomic.Int32
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

// work records one effect under a stable key, so a stolen run re-entering the
// state proves at-most-once through the counter, then blocks per the gate.
func (i *gatedInstance) work(ctx context.Context) (*workflows.State, error) {
	if err := i.Once(ctx, "count-once", func(context.Context) error {
		i.g.effects.Add(1)
		return nil
	}); err != nil {
		return nil, err
	}
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

func gatedModule(g *gate) *workflows.Module[struct{}] {
	return workflows.NewModule("gated", func(_ struct{}, deps workflows.Deps) (workflows.Instance, error) {
		return &gatedInstance{g: g}, nil
	})
}

// node builds a task manager on repo under the given node id, closed with the
// test.
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
	deadline := time.Now().Add(8 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestTwoNodesRunATaskExactlyOnce has two managers race Start on the same
// suspended task over one database: the server admits one, the other refuses
// with ErrClaimHeld.
func TestTwoNodesRunATaskExactlyOnce(t *testing.T) {
	repo := newTestTasks(t)
	g := newGate(false)
	seeder := node(t, repo, g, "seed")
	task, err := seeder.CreateTask(context.Background(), "gated", nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Park the task at its suspend boundary and detach, leaving a suspended,
	// unclaimed record — the shape a task hands between nodes cleanly.
	done := make(chan error, 1)
	go func() { _, err := task.Start(context.Background()); done <- err }()
	<-g.entered
	if err := task.Suspend(); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	g.release <- struct{}{}
	eventually(t, "a detachable park", func() bool { return task.Detach() == nil })
	if err := <-done; !errors.Is(err, tasks.ErrDetached) {
		t.Fatalf("detached Start: err = %v, want ErrDetached", err)
	}
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

// TestStolenRunFiresEffectsOnce crashes node A mid-state after its effect
// recorded, lets the lease lapse, and has node B steal and finish the task:
// the effect body runs exactly once across both nodes, and the zombie's late
// writes lose.
func TestStolenRunFiresEffectsOnce(t *testing.T) {
	repo := newTestTasks(t)
	g := newGate(true) // trap only the first run: the crash dummy
	a := node(t, repo, g, "node-a", tasks.WithLeaseTTL(150*time.Millisecond))
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

	b := node(t, repo, g, "node-b", tasks.WithLeaseTTL(150*time.Millisecond))
	eventually(t, "the thief to claim and finish the task", func() bool {
		stolen, err := b.RecoverClaimable(context.Background())
		if err != nil || len(stolen) != 1 {
			return false
		}
		if _, err := stolen[0].Start(context.Background()); err != nil {
			return false // the lease has not lapsed yet
		}
		rec, err := repo.RecoverTask(context.Background(), task.ID)
		return err == nil && rec.Status == string(workflows.StatusDone)
	})

	if n := g.effects.Load(); n != 1 {
		t.Fatalf("effect ran %d times across the two nodes, want exactly once", n)
	}

	// Release the zombie: its next write must lose to the thief's outcome.
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
}

// TestCapOneAdmitsOneAcrossManagers has two leasing managers race a cap-1
// payment over one database: one lease grants, the second waits, and the
// first release hands the capacity over.
func TestCapOneAdmitsOneAcrossManagers(t *testing.T) {
	repo := newTestPayments(t)
	ctx := context.Background()

	newNode := func() *payments.Manager {
		m, err := leasing.NewManager[payments.Payment](ctx, repo,
			leasing.WithLeaseTTL[payments.Payment, *payments.Payment](200*time.Millisecond))
		if err != nil {
			t.Fatalf("NewManager: %v", err)
		}
		t.Cleanup(func() { m.Close() })
		return m
	}
	a, b := newNode(), newNode()
	if err := a.Add(ctx, payments.Payment{Resource: leasing.Resource{ID: "p1"}}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	leaseA, err := a.Acquire(ctx, leasing.Assignment{TaskID: "task-a"})
	if err != nil {
		t.Fatalf("Acquire on a: %v", err)
	}

	granted := make(chan error, 1)
	go func() {
		lease, err := b.Acquire(ctx, leasing.Assignment{TaskID: "task-b"})
		if err == nil {
			lease.Release()
		}
		granted <- err
	}()

	select {
	case err := <-granted:
		t.Fatalf("b acquired while a held the only slot: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	leaseA.Release()
	select {
	case err := <-granted:
		if err != nil {
			t.Fatalf("Acquire on b after the release: %v", err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("b never acquired after a released")
	}
}
