package tasks

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/workflows"
)

// A linear three-state graph: s1 -> s2 -> s3 -> done. Handlers append the state
// they ran to a shared log so a test can see exactly which states executed, both
// on a fresh run and after recovery.
const (
	s1 workflows.State = "s1"
	s2 workflows.State = "s2"
	s3 workflows.State = "s3"
)

// testSnapshot is the durable JSON shape: a counter that proves restored running
// state carried across the snapshot.
type testSnapshot struct {
	Visited int `json:"visited"`
}

// testWorkflow builds Snapshotter instances over the linear graph. The shared log
// is injected into every instance it builds, so a fresh instance and a restored
// one write to the same observable log.
type testWorkflow struct {
	log *[]workflows.State
}

func (w *testWorkflow) ID() string                    { return "test" }
func (w *testWorkflow) ValidateInput(input any) error { return nil }

func (w *testWorkflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	return &testCtx{log: w.log}, nil
}

func (w *testWorkflow) RestoreInstance(deps workflows.Deps, snapshot []byte) (workflows.Instance, error) {
	var s testSnapshot
	if err := json.Unmarshal(snapshot, &s); err != nil {
		return nil, err
	}
	return &testCtx{log: w.log, visited: s.Visited}, nil
}

type testCtx struct {
	log     *[]workflows.State
	visited int
}

func (c *testCtx) Graph() workflows.Graph {
	return workflows.NewGraph(s1, workflows.States{
		s1: c.step(s1, workflows.Next(s2)),
		s2: c.step(s2, workflows.Next(s3)),
		s3: c.step(s3, nil),
	})
}

// step runs this state and advances to next.
func (c *testCtx) step(this workflows.State, next *workflows.State) workflows.StateHandler {
	return func(ctx context.Context) (*workflows.State, error) {
		*c.log = append(*c.log, this)
		c.visited++
		return next, nil
	}
}

func (c *testCtx) Snapshot() ([]byte, error) {
	return json.Marshal(testSnapshot{Visited: c.visited})
}

// bareWorkflow implements Workflow but not PersistableWorkflow, so it cannot be
// rehydrated.
type bareWorkflow struct{ log *[]workflows.State }

func (w bareWorkflow) ID() string                    { return "bare" }
func (w bareWorkflow) ValidateInput(input any) error { return nil }
func (w bareWorkflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	return &testCtx{log: w.log}, nil
}

// recordedSave is one captured checkpoint.
type recordedSave struct {
	taskID   string
	status   workflows.Status
	state    workflows.State
	snapshot []byte
}

// fakeStore captures every checkpoint and the terminal stamp so tests can
// inspect what the engine persisted. It implements Repository; the methods the
// engine never calls are no-ops. saveErr, when set, fails SaveCheckpoint for the
// states it returns an error for, simulating a store outage at a specific
// checkpoint.
type fakeStore struct {
	mu             sync.Mutex
	saves          []recordedSave
	terminal       workflows.Status
	terminalSet    bool
	terminalOutput []byte
	saveErr        func(state workflows.State) error
	// terminalErr, when set, fails MarkTerminal without recording the stamp,
	// simulating a store outage at the terminal write.
	terminalErr error
}

func (f *fakeStore) SaveCheckpoint(ctx context.Context, id, status, state string, snapshot []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saves = append(f.saves, recordedSave{id, workflows.Status(status), workflows.State(state), append([]byte(nil), snapshot...)})
	if f.saveErr != nil {
		return f.saveErr(workflows.State(state))
	}
	return nil
}

func (f *fakeStore) MarkTerminal(ctx context.Context, id, outcome string, output []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.terminalErr != nil {
		return f.terminalErr
	}
	f.terminal = workflows.Status(outcome)
	f.terminalSet = true
	// append to a nil slice preserves nil, so "no output" stays distinguishable.
	f.terminalOutput = append([]byte(nil), output...)
	return nil
}

func (f *fakeStore) CreateTask(ctx context.Context, record Task) error { return nil }
func (f *fakeStore) SaveAssignment(ctx context.Context, id string, kind leasing.Kind, a Assignment) error {
	return nil
}

func (f *fakeStore) RecoverTask(ctx context.Context, id string) (Task, error) {
	return Task{}, nil
}
func (f *fakeStore) RecoverAll(ctx context.Context) ([]Task, error)   { return nil, nil }
func (f *fakeStore) DeleteTask(ctx context.Context, id string) error  { return nil }
func (f *fakeStore) SaveGroup(ctx context.Context, group Group) error { return nil }
func (f *fakeStore) GetGroup(ctx context.Context, id string) (Group, bool, error) {
	return Group{}, false, nil
}
func (f *fakeStore) ListGroups(ctx context.Context) ([]Group, error) { return nil, nil }
func (f *fakeStore) DeleteGroup(ctx context.Context, id string) error {
	return nil
}
func (f *fakeStore) TasksInGroup(ctx context.Context, groupID string) ([]string, error) {
	return nil, nil
}

// snapshotFor returns the snapshot captured at the checkpoint that recorded state.
func (f *fakeStore) snapshotFor(state workflows.State) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, s := range f.saves {
		if s.state == state {
			return s.snapshot
		}
	}
	return nil
}

func (f *fakeStore) states() []workflows.State {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]workflows.State, len(f.saves))
	for i, s := range f.saves {
		out[i] = s.state
	}
	return out
}

// lastSuspended returns a copy of the most recent checkpoint stamped suspended.
func (f *fakeStore) lastSuspended() *recordedSave {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := len(f.saves) - 1; i >= 0; i-- {
		if f.saves[i].status == workflows.StatusSuspended {
			s := f.saves[i]
			return &s
		}
	}
	return nil
}

// waitFor polls cond until it holds or the timeout elapses, for coordinating with
// the engine goroutine.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// TestCheckpointRecordsStateBeforeEach verifies the engine checkpoints before
// entering each state, persisting that next unprocessed state and a snapshot
// reflecting progress made before it — the data recovery relies on.
func TestCheckpointRecordsStateBeforeEach(t *testing.T) {
	var log []workflows.State
	store := &fakeStore{}
	engine := newEngine(&testWorkflow{log: &log}, workflows.Deps{TaskID: "task-1"}, store)

	if err := engine.Execute(context.Background(), nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if got := log; !reflect.DeepEqual(got, []workflows.State{s1, s2, s3}) {
		t.Fatalf("executed states = %v, want [s1 s2 s3]", got)
	}
	// One checkpoint before each state; resume state equals the state about to run.
	if got := store.states(); !reflect.DeepEqual(got, []workflows.State{s1, s2, s3}) {
		t.Fatalf("checkpoint states = %v, want [s1 s2 s3]", got)
	}
	if store.saves[0].taskID != "task-1" {
		t.Fatalf("taskID = %q, want task-1", store.saves[0].taskID)
	}
	// The snapshot taken before s2 must reflect that s1 already ran (visited == 1).
	var snap testSnapshot
	if err := json.Unmarshal(store.snapshotFor(s2), &snap); err != nil {
		t.Fatalf("unmarshal s2 snapshot: %v", err)
	}
	if snap.Visited != 1 {
		t.Fatalf("s2 snapshot = %+v, want {Visited:1}", snap)
	}
	if !store.terminalSet || store.terminal != workflows.StatusDone {
		t.Fatalf("terminal = %q (set=%v), want done", store.terminal, store.terminalSet)
	}
}

// TestRehydrateResumesFromStartState runs a workflow to completion, then simulates
// a crash by rehydrating a fresh engine from the checkpoint captured before s2. It
// must resume at s2 (skipping s1) and carry the restored running state forward.
func TestRehydrateResumesFromStartState(t *testing.T) {
	var firstLog []workflows.State
	store := &fakeStore{}
	wf := &testWorkflow{log: &firstLog}

	if err := newEngine(wf, workflows.Deps{TaskID: "task-1"}, store).Execute(context.Background(), nil); err != nil {
		t.Fatalf("first Execute: %v", err)
	}

	// Recover from the snapshot persisted before s2, on a fresh engine and log.
	snapshot := store.snapshotFor(s2)
	if snapshot == nil {
		t.Fatal("no checkpoint captured for s2")
	}
	var recoveredLog []workflows.State
	recovered := &testWorkflow{log: &recoveredLog}
	recoverStore := &fakeStore{}
	engine := newEngine(recovered, workflows.Deps{TaskID: "task-1"}, recoverStore)

	if err := engine.Rehydrate(context.Background(), snapshot, s2, false); err != nil {
		t.Fatalf("Rehydrate: %v", err)
	}

	// s1 must NOT re-run; execution resumes at s2.
	if !reflect.DeepEqual(recoveredLog, []workflows.State{s2, s3}) {
		t.Fatalf("recovered states = %v, want [s2 s3]", recoveredLog)
	}
	// The restored snapshot (visited==1) carried forward: the checkpoint taken on
	// recovery before s3 should show visited==2, proving running state survived.
	var snap testSnapshot
	if err := json.Unmarshal(recoverStore.snapshotFor(s3), &snap); err != nil {
		t.Fatalf("unmarshal recovered s3 snapshot: %v", err)
	}
	if snap.Visited != 2 {
		t.Fatalf("recovered s3 snapshot visited = %d, want 2 (1 restored + s2)", snap.Visited)
	}
}

// TestRehydrateNonPersistableWorkflowErrors verifies that rehydrating a workflow
// that does not implement PersistableWorkflow fails loudly rather than silently
// starting over.
func TestRehydrateNonPersistableWorkflowErrors(t *testing.T) {
	var log []workflows.State
	engine := newEngine(bareWorkflow{log: &log}, workflows.Deps{TaskID: "task-1"}, &fakeStore{})

	err := engine.Rehydrate(context.Background(), []byte(`{}`), s2, false)
	if err == nil {
		t.Fatal("Rehydrate on non-persistable workflow: want error, got nil")
	}
	if !strings.Contains(err.Error(), "not persistable") {
		t.Fatalf("error = %v, want it to mention 'not persistable'", err)
	}
	if len(log) != 0 {
		t.Fatalf("no states should have run, got %v", log)
	}
}

// TestRehydrateIsNoOpOnceStarted verifies an engine that already ran cannot be
// rehydrated again, mirroring Execute's started-guard.
func TestRehydrateIsNoOpOnceStarted(t *testing.T) {
	var log []workflows.State
	store := &fakeStore{}
	engine := newEngine(&testWorkflow{log: &log}, workflows.Deps{TaskID: "task-1"}, store)

	if err := engine.Execute(context.Background(), nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	before := len(log)

	if err := engine.Rehydrate(context.Background(), store.snapshotFor(s2), s2, false); err != nil {
		t.Fatalf("Rehydrate after completion: %v", err)
	}
	if len(log) != before {
		t.Fatalf("Rehydrate re-ran states on a finished engine: %v", log)
	}
}

// gatedWorkflow's s1 handler blocks on release until the test lets it finish,
// closing entered when it starts so the test can deterministically suspend the
// engine mid-flight. s2 and s3 run freely.
type gatedWorkflow struct {
	log     *[]workflows.State
	entered chan struct{}
	release chan struct{}
}

func (w *gatedWorkflow) ID() string                    { return "gated" }
func (w *gatedWorkflow) ValidateInput(input any) error { return nil }

func (w *gatedWorkflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	return &gatedCtx{log: w.log, entered: w.entered, release: w.release}, nil
}

type gatedCtx struct {
	log     *[]workflows.State
	visited int
	entered chan struct{}
	release chan struct{}
}

func (c *gatedCtx) Graph() workflows.Graph {
	return workflows.NewGraph(s1, workflows.States{
		s1: func(ctx context.Context) (*workflows.State, error) {
			*c.log = append(*c.log, s1)
			c.visited++
			close(c.entered)
			<-c.release
			return workflows.Next(s2), nil
		},
		s2: c.step(s2, workflows.Next(s3)),
		s3: c.step(s3, nil),
	})
}

func (c *gatedCtx) step(this workflows.State, next *workflows.State) workflows.StateHandler {
	return func(ctx context.Context) (*workflows.State, error) {
		*c.log = append(*c.log, this)
		c.visited++
		return next, nil
	}
}

func (c *gatedCtx) Snapshot() ([]byte, error) {
	return json.Marshal(testSnapshot{Visited: c.visited})
}

// TestSuspendPersistsDurableCheckpoint verifies that suspending an in-flight task
// writes a durable checkpoint stamped suspended at the next state's resume point,
// so recovery could bring it back paused exactly where it left off, and that
// resuming continues to completion.
func TestSuspendPersistsDurableCheckpoint(t *testing.T) {
	var log []workflows.State
	store := &fakeStore{}
	wf := &gatedWorkflow{
		log:     &log,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	engine := newEngine(wf, workflows.Deps{TaskID: "task-1"}, store)

	done := make(chan error, 1)
	go func() { done <- engine.Execute(context.Background(), nil) }()

	// Wait until s1 is in flight, then suspend and let s1 finish.
	select {
	case <-wf.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("s1 never entered")
	}
	if err := engine.Suspend(); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	close(wf.release)

	// The engine must park and persist a suspended checkpoint pointing at s2.
	waitFor(t, func() bool { return store.lastSuspended() != nil })
	sv := store.lastSuspended()
	if sv.state != s2 {
		t.Fatalf("suspended checkpoint state = %v, want s2 (resume where it left off)", sv.state)
	}
	var snap testSnapshot
	if err := json.Unmarshal(sv.snapshot, &snap); err != nil {
		t.Fatalf("unmarshal suspended snapshot: %v", err)
	}
	if snap.Visited != 1 {
		t.Fatalf("suspended snapshot visited = %d, want 1 (s1 done)", snap.Visited)
	}
	if engine.Status() != workflows.StatusSuspended {
		t.Fatalf("status = %q, want suspended", engine.Status())
	}

	// Resume continues from s2 to completion.
	if err := engine.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute after resume: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task did not complete after resume")
	}

	if !reflect.DeepEqual(log, []workflows.State{s1, s2, s3}) {
		t.Fatalf("states = %v, want [s1 s2 s3]", log)
	}
	if !store.terminalSet || store.terminal != workflows.StatusDone {
		t.Fatalf("terminal = %q (set=%v), want done", store.terminal, store.terminalSet)
	}
}

// TestRecoveredSuspendedTaskStartsPaused verifies a task recovered from a
// suspended checkpoint starts parked at its resume state instead of running —
// the durable suspend must survive the crash it was persisted for, or recovery
// silently overrides an operator's pause. Resume continues where it left off.
func TestRecoveredSuspendedTaskStartsPaused(t *testing.T) {
	var log []workflows.State
	store := &fakeStore{}
	snapshot, err := json.Marshal(testSnapshot{Visited: 1})
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	task := rehydrateTask(&testWorkflow{log: &log}, Task{ID: "task-1", Snapshot: snapshot, State: string(s2), Status: string(workflows.StatusSuspended)}, nil, store, nil)

	done := make(chan error, 1)
	go func() { _, serr := task.Start(context.Background()); done <- serr }()

	// The engine must come up parked: live (started) but suspended, no state run.
	waitFor(t, func() bool { return task.IsRunning() && workflows.Status(task.Status) == workflows.StatusSuspended })
	if len(log) != 0 {
		t.Fatalf("states ran while parked: %v", log)
	}

	if err := task.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start after resume: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task did not complete after resume")
	}

	if !reflect.DeepEqual(log, []workflows.State{s2, s3}) {
		t.Fatalf("states = %v, want [s2 s3] (resumed at the suspend point)", log)
	}
}

// TestLiveStatusTracksLifecycle verifies LiveStatus follows the run live —
// not started, running, suspended, done — where the Status field only catches
// up when Start exits, and that a rehydrated-but-unstarted task falls back to
// its persisted status instead of reporting not started.
func TestLiveStatusTracksLifecycle(t *testing.T) {
	var log []workflows.State
	store := &fakeStore{}
	wf := &gatedWorkflow{
		log:     &log,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	task, err := createTask(wf, wf.ID(), nil, nil, store, "", nil, nil)
	if err != nil {
		t.Fatalf("createTask: %v", err)
	}

	if got := task.LiveStatus(); got != workflows.StatusNotStarted {
		t.Fatalf("LiveStatus before Start = %q, want not started", got)
	}

	done := make(chan error, 1)
	go func() { _, serr := task.Start(context.Background()); done <- serr }()

	select {
	case <-wf.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("s1 never entered")
	}
	if got := task.LiveStatus(); got != workflows.StatusRunning {
		t.Fatalf("LiveStatus mid-s1 = %q, want running", got)
	}

	if err := task.Suspend(); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	close(wf.release)
	waitFor(t, func() bool { return task.LiveStatus() == workflows.StatusSuspended })

	if err := task.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("task did not complete after resume")
	}
	if got := task.LiveStatus(); got != workflows.StatusDone {
		t.Fatalf("LiveStatus after completion = %q, want done", got)
	}

	// A recovered task that has not started yet answers with its persisted
	// status — an operator inspecting it must not read "not started".
	parked := rehydrateTask(&testWorkflow{log: &log}, Task{ID: "task-2", State: string(s2), Status: string(workflows.StatusSuspended)}, nil, store, nil)
	if got := parked.LiveStatus(); got != workflows.StatusSuspended {
		t.Fatalf("LiveStatus on rehydrated unstarted task = %q, want suspended", got)
	}
}

// TestRecoveredTerminalTaskRefusesStart verifies a task recovered with a
// terminal outcome cannot be started again: MarkTerminal leaves state and
// snapshot in place, so a permissive Start would silently re-execute the final
// state — duplicating real-world side effects — and overwrite the stamp.
func TestRecoveredTerminalTaskRefusesStart(t *testing.T) {
	snapshot, err := json.Marshal(testSnapshot{Visited: 2})
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	for _, status := range []workflows.Status{workflows.StatusDone, workflows.StatusKilled} {
		var log []workflows.State
		task := rehydrateTask(&testWorkflow{log: &log}, Task{ID: "task-1", Snapshot: snapshot, State: string(s3), Status: string(status)}, nil, &fakeStore{}, nil)

		if _, err := task.Start(context.Background()); !errors.Is(err, ErrAlreadyTerminal) {
			t.Fatalf("Start on recovered %q task: err = %v, want ErrAlreadyTerminal", status, err)
		}
		if len(log) != 0 {
			t.Fatalf("recovered %q task ran states: %v", status, log)
		}
	}
}

// TestRecoveredFailedTaskRetries verifies a failed task is not terminal: it
// recovers and retries from its last checkpoint, which is the whole point of
// stamping failure separately from completion.
func TestRecoveredFailedTaskRetries(t *testing.T) {
	snapshot, err := json.Marshal(testSnapshot{Visited: 1})
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	var log []workflows.State
	store := &fakeStore{}
	task := rehydrateTask(&testWorkflow{log: &log}, Task{ID: "task-1", Snapshot: snapshot, State: string(s2), Status: string(workflows.StatusFailed)}, nil, store, nil)

	if _, err := task.Start(context.Background()); err != nil {
		t.Fatalf("Start on recovered failed task: %v", err)
	}
	if !reflect.DeepEqual(log, []workflows.State{s2, s3}) {
		t.Fatalf("states = %v, want [s2 s3] (retried from the checkpoint)", log)
	}
	if store.terminal != workflows.StatusDone {
		t.Fatalf("terminal = %q, want done after a successful retry", store.terminal)
	}
}

// teardownRecorder captures Teardown invocations so tests can assert when and
// with what the engine called it. result is what Teardown returns.
type teardownRecorder struct {
	mu     sync.Mutex
	calls  int
	status workflows.Status
	runErr error
	result error
}

func (r *teardownRecorder) record(status workflows.Status, runErr error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.status = status
	r.runErr = runErr
	return r.result
}

func (r *teardownRecorder) snapshot() (int, workflows.Status, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.status, r.runErr
}

// teardownWorkflow builds Teardowner instances over the linear graph. failAt
// makes that state's handler fail; a non-nil entered gates s1 until release so
// kill tests can interrupt mid-flight.
type teardownWorkflow struct {
	rec     *teardownRecorder
	failAt  workflows.State
	entered chan struct{}
	release chan struct{}
}

func (w *teardownWorkflow) ID() string                    { return "teardown" }
func (w *teardownWorkflow) ValidateInput(input any) error { return nil }

func (w *teardownWorkflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	return &teardownCtx{w: w}, nil
}

type teardownCtx struct {
	w *teardownWorkflow
}

func (c *teardownCtx) Graph() workflows.Graph {
	return workflows.NewGraph(s1, workflows.States{
		s1: c.step(s1, workflows.Next(s2)),
		s2: c.step(s2, workflows.Next(s3)),
		s3: c.step(s3, nil),
	})
}

func (c *teardownCtx) step(this workflows.State, next *workflows.State) workflows.StateHandler {
	return func(ctx context.Context) (*workflows.State, error) {
		if this == s1 && c.w.entered != nil {
			close(c.w.entered)
			<-c.w.release
		}
		if c.w.failAt == this {
			return nil, errors.New("handler failed")
		}
		return next, nil
	}
}

func (c *teardownCtx) Teardown(ctx context.Context, status workflows.Status, runErr error) error {
	return c.w.rec.record(status, runErr)
}

// TestTeardownRunsOnCompletion verifies a clean run tears down exactly once
// with the done status and no run error, so instances can release resources
// they acquired during the run.
func TestTeardownRunsOnCompletion(t *testing.T) {
	rec := &teardownRecorder{}
	engine := newEngine(&teardownWorkflow{rec: rec}, workflows.Deps{TaskID: "task-1"}, nil)

	if err := engine.Execute(context.Background(), nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	calls, status, runErr := rec.snapshot()
	if calls != 1 {
		t.Fatalf("teardown called %d times, want 1", calls)
	}
	if status != workflows.StatusDone || runErr != nil {
		t.Fatalf("teardown saw status=%q runErr=%v, want done/nil", status, runErr)
	}
}

// TestTeardownRunsOnHandlerError verifies a failed run still tears down,
// receives the run error, and observes the failed status — resources must not
// leak on failure, and teardown must be able to tell failure from completion.
func TestTeardownRunsOnHandlerError(t *testing.T) {
	rec := &teardownRecorder{}
	engine := newEngine(&teardownWorkflow{rec: rec, failAt: s2}, workflows.Deps{TaskID: "task-1"}, nil)

	err := engine.Execute(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "handler failed") {
		t.Fatalf("Execute err = %v, want handler failure", err)
	}

	calls, status, runErr := rec.snapshot()
	if calls != 1 {
		t.Fatalf("teardown called %d times, want 1", calls)
	}
	if runErr == nil || !strings.Contains(runErr.Error(), "handler failed") {
		t.Fatalf("teardown runErr = %v, want the handler failure", runErr)
	}
	if status != workflows.StatusFailed {
		t.Fatalf("teardown status = %q, want failed", status)
	}
}

// TestHandlerErrorStampsFailed verifies an errored run is durably stamped
// failed, not done — a consumer reading the store after a restart must be able
// to tell a run that broke from one that finished, and a failed task must stay
// recoverable from its last checkpoint rather than look complete.
func TestHandlerErrorStampsFailed(t *testing.T) {
	store := &fakeStore{}
	e := newEngine(outputWorkflow{failRun: true}, workflows.Deps{TaskID: "task-1"}, store)

	if err := e.Execute(context.Background(), nil); err == nil {
		t.Fatal("Execute: want run error, got nil")
	}
	if e.Status() != workflows.StatusFailed {
		t.Fatalf("status = %q, want failed", e.Status())
	}
	if !store.terminalSet || store.terminal != workflows.StatusFailed {
		t.Fatalf("stamped outcome = %q (set=%v), want failed", store.terminal, store.terminalSet)
	}
	if store.terminal.Terminal() {
		t.Fatal("failed must not be terminal: the task is still recoverable")
	}
}

// TestTeardownRunsOnKill verifies a killed run tears down with the killed
// status, so instances can release resources even when interrupted.
func TestTeardownRunsOnKill(t *testing.T) {
	rec := &teardownRecorder{}
	wf := &teardownWorkflow{
		rec:     rec,
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	engine := newEngine(wf, workflows.Deps{TaskID: "task-1"}, nil)

	done := make(chan error, 1)
	go func() { done <- engine.Execute(context.Background(), nil) }()

	select {
	case <-wf.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("s1 never entered")
	}
	if err := engine.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	close(wf.release)

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("killed run did not exit")
	}

	calls, status, _ := rec.snapshot()
	if calls != 1 {
		t.Fatalf("teardown called %d times, want 1", calls)
	}
	if status != workflows.StatusKilled {
		t.Fatalf("teardown status = %q, want killed", status)
	}
}

// TestKillBeforeStartLatches verifies a kill delivered before Start is not
// lost: the race between an operator's kill and a scheduler's Start must
// resolve to the kill, or the task runs right after being told to die.
func TestKillBeforeStartLatches(t *testing.T) {
	var log []workflows.State
	task, err := createTask(&testWorkflow{log: &log}, "wf", nil, nil, &fakeStore{}, "", nil, nil)
	if err != nil {
		t.Fatalf("createTask: %v", err)
	}

	if err := task.Kill(); err != nil {
		t.Fatalf("Kill: %v", err)
	}
	if _, err := task.Start(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Start after kill: err = %v, want context.Canceled", err)
	}
	if len(log) != 0 {
		t.Fatalf("states ran after a pre-start kill: %v", log)
	}
	if workflows.Status(task.Status) != workflows.StatusKilled {
		t.Fatalf("status = %q, want killed", task.Status)
	}
}

// TestTeardownErrorSurfaced verifies a teardown failure is returned to the
// caller rather than swallowed, because a leaked resource must be visible.
func TestTeardownErrorSurfaced(t *testing.T) {
	rec := &teardownRecorder{result: errors.New("release failed")}
	engine := newEngine(&teardownWorkflow{rec: rec}, workflows.Deps{TaskID: "task-1"}, nil)

	err := engine.Execute(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "release failed") {
		t.Fatalf("Execute err = %v, want teardown failure surfaced", err)
	}
}

// TestCheckpointFailureAbortsRun verifies a checkpoint failure aborts the run
// before the state whose checkpoint failed executes, and that the task remains
// resumable from the last successful checkpoint — durability is mandatory, and
// a store outage must never let a state run without its resume point persisted.
func TestCheckpointFailureAbortsRun(t *testing.T) {
	var log []workflows.State
	store := &fakeStore{saveErr: func(s workflows.State) error {
		if s == s2 {
			return errors.New("store down")
		}
		return nil
	}}
	engine := newEngine(&testWorkflow{log: &log}, workflows.Deps{TaskID: "task-1"}, store)

	err := engine.Execute(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "store down") {
		t.Fatalf("Execute err = %v, want store failure", err)
	}
	if !reflect.DeepEqual(log, []workflows.State{s1}) {
		t.Fatalf("states = %v, want [s1] (aborted before s2's handler)", log)
	}
	// The aborted run must stamp failed, not done: the task is unfinished.
	if store.terminal != workflows.StatusFailed {
		t.Fatalf("stamped outcome = %q, want failed", store.terminal)
	}

	// The last good checkpoint (s2's resume point was never saved, s1's was)
	// still resumes the task: rehydrate from it and run to completion.
	snapshot := store.snapshotFor(s1)
	if snapshot == nil {
		t.Fatal("no checkpoint captured for s1")
	}
	var resumedLog []workflows.State
	resumed := newEngine(&testWorkflow{log: &resumedLog}, workflows.Deps{TaskID: "task-1"}, &fakeStore{})
	if err := resumed.Rehydrate(context.Background(), snapshot, s1, false); err != nil {
		t.Fatalf("Rehydrate after checkpoint failure: %v", err)
	}
	if !reflect.DeepEqual(resumedLog, []workflows.State{s1, s2, s3}) {
		t.Fatalf("resumed states = %v, want [s1 s2 s3]", resumedLog)
	}
}

// outputWorkflow builds single-state Outputter instances so output tests can
// assert what the engine harvests. failRun makes the lone state error before
// completing; outErr makes Output itself fail.
type outputWorkflow struct {
	output  []byte
	outErr  error
	failRun bool
}

func (w outputWorkflow) ID() string                    { return "output" }
func (w outputWorkflow) ValidateInput(input any) error { return nil }
func (w outputWorkflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	return &outputCtx{w: w}, nil
}

type outputCtx struct {
	w outputWorkflow
}

func (c *outputCtx) Graph() workflows.Graph {
	return workflows.NewGraph(s1, workflows.States{
		s1: func(ctx context.Context) (*workflows.State, error) {
			if c.w.failRun {
				return nil, errors.New("run failed")
			}
			return nil, nil
		},
	})
}

func (c *outputCtx) Output() ([]byte, error) { return c.w.output, c.w.outErr }

// TestHarvestsOutputOnCleanCompletion verifies the engine captures an Outputter
// instance's result on a clean run, exposes it, and persists it with the
// terminal stamp — so a finished task can hand its output back to the caller and
// a recovered one can read it durably.
func TestHarvestsOutputOnCleanCompletion(t *testing.T) {
	store := &fakeStore{}
	want := []byte(`{"orderID":"order-1"}`)
	e := newEngine(outputWorkflow{output: want}, workflows.Deps{TaskID: "task-1"}, store)

	if err := e.Execute(context.Background(), nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if string(e.Output()) != string(want) {
		t.Fatalf("engine output = %q, want %q", e.Output(), want)
	}
	if string(store.terminalOutput) != string(want) {
		t.Fatalf("persisted output = %q, want %q", store.terminalOutput, want)
	}
	if store.terminal != workflows.StatusDone {
		t.Fatalf("terminal = %q, want done", store.terminal)
	}
}

// TestNoOutputCapabilityYieldsNil verifies a workflow that does not implement
// Outputter completes cleanly with no output, so output stays strictly opt-in
// and the terminal stamp persists nil rather than fabricating a result.
func TestNoOutputCapabilityYieldsNil(t *testing.T) {
	var log []workflows.State
	store := &fakeStore{}
	e := newEngine(&testWorkflow{log: &log}, workflows.Deps{TaskID: "task-1"}, store)

	if err := e.Execute(context.Background(), nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if e.Output() != nil {
		t.Fatalf("output = %q, want nil for a non-Outputter workflow", e.Output())
	}
	if store.terminalOutput != nil {
		t.Fatalf("persisted output = %q, want nil", store.terminalOutput)
	}
}

// TestErroredRunHarvestsNoOutput verifies a run that errors before completing
// does not harvest output even though the instance can produce one, because
// output belongs only to clean completion.
func TestErroredRunHarvestsNoOutput(t *testing.T) {
	store := &fakeStore{}
	e := newEngine(outputWorkflow{output: []byte("x"), failRun: true}, workflows.Deps{TaskID: "task-1"}, store)

	if err := e.Execute(context.Background(), nil); err == nil {
		t.Fatal("Execute: want run error, got nil")
	}
	if e.Output() != nil {
		t.Fatalf("output = %q, want nil on error", e.Output())
	}
	if store.terminalOutput != nil {
		t.Fatalf("persisted output = %q, want nil on error", store.terminalOutput)
	}
}

// TestOutputErrorAbortsRun verifies an Output failure aborts the run with the
// error rather than reporting a clean completion, because a result that cannot
// be produced is a failure, not an empty success.
func TestOutputErrorAbortsRun(t *testing.T) {
	store := &fakeStore{}
	e := newEngine(outputWorkflow{outErr: errors.New("marshal failed")}, workflows.Deps{TaskID: "task-1"}, store)

	err := e.Execute(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "marshal failed") {
		t.Fatalf("Execute err = %v, want output failure surfaced", err)
	}
}

// TestTerminalStampFailureSurfaced verifies a run whose terminal stamp cannot
// be persisted returns the failure instead of swallowing it — otherwise the
// caller reports success while the store still says running, and a restart
// would re-execute completed work. The harvested output is still handed back:
// the workflow itself finished.
func TestTerminalStampFailureSurfaced(t *testing.T) {
	want := []byte(`{"orderID":"order-1"}`)
	store := &fakeStore{terminalErr: errors.New("store down")}
	task, err := createTask(outputWorkflow{output: want}, "wf", nil, nil, store, "", nil, nil)
	if err != nil {
		t.Fatalf("createTask: %v", err)
	}

	got, err := task.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "store down") {
		t.Fatalf("Start err = %v, want the stamp failure surfaced", err)
	}
	if string(got) != string(want) {
		t.Fatalf("Start output = %q, want %q alongside the error", got, want)
	}
	if workflows.Status(task.Status) != workflows.StatusDone {
		t.Fatalf("status = %q, want done (the workflow itself completed)", task.Status)
	}
}

// TestStartReturnsWorkflowOutput verifies Start hands the caller the workflow's
// output on clean completion — the whole point of the output capability, exposed
// at the task boundary the consumer actually calls.
func TestStartReturnsWorkflowOutput(t *testing.T) {
	want := []byte(`{"orderID":"order-1"}`)
	task, err := createTask(outputWorkflow{output: want}, "wf", nil, nil, &fakeStore{}, "", nil, nil)
	if err != nil {
		t.Fatalf("createTask: %v", err)
	}

	got, err := task.Start(context.Background())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("Start output = %q, want %q", got, want)
	}
}
