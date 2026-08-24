package tasks

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/workflows"
)

// TestNilRepositoryRunsInMemoryLifecycle verifies a Service built with a nil
// repository supports the full in-memory lifecycle — create, start to completion,
// and delete — without ever dereferencing a store. This is the purely in-memory
// use case: no durability is wanted, so a nil repository must be a valid choice
// rather than a panic.
func TestNilRepositoryRunsInMemoryLifecycle(t *testing.T) {
	var log []workflows.State
	svc := NewService(nil, comms.NewBus())
	wf := &testWorkflow{log: &log}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	task, err := svc.CreateTask(context.Background(), wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask with nil repository: %v", err)
	}

	if _, err := task.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if task.Status() != workflows.StatusDone {
		t.Fatalf("status = %q, want done", task.Status())
	}

	if err := svc.DeleteTask(context.Background(), task.ID()); err != nil {
		t.Fatalf("DeleteTask with nil repository: %v", err)
	}
}

// TestNilRepositoryRecoverAllIsEmpty verifies recovery is a harmless no-op with no
// durable store: there are no persisted tasks to rehydrate, so a startup recovery
// loop stays safe in in-memory mode rather than crashing on a nil store.
func TestNilRepositoryRecoverAllIsEmpty(t *testing.T) {
	svc := NewService(nil, comms.NewBus())

	recovered, err := svc.RecoverAll(context.Background())
	if err != nil {
		t.Fatalf("RecoverAll with nil repository: %v", err)
	}
	if len(recovered) != 0 {
		t.Fatalf("recovered %d tasks, want 0 (nothing persisted)", len(recovered))
	}
}

// TestNilRepositoryRecoverTaskErrors verifies recovering a specific task fails
// loudly with a nil repository: there is nothing durable to rehydrate from, and
// silently returning a zero task would hide that.
func TestNilRepositoryRecoverTaskErrors(t *testing.T) {
	svc := NewService(nil, comms.NewBus())

	if _, err := svc.RecoverTask(context.Background(), "missing"); err == nil {
		t.Fatal("RecoverTask with nil repository: want error, got nil")
	}
}

// recordStore is a fakeStore that recovers a canned record, standing in for a
// repository holding one persisted task.
type recordStore struct {
	fakeStore
	record Record
}

func (r *recordStore) RecoverTask(ctx context.Context, id string) (Record, error) {
	return r.record, nil
}

// TestRecoverNeverCheckpointedTaskFailsLoud verifies a task that was created
// but never checkpointed recovers into a task whose Start fails with
// ErrNoCheckpoint. Its input was never persisted, so nothing can actually
// resume; a cryptic state error — or silently rerunning — would both lie about
// what recovery can do. Durability begins at the first checkpoint.
func TestRecoverNeverCheckpointedTaskFailsLoud(t *testing.T) {
	var log []workflows.State
	store := &recordStore{record: Record{ID: "t1", WorkflowID: "test"}}
	svc := NewService(store, comms.NewBus())
	wf := &testWorkflow{log: &log}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	task, err := svc.RecoverTask(context.Background(), "t1")
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}

	if _, err := task.Start(context.Background()); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("Start err = %v, want ErrNoCheckpoint", err)
	}
	if len(log) != 0 {
		t.Fatalf("no states should have run, got %v", log)
	}
}

// memStore is an in-memory Repository with real group and record storage, so
// group placement and cascade deletion can be asserted without sqlite.
type memStore struct {
	mu      sync.Mutex
	records map[string]Record
	groups  map[string]Group
	deleted []string
}

func newMemStore() *memStore {
	return &memStore{records: map[string]Record{}, groups: map[string]Group{}}
}

func (m *memStore) CreateTask(ctx context.Context, rec Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[rec.ID] = rec
	return nil
}

func (m *memStore) SaveCheckpoint(ctx context.Context, id, status, state string, snapshot []byte) error {
	return nil
}

func (m *memStore) MarkTerminal(ctx context.Context, id, outcome string, output []byte) error {
	return nil
}

func (m *memStore) RecoverTask(ctx context.Context, id string) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[id]
	if !ok {
		return Record{}, errors.New("not found")
	}
	return rec, nil
}

func (m *memStore) RecoverAll(ctx context.Context) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, 0, len(m.records))
	for _, rec := range m.records {
		out = append(out, rec)
	}
	return out, nil
}

func (m *memStore) DeleteTask(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, id)
	m.deleted = append(m.deleted, id)
	return nil
}

func (m *memStore) SaveGroup(ctx context.Context, g Group) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groups[g.ID] = g
	return nil
}

func (m *memStore) GetGroup(ctx context.Context, id string) (Group, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[id]
	return g, ok, nil
}

func (m *memStore) ListGroups(ctx context.Context) ([]Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Group, 0, len(m.groups))
	for _, g := range m.groups {
		out = append(out, g)
	}
	return out, nil
}

func (m *memStore) DeleteGroup(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.groups, id)
	return nil
}

func (m *memStore) TasksInGroup(ctx context.Context, groupID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0)
	for id, rec := range m.records {
		if rec.GroupID == groupID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids, nil
}

func (m *memStore) record(t *testing.T, id string) Record {
	t.Helper()
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[id]
	if !ok {
		t.Fatalf("task %s not in store", id)
	}
	return rec
}

// depsWorkflow captures the Deps injected into every instance it builds, so a
// test can assert which proxy group a task was actually wired to.
type depsWorkflow struct {
	mu   sync.Mutex
	seen []workflows.Deps
}

func (w *depsWorkflow) ID() string                    { return "deps" }
func (w *depsWorkflow) ValidateInput(input any) error { return nil }

func (w *depsWorkflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	w.capture(deps)
	return &testCtx{log: &[]workflows.State{}}, nil
}

func (w *depsWorkflow) RestoreInstance(deps workflows.Deps, snapshot []byte) (workflows.Instance, error) {
	w.capture(deps)
	return &testCtx{log: &[]workflows.State{}}, nil
}

func (w *depsWorkflow) capture(deps workflows.Deps) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.seen = append(w.seen, deps)
}

// proxyGroupOf returns the proxy group the instance built for taskID was wired
// to, failing the test if no instance was built for it.
func (w *depsWorkflow) proxyGroupOf(t *testing.T, taskID string) string {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, d := range w.seen {
		if d.TaskID == taskID {
			return d.ProxyGroupID
		}
	}
	t.Fatalf("no instance built for task %s", taskID)
	return ""
}

// newDepsService returns a service with a deps-capturing workflow registered,
// alongside the store backing it.
func newDepsService(t *testing.T, opts ...ServiceOption) (Service, *memStore, *depsWorkflow) {
	t.Helper()
	store := newMemStore()
	wf := &depsWorkflow{}
	svc := NewService(store, comms.NewBus(), opts...)
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc, store, wf
}

// startAndCapture starts the task so its instance is built, exposing the Deps
// the service resolved for it.
func startAndCapture(t *testing.T, task Task) {
	t.Helper()
	if _, err := task.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// TestUngroupedTaskLandsInGlobalAndRunsProxyless verifies a task created with
// no options is placed in the global group and, because nothing assigns it a
// proxy group, runs without proxies — the documented default.
func TestUngroupedTaskLandsInGlobalAndRunsProxyless(t *testing.T) {
	svc, store, wf := newDepsService(t)
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	startAndCapture(t, task)

	rec := store.record(t, task.ID())
	if rec.GroupID != GlobalGroup {
		t.Fatalf("GroupID = %q, want %q", rec.GroupID, GlobalGroup)
	}
	if rec.ProxyGroupID != nil {
		t.Fatalf("ProxyGroupID = %q, want nil (inherit)", *rec.ProxyGroupID)
	}
	if rec.CreatedAt.IsZero() || rec.UpdatedAt.IsZero() {
		t.Fatal("timestamps not stamped at creation")
	}
	if got := wf.proxyGroupOf(t, task.ID()); got != "" {
		t.Fatalf("wired proxy group = %q, want none", got)
	}
}

// TestTaskInheritsGroupProxyAssignment verifies a task in a group with a proxy
// group assigned leases from it, which is the point of assigning one to a group.
func TestTaskInheritsGroupProxyAssignment(t *testing.T) {
	svc, _, wf := newDepsService(t)
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ProxyGroupID: "residential"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	task, err := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	startAndCapture(t, task)

	if got := wf.proxyGroupOf(t, task.ID()); got != "residential" {
		t.Fatalf("wired proxy group = %q, want residential", got)
	}
}

// TestTaskProxyAssignmentOverridesGroup verifies a task's own assignment wins
// over its group's, in both directions: naming a different group, and opting
// out of proxies entirely.
func TestTaskProxyAssignmentOverridesGroup(t *testing.T) {
	svc, store, wf := newDepsService(t)
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ProxyGroupID: "residential"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	override, err := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"), WithProxyGroup("datacenter"))
	if err != nil {
		t.Fatalf("CreateTask override: %v", err)
	}
	startAndCapture(t, override)
	if got := wf.proxyGroupOf(t, override.ID()); got != "datacenter" {
		t.Fatalf("override wired to %q, want datacenter", got)
	}

	proxyless, err := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"), WithoutProxies())
	if err != nil {
		t.Fatalf("CreateTask proxyless: %v", err)
	}
	startAndCapture(t, proxyless)
	if got := wf.proxyGroupOf(t, proxyless.ID()); got != "" {
		t.Fatalf("proxyless wired to %q, want none", got)
	}
	// The opt-out must persist as an explicit empty assignment, not as nil:
	// nil would inherit the group's proxies again on recovery.
	rec := store.record(t, proxyless.ID())
	if rec.ProxyGroupID == nil || *rec.ProxyGroupID != "" {
		t.Fatalf("persisted ProxyGroupID = %v, want explicit empty", rec.ProxyGroupID)
	}
}

// TestProxyGroupShardedAcrossManyOwners verifies one proxy group can back
// several task groups and several individually-assigned tasks at once. The
// assignment is a reference, not ownership: nothing about naming a proxy group
// may make it exclusive to the first task group that claimed it.
func TestProxyGroupSharedAcrossManyOwners(t *testing.T) {
	svc, _, wf := newDepsService(t)
	ctx := context.Background()

	for _, id := range []string{"checkout", "restock"} {
		if err := svc.CreateGroup(ctx, Group{ID: id, ProxyGroupID: "residential"}); err != nil {
			t.Fatalf("CreateGroup %s: %v", id, err)
		}
	}

	created := map[string]Task{}
	for name, opts := range map[string][]CreateOption{
		"viaCheckout":   {InGroup("checkout")},
		"viaRestock":    {InGroup("restock")},
		"direct":        {WithProxyGroup("residential")},
		"directInGroup": {InGroup("restock"), WithProxyGroup("residential")},
	} {
		task, err := svc.CreateTask(ctx, wf.ID(), nil, opts...)
		if err != nil {
			t.Fatalf("CreateTask %s: %v", name, err)
		}
		startAndCapture(t, task)
		created[name] = task
	}

	for name, task := range created {
		if got := wf.proxyGroupOf(t, task.ID()); got != "residential" {
			t.Fatalf("%s wired to %q, want residential", name, got)
		}
	}
}

// TestCreateTaskRejectsUnknownGroup verifies placing a task in a group that
// does not exist fails instead of silently creating one, so a typo cannot
// scatter tasks into a namespace nobody manages.
func TestCreateTaskRejectsUnknownGroup(t *testing.T) {
	svc, _, wf := newDepsService(t)

	if _, err := svc.CreateTask(context.Background(), wf.ID(), nil, InGroup("ghost")); err == nil {
		t.Fatal("expected error for unknown task group")
	}
}

// TestRecoveredTaskResolvesInheritedProxyGroup verifies recovery re-resolves a
// task's proxy group from its group, so a group reassigned while the task was
// offline takes effect on the next run.
func TestRecoveredTaskResolvesInheritedProxyGroup(t *testing.T) {
	svc, store, wf := newDepsService(t)
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ProxyGroupID: "residential"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	task, err := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Reassign the group, then recover: the record stores no proxy group of its
	// own, so the new assignment must apply.
	if err := store.SaveGroup(ctx, Group{ID: "checkout", ProxyGroupID: "datacenter"}); err != nil {
		t.Fatalf("SaveGroup: %v", err)
	}
	// Give the record a checkpoint so the recovered task is startable.
	rec := store.record(t, task.ID())
	rec.Status, rec.State, rec.Snapshot = string(workflows.StatusRunning), string(s3), []byte(`{"visited":2}`)
	store.CreateTask(ctx, rec)

	recovered, err := svc.RecoverTask(ctx, task.ID())
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	startAndCapture(t, recovered)

	if got := wf.proxyGroupOf(t, task.ID()); got != "datacenter" {
		t.Fatalf("recovered task wired to %q, want the group's new datacenter", got)
	}
}

// releaseSpy records the tasks it was asked to release and can be made to fail.
type releaseSpy struct {
	mu       sync.Mutex
	released []string
	err      error
}

func (r *releaseSpy) release(ctx context.Context, taskID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.released = append(r.released, taskID)
	return nil
}

func (r *releaseSpy) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.released...)
}

// TestDeleteTaskReleasesResources verifies deleting a task releases its
// external resources first. Proxy locks are durable and outlive the process,
// so a delete that skipped this would strand a proxy bound to a task that no
// longer exists — unleasable forever.
func TestDeleteTaskReleasesResources(t *testing.T) {
	spy := &releaseSpy{}
	svc, store, wf := newDepsService(t, WithTaskReleaser(spy.release))
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := svc.DeleteTask(ctx, task.ID()); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	if got := spy.calls(); len(got) != 1 || got[0] != task.ID() {
		t.Fatalf("released %v, want [%s]", got, task.ID())
	}
	if _, err := store.RecoverTask(ctx, task.ID()); err == nil {
		t.Fatal("record survived delete")
	}
}

// TestDeleteTaskAbortsOnReleaseFailure verifies a failed release aborts the
// deletion, keeping the record so the caller can retry. Dropping the task
// anyway would lose the only handle on the resource still held.
func TestDeleteTaskAbortsOnReleaseFailure(t *testing.T) {
	spy := &releaseSpy{err: errors.New("proxy store down")}
	svc, store, wf := newDepsService(t, WithTaskReleaser(spy.release))
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := svc.DeleteTask(ctx, task.ID()); err == nil {
		t.Fatal("expected delete to fail when release fails")
	}
	if _, err := store.RecoverTask(ctx, task.ID()); err != nil {
		t.Fatalf("record dropped despite failed release: %v", err)
	}
	if !svc.IsRunning(task.ID()) {
		// still registered: a second attempt must be possible.
		spy.mu.Lock()
		spy.err = nil
		spy.mu.Unlock()
		if err := svc.DeleteTask(ctx, task.ID()); err != nil {
			t.Fatalf("retry DeleteTask: %v", err)
		}
	}
}

// TestDeleteGroupCascades verifies deleting a task group deletes its member
// tasks — releasing each one's resources — and leaves other groups alone.
func TestDeleteGroupCascades(t *testing.T) {
	spy := &releaseSpy{}
	svc, store, wf := newDepsService(t, WithTaskReleaser(spy.release))
	ctx := context.Background()

	for _, id := range []string{"ga", "gb"} {
		if err := svc.CreateGroup(ctx, Group{ID: id}); err != nil {
			t.Fatalf("CreateGroup %s: %v", id, err)
		}
	}
	a1, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("ga"))
	a2, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("ga"))
	b1, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("gb"))

	if err := svc.DeleteGroup(ctx, "ga"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}

	for _, task := range []Task{a1, a2} {
		if _, err := store.RecoverTask(ctx, task.ID()); err == nil {
			t.Fatalf("member %s survived cascade", task.ID())
		}
	}
	if _, err := store.RecoverTask(ctx, b1.ID()); err != nil {
		t.Fatalf("cascade leaked into gb: %v", err)
	}
	if got := spy.calls(); len(got) != 2 {
		t.Fatalf("released %v, want both members", got)
	}
	if _, found, _ := store.GetGroup(ctx, "ga"); found {
		t.Fatal("group record survived delete")
	}
	if _, found, _ := store.GetGroup(ctx, "gb"); !found {
		t.Fatal("unrelated group deleted")
	}
}

// TestDeleteGroupSealsMembersAgainstMidSweepStart verifies a member cannot
// begin running once a cascade has started. Start never goes through the
// service, so without a latch a member could start between the is-it-running
// check and its own deletion — aborting the sweep with earlier members already
// destroyed and the group still standing.
func TestDeleteGroupSealsMembersAgainstMidSweepStart(t *testing.T) {
	store := newMemStore()
	wf := newBlockingWorkflow()

	var first, second Task
	var once sync.Once
	var startErr error
	release := func(ctx context.Context, id string) error {
		once.Do(func() {
			// Race the sweep: start the member that is not being released now.
			other := first
			if id == first.ID() {
				other = second
			}
			done := make(chan error, 1)
			go func() {
				_, err := other.Start(context.Background())
				done <- err
			}()
			select {
			case startErr = <-done:
			case <-time.After(2 * time.Second):
				startErr = errors.New("Start neither ran nor refused")
			}
		})
		return nil
	}

	svc := NewService(store, comms.NewBus(), WithTaskReleaser(release))
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()
	if err := svc.CreateGroup(ctx, Group{ID: "ga"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	first, _ = svc.CreateTask(ctx, wf.ID(), nil, InGroup("ga"))
	second, _ = svc.CreateTask(ctx, wf.ID(), nil, InGroup("ga"))
	defer close(wf.release)

	if err := svc.DeleteGroup(ctx, "ga"); err != nil {
		t.Fatalf("DeleteGroup: %v", err)
	}
	if !errors.Is(startErr, ErrTaskDeleted) {
		t.Fatalf("mid-sweep Start err = %v, want ErrTaskDeleted", startErr)
	}
	for _, task := range []Task{first, second} {
		if _, err := store.RecoverTask(ctx, task.ID()); err == nil {
			t.Fatalf("member %s survived the cascade", task.ID())
		}
	}
	if _, found, _ := store.GetGroup(ctx, "ga"); found {
		t.Fatal("group survived the cascade")
	}
}

// TestDeleteGroupReleaseFailureLeavesGroupWhole verifies a release failure
// mid-cascade destroys nothing. Records are irrecoverable once deleted, so a
// sweep that fails partway must leave every member — and the group — intact
// for the caller to retry, not annihilate the ones it reached first.
func TestDeleteGroupReleaseFailureLeavesGroupWhole(t *testing.T) {
	store := newMemStore()
	wf := &depsWorkflow{}

	var calls int
	release := func(ctx context.Context, id string) error {
		calls++
		if calls == 2 {
			return errors.New("proxy store down")
		}
		return nil
	}

	svc := NewService(store, comms.NewBus(), WithTaskReleaser(release))
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()
	if err := svc.CreateGroup(ctx, Group{ID: "ga"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	first, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("ga"))
	second, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("ga"))

	if err := svc.DeleteGroup(ctx, "ga"); err == nil {
		t.Fatal("expected the release failure to surface")
	}
	for _, task := range []Task{first, second} {
		if _, err := store.RecoverTask(ctx, task.ID()); err != nil {
			t.Fatalf("member %s destroyed by a failed cascade: %v", task.ID(), err)
		}
	}
	if _, found, _ := store.GetGroup(ctx, "ga"); !found {
		t.Fatal("group deleted despite the failed cascade")
	}
	// The abandoned cascade must leave its members runnable, not latched shut.
	if _, err := first.Start(ctx); err != nil {
		t.Fatalf("member left sealed after an abandoned cascade: %v", err)
	}
}

// TestStartAfterDeleteRefuses verifies a deleted task's handle is dead. Its
// record is gone, so a run would checkpoint into nothing and its terminal
// stamp would fail.
func TestStartAfterDeleteRefuses(t *testing.T) {
	svc, _, wf := newDepsService(t)
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := svc.DeleteTask(ctx, task.ID()); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}
	if _, err := task.Start(ctx); !errors.Is(err, ErrTaskDeleted) {
		t.Fatalf("Start err = %v, want ErrTaskDeleted", err)
	}
}

// TestDeleteUnknownGroupFails verifies deleting a group that does not exist is
// an error rather than a silent success, so a typo cannot read as a completed
// cleanup.
func TestDeleteUnknownGroupFails(t *testing.T) {
	svc, _, _ := newDepsService(t)
	if err := svc.DeleteGroup(context.Background(), "ghost"); err == nil {
		t.Fatal("expected an error deleting an unknown task group")
	}
}

// TestRecoverAllResolvesGroupAssignments verifies the bulk recovery path wires
// each task to its group's proxy group, the same as single recovery.
func TestRecoverAllResolvesGroupAssignments(t *testing.T) {
	svc, store, wf := newDepsService(t)
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ProxyGroupID: "residential"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	inherits, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"))
	overrides, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"), WithoutProxies())
	global, _ := svc.CreateTask(ctx, wf.ID(), nil)

	// Give every record a checkpoint so the recovered tasks are startable.
	for _, task := range []Task{inherits, overrides, global} {
		rec := store.record(t, task.ID())
		rec.Status, rec.State, rec.Snapshot = string(workflows.StatusRunning), string(s3), []byte(`{"visited":2}`)
		store.CreateTask(ctx, rec)
	}

	recovered, err := svc.RecoverAll(ctx)
	if err != nil {
		t.Fatalf("RecoverAll: %v", err)
	}
	if len(recovered) != 3 {
		t.Fatalf("recovered %d tasks, want 3", len(recovered))
	}
	for _, task := range recovered {
		startAndCapture(t, task)
	}
	if got := wf.proxyGroupOf(t, inherits.ID()); got != "residential" {
		t.Fatalf("inheriting task wired to %q, want residential", got)
	}
	if got := wf.proxyGroupOf(t, overrides.ID()); got != "" {
		t.Fatalf("opted-out task wired to %q, want none", got)
	}
	if got := wf.proxyGroupOf(t, global.ID()); got != "" {
		t.Fatalf("global-group task wired to %q, want none", got)
	}
}

// TestDeleteGroupRefusesRunningMember verifies a cascade refuses before
// deleting anything if any member is running, so a live task is never left
// half-deleted.
func TestDeleteGroupRefusesRunningMember(t *testing.T) {
	store := newMemStore()
	svc := NewService(store, comms.NewBus())
	wf := newBlockingWorkflow()
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "ga"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	running, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("ga"))
	idle, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("ga"))

	go running.Start(ctx)
	<-wf.entered

	err := svc.DeleteGroup(ctx, "ga")
	close(wf.release)
	if err == nil {
		t.Fatal("expected refusal while a member is running")
	}
	if _, err := store.RecoverTask(ctx, idle.ID()); err != nil {
		t.Fatalf("idle member deleted despite refusal: %v", err)
	}
	if _, found, _ := store.GetGroup(ctx, "ga"); !found {
		t.Fatal("group deleted despite refusal")
	}
}

// blockingWorkflow parks in its only state until released, so a test can hold
// a task in the running status.
type blockingWorkflow struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

// newBlockingWorkflow returns a blocking workflow with its channels wired.
func newBlockingWorkflow() *blockingWorkflow {
	return &blockingWorkflow{entered: make(chan struct{}), release: make(chan struct{})}
}

func (w *blockingWorkflow) ID() string                    { return "blocking" }
func (w *blockingWorkflow) ValidateInput(input any) error { return nil }

func (w *blockingWorkflow) NewInstance(input any, deps workflows.Deps) (workflows.Instance, error) {
	return &blockingInstance{wf: w}, nil
}

type blockingInstance struct{ wf *blockingWorkflow }

func (i *blockingInstance) Graph() workflows.Graph {
	return workflows.NewGraph(s1, workflows.States{
		s1: func(ctx context.Context) (*workflows.State, error) {
			i.wf.once.Do(func() { close(i.wf.entered) })
			<-i.wf.release
			return nil, nil
		},
	})
}

// TestRunningTasksReportsOnlyLiveLeases verifies the guard a proxy manager
// consults sees exactly the tasks that would be harmed by deleting a pool:
// running ones leasing from that group. Created-but-unstarted and finished
// tasks hold nothing, and other groups are none of its business.
func TestRunningTasksReportsOnlyLiveLeases(t *testing.T) {
	store := newMemStore()
	svc := NewService(store, comms.NewBus())
	blocking := newBlockingWorkflow()
	quick := &depsWorkflow{}
	if err := svc.RegisterWorkflow(blocking.ID(), blocking); err != nil {
		t.Fatalf("RegisterWorkflow blocking: %v", err)
	}
	if err := svc.RegisterWorkflow(quick.ID(), quick); err != nil {
		t.Fatalf("RegisterWorkflow quick: %v", err)
	}
	ctx := context.Background()

	live, _ := svc.CreateTask(ctx, blocking.ID(), nil, WithProxyGroup("residential"))
	idle, _ := svc.CreateTask(ctx, quick.ID(), nil, WithProxyGroup("residential"))
	other, _ := svc.CreateTask(ctx, blocking.ID(), nil, WithProxyGroup("datacenter"))
	proxyless, _ := svc.CreateTask(ctx, quick.ID(), nil)

	go live.Start(ctx)
	<-blocking.entered

	running, err := svc.RunningTasks(ctx, "residential")
	if err != nil {
		t.Fatalf("RunningTasks: %v", err)
	}
	if len(running) != 1 || running[0] != live.ID() {
		t.Fatalf("running = %v, want only the started %s, not the unstarted %s", running, live.ID(), idle.ID())
	}
	if got, _ := svc.RunningTasks(ctx, "datacenter"); len(got) != 0 {
		t.Fatalf("datacenter running = %v, want none: %s never started", got, other.ID())
	}
	if got, _ := svc.RunningTasks(ctx, ""); len(got) != 0 {
		t.Fatalf("proxyless running = %v, want none: %s leases nothing", got, proxyless.ID())
	}

	// A finished task releases its hold, so the group frees up.
	close(blocking.release)
	waitUntil(t, func() bool { return !svc.IsRunning(live.ID()) })
	if got, _ := svc.RunningTasks(ctx, "residential"); len(got) != 0 {
		t.Fatalf("running after completion = %v, want none", got)
	}
}

// waitUntil polls cond until it holds or the test times out.
func waitUntil(t *testing.T, cond func() bool) {
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

// TestRunningTasksTracksWiredGroupNotRecord verifies the guard reads the group
// a task is actually leasing from, not the one its record now names. A group
// reassigned mid-run must not make a live task look idle — the old pool is
// still in its hands.
func TestRunningTasksTracksWiredGroupNotRecord(t *testing.T) {
	store := newMemStore()
	svc := NewService(store, comms.NewBus())
	wf := newBlockingWorkflow()
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ProxyGroupID: "residential"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	task, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"))
	go task.Start(ctx)
	<-wf.entered
	defer close(wf.release)

	// Reassign the task group mid-run: the live task still holds residential.
	if err := store.SaveGroup(ctx, Group{ID: "checkout", ProxyGroupID: "datacenter"}); err != nil {
		t.Fatalf("SaveGroup: %v", err)
	}
	running, _ := svc.RunningTasks(ctx, "residential")
	if len(running) != 1 || running[0] != task.ID() {
		t.Fatalf("residential running = %v, want [%s]", running, task.ID())
	}
	if got, _ := svc.RunningTasks(ctx, "datacenter"); len(got) != 0 {
		t.Fatalf("datacenter running = %v, want none: nothing leases it yet", got)
	}
}

// TestGroupCreationRefusals verifies the global group cannot be re-created or
// deleted (it exists implicitly), duplicates are refused, and group operations
// fail loud with no repository rather than pretending to persist.
func TestGroupCreationRefusals(t *testing.T) {
	svc, _, _ := newDepsService(t)
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: GlobalGroup}); err == nil {
		t.Fatal("expected refusal creating the global group")
	}
	if err := svc.DeleteGroup(ctx, GlobalGroup); err == nil {
		t.Fatal("expected refusal deleting the global group")
	}
	if err := svc.CreateGroup(ctx, Group{}); err == nil {
		t.Fatal("expected refusal for an empty group id")
	}
	if err := svc.CreateGroup(ctx, Group{ID: "g"}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if err := svc.CreateGroup(ctx, Group{ID: "g"}); err == nil {
		t.Fatal("expected refusal for a duplicate group")
	}

	memory := NewService(nil, comms.NewBus())
	if err := memory.CreateGroup(ctx, Group{ID: "g"}); err == nil {
		t.Fatal("expected refusal creating a group with no repository")
	}
	if err := memory.DeleteGroup(ctx, "g"); err == nil {
		t.Fatal("expected refusal deleting a group with no repository")
	}
}
