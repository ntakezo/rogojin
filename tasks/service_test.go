package tasks

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/workflows"
)

// The resource kinds these tests place tasks under. This package stores kind
// strings and never interprets them, so what a consumer calls its leasing
// managers is exactly what appears here.
const (
	proxyKind   = "proxy"
	accountKind = "account"
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

// SaveAssignment writes one kind and copies the rest, the same per-kind
// isolation the SQLite store gets from json_set.
func (m *memStore) SaveAssignment(ctx context.Context, id string, kind string, a Assignment) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.records[id]
	if !ok {
		return errors.New("not found")
	}
	assignments := make(map[string]Assignment, len(rec.Assignments)+1)
	for k, v := range rec.Assignments {
		assignments[k] = v
	}
	assignments[kind] = a
	rec.Assignments = assignments
	m.records[id] = rec
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
// test can assert which placement a task was actually wired with.
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

// wiredWith returns the kind's placement the instance built for taskID was
// wired with, failing the test if no instance was built for it.
func (w *depsWorkflow) wiredWith(t *testing.T, taskID, kind string) workflows.Assignment {
	t.Helper()
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, d := range w.seen {
		if d.TaskID == taskID {
			return d.Assignments[kind]
		}
	}
	t.Fatalf("no instance built for task %s", taskID)
	return workflows.Assignment{}
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

// TestUngroupedTaskLandsInGlobalAndLeasesNothing verifies a task created with
// no options is placed in the global group and, because nothing assigns it a
// resource group of any kind, leases none — the documented default.
func TestUngroupedTaskLandsInGlobalAndLeasesNothing(t *testing.T) {
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
	if len(rec.Assignments) != 0 {
		t.Fatalf("Assignments = %v, want none (inherit every kind)", rec.Assignments)
	}
	if rec.CreatedAt.IsZero() || rec.UpdatedAt.IsZero() {
		t.Fatal("timestamps not stamped at creation")
	}
	if got := wf.wiredWith(t, task.ID(), proxyKind); got != (workflows.Assignment{}) {
		t.Fatalf("wired proxy placement = %+v, want none", got)
	}
}

// TestTaskInheritsGroupResourceGroups verifies a task in a group leases from
// every kind that group assigns, which is the point of assigning them to a
// group: one placement decision covers every member and every kind at once.
func TestTaskInheritsGroupResourceGroups(t *testing.T) {
	svc, _, wf := newDepsService(t)
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ResourceGroups: map[string]string{
		proxyKind:   "residential",
		accountKind: "shoppers",
	}}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	task, err := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	startAndCapture(t, task)

	if got := wf.wiredWith(t, task.ID(), proxyKind).GroupID; got != "residential" {
		t.Fatalf("wired proxy group = %q, want residential", got)
	}
	if got := wf.wiredWith(t, task.ID(), accountKind).GroupID; got != "shoppers" {
		t.Fatalf("wired account group = %q, want shoppers", got)
	}
}

// TestAssignmentOverridesGroupOneKindAtATime verifies each kind resolves on its
// own: overriding one, and opting out of another, leaves every other kind still
// inheriting. Placement per kind is the whole reason this is a map rather than
// a pair of fields.
func TestAssignmentOverridesGroupOneKindAtATime(t *testing.T) {
	svc, store, wf := newDepsService(t)
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ResourceGroups: map[string]string{
		proxyKind:   "residential",
		accountKind: "shoppers",
	}}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}

	override, err := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"), WithResourceGroup(proxyKind, "datacenter"))
	if err != nil {
		t.Fatalf("CreateTask override: %v", err)
	}
	startAndCapture(t, override)
	if got := wf.wiredWith(t, override.ID(), proxyKind).GroupID; got != "datacenter" {
		t.Fatalf("override wired to proxy group %q, want datacenter", got)
	}
	if got := wf.wiredWith(t, override.ID(), accountKind).GroupID; got != "shoppers" {
		t.Fatalf("override wired to account group %q, want the inherited shoppers", got)
	}

	optedOut, err := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"), Without(proxyKind))
	if err != nil {
		t.Fatalf("CreateTask opted out: %v", err)
	}
	startAndCapture(t, optedOut)
	if got := wf.wiredWith(t, optedOut.ID(), proxyKind).GroupID; got != "" {
		t.Fatalf("opted-out task wired to proxy group %q, want none", got)
	}
	if got := wf.wiredWith(t, optedOut.ID(), accountKind).GroupID; got != "shoppers" {
		t.Fatalf("opting out of proxies also dropped the account group %q", got)
	}
	// The opt-out must persist as an explicit empty assignment, not as nil:
	// nil would inherit the group's proxies again on recovery.
	rec := store.record(t, optedOut.ID())
	if groupID := rec.Assignments[proxyKind].GroupID; groupID == nil || *groupID != "" {
		t.Fatalf("persisted proxy group = %v, want explicit empty", groupID)
	}
}

// TestKindsDoNotCollide verifies a kind resolves only against its own
// namespace. Two managers may name their groups alike — "residential" proxies
// and "residential" accounts are unrelated pools — and a task assigned one must
// never come out wired to the other.
func TestKindsDoNotCollide(t *testing.T) {
	svc, _, wf := newDepsService(t)
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil, WithResourceGroup(accountKind, "residential"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	startAndCapture(t, task)

	if got := wf.wiredWith(t, task.ID(), accountKind).GroupID; got != "residential" {
		t.Fatalf("wired account group = %q, want residential", got)
	}
	if got := wf.wiredWith(t, task.ID(), proxyKind).GroupID; got != "" {
		t.Fatalf("wired proxy group = %q, want none: the name belongs to accounts", got)
	}
}

// TestResourceGroupSharedAcrossManyOwners verifies one resource group can back
// several task groups and several individually-assigned tasks at once. The
// assignment is a reference, not ownership: nothing about naming a resource
// group may make it exclusive to the first task group that claimed it.
func TestResourceGroupSharedAcrossManyOwners(t *testing.T) {
	svc, _, wf := newDepsService(t)
	ctx := context.Background()

	for _, id := range []string{"checkout", "restock"} {
		if err := svc.CreateGroup(ctx, Group{ID: id, ResourceGroups: map[string]string{proxyKind: "residential"}}); err != nil {
			t.Fatalf("CreateGroup %s: %v", id, err)
		}
	}

	created := map[string]Task{}
	for name, opts := range map[string][]CreateOption{
		"viaCheckout":   {InGroup("checkout")},
		"viaRestock":    {InGroup("restock")},
		"direct":        {WithResourceGroup(proxyKind, "residential")},
		"directInGroup": {InGroup("restock"), WithResourceGroup(proxyKind, "residential")},
	} {
		task, err := svc.CreateTask(ctx, wf.ID(), nil, opts...)
		if err != nil {
			t.Fatalf("CreateTask %s: %v", name, err)
		}
		startAndCapture(t, task)
		created[name] = task
	}

	for name, task := range created {
		if got := wf.wiredWith(t, task.ID(), proxyKind).GroupID; got != "residential" {
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

// TestRecoveredTaskResolvesInheritedResourceGroups verifies recovery
// re-resolves every inherited kind from the task's group, so a group reassigned
// while the task was offline takes effect on the next run.
func TestRecoveredTaskResolvesInheritedResourceGroups(t *testing.T) {
	svc, store, wf := newDepsService(t)
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ResourceGroups: map[string]string{
		proxyKind:   "residential",
		accountKind: "shoppers",
	}}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	task, err := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Reassign the group, then recover: the record stores no assignment of its
	// own for either kind, so both new assignments must apply.
	if err := store.SaveGroup(ctx, Group{ID: "checkout", ResourceGroups: map[string]string{
		proxyKind:   "datacenter",
		accountKind: "resellers",
	}}); err != nil {
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

	if got := wf.wiredWith(t, task.ID(), proxyKind).GroupID; got != "datacenter" {
		t.Fatalf("recovered task wired to proxy group %q, want the group's new datacenter", got)
	}
	if got := wf.wiredWith(t, task.ID(), accountKind).GroupID; got != "resellers" {
		t.Fatalf("recovered task wired to account group %q, want the group's new resellers", got)
	}
}

// fakeManager implements ResourceManager, recording every unlock and
// stale-lock release it is asked for and optionally failing them. It stands in
// for a leasing manager, whose real implementations these calls come off
// structurally.
type fakeManager struct {
	mu        sync.Mutex
	unlocked  []string
	stales    []leasing.Assignment
	unlockErr error
	staleErr  error
	// onUnlock, when set, runs inside Unlock so a test can interpose on the
	// release moment of a deletion or a cascade.
	onUnlock func(taskID string)
}

func (f *fakeManager) Unlock(ctx context.Context, taskID string) error {
	f.mu.Lock()
	hook := f.onUnlock
	f.mu.Unlock()
	if hook != nil {
		hook(taskID)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.unlockErr != nil {
		return f.unlockErr
	}
	f.unlocked = append(f.unlocked, taskID)
	return nil
}

func (f *fakeManager) ReleaseStaleLock(ctx context.Context, a leasing.Assignment) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.staleErr != nil {
		return f.staleErr
	}
	f.stales = append(f.stales, a)
	return nil
}

func (f *fakeManager) unlocks() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.unlocked...)
}

func (f *fakeManager) staleCalls() []leasing.Assignment {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]leasing.Assignment(nil), f.stales...)
}

func (f *fakeManager) setUnlockErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unlockErr = err
}

// TestDeleteTaskReleasesResources verifies deleting a task releases its
// external resources first. Leasing locks are durable and outlive the process,
// so a delete that skipped this would strand a resource bound to a task that no
// longer exists — unleasable forever.
func TestDeleteTaskReleasesResources(t *testing.T) {
	manager := &fakeManager{}
	svc, store, wf := newDepsService(t, WithResource(proxyKind, manager))
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := svc.DeleteTask(ctx, task.ID()); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	if got := manager.unlocks(); len(got) != 1 || got[0] != task.ID() {
		t.Fatalf("unlocked %v, want [%s]", got, task.ID())
	}
	if _, err := store.RecoverTask(ctx, task.ID()); err == nil {
		t.Fatal("record survived delete")
	}
}

// TestDeleteTaskAbortsOnReleaseFailure verifies a failed release aborts the
// deletion, keeping the record so the caller can retry. Dropping the task
// anyway would lose the only handle on the resource still held.
func TestDeleteTaskAbortsOnReleaseFailure(t *testing.T) {
	manager := &fakeManager{unlockErr: errors.New("proxy store down")}
	svc, store, wf := newDepsService(t, WithResource(proxyKind, manager))
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
		manager.setUnlockErr(nil)
		if err := svc.DeleteTask(ctx, task.ID()); err != nil {
			t.Fatalf("retry DeleteTask: %v", err)
		}
	}
}

// TestDeleteGroupCascades verifies deleting a task group deletes its member
// tasks — releasing each one's resources — and leaves other groups alone.
func TestDeleteGroupCascades(t *testing.T) {
	manager := &fakeManager{}
	svc, store, wf := newDepsService(t, WithResource(proxyKind, manager))
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
	if got := manager.unlocks(); len(got) != 2 {
		t.Fatalf("unlocked %v, want both members", got)
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
	manager := &fakeManager{}
	manager.onUnlock = func(id string) {
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
	}

	svc := NewService(store, comms.NewBus(), WithResource(proxyKind, manager))
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
	manager := &fakeManager{}
	manager.onUnlock = func(id string) {
		calls++
		if calls == 2 {
			manager.setUnlockErr(errors.New("proxy store down"))
		}
	}

	svc := NewService(store, comms.NewBus(), WithResource(proxyKind, manager))
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
// each task to its group's resource groups, the same as single recovery.
func TestRecoverAllResolvesGroupAssignments(t *testing.T) {
	svc, store, wf := newDepsService(t)
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ResourceGroups: map[string]string{proxyKind: "residential"}}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	inherits, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"))
	overrides, _ := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"), Without(proxyKind))
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
	if got := wf.wiredWith(t, inherits.ID(), proxyKind).GroupID; got != "residential" {
		t.Fatalf("inheriting task wired to %q, want residential", got)
	}
	if got := wf.wiredWith(t, overrides.ID(), proxyKind).GroupID; got != "" {
		t.Fatalf("opted-out task wired to %q, want none", got)
	}
	if got := wf.wiredWith(t, global.ID(), proxyKind).GroupID; got != "" {
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

// TestIsRunningReadsSuspendedAsLive verifies a suspended task still counts as
// running for the service: it is parked, not finished, and a deletion must
// still refuse it.
func TestIsRunningReadsSuspendedAsLive(t *testing.T) {
	store := newMemStore()
	svc := NewService(store, comms.NewBus())
	var log []workflows.State
	wf := &gatedWorkflow{log: &log, entered: make(chan struct{}), release: make(chan struct{})}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	go task.Start(ctx)
	<-wf.entered

	// Suspend parks the task at the next state boundary.
	if err := task.Suspend(); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	close(wf.release)
	waitUntil(t, func() bool { return task.Status() == workflows.StatusSuspended })

	if !svc.IsRunning(task.ID()) {
		t.Fatal("IsRunning = false while suspended: parked is not finished")
	}
	if err := svc.DeleteTask(ctx, task.ID()); err == nil {
		t.Fatal("expected a suspended task to refuse deletion")
	}

	if err := task.Resume(); err != nil {
		t.Fatalf("Resume: %v", err)
	}
	waitUntil(t, func() bool { return !svc.IsRunning(task.ID()) })
}

// TestPinIsDurablePlacementThroughRecovery verifies a pin is durable placement,
// not a runtime accident: it lands on the record, reaches the workflow through
// Deps, and comes back on recovery alongside the group it narrows. Two kinds are
// pinned at once, since each is stored under its own key.
func TestPinIsDurablePlacementThroughRecovery(t *testing.T) {
	store := newMemStore()
	svc := NewService(store, comms.NewBus())
	wf := &depsWorkflow{}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil,
		WithResourceGroup(proxyKind, "residential"), WithPin(proxyKind, "p2"),
		WithResourceGroup(accountKind, "shoppers"), WithPin(accountKind, "buyer-1"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	assertPlacement(t, task, proxyKind, "residential", "p2")
	assertPlacement(t, task, accountKind, "shoppers", "buyer-1")

	record, err := store.RecoverTask(ctx, task.ID())
	if err != nil {
		t.Fatalf("RecoverTask record: %v", err)
	}
	if pin := record.Assignments[proxyKind].ResourceID; pin == nil || *pin != "p2" {
		t.Fatalf("record proxy pin = %v, want p2", pin)
	}
	if pin := record.Assignments[accountKind].ResourceID; pin == nil || *pin != "buyer-1" {
		t.Fatalf("record account pin = %v, want buyer-1", pin)
	}

	// Both pins survive the trip through the store.
	if err := store.SaveCheckpoint(ctx, task.ID(), "running", "s1", nil); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	recovered, err := svc.RecoverTask(ctx, task.ID())
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	assertPlacement(t, recovered, proxyKind, "residential", "p2")
	assertPlacement(t, recovered, accountKind, "shoppers", "buyer-1")
}

// assertPlacement fails the test unless the task resolves the kind to exactly
// this group and pin.
func assertPlacement(t *testing.T, task Task, kind, wantGroup, wantResource string) {
	t.Helper()
	groupID, resourceID := task.Assignment(kind)
	if groupID != wantGroup || resourceID != wantResource {
		t.Fatalf("%s placement = %q/%q, want %q/%q", kind, groupID, resourceID, wantGroup, wantResource)
	}
}

// TestWithoutClearsThePin verifies opting a kind out clears its pin too, so a
// group default cannot leave a stray resource assigned — and that it clears only
// that kind.
func TestWithoutClearsThePin(t *testing.T) {
	store := newMemStore()
	svc := NewService(store, comms.NewBus())
	wf := &depsWorkflow{}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil,
		WithPin(proxyKind, "p2"), Without(proxyKind),
		WithResourceGroup(accountKind, "shoppers"), WithPin(accountKind, "buyer-1"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	assertPlacement(t, task, proxyKind, "", "")
	assertPlacement(t, task, accountKind, "shoppers", "buyer-1")
}

// TestAssignResourceRepointsAndReleasesStaleLock verifies assignment is the
// deliberate act that outranks a durable lock: it writes the new placement and
// hands the kind's manager the placement as resolved, so the lock the task no
// longer fits is dropped by the reassignment rather than by a lease.
func TestAssignResourceRepointsAndReleasesStaleLock(t *testing.T) {
	store := newMemStore()
	proxyMgr, accountMgr := &fakeManager{}, &fakeManager{}
	svc := NewService(store, comms.NewBus(),
		WithResource(proxyKind, proxyMgr),
		WithResource(accountKind, accountMgr))
	wf := &depsWorkflow{}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil, WithResourceGroup(proxyKind, "residential"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	group, pin := "datacenter", "p7"
	if err := svc.AssignResource(ctx, task.ID(), proxyKind, Assignment{GroupID: &group, ResourceID: &pin}); err != nil {
		t.Fatalf("AssignResource: %v", err)
	}

	record, _ := store.RecoverTask(ctx, task.ID())
	if groupID := record.Assignments[proxyKind].GroupID; groupID == nil || *groupID != "datacenter" {
		t.Fatalf("record group = %v, want datacenter", groupID)
	}
	if resourceID := record.Assignments[proxyKind].ResourceID; resourceID == nil || *resourceID != "p7" {
		t.Fatalf("record pin = %v, want p7", resourceID)
	}
	want := leasing.Assignment{TaskID: task.ID(), GroupID: "datacenter", ResourceID: "p7"}
	if got := proxyMgr.staleCalls(); len(got) != 1 || got[0] != want {
		t.Fatalf("proxy manager told %v, want [%v]", got, want)
	}
	if got := accountMgr.staleCalls(); len(got) != 0 {
		t.Fatalf("account manager told %v, want nothing: the move was proxies'", got)
	}

	// The live handle keeps the placement it was wired with; the move lands on
	// the next recovery.
	assertPlacement(t, task, proxyKind, "residential", "")
	if err := store.SaveCheckpoint(ctx, task.ID(), "running", "s1", nil); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	recovered, err := svc.RecoverTask(ctx, task.ID())
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	assertPlacement(t, recovered, proxyKind, "datacenter", "p7")
}

// TestAssignResourceLeavesOtherKindsAlone verifies repointing one kind is not a
// rewrite of the whole placement. A task moved to another proxy group must come
// back on the same account it was halfway through a checkout as.
func TestAssignResourceLeavesOtherKindsAlone(t *testing.T) {
	store := newMemStore()
	proxyMgr, accountMgr := &fakeManager{}, &fakeManager{}
	svc := NewService(store, comms.NewBus(),
		WithResource(proxyKind, proxyMgr),
		WithResource(accountKind, accountMgr))
	wf := &depsWorkflow{}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil,
		WithResourceGroup(proxyKind, "residential"),
		WithResourceGroup(accountKind, "shoppers"), WithPin(accountKind, "buyer-1"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	group := "datacenter"
	if err := svc.AssignResource(ctx, task.ID(), proxyKind, Assignment{GroupID: &group}); err != nil {
		t.Fatalf("AssignResource: %v", err)
	}

	if err := store.SaveCheckpoint(ctx, task.ID(), "running", "s1", nil); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	recovered, err := svc.RecoverTask(ctx, task.ID())
	if err != nil {
		t.Fatalf("RecoverTask: %v", err)
	}
	assertPlacement(t, recovered, proxyKind, "datacenter", "")
	assertPlacement(t, recovered, accountKind, "shoppers", "buyer-1")

	// Only the proxy manager is asked to drop a lock; the account lock stands.
	want := leasing.Assignment{TaskID: task.ID(), GroupID: "datacenter"}
	if got := proxyMgr.staleCalls(); len(got) != 1 || got[0] != want {
		t.Fatalf("proxy manager told %v, want [%v]", got, want)
	}
	if got := accountMgr.staleCalls(); len(got) != 0 {
		t.Fatalf("account manager told %v, want nothing", got)
	}
}

// TestAssignResourceInheritsGroupForTheReleaser verifies the manager is told
// the placement the task will actually lease from, not the nil that was stored:
// a cleared assignment inherits the task group's, and a lock is stale or not
// against that resolved group.
func TestAssignResourceInheritsGroupForTheReleaser(t *testing.T) {
	store := newMemStore()
	proxyMgr := &fakeManager{}
	svc := NewService(store, comms.NewBus(), WithResource(proxyKind, proxyMgr))
	wf := &depsWorkflow{}
	if err := svc.RegisterWorkflow(wf.ID(), wf); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ctx := context.Background()

	if err := svc.CreateGroup(ctx, Group{ID: "checkout", ResourceGroups: map[string]string{proxyKind: "residential"}}); err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	task, err := svc.CreateTask(ctx, wf.ID(), nil, InGroup("checkout"), WithResourceGroup(proxyKind, "datacenter"))
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}

	// Clearing the task's own assignment falls back to the task group's.
	if err := svc.AssignResource(ctx, task.ID(), proxyKind, Assignment{}); err != nil {
		t.Fatalf("AssignResource: %v", err)
	}
	if got := proxyMgr.staleCalls(); len(got) != 1 || got[0].GroupID != "residential" {
		t.Fatalf("manager told %v, want the inherited residential", got)
	}
}

// TestDeleteUnlocksEveryRegisteredKind verifies a deletion frees every kind's
// durable lock, not just the first. A lock nothing releases is unleasable
// forever, so a task holding a proxy and an account must give back both.
func TestDeleteUnlocksEveryRegisteredKind(t *testing.T) {
	proxyMgr, accountMgr := &fakeManager{}, &fakeManager{}
	svc, _, wf := newDepsService(t,
		WithResource(proxyKind, proxyMgr),
		WithResource(accountKind, accountMgr))
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	if err := svc.DeleteTask(ctx, task.ID()); err != nil {
		t.Fatalf("DeleteTask: %v", err)
	}

	for kind, mgr := range map[string]*fakeManager{proxyKind: proxyMgr, accountKind: accountMgr} {
		if got := mgr.unlocks(); len(got) != 1 || got[0] != task.ID() {
			t.Fatalf("%s unlocked %v, want [%s]", kind, got, task.ID())
		}
	}
}

// TestReleaseAttemptsEveryKindDespiteAFailure verifies one broken manager does
// not strand the locks the others would have freed. Every unlock is attempted,
// the error names the kind that failed, and the deletion still aborts — a task
// whose locks are only partly freed must not lose the record that says so.
func TestReleaseAttemptsEveryKindDespiteAFailure(t *testing.T) {
	proxyMgr := &fakeManager{unlockErr: errors.New("store down")}
	accountMgr := &fakeManager{}
	svc, store, wf := newDepsService(t,
		WithResource(proxyKind, proxyMgr),
		WithResource(accountKind, accountMgr))
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	err = svc.DeleteTask(ctx, task.ID())
	if err == nil {
		t.Fatal("DeleteTask succeeded, want the proxy unlock failure surfaced")
	}
	if !strings.Contains(err.Error(), proxyKind) {
		t.Fatalf("error = %v, want the failing kind named", err)
	}
	if got := accountMgr.unlocks(); len(got) != 1 {
		t.Fatalf("account unlocked %v, want attempted despite the proxy failure", got)
	}
	if _, err := store.RecoverTask(ctx, task.ID()); err != nil {
		t.Fatalf("record gone after an aborted deletion: %v", err)
	}
}

// TestRegisteringAKindTwicePanics verifies the one wiring mistake WithResource
// can detect. Two registrations of a kind could only unlock it twice, and the
// second unlock would land on whatever task took the resource next.
func TestRegisteringAKindTwicePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic: a kind registered twice would be unlocked twice")
		}
	}()
	NewService(newMemStore(), comms.NewBus(),
		WithResource(proxyKind, &fakeManager{}),
		WithResource(proxyKind, &fakeManager{}))
}

// TestRegisteringANilManagerPanics verifies wiring a kind with nothing behind
// it fails loud at construction: a nil manager could never free a lock.
func TestRegisteringANilManagerPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic: a nil manager can never unlock anything")
		}
	}()
	NewService(newMemStore(), comms.NewBus(), WithResource(proxyKind, nil))
}

// TestAssignResourceWithoutARegisteredKind verifies placement still lands for a
// kind no manager is wired for. The record is the service's to write; releasing
// a lock is the manager's, and there is no manager here to hold one.
func TestAssignResourceWithoutARegisteredKind(t *testing.T) {
	svc, store, wf := newDepsService(t)
	ctx := context.Background()

	task, err := svc.CreateTask(ctx, wf.ID(), nil)
	if err != nil {
		t.Fatalf("CreateTask: %v", err)
	}
	group := "residential"
	if err := svc.AssignResource(ctx, task.ID(), proxyKind, Assignment{GroupID: &group}); err != nil {
		t.Fatalf("AssignResource: %v", err)
	}
	record := store.record(t, task.ID())
	if stored := record.Assignments[proxyKind].GroupID; stored == nil || *stored != "residential" {
		t.Fatalf("stored group = %v, want residential", stored)
	}
}
