// Package tasks creates and manages tasks: long-running processes that execute
// workflow graphs with durable checkpoints and recovery. Tasks are collected
// into named groups, and a task or a whole group may be assigned, per resource
// kind, the group its runs lease from — a task assigned none of a kind runs
// without that resource.
//
// A kind is just a string naming one leasing manager — each resource package
// publishes its own (proxies.Kind, accounts.Kind, payments.Kind). This package
// stores and resolves placements under it and never interprets it, so a new
// resource kind needs no change here.
package tasks

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/ntakezo/rogojin/comms"
	"github.com/ntakezo/rogojin/leasing"
	"github.com/ntakezo/rogojin/workflows"
)

// ErrNoCheckpoint is returned by Start on a recovered task that was created but
// never checkpointed. Task input is not persisted — durability begins at the
// first checkpoint — so there is nothing to resume from; the task can only be
// deleted and re-created.
var ErrNoCheckpoint = errors.New("task has no checkpoint to resume from")

// ErrTaskDeleted is returned by Start on a task the manager has deleted, or is
// in the middle of deleting. Its record is gone, so a run would checkpoint into
// nothing; the handle is dead and the task must be re-created.
var ErrTaskDeleted = errors.New("task deleted")

// ErrAlreadyStarted is returned by Start on a task whose run has already begun
// — still in flight, finished, or ended by Detach. A handle drives at most one
// run, and only that run's Start brings the record current on exit; a second
// call refuses loudly rather than reading as an instant clean completion, and
// leaves the record alone. The first Start's return is the run's outcome —
// watch it through IsRunning and LiveStatus instead.
var ErrAlreadyStarted = errors.New("task already started")

// ErrAlreadyTerminal is returned by Start on a task recovered with a terminal
// outcome (done or killed): its run is over, and re-executing the final state
// would duplicate side effects. A failed task is not terminal and may be
// retried from its last checkpoint.
var ErrAlreadyTerminal = errors.New("task already reached a terminal outcome")

// ErrDetached is returned by Start when the run was ended by Detach: the
// task's durable suspended checkpoint stands, so the task is recoverable —
// the run is over but the task is not. The handle is spent; recover a fresh
// one through the Manager to start it again.
var ErrDetached = errors.New("task detached from its suspended run")

// ErrTaskNotFound is returned by a Repository when no record exists for the
// id, so a missing task is never conflated with a store failure. Every
// adapter fails with it, which is what lets callers branch on errors.Is
// without knowing which store they run over.
var ErrTaskNotFound = errors.New("task not found")

// ErrClaimHeld is returned by ClaimTask when another node holds a live claim
// on the task: its lease has not expired, so the store still presumes that
// node is running it.
var ErrClaimHeld = errors.New("task claim held by another node")

// ErrStale is returned by a conditional write that lost: the caller's version
// is behind the store's, or the caller's node no longer owns the claim. It is
// the signal to stop side-effecting — another writer owns the task now.
var ErrStale = errors.New("stale task write")

// A Task is one task, whole: the durable record of its workflow, its
// placement (task group, plus a per-kind resource assignment), its
// last-checkpointed status and resume state with the snapshot taken there —
// and, when built by a Manager, the live run surface that starts and steers
// it. A kind absent from Assignments inherits the task group's assignment for
// that kind; see Assignment for what a stored one means. Status holds the
// lifecycle as of the last checkpoint or, once the run exits, the terminal
// outcome. The framework never deletes a record on its own.
//
// Repositories store and return the record alone — a Task built from a bare
// record has no runtime, and only a Manager attaches one. The exported fields
// are written at creation, at recovery, and by Start when the run exits, and
// belong to the goroutine driving the task; any other goroutine reads them
// through Record, which synchronizes with that exit. Suspend, Resume, Kill,
// and IsRunning are live and safe from any goroutine.
type Task struct {
	ID          string                      `json:"id"`
	WorkflowID  string                      `json:"workflowId"`
	GroupID     string                      `json:"groupId"`
	Assignments map[leasing.Kind]Assignment `json:"assignments,omitempty"`
	State       string                      `json:"state"`
	Snapshot    []byte                      `json:"snapshot,omitempty"`
	Status      string                      `json:"status"`
	// Output is the workflow's result, persisted when the run completes cleanly;
	// nil for tasks that have not finished or produce no output.
	Output    []byte    `json:"output,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`

	// Version is the record's write counter, bumped by the store on every
	// conditional write; a writer carrying an old one fails with ErrStale.
	// OwnerNode and LeaseExpiresAt are the claim: which node runs the task
	// and until when the store presumes it alive. "" and the zero time mean
	// unclaimed. All three are the store's to write — set through ClaimTask
	// and the conditional writes, never by hand.
	Version        int64     `json:"version,omitempty"`
	OwnerNode      string    `json:"ownerNode,omitempty"`
	LeaseExpiresAt time.Time `json:"leaseExpiresAt,omitzero"`

	// The live run surface, attached by the Manager and absent on a bare
	// record. input feeds a fresh run; resolved is the effective placement per
	// kind with group inheritance applied; recovered marks a task rebuilt from
	// its record, so Start resumes at State from Snapshot instead of executing
	// input from the beginning. node and ttl are the manager's claim identity
	// and lease length, which Start claims the task under.
	input     any
	resolved  map[leasing.Kind]workflows.Assignment
	engine    *engine
	recovered bool
	node      string
	ttl       time.Duration
	// recordMu guards the exported record fields once a runtime is attached:
	// Start's epilogue writes Status, Output, and UpdatedAt from whatever
	// goroutine drives the run, and Record takes its copy under the same
	// lock. It is a pointer so the value copies record() and the repositories
	// traffic in never carry a lock; a bare record has no runtime, no writer
	// to race, and no mutex.
	recordMu *sync.Mutex
}

// Repository is the persistence port the consumer implements. The store is
// the authority on ownership and liveness: claims decide which node runs a
// task, leases decide for how long the store presumes it alive, and the
// version column decides whose write wins — all against the store's own
// clock, so node clock skew never decides ownership. Implementations must
// refresh the record's UpdatedAt on every write, including checkpoints and
// terminal stamps. A nil Repository is allowed and selects purely in-memory
// operation — see NewManager.
type Repository interface {
	// CreateTask inserts the record unclaimed at version 0, dropping any
	// checkpoint or claim fields smuggled in.
	CreateTask(ctx context.Context, record Task) error
	// ClaimTask atomically takes ownership for node: it succeeds iff the
	// task is unclaimed, already owned by node, or its lease has expired,
	// and returns the claimed record carrying the new version. ErrClaimHeld
	// reports a live claim by another node. Claiming has no status
	// predicate — the manager decides what is worth claiming, usually via
	// ListClaimable.
	ClaimTask(ctx context.Context, id, node string, ttl time.Duration) (Task, error)
	// RenewClaim extends the lease iff node still owns the claim, expired
	// or not — a late renewal that nobody usurped still wins. ErrStale
	// reports the claim gone or another node's; it is how a paused node
	// discovers it was usurped. Renewal moves only the lease clock and
	// never bumps the version, so it cannot invalidate the owner's own
	// in-flight conditional writes.
	RenewClaim(ctx context.Context, id, node string, ttl time.Duration) error
	// ReleaseClaim clears the claim iff node owns it, and is silently a
	// no-op otherwise: a release racing its own usurpation is a shutdown
	// path, not an error.
	ReleaseClaim(ctx context.Context, id, node string) error
	// SaveCheckpoint writes iff version matches and node owns the claim,
	// bumps the version, and returns the new one; ErrStale reports the
	// write lost.
	SaveCheckpoint(ctx context.Context, id string, version int64, node, status, state string, snapshot []byte) (int64, error)
	// MarkTerminal is SaveCheckpoint's conditionality for the terminal
	// stamp, and additionally clears the claim: a finished task is
	// nobody's to run.
	MarkTerminal(ctx context.Context, id string, version int64, node, outcome string, output []byte) (int64, error)
	// RecordEffect stores result under (taskID, key) if no record exists,
	// and returns the stored result either way; first reports whether this
	// call created it. It is the at-most-once guard for workflow effects:
	// durable the moment the effect lands, and keyed so two runs racing
	// the same task resolve to one recorded result — the loser reads the
	// winner's bytes back instead of double-recording. The store does not
	// require the task row to exist; the manager guarantees task
	// existence, and the store stays dumb.
	RecordEffect(ctx context.Context, taskID, key string, result []byte) (stored []byte, first bool, err error)
	// ListEffects returns every effect recorded for the task, keyed by
	// effect key — what recovery seeds a rebuilt instance's effect cache
	// from.
	ListEffects(ctx context.Context, taskID string) (map[string][]byte, error)
	RecoverTask(ctx context.Context, id string) (Task, error)
	// SaveAssignment repoints a task's placement for one kind, leaving every
	// other kind and the rest of the record untouched. A nil field clears the
	// stored value.
	SaveAssignment(ctx context.Context, id string, kind leasing.Kind, assignment Assignment) error
	RecoverAll(ctx context.Context) ([]Task, error)
	// ListClaimable returns the non-terminal tasks whose claim is free for
	// the taking: unclaimed, or leased past expiry — the recovery sweep's
	// worklist.
	ListClaimable(ctx context.Context) ([]Task, error)
	// DeleteTask removes the task's record and its recorded effects;
	// absent ids are a no-op.
	DeleteTask(ctx context.Context, id string) error
	SaveGroup(ctx context.Context, group Group) error
	// GetGroup reports the group and whether it exists, so a missing group is
	// not conflated with a store failure.
	GetGroup(ctx context.Context, id string) (Group, bool, error)
	ListGroups(ctx context.Context) ([]Group, error)
	DeleteGroup(ctx context.Context, id string) error
	// TasksInGroup returns the ids of every task in the group.
	TasksInGroup(ctx context.Context, groupID string) ([]string, error)
}

// createTask validates input against the workflow and returns a new unstarted
// task whose instance leases each kind under resolved, already settled against
// the task group. The caller must not modify input or the maps after creation.
func createTask(workflow workflows.Workflow, workflowID string, input any, bus comms.Bus, repo Repository, groupID string, stored map[leasing.Kind]Assignment, resolved map[leasing.Kind]workflows.Assignment) (*Task, error) {
	if err := workflow.ValidateInput(input); err != nil {
		return nil, fmt.Errorf("task input validation error: %w", err)
	}

	now := time.Now().UTC()
	t := &Task{
		ID:          uuid.NewString(),
		WorkflowID:  workflowID,
		GroupID:     groupID,
		Assignments: stored,
		CreatedAt:   now,
		UpdatedAt:   now,
		input:       input,
		resolved:    resolved,
		recordMu:    &sync.Mutex{},
	}
	t.engine = newEngine(workflow, workflows.Deps{TaskID: t.ID, Bus: bus, Assignments: resolved}, repo)
	return t, nil
}

// rehydrateTask rebuilds an unstarted task from its persisted record so Start
// resumes at its State from its Snapshot, reporting its persisted Status until
// then.
func rehydrateTask(workflow workflows.Workflow, record Task, bus comms.Bus, repo Repository, resolved map[leasing.Kind]workflows.Assignment) *Task {
	t := record
	t.input = nil
	t.resolved = resolved
	t.engine = newEngine(workflow, workflows.Deps{TaskID: t.ID, Bus: bus, Assignments: resolved}, repo)
	t.recovered = true
	t.recordMu = &sync.Mutex{}
	return &t
}

// record returns the task's durable record alone, the run surface stripped, so
// a repository never holds live engine state. It reads the record fields
// unsynchronized; callers race Start's exit unless they hold recordMu or the
// task is not yet shared — Record is the safe surface.
func (t *Task) record() Task {
	rec := *t
	rec.input, rec.resolved, rec.engine, rec.recovered, rec.recordMu = nil, nil, nil, false, nil
	return rec
}

// Record returns a point-in-time copy of the task's durable record, the run
// surface stripped — exactly what a repository stores. Start brings the record
// current from the goroutine driving the run, so any other goroutine wanting
// the record takes its copy here, where the read synchronizes with that write,
// rather than off the exported fields directly. A task built from a bare
// record has no runtime and no writer to race, and reads as-is.
func (t *Task) Record() Task {
	if t.recordMu != nil {
		t.recordMu.Lock()
		defer t.recordMu.Unlock()
	}
	return t.record()
}

// Assignment reports the placement this task leases the kind under: the group,
// and the one resource it is pinned to within it. Both are resolved at creation
// or recovery from the task's own assignment or its task group's, and are ""
// for a kind the task leases nothing of.
func (t *Task) Assignment(kind leasing.Kind) (groupID, resourceID string) {
	a := t.resolved[kind]
	return a.GroupID, a.ResourceID
}

// seal latches the task closed for deletion, reporting false if it is live.
func (t *Task) seal() bool {
	return t.engine.seal()
}

// unseal reopens a task sealed for a deletion that was abandoned.
func (t *Task) unseal() {
	t.engine.unseal()
}

// Start executes the task synchronously until completion, error, or kill,
// returning the workflow's raw output on clean completion and bringing
// Status, Output, and UpdatedAt current on exit. The output is nil if the
// workflow produces none, or if the run errors or is killed. A run whose
// terminal stamp fails to persist returns its output alongside the error.
// A handle from Create shadows this with a Start decoded into the workflow's
// declared output type. With a repository wired, Start first claims the task
// for the manager's node — refusing with ErrClaimHeld when another node holds
// a live claim — and a recovered task resumes from the claimed record's
// checkpoint, which may be fresher than the one it was recovered with; one
// resuming a suspended checkpoint starts parked there until Resume or Kill.
// A task built from a bare record has no runtime and refuses; create or
// recover it through a Manager. A handle drives at most one run: a second
// Start — the first still in flight, finished, or detached — refuses with
// ErrAlreadyStarted and leaves the record untouched.
func (t *Task) Start(ctx context.Context) ([]byte, error) {
	if t.engine == nil {
		return nil, fmt.Errorf("task %s has no runtime: create or recover it through a Manager to start it", t.ID)
	}

	// The claim comes first: the store decides which node runs the task, and
	// a Start that loses to a live claim never touches the workflow. The
	// claimed record is fresher than a recovered handle's — another node may
	// have advanced or finished the task since recovery — so it, not the
	// rehydrated copy, is what the run resumes from.
	record, err := t.engine.claim(ctx, t.node, t.ttl)
	if err != nil {
		// A record that no longer exists is a deletion, whichever node did it.
		if errors.Is(err, ErrTaskNotFound) {
			return nil, fmt.Errorf("task %s: %w", t.ID, ErrTaskDeleted)
		}
		return nil, fmt.Errorf("task %s: claim: %w", t.ID, err)
	}

	if t.recovered {
		// The claim always bumps the version, so a strictly newer record is
		// every claim's — and its checkpoint supersedes the recovered copy.
		if record.Version > t.Version {
			t.Version = record.Version
			t.Status, t.State, t.Snapshot = record.Status, record.State, record.Snapshot
			t.UpdatedAt = record.UpdatedAt
		}
		status := workflows.Status(t.Status)
		// A refusal that runs nothing releases the claim it just took, so the
		// task is immediately claimable elsewhere instead of after the lease.
		if status == workflows.StatusNotStarted {
			return nil, errors.Join(fmt.Errorf("task %s: %w", t.ID, ErrNoCheckpoint), t.engine.releaseClaim())
		}
		if status.Terminal() {
			return nil, errors.Join(fmt.Errorf("task %s is %s: %w", t.ID, status, ErrAlreadyTerminal), t.engine.releaseClaim())
		}
		err = t.engine.Rehydrate(ctx, t.Snapshot, workflows.State(t.State), status == workflows.StatusSuspended)
	} else {
		err = t.engine.Execute(ctx, t.input)
	}

	// Another Start owns this run. Its exit — not this refusal — is what
	// brings the record current, so the epilogue must not run here: rewriting
	// Status or Output now would race the goroutine the run belongs to.
	if errors.Is(err, ErrAlreadyStarted) {
		return nil, fmt.Errorf("task %s: %w", t.ID, err)
	}

	// The run is over (or refused before starting): bring the record current so
	// the caller reads the outcome off the task itself, under the record lock
	// so a concurrent Record never reads the exit half-written. Output is
	// harvested only on clean completion, so error exits still yield nil —
	// except a failed terminal stamp, where the result exists and is returned
	// alongside the error.
	t.recordMu.Lock()
	if status := t.engine.Status(); status != workflows.StatusNotStarted {
		t.Status = string(status)
		t.UpdatedAt = time.Now().UTC()
	}
	t.Output = t.engine.Output()
	output := t.Output
	t.recordMu.Unlock()
	return output, err
}

// IsRunning reports whether the task is started and not yet terminal. A
// suspended task counts: it is parked, not finished — use LiveStatus to
// distinguish. It reads the live run, so it is safe from any goroutine.
func (t *Task) IsRunning() bool {
	return t.engine != nil && t.engine.IsRunning()
}

// LiveStatus reports the task's current lifecycle status. Once the run has
// started it reads the live engine — running, suspended, killed, failed,
// done — and is safe from any goroutine. Before the run starts, or on a task
// with no runtime, it reports the durable Status mirror (the status as of the
// last checkpoint or terminal stamp), so a recovered-but-unstarted task still
// answers with the status it will start under.
func (t *Task) LiveStatus() workflows.Status {
	if t.engine != nil {
		if s := t.engine.Status(); s != workflows.StatusNotStarted {
			return s
		}
	}
	return workflows.Status(t.Status)
}

// Suspend signals the task to park before processing the next state. It is a
// no-op unless the task is running.
func (t *Task) Suspend() error {
	if t.engine == nil {
		return errors.New("task has no runtime")
	}
	return t.engine.Suspend()
}

// Resume continues a suspended task at the next state. It is a no-op unless
// the task is suspended.
func (t *Task) Resume() error {
	if t.engine == nil {
		return errors.New("task has no runtime")
	}
	return t.engine.Resume()
}

// Detach ends a suspended run, leaving its durable suspended checkpoint
// intact so the task can be recovered and resumed later — in this or another
// process. It refuses unless the task is suspended. The blocked Start returns
// ErrDetached after Teardown runs (releasing the run's leases); afterwards
// the task is no longer live: IsRunning reports false, it may be deleted, and
// Manager.RecoverTask returns a fresh handle rehydrated from the checkpoint.
// This handle is spent — a further Start on it refuses with ErrAlreadyStarted.
func (t *Task) Detach() error {
	if t.engine == nil {
		return errors.New("task has no runtime")
	}
	return t.engine.Detach()
}

// Kill stops the task as soon as possible, cancelling the in-flight state. A
// kill before Start latches: Start then refuses with context.Canceled.
func (t *Task) Kill() error {
	if t.engine == nil {
		return errors.New("task has no runtime")
	}
	return t.engine.Kill()
}
