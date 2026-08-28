# Leasing / Tasks Redesign

**Status: implemented on this branch.** All packages build, vet, and pass under
`-race`; the checkout example runs end to end; scaffold-generated projects
compile. Deltas from the original draft are marked *(as built)* below.

Direction: `tasks` depends on `leasing` (loosely — a workflow that provisions
nothing wires no manager). `leasing` never depends on `tasks`, never calls back
into it, and asks it nothing. `leasing` is mechanism; `tasks` institutes policy.
Cut every abstraction no consumer concretely needs today.

## 1. The inversion

Today `leasing` enforces "don't delete what a running task depends on" by
interrogating the task service through `UsagePolicy` — which drags in the
`Usage` func-adapter, `tasks.Guard`, the lazy `*Service` pointer, the
snapshot/retry delete loops, and a page of lock-ordering rules. All of it exists
so leasing can ask a question about tasks.

It stops asking. Leasing already owns the two facts that matter — who holds a
live lease, who holds a durable lock — and guards deletion with those alone:

- `Delete` / `DeleteGroup` refuse with `ErrResourceInUse` / `ErrGroupInUse`
  while any live lease is held on a doomed resource. No external call, so the
  whole check sits under the manager's one mutex — the snapshot/retry machinery
  is deleted, not moved.
- Deleting an idle-but-locked resource unbinds the owner and **reports** it:
  `Delete(ctx, id) (unbound []string, err)`. What to do about an orphaned task
  (kill it, reassign it, warn) is the caller's policy, not leasing's. The
  `DeletionPolicy` port, the `Reassign`/`Unbind`/`Fail` decision enum, and
  `ErrTaskOrphaned` are cut.

Three deliberate semantic changes, all defensible:

| today | after | why |
|---|---|---|
| A suspended task's held lease does not block deletion (guard asked "running?") | Any live lease blocks deletion | The lease is the fact of use. Deleting under a suspended holder was the unsafe case, not the escape hatch. Freeing a resource = the lease being released (kill the task; teardown releases). |
| A running task merely *assigned* the group, between leases, blocks group deletion | It does not; its next acquire fails `ErrGroupNotFound` and the run fails | Assignment knowledge lives in `tasks`. If a pre-flight "who is placed here" check is ever wanted, it composes at the tasks/CLI layer from facts tasks already owns. No consumer renders such a warning today. |
| `DeletionImpact` / `GroupDeletionImpact` / `PinnedTasks` preview cost | Cut | Zero consumers. The pin lives in tasks' store; if a preview is ever needed it is a tasks-layer query. |

## 2. New `leasing` surface

Kept intact — these are the real business rules: lock > pin > rotation
precedence; pin must resolve in its group, fail-fast never silent; block while
at capacity, fail fast on empty group and on foreign locks; a lease never drops
a durable lock (`ErrPinConflict` resolved only by deliberate reassignment);
store-first write ordering; per-group strategy instances learning from
release outcomes.

```go
// Construction: repo required, everything else optional.
func NewManager[T any](ctx context.Context, repo Repository[T], opts ...Option[T]) (*Manager[T], error)

// The only option. Round-robin is always installed and is always the
// default; a group naming no strategy rotates round-robin. Registering a
// factory under the round-robin name overrides the built-in. Cut: Config
// struct, DefaultStrategy, Noun, Policy, Usage.
func WithStrategy[T any](name string, f StrategyFactory[T]) Option[T]

// Acquisition (unchanged semantics)
func (m *Manager[T]) Acquire(ctx context.Context, a Assignment) (*Lease[T], error)
func (m *Manager[T]) Lock(ctx context.Context, a Assignment) (*Lease[T], error)
func (m *Manager[T]) Unlock(ctx context.Context, taskID string) error
func (m *Manager[T]) ReleaseStaleLock(ctx context.Context, a Assignment) error
func (m *Manager[T]) CheckAssignment(a Assignment) error

// Pool CRUD. Deletes guard on live leases only, and report unbound owners.
func (m *Manager[T]) Add(ctx context.Context, r Resource[T]) error
func (m *Manager[T]) Delete(ctx context.Context, id string) (unbound []string, err error)
func (m *Manager[T]) CreateGroup(ctx context.Context, g Group) error
func (m *Manager[T]) DeleteGroup(ctx context.Context, id string) (unbound []string, err error)
```

`MaxHolders` gets one home: the resource (0 → 1, `UnlimitedHolders` lifts it).
The group-level default and the three-step inheritance are cut — no consumer
sets either level today, and one place to look beats a resolution chain. `Group`
keeps only `ID` + `Strategy` (+ timestamps).

Errors kept: `ErrNoResources`, `ErrGroupNotFound`, `ErrResourceInUse`,
`ErrGroupInUse`, `ErrResourceNotFound`, `ErrResourceNotInGroup`,
`ErrResourceLocked`, `ErrPinConflict`. Cut: `ErrTaskOrphaned`.

`Repository[T]` is unchanged: a dumb store of resources and groups.

## 3. New `tasks` surface

`tasks` names the contract it needs — Go-style, consumer-defined — and any
`*leasing.Manager[T]` satisfies it structurally:

```go
// ResourceManager is the lock surface tasks drives per registered kind:
// Unlock when a task is deleted, ReleaseStaleLock when one is reassigned.
// Every *leasing.Manager[T] satisfies it.
type ResourceManager interface {
	Unlock(ctx context.Context, taskID string) error
	ReleaseStaleLock(ctx context.Context, a leasing.Assignment) error
}

// The whole wiring for one kind. Replaces WithResource(kind, unlock, stale)
// + WithUsagePolicy(NewGuard(&svc, kind)) + the var-before-use dance.
func WithResource(kind string, m ResourceManager) ServiceOption
```

Policies `tasks` institutes (unchanged in substance, now clearly its own):

- Deleting a task releases every registered kind's lock; all attempted even
  when one fails. Group cascade seals members first, releases before removing.
- `AssignResource` writes the placement, then drops the stale lock —
  reassignment, never a lease, is what outranks a lock.
- Placement resolution (task overrides task-group, `""` is explicit none),
  seal-before-delete, recovery semantics: untouched.
- Since leasing never re-enters tasks, the manager calls are now safe under the
  registry lock — the deadlock documentation and its contortions go away.

Cut from `tasks`: `Guard`/`NewGuard`, `Service.RunningTasks` /
`TaskIsRunning` / `PinnedTasks` (existed only to answer leasing),
`ReleaseFunc`, `StaleLockFunc`, `WithTaskReleaser` (no consumer),
`Repository.TasksPinnedTo` (the repository port shrinks by one method).
Engine, checkpointing, groups, create options: untouched.

## 4. Kind packages collapse

`proxies` / `accounts` / `cards` stop re-exporting the entire leasing API
through rename adapters (~550 lines each). Each becomes a payload type, its
strategies, and a constructor — aliases, not wrappers:

```go
package proxies

type Attrs struct {
	URL string `json:"url"`
}

type Proxy = leasing.Resource[Attrs]
type Group = leasing.Group
type Repository = leasing.Repository[Attrs]
type Manager = leasing.Manager[Attrs]

const StrategyBayesian = "bayesian"

func NewManager(ctx context.Context, repo Repository) (*Manager, error) {
	return leasing.NewManager(ctx, repo,
		leasing.WithStrategy(StrategyBayesian, func() leasing.Selection[Attrs] { return NewBayesian() }))
}
```

`accounts` and `cards` are the same shape with `json.RawMessage` payloads
(`Account = leasing.Resource[json.RawMessage]`), each keeping its `Bind[F]`
decoder. Gone per package: repoAdapter, policyAdapter, selectionAdapter,
coreFactory, toResource/fromResource, toAssignment, wrapLease, the Lease
wrapper, and every re-exported error/const/doc block. Workflows read
`lease.Resource().Attrs.URL` instead of `lease.Proxy().URL`, and
`Assignment.ProxyID/AccountID/CardID` become `Assignment.ResourceID`.

*(As built)* one loosening to know about: the old accounts/cards wrappers
cloned the JSON payload at every boundary so a holder could not mutate the
pool's copy through the shared backing array. Aliases cannot intercept, so the
contract is documented instead: treat a leased `Attrs` as read-only.

Persistence (`proxysqlite` etc.) implements `leasing.Repository[Attrs]`
directly — the schema↔record translation lives in the store, the one place
that already knows the columns.

## 5. Consumer wiring, before and after

```go
// before: forward-declare, guard, three-part registration per kind
var svc tasks.Service
mgr, _ := proxies.NewManager(ctx, repo, nil,
	proxies.WithUsagePolicy(tasks.NewGuard(&svc, "proxy")))
svc = tasks.NewService(taskRepo, bus,
	tasks.WithResource("proxy", mgr.Unlock, proxies.StaleLockReleaser(mgr)))

// after
mgr, _ := proxies.NewManager(ctx, repo)
svc := tasks.NewService(taskRepo, bus,
	tasks.WithResource("proxy", mgr))
```

A workflow that provisions nothing registers no kinds and never touches
leasing — the dependency is opt-in per deployment, exactly as today, minus the
ceremony.

## 6. Who enforces what

| rule | owner |
|---|---|
| lock > pin > rotation; pin validation; holder caps; block-vs-fail | leasing |
| refuse delete while leased; report unbound owners | leasing |
| when locks are released (task delete, reassignment) | tasks |
| placement resolution and inheritance | tasks |
| seal-before-delete, cascade ordering, recovery | tasks |
| fate of an orphaned task | caller (CLI / operator), informed by `unbound` |

## 7. Migration

- **Durable schemas** *(as built)*: resources unchanged (keep `max_holders`);
  group `max_holders` columns stay but are no longer read or written (the
  migration ledger is append-only, so the column is documented legacy rather
  than dropped); `account_groups` and `card_groups` gain a `strategy` column
  via one new migration each — existing rows read as `''`, which resolves to
  round robin, so upgraded databases behave identically; task records
  unchanged; strategy names unchanged (`roundrobin`, `bayesian`);
  `tasksqlite` loses the `TasksPinnedTo` query.
- **Compile-time breaks**: kind-package types become aliases (call-site changes
  like `lease.Proxy()` → `lease.Resource()`, `p.URL` → `p.Attrs.URL`,
  `a.Fields` → `a.Attrs`); `Delete`/`DeleteGroup` gain the `unbound` return;
  removed symbols listed in §2–§3.
- **Behavioral deltas**: the three rows in §1's table.

Size as landed: leasing 1,355 → ~880 lines; the three kind packages ~1,650 →
~230; tasks sheds ~250; the guard/spy test scaffolding went with them. Every
deleted line was interface, adapter, or retry machinery — not behavior.
