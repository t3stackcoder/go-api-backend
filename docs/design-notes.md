# Design notes: how the implementation maps to spec.md

These notes record the decisions that resolve Go import constraints the spec
does not spell out, the exact package contracts every package codes against,
and the places where the implementation deliberately deviates from spec.md.
Where spec.md and these notes disagree, spec.md wins unless the deviation is
listed in section 6 below.

## 1. Import graph

Arrows point from importer to imported. No cycles.

```
retry, ratelimit, authz, testkit           (leaves; import nothing of ours)
        ^
mediator (core)  -> authz, retry, ratelimit, google/uuid, otel api
        ^
validate         -> mediator (for ValidationError, IsMarker)
httpapi          -> mediator, authz            (Route type lives here)
openapi          -> mediator, validate, httpapi (Operation type lives here)
pg               -> mediator, testkit, pgx
redisx           -> mediator, pg, testkit, go-redis   (implements pg.StreamSink, pg.StreamTrimmer)
behavior         -> mediator, validate, pg, redisx, otel
testkit/memstore -> pg                          (in-memory pg.Store)
examples, cmd, test/* -> everything
```

The core detects traits whose types live in leaf packages (`authz.Requirement`,
`retry.Policy`, `ratelimit.Policy`) and records them in `mediator.Traits`.
Traits whose types live in packages that import the core (`pg.TxOptions`,
`httpapi.Route`, `openapi.Operation`) are detected by those packages with
`RequestInfo.Implements(reflect.TypeFor[pg.TxOptioner]())` at Build.

## 2. Core contracts (package mediator, already implemented)

* `Kind` has five values: command, query, stream, notification, consumer.
  `Use` scopes default to the three request kinds; `Notifications()` and
  `Consumers()` add the two event paths; `Everywhere()` selects all.
* `Behavior.Handle(ctx, req any, info *RequestInfo, next Next)`. On the
  notification and consumer paths `req` is the event value and the result is
  always nil. `info.Group` is set on the consumer path.
* `StreamBehavior` adds `HandleStream(ctx, req, info, next StreamNext) iter.Seq2[any, error]`.
  Any behavior that derives a cancellable context (Timeout, UnitOfWork,
  Tracing, Logging, Metrics, Recovery) must implement it, because a plain
  `Handle` returns before the sequence is iterated.
* `Preparer.Prepare(infos []*RequestInfo) error` runs at Build for behaviors
  that validate configuration (Validation compiles tags, Authorization
  enforces RequireAuthByDefault, Retry checks policies). Errors are joined.
* `m.OnBuild(func(*Mediator) error)` lets other packages add Build checks
  (httpapi route validation).
* `mediator.UnitOfWork` interface (`ReadOnly`, `AppendOutbox`) is what
  `Publish` uses; `pg` puts its implementation in the context with
  `mediator.WithUnitOfWork`.
* `m.Deliver(ctx, group, env, payload)` runs the consumer chain for one
  delivery. Transports (redisx) call it. `ConsumerStateFrom(ctx).Duplicate`
  is set by the inbox behavior when the handler was skipped.
* Core never panics out of `Send`, `Publish`, `Stream`, or `Deliver`; the
  Recovery behavior adds logging and metrics on top.
* `mediator.CanonicalJSON` / `CanonicalHash` implement 6.6 hashing.
* `mediator.Partition(key, P)` is fnv1a64 mod P.
* `mediator.NewID(now)` is a UUIDv7 without allocations.
* Behavior name constants live in core (`mediator.NameRecovery` ...);
  `behavior` re-exports them as `behavior.Recovery` etc.
* Errors: `*mediator.Error` (code, message, details, cause);
  `*mediator.ValidationError`; sentinels `ErrHandlerNotFound`,
  `ErrNoUnitOfWork`, `ErrDurablePublishInQuery`, `ErrAlreadyBuilt`,
  `ErrNotBuilt`, `ErrDepthExceeded`. `mediator.MarkAmbiguous` /
  `IsAmbiguous` mark an unknown commit outcome. Drivers add transient
  classifiers with `mediator.RegisterTransient` from `init`.
* `mediator.StatusOf(code)` and `Code.Title()` are the HTTP mapping.

## 3. Package contracts for the remaining packages

### 3.1 validate

```go
type Validator struct{ ... }
func New(opts ...Option) *Validator
func (v *Validator) Compile(t reflect.Type) error      // parses every tag in the type graph; all errors joined; unknown rule is an error
func (v *Validator) Check(ctx context.Context, val any) error // tag rules then Validate(ctx) on nested structs and root; returns *mediator.ValidationError or the error of Validate
type Schema struct { ... }                               // JSON Schema 2020-12 subset, marshals with json/v2 (use json.Deterministic(true) or ordered fields)
type Schemas struct { Defs map[string]*Schema }           // component registry; names are Go type names, package-qualified on collision
type SchemaOptions struct { AllowUnknownFields bool; SkipField func(reflect.StructField) bool; Descriptions bool }
func (v *Validator) SchemaFor(t reflect.Type, reg *Schemas, opts SchemaOptions) (*Schema, error) // returns the schema, or a $ref "#/components/schemas/<name>" for named struct types registered in reg
type Schemer interface{ JSONSchema() *Schema }          // a type supplies its own schema
```

Tag grammar and semantics: spec 5.8 and Appendix B exactly. Marker fields
(`mediator.IsMarker`) and `json:"-"` fields are skipped. Field paths are JSON
Pointers from `json` names. Descriptions from `doc:"..."` tags.

### 3.2 pg

Interfaces are in `pg/ports.go` (Store, Tx, IdempotencyRow, OutboxEntry,
StreamSink, StreamTrimmer, FencingSource, TxOptions, TxOptioner).

```go
func NewPool(ctx, url string, cfg PoolConfig) (*pgxpool.Pool, error)
func NewStore(pool *pgxpool.Pool, cfg StoreConfig) *PgStore   // StoreConfig{Partitions int} ; implements Store
func TxFrom(ctx) (pgx.Tx, bool)          // pgx tx of the ambient unit of work; false with memstore
func StoreTxFrom(ctx) (Tx, bool)         // the Tx interface of the ambient unit of work
func WithTx(ctx, store Store, opts TxOptions, f func(ctx) error) error   // same semantics as the behavior, outside the pipeline
func OnCommit(ctx, hook func(ctx))       // after successful COMMIT, in the owner goroutine, before Send returns; errors/panics logged
func BeforeCommit(ctx, hook func(ctx) error) // inside the tx just before COMMIT; an error aborts the commit
func UnitOfWork(store Store, cfg UnitOfWorkConfig) mediator.Behavior   // name mediator.NameUnitOfWork; implements StreamBehavior
func Idempotency(cfg IdempotencyConfig) mediator.Behavior            // name mediator.NameIdempotency; commands with a key; runs inside UoW
func Inbox() mediator.Behavior                                         // name mediator.NameInbox; consumer path; runs inside UoW
func Migrate(ctx, pool) error ; MigrationStatus(ctx, pool) ...
type Relay struct{...}  func NewRelay(pool, sink StreamSink, cfg RelayConfig) *Relay   // mediator.Component
type Janitor struct{...} func NewJanitor(pool, cfg JanitorConfig) *Janitor            // mediator.Component; JanitorConfig.Trimmer StreamTrimmer optional
```

Unit of work rules (6.1): join ambient tx when Propagation is Required; on
the consumer path the tx is read-write ReadCommitted; a stream's tx opens at
first iteration and closes when the sequence ends; commit failures with a
SQLSTATE are definite (not ambiguous), connection-level failures at COMMIT
are `MarkAmbiguous(Wrap(CodeUnavailable, "commit outcome unknown", err))`.
Deadline: `SET LOCAL idle_in_transaction_session_timeout` from the request
deadline when present.

The relay wake-up: `Tx.Notify(ctx, pg.NotifyChannel, topic+":"+partition)`
is issued inside the transaction (deduplicated per (topic, partition) per
transaction by the UoW); Postgres delivers it at commit, which is the
"OnCommit hook" of 6.3 without needing a second connection.

Idempotency scope is `tenant + ":" + info.Name` when the principal has a
tenant, else `info.Name`. Key sources: `IdempotencyKey()` trait first, then
`mediator.IdempotencyKeyFrom(ctx)`. Request hash is
`mediator.CanonicalHash(req)`. Lock timeout SQLSTATE 55P03 maps to
`CodeIdempotencyBusy` with `Details["retry_after_ms"]`.

### 3.3 redisx

```go
type Config struct { Addr, Username, Password string; DB int; Prefix string /* "mediator" */; PartitionsPerTopic int; LeaseTTL, LeaseRenew, ClaimMinIdle, ReadBlock time.Duration; ReadBatch, MaxAttempts int; CacheDefaultTTL time.Duration; ReplyStreamMaxLen int64; AssumeNoEviction bool; NodeID string }
func NewClient(ctx, cfg Config) (*redis.Client, error)
func CheckEviction(ctx, client) error                       // 7.7 maxmemory-policy check
type Streams struct{...}; func NewStreams(client, cfg) *Streams   // implements pg.StreamSink and pg.StreamTrimmer; XADD field layout below
type Consumers struct{...}; func NewConsumers(m *mediator.Mediator, client, cfg Config, fencing pg.FencingSource) *Consumers // mediator.Component; leases, drain rules, DLQ, halted gauge
type Cache struct{...}; func NewCache(client, cfg) *Cache
    func (c *Cache) Get(ctx, key string) (body []byte, ok bool, err error)                        // Lua cache_get
    func (c *Cache) SnapshotTags(ctx, tags []string) (map[string]int64, error)                     // MGET
    func (c *Cache) Set(ctx, key string, snapshot map[string]int64, body []byte, ttl time.Duration) error // Lua cache_set
    func (c *Cache) BumpTagsPre(ctx, tags []string) error ; BumpTagsPost(ctx, tags []string) error // INCR each; distinct fault points
type Limiter struct{...}; func NewLimiter(client, cfg) *Limiter
    func (l *Limiter) Check(ctx, name, key string, p ratelimit.Policy) (ratelimit.Decision, error) // Lua GCRA with TIME
type Remote struct{...}; func NewRemote(m, client, cfg) *Remote  // implements mediator.RemoteDispatcher (client side)
type RemoteServer / ReplyReader                                  // mediator.Component
func DLQList/DLQRequeue/DLQDrop, LeaseList/LeaseRelease, ConsumerLag                // ops functions used by mediatorctl
```

Stream entry fields (XADD): `id`, `type`, `topic`, `key`, `seq`,
`partition`, `at` (RFC3339Nano), `corr`, `cause`, `trace`, `schema`,
`headers` (JSON object), `payload` (JSON), `outbox_id`. Consumers rebuild
`mediator.Envelope` from them. Key layout: spec 7.1 with `cfg.Prefix` in
place of the literal `mediator`.

### 3.4 behavior

```go
type Config struct {
    Logger *slog.Logger; Tracer trace.Tracer; Meter metric.Meter; Clock mediator.Clock
    DefaultTimeout time.Duration   // 30s
    LogPayloads bool; RequireAuthByDefault bool
    Validator *validate.Validator  // nil -> validate.New()
    Store pg.Store                 // nil -> UnitOfWork, Idempotency, Inbox are omitted
    UnitOfWork pg.UnitOfWorkConfig; Idempotency pg.IdempotencyConfig
    Cache CacheBackend; CacheTTL time.Duration   // nil -> Cache omitted
    Limiter RateLimiter; FailClosed bool         // nil -> RateLimit omitted
}
type CacheBackend interface { Get; SnapshotTags; Set; BumpTagsPre; BumpTagsPost }   // *redisx.Cache satisfies it
type RateLimiter interface { Check(ctx, name, key string, p ratelimit.Policy) (ratelimit.Decision, error) }
type Entry struct { Behavior mediator.Behavior; Options []mediator.UseOption }
func Standard(cfg Config) []Entry          // default order of 5.1 with scopes
func UseStandard(m *mediator.Mediator, cfg Config) error
const Recovery = mediator.NameRecovery ... // re-exported names
```

Scopes of the standard set: Recovery, Tracing, Logging, Metrics use
`Everywhere()`; Timeout uses requests and consumers; Authorization,
RateLimit, Validation, Cache, Retry use requests with trait predicates;
UnitOfWork uses requests and consumers minus `NoUnitOfWork` types;
Idempotency uses commands with a key trait or always for commands (it skips
at run time when no key is present); Inbox uses `Consumers()`.

Cache invalidation for commands is a separate inner behavior named
`CacheInvalidation`, positioned after Idempotency (inside the unit of work):
after `next` succeeds it bumps tags pre-commit directly and registers the
post-commit bump with `pg.OnCommit`. A replayed idempotent command never
reaches it, which is correct because a replay changes nothing.

### 3.5 httpapi

```go
type Config struct { Prefix string; MaxBodyBytes int64; AllowUnknownFields bool; Authenticator func(*http.Request) (authz.Principal, error); Logger *slog.Logger; Docs http.Handler /* served at /docs and /openapi.json when set */; ReadyChecks []func(context.Context) error; ReadyMaxLag time.Duration; KeepAlive time.Duration /* SSE, 15s */ }
func BuildCheck(m *mediator.Mediator) error          // for m.OnBuild: validates Route traits and bindings, all violations at once
func New(m *mediator.Mediator, cfg Config) (*Server, error) // after Build
func (s *Server) Handler() http.Handler
func (s *Server) Routes() []RouteInfo                  // method, path, *RequestInfo, bindings; openapi consumes it
func RouteOf(info *mediator.RequestInfo) Route         // RPC default or trait
func BindingsOf(t reflect.Type) ([]Binding, error)     // path/query/header/body per field
func NewListener(s *Server, addr string, drain time.Duration) *Listener // mediator.Component
func WriteProblem(w http.ResponseWriter, r *http.Request, err error)
```

## 4. Test placement

* Unit tests sit beside the code.
* Integration tests that need containers sit beside the code too, in files
  named `*_integration_test.go` with `//go:build integration`, each package
  owning its `TestMain`. `test/integration` holds cross-package scenarios
  (runtime shutdown, end-to-end example service).
* `testkit/memstore` is the in-memory `pg.Store`; `pg/storetest` is the
  conformance suite both implementations pass.
* The race detector needs cgo; on this Windows machine there is no C
  compiler, so `-race` runs in CI (Linux). Locally use `go test -count=2 -shuffle=on`.
* The coverage profile of `tools/task cover` is written with
  `-tags integration,faultinject`, so the pg and redisx drivers are measured
  with the tests that talk to Postgres and Redis (Docker); unit tests alone
  leave them near 50 and 40 percent because almost every statement is an I/O
  call site. Spec 11.2 fixes the command and the thresholds, not the tags.

## 5. Toolchain facts verified on 2026-09-26

* `go 1.27` in go.mod; toolchain go1.27.0 auto-downloads.
* `encoding/json/v2` and `encoding/json/jsontext` are stable.
  Differences from v1 that matter: nil slices encode as `[]` and nil maps
  as `{}`; map key order is unspecified unless `json.Deterministic(true)`;
  unknown members are rejected only with `json.RejectUnknownMembers(true)`;
  names are case-sensitive; duplicate names are rejected.
* `httptest.NewTestServer(t, handler)` (Go 1.27) takes a `testing.TB`.
* Go 1.27's `encoding/json/v2` has no single-quoted member names: `json:"'-'"` and
  `json:"-,"` are malformed, a member literally named `-` is `json:"-,omitempty"`,
  and staticcheck SA5008 still follows the experiment grammar and flags that form.
* `testing/synctest.Test(t, func(t *testing.T))` is the bubble API.

## 6. Deviations from spec.md

| Spec | Implementation | Why |
|---|---|---|
| `testkit.MemStore` | `testkit/memstore.Store` | `pg` imports `testkit` for fault points; the fake must import `pg` for the interfaces, so it lives in a subpackage |
| Relay wake-up "from an OnCommit hook" | `pg_notify` issued inside the transaction, delivered at commit | Same latency, no second connection, and a rolled-back transaction sends nothing |
| Cache behavior bumps tags "before COMMIT" | A separate `CacheInvalidation` behavior inside the unit of work | The `Cache` behavior sits outside the unit of work and cannot reach its hooks |
| Integration tests in `test/integration` only | Package-level `*_integration_test.go` plus `test/integration` for cross-package scenarios | Keeps each package's `TestMain` and containers independent |
| `Kind` has three values | Five: adds notification and consumer | One pipeline mechanism serves all three paths described in Appendix D |
| `redisx` never imports `pg` | `redisx` imports `pg` for `OutboxEntry` and the sink interfaces | Avoids duplicating the relay entry type; `pg` still never imports `redisx` |
| Module path `github.com/OWNER/go-api-backend` | `github.com/t3stackcoder/go-api-backend` | OWNER replaced with the repository owner |
| Consumer group `read-model`, `Descriptions` default true | Group `read_model`; `openapi.Config.NoDescriptions` (descriptions on unless disabled) | `NamePattern` is `^[A-Za-z][A-Za-z0-9_.]{0,127}$` and forbids `-`, so every persisted group name uses `_` (`read_model`, `audit`, `poison`, `inventory`); a zero `openapi.Config` should produce the documented default, which a `Descriptions bool` cannot |
| Shutdown on SIGTERM only | Also on stdin EOF when `SHUTDOWN_ON_STDIN_EOF=1` (`examples/orders`) | Windows cannot deliver SIGTERM to a child process; integration tests close the child's stdin to trigger the same drain |
| Behavior constructors named after the behavior (`behavior.Cache`) | `behavior.New*` constructors; the bare names are the name constants (`behavior.Cache == mediator.NameCache`) | The name constants are re-exported from the core and are what `Use` options and logs refer to |
| Timeout "applies `context.WithTimeout`" (5.6) | `behavior.deadlineCtx`: one allocation, observably equivalent, lazily backed by `context.WithDeadline` on the first `Done()` | G17: saves 4 allocations on the no-I/O path; children derived by pgx, go-redis, or errgroup still attach without a watcher goroutine (`TestDeadlineCtx_NoWatcherGoroutines`) |
| Spec 6.2 table list | Migration `0002_partition_epoch.sql` adds `mediator_partition_epoch (consumer_group, topic, partition, epoch, updated_at)`, primary key `(consumer_group, topic, partition)` | Fencing at the effect (G14, 8.7): the lease store cannot close the window between Redis forgetting a lease key and the old owner's next renewal tick, so the consumer's token is checked in the transaction that applies the effect |
| `pg.Tx` interface frozen | `FencePartition(ctx, group, topic, partition, token) (ok bool, err error)` added, fault point `pg.inbox.fence`; `Inbox()` calls it before `InboxInsert` whenever `mediator.FencingToken(ctx)` is present and returns `CodeConflict` wrapping `pg.ErrStaleLease` (not transient) when the token is below the stored epoch | The token has to be checked where the effect happens, which is Postgres (G14, 8.7); the upsert's row lock also serializes the two owners' transactions |
| Spec 11.3: tiers 0 to 4 on every push, the long tiers nightly (11.6 and 14 count on nightly runs) | No cron schedule and no push or pull-request trigger: nothing runs in CI unless someone dispatches the workflow by hand from the Actions tab. A dispatch runs static, unit (with `-race` and the coverage gate), short fuzz, integration, and the openapi job, about ten minutes of wall clock; ticking `nightly=true` adds the fault sweep, 30-minute fuzz, mutation gate, benchmarks, and chaos matrix | The user's decision in the eighth and ninth sessions: the framework is finished and is not to be re-tested unless it changes, with no front end there is nothing to soak every night, and the sweep's 40 to 75 minutes per run is not worth waiting for. A future change is verified by dispatching the workflow by hand or with the local commands of handoff section 4; spec 14's two weeks of green nightlies is on hold until there is something to soak |

## 7. Decisions recorded by the package agents

These resolve details spec.md leaves open. They live in package doc comments
too; this list is the index.

### 7.1 pg

* The envelope's correlation ID, causation ID, trace parent, and occurred-at
  are stored inside `mediator_outbox.headers` JSONB (`corr`, `cause`,
  `trace`, `at`; user headers under `h`) because the 6.2 DDL has no columns
  for them. Causation equals `mediator.RequestID(ctx)` of the publishing
  Send. The workload therefore also tags every durable event with
  `Headers({"cmd": CmdID})` and records the request ID in `wl_cmd_log`.
* Idempotency scope is `<tenant>:<Name>` when the principal has a tenant,
  else `<Name>`; keys are 1 to 200 characters; lock timeout 55P03 maps to
  `CodeIdempotencyBusy` with `Details["retry_after_ms"]`.
* A commit failure carrying a SQLSTATE is definite; a connection-level
  failure at COMMIT is `MarkAmbiguous`. `OnCommit` hooks do not run after an
  ambiguous commit and receive the parent context.
* The `Inbox` behavior sets `ConsumerState.Duplicate` and returns without
  calling next; the unit of work then commits the empty transaction.
* Advisory locks used by the relay, janitor, and migrations are
  database-global, so integration tests that run them are sequential or use
  a database per test.

### 7.2 redisx

* Consumer groups on partition streams are created at ID `0` (spec 7.7
  replay); the RPC `handlers` group at `$`.
* Remote dispatch matches replies with a per-call `call` field (UUIDv7);
  `corr` carries the caller's correlation ID unchanged. `redisx.NewRemote`
  takes no mediator: the dispatcher is passed to `mediator.WithRemote` at
  `New`.

### 7.3 behavior

* `CacheInvalidation` (name constant `behavior.CacheInvalidation`) is a
  separate inner behavior after `Idempotency`; `Cache` (queries) sits
  outside the unit of work.
* Idempotency is scoped to commands that have a unit of work
  (`mediator.Where(hasUnitOfWork)`), which makes
  the core's "RetryPolicy with NoUnitOfWork is allowed when IdempotencyKey exists"
  rule vacuous for the standard chain. The core keeps allowing it (an
  application may wire its own key-based idempotency without a transaction),
  but the standard Retry behavior's Prepare rejects the combination at Build,
  because a retry outside a unit of work could repeat effects.

### 7.4 httpapi and openapi

* `httpapi` always serves stream routes as SSE regardless of `Accept`, and
  binds body fields of RPC GET queries from the query string.
* `openapi` puts `x-sse-item` on the media type object next to `schema`,
  which is where OpenAPI 3.2's `itemSchema` goes (spec 15.2 q6).

### 7.5 Names

* Workload request names are dotted (`wl.SetValue`); consumer groups are
  `read_model`, `audit`, `poison`, `inventory`.
* Every I/O call site in `pg`, `redisx`, and the behaviors passes a literal
  fault point name from `mediator/testkit/faultpoints.txt`;
  `TestFaultPointCatalogue` fails on an unlisted literal.
* Unreachable lines carry `// covergate:ignore <reason>`; `tools/covergate`
  rejects an empty reason.

### 7.6 test/integration

* Each test gets its own database (`CREATE DATABASE` / `DROP DATABASE WITH
  (FORCE)`), not a schema: the relay slot, janitor, and migration advisory
  locks are database-global, so two nodes in one database would contend for
  the same slots.
* The example service is built once per package in `TestMain`; nodes run
  as child processes with `SHUTDOWN_ON_STDIN_EOF=1` and a stdin pipe, so
  the shutdown tests work on Windows (stdin closed) and Unix (SIGTERM).
* The readiness test binds its own Redis container to a fixed host port
  (`HostConfigModifier`) because Docker may assign a new ephemeral port to a
  restarted container while the node keeps the address it started with;
  this makes `github.com/moby/moby/api` a direct dependency of the test tree.
* A rejected stream request (for example an unauthenticated
  `GET /orders/{id}/events`) answers HTTP 200 with one `event: error` frame
  carrying the problem body, because stream routes are always SSE (3.5).

### 7.7 test/chaos

* Result classification of a node's HTTP answer: 2xx is `ok`; the definite
  problem codes (`bad_request`, `validation`, `not_found`, `conflict`,
  `precondition_failed`, `unauthorized`, `forbidden`, `rate_limited`,
  `idempotency_mismatch`, `handler_not_found`, `method_not_allowed`,
  `payload_too_large`, `unsupported_media_type`) and dial failures are
  `fail`; timeout, unavailable, internal, `idempotency_in_progress`, and a
  broken connection are `info`.
* `bank-idempotent` keeps one operation open and retries the same
  idempotency key across nodes until the result is definite. L2 re-sends of
  retired `info` operations are not added to the history (a replay would
  misrepresent a `set`); they run after healing and before the final reads.
* Degraded windows for the cache checkers come from Redis-affecting nemeses
  only (`partition-redis`, `redis-restart`, `redis-flush`,
  `redis-restore-old`, and `latency`, `reset`, `bandwidth` on Redis
  proxies), global across nodes.
* `redis-restore-old` snapshots `/data/appendonlydir` once after warm-up
  (after `BGREWRITEAOF`) and restores it with `docker stop`, `docker cp`,
  `docker start`.
* Heal starts stopped containers and unpauses paused ones but does not
  restart running nodes, so recovery state is not laundered by a restart.
  Every run resets by stopping the nodes, truncating the `mediator_*` and
  `wl_*` tables, seeding the bank, `FLUSHALL`, then starting the nodes.
* Every nemesis holds per-resource locks (node, postgres, redis, relay,
  handlers, clock per node), so at most one Docker-level fault targets a
  container at a time; toxics hold no lock. A no-op nemesis (for example
  `handler-rotate` outside `remote-send`) ends immediately and is logged
  with `noop: true`.
* Containers are reused across runs, so node logs are collected with
  `docker logs --since <run start>`.
* The compose chaos profile sets `CHAOS_GROUPS=read_model,audit` (the two
  groups of the `events` workload); the node default of all three groups,
  including `poison`, serves the fault sweep.
* First results (60 s runs, seeds 1 to 5): every workload passes except
  `events`, which exposed two framework bugs recorded in section 8.

## 8. Hardening outcomes (M7)

### 8.1 G17

Before: 9 allocations per `Send` with an ambient correlation ID and 10 with
a cold context (core 4: request box, scope, `WithValue`, response box; +1
cold for the formatted correlation string; Timeout +4 from
`context.WithTimeout`: timer context, timer, timer callback, cancel closure;
Tracing +1 for the no-op tracer's `ContextWithSpan`). After: 6 and 6,
meeting the spec 4.12 target, at 0.42 to 0.46 µs per `Send` for the full
default chain with no I/O (5 allocs/op and 280 B/op under `-benchmem`;
baseline 0.54 µs warm and 1.02 µs cold).

Two changes. The core keeps a generated correlation ID as the UUID in the
scope and formats it only when `CorrelationID` is read. The Timeout behavior
uses `deadlineCtx`, a one-allocation deadline context whose timer and Done
channel are created on the first `Done` call (only I/O paths make one) and
whose `Err` answers from the clock until then. It is not a foreign context
to the standard library: its Done channel and cancel key resolve to an
inner `context.WithDeadline` created lazily, so children derived by pgx,
go-redis, errgroup, or through `WithValue` wrappers attach without a
watcher goroutine (`TestDeadlineCtx_NoWatcherGoroutines` pins this against
a foreign-context control); the I/O path pays one extra allocation on the
first `Done`. Known difference from `context.WithTimeout`: `context.Cause`
before any `Done` call follows the nearest standard-library ancestor; the
framework classifies outcomes through `Err`. Pinned floors:
`TestSend_DefaultChainAllocations` 6/6, `TestSend_CoreAllocations` 4/4.

### 8.2 Schema component names

Component names strip package paths from the type arguments of generic
instantiations (`Page[.../orders.OrderSummary]` becomes `Page_OrderSummary_`,
`Map[K,V]` becomes `Map_K_V_`). Two instantiations whose arguments share a
short name collide like any other names and are disambiguated by
`qualifiedName` plus the counter (`pkg.Page_X_`, `pkg.Page_X__2`).

### 8.3 Metrics

`mediator.consumer.processed` has one emitter, `otel.NewConsumersObserver`
on `redisx.Consumers`, which sees every delivery outcome including `dlq`
and `skip`. The Metrics behavior records consumer deliveries only as
`mediator.request.duration` and `mediator.request.inflight` with kind
`consumer`, so an application wiring both no longer counts each delivery
twice.

### 8.4 Validation tag grammar

`validate` derives member names with the `encoding/json/v2` grammar as
shipped in Go 1.27 (G13): `-` skips, the name runs to the first comma and
may not contain a comma, backslash, or quote, options are ignored, and a
tag json/v2 rejects (trailing comma, empty option, quoted name, any tag but
`-` on an unexported field) is a `Compile` and therefore Build error naming
the field. `TestJSONNameAgreesWithJSONV2` uses json/v2 itself as the oracle.
The other tag readers (`behavior/redact.go`, `httpapi/routing.go`,
`openapi/generate.go`, `testkit/history/values.go`) still cut at the first
comma; they agree with `validate` on every tag json/v2 accepts and only
differ on tags that now fail Build.

### 8.5 Chaos findings under repair

The `events` workload exposed two protocol bugs, both fixed after the first
chaos runs (see the relay and consumers doc comments):

* A voluntary lease release could happen while the worker's blocking
  `XREADGROUP` was outstanding, so an entry landed in the old owner's PEL
  after the new owner's drain-first pass and was applied out of order by
  the later `XAUTOCLAIM` (G6, I6).
* The relay checked for Redis data loss only on its poll branch; under
  wake-up load its own next `XADD` moved the stream tail past the cursor
  and a `FLUSHALL` went undetected (G12, G5, I2, I3).

### 7.8 deploy

* The example service is deployed in the compose stack as profile `orders`
  (`go run ./tools/task orders-up`, Dockerfile target `orders`, port 8080),
  separate from the default profile so that `task up` still starts only
  Postgres and Redis for the fault sweep. `JWT_SECRET` is passed through from
  the host environment; empty means the development secret, which the service
  logs a warning about at start.

### 7.9 testkit/netfault

* Connection-level faults are injected in process rather than through a
  proxy: `netfault.Dialer` is a pgconn `DialFunc` whose connections fail
  the next write that carries a chosen SQL fragment (`FailWrite`, nothing
  is sent and the connection closes) or send it and withhold the reply
  (`DropAfterWrite`, the lost acknowledgement of spec 6.1 step 5). The
  fault points of `testkit` sit before and after each driver call and
  cannot fail the call itself; the dialer reaches the driver's own error
  branches (a failed COMMIT or ROLLBACK, an advisory lock that cannot be
  taken or released, a LISTEN connection lost at a query) deterministically
  and without protocol parsing, because pgx writes the statement text in
  the Parse or Query message.
* `Dialer.Config` returns a pool of one connection in
  `pgx.QueryExecModeDescribeExec`: with the statement cache a repeated
  statement carries only its name on the wire, and one connection keeps
  the statement under test on the connection the rule watches. Tests set
  `search_path` on the config's `RuntimeParams`.
* A rule fires once and is consumed by whichever connection writes the
  fragment first, so a test arms it right before the call and runs the
  call on a single goroutine; the relay's LISTEN connection test sets
  `MaxConns` to two because the slot worker needs a pooled connection
  beside the hijacked one.

### 8.6 Fault sweep (tier 3)

First complete run on 2026-09-26 in quick mode (first hit of every point, no
resource variants): all eight scenarios pass and the completeness gate accepts
the matrix. cancel, crash, delay, error, permanent, and timeout reach 37 of
the 39 catalogue points; the two outright exclusions are `pg.migrate.apply`
and `http.sse.write`. ambiguous reaches the 8 points that have a
`testkit.FaultAfter` site and is excused at the other 29 by construction, and
the crash and ambiguous subtests at those 29 points are skipped in the log
(crash-before covers them for the gate). The quick run takes about nine
minutes, dominated by `SweepConsumer` (156 s), `SweepRemote` (97 s), and
`SweepRelay` (75 s). `tools/task test-sweep` passes `-timeout 60m` because
the full matrix exceeds go test's default ten minutes.

### 8.7 Chaos findings, second round (fixed in the fifth session)

`events`, 60 s, seeds 1 to 4 on the node image that carries the 8.5 fixes:
seeds 3 and 4 passed; seeds 1 and 2 failed I6 (seed 1 also I3, seed 2 also the
fencing checkers). Run directories `test/chaos/runs/events-seed1-20260926-165740`
and `events-seed2-20260926-165925` hold the evidence. Two defects in the
`redisx` lease protocol, both fixed as described below; the analysis is kept
because the fixes are only understandable against it.

**Same-node re-acquire overlaps the old reader.** Stream commands run under
the node ID as consumer name. When Redis forgets a lease key (FLUSHALL,
restore) the next renewal reports "lost to another owner", and `rebalance`
in the same tick re-acquired the partition with a new epoch and started a
new worker. The old worker's `XREADGROUP ... BLOCK` is still outstanding:
go-redis 9.22 runs a command synchronously on its connection and takes only
the socket deadline from the context, so cancelling the lease context does
not interrupt the read. Redis serves the older blocked reader first, so the
next entry lands in the node's PEL under the same consumer name; the new
worker's claim-before pass skipped own entries, read past it, and the entry
waited for the periodic claim pass (ClaimMinIdle, 30 s) or the next surplus
drain. Seed 1, node3: e2 seq 217 was created at 21:58:46.049 and applied at
21:58:50.460 (audit, epoch 672, at the surplus drain) and 21:59:16.108
(read_model, epoch 696, 30 s after 218), while 218 was applied at
21:58:46.222; the leases were lost and re-acquired between 21:58:45.18 and
45.26. Fix, in `mediator/redisx`: (a) `leaseManager.ending` keeps an ended
lease whose worker has not exited; `end` parks it there when a worker is
attached and has not reported back, `workerDone` (deferred by the worker so
that it runs after `exit`) unparks it, and `rebalance` treats a parked
partition as taken without counting it toward the desired number, so the
node takes other free partitions meanwhile and this one at a later tick.
The park decision and the exited mark are both taken under the manager
lock, so a worker that returns between the lease cancel and the park cannot
leave the partition parked forever. (b) `claimPendingBefore` /
`pendingBefore` (formerly `claimForeignBefore` / `foreignBefore`) claim every
pending entry below the batch, whichever consumer holds it and the node's
own name included; the XPENDING summary's lowest ID decides in one round
trip whether the range scan is needed at all. The worker skips the pass once
it is stopping, since the batch then stays pending for the next owner and a
claim would only reset the idle time its drain-first pass waits on. (c) The
`readCtx` comment states the go-redis behaviour correctly. Tests:
`TestLeaseManager_LostLeaseWaitsForWorkerExit` and
`TestLeaseManager_EndingTracksWorkerExit` (synctest),
`TestCompareStreamIDs`, and the integration test
`TestConsumers_ClaimsOwnStaleReaderEntriesBelowBatch`, which issues the
ghost read in the node's own name.

**A stale owner writes for up to LeaseRenew after Redis forgets its key.**
After `redis-restore-old` (equally a flush, or a restart that loses the last
second of AOF) the lease keys are gone; another node acquires them at once
with higher tokens; the old owner learns at its next renewal tick and kept
reading and applying until then. Seed 2: node1 applied with 1414, 1428, and
1474 from 22:00:24.08, when node2 acquired the same partitions with 1507,
1523, and 1527, until its tick at 22:00:27.65. The two readers split the
entries, and the fencing checkers flag every lower token that applies after
a higher one. The lease store cannot close this window; the token has to be
checked where the effect happens, which for the framework is Postgres. Fix:
migration `0002_partition_epoch.sql` with
`mediator_partition_epoch (consumer_group, topic, partition, epoch, updated_at)`;
`pg.Tx.FencePartition(ctx, group, topic, partition, token) (ok bool, err error)`
as `INSERT ... ON CONFLICT DO UPDATE SET epoch = EXCLUDED.epoch WHERE
mediator_partition_epoch.epoch <= EXCLUDED.epoch RETURNING 1` (Postgres
locks the conflicting row even when the WHERE rejects the update, which is
what serializes the two owners' transactions); the Inbox behavior calls it
before `InboxInsert` whenever `mediator.FencingToken(ctx)` is present and
returns `mediator.Wrap(CodeConflict, ..., pg.ErrStaleLease)` (not
transient) when no row comes back; `redisx` `handle` recognizes
`pg.ErrStaleLease`, releases the lease with reason `LeaseEndLost` (a
compare-and-delete, so a key Redis still holds with this node's old value
after a snapshot restore is freed at once instead of blocking the
re-acquire until the TTL) and stops the worker without counting an error;
the entry stays pending for the new owner's claim pass. `testkit/memstore`
and `pg/storetest` gained the method and the `PartitionEpochFence`
conformance test; new fault point `pg.inbox.fence` (a `testkit.Fault` call
only, so the ambiguous kind is excused by construction), swept by
`SweepConsumer`. The table and the `Tx` method are recorded in the section 6
table. Tests: `TestInbox_FencesStaleLease` (memstore) and the integration
test `TestConsumers_StaleLeaseFenceEndsLeaseAndReacquires`, which also pins
the re-acquire under LeaseTTL. Seed 2's I6 violations (e6, e0) fit the
first defect: node2 lost and re-acquired its leases within one tick at
21:59:58.97 and 22:00:24.06.

The chaos results after both fixes are in 8.8.

### 8.8 Chaos results after the 8.7 fixes

`events`, 60 s, on the node image rebuilt with both fixes (fifth session,
2026-09-26). Run directories under `test/chaos/runs/` (gitignored).

| Seed | Run | Nemeses drawn | Result |
|---|---|---|---|
| 1 | `events-seed1-20260926-182859` | bandwidth, handler-rotate, latency, pause, redis-flush | pass (was I3, I6) |
| 2 | `events-seed2-20260926-183044` | graceful-restart, latency, pause, redis-restore-old x2 | pass (was I6, fencing_db, fencing_logs) |
| 3 | `events-seed3-20260926-183247` | graceful-restart, latency, partition-pg, partition-redis, reset | pass |
| 4 | `events-seed4-20260926-183429` | bandwidth, pause, relay-kill, reset x2 | pass |
| 5 | `events-seed5-20260926-183721` | bandwidth, handler-rotate x2, partition-pg, partition-redis, redis-flush | pass |
| 6 | `events-seed6-20260926-183952` | bandwidth, handler-rotate, partition-redis, redis-flush, reset | pass |
| 7 | `events-seed7-20260926-184133` | kill, latency, partition-pg, pg-restart, relay-kill x2 | pass |
| 8 | `events-seed8-20260926-184259` | handler-rotate x2, pg-restart, redis-flush, redis-restore-old | pass |

Every run passes all fifteen checkers (I1 to I6, L1, L2, DLQ, fencing_db,
fencing_logs, logscan, shutdown, workload, workload_db). The fixes are
visible in the node logs: seed 2 shows `delivery rejected by the partition
fence; giving the lease up` 10 times (node1 4, node2 6) and `claimed pending
entries below the batch` 15 times across the three nodes; seed 1 shows 32
`lease lost to another owner` lines and neither message, because after a
flush the workers' blocked reads return at once (Redis unblocks a reader
whose stream key is deleted), so the workers exit and unpark their
partitions before the same tick's rebalance, and the re-acquired worker
finds nothing foreign to claim; the synctest tests cover the case where
the worker is still blocked. Seeds 5, 6, and 8 exercised both paths too
(fence rejections 2, 2, and 0; claim-below passes 13, 10, and 7); seed 7
lost only 3 leases and needed neither. Seeds 7 and 8 are the first live
runs to draw `kill` (seed 7) and `pg-restart` (both); both pass.

### 8.9 Fault sweep, second round

Quick matrix (`SWEEP_QUICK=1`, first hit of every point, no resource
variants) after the 8.7 fixes, with the catalogue at 40 points: all eight
scenarios ran in 543 s, `TestFaultSweepCompleteness` passes (`cancel`,
`crash`, `delay`, `error`, `permanent`, `timeout` 38/40 each, the two
outright exclusions; `ambiguous` 8/40 with 30 points excused for having no
`FaultAfter` site), and `pg.inbox.fence` is swept by `SweepConsumer` and
`SweepShutdown` under every kind (its ambiguous cell is excused by
construction, its crash cell is a `crash-before`). One cell failed:
`SweepShutdown/redis.xreadgroup#1/shutdown` reported `lease still held
after shutdown: group=read_model partition=0 node=n1 epoch=6 ttl=1.19s`
2 times in 4 reruns, always with the runtime returning about 290 ms after
start.

Root cause, pre-existing and unrelated to 8.7: the shutdown kind cancels
the runtime while the first `XREADGROUP` is delayed, about 90 ms after
start, and the lease loop's first tick can still be inside `acquire` for
the second scope (the fencing token comes from Postgres, whose pool is cold
while the runtime starts everything at once). `acquire` then finds
`stopped` set after its `SET NX` and gives the key back with a
compare-and-delete under the tick's context; `Consumers.Run` cancels that
context (`stopLoop`) right after `releaseAll`, which released only the
leases that were owned by then, so the late delete was skipped and the key
sat unowned until its TTL. The surviving key's remaining TTL dated its
`SET` to about 300 ms before the check, i.e. just after the stop. Fix: the
post-stop release in `acquire` runs under a detached, `opTimeout`-bounded
context derived from the manager's base context, and a failure is logged.
Test: `TestLeaseManager_AcquireAfterStopReleasesUnderDetachedContext`, with
the fake lease store now honouring context cancellation like go-redis and
offering an `afterAcquire` hook to land the stop between the write and the
stopped check. After the fix the cell passed 4 of 4 reruns (it had failed 2
of 4 before) and the whole `redisx` integration package stays green.

Full matrix (`go test -tags faultsweep,faultinject -count=1 -timeout 60m -v
./test/faultsweep/`, the sweep task's command with the single package path
so the output streams): `SweepConsumer` failed 3 of 495 cells,
`redis.xautoclaim#4`, `redis.xreadgroup#13`, and `redis.xreadgroup#14`,
all `crash-before`, all with `G16: group poison acknowledged entry ... (key
poison seq 1) without an inbox row` in the state check after the crash.
That is a gap in the harness, not in the framework: `checkAckedImpliesInbox`
flagged every delivered, non-pending entry without an inbox row, and a dead
letter is acknowledged after its copy was added to the group's DLQ with no
inbox row by design (spec 7.3), so any crash cell late enough for the
`poison` group's third failed attempt to have dead-lettered the entry
failed the check. The quick matrix only runs first hits, which crash before
that, which is why the gap never showed. The check now exempts entries whose
original stream ID is in the group's DLQ (`redisx.DLQList`).
The full matrix otherwise passed: 2992 s, every other scenario green
(`SweepShutdown` 433 s including every cell of the point that leaked
before), `TestFaultSweepCompleteness` passing with the same per-kind
coverage as the quick run, and the resource variants green except
`SweepConsumer/pool1` at `pg.tx.rollback#1` (`error` and `timeout`), which
failed in the recovery build with `redis client: redisx: ping
localhost:32826: context deadline exceeded` after 240 s each: the Redis
test container stopped answering for about eight minutes and the cells
after it passed, so that is the test host, not the framework (the same
cells pass without the variant and the other scenarios' `pool1` variants
pass). That reading was wrong: the cells before and after ran at normal
speed, and both failing cells are the two kinds the `pool1` variant runs at
`pg.tx.rollback`. The fault fired before the driver call, so `pgTx.Rollback`
returned the injected error without ever issuing ROLLBACK, the unit of work
logged the failure and dropped the transaction, and the pool's only
connection stayed checked out in an open transaction; the node hung for the
rest of the four-minute cell budget and the recovery step then failed on
the first thing it tried under the exhausted deadline, the Redis ping. A
real failed ROLLBACK breaks the connection and pgxpool releases it, so the
injection modelled a leak the driver does not have. `pgTx.Rollback` now
tears the transaction down under a detached context when the fault fires
and still returns the injected error; the first fault-injection test of the
`pg` package, `TestFault_RollbackFaultReleasesConnection` (integration and
faultinject tags, pool of one, `error` and `timeout` kinds), pins it and
fails without the fix. Re-runs: the two `pool1` cells failed again on the
binary built before the fix (240 s each, deterministic), and all 18
`SweepConsumer` crash-before cells at `redis.xautoclaim` and
`redis.xreadgroup` pass with the corrected check (50 s); with the fix the
two `pool1` cells pass in 1.4 s and 3.4 s, twice.

### 8.10 Coverage gate with the integration tags

`task cover` now measures `./mediator/...` with the `integration` and
`faultinject` tags (fourth session) and ran for the first time in that form
in the fifth. Its first run failed before the gate: the CLI integration
test of `mediator/ctl` hard-coded one migration; it now derives the expected
version from the embedded files. With the profile complete, the three
packages held at 100 percent (`mediator`, `behavior`, `validate`) are at
100, `redisx` rose from 92.5 to 95.7 percent with the 8.7 tests, and five
packages sat below the 95 percent default: `pg` 89.8, `pg/storetest` 80.1,
`testkit` 94.2, `testkit/invariants` 94.8, `testkit/workload` 93.9.
`testkit`'s gap was four one-line classification methods of
`InjectedError` that no test called; `TestInjectedError_Classification`
covers them (97.5 percent). The other four are held by documented
thresholds at their measured levels, `coverThresholds` in
`tools/task/tasks.go`, so the numbers can only go up: `pg`'s uncovered
statements are the Postgres error branches of relay (`BeginBatch`,
`SaveCursor`, `Mark`, `check`), janitor (`sweep`), migrate (`withMigrateLock`,
`inTx`, `MigrateDown`), outbox (`OutboxReshard`, `scanOutboxEntry`,
`encodeOutboxHeaders`) and the statement failures of `tx.go`, which need
connection-level faults the fault points do not inject; `pg/storetest` is a
conformance suite whose failure branches run only when a store does not
conform; `invariants` and `workload` are chaos and sweep support whose
uncovered lines are error returns that only a failing Postgres reaches. An
explicit `-thresholds` argument to `task cover` replaces the defaults.
With the regenerated profile the gate passes: all 19 packages at or above
their thresholds (`coverage/summary.md`).

### 8.11 Coverage thresholds raised (sixth session)

The three held thresholds of 8.10 are gone: `pg` is at 100.0 percent (1052
statements, two ignored), `testkit/invariants` at 100.0 and
`testkit/workload` at 100.0 (three ignored), and `coverThresholds` names
only `pg/storetest` (80.1, the conformance suite). The gate now lists 20
packages; the new one, `testkit/netfault` (7.9), is at 100.0. Nothing in
the framework changed except two seams and three ignore comments; the
gaps were closed by kind:

* **Statements the server rejects while they run** (no proxy needed): a
  plpgsql trigger that raises, BEFORE INSERT, UPDATE, or DELETE on the
  table of the statement under test (outbox insert, relay mark and cursor
  upsert, reshard update and cursor reset, idempotency store, the bank
  debit and credit); a deferred constraint trigger, which fails the COMMIT
  itself with SQLSTATE P0001 and is the definite commit failure of 6.1
  (`isDefiniteCommitFailure` true, nothing committed); `ALTER COLUMN ...
  TYPE text` for a row that no longer scans (outbox seq, stats partition,
  bank balance, append value, bump count); a view whose column is a
  plpgsql function that raises, for a query that fails after it started
  returning rows (pgx v5 prepares a statement before it executes it, so a
  missing table fails `Query` itself and only a runtime error surfaces in
  `rows.Err()`); a `mediator_schema_version` with the wrong columns or
  text values; a table that already exists before `Migrate`; a `NULL`
  stream key after `DROP NOT NULL`; a headers document of the wrong shape
  (`{"h": 5}`); and a row lock held under a 200 ms `lock_timeout`, whose
  failure arrives with the rows (`SELECT ... FOR UPDATE`).
* **Connection-level failures**, through `testkit/netfault` (7.9): the
  failed ROLLBACK of `pgTx` and `pgBatch` and the unit of work logging it;
  the migration advisory lock, unlock, and `begin`; the janitor lock and
  the unlock that hijacks and closes the connection; the relay's LISTEN
  connection dropped at LISTEN itself and at the advisory lock query (the
  session ends, the relay counts it, the next session owns the slot; the
  LISTEN branch was covered before only when the terminate-backend test
  happened to race a reconnect); and the lost COMMIT
  acknowledgement, where the test reads the committed row through a fresh
  connection while the client saw an indefinite failure. In `invariants`,
  one statement of each check is dropped in turn (16 statements).
* **Fault-point returns**: `TestFault_PointsReturnTheInjectedError` arms
  every `pg.*` point of the catalogue with the error kind against the real
  store, the mark point also with the ambiguous kind (`FaultAfter`), and
  `TestFault_RelayLockFaultIsRetried` shows a fault at `pg.relay.lock`
  skips the slot for one pass. A relay batch locks its row `FOR UPDATE`,
  so a test that opens several must roll each back before the next selects
  with `SKIP LOCKED`, or the next sees nothing; that cost one hung run.
* **Seams**: `migrationsFS` (`SetMigrationsFS`) for a source that cannot be
  listed or read and a migration without a down section;
  `NewPgSlotStoreForTest`; the fake slot store gained `failCursor` and
  `onBegin`; a closed pool for the relay session and the relay begin.
* **Ignored with a reason**: `pgxpool.NewWithConfig` fails only for a pool
  size below one, which `ParseConfig` rejects; the stream unit of work's
  unsettled rollback, which only `runtime.Goexit` reaches; the three
  envelope checks of the workload consumers, which `Deliver` always
  satisfies.
* **Redis**: a key of the wrong type (`SET` on a dead-letter key, then on a
  partition stream) fails `XRANGE`, `XPENDING`, and `ConsumerLag` with
  WRONGTYPE, which the checks must report rather than read as empty; 25
  dead letters show the capped report.

### 8.12 Chaos re-run and benchmark baseline

The seven workloads that were not re-run after the 8.7 fixes ran on the
rebuilt image at seed 1 for 60 s each (`register`, `register-cached`,
`bank`, `bank-idempotent`, `idempotent-append`, `remote-send`,
`cache-staleness`): all pass, every checker green (I1 to I6, L1, L2, DLQ,
fencing from the database and the logs, log scan, shutdown, workload),
four or five nemeses per run across all fifteen kinds, about 1.8k
operations per run, no relay replays. With the `events` seeds of 8.8,
every workload is green on the image that carries the 8.7 fixes.

`task bench` ran for the first time and `task bench-baseline` recorded
`coverage/bench-baseline.txt` (six repetitions, quiet machine, AMD Ryzen AI
9 HX 370, 24 threads, Windows, Go 1.27). G17 holds with room:
`BenchmarkSend_DefaultChain` 296 ns/op, 280 B/op, 5 allocs/op (cold
context 283 ns), `BenchmarkSend_Core` 78 ns and 3 allocs, `BenchmarkPublish_InProcess`
36 ns and 0 allocs, `BenchmarkCache_Hit` 3.1 µs and 44 allocs, the single
behaviors between 86 and 141 ns except `CacheInvalidation` at 1.0 µs.
The directory is ignored by git, so the baseline is local to this
machine; spec 14's open question 5 leaves the reference host to CI.

### 8.13 Mutation gate, first runs

`task mutate` (gremlins v0.6.0, `unleash ./mediator --threshold-efficacy
80`, unit tests only, no build tags) ran for the first time, three times,
and the first two runs were wrong in ways worth recording.

* **Windows path bug in gremlins.** `removeModuleFromPath` keys the
  coverage profile with `filepath.Rel`, which uses backslashes on Windows,
  while mutant positions come from an `fs.FS` walk with forward slashes, so
  no file below the calling directory ever matches its coverage and every
  mutant there is "not covered"; only the root package's files (no
  separator) are tested. First run: 302 killed, 25 lived, 2768 not
  covered, efficacy 92.35 percent, mutator coverage 10.57 percent, 2.5
  minutes, and a gate that passes on a tenth of the code. Linux CI is not
  affected. The runs below used a local build with `filepath.ToSlash`
  applied to that path (one line, not in the repository; worth an upstream
  pull request).
* **Timeouts sized from a cached coverage run.** gremlins sets each
  mutant's test timeout to three times the wall time of its coverage run.
  With a warm test cache that run took 2.7 s, so every mutant got 8 s for
  a cold compile and run of its package under 24 concurrent workers, and
  266 of the first 361 mutants timed out (`behavior`, `ctl`); a gate that
  ignores timeouts would have passed on the rest. `taskMutate` now runs
  `go clean -testcache` first; the coverage run then takes about 6 s and
  the timeout about 20 s.
* **The real run** (patched build, cleared cache): 12 minutes 45 seconds,
  2018 killed, 227 lived, 57 timed out, 794 not covered, efficacy 89.89
  percent against the 80 gate, mutator coverage 73.87 percent. The 794
  uncovered mutants sit on lines only the integration tiers reach (the
  drivers of `pg` and `redisx`, `ctl`'s commands); a run with `-tags
  integration,faultinject` would need Docker under every worker and is not
  what the nightly job asks for. The 57 timeouts are mutants that hang
  their tests (22 in `behavior/behavior.go`, 6 in `pg/uow.go`, 5 in the
  cache scheduler, 4 in `httpapi/sse.go`): each of those packages runs
  cold in one to two seconds, so a mutant that takes twenty is a loop or a
  wait the mutation broke, a kill in effect that gremlins counts apart.
* **Survivors, for the triage of spec 11.3** (item 1 of the handoff): 165
  of the 227 are `CONDITIONALS_BOUNDARY` (a `<` that a `<=` passes just
  as well), 32 `CONDITIONALS_NEGATION`, 22 `ARITHMETIC_BASE`, 6
  `INCREMENT_DECREMENT`, 2 `INVERT_NEGATIVES`. By file: `validate`
  (`format.go` 19, `checkers.go` 18, `schemagen.go` 9, `compile.go` 8),
  `testkit/history` (`logscan.go` 11, `checkers.go` 9, `history.go` 7,
  `porcupine.go` 4), `redisx` (`consumers.go` 11, `lease.go` 9,
  `backoff.go` 6, `limiter.go` 5, `cache.go` 5, `keys.go` 3, others 6),
  `pg` (`relay.go` 11, `pool.go` 6, `store.go` 4, `uow.go` 3), the core
  (`mediator.go` 8, `send.go` 5, `pipeline.go` 5, `names.go` 4,
  `typed.go` 1), `retry/retry.go` 7, `behavior` (`redact.go` 5,
  `cache_invalidation.go` 3, `timeout.go` 2, `logging.go` 2,
  `cachemodel/scheduler.go` 3), `httpapi` 7, `ctl/main.go` 2,
  `testkit/workload/handlers.go` 2, `testkit/memstore` 3,
  `testkit/invariants` 2. The boundary survivors in `validate` are the
  length and range checks (`min`, `max`, `len`) whose tables test one side
  of each boundary; those are the cheapest rows to add.

### 8.14 Mutation survivors triaged (seventh session)

The 227 survivors of 8.13 were triaged one by one, plus three more that
the per-package runs exposed outside that list
(`validate/checkers.go:222:21`, `behavior/logging.go:41:16`,
`behavior/behavior.go:205:18`; a fourth, `redisx/lease.go:396:37`, lived
only in a partial log under load and the existing tests kill it). The
by-file list in 8.13 sums to 215: it names no file for 12 of the 227,
and the tables below are the complete set. Each package ran alone,
gremlins pointed at its own directory (the root package through
`-E '^[a-z]+/'`, which keeps the subdirectories out), and every survivor
ended in one of three places: killed by a new row in a unit test of the
mutant's own package, recorded as equivalent with a one-sentence reason,
or left open.
Every kill was verified by applying the mutation by hand and watching the
named test fail. The outcome: 156 killed (36 test files touched, no
source file changed), 70 equivalent, 1 open (`send.go:293:15`, taken up
below). Efficacy per package afterwards: `validate` 98.2, root `mediator`
94.5, `retry` 83.3 (its four survivors are equivalent), `ratelimit` 100,
`behavior` 99.0, `httpapi` 99.0, `pg` 98.0, `ctl` 98.6, `redisx` 91.3,
`testkit/history` 97.0, `testkit/memstore` 97.5, `testkit/workload` 100
percent. The tables below are the record, one pair per package group: a
kill names the test that fails under the mutation, an equivalent gives
the reason the mutation cannot be observed. Positions are
`file:line:column` as gremlins prints them, and a position that carries
two mutators appears once per mutator; a kill row names its mutator only
where the report did.

* **The core (root `mediator`, `retry`, `ratelimit`): 11 killed, 21
  equivalent, 1 open.** gremlins on the root package alone (a `git
  archive` export plus the five changed test files): 309 killed, 18 lived,
  1 not covered, 1 timed out (`names.go:76:29`, a hang), efficacy 94.50
  percent, 4 minutes 45 seconds; `retry`: 20 killed, 4 lived, 2 timed out
  (`retry.go:37:16` and `37:28`, loop-header hangs), 83.33 percent;
  `ratelimit`: 11 killed, 0 lived, 100 percent. A fallback run over the
  whole tree, made when the `-E` of the assignment did not take, lasted
  1 hour 21 minutes (2052 killed, 223 lived, 794 not covered, 27 timed
  out, 90.20 percent) against the mid-work state of the other packages.
  The `ratelimit` kill is amd64-specific: `int64(NaN)` is 0 on arm64.
  Files: `mediator/build_test.go` (+59), `pipeline_test.go` (+47/-3),
  `publish_test.go` (+16/-3), `retry/retry_test.go` (+19),
  `ratelimit/ratelimit_test.go` (+1).

  | File | Mutant | Killed by |
  |---|---|---|
  | `mediator.go` | `602:9` | `TestBuild_ReportsEveryProblemAtOnce` (`noUnmarshalQuery`) |
  | `mediator.go` | `744:27` | `TestBuild_DuplicateConsumersKeepRegistrationOrder` |
  | `names.go` | `90:17` | `TestBuild_DuplicateConsumersKeepRegistrationOrder` (`Names()`) |
  | `pipeline.go` | `357:17` | `TestPipeline_HandlerResultBesideError` |
  | `send.go` | `333:25` | `TestPublishAll_OrdersDurableAppends` |
  | `send.go` | `378:17` | `TestPublish_StrategyOverrideAndSuccess` |
  | `typed.go` | `75:11` | `TestPipeline_TypedBehaviorsPositionedAndNamed` |
  | `retry/retry.go` | `39:11` | `TestBackoff` ("zero base with cap stays zero") |
  | `retry/retry.go` | `61:11`, `66:4` | `TestDelay` |
  | `ratelimit/ratelimit.go` | `29:29` | `TestEmissionInterval` ("nan rate zero period") |

  | Mutant | Mutator | Reason |
  |---|---|---|
  | `envelope.go:30:7` | `CONDITIONALS_BOUNDARY` | `p <= 1` to `p < 1` differs only at `p == 1`, where `h.Sum64() % 1` is 0, the early return's value. |
  | `errors.go:145:50` | `ARITHMETIC_BASE` | `len(e.Details)+1` to `-1` only changes the size hint of `make`; a negative hint is clamped and capacity is unobservable. |
  | `mediator.go:615:59` | `CONDITIONALS_BOUNDARY` | `allInfos` sorts requests by `Name`; names are unique after `Build`'s claim check, and equal names exist only in a failed duplicate-name `Build`, whose map iteration order is random in both programs. |
  | `mediator.go:620:57` | `CONDITIONALS_BOUNDARY` | The same for notifications. |
  | `mediator.go:636:59` | `CONDITIONALS_BOUNDARY` | The same for `Requests()`. |
  | `mediator.go:648:59` | `CONDITIONALS_BOUNDARY` | The same for `Events()`. |
  | `mediator.go:712:66` | `CONDITIONALS_BOUNDARY` | `ChainFor` sorts by rank; behavior names are unique and ranks distinct after `Build`, and before `Build` `ChainFor` is documented invalid. |
  | `mediator.go:742:24` | `CONDITIONALS_BOUNDARY` | Guarded by `out[i].Group != out[j].Group` on the previous line. |
  | `names.go:38:43` | `CONDITIONALS_BOUNDARY` | `i >= 0` to `> 0` differs only when `reflect.Type.Name()` starts with `[`, which is impossible. |
  | `names.go:85:17` | `CONDITIONALS_BOUNDARY` | Guarded by `a.Kind != b.Kind`. |
  | `names.go:88:17` | `CONDITIONALS_BOUNDARY` | Guarded by `a.Name != b.Name`. |
  | `pipeline.go:244:23` | `CONDITIONALS_BOUNDARY` | Positions are unique per name; `p == lo` only when the same `After` anchor repeats, where `anchor = a` is a no-op. |
  | `pipeline.go:251:23` | `CONDITIONALS_BOUNDARY` | The same for `hi` and `Before`. |
  | `pipeline.go:292:14` | `CONDITIONALS_BOUNDARY` | `pos(a) == p` means `a == r.name`, rejected earlier by the "positioned relative to itself" check. |
  | `pipeline.go:297:14` | `CONDITIONALS_BOUNDARY` | The same for `Before` anchors. |
  | `send.go:331:14` | `CONDITIONALS_BOUNDARY` | Guarded by `ti != tj`. |
  | `send.go:369:19` | `CONDITIONALS_BOUNDARY` | With no handlers every strategy branch is a no-op and control reaches `appendDurable` either way. |
  | `retry/retry.go:24:19` | `CONDITIONALS_BOUNDARY` | `MaxAttempts < 1` to `<= 1`: at 1 both branches return 1. |
  | `retry/retry.go:33:13` | `CONDITIONALS_BOUNDARY` | `attempt < 1` to `<= 1`: at 1 the assignment sets 1. |
  | `retry/retry.go:46:26` | `CONDITIONALS_BOUNDARY` | Differs only at `d == MaxDelay`; every continuation path yields `MaxDelay`. |
  | `retry/retry.go:50:25` | `CONDITIONALS_BOUNDARY` | Differs only at `d == MaxDelay`, where both branches return the same value. |

* **`validate`: 46 killed, 8 equivalent, 0 open.** One more kill fell
  outside the list, `checkers.go:222:21` (`CONDITIONALS_NEGATION`), the
  format schema now pinned in `TestTypeMapping`. gremlins: 431 killed, 8
  lived, 17 not covered, 8 timed out, efficacy 98.18 percent, mutator
  coverage 96.27 percent, 22 minutes 5 seconds under load. Of the 8
  timeouts, the `CONDITIONALS_BOUNDARY` and `CONDITIONALS_NEGATION`
  mutants at `schemagen.go:78:20` and `78:32` fail in five to six seconds
  when re-run by hand under `-timeout 120s`, so they are kills that hit a
  load spike; `schemagen.go:132:26`, `319:11`, `343:23` and `347:10`
  (`CONDITIONALS_NEGATION`) are genuine hangs outside the list. Files:
  `check_test.go`, `compile_test.go`, `coverage_test.go`,
  `format_test.go`, `schema_test.go` (no new files). No suspected defect.
  One observation: the compiler does not cross-check a field's rules, so
  `max=0,len=3` compiles and renders `minLength` 3 with `maxLength` 0;
  `TestZeroBoundsInSchema` documents this as it is.

  | File | Mutant | Killed by |
  |---|---|---|
  | `checkers.go` | `170:13`, `176:14` | `TestStringRules` |
  | `checkers.go` | `365:13`, `371:16` | `TestSliceRules` |
  | `checkers.go` | `485:16` | `TestMapRules` |
  | `checkers.go` | `199:13`, `201:9`, `207:8`, `210:8`, `419:13`, `421:9`, `427:8`, `430:8`, `516:16`, `519:16` | `TestZeroBoundsInSchema` |
  | `compile.go` | `346:20` | `TestPlanFlags` (white-box, through `v.plan`) |
  | `compile.go` | `491:39` | `TestTypeMapping` (`ArrR`) |
  | `compile.go` | `531:21`, `531:30` | `TestStringRules` |
  | `compile.go` | `531:37` (`ARITHMETIC_BASE` and `INVERT_NEGATIVES`) | `TestCompileErrors` ("string length ceiling plus one") |
  | `format.go` | `53:26`, `79:12`, `83:35`, `96:23`, `96:35`, `96:47`, `96:59`, `96:71`, `102:38`, `102:51`, `102:51` (`CONDITIONALS_NEGATION`), `102:64`, `102:77`, `102:77` (`CONDITIONALS_NEGATION`), `109:12`, `117:35`, `120:16` | `TestFormats` rows |
  | `schemagen.go` | `78:20`, `78:32`, `78:44`, `78:56`, `78:68` | `TestStripTypeArgPackages` (`zAZ09`) |
  | `schemagen.go` | `95:10`, `103:46` | scanner rows |
  | `schemagen.go` | `132:35` | `TestSchemasNaming` (the `_3` counter) |

  | Mutant | Mutator | Reason |
  |---|---|---|
  | `checkers.go:173:14` | `CONDITIONALS_BOUNDARY` | `c.minLen >= 0` to `> 0` differs only at `minLen == 0`, where the guarded test is `n < 0`, impossible for a rune count. |
  | `checkers.go:368:16` | `CONDITIONALS_BOUNDARY` | The same for `minItems`. |
  | `checkers.go:482:16` | `CONDITIONALS_BOUNDARY` | The same for `minProps`. |
  | `compile.go:83:14` | `CONDITIONALS_BOUNDARY` | `f.single >= 0` to `> 0` differs only at `single == 0`, where the fallback `fieldByIndex(v, []int{0})` returns the same value. |
  | `compile.go:93:8` | `CONDITIONALS_BOUNDARY` | `i > 0` to `>= 0` differs only at `i == 0`, where `v` is always the struct value itself (root and `ptrChecker` dereference first), so the `Pointer` kind test is false. |
  | `format.go:113:8` | `CONDITIONALS_BOUNDARY` | `at < 0` to `<= 0` differs only at `at == 0`, where `local` is empty and line 117 rejects it the same way. |
  | `format.go:134:17` | `CONDITIONALS_BOUNDARY` | `len(domain) > 1` to `>= 1` differs only for a one-character domain, where `domain[0]` cannot be both `[` and `]`, so both fall through to `isHostname`. |
  | `schemagen.go:123:45` | `CONDITIONALS_BOUNDARY` | `i >= 0` to `> 0` differs only when `PkgPath` begins with `/`, which no Go import path can, and reflect cannot construct named types. |

* **`behavior`, `cachemodel` and `httpapi`: 22 killed, 3 equivalent, 0
  open.** Two more fell outside the list: `behavior/logging.go:41:16`
  (`CONDITIONALS_NEGATION`) is killed, since
  `TestLogging_ConsumerAndNotification` now asserts the group on the
  start record, and `behavior/behavior.go:205:18`
  (`CONDITIONALS_NEGATION`) is equivalent, the `idem.Logger` default in
  `Standard`, because `pg.IdempotencyConfig.Logger` is documented unused
  and never read. gremlins on `behavior` (which recurses into
  `cachemodel`): 201 killed, 2 lived, 21 not covered, 6 timed out,
  efficacy 99.01 percent, 4 minutes 24 seconds; `httpapi`: 197 killed, 2
  lived, 20 not covered, 3 timed out, 98.99 percent, 4 minutes 23 seconds.
  The timeouts are the known ones outside the list:
  `cachemodel/scheduler.go:120:32`, `123:32`, `168:8` (twice) and `168:18`
  (`CONDITIONALS_NEGATION`), `retry.go:100:29`, and `httpapi/sse.go:58:11`,
  `126:54`, `129:45`. Files (9, +423/-47): `behavior/internal_test.go`,
  `logging_test.go`, `cache_invalidation_test.go` (`invalidationBubble`
  now takes a `Clock`), `cachemodel/cachemodel_test.go`,
  `httpapi/handle_test.go`, `problem_test.go`, `internal_test.go`,
  `listener_test.go`, `routing_test.go`. No suspected defect. The hints
  the triage started from needed correcting: `redact.go` is a
  depth-bounded reflection walker (`maxRedactDepth` 32); `logging.go` 71
  and 86 are the stream item count; `cache_invalidation.go` 185 to 204
  are the retry attempt numbers in the log records; `problem.go` 76 and
  91 are empty-slice and empty-map guards.

  | File | Mutant | Killed by |
  |---|---|---|
  | `behavior/cache.go` | `111:16` | `TestCache_PrepareWarmsCachedQueries` |
  | `behavior/cache_invalidation.go` | `185:29`, `201:92` | `TestCacheInvalidation_RetryRecovers` (`attempts == 3`) |
  | `behavior/cache_invalidation.go` | `204:38` | `TestCacheInvalidation_RetryAttemptsAreNumbered` (`synctest` and `FakeClock`) |
  | `behavior/cachemodel/scheduler.go` | `168:18`, `212:34` | `TestScheduler_Model` |
  | `behavior/common.go` | `123:17` | `TestLogLimiter` (boundary rows at `logLimiterMaxKeys`) |
  | `behavior/logging.go` | `71:11` | `TestLogging_Stream` (empty stream, `items` 0) |
  | `behavior/logging.go` | `86:47` | `TestLogging_Outcomes` (no `items` on non-streams) |
  | `behavior/redact.go` | `63:11`, `164:44`, `165:26` | `TestRedactor_DepthLimit`, `TestRedactor_EdgeCases` |
  | `behavior/redact.go` | `100:57`, `136:37` | `TestRedactor_DepthThroughMapsAndLists` |
  | `behavior/timeout.go` | `50:23` | `TestTimeout_OnBuildRecordsExplicitTimeoutsOnly` (internal seam) |
  | `behavior/timeout.go` | `79:9` | `TestTimeoutErrorAndPhase` (identity) |
  | `httpapi/handle.go` | `171:21` | `TestDecode` (64 and 65 bytes at `MaxBodyBytes` 64) |
  | `httpapi/listener.go` | `46:11` | `TestNewListener_Drain`, `TestListener_DrainsInFlight` (drain 0) |
  | `httpapi/problem.go` | `76:43`, `91:35` | `TestProblemOf` (empty fields, empty details map, nil `Errors`) |
  | `httpapi/routing.go` | `384:32`, `384:50` | `TestBuildCheck_Violations` (status 199, 200, 299, 300) |

  | Mutant | Mutator | Reason |
  |---|---|---|
  | `behavior/cachemodel/scheduler.go:273:16` | `CONDITIONALS_BOUNDARY` | `if a.written > s.lastReturned { s.lastReturned = a.written }` to `>=`: the equal case assigns the value already there. |
  | `httpapi/handle.go:228:24` | `CONDITIONALS_BOUNDARY` | `len(rt.boundNames) > 0` to `>= 0` only decides whether `boundMembersInBody` runs with an empty map, which matches nothing and returns nil either way. |
  | `httpapi/problem.go:180:10` | `CONDITIONALS_BOUNDARY` | `if secs < 1 { secs = 1 }` to `<= 1`: at 1 the assignment is a no-op. |
  | `behavior/behavior.go:205:18` (outside the list) | `CONDITIONALS_NEGATION` | The `idem.Logger == nil` default: the field is never read. |

* **`pg` and `ctl`: 23 killed, 7 equivalent, 0 open.** gremlins on `pg`:
  191 killed, 4 lived, 221 not covered, 12 timed out, efficacy 97.95
  percent, 29 minutes 34 seconds under load; `ctl`: 218 killed, 3 lived,
  1 not covered, 0 timed out, 98.64 percent, 2 minutes 45 seconds. The 12
  `pg` timeouts are loop-breaking mutants outside the list:
  `janitor.go:74:14`, `122:34`, `122:54`; `relay.go:124:36`, `359:8`
  (`CONDITIONALS_NEGATION`), `554:11`; `uow.go:127:17`, `129:8` (twice),
  `176:17`, `178:8` (twice). Files: `pg/export_test.go` (the seams
  `SetOwnedForTest`, `TakeWake`, `LockTimeoutForTest`,
  `FencingSQLForTest`), `pg/relay_test.go` (`gaugeCalls` and `setGauges`
  on `fakeSlotStore`, `countingSink`, four new tests),
  `pg/failure_test.go` (three new tests), `pg/misc_test.go` (extended,
  plus `TestNewStore_LockTimeoutAndFencingSQL`), `ctl/ctl_test.go`
  (`TestParseDuration` rows). No suspected defect.

  | File | Mutant | Killed by |
  |---|---|---|
  | `ctl/main.go` | `431:39` | `TestParseDuration` (the exact messages for `d` and `d12h`) |
  | `pg/hooks.go` | `45:28` | `TestBeforeCommit_OutsideUnitOfWorkLogsOnlyAFailure` (`slog.Default` swap) |
  | `pg/pool.go` | `42:9`, `54:18`, `57:18`, `60:25` (twice), `63:25` | `TestConfigDefaultsAndPool` (a zero `PoolConfig` keeps the URL values; `pg: ping`) |
  | `pg/relay.go` | `249:7` (twice), `252:34`, `253:9`, `258:14`, `258:38` | `TestRelay_WakeTargetsOneOwnedSlot` (10 rows) |
  | `pg/relay.go` | `359:8`, `362:8` | `TestSlot_RunLoop_FullBatchPollsAgainAtOnce` (`synctest`) |
  | `pg/relay.go` | `384:67` | `TestSlot_RunLoop_GaugesKeepTheLastGoodReading` |
  | `pg/relay.go` | `500:49` | `TestSlot_DataLossRecovery_RecreatesOnlyKnownGroups` (`countingSink`) |
  | `pg/store.go` | `47:19` (twice), `53:14` | `TestNewStore_LockTimeoutAndFencingSQL` |
  | `pg/uow.go` | `117:37` | `TestUnitOfWork_LogsRollbackFailure` |
  | `pg/uow.go` | `191:24` | `TestUnitOfWork_LogsOnCommitHookPanic` |

  | Mutant | Mutator | Reason |
  |---|---|---|
  | `ctl/commands.go:81:79` | `CONDITIONALS_BOUNDARY` | `Changed` holds one row per migration version (`loadMigrations` rejects duplicates), so the down-sort never compares equal keys. |
  | `ctl/main.go:386:18` | `CONDITIONALS_BOUNDARY` | Every caller of `require` passes (condition, name) pairs, so `len(pairs)` is even and `i+1 < len` against `<= len` never differ. |
  | `ctl/results.go:70:7` | `CONDITIONALS_BOUNDARY` | `if d < 0 { d = 0 }` to `<=`: at `d == 0` the mutant assigns 0 to a zero. |
  | `pg/migrate.go:74:62` | `CONDITIONALS_BOUNDARY` | The sort's `<` to `<=` differs only for equal versions, rejected by the `seen` map ten lines earlier. |
  | `pg/relay.go:534:7` | `CONDITIONALS_BOUNDARY` | `parseStreamID` with `-` at index 0: `strconv.ParseUint("")` fails, so `i < 0` and `i <= 0` give identical results. |
  | `pg/store.go:111:16` | `CONDITIONALS_BOUNDARY` | `if remaining < 1 { remaining = 1 }` to `<=`: at 1 the mutant assigns the value it already has. |
  | `pg/uow.go:378:20` | `CONDITIONALS_BOUNDARY` | `opts.LockTimeout` is zero before the check (`defaultTxOptions` never sets it), so copying a zero `o.LockTimeout` changes nothing and line 382 applies `defaultLock` either way. |

* **`redisx`: 23 killed, 22 equivalent, 0 open.** gremlins: 232 killed,
  22 lived, 290 not covered, 1 timed out, efficacy 91.34 percent, 6
  minutes 47 seconds. The one timeout is `lease.go:404:36`
  (`INCREMENT_DECREMENT`), an infinite loop, outside the list. A partial
  log from before a crash showed `lease.go:412:13` and `416:10` timed out
  and `lease.go:396:37` (`INVERT_NEGATIVES`) lived; those were load
  artefacts, all killed on a quiet machine. Files (+257/-9):
  `backoff_test.go`, `cache_test.go`, `consumers_test.go`,
  `keys_test.go`, `lease_test.go`, `limiter_test.go`, `remote_test.go`,
  `streams_test.go`; `lease_test.go` gained `logCapture`, a recording
  `slog` handler. One nuance, pinned with a comment and not a defect: the
  `d >= max/2` guard of `backoff.delay` returns the cap one step early
  for an odd cap (`{3,7}` gives 3, 7, 7).

  | File | Mutant | Killed by |
  |---|---|---|
  | `backoff.go` | `25:16`, `25:8`, `44:34` | `TestBackoff_CapBoundaries`, `TestBackoff_Schedule` |
  | `cache.go` | `137:28` | `TestCacheEntry_Malformed` |
  | `consumers.go` | `96:8`, `114:8`, `348:76`, `348:79` | `TestNewConsumers_Scopes` |
  | `consumers.go` | `300:19` | `TestConsumers_IdleRun` |
  | `keys.go` | `73:21` | `TestKeys_ParseLease` |
  | `keys.go` | `115:40` | `TestStreamIDMillis` |
  | `lease.go` | `210:42` | `TestLease_BeginEndMarks` |
  | `lease.go` | `412:13`, `416:10` | `TestLeaseManager_AcquiresExactlyDesired` |
  | `lease.go` | `451:103` | `TestLeaseManager_AcquireAfterStopReleasesUnderDetachedContext` |
  | `lease.go` | `475:109` | `TestLeaseManager_ReleaseLogsOutcome` |
  | `lease.go` | `574:67` (`CONDITIONALS_NEGATION`) | `TestLeaseManager_SingleNodeOwnsAll` |
  | `limiter.go` | `69:19` | `TestLimiterArgs` |
  | `ops.go` | `59:9` | `TestDLQFields_Shape` |
  | `redisx.go` | `117:24`, `117:9` | `TestConfig_WithDefaults` |
  | `remote_server.go` | `369:82` | `TestDecodeRequest` |
  | `streams.go` | `168:19` | `TestEntry_Defaults` |

  | Mutant | Mutator | Reason |
  |---|---|---|
  | `backoff.go:20:11` | `CONDITIONALS_BOUNDARY` | `retry < 1` to `<= 1`: at `retry == 1` the body assigns `retry = 1`. |
  | `backoff.go:30:7` | `CONDITIONALS_BOUNDARY` | `d > max` to `>=`: at `d == max` both branches return `max`. |
  | `backoff.go:40:7` | `CONDITIONALS_BOUNDARY` | `d <= 0` to `< 0`: at `d == 0` the fall-through computes `Duration(0*f)`, which is 0. |
  | `cache.go:132:57` | `CONDITIONALS_BOUNDARY` | `len < 7` to `<= 7`: the only 7-byte entry with the prefix and a closing brace is `{"v":{}`, which the later `,"b":` check rejects with the same error. |
  | `cache.go:132:68` | `ARITHMETIC_BASE` | `len(head)+1` to `-1`: an entry with the 6-byte prefix ending in `}` has at least 7 bytes anyway. |
  | `cache.go:161:11` | `CONDITIONALS_BOUNDARY` | `endv >= 0` to `> 0`: `endv` is -1 or at least 6, never 0. |
  | `cache.go:165:10` | `CONDITIONALS_BOUNDARY` | `endv < 0` to `<= 0`: the same range argument. |
  | `consumers.go:265:29` | `CONDITIONALS_BOUNDARY` | `group < group` to `<=` is only reached after line 264 established that the groups differ. |
  | `consumers.go:267:28` | `CONDITIONALS_BOUNDARY` | Scopes are deduplicated by (group, topic), so same-group scopes never share a topic, and `<` and `<=` agree on distinct strings. |
  | `consumers.go:894:40` | `CONDITIONALS_BOUNDARY` | `i >= 0` to `> 0` in `splitStreamID`: an ID starting with `-` fails `ParseUint` either way. |
  | `consumers.go:911:7` | `CONDITIONALS_BOUNDARY` | `i < 0` to `<= 0` in `nextStreamID`: at `i == 0` the original parses `id[:0]`, which is rejected, so both return `("", false)`. |
  | `consumers.go:1172:39` | `ARITHMETIC_BASE` | `make(map, len+7)` to `len-7`: a capacity hint only; a negative hint is clamped by the runtime (probed). |
  | `consumers.go:1180:14` | `CONDITIONALS_BOUNDARY` | `len(msg) > 4096` to `>=`: at 4096 `msg[:4096]` is the identity. |
  | `keys.go:69:7` | `CONDITIONALS_BOUNDARY` | `i < 0` to `<= 0` in `ParseLease`: with `i == 0` `rest` is empty and the later `j <= 0` check rejects the key anyway. |
  | `lease.go:326:8` | `CONDITIONALS_BOUNDARY` | `n < 1` to `<= 1`: at `n == 1` the body assigns `n = 1`. |
  | `lease.go:395:72` | `CONDITIONALS_BOUNDARY` | `mine` holds one scope's leases keyed by `leaseKey` with distinct partitions, so the sort never compares an element with itself. |
  | `lease.go:574:67` | `CONDITIONALS_BOUNDARY` | Snapshot keys are distinct and `leaseKey.String()` is injective (`NamePattern` forbids `/`), so the sort never compares an element with itself. |
  | `limiter.go:61:14` | `CONDITIONALS_BOUNDARY` | `interval < 1us` to `<=`: at exactly 1 µs the body assigns 1 µs. |
  | `limiter.go:65:14` | `CONDITIONALS_BOUNDARY` | `capacity < 1` to `<=`: at 1 the body assigns 1. |
  | `limiter.go:69:48` | `CONDITIONALS_BOUNDARY` | `e > expire` to `>=`: at `e == expire` the assignment is a no-op. |
  | `limiter.go:72:12` | `CONDITIONALS_BOUNDARY` | `expire < 1s` to `<=`: at exactly 1 s the body assigns 1 s. |
  | `streams.go:63:22` | `CONDITIONALS_BOUNDARY` | `len(env.Headers) > 0` to `>= 0`: json/v2 marshals a nil or empty map as `{}`, the literal the original uses. |

* **The `testkit` family: 31 killed, 9 equivalent, 0 open.** gremlins on
  `testkit/history`: 161 killed, 5 lived, 16 not covered, 0 timed out,
  efficacy 96.99 percent; `testkit/memstore`: 77 killed, 2 lived, 2 not
  covered, 2 timed out (`locks.go:31:13` and `47:15`, outside the list),
  97.47 percent; `testkit/workload`: 24 killed, 0 lived, 47 not covered,
  100 percent; `testkit/invariants` was not re-run, since both of its
  survivors are equivalent and nothing changed there. A final run on the
  `testkit` root, which recurses into every subpackage, took 15 minutes 3
  seconds: 280 killed, 9 lived, 217 not covered, 8 timed out, efficacy
  96.89 percent, mutator coverage 57.11 percent; the 9 that lived are
  exactly the 9 equivalents, and `clock.go:95:7` is killed. The 8
  timeouts under the whole-tree load: `history/logscan.go:110:32`,
  `110:44`, `115:11`, `115:26`; `memstore/locks.go:31:13`, `47:15`; and
  `memstore/memstore.go:102:20` with both `CONDITIONALS_NEGATION` and
  `CONDITIONALS_BOUNDARY`, of which the boundary one is killed in the
  `memstore`-only run and by hand. Files: `testkit/clock_test.go`;
  `testkit/history/history_test.go` (an `oneShotReader` helper, two new
  tests, eight extended); `testkit/memstore/memstore_test.go`
  (`TestNewDefaults`); `testkit/workload/workload_test.go`
  (`recordingStore`, `recordingTx` and `recordingPgxTx` fakes,
  `TestRegister_NodeID`). No suspected defect.

  | File | Mutant | Killed by |
  |---|---|---|
  | `testkit/clock.go` | `95:7` | `TestFakeClock_Set` |
  | `history/checkers.go` | `39:71`, `74:12`, `98:70`, `137:15` | `TestAppendListChecker_Boundaries` |
  | `history/checkers.go` | `179:31`, `182:33`, `227:51`, `240:71` | `TestCheckStaleness_Boundaries` |
  | `history/history.go` | `185:13` | `TestRecorder_OKFailAndExtra` |
  | `history/history.go` | `298:30`, `298:40`, `298:45`, `302:7`, `318:64` | `TestJSONL_Errors` |
  | `history/history.go` | `339:29` | `TestBoundedStaleness` |
  | `history/logscan.go` | `57:13`, `58:28`, `60:25`, `63:11`, `63:24` | `TestLogScan` |
  | `history/logscan.go` | `92:12`, `177:14` (`CONDITIONALS_NEGATION`) | `TestFencingFromLogs` |
  | `history/porcupine.go` | `60:71` | `TestToPorcupine` |
  | `history/porcupine.go` | `226:19`, `258:30`, `261:25` | `TestBankModel` |
  | `history/values.go` | `110:39` | `TestValues` |
  | `memstore/memstore.go` | `102:20` | `TestNewDefaults` |
  | `workload/handlers.go` | `48:17`, `51:17` | `TestRegister_NodeID` (a recording `pgx.Tx` through a fake `pg.Store` under `pg.WithTx`) |

  | Mutant | Mutator | Reason |
  |---|---|---|
  | `history/checkers.go:140:15` | `CONDITIONALS_BOUNDARY` | The branch only does `last = pos[0]`; when `pos[0] == last` the assignment is a no-op. |
  | `history/logscan.go:43:21` | `ARITHMETIC_BASE` | The initial value of `last` is dead: `last` is only read when `first >= 0`, and every assignment of `first` also assigns `last`. |
  | `history/logscan.go:43:21` | `INVERT_NEGATIVES` | The same reason. |
  | `history/logscan.go:140:21` | `CONDITIONALS_BOUNDARY` | With zero records `timed` only gates a sort of an empty slice and the loop has nothing to check; both return nil. |
  | `history/logscan.go:177:14` | `CONDITIONALS_BOUNDARY` | The branch only does `s.max = r.Token`; when equal the assignment is a no-op. |
  | `invariants/checks.go:452:12` | `CONDITIONALS_BOUNDARY` | `capSeqs`: when `len(s) == maxDetails`, `s[:maxDetails]` is the same slice. |
  | `invariants/invariants.go:218:14` | `CONDITIONALS_BOUNDARY` | `sortedIDs`: the same, `out[:maxDetails]` is identical to `out`. |
  | `memstore/memstore.go:161:57` | `CONDITIONALS_BOUNDARY` | Outbox IDs are unique (`nextID++` under the store lock), so the sort comparator never sees equal IDs. |
  | `memstore/streams.go:201:61` | `CONDITIONALS_BOUNDARY` | `Truncate` with `keep == len(entries)` reslices to itself. |

* **The open one, `send.go:293:15`.** Decided a defect and fixed. `Publish`
  installed the call's `publishOptions` in the context only when options
  were passed (`if len(opts) > 0`), and the context that reaches an
  in-process handler still carries the enclosing call's options, so a
  nested `Publish` made from a handler without options inherited the outer
  call's `Strategy` and `Headers` on every path that reads them (the
  fan-out strategy, the fan-out headers, and the unregistered-durable
  append). The docs on `PublishOption` and `Strategy` say an option
  configures one call, and spec 4.4 says the strategy is "overridden per
  call". The `CONDITIONALS_BOUNDARY` mutant (`>=`, install the call's own
  option set unconditionally) is what the code should have said, minus its
  cost: `context.WithValue` on every call would add two allocations to a
  path that `BenchmarkPublish_InProcess` pins at 36 ns and 0 allocs, and
  `task bench` gates sec/op at 10 percent. The fix keeps the `>` and adds a
  second case: when the call passes no options and the context already
  carries an option set, install the package-level zero set
  (`noPublishOptions`: no strategy override, nil headers) instead, so the
  mask costs one context frame and only in the nested case; a `Publish` on
  a context without options still allocates nothing. `PublishAll` calls
  `Publish` per event with the same options, so it is covered. Pinned by
  `TestPublish_NestedCallDoesNotInheritOptions` (`publish_test.go`, three
  subtests: strategy, headers, unregistered durable; each fails with the
  masking case deleted) and `TestPublish_InProcessAllocations`
  (`bench_test.go`, `testing.AllocsPerRun` budget 0 beside
  `TestSend_CoreAllocations`; fails under the `>=` mutant with 2 allocs).
  The allocation test boxes the event into `Notification` once outside the
  measured closure: a struct captured by a closure is boxed on every call,
  which is the caller's allocation, while the benchmark's constant literal
  is folded to a static and shows none. The benchmark after the fix:
  35.6 to 35.9 ns and 0 allocs over three runs, against a baseline of 35.7
  to 36.3. No test anywhere in the module depended on the inherited
  behaviour. This is the first source change of the whole triage.
* **The full run after the triage.** `task mutate` once, alone on the
  machine, with the patched gremlins of 8.13 first on `PATH`, the test
  cache cleared, and the `Publish` fix in the tree: 12 minutes 36 seconds,
  2167 killed, 70 lived, 67 timed out, 793 not covered, efficacy 96.87
  percent (89.89 in 8.13), mutator coverage 73.83 percent, exit 0 against
  the 95 gate. The 70 that lived are exactly the 70 equivalents of the
  tables above: the full run prints positions relative to `./mediator`, so
  `backoff.go:20:11` appears as `redisx/backoff.go:20:11`, and the fix
  moved the two `send.go` equivalents down by 11 lines, to `342:14` and
  `380:19`. The seventy-first, `behavior/behavior.go:205:18`, timed out
  this time instead of living. No mutant in `send.go` lived. The 67
  timeouts (57 in 8.13) by file: `behavior/behavior.go` 19,
  `behavior/common.go` 9, `behavior/logging.go` 7, `pg/uow.go` 6,
  `behavior/cachemodel/scheduler.go` 5, `httpapi/sse.go` 4, `pg/janitor.go`
  3, `pg/relay.go` 3, `behavior/authorization.go` 2, `retry/retry.go` 2,
  `testkit/memstore/locks.go` 2, and one each in `authz/authz.go`,
  `behavior/retry.go`, `names.go`, `redisx/lease.go`,
  `validate/schemagen.go`. Among them are `behavior/common.go:123:17`,
  `behavior/logging.go:41:16`, `71:11` and `86:47`, which the tables above
  record as kills in the per-package runs: under the full run's 24 workers
  a kill can present as a timeout, which gremlins counts against neither
  side, so it lowers the killed count and the figure is conservative. The
  793 uncovered mutants are still the integration-only lines of 8.13. The
  gate is raised from 80 to 95 in `taskMutate` (`tools/task/tasks.go`),
  the assertion in `tools/task/app_test.go`, and the `nightly-mutate` job
  name in `.github/workflows/ci.yml`; spec 11's 80 was "rising as the suite
  matures". The log of the run is not in the repository.
* **Facts for the next run.** `gremlins -E` matches paths relative to the
  target directory, so `-E '^[a-z]+/'` restricts `./mediator` to the root
  package. A per-package run needs `--timeout-coefficient 30`, because
  the timeout is sized from a coverage run that takes under a second (the
  sizing rule of 8.13). In Git Bash the gobin goes on `PATH` in its
  `/c/...` form. Concurrent runs make kills show as TIMED OUT, which
  gremlins does not count against efficacy, so run one package at a time.
  On Windows gremlins cannot remove its temporary folder while a
  `.test.exe` is still held, which prints an "Access is denied" line
  after the summary; it is harmless. Two agents sharing one working tree
  collided on `.git/index.lock`, which broke a `git checkout` restore of a
  test file; the agent switched to restoring from backup copies and the
  tree was re-checked clean.

### 8.15 First CI run and its findings (ninth session)

The eighth session pushed `b084ebb` to github.com/t3stackcoder/go-api-backend
and the workflow ran for the first time (run 36318721586 on the Actions tab).
The openapi job and tier 4 integration (testcontainers on a runner that had
never seen the project) passed; three jobs failed; the fault sweep was
cancelled after 34 minutes at the user's request, along with the two
follow-up runs. The findings, all in test code or comments except one
decoder line, were fixed in the ninth session and verified here with the
same tools CI uses, since the local tree had been reset to `31504d4` and
was fast-forwarded back to `9989273` first.

* **Tier 0 static, golangci-lint v2.14.0 with `.golangci.yml`, three
  findings.** `misspell`: `modelling` in the `Rollback` comment of
  `pg/tx.go:80`, now `modeling` (the config's locale is US). staticcheck
  QF1008 twice in `testkit/netfault/netfault.go` (`139`, `146`): `conn`
  embeds `net.Conn` and overrides only `Read` and `Write`, so
  `c.Conn.Close()` and `c.Conn.SetReadDeadline(...)` are `c.Close()` and
  `c.SetReadDeadline(...)`. gofmt, vet, staticcheck, and govulncheck had
  passed in the same job. Locally: golangci-lint v2.14.0 built from source
  with Go 1.27 into a scratch `GOBIN` reports 0 issues; staticcheck 2026.2.1
  reports nothing.
* **Tier 1 unit under `-race`, four data races.** The race detector's first
  run anywhere (this machine has no C compiler). Every one is a test
  sharing a variable or a fake with a goroutine of the code under test,
  and three of the four are in tests the seventh session's triage extended.
  The fixes:
  * `behavior` `TestCacheInvalidation_RetryRecovers`: `invalidationBubble`
    handed out a `*bool` that the test flips between virtual sleeps while
    the invalidation retry goroutine reads it through `Memory.Fail`. The
    race detector does not treat synctest's durable blocking or a timer
    firing in virtual time as synchronization, so the flag is now an
    `atomic.Bool` (the same fix for the three sibling tests that use the
    helper).
  * `httpapi` `TestHealth/readyz with passing checks`: `runReadyChecks`
    runs each check in its own goroutine and two checks did `calls++` on
    one int. Now `atomic.Int32`.
  * `pg` `TestSlot_RunLoop`: the test reassigned `sink.Hooks.Append` while
    the slot goroutine was in its backoff timer, and `memstore.Streams`
    reads its hooks without its lock. That is by design, a set-before-use
    seam, and is now said in the `StreamHooks` doc; the test installs one
    hook before the slot starts and switches the failure with an
    `atomic.Bool`. The other hook assignments in tests either run with no
    goroutine alive or precede a `Wake`, whose channel send is an edge.
  * `redisx` `TestLeaseManager_RunLoop`: the test read
    `store.calls["renew"]` without the fake's mutex while the manager's tick
    wrote `calls["beat"]` under it. `fakeLeaseStore.count(op)` reads under
    the lock.

  Verification: `go test -race -shuffle=on -count=2 ./...`, the unit job's
  exact command, in a `golang:1.27` Linux container (go1.27.1) with the
  repository and the module cache bind-mounted: 24 packages `ok`, no
  `DATA RACE`, 61 seconds end to end. The command is in handoff section 4;
  it replaces "push and let CI check".
* **Tier 2 short fuzz: one crasher in CI, a second one here, both in the
  decoder.** `FuzzEnvelopeDecode` found in CI, after 29 seconds, an entry whose `at` field is
  `0000-01-01T0:00:00+00:00`. `time.Parse` keeps a numeric offset as the
  `Local` location when the offsets agree (the runner's local zone is UTC)
  or as a `FixedZone` otherwise, never as `UTC` itself, while `EncodeEntry`
  writes `OccurredAt.UTC()` and a `Z` suffix parses to the UTC location. So
  the decoded envelope and its re-decoded twin were the same instant with
  different `Location` pointers: `reflect.DeepEqual` false, `%+v` identical,
  which is why the failure message showed two equal lines. `DecodeEntry` now
  returns `t.UTC()`, matching the field's documented form ("RFC3339Nano",
  written in UTC). Only an entry written by another producer with a non-`Z`
  offset ever saw the difference; the framework's own entries always
  carried `Z`. The crasher is committed as
  `mediator/redisx/testdata/fuzz/FuzzEnvelopeDecode/5515dd0f123bbdbc`
  (spec 11.3: any crash is a regression test) and failed on this machine
  before the fix (local zone is not UTC, so the `FixedZone` path) and
  passes after. With that fixed, a local `task fuzz -fuzztime 30s` found
  the next one in 13 seconds: `at` = `0000-01-01T0:00:00+01:00`, which is
  23:00 on the last day of year -1 once in UTC. RFC 3339 writes four-digit
  years, so `EncodeEntry` produced `-0001-12-31T23:00:00Z` and the
  re-decode failed with a parse error. That was already so before the
  `.UTC()` change, since the encoder formatted in UTC all along, and it
  happens at the other end too (`9999-12-31T23:59:59-01:00` is year 10000).
  `DecodeEntry` now rejects an `at` whose UTC year is outside 0000 to 9999
  with `ErrBadEntry`: the field is defined as RFC3339Nano, an instant the
  format cannot carry is malformed like any other bad field, and the
  consumer dead-letters it. Only garbage reaches that branch; the
  framework's own entries carry a `Z` and a real year. Pinned by
  `TestEntry_OccurredAtUTC` (the UTC location whatever the offset; years
  0000 and 9999 decode and format back unchanged, which kills the boundary
  mutants of the new check) and two `TestEntry_Garbage` rows; the second
  crasher is committed beside the first as `.../20a0251787e92bfe`. The six
  targets then ran 30 seconds each here without a finding. The two other
  hashed files in the run's `fuzz-crashers`
  artifact (`FuzzProblemJSON/d8889adbbe058a4f`,
  `FuzzRequestDecode/8adde3e8e5de680f`) are committed corpus entries that
  the artifact's `**/testdata/fuzz/**` glob swept up, not new crashers; the
  other five targets passed.
* **The fault sweep job moved to manual dispatch** (section 6). Spec 11.3
  runs tier 3 on every push; the first run showed what that costs (the job
  was at 34 minutes when it was cancelled, cap 75) and the user had asked,
  in the same session, for the nightly schedule to go because there is
  nothing to soak yet. The `sweep` job now carries the same
  `workflow_dispatch && inputs.nightly` condition as the long tiers. Later
  in the session, at the user's direction, the push and pull-request
  triggers went too: the workflow now runs only when dispatched by hand,
  the five short jobs on every dispatch and the long tiers when
  `nightly=true` is ticked (section 6). The dispatch input is still named
  `nightly` (its description lists the fault sweep now) so the documented
  `nightly=true` keeps working.
* **State of the two copies.** GitHub holds `9989273`; the local clone is
  one commit ahead with these fixes and has no remote configured (the user
  had it removed in the eighth session). Nothing was pushed in the ninth
  session; the push command is in handoff section 3.
