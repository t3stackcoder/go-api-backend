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
