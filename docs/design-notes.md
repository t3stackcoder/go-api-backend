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
