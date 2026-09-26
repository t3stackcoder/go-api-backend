# Mediator: a MediatR-style CQRS framework for Go

| | |
|---|---|
| Status | Draft v0.2, for review (v0.1 audited and amended 2026-09-26) |
| Date | 2026-09-26 |
| Repository | `go-api-backend` |
| Module path | `github.com/OWNER/go-api-backend` (replace `OWNER` before first commit) |
| Toolchain | Go 1.27 (`go 1.27` in `go.mod`; `encoding/json/v2` requires it), Postgres 18, Redis 8 (8.10 at time of writing), Docker Compose v2 |

This document is the complete specification. It covers the public API, the persistence and transport protocols, the HTTP and OpenAPI adapter, the operational surface, the precise guarantees the framework makes, and the test program that verifies every one of those guarantees. Implementation follows this document; where the two disagree, this document wins until it is amended.

---

## 0. Decisions already confirmed

These were agreed before writing and are treated as fixed:

1. **Cross-process dispatch is in scope.** A command sent on node A may be handled on node B over Redis Streams. Local dispatch remains the default and fastest path.
2. **No event sourcing.** The write model is state-stored in Postgres. Events flow through a transactional outbox to drive projections and integrations. Aggregate replay is a non-goal.
3. **Greenfield.** No existing module path, logging library, or telemetry conventions to honor. Standard library `log/slog` and OpenTelemetry are used.
4. **Spec lives in the repo** as this Markdown file.
5. **OpenAPI 3.1 is generated code-first** from the mediator's own registry, served with an embedded UI, committed to the repo, and checked for drift in CI. The frontend generates TypeScript from the committed document. OpenAPI 3.2 (September 2025) is not adopted until `openapi-typescript` and Scalar support it; see 8.6 and 15.1.
6. **Behaviors ship as a standard set** with a fixed default order: recovery, tracing, logging, metrics, timeout, authorization, rate limiting, validation, caching, retry, unit of work, idempotency.
7. **Testing follows two traditions at once.** SQLite-style exhaustive fault sweeps, crash sweeps, fuzzing, and coverage gates on the one hand. Jepsen-style multi-node chaos runs with recorded histories and linearizability checking on the other.

---

## 1. Purpose, goals, non-goals

### 1.1 Purpose

Provide Go services with the same programming model MediatR gives .NET services: every use case is a typed request with exactly one handler, cross-cutting concerns are composable pipeline behaviors, and domain events fan out to zero or more handlers. Extend that model with the durable and distributed pieces a real service needs, namely transactions, a transactional outbox, exactly-once-effect consumers, idempotent commands, caching, and cross-process dispatch, and make every guarantee those pieces claim falsifiable by a test.

### 1.2 Goals

- **Type safety at the edges.** Sending `CreateOrder` returns `CreateOrderResult`. Registering a handler with the wrong signature fails to compile. Internally the pipeline is type-erased for simplicity and speed.
- **Explicit over magic.** Handlers and behaviors are registered in code. There is no assembly scanning, no reflection-based discovery, and no global mediator instance.
- **One source of truth for a request's shape.** The Go struct defines the JSON body, the validation rules, and the OpenAPI schema. They cannot drift because they are produced by the same interpreter.
- **Guarantees stated precisely.** Section 10 lists every guarantee, its preconditions, and the test that would catch its violation.
- **Operable.** Metrics, health checks, and a CLI for the outbox, inbox, idempotency store, and dead-letter queues.
- **Fast local path.** Local `Send` with the full default behavior chain and no I/O behaviors active must cost on the order of a microsecond and a handful of allocations.

### 1.3 Non-goals

- Event sourcing, aggregate replay, snapshots, upcasters.
- Sagas or process managers as a first-class construct. They can be built as durable consumers that send commands.
- Multi-region or multi-Postgres topologies. One Postgres primary is the single source of truth.
- Redis Cluster in v1. Partition-scoped keys use hash tags so a later move is possible; the cache scripts would need the change described in 7.4. Single node or Sentinel is supported.
- Kafka, NATS, or RabbitMQ transports. The transport interface allows them later.
- Authentication. The HTTP adapter accepts a pluggable authenticator; token parsing is the application's job.
- A dependency injection container. Handlers are plain values; use constructor injection.

---

## 2. Glossary

| Term | Meaning |
|---|---|
| Request | A command, query, or stream request. Exactly one handler. |
| Command | A request that mutates state. Runs inside a unit of work. May publish events. Returns a result or `Void`. |
| Query | A request that reads state. Runs in a read-only transaction by default. May be cached. Cannot publish durable events. |
| Stream request | A query whose result is a sequence delivered incrementally. |
| Notification | A message with zero or more handlers. Called an event when it describes something that happened. |
| In-process handler | A notification handler that runs synchronously inside the publisher's call and transaction. |
| Durable consumer | A notification handler that runs on any node, fed by the outbox through Redis Streams, with at-least-once delivery and inbox deduplication. |
| Behavior | Middleware wrapping a handler. Ordered. May short-circuit. |
| Trait | An optional interface a request type implements to opt into a behavior's feature, such as `CacheTags()` or `RetryPolicy()`. |
| Unit of work (UoW) | The Postgres transaction that wraps a command handler, its outbox writes, its idempotency row, and, on the consumer side, its inbox row. |
| Outbox | Table of events written in the same transaction as the state change that produced them. |
| Relay | The process that moves outbox rows to Redis Streams and marks them published. |
| Inbox | Table recording which events a consumer group has already processed. |
| Stream key | The ordering and partitioning key of a durable event, typically an aggregate ID. |
| Partition | One of P Redis streams per topic. Events with the same stream key always land in the same partition. |
| Lease | A time-bounded claim by one node on one partition for one consumer group. |
| Fencing token | Monotonic integer issued with a lease, exposed to handlers that need to defend external systems against stale owners. |
| Remote dispatch | Sending a command whose handler lives on another node, over Redis Streams with a correlated reply. |
| History | Jepsen term. The recorded list of every operation a chaos run invoked, with invoke and return times and outcomes. |
| Nemesis | Jepsen term. The component that injects faults during a chaos run. |
| Checker | Jepsen term. A function from a history to pass or fail. |

---

## 3. Architecture

### 3.1 Overview

```
                 HTTP / SSE                       other nodes
                     |                                 |
             +-------v--------+               +--------v--------+
             |   httpapi      |               |  redisx.Remote  |  (request/reply over Streams)
             |  openapi docs  |               +--------+--------+
             +-------+--------+                        |
                     |            Send / Publish / Stream
             +-------v-------------------------------------v-------+
             |                     mediator                        |
             |  registry -> per-type behavior chain -> handler     |
             |  recovery tracing logging metrics timeout authz     |
             |  ratelimit validation cache retry uow idempotency   |
             +----+------------------+------------------------+----+
                  |                  |                        |
          +-------v------+   +-------v--------+      +--------v---------+
          | pg.UnitOfWork|   | redisx.Cache   |      | in-process       |
          | outbox write |   | tag versions   |      | notification     |
          | inbox check  |   | rate limiter   |      | handlers         |
          | idempotency  |   | leases         |      +------------------+
          +-------+------+   +-------+--------+
                  |                  |
          +-------v------+   +-------v--------+
          |  Postgres 18 |   |    Redis 8     |
          |  (truth)     |   |  (transport,   |
          +-------^------+   |   cache)       |
                  |          +---^--------^---+
          +-------+------+       |        |
          |  pg.Relay    +-------+        |
          | outbox->XADD |                |
          +--------------+       +--------+---------+
                                 | redisx.Consumers |
                                 | lease partitions |
                                 | XREADGROUP       |
                                 | handler in UoW   |
                                 | inbox dedup, XACK|
                                 +------------------+
```

Postgres is the source of truth for state, outbox, inbox, and idempotency. Redis is a transport and a cache. Nothing in Redis is required for correctness; losing all of Redis is a recoverable incident, not data loss (see 7.7).

### 3.2 Repository layout

```
go-api-backend/
  go.mod                        module github.com/OWNER/go-api-backend
  spec.md                       this document
  Makefile                      dev, test, chaos, openapi, coverage targets
  api/openapi.json              committed OpenAPI document of the example service
  deploy/
    docker-compose.yml          postgres, redis, toxiproxy, chaos nodes (profiles)
    Dockerfile                  multi-stage build of cmd/ binaries
    toxiproxy.json              proxy definitions
    postgres/init.sql           roles and extensions
  migrations/                   embedded SQL, one file per version
  mediator/                     core: markers, registry, Send/Publish/Stream, pipeline
  mediator/behavior/            built-in behaviors, one file each
  mediator/validate/            tag grammar, validator, JSON Schema mapping
  mediator/authz/               principal, requirements
  mediator/retry/               retry.Policy (leaf package, imported by request types)
  mediator/ratelimit/           ratelimit.Policy (leaf package, imported by request types)
  mediator/pg/                  pool, UoW, outbox, inbox, idempotency, relay, migrate
  mediator/redisx/              streams consumer, leases, cache, ratelimit, remote
  mediator/httpapi/             routing, binding, problem+json, SSE
  mediator/openapi/             document generation from the registry
  mediator/otel/                tracing and metrics glue
  mediator/testkit/             fault points, fakes, clock, history
  mediator/testkit/invariants/  I1 to I7, shared by the fault sweep and chaos tiers
  cmd/mediatorctl/              operations CLI
  cmd/chaosnode/                node binary for chaos runs
  examples/orders/              sample service using everything
  test/integration/             real Postgres and Redis via testcontainers
  test/faultsweep/              exhaustive fault and crash sweeps
  test/chaos/                   Jepsen-style harness: generators, nemeses, checkers
  tools/covergate/              per-package coverage thresholds
  tools/task/                   Go implementation of every Makefile target (12.2)
```

Package names are short and import as `mediator`, `behavior`, `pg`, `redisx`, `httpapi`, `openapi`. The `redisx` name avoids shadowing the go-redis package.

### 3.3 Dependencies

| Need | Choice | Why |
|---|---|---|
| Postgres driver | `github.com/jackc/pgx/v5` | Native protocol, `pgxpool`, `LISTEN/NOTIFY`, copy support |
| Redis client | `github.com/redis/go-redis/v9` | Streams, Lua, hooks for fault injection, Redis 8 commands |
| UUIDs | `github.com/google/uuid` | UUIDv7 for time-ordered event and correlation IDs. Generated in Go rather than with Postgres 18's `uuidv7()` because the envelope needs the ID before the insert |
| Telemetry | `go.opentelemetry.io/otel` | Tracing and metrics |
| Logging | `log/slog` | Standard library |
| Router | `net/http.ServeMux` | Method and pattern routing since Go 1.22, no framework |
| JSON | `encoding/json/v2`, `encoding/json/jsontext` | Standard library, stable since Go 1.27. Strict defaults (case-sensitive names, duplicate names and invalid UTF-8 rejected) suit the HTTP decoder and the canonical hasher; `jsontext` supplies the token stream the hasher needs |
| Validation | own package `mediator/validate` | Single interpreter for runtime rules and OpenAPI constraints (see 5.8) |
| Linearizability | `github.com/anishathalye/porcupine` | Go equivalent of Knossos, with HTML visualization |
| Property tests | `pgregory.net/rapid` | Shrinking generators |
| Containers | `github.com/testcontainers/testcontainers-go` | Integration tier |
| Fault proxy | `github.com/Shopify/toxiproxy/v2/client` | Chaos tier network faults |
| JSON Schema check | `github.com/santhosh-tekuri/jsonschema/v6` | Proves validator and schema agree |
| Mutation testing | `github.com/go-gremlins/gremlins` | Approximates branch-coverage rigor |

No web framework, no DI container, no ORM.

### 3.4 Runtime components and lifecycle

The `*mediator.Mediator` is immutable after `Build()`. Everything with a goroutine lives in a separate component with a `Run(ctx) error` method:

| Component | Package | Role |
|---|---|---|
| `pg.Relay` | pg | Moves outbox rows to Redis, one goroutine per owned relay slot |
| `redisx.Consumers` | redisx | Owns partition leases, reads streams, runs durable handlers |
| `redisx.RemoteServer` | redisx | Serves remote dispatch requests for locally registered handlers |
| `redisx.ReplyReader` | redisx | Delivers remote replies to waiting callers on this node |
| `pg.Janitor` | pg | Retention sweeps for outbox, inbox, idempotency; trims Redis streams through an optional `StreamTrimmer` implemented by `redisx` (6.8) |
| `httpapi.Server` | httpapi | HTTP listener |

`mediator.Runtime` composes them with an `errgroup` and enforces shutdown order: stop accepting HTTP, stop accepting remote requests, wait for in-flight requests up to a drain timeout, stop consumers after the current message is acknowledged, stop the relay after the current batch is marked, release leases, close pools. Every component exposes `Healthy() error` for readiness.

---

## 4. Core mediator (`mediator`)

### 4.1 Request markers

A request is a struct that embeds one marker. The marker carries the response type and the kind. Embedding is the only way to satisfy the `Request` interface because its methods are unexported.

```go
package mediator

type Kind uint8

const (
    KindCommand Kind = iota + 1
    KindQuery
    KindStream
)

// Void is the response type of commands that return nothing.
type Void struct{}

// Command marks a request that mutates state and returns R.
type Command[R any] struct{}

func (Command[R]) mediatorRequest(R)   {}
func (Command[R]) mediatorKind() Kind  { return KindCommand }

// Query marks a request that reads state and returns R.
type Query[R any] struct{}

func (Query[R]) mediatorRequest(R)   {}
func (Query[R]) mediatorKind() Kind  { return KindQuery }

// Request is satisfied only by types that embed Command[R] or Query[R].
type Request[R any] interface {
    mediatorRequest(R)
    mediatorKind() Kind
}
```

Usage:

```go
type CreateOrder struct {
    mediator.Command[CreateOrderResult]

    CustomerID string      `json:"customerId" validate:"required,uuid"`
    Lines      []OrderLine `json:"lines"      validate:"required,min=1,max=100,dive"`
}

type CreateOrderResult struct {
    OrderID string `json:"orderId"`
}

type GetOrder struct {
    mediator.Query[OrderView]
    OrderID string `json:"orderId" path:"orderId" validate:"required,uuid"`
}
```

Rules:

- Requests are passed by value. A pointer to a registered request type is accepted by `Send` and dereferenced.
- The marker contributes no JSON fields and is skipped by validation and schema generation.
- A request type embeds exactly one marker. `Build()` rejects types that embed both.
- Response types must round-trip through JSON. This is required by idempotency replay, caching, remote dispatch, and the HTTP adapter. `Build()` verifies it by encoding and decoding the zero value.

### 4.2 Handlers and registration

```go
type Handler[Q Request[R], R any] interface {
    Handle(ctx context.Context, req Q) (R, error)
}

type HandlerFunc[Q Request[R], R any] func(context.Context, Q) (R, error)

func (f HandlerFunc[Q, R]) Handle(ctx context.Context, q Q) (R, error) { return f(ctx, q) }

// Handle registers h as the single handler for Q. Returns an error if Q is
// already registered, if Q is a pointer type, or if the mediator is built.
func Handle[Q Request[R], R any](m *Mediator, h Handler[Q, R]) error

// HandleFunc is the closure form. Type arguments are inferred from the function signature.
func HandleFunc[Q Request[R], R any](m *Mediator, f func(context.Context, Q) (R, error)) error

func MustHandle[Q Request[R], R any](m *Mediator, h Handler[Q, R])
```

Type inference: `HandleFunc(m, func(ctx context.Context, c CreateOrder) (CreateOrderResult, error) {...})` infers both type arguments from the literal. `Handle(m, &createOrderHandler{})` infers them from the method set of the handler, which Go supports since 1.21. When inference fails for an unusual shape, instantiate explicitly: `Handle[CreateOrder, CreateOrderResult](m, h)`.

Handlers must be safe for concurrent use. The framework never copies or serializes handler values.

### 4.3 Send

```go
// Send dispatches req through the pipeline to its handler and returns the typed response.
func Send[R any](ctx context.Context, m *Mediator, req Request[R]) (R, error)
```

Semantics:

1. The mediator looks up the concrete type of `req` in the frozen registry. An unknown type returns `ErrHandlerNotFound` with code `HandlerNotFound`, unless remote dispatch is enabled and the handler directory lists a live remote handler (see 7.6).
2. The precomputed behavior chain for that type runs. Each behavior receives the request as `any` plus `*RequestInfo`.
3. The handler runs at most once per `Send` unless a retry policy explicitly permits re-execution, and then only after the unit of work has been rolled back.
4. The response is unboxed to `R`. A handler that returns a value of the wrong dynamic type is a programming error caught at `Build()`, not at run time.
5. Nested `Send` from inside a handler is supported. The nested call gets its own behavior chain but joins the ambient unit of work, correlation ID, principal, and deadline. Nesting depth is capped at 32 to catch accidental recursion.

`Send` never panics on handler panics. The recovery behavior converts them to an error with code `Internal` and records the stack.

### 4.4 Notifications

```go
type Event struct{}

func (Event) mediatorEvent() {}

type Notification interface{ mediatorEvent() }

// Durable notifications are written to the outbox and delivered to consumers on any node.
type Durable interface {
    Notification
    StreamKey() string // ordering and partition key, e.g. the aggregate ID
}

type NotificationHandler[E Notification] interface {
    Handle(ctx context.Context, e E) error
}

// On registers an in-process handler. It runs synchronously inside Publish,
// in the goroutine and unit of work of the publisher.
func On[E Notification](m *Mediator, h NotificationHandler[E]) error
func OnFunc[E Notification](m *Mediator, f func(context.Context, E) error) error

// Consume registers a durable consumer in the named group. Delivery is
// at-least-once; effect is exactly-once when the effects of the handler are
// inside the unit of work (see 6.5).
func Consume[E Durable](m *Mediator, group string, h NotificationHandler[E], opts ...ConsumeOption) error

func Publish(ctx context.Context, m *Mediator, e Notification, opts ...PublishOption) error
```

`Publish` does two things in order:

1. **In-process fan-out.** All `On` handlers for the dynamic type run according to the publish strategy. The default `StopOnFirstError` matches MediatR. `ContinueOnError` runs every handler and returns `errors.Join` of the failures. `Parallel` runs handlers in goroutines and waits. Strategy is set per mediator with `WithPublishStrategy` and overridden per call with `mediator.Strategy(s)`.
2. **Durable append.** If `e` implements `Durable`, one outbox row is inserted in the ambient unit of work (6.3). If there is no ambient unit of work, `Publish` returns `ErrNoUnitOfWork`. A query handler that publishes a durable event gets `ErrDurablePublishInQuery` because its transaction is read-only. Use `pg.WithTx` to publish from outside the pipeline.

Because in-process handlers run inside the transaction of the publisher, a failing in-process handler fails the command and rolls everything back. This is deliberate: it is how same-transaction projections stay consistent. Handlers that must not affect the outcome of the command belong in a durable consumer.

Each durable event gets an envelope at publish time:

```go
type Envelope struct {
    ID            uuid.UUID // UUIDv7, unique per event
    Type          string    // stable event name
    Topic         string    // defaults to Type; override with Topic() string
    StreamKey     string
    Seq           int64     // dense per (topic, stream key), see 6.3
    OccurredAt    time.Time
    CorrelationID string
    CausationID   string    // the request that produced this event
    TraceParent   string    // W3C trace context
    SchemaVersion int       // from SchemaVersion() int if implemented, else 1
    Headers       map[string]string
}

func EnvelopeFrom(ctx context.Context) (Envelope, bool) // inside a consumer handler
```

### 4.5 Stream requests

```go
type StreamQuery[T any] struct{}

func (StreamQuery[T]) mediatorStream(T)  {}
func (StreamQuery[T]) mediatorKind() Kind { return KindStream }

type StreamRequest[T any] interface {
    mediatorStream(T)
    mediatorKind() Kind
}

type StreamHandler[Q StreamRequest[T], T any] interface {
    Handle(ctx context.Context, req Q) iter.Seq2[T, error]
}

func HandleStream[Q StreamRequest[T], T any](m *Mediator, h StreamHandler[Q, T]) error

func Stream[T any](ctx context.Context, m *Mediator, req StreamRequest[T]) iter.Seq2[T, error]
```

The request-level behaviors run once when `Stream` is called, before the first item. Stream behaviors (`StreamBehavior` interface, same shape but returning an iterator) wrap the sequence so logging, metrics, and tracing observe item counts, duration, and the terminal error. Cancelling `ctx` stops the sequence; the iterator of the handler must return promptly on `ctx.Done()`. A stream that yields an error terminates. The HTTP adapter serves streams as Server-Sent Events (8.5).

Streams run in a read-only transaction by default so the whole sequence is one snapshot. `NoUnitOfWork()` opts out for streams that tail live data.

### 4.6 Pipeline and behaviors

```go
type Next func(ctx context.Context, req any) (any, error)

type Behavior interface {
    Name() string
    Handle(ctx context.Context, req any, info *RequestInfo, next Next) (any, error)
}

type BehaviorFunc struct {
    N string
    F func(ctx context.Context, req any, info *RequestInfo, next Next) (any, error)
}

type RequestInfo struct {
    Name         string       // stable request name
    Kind         Kind
    RequestType  reflect.Type
    ResponseType reflect.Type
    Traits       Traits       // precomputed at Build from the interfaces of the request type
}

// Use registers a behavior. Order is registration order unless a position option is given.
func Use(m *Mediator, b Behavior, opts ...UseOption) error

// Position and scope options.
func Before(name string) UseOption
func After(name string) UseOption
func Commands() UseOption
func Queries() UseOption
func Streams() UseOption
func For(types ...reflect.Type) UseOption
func Where(pred func(*RequestInfo) bool) UseOption
```

At `Build()` the mediator resolves positions into one total order, rejects contradictions (`After("x")` where x is not registered, or cycles), then compiles one chain per registered request type by filtering by scope and composing closures. Dispatch is a map lookup and a call. Behavior names are unique; registering two behaviors with the same name is an error.

`behavior.Standard(cfg)` returns the default set in the default order (Section 5). Applications insert their own behaviors with `Before` and `After` relative to standard names, for example `mediator.Use(m, tenantBehavior, mediator.After(behavior.Authorization))`.

### 4.7 Typed behaviors, processors, and error handlers

These mirror the typed extension points of MediatR and are registered per request type:

```go
type TypedBehavior[Q Request[R], R any] interface {
    Handle(ctx context.Context, req Q, next func(context.Context, Q) (R, error)) (R, error)
}
func UseFor[Q Request[R], R any](m *Mediator, b TypedBehavior[Q, R], opts ...UseOption) error

// PreProcessor runs after validation and before the handler.
type PreProcessor[Q Request[R], R any] interface{ Process(ctx context.Context, req Q) error }
func Pre[Q Request[R], R any](m *Mediator, p PreProcessor[Q, R]) error

// PostProcessor runs after a successful handler, before the unit of work commits.
type PostProcessor[Q Request[R], R any] interface{ Process(ctx context.Context, req Q, res R) error }
func Post[Q Request[R], R any](m *Mediator, p PostProcessor[Q, R]) error

// ErrorHandler may translate or recover an error into a response.
type ErrorHandler[Q Request[R], R any] interface {
    Handle(ctx context.Context, req Q, err error) (res R, handled bool, out error)
}
func OnError[Q Request[R], R any](m *Mediator, h ErrorHandler[Q, R]) error
```

Typed behaviors default to the innermost position, immediately around the handler, and can be repositioned with the same options as untyped ones. Error handlers wrap only the handler and typed behaviors; failures inside outer behaviors (for example a validation failure) do not reach them.

### 4.8 Context values

All per-request state travels in `context.Context` under unexported keys with typed accessors:

| Accessor | Set by | Notes |
|---|---|---|
| `mediator.CorrelationID(ctx)` | HTTP adapter, remote server, consumer, or generated at first `Send` | Propagated to events and remote calls |
| `mediator.CausationID(ctx)` | `Send`, `Publish` | ID of the request or event that caused the current one |
| `mediator.RequestID(ctx)` | `Send` | Unique per `Send` call |
| `authz.PrincipalFrom(ctx)` | HTTP authenticator, remote server headers | Zero principal means anonymous |
| `mediator.IdempotencyKeyFrom(ctx)` | HTTP `Idempotency-Key` header or `WithIdempotencyKey` | Consumed by the idempotency behavior |
| `pg.TxFrom(ctx)` | Unit of work behavior | `pgx.Tx`; handlers use it for all database access |
| `mediator.EnvelopeFrom(ctx)` | Consumers | The envelope of the current event |
| `mediator.FencingToken(ctx)` | Consumers | Lease epoch for the current partition |
| `mediator.NoCache(ctx)` | Callers | Bypass cache reads for this call |
| `mediator.Depth(ctx)` | `Send` | Nesting depth |

Handlers must use the transaction from the context. Reaching for a pool directly inside a command handler breaks atomicity and is caught by the fault sweep (11.4), which fails any scenario whose invariants hold only when every write shares the transaction.

### 4.9 Error model

One error type carries a machine-readable code, a safe message, optional details, and the cause:

```go
type Code string

const (
    CodeBadRequest          Code = "bad_request"              // 400, body is not well-formed JSON
    CodePayloadTooLarge     Code = "payload_too_large"        // 413, body exceeds MaxBodyBytes
    CodeUnsupportedMedia    Code = "unsupported_media_type"   // 415, body Content-Type is not JSON
    CodeValidation          Code = "validation"               // 422
    CodeNotFound            Code = "not_found"                // 404
    CodeConflict            Code = "conflict"                 // 409
    CodePrecondition        Code = "precondition_failed"      // 412
    CodeUnauthorized        Code = "unauthorized"             // 401
    CodeForbidden           Code = "forbidden"                // 403
    CodeRateLimited         Code = "rate_limited"             // 429
    CodeTimeout             Code = "timeout"                  // 504
    CodeUnavailable         Code = "unavailable"              // 503, transient
    CodeIdempotencyMismatch Code = "idempotency_mismatch"     // 422
    CodeIdempotencyBusy     Code = "idempotency_in_progress"  // 409 + Retry-After
    CodeHandlerNotFound     Code = "handler_not_found"        // 501
    CodeInternal            Code = "internal"                 // 500
)

type Error struct {
    Code    Code
    Message string          // safe to show to clients
    Details map[string]any  // safe to show to clients
    Err     error           // never shown to clients
}

func (e *Error) Error() string
func (e *Error) Unwrap() error
func E(code Code, msg string) *Error
func Wrap(code Code, msg string, err error) *Error
func CodeOf(err error) Code   // CodeInternal for unknown errors

type FieldError struct {
    Path    string // JSON Pointer, e.g. /lines/0/qty
    Rule    string // e.g. min
    Message string
}

type ValidationError struct{ Fields []FieldError }
```

`errors.Is` and `errors.As` work through the chain. Handlers return domain errors as `mediator.E(CodeNotFound, "order not found")`. Unknown errors are `Internal` and their text never leaves the process.

Transient classification is one function, `mediator.IsTransient(err)`, true for `CodeUnavailable`, `CodeTimeout`, context deadline, Postgres serialization failure `40001`, deadlock `40P01`, connection errors, and Redis connection errors. Retry and the consumer loop share it.

### 4.10 Traits

Request and event types opt into features by implementing small interfaces. Presence is detected once at `Build()` with `reflect.Type.Implements`; the methods are invoked on the actual value at run time.

| Trait | Applies to | Effect |
|---|---|---|
| `Named`: `Name() string` | all | Pins the persisted name (4.11). Default is the Go type name. |
| `Timeout() time.Duration` | requests | Overrides the default deadline |
| `Requires() authz.Requirement` | requests | Authorization check and OpenAPI security |
| `RateLimit() ratelimit.Policy` | requests | Redis GCRA limiter |
| `CacheTags() []string` | queries | Enables caching, names the tags this result depends on |
| `CacheTTL() time.Duration` | queries | Overrides the default TTL |
| `Invalidates() []string` | commands | Tags bumped before and after commit |
| `RetryPolicy() retry.Policy` | commands | Enables retry around the unit of work |
| `TxOptions() pg.TxOptions` | requests | Isolation, access mode, lock timeout |
| `NoUnitOfWork()` | requests | Skip the transaction entirely |
| `IdempotencyKey() string` | commands | Key from the request body instead of the header |
| `Route() httpapi.Route` | requests | REST method and path instead of the RPC default |
| `Describe() openapi.Operation` | requests | Summary, description, tags, deprecation |
| `Validate(ctx) error` | requests, nested structs | Cross-field rules, run after tag rules |
| `Topic() string` | durable events | Override the stream topic |
| `SchemaVersion() int` | durable events | Recorded in the envelope |

### 4.11 Naming and stability

Request names, event types, topics, and consumer group names are persisted in the outbox, inbox, idempotency table, stream keys, and the OpenAPI document. Renaming a Go type therefore changes persisted identifiers. `Build()` logs every name it derived, and `mediatorctl names` prints them. Types whose names must survive refactors implement `Named`. Names match `^[A-Za-z][A-Za-z0-9_.]{0,127}$`.

### 4.12 Concurrency and performance

- After `Build()` the mediator holds only immutable maps and precomposed closures. `Send`, `Publish`, and `Stream` take no locks.
- Per-`Send` allocations in the core, with no I/O behaviors active: boxing the request, boxing the response, one context derivation per behavior that adds a value. Target at most 6 allocations and under 1.5 µs for the full default chain on a laptop, measured by `BenchmarkSend_DefaultChain` and gated by `benchstat` at a 10 percent regression threshold.
- Behaviors that need per-request state use the context, never fields on the behavior.
- The registry rejects registration after `Build()`; there is no run-time mutation path, which is what makes the lock-free hot path sound. The race detector runs across the entire suite.

---

## 5. Built-in behaviors (`mediator/behavior`)

### 5.1 Default order

Outermost first. The column "scope" is the default scope; each behavior can be narrowed with `UseOption`s.

| # | Name | Scope | Needs | Purpose |
|---|---|---|---|---|
| 1 | `Recovery` | all | nothing | Convert panics to `Internal` errors |
| 2 | `Tracing` | all | OTel tracer | One span per request, context propagation |
| 3 | `Logging` | all | slog | Structured start and end records |
| 4 | `Metrics` | all | OTel meter | Duration histogram, outcome counter |
| 5 | `Timeout` | all | nothing | Deadline from trait or default |
| 6 | `Authorization` | all | principal in ctx | Requirement check |
| 7 | `RateLimit` | requests with `RateLimit()` | Redis | GCRA limiter |
| 8 | `Validation` | all | nothing | Tag rules then `Validate()` |
| 9 | `Cache` | queries with `CacheTags()` | Redis | Tag-versioned read-through |
| 10 | `Retry` | commands with `RetryPolicy()` | nothing | Re-run on transient failure |
| 11 | `UnitOfWork` | all except `NoUnitOfWork()` | Postgres | Transaction, post-commit hooks |
| 12 | `Idempotency` | commands with a key | inside UoW | Reserve, execute once, replay |
| 13 | typed behaviors, pre-processors | per type | | |
| 14 | handler, error handlers, post-processors | | | |

Why this order:

- Recovery is outermost so a panic anywhere, including in another behavior, becomes an error with a logged stack.
- Tracing precedes logging and metrics so both can attach the trace ID.
- Timeout precedes retry so the deadline bounds the total, not each attempt.
- Authorization precedes rate limiting because limits are keyed by principal, and precedes validation so unauthorized callers learn nothing about field rules.
- Cache precedes the unit of work so a hit costs no transaction.
- Retry wraps the unit of work so each attempt runs in a fresh transaction.
- Idempotency sits inside the unit of work because its reservation row must commit or roll back with the effects of the handler.

The publish path for in-process notification handlers uses Recovery, Tracing, Logging, and Metrics only. The consumer path is described in 7.3.

### 5.2 Recovery

Catches panics from anything inside it. Produces `Wrap(CodeInternal, "panic in handler", &PanicError{Value, Stack})`. Logs at error level with the stack. Re-panics only for `runtime.Error` values that indicate memory corruption is impossible to recover from; in practice never. Counted by the metrics behavior as outcome `panic`.

### 5.3 Tracing

Starts a span named `mediator.{Kind} {Name}` with attributes `mediator.request.name`, `mediator.request.kind`, `mediator.correlation_id`, `mediator.causation_id`, `mediator.depth`. Sets span status from the error code. Records `mediator.error.code` on failure. Consumers create a span linked to the producer span using the `TraceParent` header from the envelope. Remote dispatch injects and extracts W3C headers.

### 5.4 Logging

Two records per request at info level: `request.start` and `request.end` with `name`, `kind`, `correlation_id`, `request_id`, `duration_ms`, `outcome`, `error_code`. Request bodies are not logged by default. With `LogPayloads: true`, the request is logged after redaction: fields tagged `log:"-"` or `log:"redact"` are replaced by `"[redacted]"`. Field-level redaction is verified by a test that fails if any field tagged redact reaches a log sink.

### 5.5 Metrics

| Instrument | Type | Attributes |
|---|---|---|
| `mediator.request.duration` | histogram, seconds | `name`, `kind`, `outcome` |
| `mediator.request.inflight` | up-down counter | `name`, `kind` |
| `mediator.notification.handlers` | histogram, count | `event` |

Outcome is `ok`, `error`, `panic`, or `timeout`. Cardinality is bounded by the registry size.

### 5.6 Timeout

Applies `context.WithTimeout` using `Timeout()` if implemented, else the configured default of 30 seconds. If the incoming context already has an earlier deadline, that deadline wins. A handler that returns after the deadline gets its result discarded and `CodeTimeout` returned. The unit of work observes the cancellation and rolls back.

### 5.7 Authorization

```go
package authz

type Principal struct {
    Subject     string
    Roles       []string
    Permissions []string
    Tenant      string
    Claims      map[string]any
}

type Requirement interface {
    Check(ctx context.Context, p Principal) error
    Describe() Description // roles and permissions for OpenAPI security scopes
}

func Role(names ...string) Requirement          // any of
func Permission(names ...string) Requirement    // all of
func Authenticated() Requirement
func Any(reqs ...Requirement) Requirement
func All(reqs ...Requirement) Requirement
func Custom(name string, f func(context.Context, Principal) error) Requirement
```

If the request implements `Requires()`, the behavior loads the principal from the context. No principal and a non-empty requirement returns `CodeUnauthorized`. A principal that fails the requirement returns `CodeForbidden`. Requests without `Requires()` are allowed by default; `RequireAuthByDefault: true` flips that so every request must declare its requirement, which `Build()` enforces.

Resource-level checks that need the loaded entity happen in the handler with `authz.Check(ctx, requirement)`.

### 5.8 Validation

The validator is the package `mediator/validate`, written for this framework so that one interpreter produces both run-time checks and JSON Schema constraints. The tag grammar is a closed set; unknown rules are a `Build()` error, never a silent no-op.

| Tag | Applies to | JSON Schema |
|---|---|---|
| `required` | any | listed in `required`; for strings also `minLength: 1` unless `allowempty` |
| `min=n`, `max=n` | numbers | `minimum`, `maximum` |
| `min=n`, `max=n` | strings, slices, maps | `minLength`/`maxLength`, `minItems`/`maxItems`, `minProperties`/`maxProperties` |
| `gt=n`, `gte=n`, `lt=n`, `lte=n` | numbers | `exclusiveMinimum`, `minimum`, `exclusiveMaximum`, `maximum` |
| `len=n` | strings, slices | equal min and max |
| `pattern=^...$` | strings | `pattern` (RE2 syntax, which is ECMA-compatible for the allowed subset). Must be the last rule in the tag; its value runs to the end of the tag string, so it may contain commas such as `{3,32}` |
| `oneof=a b c` | strings, ints | `enum` |
| `email`, `uuid`, `url`, `datetime`, `ipv4`, `ipv6`, `hostname` | strings | `format` |
| `unique` | slices of scalars | `uniqueItems` |
| `dive` | slices, maps | rules after `dive` apply to elements |
| `allowempty` | strings | removes the `minLength: 1` from `required` |

Semantics:

- Nested structs are validated recursively. Pointers are optional unless `required`. Nil slices and maps fail `required` and pass `min=0`.
- Field paths in errors are JSON Pointers built from `json` tag names.
- After tag rules pass, if the request (or any nested struct) implements `Validate(ctx) error`, it runs. It returns `*ValidationError` for field-level results or any error for a whole-request rejection. Cross-field rules are described in OpenAPI only through the operation description.
- Unknown JSON fields are rejected by the HTTP decoder by default, and the schema sets `additionalProperties: false`. `AllowUnknownFields: true` flips both together.

Property test (11.3) generates random request values, encodes them, and checks that the validator and a JSON Schema validator agree on acceptance for every value. This is the test that guarantees the frontend sees the same rules the server enforces.

### 5.9 RateLimit

```go
package ratelimit

type Policy struct {
    Rate   float64       // permitted operations per Period
    Period time.Duration
    Burst  int
    Key    func(ctx context.Context, req any) string // default: principal subject, else remote IP, else "global"
}
```

GCRA in one Lua script keyed `mediator:rl:<name>:<key>` (7.1). Over limit returns `CodeRateLimited` with `Details["retry_after_ms"]`. When Redis is unreachable the behavior fails open and increments `mediator.ratelimit.degraded`; `FailClosed: true` reverses that.

### 5.10 Cache

Queries that implement `CacheTags()` are cached in Redis with the tag-version protocol in 7.4. Commands that implement `Invalidates()` bump tag versions. Hits skip the unit of work. Misses use in-process `singleflight` per cache key so one node makes one database read per key at a time. `mediator.NoCache(ctx)` bypasses reads but still writes. Only successful responses are cached. Cache failures are never surfaced to the caller: a Redis error degrades to a miss, is counted, and is logged at warn level with rate limiting on the log line.

### 5.11 Retry

```go
package retry

type Policy struct {
    MaxAttempts int           // including the first
    BaseDelay   time.Duration // exponential with full jitter, capped by MaxDelay
    MaxDelay    time.Duration
    RetryIf     func(error) bool // default mediator.IsTransient
}
```

Retry re-invokes `next` after the unit of work has rolled back and the delay has elapsed, honoring the deadline. It never retries a handler that has no unit of work unless the request also has an idempotency key, because it cannot know whether the effects happened. `Build()` rejects `RetryPolicy()` on a request that has `NoUnitOfWork()` and no `IdempotencyKey()`.

Serialization failures and deadlocks are transient by definition, which is what makes `SERIALIZABLE` commands practical: the policy on the request decides how many times to try.

### 5.12 UnitOfWork

Detailed in 6.1. Summary: opens a transaction with the options from `TxOptions()` or the defaults (commands `READ COMMITTED` read-write, queries and streams `REPEATABLE READ` read-only), puts it in the context, runs `next`, commits on success, rolls back on error or panic. Registers post-commit hooks that run after a successful commit, used by the cache behavior for the second tag bump and by the relay wake-up notification. Joins an ambient transaction on nested `Send`.

### 5.13 Idempotency

Detailed in 6.6. Summary: if the context or the request carries a key, reserve `(scope, key)` inside the transaction, execute once, store the response, and on replay return the stored response without running the handler. Concurrent duplicates block on the row lock until the first attempt commits or rolls back.

---

## 6. Postgres persistence (`mediator/pg`)

Postgres is the source of truth. Every durability guarantee in Section 10 reduces to "Postgres committed it."

### 6.1 Unit of work

```go
package pg

type TxOptions struct {
    Isolation   pgx.TxIsoLevel   // default ReadCommitted for commands, RepeatableRead for queries
    ReadOnly    bool             // default false for commands, true for queries and streams
    LockTimeout time.Duration    // SET LOCAL lock_timeout; default 5s
    Propagation Propagation      // Required (join ambient) or RequiresNew
}

func TxFrom(ctx context.Context) (pgx.Tx, bool)

// WithTx runs f inside a transaction with the same semantics as the behavior,
// for use outside the pipeline (jobs, tests, migrations).
func WithTx(ctx context.Context, pool *pgxpool.Pool, opts TxOptions, f func(ctx context.Context) error) error

// OnCommit registers a hook that runs after the ambient transaction commits.
// Hooks run in registration order, in the goroutine that owns the unit of work (the caller
// of Send, or the consumer loop), before that call returns.
// A hook error is logged and counted, never returned: the transaction is already durable.
func OnCommit(ctx context.Context, hook func(ctx context.Context))
```

Behavior sequence for a command:

1. If the context already carries a transaction and propagation is `Required`, run `next` inside it and return. No commit, no hooks; the outer unit of work owns both.
2. Acquire a connection, `BEGIN` with the isolation and access mode, `SET LOCAL lock_timeout`, `SET LOCAL idle_in_transaction_session_timeout` to the request deadline.
3. Put the transaction in the context and run `next`.
4. On error or panic: `ROLLBACK`, return the error. A rollback failure is logged; the original error is returned.
5. On success: `COMMIT`. If commit fails with a transient error (connection lost during commit), the outcome is unknown. The behavior returns `Wrap(CodeUnavailable, "commit outcome unknown", err)` and marks the error with `pg.IsAmbiguous(err) == true`. Retry policies do not automatically retry ambiguous commits unless the request has an idempotency key, because the idempotency reservation is what makes a re-execution safe.
6. After a successful commit, run the `OnCommit` hooks, then return.

The connection is never shared between goroutines; a handler that spawns goroutines must not pass the transaction to them, and `pgx` will surface that as a conn-busy error which the fault sweep treats as a test failure.

### 6.2 Schema

All framework tables carry the `mediator_` prefix. DDL lives in `migrations/0001_init.sql` and is embedded.

```sql
CREATE TABLE mediator_outbox (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id      UUID        NOT NULL UNIQUE,
    topic         TEXT        NOT NULL,
    stream_key    TEXT        NOT NULL,
    seq           BIGINT      NOT NULL,
    partition     SMALLINT    NOT NULL,
    event_type    TEXT        NOT NULL,
    schema_ver    INT         NOT NULL DEFAULT 1,
    payload       JSONB       NOT NULL,
    headers       JSONB       NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at  TIMESTAMPTZ
);
CREATE INDEX mediator_outbox_unpublished_idx
    ON mediator_outbox (topic, partition, id) WHERE published_at IS NULL;
CREATE UNIQUE INDEX mediator_outbox_key_seq_idx
    ON mediator_outbox (topic, stream_key, seq);
CREATE INDEX mediator_outbox_published_idx
    ON mediator_outbox (published_at) WHERE published_at IS NOT NULL;

CREATE TABLE mediator_stream_seq (
    topic       TEXT   NOT NULL,
    stream_key  TEXT   NOT NULL,
    next_seq    BIGINT NOT NULL,
    PRIMARY KEY (topic, stream_key)
);

CREATE TABLE mediator_inbox (
    consumer_group TEXT        NOT NULL,
    event_id       UUID        NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_group, event_id)
);
CREATE INDEX mediator_inbox_processed_idx ON mediator_inbox (processed_at);

CREATE TABLE mediator_idempotency (
    scope         TEXT        NOT NULL,
    key           TEXT        NOT NULL,
    request_hash  BYTEA       NOT NULL,
    response      JSONB,                       -- NULL until the handler succeeds
    hits          INT         NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (scope, key)
);
CREATE INDEX mediator_idempotency_expiry_idx ON mediator_idempotency (expires_at);

CREATE TABLE mediator_relay_cursor (
    topic           TEXT        NOT NULL,
    partition       SMALLINT    NOT NULL,
    last_outbox_id  BIGINT      NOT NULL,
    last_stream_id  TEXT        NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);

CREATE TABLE mediator_schema_version (
    version    INT         PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fencing tokens for partition leases (7.2). A Postgres sequence rather than a Redis
-- counter so that tokens stay monotonic across Redis data loss (7.7, G14).
CREATE SEQUENCE mediator_fencing_seq AS BIGINT;
```

### 6.3 Outbox write path

Called by `Publish` for a `Durable` event inside the ambient transaction:

```sql
-- 1. Serialize writers of this stream key until commit, and take the next sequence number.
INSERT INTO mediator_stream_seq (topic, stream_key, next_seq)
VALUES ($1, $2, 1)
ON CONFLICT (topic, stream_key) DO UPDATE SET next_seq = mediator_stream_seq.next_seq + 1
RETURNING next_seq;

-- 2. Append the event.
INSERT INTO mediator_outbox
    (event_id, topic, stream_key, seq, partition, event_type, schema_ver, payload, headers)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
```

Properties this gives:

- **Per-key serialization.** The row lock on `mediator_stream_seq` is held until commit, so two transactions publishing to the same key commit in `seq` order, and `seq` is dense: a rolled-back transaction releases its number.
- **Identity order equals commit order within a key.** The outbox `id` is assigned after the lock is taken, so within one key, ascending `id` is ascending `seq` is commit order. Across keys there is no ordering claim.
- **Partition is fixed at write time.** `partition = fnv1a64(stream_key) mod P`, with `P` stored in config and checked at startup against `mediator_relay_cursor`; changing `P` requires a documented resharding procedure (`mediatorctl outbox reshard`), because it changes which stream carries which key.
- **Deadlocks are possible** when one transaction publishes keys A then B and another publishes B then A. Postgres detects them; the error is transient; a retry policy handles it. `Publish` sorts multiple events published in one transaction by key before taking locks, which removes the common case.

The relay is woken by `NOTIFY mediator_outbox, '<topic>:<partition>'` from an `OnCommit` hook, so latency is normally milliseconds, and it also polls every `PollInterval` (default 1 s) so a lost notification only adds latency.

### 6.4 Relay

One relay slot per (topic, partition). A relay process tries to own slots with `pg_try_advisory_lock(hashtext('mediator_relay:' || topic || ':' || partition))` on a dedicated connection; ownership lasts as long as that connection. Multiple relay processes are safe and share the slots.

Per owned slot, loop:

```sql
BEGIN;
SELECT id, event_id, topic, stream_key, seq, event_type, schema_ver, payload, headers, created_at
FROM mediator_outbox
WHERE topic = $1 AND partition = $2 AND published_at IS NULL
ORDER BY id
LIMIT $3
FOR UPDATE SKIP LOCKED;
```

For each row in order: `XADD mediator:{topic:p} * ...` with the envelope fields and `outbox_id`. Then:

```sql
UPDATE mediator_outbox SET published_at = now() WHERE id = ANY($1);
INSERT INTO mediator_relay_cursor (topic, partition, last_outbox_id, last_stream_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT (topic, partition) DO UPDATE
    SET last_outbox_id = EXCLUDED.last_outbox_id,
        last_stream_id = EXCLUDED.last_stream_id,
        updated_at = now();
COMMIT;
```

Failure analysis:

- Crash after `XADD`, before `COMMIT`: rows stay unpublished, next loop re-adds them. Redis receives duplicates with the same `event_id`. Consumers deduplicate through the inbox. This is the at-least-once edge.
- `XADD` fails midway through a batch: the transaction rolls back, no row is marked published, and the already-added entries are duplicates on the next pass. The relay never marks a row it did not add.
- The late-commit gap: a transaction holding a lower `id` may commit after a higher `id` was already relayed. Because `published_at IS NULL` is the selection criterion, not a watermark, the late row is picked up on the next pass. It is only out of order relative to other keys, which is permitted.
- Redis unreachable: the slot backs off exponentially from 100 ms to 10 s and reports lag through metrics. Nothing is lost.

Backpressure: `XADD` uses `MAXLEN ~ StreamMaxLen` only when `TrimByLength` is set; the default is time-based trimming by the janitor with `XTRIM MINID` at the retention horizon so the stream and the outbox retain the same window.

### 6.5 Inbox

Consumers (7.2) run each handler in a unit of work and, in that same transaction, execute:

```sql
INSERT INTO mediator_inbox (consumer_group, event_id) VALUES ($1, $2)
ON CONFLICT DO NOTHING RETURNING 1;
```

If no row is returned, the event was already processed by this group: the consumer skips the handler, commits the empty transaction, and acknowledges. If a row is returned, the handler runs, and the commit makes the inbox row and the handler's writes durable together. A crash between commit and `XACK` leads to redelivery, which the inbox row now rejects. This is what "exactly-once effect" means in Section 10: the handler's Postgres writes happen exactly once. Effects outside Postgres are at-least-once and the handler gets the fencing token to defend them.

The inbox insert happens before the handler runs so that a handler with a long transaction cannot race with a redelivery of the same event on a lease handover: the second attempt blocks on the row lock until the first commits or rolls back, then either skips or proceeds.

### 6.6 Idempotency

Scope is `{Name}` of the command, prefixed with the principal's tenant when present, so the same key used for two different commands never collides and tenants cannot replay each other's results.

Key sources in priority order: `IdempotencyKey()` on the request, then the context (from the `Idempotency-Key` HTTP header). Keys are 1 to 200 characters. Commands without a key skip the behavior entirely.

Request hash is SHA-256 over canonical JSON of the request (object keys sorted, no insignificant whitespace, numbers in shortest round-trip form). Field order in the wire body does not affect the hash.

Inside the unit of work, before the handler:

```sql
INSERT INTO mediator_idempotency (scope, key, request_hash, expires_at)
VALUES ($1, $2, $3, now() + $4)
ON CONFLICT (scope, key) DO UPDATE SET hits = mediator_idempotency.hits + 1
RETURNING hits, request_hash, response;
```

Outcomes:

| `hits` | `response` | Meaning | Action |
|---|---|---|---|
| 0 | NULL | Reserved now | Run the handler, then `UPDATE ... SET response = $1` in the same transaction, commit |
| > 0 | non-NULL, hash equal | Completed earlier | Return the stored response, do not run the handler. The transaction commits the `hits` increment |
| > 0 | non-NULL, hash differs | Same key, different payload | Roll back, return `CodeIdempotencyMismatch` |
| > 0 | NULL | Cannot happen | An uncommitted reservation is invisible to other transactions; a committed one always has a response. Asserted in the fault sweep |

Concurrency: the `DO UPDATE` arm takes the row lock. A concurrent duplicate blocks until the first transaction commits or rolls back. If it committed, the duplicate sees the completed row and replays. If it rolled back, the duplicate's insert proceeds and it becomes the executor. Blocking is bounded by `lock_timeout`; on `55P03` the behavior returns `CodeIdempotencyBusy` with `Retry-After`. There is no in-progress state visible to anyone, so there is nothing to clean up after a crash.

Only successful outcomes are stored. A handler error rolls back the reservation, so the next attempt with the same key executes again. This matches the intent of clients that retry on failure.

Retention default 24 hours, sweep by the janitor. Replays extend nothing; the row expires from its creation.

### 6.7 Migrations

`pg.Migrate(ctx, pool)` takes `pg_advisory_lock(hashtext('mediator_migrate'))`, reads `mediator_schema_version`, and applies embedded files with higher versions in a transaction each. Files are `NNNN_name.sql` with an optional `-- down` section used only by tests, which verify every migration applies, reverts, and re-applies on an empty database and on a database seeded by the previous version. Migrations are append-only once merged.

### 6.8 Retention and the janitor

`pg.Janitor` runs on one node at a time (advisory lock) every `Interval` (default 1 minute) and deletes in batches of 5000 with short transactions:

| Table | Condition | Default |
|---|---|---|
| `mediator_outbox` | `published_at < now() - OutboxRetention` | 7 days |
| `mediator_inbox` | `processed_at < now() - InboxRetention` | 7 days |
| `mediator_idempotency` | `expires_at < now()` | key-specific, default 24 h |
| Redis streams | `XTRIM MINID ~ <now - OutboxRetention>` per partition | 7 days |

The Redis row runs only when the janitor is given a `StreamTrimmer`, an interface implemented by `redisx`, so `pg` does not import `redisx`. Inbox retention must not be shorter than the stream retention or an old redelivery could be reprocessed. `Build()` rejects a configuration where `InboxRetention < OutboxRetention`.

---

## 7. Redis 8 (`mediator/redisx`)

Redis carries events between nodes, holds the cache, and coordinates partition ownership. It is not a source of truth. Every Redis structure can be rebuilt from Postgres (7.7).

### 7.1 Key layout

Hash tags (`{...}`) keep everything for one partition in one slot so the layout survives a future move to Redis Cluster.

| Key | Type | Purpose |
|---|---|---|
| `mediator:{<topic>:p<n>}` | stream | Events of partition n of a topic |
| `mediator:dlq:{<group>}` | stream | Dead letters for a consumer group |
| `mediator:lease:{<group>:<topic>:p<n>}` | string | `<nodeID>:<epoch>` with TTL |
| `mediator:members:<group>` | zset | Node heartbeats, score is unix millis |
| `mediator:cache:<query>:<hash>` | string | Cached response with tag versions |
| `mediator:tagver:<tag>` | string | Integer version of a cache tag |
| `mediator:rl:<name>:<key>` | string | GCRA state |
| `mediator:rpc:<request>` | stream | Remote dispatch requests for one command type |
| `mediator:reply:<nodeID>` | stream | Replies destined for one node |
| `mediator:handlers:<request>` | zset | Nodes serving a request type, score is heartbeat |

Configuration: `PartitionsPerTopic` (P, default 16), one relay slot per (topic, partition), `LeaseTTL` 15 s, `LeaseRenew` 5 s, `ClaimMinIdle` 30 s, `ReadBlock` 1 s, `ReadBatch` 64.

### 7.2 Consumers and partition leases

A consumer group has at most one active reader per partition at any time. That single-reader rule is what preserves per-key order. It is enforced with leases and made safe with the inbox.

Membership: each node `ZADD`s itself to `mediator:members:<group>` every `LeaseRenew` and prunes members older than `LeaseTTL`. Desired partitions per node is `ceil(P / liveMembers)`. A node holding more than that releases the surplus gracefully after finishing its current message; a node holding fewer tries to acquire unowned partitions in a random order to spread contention.

Acquire:

```
epoch = SELECT nextval('mediator_fencing_seq')   -- Postgres; fault point pg.lease.epoch
ok    = SET mediator:lease:{g:t:pn} "<node>:<epoch>" NX PX <LeaseTTL>
```

The epoch is the fencing token. It comes from a Postgres sequence rather than a Redis counter so that it survives Redis data loss (7.7). A failed `SET NX` burns a number, which is harmless. Acquisition happens only on rebalance and handover, so the extra round trip does not matter.

Renew every `LeaseRenew` with a Lua script that extends only if the value still equals `<node>:<epoch>`. Release with the same compare-and-delete pattern. A node that fails to renew before TTL stops reading that partition and cancels the context of any in-flight handler for it; the unit of work rolls back.

Processing loop for an owned partition, in order:

1. **Drain claimed.** `XAUTOCLAIM <stream> <group> <node> <ClaimMinIdle> 0-0 COUNT <ReadBatch>` repeatedly until empty. These are entries a previous owner failed to acknowledge. They are processed in stream ID order before anything else.
2. **Drain own pending.** `XREADGROUP GROUP <group> <node> STREAMS <stream> 0` until empty. These are entries this node read before a crash.
3. **Read new.** `XREADGROUP GROUP <group> <node> COUNT <ReadBatch> BLOCK <ReadBlock> STREAMS <stream> >`.

Each entry: decode the envelope, build the context with correlation ID, causation ID, trace link, fencing token, and envelope; run the consumer pipeline (7.3); on success `XACK`.

Why order is preserved across a lease handover: the new owner drains the old owner's pending entries first, in ID order, and only then reads new entries. If the old owner is merely paused rather than dead, it may wake and process an entry the new owner already handled. The inbox makes that a no-op, and the paused owner discovers its lease is gone at its next renewal and stops. The only observable effect of a handover is a possible duplicate delivery, never a reordering of effects.

### 7.3 Consumer pipeline, retries, dead letters

Consumer chain, outermost first: Recovery, Tracing (linked span), Logging, Metrics, Timeout (`ConsumeOption` `HandlerTimeout`, default 30 s), UnitOfWork, Inbox, handler.

On handler error the entry is not acknowledged. Because the partition has one reader, the reader retries the same entry in place with exponential backoff from 200 ms to 30 s, honoring `IsTransient` for fast retry and treating non-transient errors the same way but with a lower attempt cap. Attempt count comes from the delivery counter in `XPENDING`, so it survives node restarts. After `MaxAttempts` (default 10) the entry is copied to `mediator:dlq:{<group>}` with the last error, attempt count, and original stream ID, then acknowledged, and the partition moves on.

`ConsumeOption` `StrictOrder(true)` disables dead-lettering: the partition halts on a poison entry, the `mediator.consumer.halted` gauge goes to 1, and an operator uses `mediatorctl dlq` to skip or requeue. The default favors progress over strict order because most projections tolerate a gap better than a stall.

Redelivery to another node happens only through lease handover and `XAUTOCLAIM`, never by an active owner releasing an entry.

### 7.4 Cache protocol

Tags are versioned integers. An entry records the versions it was built against. An entry is valid only while every tag it depends on still has the same version. Invalidation is an `INCR`, which is O(1) and never enumerates keys.

Read (Lua `cache_get`, one round trip):

```
entry = GET cachekey                        -- JSON {"v":{"tag":ver,...},"b":<body>}
if not entry then return nil end
for tag, ver in entry.v:
    if tonumber(GET "mediator:tagver:"..tag or "0") ~= ver then
        UNLINK cachekey
        return nil
return entry.b
```

Miss path in the behavior:

1. `snapshot = MGET tagver:*` for the query's tags, before touching Postgres.
2. Run the query through the rest of the chain.
3. Lua `cache_set(cachekey, snapshot, body, ttl)`: compare each tag's current version with the snapshot; if all equal, `SET cachekey ... PX ttl`; otherwise do nothing.

Write path in the command's unit of work, for a command with `Invalidates()`:

1. Before `COMMIT`: `INCR tagver:<tag>` for each tag.
2. In an `OnCommit` hook: `INCR tagver:<tag>` again.

Why both bumps: with only the post-commit bump, a reader could snapshot, read stale data before the commit, and store it after the commit but before the bump, then serve it until TTL. The pre-commit bump makes such a reader's snapshot stale at set time. With only the pre-commit bump, a reader that snapshots after the bump and reads before the commit stores stale data with a fresh version. The post-commit bump invalidates it. Together they reduce the window in which a stale entry can be served to the interval between the commit and the post-commit hook, which is inside the `Send` of the command; once `Send` returns, no cached read for those tags can return pre-command state. That is the guarantee tested in 11.6.

Degradation: if either bump fails because Redis is unreachable, the behavior schedules the bump for retry with backoff for up to the TTL, increments `mediator.cache.invalidation_failed`, and logs. During that window the bound is the TTL. Reads that fail are misses.

Stampede control: in-process `singleflight` keyed by cache key. There is no cross-node lock; N nodes may make N reads for one hot key on a miss, which is bounded and acceptable.

Cluster note: `cache_get` and `cache_set` read the entry and its tag-version keys in one script, and those keys do not share a hash slot. That is fine on a single node or Sentinel. A move to Redis Cluster must either replace the scripts with a pipeline (`GET` the entry, then `MGET` its tag versions, two round trips) or hash-tag each entry and its tags under one tag, which is only possible when a query's tags are static.

### 7.5 Rate limiter

GCRA in Lua with `TIME` for the clock so all nodes agree. State is one string per key holding the theoretical arrival time. Returns `allowed`, `remaining`, `retry_after_ms`. Keys expire after `Period * Burst`.

### 7.6 Remote dispatch

Enabled with `mediator.WithRemote(redisx.Remote{...})`. Then:

Server side: at `Build()` each node registers every locally handled command in `mediator:handlers:<request>` and refreshes the heartbeat every 5 s. `RemoteServer` runs `XREADGROUP` on `mediator:rpc:<request>` with group `handlers` and consumer `<nodeID>` for each local command. For each entry it decodes the request by name through the registry, restores correlation ID, principal, idempotency key, and trace context from the entry fields, and calls local `Send`, so the full behavior chain runs on the handling node. The reply is `XADD mediator:reply:<callerNode> * corr <id> status ok|err body <json>`.

Client side: `Send` for a type with no local handler consults a 5 s cached view of `mediator:handlers:<request>`. If no node has a fresh heartbeat, `Send` returns `HandlerNotFound` immediately. Otherwise it `XADD`s the request to `mediator:rpc:<request>` and waits on a channel keyed by correlation ID that `ReplyReader` feeds from `mediator:reply:<nodeID>`. The wait is bounded by the request deadline.

Delivery semantics:

| Request has idempotency key | Server acknowledges | Result |
|---|---|---|
| No | before executing | At-most-once execution. A server crash mid-handler loses the request; the caller gets `CodeTimeout` and the outcome is unknown but never doubled |
| Yes | after replying | At-least-once attempt, at-most-once execution. A crashed attempt is `XAUTOCLAIM`ed by another server after `ClaimMinIdle` and re-executed; the idempotency reservation guarantees a single effect and the same response |

Callers retrying a timed-out keyed command simply `Send` again with the same key; the reply is the stored response. The chaos workload in 11.6 verifies both rows of the table.

Reply streams are trimmed to `MAXLEN ~ 10000`. A reply for a caller that has given up is dropped by `ReplyReader`.

### 7.7 Redis data loss and recovery

Redis is configured with `appendonly yes` and `appendfsync everysec`, so a crash loses at most the last second. Redis may also be flushed, restored from an old snapshot, or replaced entirely. The framework treats all of these the same way.

On start and every `PollInterval`, each relay slot compares its `mediator_relay_cursor.last_stream_id` with the last entry of the stream:

- Stream missing or its last ID is older than the cursor: Redis lost data. The slot reads the `outbox_id` of the last entry the stream still has (or zero), then re-`XADD`s every row with `published_at IS NOT NULL AND topic = t AND partition = p AND id > that` in order, then continues normally. Consumer groups that vanished are recreated with `XGROUP CREATE ... 0 MKSTREAM`, so consumers replay the retained window and the inbox drops everything they already applied.
- Stream ahead of the cursor: normal, the relay crashed after `XADD` and before commit; those entries are duplicates the inbox handles.

Leases, membership, tag versions, and rate limiter state are all safe to lose: leases re-form, tag versions restart from zero which invalidates every cache entry because stored versions no longer match, and limiters simply reset. Fencing tokens are unaffected because they come from a Postgres sequence (7.2), so G14 holds across Redis loss.

`maxmemory-policy` must be `noeviction`. Startup checks `CONFIG GET maxmemory-policy` and refuses to run consumers against an evicting Redis, because eviction would silently drop stream entries between relay and consumer, and the cursor comparison cannot detect a hole in the middle. Managed Redis offerings often disable `CONFIG`; `AssumeNoEviction: true` skips the check and puts the responsibility on the operator.

### 7.8 Configuration

```go
type Config struct {
    Addr, Username, Password string
    DB                       int
    PartitionsPerTopic       int           // default 16
    LeaseTTL, LeaseRenew     time.Duration // 15s, 5s
    ClaimMinIdle             time.Duration // 30s
    ReadBlock                time.Duration // 1s
    ReadBatch                int           // 64
    MaxAttempts              int           // 10
    CacheDefaultTTL          time.Duration // 5m
    ReplyStreamMaxLen        int64         // 10000
    AssumeNoEviction         bool          // false; skip the maxmemory-policy check (7.7)
}
```

---

## 8. HTTP adapter and OpenAPI (`mediator/httpapi`, `mediator/openapi`)

### 8.1 Routing

Every registered request becomes one HTTP operation. Two conventions:

- **RPC default.** Commands: `POST /rpc/{Name}` with a JSON body. Queries: `GET /rpc/{Name}` with fields bound from the query string, or `POST` when any field is not a scalar or a slice of scalars. Streams: `GET /rpc/{Name}` with `Accept: text/event-stream`.
- **REST override.** A request implementing `Route()` declares method and path:

```go
func (GetOrder) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/orders/{orderId}"} }
func (CreateOrder) Route() httpapi.Route { return httpapi.Route{Method: "POST", Path: "/orders"} }
```

Binding uses struct tags: `path:"orderId"` from path segments, `query:"page"` from the query string, `header:"X-Tenant"` from headers, everything else from the JSON body. A field may have at most one binding source. `Build()` validates that every path parameter in the pattern has a field and that GET operations have no body-bound fields, and reports every violation at once.

Routes are registered on a `net/http.ServeMux` under a configurable prefix. Conflicting patterns are a `Build()` error.

### 8.2 Request handling

1. Enforce `MaxBodyBytes` (default 1 MiB) and `Content-Type: application/json` for bodies. Violations are `CodePayloadTooLarge` and `CodeUnsupportedMedia`.
2. Decode with `encoding/json/v2`, rejecting unknown members unless `AllowUnknownFields`. A body that is not well-formed JSON is `CodeBadRequest`. A well-formed body that does not fit the type (wrong member type, unknown member, duplicate name) is `CodeValidation` with the path of the offending member.
3. Bind path, query, and header fields with type conversion; conversion failures are `CodeValidation` field errors.
4. Build the context: correlation ID from `X-Correlation-ID` or a new UUIDv7, trace context from `traceparent`, idempotency key from `Idempotency-Key`, principal from the configured `Authenticator`.
5. Call `Send` or `Stream`.
6. Encode the response as JSON with status 200, or 201 when a command declares `Route{Status: 201}`, or 204 for `Void`.

`Authenticator` is `func(*http.Request) (authz.Principal, error)`. An error from it is `CodeUnauthorized`. The example service ships an HMAC JWT authenticator; production applications plug their own.

### 8.3 Errors as problem details

Errors are RFC 9457 `application/problem+json`:

```json
{
  "type": "urn:mediator:error:validation",
  "title": "Validation failed",
  "status": 422,
  "detail": "2 fields are invalid",
  "instance": "/orders",
  "correlationId": "0192e1b4-...",
  "code": "validation",
  "errors": [
    {"path": "/lines/0/qty", "rule": "min", "message": "must be at least 1"},
    {"path": "/customerId", "rule": "uuid", "message": "must be a UUID"}
  ]
}
```

Status mapping follows the table in 4.9. `CodeIdempotencyBusy` and `CodeRateLimited` set `Retry-After`. `CodeInternal` sends a fixed detail string and logs the cause with the correlation ID. The problem schema is part of the OpenAPI document so the frontend can type its error handling.

### 8.4 Headers

| Header | Direction | Meaning |
|---|---|---|
| `X-Correlation-ID` | both | Echoed back; generated if absent |
| `Idempotency-Key` | request | Commands only; ignored with a warning header on queries |
| `traceparent`, `tracestate` | both | W3C trace context |
| `Cache-Control: no-cache` | request | Sets `mediator.NoCache` for that call |
| `X-Mediator-Cache` | response | `hit`, `miss`, or `bypass` for cached queries |

### 8.5 Streams over SSE

`GET` with `Accept: text/event-stream` opens a response with `Content-Type: text/event-stream`, flushes headers, then writes one `data:` frame per item as JSON, an `event: error` frame with a problem body on a terminal error, and closes. A heartbeat comment `: keepalive` is written every 15 s. The client disconnecting cancels the handler context. `Last-Event-ID` is passed to the request when the request has a field tagged `header:"Last-Event-ID"`, which is how resumable streams are built by the application.

### 8.6 OpenAPI 3.1 generation

`openapi.Generate(m, openapi.Info{...}) (*openapi.Document, error)` walks the registry:

- Each request becomes an operation with `operationId` equal to the request name in lowerCamelCase, so the frontend gets a function per use case.
- Request struct to schema via reflection, using `json` tag names, skipping the marker and `json:"-"` fields, placing bound fields under `parameters` instead of the body.
- Validation tags map to constraints exactly as the table in 5.8 says.
- Response type to a `components.schemas` entry; `Void` gives 204 with no body.
- Errors: every operation lists 400, 422, and 500; operations with a body add 413 and 415; plus 401 and 403 when `Requires()` is present, 404 when the handler documents it through `Describe().Errors`, 409 and 422 idempotency codes on commands, 429 when rate limited, all referencing the shared `Problem` schema.
- Security: `Requires()` descriptions become `security` entries with scopes from roles and permissions, under a configurable scheme (bearer by default).
- Streams: the operation response is `text/event-stream` with the item schema referenced through `x-sse-item`, and the item type also lives in `components.schemas` so it is generated for the frontend. `x-sse-item` has the same shape as OpenAPI 3.2's `itemSchema`, so moving to 3.2 is a rename once the toolchain supports it (15.1).

Type mapping: `time.Time` is `string` with `format: date-time`; `uuid.UUID` is `string` with `format: uuid`; `[]byte` is `string` with `contentEncoding: base64`; pointers are `type: [T, "null"]`; maps are `additionalProperties`; `json.RawMessage` and `any` are the empty schema; named string types with `oneof` tags become `enum`; a type implementing `openapi.Schemer` supplies its own schema; embedded structs are flattened like `encoding/json` does. Schema names are the Go type name, qualified with the package when two packages collide. Descriptions come from a `doc:"..."` struct tag on fields and from `Describe()` on requests; an AST-based comment extractor is a later addition.

The document is validated at generation time against the embedded OpenAPI 3.1 meta-schema. Served at `GET /openapi.json` and rendered by an embedded single-file Scalar page at `GET /docs`.

Drift check: `make openapi` writes `api/openapi.json`; CI runs it and fails on `git diff --exit-code`. A second CI job runs `openapi-typescript` and `tsc --noEmit` on the output inside a Node container so a change that breaks frontend generation is caught here, not downstream.

---

## 9. Observability and operations

### 9.1 Metrics

All under the OTel meter `mediator`. Names are stable; attributes are bounded.

| Name | Type | Attributes |
|---|---|---|
| `mediator.request.duration` | histogram s | `name`, `kind`, `outcome` |
| `mediator.request.inflight` | up-down counter | `name`, `kind` |
| `mediator.notification.handlers` | histogram count | `event` |
| `mediator.outbox.unpublished` | gauge | `topic`, `partition` |
| `mediator.outbox.oldest_unpublished_age` | gauge s | `topic`, `partition` |
| `mediator.relay.published` | counter | `topic`, `partition` |
| `mediator.relay.replayed` | counter | `topic`, `partition` |
| `mediator.consumer.pending` | gauge | `group`, `topic`, `partition` |
| `mediator.consumer.lag` | gauge s (age of oldest pending) | `group`, `topic`, `partition` |
| `mediator.consumer.processed` | counter | `group`, `outcome` (`ok`, `dedup`, `error`, `dlq`) |
| `mediator.consumer.halted` | gauge | `group`, `topic`, `partition` |
| `mediator.lease.owned` | gauge | `group`, `topic` |
| `mediator.lease.handovers` | counter | `group`, `topic` |
| `mediator.idempotency.outcome` | counter | `name`, `outcome` (`executed`, `replayed`, `mismatch`, `busy`) |
| `mediator.cache.requests` | counter | `name`, `result` (`hit`, `miss`, `bypass`, `error`) |
| `mediator.cache.invalidation_failed` | counter | `tag` |
| `mediator.ratelimit.decisions` | counter | `name`, `result` |
| `mediator.ratelimit.degraded` | counter | `name` |
| `mediator.remote.duration` | histogram s | `name`, `outcome` |
| `mediator.dlq.size` | gauge | `group` |

### 9.2 Health

`GET /healthz` returns 200 while the process is alive. `GET /readyz` returns 200 only when Postgres answers `SELECT 1`, Redis answers `PING`, every component's `Healthy()` is nil, and consumer lag is below `ReadyMaxLag` (default 60 s). Readiness failing on lag lets an orchestrator stop routing to a node that is behind without killing it.

### 9.3 CLI (`mediatorctl`)

| Command | Purpose |
|---|---|
| `migrate up`, `migrate status` | Apply and inspect migrations |
| `names` | Print every persisted name the registry derives |
| `outbox stats` | Unpublished count and oldest age per partition |
| `outbox replay --topic T --partition N --from-id X` | Re-add published rows to Redis |
| `outbox reshard --partitions N` | Recompute partitions after changing P, with consumers stopped |
| `inbox purge --older-than 7d` | Manual retention |
| `idem purge`, `idem show --scope S --key K` | Inspect and clean idempotency rows |
| `consumer lag` | Pending and lag per group and partition |
| `dlq list --group G`, `dlq requeue --id ID`, `dlq drop --id ID` | Dead letter handling |
| `lease list`, `lease release --group G --partition N` | Force a handover |
| `openapi export --out api/openapi.json` | Generate the document |

All commands are also Go functions in their packages so tests use them directly.

### 9.4 Configuration

Structs with defaults, populated by the application from environment or files. Nothing in the framework reads the environment. `Build()` validates the combination and reports all problems at once: retention ordering, partition count agreement with the cursor table, retry on non-transactional requests without keys, unknown validation tags, unbound path parameters, and duplicate behavior names.

### 9.5 Logging conventions

Every log record carries `correlation_id` when in a request context. Framework logs use the `mediator` logger group. Levels: debug for pipeline steps, info for request start and end and lease changes, warn for degraded modes (cache invalidation retry, limiter fail-open, relay backoff), error for panics, dead letters, and ambiguous commits.

---

## 10. Guarantees

Every row is a claim the framework makes, the conditions under which it holds, and the test that would catch its violation. Test identifiers refer to Section 11. A guarantee without a test does not go in this table.

| ID | Guarantee | Preconditions | Verified by |
|---|---|---|---|
| G1 | A local `Send` runs the handler at most once, synchronously, and returns a value of the declared response type. | No retry policy, or the retry policy only re-runs after a rollback. | Unit `TestSend_*`; property `PropPipeline_HandlerAtMostOnce`; fault sweep asserts handler invocation count under every fault. |
| G2 | The behavior chain for a request type is a fixed total order determined at `Build()`, identical on every node with the same registration code. | | Unit `TestBuild_Order_*`; golden test of the resolved order per type. |
| G3 | No panic escapes `Send`, `Publish`, `Stream`, a consumer, or the remote server. | | Unit tests that panic in every position (each behavior, handler, notification handler, iterator body, post-commit hook); chaos log-scan checker fails on any goroutine panic in node logs. |
| G4 | A command's handler writes, its outbox rows, its idempotency reservation, and its in-process notification handlers' writes commit atomically or not at all. | Handler uses `pg.TxFrom(ctx)` for all writes. | Fault sweep `SweepCommandAtomicity` with every I/O step failing, timing out, being ambiguous, or crashing; invariant I1. |
| G5 | Every outbox row is eventually delivered to its Redis partition stream at least once, in `id` order within a stream key, while Postgres is durable. Across keys in one partition a later-committed lower `id` may follow a higher one (6.4, late-commit gap). | Relay running; Redis eventually reachable. | Fault sweep `SweepRelay`; chaos workload `events` with relay kills, Redis partitions, and Redis restarts; invariants I2 and I6; liveness check L1. |
| G6 | Within one stream key, events are delivered to a consumer group in `seq` order and their Postgres effects are applied in `seq` order, with no gaps and no repeats. | Handler effects are inside the unit of work; `StrictOrder` or no dead-lettering occurred for that key. | Chaos `events` workload with lease handovers forced by node pause and kill; invariant I6 checks the projection's applied sequence per key is exactly 1..n. |
| G7 | A durable consumer's Postgres effects for one event happen exactly once per consumer group, regardless of redelivery count. | Effects inside the unit of work. | Fault sweep `SweepConsumer` with crash before and after `XACK`; chaos invariant I3 compares the inbox to the set of delivered event IDs and the projection to the derived state. |
| G8 | For one `(scope, key)`, the handler executes at most once ever within the retention window, concurrent duplicates receive the same response as the first execution, and a payload mismatch is rejected. | Response type round-trips through JSON. | Unit tests with two goroutines racing on one key against real Postgres; fault sweep `SweepIdempotency` including ambiguous commit and retry; chaos `idempotent-append` workload with client retries after timeouts; invariant I4. |
| G9 | After `Send` of a command with `Invalidates()` returns successfully, no cached read of a query depending on those tags returns state that predates the command. | Redis reachable for both bumps. If a bump failed, staleness is bounded by the TTL. | Fault sweep on the cache protocol with interleavings generated by the scheduler in 11.4; chaos `cache-staleness` workload and its checker, which distinguishes healthy from degraded windows. |
| G10 | A remote command without an idempotency key executes at most once. With a key, it executes at most once and every attempt returns the same response. | | Chaos `remote-send` workload; handler nodes are killed mid-handler; Porcupine linearizability on the register model with timeouts as indeterminate operations; count of executions per key equals one. |
| G11 | The write model behaves as a linearizable register per key through the full stack, including retries, timeouts, and remote dispatch. | Each key is one row; commands update it with a single statement. | Chaos `register` and `bank` workloads checked by Porcupine; `bank` additionally checks conservation. |
| G12 | Loss of all Redis data causes no loss of events and no duplicate effects. | Postgres retains the outbox window. | Chaos nemesis `redis-flush` and `redis-restore-old`; invariants I2, I3 after recovery; relay replay counter is non-zero. |
| G13 | The validator and the generated JSON Schema accept and reject exactly the same request bodies for the supported tag set. | | Property test `PropSchemaAgreement` with rapid-generated values and `santhosh-tekuri/jsonschema`; fuzz `FuzzTagGrammar`. |
| G14 | Stale lease holders cannot reorder effects, and every handler receives a fencing token that is strictly greater than any token a previous owner of the same partition received. | | Unit tests of the lease scripts under synctest; chaos checker compares tokens observed per partition in node logs and asserts monotonicity across handovers. |
| G15 | Liveness: within `RecoveryBound` (default 60 s) after all faults are healed, the outbox has no unpublished rows older than the bound, no consumer group has pending entries older than the bound, and no idempotency operation is blocked. | Faults healed; nodes running. | Chaos liveness checker L1 and L2 at the end of every run. |
| G16 | Shutdown drains: a node receiving SIGTERM finishes in-flight requests up to the drain timeout, acknowledges the current consumer entry only after commit, releases leases, and exits with code 0. | | Integration `TestRuntime_Shutdown_*`; chaos nemesis `graceful-restart` must produce zero errors on clients that retry once. |
| G17 | The core hot path allocates at most 6 times and completes in under 1.5 µs for the default chain with no I/O behaviors, on the reference machine. | | `BenchmarkSend_DefaultChain` with `testing.AllocsPerRun` assertion and `benchstat` gate. |

### 10.1 Explicit non-guarantees

- Cross-key ordering of events. Two events with different stream keys may be delivered in any order, even when they were committed in one transaction. Use one stream key when order matters.
- Exactly-once effects outside Postgres. A consumer that calls a third-party API may call it twice on a redelivery. The fencing token and the event ID are provided for the handler to build its own idempotency.
- Read-your-writes through the cache while Redis is unreachable. The bound degrades to the TTL.
- Linearizability of cached queries in general. Cached reads are permitted to be stale up to the bound in G9.
- Fairness of remote dispatch across handler nodes.

---

## 11. Testing strategy

Two traditions, one program. From SQLite: an exhaustive attitude toward the code, meaning coverage gates, boundary tables, fuzzing, mutation testing, and a harness that makes every I/O operation fail in every way at least once. From Jepsen: an adversarial attitude toward the system, meaning many nodes, real faults, a recorded history, and checkers that decide whether the claimed consistency held.

### 11.1 Tiers

| Tier | Where | Needs | Runs | Gate |
|---|---|---|---|---|
| 0 Static | everywhere | nothing | every push | `gofmt`, `go vet`, `staticcheck`, `govulncheck`, `golangci-lint` v2 with the errcheck, gosec, and exhaustive linters |
| 1 Unit | `*_test.go` beside code | nothing, in-memory fakes | every push | `-race -shuffle=on -count=2`; coverage gate |
| 2 Property and fuzz | beside code, `testdata/fuzz` corpora | nothing | every push (short), nightly (long) | corpus grows only; any crash is a regression test |
| 3 Fault sweep | `test/faultsweep` | Postgres and Redis containers | every push | every scenario passes for every fault point and fault kind |
| 4 Integration | `test/integration` | containers | every push | real-service behavior of each package |
| 5 Chaos | `test/chaos` | compose stack, 3+ nodes, Toxiproxy | nightly, on demand, before release | all checkers pass across all seeds |
| 6 Soak and bench | `test/chaos -soak`, `*_bench_test.go` | containers | nightly | no invariant violation over hours; `benchstat` regression gate |

Build tags: `integration`, `faultsweep`, `chaos`. Tier 1 and 2 run with plain `go test ./...`. Everything else is explicit.

### 11.2 Unit tier

- Every exported function and every behavior has table-driven tests with boundary rows: empty, one, max, max plus one, nil pointer, zero value, cancelled context, expired deadline.
- Generic inference is tested by compiling: `inference_test.go` contains every registration and send shape the spec documents and fails to compile if inference regresses.
- Pipeline tests assert order with a recording behavior placed at every position, assert scope filters, assert `Before` and `After` resolution including error cases, and assert that a behavior that skips `next` short-circuits every inner behavior.
- Time is injected through `testkit.Clock` and, for anything with timers, tests run inside `testing/synctest` bubbles so timeouts, backoffs, lease renewals, and heartbeats are tested deterministically in microseconds. HTTP adapter tests use `httptest.NewTestServer` (Go 1.27), whose in-memory network works inside a bubble, so SSE keepalives and client disconnects are tested the same way.
- Fakes: `testkit.MemStore` implements the store interfaces used by the unit of work, outbox, inbox, and idempotency behaviors in memory with the same locking semantics, so behavior logic is tested without a database. Fakes are themselves tested against the real implementations in tier 4 with a shared conformance suite (`storetest.Run(t, factory)`), which is how the fakes are kept honest.
- Coverage: `go test -covermode=atomic -coverpkg=./mediator/...`. `tools/covergate` enforces 100 percent statement coverage for `mediator`, `mediator/behavior`, `mediator/validate`, and 95 percent for the rest. Lines excluded with `// covergate:ignore <reason>` must carry a reason and are listed in the CI summary. Go measures statements, not branches; mutation testing below covers the gap.
- Mutation: `gremlins` nightly on the core packages with a mutation score gate of 80 percent, rising as the suite matures. Surviving mutants are triaged into new table rows.

### 11.3 Property and fuzz tier

Property tests use rapid with shrinking:

| Test | Property |
|---|---|
| `PropPipeline_Composition` | For any list of pure behaviors, the observed call order equals the resolved order, and inserting an identity behavior changes nothing |
| `PropPipeline_HandlerAtMostOnce` | For any behavior list without retry, the handler is called at most once per `Send`, including when behaviors error or panic |
| `PropSchemaAgreement` | For any generated request value, `validate.Check(v)` succeeds iff the JSON Schema generated for the type accepts `json.Marshal(v)` |
| `PropCanonicalJSON` | Hashing a value and hashing the same value with permuted map insertion order and re-encoded numbers gives the same digest |
| `PropPartition_Stable` | Partition assignment depends only on key and P, and is uniform enough that no partition holds more than 3x the mean over 100k random keys |
| `PropCacheProtocol` | For any interleaving of reader steps and writer steps produced by the model scheduler, a read that returns from cache never returns a value older than the last committed write whose `Send` returned before the read was invoked |
| `PropSeqDense` | For any interleaving of concurrent publishers on one key in the in-memory store, the resulting sequences are exactly 1..n |

Fuzz targets, run for 30 s per push and 30 min nightly, corpora committed:

- `FuzzTagGrammar`: arbitrary `validate` tag strings never panic; parse and print round-trip.
- `FuzzRequestDecode`: arbitrary bytes into every request type in the example service through the HTTP decoder never panic and either produce a validation error or a validated value.
- `FuzzEnvelopeDecode`: arbitrary stream entry fields into `Envelope` never panic.
- `FuzzProblemJSON`: arbitrary errors render valid problem details.
- `FuzzSchemaGen`: arbitrary struct shapes (generated via reflection from a fuzzed descriptor) produce a document that validates against the meta-schema.
- `FuzzRoutePattern`: arbitrary route patterns are either rejected at `Build()` or registered without panic.

### 11.4 Fault sweep tier (SQLite-style anomaly testing)

The idea, borrowed from SQLite's I/O error and crash test loops: run a scenario once cleanly and count its I/O operations, then run it again once per operation per fault kind, injecting exactly that fault at that operation, then run recovery and check every invariant. It is exhaustive over the protocol, not random.

**Fault points.** Every I/O call site in `pg`, `redisx`, and the behaviors passes through `testkit.Fault(ctx, "pg.outbox.insert")`. Compiled out unless the `faultinject` build tag is set; with the tag but no schedule it is one atomic load. Points are named and the list is a golden file, so adding an I/O site without naming it fails a test.

**Fault kinds** per point:

| Kind | Effect |
|---|---|
| `error` | Return a transient error without performing the operation |
| `permanent` | Return a non-transient error |
| `timeout` | Block until the context deadline, then return its error |
| `delay(d)` | Perform after d, used to force lease expiry and lock timeouts |
| `ambiguous` | Perform the operation, then return a connection error. The most important kind: it models a commit or `XADD` whose acknowledgement was lost |
| `crash` | `os.Exit(137)` after performing the operation; the harness runs the scenario in a child process and restarts it |
| `cancel` | Cancel the request context at this point |

**Scenarios**, each a small program with a known expected end state and a list of invariants:

- `SweepCommandAtomicity`: a command that writes a row, publishes two durable events on two keys, has an in-process handler writing a second row, and has an idempotency key. Expect: either everything committed or nothing, and the idempotency table agrees.
- `SweepRelay`: a relay batch with 5 rows across 2 keys. Expect: all rows eventually published in order per key; duplicates in Redis permitted; cursor consistent.
- `SweepConsumer`: a consumer processing 3 events for one key with a projection. Expect: projection applied exactly once per event in order; inbox rows present; entries acknowledged after recovery.
- `SweepIdempotency`: two clients with the same key, the second starting at every step of the first. Expect: one execution, identical responses, no leaked reservation.
- `SweepCache`: writer and reader interleavings at every step; the model scheduler in `PropCacheProtocol` is reused against real Redis.
- `SweepRemote`: caller and handler node with faults at every step of the request and reply paths, with and without a key. Expect: the table in 7.6.
- `SweepLease`: two consumers contending for one partition with delays and pauses at every step. Expect: G14.
- `SweepShutdown`: SIGTERM at every step of every loop. Expect: G16.

**Invariants** (package `testkit/invariants`, shared with the chaos tier):

| ID | Check |
|---|---|
| I1 | For every command in the scenario, its state row, outbox rows, in-process handler rows, and idempotency row are all present or all absent |
| I2 | Every outbox row with `published_at` set appears in its partition stream at least once, and after recovery no unpublished row is older than the bound |
| I3 | Inbox row count per group equals the number of distinct event IDs delivered; the projection equals the state derived from the committed commands |
| I4 | At most one execution per `(scope, key)`, measured by a counter the scenario handler increments in the transaction; every response for that key is byte-identical |
| I5 | No idempotency row with NULL response exists after recovery; no pending entry older than the bound after recovery; no goroutine leak (`goleak`) |
| I6 | For every key, the projection's applied sequence numbers are exactly 1..n and were applied in ascending order (the projection records apply order) |
| I7 | No cached read returned a value older than the last committed write that returned before the read began, outside degraded windows |

**Crash sweep.** Scenarios run in a child process built with `faultinject`. The parent selects the crash point through an environment variable, waits for exit, verifies invariants against the database and Redis directly (the child cannot lie about what it committed), then restarts the child in recovery mode and verifies again after it reports idle. A separate variant restarts Postgres between the crash and the recovery to prove nothing relied on connection state.

**Resource exhaustion.** Each scenario also runs with a pool of size 1, with `statement_timeout` at 50 ms plus injected delays, with `MaxBodyBytes` at the request size minus one, and with Redis `maxmemory` set just above the working set so that `OOM command not allowed` errors surface through the fault paths.

### 11.5 Integration tier

`testcontainers-go` starts `postgres:18` and `redis:8` once per package in `TestMain`. Each test uses a fresh schema (Postgres `CREATE SCHEMA` per test with `search_path`) and a fresh Redis logical database or key prefix, so tests run in parallel.

Covers: migrations up, down, up; the store conformance suite; relay end to end; consumer end to end with three consumer instances; cache protocol against real Lua scripts; limiter accuracy against `TIME`; remote dispatch between two mediators in one process; HTTP adapter with a real listener; OpenAPI document served, valid, and matching `api/openapi.json`; runtime shutdown ordering with a `SIGTERM` sent to a child process.

### 11.6 Chaos tier (Jepsen-style)

**Topology.** `deploy/docker-compose.yml` with profile `chaos` starts Postgres, Redis, Toxiproxy, and three `chaosnode` containers (five for the large profile). Every node reaches Postgres and Redis only through its own Toxiproxy listeners, so faults can be per node, per service, and per direction. The controller is the Go test process on the host. It speaks HTTP to the nodes, the Toxiproxy API, and the Docker CLI.

**Node binary.** `cmd/chaosnode` registers the workload request types, the full standard behavior chain, consumers for the `events` workload, remote dispatch, and an admin endpoint `/chaos/clock?offset=` that shifts the injectable clock, plus `/chaos/fault` to arm fault points remotely. Nodes are built with the `faultinject` tag.

**Processes and history.** Following Jepsen, each logical client is a process with at most one operation outstanding. An operation is recorded at invoke and at return as `ok`, `fail` (definitely did not happen), or `info` (unknown, typically a timeout). A process whose operation ended in `info` is retired and a fresh process number takes over, because its operation may complete at any later time. Histories are written as JSON Lines and as Porcupine operations. Every run directory holds `history.jsonl`, `nemesis.jsonl`, `summary.json`, node logs, `porcupine.html` for failures, and the seed.

**Workloads.**

| Name | Operations | Model and checker |
|---|---|---|
| `register` | `SetValue{key,val}` command, `GetValue{key}` query with cache disabled | Porcupine, per-key register model, partitioned by key for tractability |
| `register-cached` | same, cache enabled | Bounded-staleness checker: a read invoked after a write returned must not see an older value unless a Redis fault was active for those tags within the TTL |
| `bank` | `Transfer{from,to,amt}` in one transaction, `ReadAll` query | Porcupine bank model on 4 accounts; conservation checker on every read |
| `bank-idempotent` | same, every transfer carries a key and the client retries on `info` until a definite result | Same as `bank`; additionally the history must contain zero `info` results at the end, proving retries resolve |
| `idempotent-append` | `Append{key, val, idemKey}` with random client retries; final `ReadList{key}` | Every `ok` value appears exactly once; every `fail` value is absent; `info` values appear at most once; per key, the observed list order is a valid interleaving |
| `events` | `Bump{key}` commands each publishing `Bumped{key, n}`; a projection consumer maintains `count` and `last_seq` per key; a second consumer group writes an audit row per event | I3 and I6 on both groups after quiescence; DLQ must be empty |
| `remote-send` | `register` where handlers are registered on only one node at a time (rotated by the nemesis) so every `Send` from the other nodes is remote | Porcupine as `register`; execution counter per keyed command equals 1 |
| `cache-staleness` | writers and cached readers on shared tags with recorded return and invoke times | I7 with degraded windows derived from the nemesis log |

**Nemeses**, each with a random duration between 2 and 20 s, chosen at random with a seed:

| Nemesis | Mechanism |
|---|---|
| `partition-pg` | Toxiproxy `timeout` toxic (blackhole) on one or all nodes' Postgres proxies, upstream, downstream, or both |
| `partition-redis` | Same on Redis proxies |
| `latency` | Toxiproxy `latency` with jitter 50 to 2000 ms |
| `reset` | Toxiproxy `reset_peer` and `slow_close` |
| `bandwidth` | Toxiproxy `bandwidth` at 8 KB/s to force partial writes |
| `pause` | `docker pause` a node (models GC and VM stalls; forces lease expiry while holding state) |
| `kill` | `docker kill -s KILL` a node, restart after a delay |
| `graceful-restart` | `docker kill -s TERM`, expect clean shutdown, restart |
| `pg-restart` | `docker restart postgres`; connections drop mid-transaction |
| `redis-restart` | `docker restart redis`; up to 1 s of AOF loss |
| `redis-flush` | `redis-cli FLUSHALL`; total loss |
| `redis-restore-old` | stop Redis, replace its data directory with a snapshot taken earlier in the run, start |
| `clock-skew` | shift one node's clock by -60 s to +60 s via the admin endpoint; also `LeaseTTL` interplay |
| `handler-rotate` | for `remote-send`, move the handler registration to a different node |
| `relay-kill` | stop the relay component on all nodes for a period |

**Schedule.** Warm up 10 s with no faults. Mayhem for the configured duration (default 5 min, nightly 20 min, weekly 2 h) with a nemesis operation every 5 to 15 s and at most two active at a time. Heal: remove all toxics, unpause and restart everything. Quiesce: wait until the outbox has no unpublished rows and no group has pending entries, up to `RecoveryBound`; exceeding it fails L1. Final reads. Run every checker. Repeat for each seed in the matrix.

**Checkers** (all must pass):

- Workload checker from the table above.
- Invariants I1 to I7 on the database and Redis directly, not through the nodes.
- Liveness L1 (quiescence within bound) and L2 (every retired `info` operation for a keyed command eventually resolved when re-sent after healing).
- Fencing monotonicity from node logs (G14).
- Log scan: no `panic`, no `invariant violated`, no `conn busy`, no data race report, no goroutine count growth above baseline across the run.
- Shutdown: every `graceful-restart` produced exit code 0 within the drain timeout.

**Reproducibility.** `CHAOS_SEED` drives the generator, the nemesis sequence, and the durations. Wall-clock timing is not deterministic, so a failure is reproduced by re-running the seed several times and by replaying the recorded history through the checker offline.

**Environment.** Runs on Docker Desktop on Windows with Linux containers; `docker pause`, `docker kill -s`, and Toxiproxy all work there. CI runs the same compose file on a Linux runner.

### 11.7 Soak and benchmarks

Soak is the chaos harness with a low nemesis rate for hours, race detector off, invariant checks every 5 minutes against the database. Benchmarks: `BenchmarkSend_DefaultChain`, `BenchmarkPublish_InProcess`, `BenchmarkRelay_Batch`, `BenchmarkConsumer_Partition`, `BenchmarkCache_Hit`, `BenchmarkHTTP_Command`. Results are stored per commit and compared with `benchstat`; a regression above 10 percent on the median fails the nightly job.

### 11.8 Test-to-code expectations

The core packages are expected to carry several times more test code than production code once the sweeps and properties are in place. That ratio is reported in CI, not gated. The gates are coverage, mutation score, fault sweep completeness (every named fault point exercised by every kind in at least one scenario), and a green chaos matrix.

---

## 12. Docker and local development

### 12.1 Compose

`deploy/docker-compose.yml`:

```yaml
name: go-api-backend

services:
  postgres:
    image: postgres:18
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: app
    command:
      - postgres
      - -c
      - max_connections=300
      - -c
      - shared_preload_libraries=pg_stat_statements
      - -c
      - log_lock_waits=on
      - -c
      - deadlock_timeout=200ms
    ports: ["5432:5432"]
    volumes:
      - pgdata:/var/lib/postgresql   # 18+ images mount here; /var/lib/postgresql/data fails at start
      - ./postgres/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app -d app"]
      interval: 2s
      timeout: 2s
      retries: 30

  redis:
    image: redis:8
    command:
      - redis-server
      - --appendonly
      - "yes"
      - --appendfsync
      - everysec
      - --maxmemory-policy
      - noeviction
      - --save
      - ""
    ports: ["6379:6379"]
    volumes:
      - redisdata:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 2s
      retries: 30

  toxiproxy:
    profiles: [chaos]
    image: ghcr.io/shopify/toxiproxy:2.12.0
    command: ["-host=0.0.0.0", "-config=/config/toxiproxy.json"]
    volumes:
      - ./toxiproxy.json:/config/toxiproxy.json:ro
    ports:
      - "8474:8474"          # API
      - "15432-15436:15432-15436"   # per-node Postgres listeners
      - "16379-16383:16379-16383"   # per-node Redis listeners
    depends_on:
      postgres: { condition: service_healthy }
      redis:    { condition: service_healthy }

  node1: &node
    profiles: [chaos]
    build: { context: .., dockerfile: deploy/Dockerfile, target: chaosnode }
    environment:
      NODE_ID: node1
      PG_URL: postgres://app:app@toxiproxy:15432/app
      REDIS_ADDR: toxiproxy:16379
      HTTP_ADDR: :8080
    ports: ["18081:8080"]
    depends_on: [toxiproxy]
    stop_grace_period: 20s

  node2:
    <<: *node
    environment:
      NODE_ID: node2
      PG_URL: postgres://app:app@toxiproxy:15433/app
      REDIS_ADDR: toxiproxy:16380
      HTTP_ADDR: :8080
    ports: ["18082:8080"]

  node3:
    <<: *node
    environment:
      NODE_ID: node3
      PG_URL: postgres://app:app@toxiproxy:15434/app
      REDIS_ADDR: toxiproxy:16381
      HTTP_ADDR: :8080
    ports: ["18083:8080"]

volumes:
  pgdata:
  redisdata:
```

`deploy/toxiproxy.json` defines one proxy per node per service (`pg-node1` on `15432` to `postgres:5432`, `redis-node1` on `16379` to `redis:6379`, and so on). The chaos controller adds and removes toxics on those names.

`deploy/Dockerfile` is a multi-stage build: `golang:1.27` builds `cmd/chaosnode` with `-tags faultinject` and `cmd/mediatorctl`; the runtime stage is `gcr.io/distroless/static` with the binaries. The example service has its own target without the fault tag.

### 12.2 Make targets

| Target | Does |
|---|---|
| `make up` / `make down` | Compose without profiles: Postgres and Redis only |
| `make test` | Tiers 0 to 2 |
| `make test-integration` | Tier 4 via testcontainers (no compose needed) |
| `make test-sweep` | Tier 3 |
| `make chaos WORKLOAD=register SEED=1 DURATION=5m` | One chaos run against the compose profile |
| `make chaos-matrix` | The nightly matrix |
| `make openapi` | Regenerate `api/openapi.json` |
| `make cover` | Coverage report and gate |
| `make mutate` | Mutation testing on core packages |
| `make bench` | Benchmarks with `benchstat` against the stored baseline |

Windows note: the Makefile is POSIX and runs under Git Bash. Each target is also a `go run ./tools/task <name>` so nothing depends on `make` being installed.

---

## 13. Example service (`examples/orders`)

A small but complete service that exercises every feature and doubles as the OpenAPI fixture.

**Requests**

| Type | Kind | Traits |
|---|---|---|
| `CreateOrder` | command, returns `CreateOrderResult` | `Route POST /orders`, `Requires(Permission("orders:write"))`, `IdempotencyKey` from header, `Invalidates("orders:list")`, `RetryPolicy` 3 attempts |
| `AddLine` | command, `Void` | `Route POST /orders/{orderId}/lines`, `Invalidates("order:{id}", "orders:list")` |
| `SubmitOrder` | command, `Void` | `Route POST /orders/{orderId}/submit`, `TxOptions{Isolation: Serializable}`, publishes `OrderSubmitted` |
| `GetOrder` | query, `OrderView` | `Route GET /orders/{orderId}`, `CacheTags("order:{id}")`, TTL 5 m |
| `ListOrders` | query, `Page[OrderSummary]` | `GET /orders?customerId=&page=&size=`, `CacheTags("orders:list")` |
| `WatchOrder` | stream of `OrderEvent` | `GET /orders/{orderId}/events` over SSE |

**Events and handlers**

- `OrderSubmitted{OrderID, CustomerID, Total}` is `Durable` with `StreamKey() = OrderID`.
- `AuditLogger` is an in-process handler that writes an audit row in the same transaction.
- `InventoryReserver` is a durable consumer in group `inventory`; it reserves stock and sends a `ReserveStock` command through the mediator from inside the consumer, which demonstrates nested `Send` in a consumer unit of work.
- `OrderSummaryProjector` is a durable consumer in group `read-model`; it maintains `order_summaries` with `last_seq`.

**Wiring** in `examples/orders/main.go`: pool, Redis client, `mediator.New`, `behavior.Standard`, registrations, `Build`, `httpapi.New`, `mediator.Runtime{...}.Run(ctx)`. About sixty lines, and it is the template for real services.

`make openapi` runs against this service, so `api/openapi.json` is a living fixture that shows every generator feature.

---

## 14. Milestones and acceptance

Each milestone is done only when its tests are green in CI. Estimates are for one engineer working full time and are rough.

| # | Milestone | Deliverables | Acceptance |
|---|---|---|---|
| M0 | Scaffold | module, layout, compose, Dockerfile, Makefile, CI with tiers 0 and 1, covergate tool | `make test` green on an empty core |
| M1 | Core mediator | Section 4 complete; behaviors 1 to 8 with in-memory fakes; validation package; error model | Unit and property tiers green; G1, G2, G3, G13 tests exist and pass; benchmark baseline recorded |
| M2 | Postgres | Section 6 complete; UoW, outbox, relay, inbox, idempotency, migrations, janitor; store conformance suite; fault sweep harness with `SweepCommandAtomicity`, `SweepRelay`, `SweepConsumer`, `SweepIdempotency` | Tier 3 and 4 green; G4, G5, G7, G8 verified |
| M3 | Redis | Section 7 except remote dispatch; consumers, leases, DLQ, cache, limiter, data-loss recovery; `SweepCache`, `SweepLease`, `SweepShutdown` | G6, G9, G12, G14 verified in tiers 3 and 4 |
| M4 | HTTP and OpenAPI | Section 8; example service; TypeScript generation job | Document valid, drift check active, `tsc --noEmit` green on generated client |
| M5 | Chaos harness | Section 11.6; node binary; controller; nemeses; workloads `register`, `register-cached`, `bank`, `bank-idempotent`, `idempotent-append`, `events`, `cache-staleness`; nightly matrix | Ten consecutive green nightly runs across seeds; G11, G15, G16 verified |
| M6 | Remote dispatch | 7.6; `SweepRemote`; `remote-send` workload | G10 verified in chaos |
| M7 | Hardening | mutation gate, soak, `mediatorctl` complete, docs, performance tuning to G17 | All gates on; release candidate |

Definition of done for v1.0: every guarantee in Section 10 has a passing test, every gate in Section 11 is enforced, the chaos matrix has been green for two consecutive weeks, and the example service is deployed in the compose stack with its OpenAPI document consumed by a generated TypeScript client in CI.

---

## 15. Decision log and open questions

### 15.1 Decisions

| Decision | Alternatives considered | Reason |
|---|---|---|
| Own validation package with a closed tag grammar | go-playground/validator | One interpreter guarantees runtime rules and OpenAPI constraints agree (G13); the ecosystem library has hundreds of tags that have no schema equivalent |
| Idempotency reservation inserted before the handler, in the same transaction | Insert after the handler; separate transaction | Serializes concurrent duplicates before any effect; crash leaves no state to clean up |
| Per-key sequence table locked until commit | Aggregate row locks by the application; `max(seq)+1` | Self-contained; dense sequences; no assumption about the application's locking |
| Relay slots by advisory lock, one writer per partition | `SKIP LOCKED` with many writers | Multiple writers per partition could publish out of order |
| Leases in Redis are not correctness-critical | Postgres-based leases; Redlock | Order and effect safety come from the drain-first rule and the inbox, so a cheap lease suffices; Redlock adds no safety over a single node for this use |
| Both pre-commit and post-commit tag bumps | Single post-commit bump; delete-based invalidation | Only the double bump closes the stale-populate race; version bumps avoid key enumeration |
| Remote dispatch acks after reply only when a key is present | Always at-least-once; always at-most-once | Matches what can be guaranteed; see 7.6 |
| Consumers dead-letter by default rather than halt | Strict order always | Progress is the safer default for projections; strict mode remains available |
| `net/http.ServeMux` | chi, gin, echo | Standard library patterns cover method and path parameters; fewer dependencies to fault-inject |
| `encoding/json/v2` | v1 | v2 left experiment status in Go 1.27 (August 2026) and v1 is now a wrapper over it. Its strict defaults are what the HTTP decoder and canonical hasher want anyway. Pins the minimum Go at 1.27 |
| UUIDv7 for event and correlation IDs | UUIDv4, ULID | Time-ordered for index locality and readable in logs; google/uuid supports it |
| Postgres 18 pinned | 17, 19 | 18 is the current stable release (September 2025); 19 is at beta 4 as of 2026-09-24 with GA expected in October 2026. Nothing here needs 18-only features, but the 18+ Docker image mounts its volume at `/var/lib/postgresql`, which 12.1 reflects |
| Fencing token from a Postgres sequence | Redis `INCR` counter | A Redis counter restarts at zero after a flush or restore, which could hand a stale owner a larger token than the new owner. Postgres is the source of truth and acquisition is rare |
| OpenAPI 3.1 | 3.2 (September 2025) | 3.2 adds `itemSchema` for SSE, which would replace `x-sse-item`, but `openapi-typescript` supports 3.0 and 3.1 only and Scalar's 3.2 rendering is partial. Revisit at M4 |
| Mutation testing with `gremlins` | `gomutants` | gremlins v0.6.0 (December 2025) is maintained and simple to run cold. gomutants is faster on reruns with a superset of mutators and is the fallback if gremlins lags a Go release |

### 15.2 Open questions

These do not block M0 to M2. Defaults are stated; change them by amending this document.

1. **Tenant scoping.** Idempotency scope and cache keys are prefixed by `Principal.Tenant` when present. Should stream keys also be tenant-prefixed so one tenant's hot aggregate cannot delay another's partition? Default: no, application chooses its stream keys.
2. **Event schema evolution.** `SchemaVersion` is recorded but there is no upcaster hook. Default: consumers decode leniently and handle versions themselves; an upcaster registry is a candidate for v1.1.
3. **Read-model rebuild.** Replaying the outbox window into a fresh consumer group is supported by `XGROUP CREATE ... 0`, but the window is only 7 days. A full rebuild needs the application to re-emit events from state. Default: out of scope; document the pattern.
4. **Postgres 19.** GA expected October 2026. Evaluate at M7; switch if CI is green.
5. **Reference machine for G17.** Fix the benchmark host once CI hardware is chosen; until then the gate is relative (`benchstat` against the previous run), not absolute.
6. **OpenAPI 3.2.** Rename `x-sse-item` to `itemSchema` and bump the document version once `openapi-typescript` and Scalar support 3.2. Default: stay on 3.1.

---

## Appendix A. Fault point catalogue

Golden file `mediator/testkit/faultpoints.txt`, maintained by a test that fails when a point is added or removed without updating it. Initial catalogue:

```
pg.tx.begin            pg.tx.commit           pg.tx.rollback
pg.outbox.seq          pg.outbox.insert       pg.outbox.notify
pg.relay.lock          pg.relay.select        pg.relay.mark        pg.relay.cursor
pg.inbox.insert        pg.idem.reserve        pg.idem.store
pg.janitor.delete      pg.migrate.apply       pg.lease.epoch
redis.xadd             redis.xreadgroup       redis.xautoclaim     redis.xack
redis.xadd.dlq         redis.lease.acquire    redis.lease.renew    redis.lease.release
redis.members.beat     redis.cache.get        redis.cache.set      redis.tag.bump.pre
redis.tag.bump.post    redis.rl.check         redis.rpc.xadd       redis.rpc.reply
redis.rpc.readreply    redis.handlers.beat    redis.stream.info    redis.stream.replay
http.decode            http.encode            http.sse.write
```

## Appendix B. Validation tag to JSON Schema examples

```go
type Line struct {
    SKU   string  `json:"sku"   validate:"required,pattern=^[A-Z0-9-]{3,32}$"`
    Qty   int     `json:"qty"   validate:"required,min=1,max=1000"`
    Price float64 `json:"price" validate:"gte=0"`
    Note  *string `json:"note"  validate:"max=200"`
}
```

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["sku", "qty"],
  "properties": {
    "sku":   {"type": "string", "pattern": "^[A-Z0-9-]{3,32}$", "minLength": 1},
    "qty":   {"type": "integer", "minimum": 1, "maximum": 1000},
    "price": {"type": "number", "minimum": 0},
    "note":  {"type": ["string", "null"], "maxLength": 200}
  }
}
```

## Appendix C. History record format

One JSON object per line. Times are nanoseconds from the run start on the controller clock.

```json
{"process": 7, "client": "node2", "type": "invoke", "f": "set", "key": "k3", "value": 41, "time": 1203004000}
{"process": 7, "client": "node2", "type": "ok",     "f": "set", "key": "k3", "value": 41, "time": 1209881000}
{"process": 8, "client": "node1", "type": "invoke", "f": "get", "key": "k3", "time": 1210000000}
{"process": 8, "client": "node1", "type": "info",   "f": "get", "key": "k3", "error": "timeout", "time": 1240000000}
```

`info` retires the process. The Porcupine adapter maps `ok` and `info` to operations, with `info` operations given a return time of infinity, and drops `fail` operations for models where a failed operation has no effect.

## Appendix D. Behavior order quick reference

```
Recovery > Tracing > Logging > Metrics > Timeout > Authorization > RateLimit >
Validation > Cache(queries) > Retry(commands) > UnitOfWork > Idempotency(commands) >
typed behaviors > pre-processors > handler > error handlers > post-processors
```

Consumer path:

```
Recovery > Tracing(linked) > Logging > Metrics > Timeout > UnitOfWork > Inbox > handler
```

In-process notification path:

```
Recovery > Tracing > Logging > Metrics > handler
```
