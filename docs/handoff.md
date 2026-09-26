# Handoff: implementing spec.md

Written for the next agent continuing this work. Read `spec.md` (the
contract), `docs/design-notes.md` (decisions and deviations), then this file.

## 1. Where things stand

Every package in the table below is complete, and the whole tree passes
`go build ./...` and `go vet ./...`. The last full unit run before the final
wave was green across 20 packages (`go test -count=1 ./...`); the partial
packages listed in section 2 have NOT been run since, and `gofmt -l .` flags
one of their files. Nothing is committed yet: the branch `main` has no
commits and the user has not asked for one. Module path is
`github.com/t3stackcoder/go-api-backend`, `go 1.27` (toolchain auto-downloads).

| Package | State | Coverage (unit / with `-tags integration`) |
|---|---|---|
| `mediator` (core) | done | 99.5 (7 lines annotated unreachable) |
| `mediator/authz`, `retry`, `ratelimit` | done | 100 |
| `mediator/testkit` (fault points, clocks) | done | 100 / 97 under `faultinject` |
| `mediator/validate` | done | 100 |
| `mediator/httpapi` | done | 99.4 / 100 under `faultinject` |
| `mediator/openapi` | done | 100 |
| `mediator/pg` (+ `migrations/`) | done | 53 / 89 |
| `mediator/pg/storetest`, `testkit/memstore` | done | 99 (memstore) |
| `mediator/redisx` | done | 38 / 92.5 (95.6 with `faultinject`) |
| `mediator/behavior` (+ `cachemodel`), `mediator/otel` | done | 100 |
| `mediator/ctl`, `cmd/mediatorctl` | done | 96.6 / 99 |
| `mediator/testkit/history` | done | 98.6 |
| `mediator/testkit/workload`, `testkit/invariants` | done | 43 / 94, 14 / 95 |
| `tools/task`, `tools/covergate`, `Makefile`, `deploy/`, `.github/workflows/ci.yml` | done | 97 |

Agents completed each package with a final report; the exported APIs are
summarized in `docs/design-notes.md` section 3 and in each package's doc
comments. Deviations from spec.md are listed in design-notes section 6.

## 2. Work that was in flight when this handoff was written

Four background agents were STOPPED mid-task at the user's request. The
tree still passes `go build ./...` and `go vet ./...` with their partial
output in place. Exact state at the stop:

* `examples/orders/`: library package `orders/` (consumers, events,
  handlers, register, requests, schema, doc, a unit test), `main.go`,
  `auth.go`, `auth_test.go`, `ready.go` all exist. The agent's last note:
  `auth_test.go` referenced a missing `testRedisConfig()` helper and it was
  about to switch to a zero `redisx.Config` and run the unit tests. Not yet
  verified: `go test ./examples/...`, the integration tests (none written),
  `test/integration/` (empty), `cmd/mediatorctl/registry.go` (still the
  erroring default), `docker build --target orders`. `api/openapi.json`
  exists but was written before the registry was linked, so regenerate it
  with `go run ./tools/task openapi` once `registry.go` links
  `orders.Registry`, then run `openapi-check`. `gofmt -l` flags
  `examples/orders/orders/orders_test.go`.
* `cmd/chaosnode/`: admin.go, config.go, fault_inject.go, fault_noinject.go,
  fencinglog.go, main.go, proxy.go, server.go, toggle.go exist and compile.
  Nothing under `test/chaos/` was written. A stray build artifact
  `chaosnode.exe` sits in the repository root: delete it and add `*.exe` to
  `.gitignore`. The compose chaos profile has not been started.
* `test/faultsweep/`: only the tagless `plan/` subpackage exists
  (plan.go, plan_test.go, dryrun.go, dryrun_test.go); the harness, the
  eight scenarios, the crash sweep, and the completeness test are not
  written. Check `go test ./test/faultsweep/plan/` first.
* Lint cleanup: the agent had edited at least `mediator/runtime.go`
  (duplicate failure report dedupe) before being stopped; the rest of the
  71 findings remain. Re-run the linter (section 2.4) to see what is left.

If a directory is incomplete, redo that item from the brief below (each is
self-contained given spec.md and design-notes; the original agent briefs
were longer, and the spec sections cited are the source of truth).

### 2.0 Resume in this order

1. Delete `chaosnode.exe`, add `*.exe` to `.gitignore`, run `gofmt -w` on
   the flagged file, then `go build ./... && go vet ./... && go test -count=1 ./...`
   to learn the real state of the partial packages before changing anything.
2. Finish 2.1 (example service) first: it links the CLI registry, which
   regenerates `api/openapi.json`, which the `openapi` CI job checks.
3. Then 2.2 (chaos) and 2.3 (fault sweep) can run as two parallel agents:
   they touch disjoint directories (`cmd/chaosnode` + `test/chaos` versus
   `test/faultsweep`). Both use Docker heavily; do not add a third
   container-heavy agent alongside them or testcontainers start to time out.
4. Then 2.4 (lint) and the hardening items in section 3. The lint pass and
   the coverage-raising pass both edit `*_test.go` files in `pg` and `redisx`,
   so run them one after the other, not concurrently.
5. Agents must not change exported signatures of finished packages; if one
   is needed, record it in `docs/design-notes.md` first.

### 2.1 Example service (spec 13, M4)

Expected files: `examples/orders/orders/*.go` (library: request types,
handlers, consumers, `Register`, `NewMediator(Deps)`, `Registry()`,
`OpenAPIConfig()`, `Migrate`), `examples/orders/main.go` and `auth.go`
(HMAC JWT), `cmd/mediatorctl/registry.go` replaced to link `orders.Registry`,
`api/openapi.json` committed, `test/integration/*_test.go` with
`//go:build integration` (runtime shutdown G16, consumer ack-after-commit,
served OpenAPI equals committed, readiness reflecting Redis down).
Verify: `go run ./tools/task openapi-check`; `go test -tags integration
-count=1 ./examples/... ./test/integration/...`; `docker build --target
orders -f deploy/Dockerfile .`. Cross-platform shutdown: the binary must also
exit cleanly on stdin EOF when `SHUTDOWN_ON_STDIN_EOF=1` (Windows cannot send
SIGTERM to a child). Consumer group names cannot contain `-` (core
`NamePattern`), so the spec's `read-model` is `read_model`.

### 2.2 Chaos tier (spec 11.6, M5, M6)

Expected: `cmd/chaosnode/` (node binary built with `-tags faultinject`;
admin endpoints `/chaos/clock`, `/chaos/fault`, `/chaos/remote`,
`/chaos/relay`, `/chaos/stats`, `/chaos/goroutines`; logs one
`fencing=<n> partition=<p> group=<g> node=<id> topic=<t>` record per consumer
apply and a periodic `goroutines=<n>` marker) and `test/chaos/` (build tag
`chaos`, `TestChaos` with `-workload -seed -duration`; controller over node
HTTP, Toxiproxy API on :8474, Docker CLI; all eight workloads and fifteen
nemeses of the 11.6 tables; run directories with `history.jsonl`,
`nemesis.jsonl`, `summary.json`, node logs, `porcupine.html`; `TestReplay`
offline). Verify: `go run ./tools/task chaos-up`, then
`go run ./tools/task chaos -workload register -seed 1 -duration 60s`, then
`events`, `bank-idempotent`, `remote-send`, `register-cached`; finally
`chaos-down`. The workload under test, history recorder, Porcupine models,
and invariants already exist under `mediator/testkit/{workload,history,invariants}`.

### 2.3 Fault sweep tier (spec 11.4, M2/M3/M6 acceptance)

Expected: `test/faultsweep/` with build tags `faultsweep` and `faultinject`
(a tagless `doc.go`), a harness that runs each scenario clean, records
`testkit.Hits()`, then re-runs once per (point, hit, kind) with one armed
`testkit.Schedule`, recovers, and verifies invariants; the eight scenarios
`SweepCommandAtomicity`, `SweepRelay`, `SweepConsumer`, `SweepIdempotency`,
`SweepCache` (reusing `behavior/cachemodel.Scheduler` against real Redis),
`SweepRemote`, `SweepLease`, `SweepShutdown`; the crash sweep in a child
process (`os.Exit(137)`, recovery mode, optional Postgres restart between);
resource exhaustion variants; `TestFaultSweepCompleteness` comparing
`testkit.Observed()` with `mediator/testkit/faultpoints.txt` (with justified
exclusions such as `pg.migrate.apply` and `http.*`); tagless unit tests in
`test/faultsweep/plan`. Verify: `go run ./tools/task test-sweep`.

### 2.4 Lint cleanup

A lint agent was fixing the 71 findings of the CI configuration
(`.golangci.yml`) across `mediator/` and `tools/` without changing exported
APIs. Verify with a golangci-lint 2.14 built for Go 1.27: the user's
installed binary is a Go 1.26 build and refuses this module, so run
`go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0`
into a scratch `GOBIN` and run it with the default config, then with
`--build-tags integration` and `--build-tags faultinject`. Expected: 0 issues.
Intentional `//nolint:gosec` annotations: `math/rand/v2` in
`mediator/context.go` (UUIDv7 random bits) and `mediator/retry/retry.go` (jitter).

## 3. Remaining work after those land (hardening, spec M7)

1. **Coverage gate.** `tools/task cover` runs unit-only, so `pg` (53),
   `redisx` (38), `testkit/workload` (43), and `testkit/invariants` (14) fail
   the 95 percent default. Change the cover task to run with
   `-tags integration,faultinject` (CI's ubuntu runner has Docker), then raise
   `pg` from 89 and `workload`/`invariants` from about 94 to 95, or set
   explicit `-thresholds` with written reasons. Do not lower `mediator`,
   `behavior`, `validate` below 100.
2. **G17.** Time bound holds (0.57 µs full default chain, no I/O). The
   allocation bound (6) does not: 8 warm, 9 cold. Breakdown: core 4 warm
   (request box, scope, `WithValue`, response box), +1 cold for the generated
   correlation string, Timeout +4 (`context.WithTimeout`), Tracing +1 (noop
   `ContextWithSpan`). Candidates: lazy correlation string in core (store the
   UUID, format on read), a custom deadline context in `behavior/timeout.go`.
   The benchmark gate is relative (`benchstat` vs baseline, spec 15.2 q5);
   `TestSend_DefaultChainAllocations` in `behavior` pins the measured numbers
   so regressions fail. Record the outcome in design-notes.
3. **Generic component names.** `validate.shortName` turns
   `Page[OrderSummary]` into `Page_github.com_..._OrderSummary_` in OpenAPI
   components. Strip package paths inside `[...]` (e.g. `Page_OrderSummary_`),
   then regenerate `mediator/openapi/testdata/golden.json`
   (`go test ./mediator/openapi/ -run 'TestGolden$' -args -update`) and
   `api/openapi.json` (`go run ./tools/task openapi`), and re-run the
   TypeScript check (`OPENAPI_TS=1 go test -run TestTypeScriptClient ./mediator/openapi/`).
4. **Metrics double count.** `behavior.NewMetrics` emits
   `mediator.consumer.processed` on the consumer path and
   `otel.NewConsumersObserver` emits it too; wire only the observer in apps
   (it also sees `dlq` and `skip`) or drop it from Metrics. Decide and document.
5. **Idempotency scope.** `behavior` scopes Idempotency to commands without
   `NoUnitOfWork`, which makes the core's "RetryPolicy with NoUnitOfWork is
   allowed when IdempotencyKey exists" rule vacuous. Either tighten the core
   Build check (reject RetryPolicy on any NoUnitOfWork request) or document.
6. **Mutation testing** (`gremlins`, 80 percent gate) has not been run;
   `tools/task mutate` exists. **Soak** mode exists in the chaos harness brief
   but has not been exercised. **Benchmarks baseline**
   (`tools/task bench-baseline`) has not been recorded.
7. **Design-notes refresh.** Add: `Kind` five values (present), consumer
   group underscore rule, `openapi.Config.NoDescriptions` (spec said
   Descriptions default true), `CacheInvalidation` behavior name, G17 numbers,
   stdin-EOF shutdown, the `read_model` group name, and anything the tier
   agents report. Keep section 6 (deviations table) authoritative.
8. **README** should mention the tiers' commands and the Windows notes
   (no race detector locally, golangci-lint rebuild).
9. **Commit.** When the user asks: replace nothing else, commit with the
   attribution line the session specifies.

## 3.1 Decisions that are not in spec.md

These were made by the package agents and live in package doc comments;
they matter when writing the tiers.

* `pg` stores the envelope's correlation ID, causation ID, trace parent, and
  occurred-at inside the `mediator_outbox.headers` JSONB (`corr`, `cause`,
  `trace`, `at`, user headers under `h`) because the 6.2 DDL has no columns
  for them. Causation equals `mediator.RequestID(ctx)` of the publishing
  Send. The workload therefore also tags every durable event with
  `Headers({"cmd": CmdID})` and records the request ID in `wl_cmd_log`.
* Consumer groups on partition streams are created at ID `0` (spec 7.7
  replay); the RPC `handlers` group at `$`.
* Remote dispatch matches replies with a per-call `call` field (UUIDv7);
  `corr` carries the caller's correlation ID unchanged. `redisx.NewRemote`
  takes no mediator (it is passed to `mediator.WithRemote` at `New`).
* Idempotency scope is `<tenant>:<Name>` when the principal has a tenant,
  else `<Name>`; keys are 1 to 200 characters; lock timeout 55P03 maps to
  `CodeIdempotencyBusy` with `Details["retry_after_ms"]`.
* A commit failure carrying a SQLSTATE is definite; a connection-level
  failure at COMMIT is `MarkAmbiguous`. `OnCommit` hooks do not run after an
  ambiguous commit and receive the parent context.
* The `Inbox` behavior sets `ConsumerState.Duplicate` and returns without
  calling next; the unit of work then commits the empty transaction.
* `CacheInvalidation` is a separate inner behavior after `Idempotency`;
  `Cache` (queries) sits outside the unit of work. `behavior` constructors
  are `New*` because the bare names are the name constants.
* The core `NamePattern` forbids `-`, so groups are `read_model`, `audit`,
  `poison`, `inventory`; workload request names are dotted (`wl.SetValue`).
* `httpapi` always serves stream routes as SSE regardless of `Accept`, and
  binds body fields of RPC GET queries from the query string.
* `openapi` puts `x-sse-item` on the media type object next to `schema`,
  which is where OpenAPI 3.2's `itemSchema` goes.

## 4. Environment facts (verified 2026-09-26)

* Windows 11, Git Bash for commands; `make` not installed (use
  `go run ./tools/task <name>`); Docker Desktop 29 with Linux containers and
  Compose v2.40; Node 24 with npx.
* No C compiler: `-race` cannot run locally; CI runs it. Use
  `go test -count=2 -shuffle=on` locally.
* `encoding/json/v2` facts that bit agents: nil slices encode as `[]`, map
  order needs `json.Deterministic(true)`, unknown members need
  `json.RejectUnknownMembers(true)`, the map-merge tag is `json:",embed"`,
  `time.Duration` has no default encoding (request types must not use it
  in JSON fields).
* `httptest.NewTestServer(t, h)` works inside `synctest.Test`.
* Postgres advisory locks used by the relay, janitor, and migrations are
  database-global: integration tests that run them are sequential or use a
  database per test.

## 5. Conventions the agents followed

* Exported APIs were frozen once another package depended on them; changes
  go through design-notes first.
* Every I/O call site passes a literal fault point name from
  `mediator/testkit/faultpoints.txt`; `TestFaultPointCatalogue` fails on an
  unlisted literal.
* Integration tests sit beside the code as `*_integration_test.go` with
  `//go:build integration` and their own `TestMain` (testcontainers
  `postgres:18`, `redis:8`); `test/integration` holds cross-package scenarios.
* Unreachable lines carry `// covergate:ignore <reason>`; `tools/covergate`
  rejects an empty reason.
