# Handoff: implementing spec.md

Written for the next agent continuing this work. Read `spec.md` (the
contract), `docs/design-notes.md` (decisions, deviations, and the hardening
and tier outcomes in sections 7 and 8), then this file. Keep this file
current: update it in the same commit as the work it describes.

Last updated 2026-09-27 at the end of the ninth session (the one that
fixed what the first CI run found, verified the fixes with the race detector
in a container, and moved the fault sweep job to manual dispatch).

## 1. Where things stand

Module `github.com/t3stackcoder/go-api-backend`, `go 1.27`. The branch is
`main`. GitHub (github.com/t3stackcoder/go-api-backend, public) holds it at
`9989273`, the eighth session's last push; the local clone is one commit
ahead with the ninth session's fixes and has no remote configured (the user
had it removed in the eighth session), so a push is
`git push https://github.com/t3stackcoder/go-api-backend main`. CI
(`.github/workflows/ci.yml`) has run once, on the eighth session's push; its
findings are fixed in the local commit (design-notes 8.15) and the three
runs of that day were cancelled. The workflow has no cron schedule and no
push or pull-request trigger (design-notes 6): nothing runs in CI unless
someone dispatches it by hand from the Actions tab, which runs the five
short jobs (static, unit, short fuzz, integration, openapi, about ten
minutes) and, with `nightly=true` ticked, the long tiers as well. The whole
tree builds, vets,
and is gofmt-clean under every build tag (`integration`, `faultinject`,
`faultsweep`, `chaos`), `go mod tidy` is a no-op, golangci-lint v2.14.0
with `.golangci.yml` and staticcheck report nothing, and
`go test -race -shuffle=on -count=2 ./...` is green in a `golang:1.27`
container (section 4).

| Area | State |
|---|---|
| Core, behaviors, validate, pg, redisx, httpapi, openapi, ctl, otel, testkit | complete; `go test -count=1 -shuffle=on ./...` green in the 24 packages that have tests (31 in the module) and green under `-race -shuffle=on -count=2` in the container (ninth session); lint at zero with golangci-lint v2.14.0 and staticcheck (design-notes 8.15) |
| Fuzz corpora (6 targets, `testdata/fuzz`) | two `FuzzEnvelopeDecode` crashers, one from CI and one from the local 30 s run that followed, both decoder defects on the `at` field: `DecodeEntry` now returns `OccurredAt` in UTC as `EncodeEntry` writes it and rejects an instant outside RFC 3339's years 0000 to 9999; both inputs are committed as corpus entries (design-notes 8.15); all six targets then passed 30 s each here |
| Example service and integration tier (`examples/orders`, `test/integration`) | complete and verified in the fifth session: shutdown drain, ack-after-commit, OpenAPI drift, readiness, end to end, Docker target `orders`, compose profile `orders` smoke-tested; tier 4 and the openapi job also passed in CI |
| Chaos tier (`test/chaos`, `cmd/chaosnode`) | complete; every workload green on the rebuilt image: `events` seeds 1 to 8 (fifth session, design-notes 8.8) and the other seven at seed 1 (sixth session, 8.12), 60 s runs |
| M7 hardening | done: G17 met (5 allocs, 296 ns on this machine; 6 and 0.44 µs measured in the fifth), generic schema names, metrics single-sourced, retry rule, json/v2 tag grammar (design-notes 8.1 to 8.4) |
| Fault sweep tier (`test/faultsweep`) | complete; 40 fault points; quick and full matrices green after the fifth session's fixes (design-notes 8.9); not re-run since (nothing under it changed); the CI job is manual dispatch only (design-notes 6) |
| Coverage gate (`task cover`, integration and fault-injection tags) | green with the default 95 percent threshold everywhere but `pg/storetest` (80): `pg` 100, `testkit/invariants` 100, `testkit/workload` 100, `redisx` 95.8, `testkit/netfault` 100; 20 packages; `coverage/summary.md` is the record (design-notes 8.11); not re-run in the ninth session (its only production change, in `DecodeEntry`, has unit rows for the new branch) |
| Benchmark baseline | recorded: `coverage/bench-baseline.txt` from `task bench` then `task bench-baseline` on a quiet machine; `task bench` gates against it at 10 percent (design-notes 8.12); `BenchmarkPublish_InProcess` re-measured at 35.6 to 35.9 ns and 0 allocs after the eighth session's fix |
| Mutation gate (`task mutate`, gremlins, 95 percent efficacy) | green at the 95 gate: efficacy 96.87 percent (2167 killed, 70 lived and all equivalent, 67 timed out, 793 on integration-only lines), 12.6 minutes with the patched gremlins after the `Publish` fix (design-notes 8.14) |

Verified complete against the spec before the sixth session: all 7 property
tests, 6 fuzz targets, 16 CLI commands, 20 metrics, every Makefile target,
Defects A to D of the chaos rounds (design-notes 8.5, 8.7, 8.8).

## 2. What the eighth and ninth sessions did

* **Mutation triage closed** (eighth session). Design-notes 8.14 holds the
  per-mutant record of the seventh session's triage (156 killed by new unit
  rows in 36 test files, 70 equivalent with a reason each). The one open
  survivor, `send.go:293:15`, was a defect: a nested `Publish` made from an
  in-process handler without options inherited the enclosing call's
  `Strategy` and `Headers`. The fix masks inherited options with a
  package-level zero set when the nested call passes none, so the common
  path still allocates nothing; pinned by
  `TestPublish_NestedCallDoesNotInheritOptions` (`mediator/publish_test.go`)
  and `TestPublish_InProcessAllocations` (`mediator/bench_test.go`),
  `BenchmarkPublish_InProcess` unchanged at 0 allocs. The full gate then ran
  once, alone, with the patched gremlins: 2167 killed, 70 lived (the 70
  equivalents), 67 timed out, 793 not covered, efficacy 96.87 percent, and
  `--threshold-efficacy` rose from 80 to 95 in `tools/task/tasks.go`, its
  assertion in `tools/task/app_test.go`, and the `nightly-mutate` job name.
* **Pushed, and the first CI run** (eighth session). Committed and pushed at
  the user's request; the nightly cron removed at the user's request in a
  second commit; the run's findings recorded in a third. Then, at the
  user's request, the local `main` was reset to `31504d4`, the `origin`
  remote removed, and the three CI runs cancelled.
* **The first CI run's findings fixed** (ninth session, design-notes 8.15).
  The local tree was fast-forwarded back to `9989273` by a fetch from the
  URL (still no remote), then the seven findings were fixed without touching
  the framework's behaviour except one decoder line:
  * tier 0: `modelling` in a comment of `mediator/pg/tx.go`; two staticcheck
    QF1008 hints in `mediator/testkit/netfault/netfault.go`, where
    `c.Conn.Close()` and `c.Conn.SetReadDeadline(...)` lose the `Conn`.
  * tier 1, four data races, each a test sharing state with a goroutine of
    the code under test: `behavior` (`invalidationBubble`'s failing flag is
    an `atomic.Bool`), `httpapi` (`TestHealth` counts the concurrent
    readiness checks with an `atomic.Int32`), `pg` (`TestSlot_RunLoop`
    installs its `Hooks.Append` before the slot starts and switches the
    failure with an atomic; `memstore.StreamHooks` now documents that hooks
    are read without locking), `redisx` (`fakeLeaseStore.count(op)` reads
    the call counters under the fake's lock).
  * tier 2: `DecodeEntry` (`mediator/redisx/streams.go`) returns
    `OccurredAt` in UTC, matching `EncodeEntry`, and rejects an `at` whose
    UTC year RFC 3339 cannot write (a second crasher the local fuzz run
    found once the first was fixed); both inputs are committed under
    `mediator/redisx/testdata/fuzz/FuzzEnvelopeDecode/`, pinned by
    `TestEntry_OccurredAtUTC` and two `TestEntry_Garbage` rows.
  Verified with the tools CI uses: `go test -race -shuffle=on -count=2
  ./...` in a `golang:1.27` container (24 packages ok, 61 seconds),
  golangci-lint v2.14.0 built from source (0 issues), staticcheck, gofmt,
  `go vet` under every tag, `go mod tidy -diff`, the full suite here, and
  `task fuzz -fuzztime 30s` over the six targets.
* **CI made manual only** (ninth session). First the `sweep` job in
  `.github/workflows/ci.yml` got the long tiers' condition
  (`workflow_dispatch` with `nightly=true`); then, at the user's direction,
  the push and pull-request triggers were removed altogether, so the
  workflow runs only when dispatched by hand. Recorded as a deviation from
  spec 11.3 in design-notes 6 with the reason: the framework is finished
  and is not re-tested unless it changes. The dispatch input keeps the
  name `nightly`.
* Docs: design-notes 6 row and 8.15 added; this file rewritten.

## 3. What is left, in order

Against spec 14's definition of done for v1.0: every milestone's
deliverables exist, every tier is green locally, the mutation triage is
closed, and the first CI run's findings are fixed. What remains needs the
user's decision or calendar time.

1. **Push, when the user wants it.** The local commit carries everything;
   `git push https://github.com/t3stackcoder/go-api-backend main` (or
   `git remote add origin https://github.com/t3stackcoder/go-api-backend`
   first) fast-forwards GitHub. A push starts nothing in CI; to check a
   change there, dispatch the workflow by hand from the Actions tab (the
   five short jobs, about ten minutes; tick `nightly=true` for the long
   tiers). Each job that failed in the first run was re-run here with the
   same tool and command. The one step not repeated locally is the
   coverage gate inside the unit job (`task cover`, four minutes with
   Docker); the only production change is in `DecodeEntry`'s handling of
   the `at` field, and its new branch has unit rows.
2. **Chaos at spec scale.** The matrix is 5 minutes per cell over seeds 1
   to 3 (`task chaos-matrix`, `CHAOS_DURATION`; 20 minutes per cell in the
   CI job); every cell has passed at 60 s. spec 14 wants two weeks of
   green nightly runs; the schedule is off by the user's decision, so this
   is on demand until there is something substantial to soak. Soak mode
   (`-soak`) exists and has not been exercised.
3. **Commit** only when the user asks, with the attribution line the session
   specifies. The ninth session is committed; the tree was clean at the end
   of it.

## 4. How to run each tier here

```sh
go run ./tools/task test                     # tiers 0-2 (no -race natively)
MSYS_NO_PATHCONV=1 docker run --rm -v "C:/Projects/go-api-backend:/src" -v "C:/Users/dwhit/go/pkg/mod:/go/pkg/mod" -w /src -e GOFLAGS=-buildvcs=false golang:1.27 go test -race -shuffle=on -count=2 ./...   # tier 1 as CI runs it; one minute alone, three beside a fuzz run
<scratch GOBIN>/golangci-lint run --timeout=10m   # tier 0 lint as CI runs it; v2.14.0 built from source (section 5)
go run ./tools/task fuzz -fuzztime 30s       # tier 2 as CI runs it; six targets, about four minutes
go run ./tools/task test-integration         # tier 4, testcontainers
SWEEP_QUICK=1 go test -tags faultsweep,faultinject -count=1 -timeout 60m -v ./test/faultsweep/...
go run ./tools/task test-sweep               # tier 3, full matrix
go run ./tools/task chaos-up                 # builds the node image (--build)
go run ./tools/task chaos -workload events -seed 1 -duration 60s
go run ./tools/task chaos-down
go run ./tools/task cover                    # needs Docker; about 4 minutes
go run ./tools/task bench                    # gates against coverage/bench-baseline.txt
go run ./tools/task mutate                   # gremlins on PATH; on Windows the patched build of design-notes 8.13
go run ./tools/task orders-up                # example service on :8080, then orders-down
```

`go test -v` in package-list mode buffers a package's output until it
finishes, so a sweep log stays empty for minutes while the containers are
visibly cycling in `docker ps`; pass a single package path or `-json` to
stream. A package whose test hangs loses all its output to the timeout
panic; run the suspect test alone with `-run` and `-v`. The sweep uses
testcontainers unless `PG_URL` and `REDIS_ADDR` are both set (CI sets them
after `task up`). A chaos run writes `summary.json` (`pass`, per-checker
`checkers.<name>.ok` and `violations`, `nemesis_total`, `nemeses` by kind,
`ops`), `nemesis.jsonl`, and the node logs into its run directory; the
node logs carry one `consumer apply` line per delivery with `fencing=`,
`partition=`, `group=`, `key=`, `seq=`, and `event_id=`, and the event ID
is a UUIDv7 whose first 48 bits are the creation time in milliseconds,
which is how the 8.7 timelines were reconstructed. Do not run the sweep and
chaos at the same time, and do not run `orders-up`/`orders-down` while the
chaos stack is up: both profiles belong to the same compose project, and
`down -v` removes the shared Postgres and Redis. The mutation run is CPU
bound for minutes; do not run it beside the integration tiers, whose
`eventually` windows are seconds. On Windows the stock gremlins tests only
the root package (design-notes 8.13); build it from source with the one-line
`filepath.ToSlash` patch into a scratch `GOBIN`, put that directory first on
`PATH` in `/c/...` form, and run `task mutate` once, detached, on an idle
machine: about 13 minutes at 24 workers, and a tool timeout that kills it
part-way wastes the run.

## 5. Environment facts (verified 2026-09-27)

* Windows 11, Git Bash for commands; `make` not installed (use
  `go run ./tools/task <name>`); Docker Desktop 29.1 with Linux containers
  and Compose v2.40 (it may not be running when a session starts: start
  `Docker Desktop.exe` and wait for the engine, about a minute); Node 24
  with npx; benchstat installed; the user's golangci-lint on `PATH` is a Go
  1.26 build and refuses the module, so build v2.14.0 from source with Go
  1.27 into a scratch `GOBIN` (`GOBIN=<dir> go install
  github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0`, about
  two minutes; likewise `honnef.co/go/tools/cmd/staticcheck@latest`), as the
  ninth session did; gremlins v0.6.0 installs and runs, with the path
  caveat of design-notes 8.13.
* No C compiler: `-race` cannot run natively. It runs in a `golang:1.27`
  Linux container with the repository and the module cache bind-mounted
  (section 4): the whole suite at `-count=2` takes a minute on an idle
  machine (three beside a fuzz run), and
  `MSYS_NO_PATHCONV=1` keeps Git Bash from rewriting the container paths.
  The race detector does not treat synctest's durable blocking or a timer
  firing in virtual time as synchronization: a flag a test flips between
  `time.Sleep`s and a goroutine reads after its timer fires is a data race,
  and the fix is an atomic (design-notes 8.15).
* `time.Parse` keeps a numeric offset as `Local` (when the offsets agree)
  or as a `FixedZone`, never as `UTC` itself, while a `Z` suffix parses to
  UTC; a `time.Time` that goes through RFC 3339 and back compares equal
  under `reflect.DeepEqual` only after `.UTC()` on both sides (design-notes
  8.15).
* go-redis is v9.22.0 with `ContextTimeoutEnabled`: a command runs
  synchronously on its connection and only the socket deadline comes from
  the context, so cancelling the context of a blocking `XREADGROUP` does not
  interrupt it; the read returns when Redis replies or `BLOCK` expires.
  Redis does unblock a reader whose stream key is deleted (flush).
* pgx is v5.11: `Query` prepares a statement before it executes it, so a
  missing table or column fails `Query` itself and only a runtime error
  (a raising function, a lock timeout) surfaces in `rows.Err()`; with the
  default statement cache a repeated statement carries only its name on
  the wire (`netfault` uses `QueryExecModeDescribeExec` for that reason);
  a pooled connection that is closed is destroyed at release, so the next
  acquire opens a fresh one; `pgxpool.NewWithConfig` does not connect
  unless `MinConns` is set, so a closed pool is a cheap "every acquire
  fails" fake.
* Postgres facts that bit tests: a deferred constraint trigger fails the
  COMMIT with the trigger's SQLSTATE (P0001 for RAISE); `SELECT ... FOR
  UPDATE SKIP LOCKED` sees nothing while another transaction of the same
  test holds the rows; `DROP SCHEMA ... CASCADE` waits for every
  transaction that holds a lock in it, so a test that leaks a transaction
  hangs its own cleanup; `ALTER COLUMN ... TYPE text` is enough to make a
  scan fail; `pg_catalog` is searched before `search_path`, so a function
  there cannot be shadowed by the test schema.
* `encoding/json/v2` facts that bit agents: nil slices encode as `[]`, map
  order needs `json.Deterministic(true)`, unknown members need
  `json.RejectUnknownMembers(true)`, the map-merge tag is `json:",embed"`,
  `time.Duration` has no default encoding (request types must not use it
  in JSON fields), invalid UTF-8 in a string is a marshal error, and
  `mediator.Build` checks that every response type round-trips its zero
  value, so a test type whose codec fails must fail only for non-zero values.
* `httptest.NewTestServer(t, h)` works inside `synctest.Test`.
* Postgres advisory locks used by the relay, janitor, and migrations are
  database-global: integration tests that run them are sequential or use a
  database per test.
* Postgres `INSERT ... ON CONFLICT DO UPDATE ... WHERE` locks the
  conflicting row even when the `WHERE` rejects the update; the partition
  fence relies on that to serialize two owners.

## 6. Conventions

* Exported APIs of finished packages are frozen; a needed change is recorded
  in `docs/design-notes.md` section 6 first (Defect D's `pg.Tx.FencePartition`
  and the `mediator_partition_epoch` table are recorded there). Test seams
  go in `export_test.go`, never in the API.
* Every I/O call site passes a literal fault point name from
  `mediator/testkit/faultpoints.txt`; `TestFaultPointCatalogue` fails on an
  unlisted literal, `TestParseCatalogue_GoldenFile` in `test/faultsweep/plan`
  pins the count, and `TestFaultSweepCompleteness` fails when a point is
  not swept by every kind (exclusions with reasons live in
  `test/faultsweep/zz_completeness_test.go`; a point with no `FaultAfter`
  call is excused from the ambiguous kind automatically). A new point must
  also be added to the `patterns` of the sweep scenario that exercises it.
* Fault schedules (`testkit.Arm`) and the migration source seam
  (`pg.SetMigrationsFS`) are process-wide: a test that uses them does not
  call `t.Parallel`, so it runs while the parallel tests are paused.
* Connection-level failures use `testkit/netfault` (design-notes 7.9):
  build the pool from `Dialer.Config`, warm it with a ping, arm one rule
  right before the call. Server-side failures prefer a trigger, a retyped
  column, or a view over the dialer, because they do not depend on what pgx
  puts on the wire.
* Integration tests sit beside the code as `*_integration_test.go` with
  `//go:build integration` and their own `TestMain` (testcontainers
  `postgres:18`, `redis:8`); `test/integration` holds cross-package scenarios.
* Unreachable lines carry `// covergate:ignore <reason>`; `tools/covergate`
  rejects an empty reason.
* Every place that enumerates the framework tables (the chaos controller's
  `TRUNCATE`, the sweep harness's `frameworkTables`, the migration
  up-down-up test's seed and version-0 list) must learn about a new table,
  and a new migration must be named in `TestMigrationsEmbedded`
  (`mediator/pg/misc_test.go`) and in the `migrate` subtests of
  `mediator/ctl/integration_test.go`.
* Decisions that are not in spec.md are indexed in design-notes section 7
  (pg, redisx, behavior, httpapi and openapi, names, test/integration,
  test/chaos, deploy, testkit/netfault); do not duplicate them here.
