# Handoff: implementing spec.md

Written for the next agent continuing this work. Read `spec.md` (the
contract), `docs/design-notes.md` (decisions, deviations, and the hardening
and tier outcomes in sections 7 and 8), then this file. Keep this file
current: update it in the same commit as the work it describes.

Last updated 2026-09-27 at the end of the seventh session (the one that
triaged the mutation survivors; it was cut short, so section 3 item 1 lists
what it left unfinished and where its records are).

## 1. Where things stand

Module `github.com/t3stackcoder/go-api-backend`, `go 1.27`. The branch is
`main`; there is no git remote, so CI (`.github/workflows/ci.yml`) has never
run. The whole tree builds, vets, and is gofmt-clean under every build tag
(`integration`, `faultinject`, `faultsweep`, `chaos`), and `go mod tidy` is
a no-op.

| Area | State |
|---|---|
| Core, behaviors, validate, pg, redisx, httpapi, openapi, ctl, otel, testkit | complete; `go test -count=1 -shuffle=on ./...` green in the 24 packages that have tests (31 in the module); lint at zero as of the fifth session (design-notes 5, 8.4) |
| Example service and integration tier (`examples/orders`, `test/integration`) | complete and verified in the fifth session: shutdown drain, ack-after-commit, OpenAPI drift, readiness, end to end, Docker target `orders`, compose profile `orders` smoke-tested |
| Chaos tier (`test/chaos`, `cmd/chaosnode`) | complete; every workload green on the rebuilt image: `events` seeds 1 to 8 (fifth session, design-notes 8.8) and the other seven at seed 1 (this session, 8.12), 60 s runs |
| M7 hardening | done: G17 met (5 allocs, 296 ns on this machine; 6 and 0.44 µs measured in the fifth), generic schema names, metrics single-sourced, retry rule, json/v2 tag grammar (design-notes 8.1 to 8.4) |
| Fault sweep tier (`test/faultsweep`) | complete; 40 fault points; quick and full matrices green after the fifth session's fixes (design-notes 8.9); not re-run this session (nothing under it changed) |
| Coverage gate (`task cover`, integration and fault-injection tags) | green with the default 95 percent threshold everywhere but `pg/storetest` (80): `pg` 100, `testkit/invariants` 100, `testkit/workload` 100, `redisx` 95.8, new `testkit/netfault` 100; 20 packages; `coverage/summary.md` is the record (design-notes 8.11) |
| Benchmark baseline | recorded: `coverage/bench-baseline.txt` from `task bench` then `task bench-baseline` on a quiet machine; `task bench` now gates against it at 10 percent (design-notes 8.12) |
| Mutation gate (`task mutate`, gremlins, 80 percent efficacy) | run for the first time: efficacy 89.89 percent (2018 killed, 227 lived, 57 hung, 794 on integration-only lines), 12.7 minutes; the task now clears the test cache first; a gremlins path bug needs a one-line local patch on Windows (section 2, design-notes 8.13) |

Verified complete against the spec before this session: all 7 property
tests, 6 fuzz targets, 16 CLI commands, 20 metrics, every Makefile target,
Defects A to D of the chaos rounds (design-notes 8.5, 8.7, 8.8).

## 2. What the sixth session did

* **Coverage thresholds raised** (item 1 of the last handoff): the held
  thresholds for `pg`, `testkit/invariants`, and `testkit/workload` are
  gone from `coverThresholds` (`tools/task/tasks.go`); only `pg/storetest`
  remains at 80. The gaps were the driver's error branches, closed three
  ways (design-notes 8.11): statements the server rejects while they run
  (triggers that raise, a deferred constraint trigger that fails COMMIT,
  columns retyped to text, views whose column raises, a locked row under a
  short `lock_timeout`); connection-level failures through the new
  `mediator/testkit/netfault` package (design-notes 7.9), a pgconn
  `DialFunc` that fails the next write carrying a chosen SQL fragment or
  sends it and withholds the reply; and arming every `pg.*` fault point
  against the real store. Two seams were added to `pg` (`migrationsFS`,
  `NewPgSlotStoreForTest`), and three defensive lines carry
  `covergate:ignore` with reasons. No framework code changed otherwise.
  New tests: `pg/failure_test.go` (unit), `pg/dbfault_integration_test.go`,
  `pg/faultpoints_integration_test.go` (integration and faultinject),
  `invariants` `TestIntegration_CheckFailures`, `workload`
  `TestIntegration_HandlerFailures` plus three rows of
  `TestIntegration_StorageErrors`, and `netfault`'s own unit test.
* **Chaos re-run** (item 2): the seven workloads other than `events` at seed
  1, 60 s, all green with every checker passing (design-notes 8.12); the
  stack was torn down afterwards.
* **Benchmark baseline** (item 3, first half): `task bench` on the quiet
  machine, then `task bench-baseline`; `BenchmarkSend_DefaultChain` at
  296 ns and 5 allocs (design-notes 8.12). The file is under the
  git-ignored `coverage/`, so it lives on this machine only.
* **Mutation gate** (item 3, second half): `task mutate` with gremlins
  v0.6.0 installed into a scratch `GOBIN`. Finding: gremlins keys its
  coverage profile with `filepath.Rel`, which uses backslashes on Windows,
  while mutant positions come from an `fs.FS` walk with forward slashes, so
  every mutant below the calling directory is reported "not covered" and
  only the root `mediator` package is actually tested (first run: 302
  killed, 25 lived, 2768 not covered, efficacy 92.35 percent, mutator
  coverage 10.57 percent, 2.5 minutes). Linux CI does not have the
  problem. A one-line local patch (`filepath.ToSlash` in
  `internal/coverage/coverage.go` `removeModuleFromPath`) makes the gate
  real on Windows. Second finding: gremlins sizes every mutant's test
  timeout from the wall time of its coverage run, so a run served from the
  test cache (2.7 s) starved the real test runs and 266 of the first 361
  mutants timed out; `taskMutate` now runs `go clean -testcache` first
  (`tools/task/tasks.go`, with its test). The real run: 12 minutes 45
  seconds, 2018 killed, 227 lived, 57 timed out (mutants that hang their
  tests), 794 not covered (integration-only lines), efficacy 89.89 percent
  against the 80 gate (design-notes 8.13, with the survivors by file). The
  gremlins patch is not in the repository; the upstream fix is worth a
  pull request.
* Docs: design-notes 7.9, 8.11, 8.12, 8.13 added; this file rewritten.

## 3. What is left, in order

Against spec 14's definition of done for v1.0: every milestone's
deliverables exist and every tier is green locally. What remains is the
acceptance tail that needs a remote or calendar time, plus the survivors of
the mutation gate.

1. **Mutation survivors: finish the triage.** The seventh session triaged
   all 227 survivors of design-notes 8.13, plus four more that longer
   per-package timeouts exposed: 156 killed by new unit-test rows in the
   mutant's own package (36 test files, committed with this handoff; no
   source file changed), 70 recorded as equivalent, 1 open. Every kill was
   verified by applying the mutation by hand and watching the named test
   fail. Per-package gremlins efficacy afterwards: validate 98.2, root
   `mediator` 94.5, `retry` 83.3 (its four survivors are equivalent),
   `ratelimit` 100, `behavior` 99.0, `httpapi` 99.0, `pg` 98.0, `ctl` 98.6,
   `redisx` 91.3, `testkit/history` 97.0, `testkit/memstore` 97.5,
   `testkit/workload` 100 percent. Left to do, in order:
   * Move the triage record into design-notes 8.14. The per-mutant tables
     (test name per kill, one-sentence reason per equivalent, timeouts
     seen) are six files in the session scratchpad
     `C:\Users\dwhit\AppData\Local\Temp\claude\c--Projects-go-api-backend\cd632d37-9699-4f87-9675-787c84548bf8\scratchpad\reports\`
     and nowhere else; copy them before anything cleans that directory.
   * Decide the open one, `send.go:293:15` (`CONDITIONALS_BOUNDARY`), a
     suspected defect: `Publish` stores the call's options in the context
     only when options were passed, so a nested `Publish` made from an
     in-process handler without options inherits the enclosing call's
     `Strategy` and `Headers`, while the option docs say an option
     configures one call. The fix is to mask inherited options when the
     nested call passes none (`else if ctx.Value(publishOptsKey{}) != nil`,
     install an empty `publishOptions`); an unconditional install would add
     an allocation to every `Publish` and fail `BenchmarkPublish_InProcess`
     (36 ns, 0 allocs) under the 10 percent gate. Not applied; it needs a
     pinning test in `publish_test.go` and a design-notes entry.
   * Run the full gate once on a quiet machine (`task mutate` with the
     patched gremlins of section 2 on `PATH`; the session's own full run was
     interrupted) and raise `--threshold-efficacy` in `tools/task/tasks.go`
     from 80 (with the assertion in `app_test.go` and the CI job name).
     Expected efficacy is about 97 percent; 95 is the suggested gate.
   * Facts for the next run: `gremlins -E` matches paths relative to the
     target directory (`-E '^[a-z]+/'` restricts `./mediator` to the root
     package); a per-package run needs `--timeout-coefficient 30` because
     the timeout is sized from a sub-second coverage run; in Git Bash put
     the gobin on `PATH` in `/c/...` form; concurrent runs make kills show
     as TIMED OUT (not counted against efficacy), so run one at a time.
2. **CI.** No remote exists. The first push exercises tiers 0 to 4 and the
   openapi job for the first time, including the race detector, which has
   never run anywhere (no C compiler on this machine), and the nightly
   mutation and benchmark jobs. The `mutate` job runs on Linux, where the
   gremlins path bug of section 2 does not apply.
3. **Chaos at spec scale.** The nightly matrix is 5 minutes per cell over
   seeds 1 to 3 (`task chaos-matrix`, `CHAOS_DURATION`); every cell has
   passed at 60 s; the definition of done wants two weeks of green
   nightlies, which is calendar time. Soak mode (`-soak`) exists and has
   not been exercised.
4. **Commit** only when the user asks, with the attribution line the session
   specifies. The seventh session is committed; the tree was clean at the
   end of it.

## 4. How to run each tier here

```sh
go run ./tools/task test                     # tiers 0-2 (no -race locally)
go run ./tools/task test-integration         # tier 4, testcontainers
SWEEP_QUICK=1 go test -tags faultsweep,faultinject -count=1 -timeout 60m -v ./test/faultsweep/...
go run ./tools/task test-sweep               # tier 3, full matrix
go run ./tools/task chaos-up                 # builds the node image (--build)
go run ./tools/task chaos -workload events -seed 1 -duration 60s
go run ./tools/task chaos-down
go run ./tools/task cover                    # needs Docker; about 4 minutes
go run ./tools/task bench                    # gates against coverage/bench-baseline.txt
go run ./tools/task mutate                   # gremlins on PATH; see section 2 for Windows
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
`eventually` windows are seconds.

## 5. Environment facts (verified 2026-09-27)

* Windows 11, Git Bash for commands; `make` not installed (use
  `go run ./tools/task <name>`); Docker Desktop 29.1 with Linux containers
  and Compose v2.40 (it may not be running when a session starts: start
  `Docker Desktop.exe` and wait for the engine, about a minute); Node 24
  with npx; benchstat installed; staticcheck and golangci-lint not
  installed (the user's golangci-lint is a Go 1.26 build and refuses the
  module; install v2.14.0 from source with Go 1.27 into a scratch `GOBIN`);
  gremlins v0.6.0 installs and runs, with the path caveat of section 2.
* No C compiler: `-race` cannot run locally; CI runs it. Use
  `go test -count=2 -shuffle=on` locally.
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
