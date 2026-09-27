# Handoff: implementing spec.md

Written for the next agent continuing this work. Read `spec.md` (the
contract), `docs/design-notes.md` (decisions, deviations, and the hardening
and tier outcomes in sections 7 and 8), then this file. Keep this file
current: update it in the same commit as the work it describes.

Last updated 2026-09-27 at the end of the eighth session (the one that
closed the mutation triage: the record in design-notes 8.14, the `Publish`
fix, the full gate run, and the 95 percent gate).

## 1. Where things stand

Module `github.com/t3stackcoder/go-api-backend`, `go 1.27`. The branch is
`main`, pushed to `origin` (github.com/t3stackcoder/go-api-backend, public)
in the eighth session; CI (`.github/workflows/ci.yml`) ran for the first
time on that push. The workflow has no cron schedule: the user removed it,
because there is nothing substantial to soak nightly until there is a
front end, so the long tiers (fuzz 30 m, mutation, bench, chaos matrix)
run only by manual dispatch with `nightly=true`. The whole tree builds,
vets, and is gofmt-clean under every build tag
(`integration`, `faultinject`, `faultsweep`, `chaos`), and `go mod tidy` is
a no-op.

| Area | State |
|---|---|
| Core, behaviors, validate, pg, redisx, httpapi, openapi, ctl, otel, testkit | complete; `go test -count=1 -shuffle=on ./...` green in the 24 packages that have tests (31 in the module); lint at zero as of the fifth session (design-notes 5, 8.4) |
| Example service and integration tier (`examples/orders`, `test/integration`) | complete and verified in the fifth session: shutdown drain, ack-after-commit, OpenAPI drift, readiness, end to end, Docker target `orders`, compose profile `orders` smoke-tested |
| Chaos tier (`test/chaos`, `cmd/chaosnode`) | complete; every workload green on the rebuilt image: `events` seeds 1 to 8 (fifth session, design-notes 8.8) and the other seven at seed 1 (sixth session, 8.12), 60 s runs |
| M7 hardening | done: G17 met (5 allocs, 296 ns on this machine; 6 and 0.44 µs measured in the fifth), generic schema names, metrics single-sourced, retry rule, json/v2 tag grammar (design-notes 8.1 to 8.4) |
| Fault sweep tier (`test/faultsweep`) | complete; 40 fault points; quick and full matrices green after the fifth session's fixes (design-notes 8.9); not re-run since (nothing under it changed) |
| Coverage gate (`task cover`, integration and fault-injection tags) | green with the default 95 percent threshold everywhere but `pg/storetest` (80): `pg` 100, `testkit/invariants` 100, `testkit/workload` 100, `redisx` 95.8, `testkit/netfault` 100; 20 packages; `coverage/summary.md` is the record (design-notes 8.11) |
| Benchmark baseline | recorded: `coverage/bench-baseline.txt` from `task bench` then `task bench-baseline` on a quiet machine; `task bench` gates against it at 10 percent (design-notes 8.12); `BenchmarkPublish_InProcess` re-measured at 35.6 to 35.9 ns and 0 allocs after the eighth session's fix |
| Mutation gate (`task mutate`, gremlins, 95 percent efficacy) | green at the new 95 gate: efficacy 96.87 percent (2167 killed, 70 lived and all equivalent, 67 timed out, 793 on integration-only lines), 12.6 minutes with the patched gremlins after the `Publish` fix (design-notes 8.14) |

Verified complete against the spec before the sixth session: all 7 property
tests, 6 fuzz targets, 16 CLI commands, 20 metrics, every Makefile target,
Defects A to D of the chaos rounds (design-notes 8.5, 8.7, 8.8).

## 2. What the seventh and eighth sessions did

* **Mutation survivors triaged** (seventh session, item 1 of the sixth
  handoff): all 227 survivors of design-notes 8.13, plus three more
  outside its list that the per-package runs exposed, went through
  gremlins one package at a time: 156 killed by new unit-test rows in the
  mutant's own package (36 test files, no source file changed), 70
  recorded as equivalent with a one-sentence reason each, one left open.
  Every kill was verified by applying the mutation by hand and watching
  the named test fail. The per-mutant record (test name per kill, reason
  per equivalent, timeouts seen, per-package efficacy) is design-notes
  8.14; the scratch reports it came from were in a session temp directory
  and are no longer needed.
* **The open survivor fixed** (eighth session): `send.go:293:15` was a
  defect. `Publish` installed the call's options in the context only when
  options were passed, so a nested `Publish` made from an in-process
  handler without options inherited the enclosing call's `Strategy` and
  `Headers`, against the documented "one call" semantics. The fix masks
  inherited options with a package-level zero set when the nested call
  passes none, so the common path still allocates nothing (design-notes
  8.14). Pinned by `TestPublish_NestedCallDoesNotInheritOptions`
  (`mediator/publish_test.go`) and `TestPublish_InProcessAllocations`
  (`mediator/bench_test.go`); `BenchmarkPublish_InProcess` unchanged at
  0 allocs. The only source change of the triage.
* **Full gate run** (eighth session): `task mutate` once, alone, with the
  patched gremlins first on `PATH` and the `Publish` fix in the tree: 12
  minutes 36 seconds, 2167 killed, 70 lived, 67 timed out, 793 not
  covered, efficacy 96.87 percent (89.89 in 8.13), exit 0 against the 95
  gate. The 70 that lived are the 70 equivalents of 8.14 (the run prints
  positions relative to `./mediator`, and the fix moved the two `send.go`
  equivalents down by 11 lines to `342:14` and `380:19`); the
  seventy-first timed out. Four `behavior` kills of 8.14 showed as
  timeouts under the full run's load; a timeout counts against neither
  side, so the figure is conservative.
* **Gate raised to 95** (eighth session): `--threshold-efficacy` in
  `taskMutate` (`tools/task/tasks.go`), the assertion in
  `tools/task/app_test.go`, and the `nightly-mutate` job name in
  `.github/workflows/ci.yml`. spec 11 still says "80 percent, rising as the
  suite matures", which this is.
* Docs: design-notes 8.14 added; this file rewritten. Committed and
  pushed at the user's request, then the cron schedule removed and the
  first CI run's findings recorded (section 3 item 1) in two more commits.

## 3. What is left, in order

Against spec 14's definition of done for v1.0: every milestone's
deliverables exist, every tier is green locally, and the mutation triage is
closed. What remains is the acceptance tail that needs a remote or calendar
time.

1. **Fix what the first CI run found.** The first push run (the eighth
   session, run 36318721586 on the Actions tab) exercised tiers 0 to 4 and
   the openapi job for the first time. openapi and tier 4 integration
   passed; the fault sweep was still running when this was written; three
   jobs failed, none of it caused by that session's changes:
   * **Tier 0 static (golangci-lint), three findings.** A comment
     misspelling, `modelling`, at `mediator/pg/tx.go:80` (misspell); two
     staticcheck QF1008 hints, "could remove embedded field `Conn` from
     selector", at `mediator/testkit/netfault/netfault.go:139` and `:146`.
     staticcheck, govulncheck, gofmt and vet passed.
   * **Tier 1 unit under `-race`, four data races**, the race detector's
     first run anywhere (no C compiler on this machine). Every frame pair
     is a test fake shared between goroutines without a mutex; three of
     the four tests were extended by the seventh session's triage. In
     `behavior`, `TestCacheInvalidation_RetryRecovers`: a write at
     `cache_invalidation_test.go:158` (the test's metrics recorder, under
     `metrics.go:91`) against a read at `:115` from the invalidation
     goroutine (`cache_invalidation.go:177`, through
     `cachemodel/memory.go:72`). In `httpapi`,
     `TestHealth/readyz_with_passing_checks`: two health checks started at
     `httpapi.go:236` both write the test's variable at
     `health_test.go:32`. In `pg`, `TestSlot_RunLoop`: the test writes at
     `relay_test.go:482` while the slot loop (`relay.go:344`, `:431`) reads
     the memstore stream at `testkit/memstore/streams.go:85`; the only one
     with a production file on the read side, so check whether
     `memstore` takes its lock on that path before blaming the test. In
     `redisx`, `TestLeaseManager_RunLoop`: the lease manager (`lease.go:298`,
     `:320`) writes into the test's fake at `lease_test.go:119` while the
     test goroutine started at `lease_test.go:580` reads it. The full
     reports are in the job log; the fix is a mutex (or an atomic) in each
     fake. `go test -race` cannot run here; push and let CI check.
   * **Tier 2 short fuzz, one failure.** `FuzzEnvelopeDecode` in
     `mediator/redisx` (`fuzz_test.go:36`, "round trip changed the entry")
     on a 325-byte input that the fuzzer minimised and wrote to
     `testdata/fuzz/FuzzEnvelopeDecode/5515dd0f123bbdbc` on the runner; it
     is in the `fuzz-crashers` artifact of that run, not in the
     repository. The printed `got` and `want` are identical, so the
     difference is one `%v` hides and `reflect.DeepEqual` sees: most
     likely a nil `Headers` map on one side and an empty one on the other
     (design-notes 5 lists json/v2's `{}` behaviour), possibly a
     `time.Time` location. Download the artifact, put the file under
     `mediator/redisx/testdata/fuzz/FuzzEnvelopeDecode/`, run
     `go test -run=FuzzEnvelopeDecode/5515dd0f123bbdbc ./mediator/redisx`,
     and either fix the decoder or make the comparison canonical; the
     corpus entry is then committed as a regression test (spec 11.3).
   The other five short fuzz targets passed. The long tiers have not run
   in CI. When the `mutate` job is dispatched it runs on Linux, where the
   gremlins path bug of design-notes 8.13 does not apply, and gates at 95;
   a slower runner may turn kills into timeouts and lower the killed count
   that efficacy is computed from, so if it fails narrowly, compare its
   LIVED list with 8.14 before touching the gate.
2. **Chaos at spec scale.** The matrix is 5 minutes per cell over seeds 1
   to 3 (`task chaos-matrix`, `CHAOS_DURATION`; 20 minutes per cell in the
   CI job); every cell has passed at 60 s. spec 14 wants two weeks of
   green nightly runs; the schedule is off by the user's decision, so this
   is on demand until there is something substantial to soak. Soak mode
   (`-soak`) exists and has not been exercised.
3. **Commit** only when the user asks, with the attribution line the session
   specifies.

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
