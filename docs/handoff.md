# Handoff: implementing spec.md

Written for the next agent continuing this work. Read `spec.md` (the
contract), `docs/design-notes.md` (decisions, deviations, and the hardening
and tier outcomes in sections 7 and 8), then this file. Keep this file
current: update it in the same commit as the work it describes.

Last updated 2026-09-26 at the end of the fifth session (the one that fixed
the two chaos defects of design-notes 8.7 and ran the tiers listed in
section 2).

## 1. Where things stand

Module `github.com/t3stackcoder/go-api-backend`, `go 1.27`. The branch is
`main`; there is no git remote, so CI (`.github/workflows/ci.yml`) has never
run. The whole tree builds, vets, and is gofmt-clean under every build tag
(`integration`, `faultinject`, `faultsweep`, `chaos`), and `go mod tidy` is
a no-op.

| Area | State |
|---|---|
| Core, behaviors, validate, pg, redisx, httpapi, openapi, ctl, otel, testkit | complete; `go test -count=1 ./...` green in 25 packages; lint at zero (design-notes 5, 8.4) |
| Example service and integration tier (`examples/orders`, `test/integration`) | complete and verified: shutdown drain, ack-after-commit, OpenAPI drift, readiness, end to end, Docker target `orders` |
| Chaos tier (`test/chaos`, `cmd/chaosnode`) | complete; `events` seeds 1 to 8 green on 60 s runs after the fixes below (design-notes 8.8); the other 7 workloads were green before this session and were not re-run |
| M7 hardening | done: G17 met (6 allocs, 0.44 µs), generic schema names, metrics single-sourced, retry rule, json/v2 tag grammar (design-notes 8.1 to 8.4) |
| Fault sweep tier (`test/faultsweep`) | complete; 40 fault points; quick matrix: completeness gate passing with `pg.inbox.fence` swept by every kind, one timing-dependent cell (`SweepShutdown/redis.xreadgroup#1/shutdown`) traced to a pre-existing lease leak at shutdown, fixed, cell green 4 of 4 after; full matrix run for the first time (50 min, 2992 s): every scenario green except 5 cells that exposed two more gaps, both fixed and re-run green (`checkAckedImpliesInbox` now exempts dead letters; `pgTx.Rollback` tears the transaction down under an injected fault so a pool of one is not starved) (design-notes 8.9) |
| Bug A (G6, voluntary release) and Bug B (G12, relay data loss) | fixed and verified in the fourth session |
| Defect C, same-node re-acquire overlaps the old reader (G6, involuntary loss) | fixed and verified: unit (`TestLeaseManager_LostLeaseWaitsForWorkerExit`, `TestLeaseManager_EndingTracksWorkerExit`, `TestCompareStreamIDs`), integration (`TestConsumers_ClaimsOwnStaleReaderEntriesBelowBatch`), chaos seed 1 (design-notes 8.7, 8.8) |
| Defect D, stale owner writes until its next renewal (G14) | fixed and verified: migration `0002_partition_epoch.sql`, `pg.Tx.FencePartition`, `pg.ErrStaleLease`; unit (`TestInbox_FencesStaleLease`, storetest `PartitionEpochFence` on memstore), integration (storetest on Postgres, `TestIntegration_Migrations_UpDownUp`, `TestConsumers_StaleLeaseFenceEndsLeaseAndReacquires`), chaos seed 2 (design-notes 6, 8.7, 8.8) |
| Coverage gate (`task cover`, integration and fault-injection tags) | run for the first time in this form; all 19 packages meet their thresholds: `mediator`, `behavior`, `validate` at 100, `redisx` 95.7, `testkit` 97.5 (new test), `pg` 89.8, `pg/storetest` 80.1, `testkit/invariants` 94.8, `testkit/workload` 93.9 held by the documented `coverThresholds` default of the cover task (design-notes 8.10); `coverage/summary.md` is the record |
| Example service in the compose stack (profile `orders`) | smoke-tested: `orders-up` builds and starts healthy on a fresh database (migrations 0001 and 0002 apply at start), `GET /readyz` and `/healthz` 200, `GET /openapi.json` 200 (OpenAPI 3.1.0, six paths), all eight partition leases acquired, `orders-down` clean |

Verified complete against the spec before this session: all 7 property
tests, 6 fuzz targets, 16 CLI commands, 20 metrics, every Makefile target.
The fault point catalogue is 40 points since this session (`pg.inbox.fence`
was added).

## 2. What the fifth session did

* **Defect C** (`mediator/redisx`): the lease manager parks an ended lease
  whose worker has not exited (`leaseManager.ending`, `workerDone`) and
  `rebalance` will not re-acquire that partition until the worker is gone;
  the claim-before pass became `claimPendingBefore` / `pendingBefore` and
  claims every pending entry below the batch, the node's own name included,
  deciding from the XPENDING summary's lowest ID whether the range scan is
  needed; the `readCtx` comment states the go-redis behaviour correctly.
  The park decision and the worker's exited mark are both taken under the
  manager lock (a worker returning between the lease cancel and the park
  would otherwise leave the partition parked forever).
* **Defect D** (`migrations`, `mediator/pg`, `mediator/testkit/memstore`,
  `mediator/pg/storetest`, `mediator/redisx`): table
  `mediator_partition_epoch`, `pg.Tx.FencePartition` (upsert with
  `WHERE epoch <= EXCLUDED.epoch RETURNING 1`, fault point `pg.inbox.fence`,
  before-fault only), `pg.ErrStaleLease` (not transient), the fence in
  `pg.Inbox()` before `InboxInsert` whenever a fencing token is in the
  context, the memstore implementation with per-transaction buffering and
  the row lock, the storetest conformance subtest, and the `redisx` `handle`
  branch that *releases* the lease with reason `lost` (compare-and-delete,
  so a key Redis still holds with the node's old value after a snapshot
  restore is freed at once; a plain end made the re-acquire wait for the
  TTL, 2 s in the integration test) and stops the worker without counting
  an error. The chaos controller and the sweep harness truncate the new
  table between runs; `TestParseCatalogue_GoldenFile` expects 40 points.
* **Verification, in the handoff's order:** `go test -count=1 -shuffle=on
  ./...` green (25 packages); `go test -tags integration,faultinject
  -count=1 ./mediator/pg/ ./mediator/redisx/ ./mediator/testkit/...` green;
  chaos `events` seeds 1 to 4 pass on the rebuilt image, with the fence and
  the claim-below pass visibly firing in seed 2 (design-notes 8.8);
  seeds 5 to 8 pass as well, seed 7 being the first live run to draw
  `kill` and `pg-restart` (seed 8 also drew `pg-restart`); the quick sweep's completeness gate passes with the new point
  swept by every kind, and its one failing cell exposed a third defect,
  below; the full matrix (never run before) was green except three
  `SweepConsumer` crash-before cells and the two `pool1` cells at
  `pg.tx.rollback`, which exposed the two further gaps below, both fixed;
  `task cover` passes with all 19 packages at or above their thresholds
  after the documented defaults and the testkit test (design-notes 8.10);
  `task test-integration` (whole tree, integration tag) is green after the
  two test fixes below; the compose smoke test of the `orders` profile passes (`/readyz`, `/healthz`, `/openapi.json` all 200 on a fresh database, then `orders-down`).
* **Lease leak at shutdown** (`mediator/redisx/lease.go`, found by the
  sweep's `SweepShutdown/redis.xreadgroup#1/shutdown` cell, pre-existing):
  an acquisition whose `SET NX` landed after `stopAcquiring` gave the key
  back under the tick's context, which `Run` cancels right after
  `releaseAll`, so a slow first-tick acquisition (cold Postgres pool for the
  fencing token) left a key unowned until its TTL. The post-stop release now
  runs under a detached, bounded context; the fake lease store honours
  context cancellation and has an `afterAcquire` hook;
  `TestLeaseManager_AcquireAfterStopReleasesUnderDetachedContext` pins it
  (design-notes 8.9).
* **Sweep harness gap** (`test/faultsweep/harness_test.go`):
  `checkAckedImpliesInbox` flagged dead letters, which are acknowledged
  after the DLQ copy with no inbox row by design, so any consumer crash cell
  late enough for the `poison` group to have dead-lettered failed the state
  check (three cells in the full matrix; the quick matrix's first hits
  crash before that). The check now exempts entries whose original stream
  ID is in the group's DLQ; all 18 crash-before cells at the two read
  points pass (design-notes 8.9).
* **Rollback fault point starved a pool of one** (`mediator/pg/tx.go`):
  a fault injected before ROLLBACK returned without tearing the transaction
  down, so the connection stayed checked out; with `pool1` the node hung
  for the cell budget. A real failed ROLLBACK breaks the connection and
  pgxpool releases it, so the fault point now rolls back underneath and
  still returns the injected error. `TestFault_RollbackFaultReleasesConnection`
  (the first `integration && faultinject` test in `mediator/pg`) pins it
  and fails without the fix (design-notes 8.9).
* `TestConsumers_LeaseLossCancelsHandler` (`mediator/redisx`) read the
  outcome counters the instant the handler had recorded the delivery, a
  moment before the worker acknowledges and counts it; it failed once in the
  whole-tree integration run and now waits for the counter like its other
  steps.
* `mediator/ctl`'s CLI integration test hard-coded one migration; it now
  derives the expected version from the embedded migration files and checks
  both migrations by name (found by the coverage gate, whose first run
  failed there).
* Docs: design-notes 6 (two rows), 8.7 rewritten as fixed, 8.8 and 8.9
  added; this file rewritten.

## 3. What is left, in order

Against spec 14's definition of done for v1.0: every milestone's
deliverables exist and every tier is green locally; what remains is the
acceptance tail. Never run anywhere: the race detector, the mutation gate,
the benchmark baseline (an M1 acceptance item), soak mode, and CI itself
(including the TypeScript client job). Not started: the two weeks of green
nightlies, which need a remote. The items below are that tail, in order.

1. **Raise the coverage thresholds** (`coverThresholds` in
   `tools/task/tasks.go`, design-notes 8.10): `pg` 89.8 needs
   connection-level fault injection for the relay, janitor, migrate, outbox,
   and statement error branches (a pgx-level fault hook, or a proxy that
   drops the connection at a chosen statement); `testkit/invariants` 94.8
   and `testkit/workload` 93.9 need tests of their error returns;
   `pg/storetest` 80.1 is a conformance suite and can stay excused.
2. **Re-run the other seven chaos workloads** on the rebuilt image (only
   `events` was re-run after the 8.7 fixes; they were green before).
3. **Benchmark baseline** (`go run ./tools/task bench` then `bench-baseline`)
   on a quiet machine, and **mutation testing** (`task mutate`, gremlins,
   80 percent gate, nightly in CI). Neither has been run.
4. **CI.** No remote exists. The first push exercises tiers 0 to 4 and the
   openapi job for the first time, including the race detector, which has
   never run anywhere (no C compiler on this machine).
5. **Chaos at spec scale.** The nightly matrix is 5 minutes per cell over
   seeds 1 to 3 (`task chaos-matrix`, `CHAOS_DURATION`). The `kill` and
   `pg-restart` nemeses were drawn for the first time in seeds 7 and 8 of
   this session (60 s runs, both green); the other seven workloads have not
   been re-run since the 8.7 fixes (they were green before); the definition
   of done wants two weeks of green nightlies, which is calendar time. Soak
   mode (`-soak`) exists and has not been exercised.
6. **Commit** only when the user asks, with the attribution line the session
   specifies. The working tree currently holds the whole fifth session
   uncommitted (30 files, 26 modified and 4 new, see `git status`).

## 4. How to run each tier here

```sh
go run ./tools/task test                     # tiers 0-2 (no -race locally)
go run ./tools/task test-integration         # tier 4, testcontainers
SWEEP_QUICK=1 go test -tags faultsweep,faultinject -count=1 -timeout 60m -v ./test/faultsweep/...
go run ./tools/task test-sweep               # tier 3, full matrix
go run ./tools/task chaos-up                 # builds the node image (--build)
go run ./tools/task chaos -workload events -seed 1 -duration 60s
go run ./tools/task chaos-down
go run ./tools/task cover                    # needs Docker now
go run ./tools/task orders-up                # example service on :8080, then orders-down
```

`go test -v` in package-list mode buffers a package's output until it
finishes, so a sweep log stays empty for minutes while the containers are
visibly cycling in `docker ps`; pass a single package path or `-json` to
stream. The sweep uses testcontainers unless `PG_URL` and `REDIS_ADDR` are
both set (CI sets them after `task up`). A chaos run writes `summary.json`
(`pass`, per-checker `checkers.<name>.ok` and `violations`, `nemeses`),
`nemesis.jsonl`, and the node logs into its run directory; the node logs
carry one `consumer apply` line per delivery with `fencing=`, `partition=`,
`group=`, `key=`, `seq=`, and `event_id=`, and the event ID is a UUIDv7
whose first 48 bits are the creation time in milliseconds, which is how the
8.7 timelines were reconstructed. The fixes of 8.7 log `delivery rejected
by the partition fence; giving the lease up` and `claimed pending entries
below the batch`. Do not run the sweep and chaos at the same time, and do
not run `orders-up`/`orders-down` while the chaos stack is up: both
profiles belong to the same compose project, and `down -v` removes the
shared Postgres and Redis.

## 5. Environment facts (verified 2026-09-26)

* Windows 11, Git Bash for commands; `make` not installed (use
  `go run ./tools/task <name>`); Docker Desktop 29 with Linux containers and
  Compose v2.40 (it may not be running when a session starts: start
  `Docker Desktop.exe` and wait for the engine); Node 24 with npx;
  staticcheck and golangci-lint not installed (the user's golangci-lint is
  a Go 1.26 build and refuses the module; install v2.14.0 from source with
  Go 1.27 into a scratch `GOBIN`).
* No C compiler: `-race` cannot run locally; CI runs it. Use
  `go test -count=2 -shuffle=on` locally.
* go-redis is v9.22.0 with `ContextTimeoutEnabled`: a command runs
  synchronously on its connection and only the socket deadline comes from
  the context, so cancelling the context of a blocking `XREADGROUP` does not
  interrupt it; the read returns when Redis replies or `BLOCK` expires.
  Redis does unblock a reader whose stream key is deleted (flush).
* `encoding/json/v2` facts that bit agents: nil slices encode as `[]`, map
  order needs `json.Deterministic(true)`, unknown members need
  `json.RejectUnknownMembers(true)`, the map-merge tag is `json:",embed"`,
  `time.Duration` has no default encoding (request types must not use it
  in JSON fields).
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
  and the `mediator_partition_epoch` table are recorded there).
* Every I/O call site passes a literal fault point name from
  `mediator/testkit/faultpoints.txt`; `TestFaultPointCatalogue` fails on an
  unlisted literal, `TestParseCatalogue_GoldenFile` in `test/faultsweep/plan`
  pins the count, and `TestFaultSweepCompleteness` fails when a point is
  not swept by every kind (exclusions with reasons live in
  `test/faultsweep/zz_completeness_test.go`; a point with no `FaultAfter`
  call is excused from the ambiguous kind automatically). A new point must
  also be added to the `patterns` of the sweep scenario that exercises it.
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
  test/chaos, deploy); do not duplicate them here.
