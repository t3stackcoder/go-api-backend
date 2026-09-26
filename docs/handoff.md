# Handoff: implementing spec.md

Written for the next agent continuing this work. Read `spec.md` (the
contract), `docs/design-notes.md` (decisions, deviations, and the hardening
and tier outcomes in sections 7 and 8), then this file. Keep this file
current: update it in the same commit as the work it describes.

Last updated 2026-09-26 at the end of the fourth session (the one that
verified the WIP commit `43fca39` and found the two defects in section 3).

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
| Chaos tier (`test/chaos`, `cmd/chaosnode`) | complete; 7 of 8 workloads green on 60 s runs; `events` fails 2 of 4 seeds on the two open defects of section 3 (design-notes 8.7) |
| M7 hardening | done: G17 met (6 allocs, 0.44 µs), generic schema names, metrics single-sourced, retry rule, json/v2 tag grammar (design-notes 8.1 to 8.4) |
| Fault sweep tier (`test/faultsweep`) | complete; quick matrix green with the completeness gate passing (design-notes 8.6); the full matrix has not been run |
| Bug A, lease-handover reorder (G6, voluntary release) | fixed and verified: unit, integration (`TestConsumers_SurplusReleaseDrainsBeforeHandover`, `TestConsumers_ClaimsForeignEntriesBelowBatch`, `TestFault_ClaimForeignBefore`) |
| Bug B, relay data loss under wake-up load (G12) | fixed and verified: unit (`TestSlot_TailCheckPerBatch`, `TestSlot_RunLoop_DetectsLossOnWake`), integration (`TestIntegration_Relay_RedisFlushBetweenWakeups`) |
| Defect C, same-node re-acquire overlaps the old reader (G6, involuntary loss) | open; analyzed with a fix plan in design-notes 8.7 |
| Defect D, stale owner writes until its next renewal after Redis forgets its lease (G14) | open; analyzed with a fix plan in design-notes 8.7 |
| Example service in the compose stack | added as profile `orders` (`task orders-up` / `orders-down`); compose config validates; not yet started end to end |

Verified complete against the spec before this session: all 7 property
tests, 6 fuzz targets, 16 CLI commands, 20 metrics, 39 fault points, every
Makefile target.

## 2. What the fourth session did

The previous session committed `43fca39` ("WIP: fault sweep tier and the two
chaos bug fixes, unverified") without updating this file. This session
verified that commit, finished the small items around it, and analyzed what
the chaos re-run exposed.

* Bug A and Bug B: the relay fix the WIP message called "partial" was in
  fact complete in `mediator/pg/relay.go`. Both fixes pass their unit tests
  and their new integration tests under `-tags integration,faultinject`.
* Fault sweep, quick mode (`SWEEP_QUICK=1`, first hit of every point, no
  resource variants): all eight scenarios pass in 517 s and
  `TestFaultSweepCompleteness` passes. Per-kind coverage and the skip
  accounting are in design-notes 8.6.
* Chaos `events`, seeds 1 to 4, 60 s each, on the node image rebuilt with
  both fixes: seeds 3 and 4 pass; seeds 1 and 2 fail I6 (seed 1 also I3,
  seed 2 also the fencing checkers). The run directories are kept under
  `test/chaos/runs/` (gitignored): `events-seed1-20260926-165740` and
  `events-seed2-20260926-165925`. Root causes and fix plans: design-notes
  8.7 and section 3 below. No code was changed for them.
* `tools/task test-sweep` now passes `-timeout 60m` (the matrix exceeds go
  test's default; the CI sweep job is 75 minutes so the go test timeout
  fires first and prints goroutines).
* `tools/task cover` now runs with `-tags integration,faultinject` so the
  driver packages are measured with their container tests; the CI unit job
  is 45 minutes. The gate has NOT been run with the new tags yet.
* Compose profile `orders` (example service on :8080), tasks `orders-up` and
  `orders-down`, Makefile aliases, README section (design-notes 7.8).
* README: tier commands and Windows notes (race detector substitute,
  golangci-lint from source, stdin-EOF shutdown).
* `go mod tidy` removed the unused toxiproxy client and its transitive
  dependencies (the chaos controller speaks Toxiproxy's HTTP API directly).
* This file was rewritten; design-notes gained a section 4 bullet on the
  coverage tags, 7.8, 8.6, and 8.7.

## 3. What is left, in order

1. **Defect C** (design-notes 8.7, first item). In `mediator/redisx`:
   (a) `leaseManager` keeps the ended leases whose worker has not exited
   (`ending map[leaseKey]*lease`, filled by `end` for leases with a worker,
   cleared by a `workerDone` the worker defers after `exit`) and `rebalance`
   skips those partitions; (b) `foreignBefore` claims every pending entry
   below the batch, own name included, and skips the range scan when the
   XPENDING summary's lowest ID is at or above the batch; (c) fix the
   `readCtx` comment. Tests: a synctest lease-manager test for (a) next to
   `TestLeaseManager_WorkerOwnsSurplusRelease`; an integration test for (b)
   that reuses `ghostRead` with the node's own name so the ghost's entry
   lands in the node's PEL and must be applied before the next batch
   (`cfg.ClaimMinIdle` high enough that the periodic pass cannot be what
   restores the order).
2. **Defect D** (design-notes 8.7, second item). Migration
   `migrations/0002_partition_epoch.sql` (forward DDL, then `-- down`), the
   `pg.Tx.FencePartition` method in `pg/ports.go`, `pg/tx.go`, and
   `testkit/memstore/tx.go` (buffer the epoch per transaction, apply at
   commit, lock the row key like the inbox), a `storetest` conformance test
   (monotonic; equal token accepted; independent per partition; rollback
   keeps the old value; add the method to the read-only and closed-tx
   tables), the check in `pg.Inbox()` before `InboxInsert`, the sentinel
   `pg.ErrStaleLease`, the `redisx` `handle` branch that ends the lease and
   stops the worker on it, the fault point `pg.inbox.fence` in
   `mediator/testkit/faultpoints.txt`, and the section 6 rows in
   design-notes. `TestIntegration_Migrations_UpDownUp` exercises every
   migration; `TestInbox_*` in `pg/inbox_test.go` is where the behavior
   test goes (memstore, `mediator.WithFencingToken`).
3. **Verify both** in this order: `go test -count=1 ./mediator/...`;
   `go test -tags integration,faultinject -count=1 ./mediator/pg/ ./mediator/redisx/ ./mediator/testkit/...`;
   `go run ./tools/task chaos-up` (rebuilds the image) then `events` seeds
   1 to 4 (and 5 to 8 if time allows), then `chaos-down`; then the quick
   sweep, which must show `pg.inbox.fence` covered by every kind; then the
   full sweep (`go run ./tools/task test-sweep`, about an hour). Do not run
   the sweep and chaos at the same time.
4. **Coverage gate with the new tags.** Run `go run ./tools/task cover`
   (Docker) and read `coverage/summary.md`. Before this session the numbers
   were `pg` 89, `testkit/workload` and `testkit/invariants` about 94,
   `redisx` 92.5 to 95.6, all under the 95 default; `mediator`, `behavior`,
   `validate` are at 100 and must stay there. Either raise the packages
   below 95 with tests or add `-thresholds` to the cover task with the
   reason written next to it.
5. **Compose smoke test** of the `orders` profile: `go run ./tools/task
   orders-up`, then `GET http://localhost:8080/readyz` and `/openapi.json`,
   then `orders-down`.
6. **Benchmark baseline** (`go run ./tools/task bench` then `bench-baseline`)
   on a quiet machine, and **mutation testing** (`task mutate`, gremlins,
   80 percent gate, nightly in CI). Neither has been run.
7. **CI.** No remote exists. The first push exercises tiers 0 to 4 and the
   openapi job for the first time, including the race detector, which has
   never run anywhere (no C compiler on this machine).
8. **Chaos at spec scale.** The nightly matrix is 5 minutes per cell over
   seeds 1 to 3 (`task chaos-matrix`, `CHAOS_DURATION`). The `kill` and
   `pg-restart` nemeses have not been drawn in a live run yet; the
   definition of done wants two weeks of green nightlies, which is calendar
   time. Soak mode (`-soak`) exists and has not been exercised.
9. **Commit** only when the user asks, with the attribution line the session
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
go run ./tools/task cover                    # needs Docker now
```

`go test -v` in package-list mode buffers a package's output until it
finishes, so a sweep log stays empty for minutes while the containers are
visibly cycling in `docker ps`; pass a single package path or `-json` to
stream. The sweep uses testcontainers unless `PG_URL` and `REDIS_ADDR` are
both set (CI sets them after `task up`). A chaos run writes `summary.json`,
`nemesis.jsonl`, and the node logs into its run directory; the node logs
carry one `consumer apply` line per delivery with `fencing=`, `partition=`,
`group=`, `key=`, `seq=`, and `event_id=`, and the event ID is a UUIDv7
whose first 48 bits are the creation time in milliseconds, which is how the
8.7 timelines were reconstructed.

## 5. Environment facts (verified 2026-09-26)

* Windows 11, Git Bash for commands; `make` not installed (use
  `go run ./tools/task <name>`); Docker Desktop 29 with Linux containers and
  Compose v2.40; Node 24 with npx; staticcheck and golangci-lint not
  installed (the user's golangci-lint is a Go 1.26 build and refuses the
  module; install v2.14.0 from source with Go 1.27 into a scratch `GOBIN`).
* No C compiler: `-race` cannot run locally; CI runs it. Use
  `go test -count=2 -shuffle=on` locally.
* go-redis is v9.22.0 with `ContextTimeoutEnabled`: a command runs
  synchronously on its connection and only the socket deadline comes from
  the context, so cancelling the context of a blocking `XREADGROUP` does not
  interrupt it; the read returns when Redis replies or `BLOCK` expires.
* `encoding/json/v2` facts that bit agents: nil slices encode as `[]`, map
  order needs `json.Deterministic(true)`, unknown members need
  `json.RejectUnknownMembers(true)`, the map-merge tag is `json:",embed"`,
  `time.Duration` has no default encoding (request types must not use it
  in JSON fields).
* `httptest.NewTestServer(t, h)` works inside `synctest.Test`.
* Postgres advisory locks used by the relay, janitor, and migrations are
  database-global: integration tests that run them are sequential or use a
  database per test.

## 6. Conventions

* Exported APIs of finished packages are frozen; a needed change is recorded
  in `docs/design-notes.md` first (Defect D adds a `pg.Tx` method: record
  it in section 6 in the same commit).
* Every I/O call site passes a literal fault point name from
  `mediator/testkit/faultpoints.txt`; `TestFaultPointCatalogue` fails on an
  unlisted literal, and `TestFaultSweepCompleteness` fails when a point is
  not swept by every kind (exclusions with reasons live in
  `test/faultsweep/zz_completeness_test.go`).
* Integration tests sit beside the code as `*_integration_test.go` with
  `//go:build integration` and their own `TestMain` (testcontainers
  `postgres:18`, `redis:8`); `test/integration` holds cross-package scenarios.
* Unreachable lines carry `// covergate:ignore <reason>`; `tools/covergate`
  rejects an empty reason.
* Decisions that are not in spec.md are indexed in design-notes section 7
  (pg, redisx, behavior, httpapi and openapi, names, test/integration,
  test/chaos, deploy); do not duplicate them here.
