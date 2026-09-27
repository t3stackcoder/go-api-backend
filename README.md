# go-api-backend

A MediatR-style CQRS framework for Go 1.27: typed requests with exactly one
handler, composable pipeline behaviors, domain events through a transactional
outbox in Postgres 18, durable consumers and caching on Redis 8, cross-process
dispatch, an HTTP adapter with code-first OpenAPI 3.1, and a test program that
makes every guarantee falsifiable (SQLite-style fault sweeps and Jepsen-style
chaos runs).

- [spec.md](spec.md) is the complete specification and wins over everything else.
- [docs/design-notes.md](docs/design-notes.md) records the package contracts,
  the import graph, and the deliberate deviations from the spec.
- [spec-billing.md](spec-billing.md) specifies the first application built on
  the framework, a usage-based billing service with Polar as the payment
  provider; it is written, not yet implemented.

## Layout

| Path | What |
|---|---|
| `mediator/` | core: markers, registry, `Send`/`Publish`/`Stream`, pipeline |
| `mediator/behavior/` | the standard behavior set in its fixed order |
| `mediator/validate/` | tag grammar, validator, JSON Schema mapping |
| `mediator/pg/`, `mediator/redisx/` | Postgres unit of work, outbox, inbox, idempotency; Redis streams, leases, cache, limiter, remote dispatch |
| `mediator/httpapi/`, `mediator/openapi/` | routing, problem+json, SSE; document generation |
| `mediator/testkit/` | fault points, fakes, clock, history, invariants |
| `cmd/mediatorctl`, `cmd/chaosnode` | operations CLI; chaos node binary |
| `examples/orders/` | sample service, also the OpenAPI fixture |
| `test/integration`, `test/faultsweep`, `test/chaos` | tiers 3 to 6 |
| `deploy/` | compose stack, Dockerfile, Toxiproxy proxies, Postgres init |
| `tools/task/` | Go implementation of every Makefile target |
| `tools/covergate/` | per-package coverage thresholds |
| `api/openapi.json` | committed OpenAPI document, drift-checked in CI |

## Running things

Every target is a Go program, so `make` is optional:

```sh
go run ./tools/task help            # list every task
go run ./tools/task test            # tiers 0-2: go test -shuffle=on -count=2 ./...
go run ./tools/task up              # Postgres 18 and Redis 8 via docker compose
go run ./tools/task down
go run ./tools/task orders-up       # the same plus the example service on http://localhost:8080
go run ./tools/task orders-down
go run ./tools/task cover           # coverage profile + covergate thresholds
go run ./tools/task lint            # go vet + staticcheck, govulncheck, golangci-lint when installed
go run ./tools/task fuzz -fuzztime 30s
go run ./tools/task test-integration   # tier 4, testcontainers
go run ./tools/task test-sweep         # tier 3, after `up`; SWEEP_QUICK=1 for a short local run
go run ./tools/task chaos-up
go run ./tools/task chaos -workload=register -seed=1 -duration=5m
go run ./tools/task chaos-down
```

With make installed the same targets are `make test`, `make up`, `make cover`,
`make chaos WORKLOAD=bank SEED=2 DURATION=10m`, and so on (see `Makefile`).

## Test tiers

| Tier | Command | Needs |
|---|---|---|
| 0 static | `task fmt`, `task lint` | nothing (optional linters are skipped when absent, required in CI) |
| 1 unit, 2 property and fuzz | `task test`, `task fuzz` | nothing |
| 3 fault sweep | `task up` then `task test-sweep` | Postgres and Redis |
| 4 integration | `task test-integration` | Docker (testcontainers) |
| 5 chaos | `task chaos-up` then `task chaos` / `task chaos-matrix` | compose `chaos` profile |
| 6 soak and bench | `task bench`, `task bench-baseline`, `task mutate` | benchstat, gremlins |

Coverage gates: 100 percent for `mediator`, `mediator/behavior`,
`mediator/validate`; 95 percent elsewhere. A line may be excluded with
`// covergate:ignore <reason>`; the reason is mandatory and every exclusion is
listed in the CI summary.

## Windows

The Makefile is POSIX and runs under Git Bash, but nothing requires it: use
`go run ./tools/task <name>`. The race detector needs cgo and a C compiler;
`task test` enables `-race` automatically when `CGO_ENABLED=1` and the
compiler from `go env CC` is on PATH, and says which mode it used. CI runs
with the race detector on Linux; locally, `go test -count=2 -shuffle=on` is
the substitute. Docker Desktop with Linux containers is enough for the
compose stack, Toxiproxy, and the chaos nemeses. A prebuilt golangci-lint
compiled with an older Go refuses this module: install it from source with
the module's Go (`go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0`).
Windows cannot send SIGTERM to a child process, so the example service also
shuts down on stdin EOF when `SHUTDOWN_ON_STDIN_EOF=1`; the integration tier
uses that.
