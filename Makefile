# Thin aliases over `go run ./tools/task <name>` (spec 12.2).
#
# POSIX make, runs under Git Bash on Windows. Nothing depends on make itself:
# every target below is exactly `go run ./tools/task <name> [args]`, so on a
# machine without make run that directly. `make help` lists the tasks.

GO   ?= go
TASK  = $(GO) run ./tools/task

# `make chaos WORKLOAD=bank SEED=2 DURATION=10m`
WORKLOAD ?= register
SEED     ?= 1
DURATION ?= 5m

# `make fuzz FUZZTIME=5m`
FUZZTIME ?= 30s

.PHONY: help up down orders-up orders-down chaos-up chaos-down fmt lint test fuzz test-integration \
        test-sweep chaos chaos-matrix openapi openapi-check cover mutate \
        bench bench-baseline ci

help:
	$(TASK) help

# Compose without profiles: Postgres and Redis only.
up:
	$(TASK) up

down:
	$(TASK) down

# Compose with the orders profile: adds the example service of spec 13 on :8080.
orders-up:
	$(TASK) orders-up

orders-down:
	$(TASK) orders-down

# Compose with the chaos profile: adds Toxiproxy and node1..node3.
chaos-up:
	$(TASK) chaos-up

chaos-down:
	$(TASK) chaos-down

# Tier 0.
fmt:
	$(TASK) fmt

lint:
	$(TASK) lint

# Tiers 0 to 2 (unit, property, short fuzz corpora as regression tests).
test:
	$(TASK) test

# Every Fuzz target for FUZZTIME each.
fuzz:
	$(TASK) fuzz -fuzztime $(FUZZTIME)

# Tier 4 via testcontainers (no compose needed).
test-integration:
	$(TASK) test-integration

# Tier 3 (needs `make up`).
test-sweep:
	$(TASK) test-sweep

# One chaos run against the compose chaos profile (needs `make chaos-up`).
chaos:
	$(TASK) chaos -workload=$(WORKLOAD) -seed=$(SEED) -duration=$(DURATION)

# The full chaos matrix: every workload x seeds 1,2,3; CHAOS_DURATION from env.
chaos-matrix:
	$(TASK) chaos-matrix

# Regenerate api/openapi.json from the example service.
openapi:
	$(TASK) openapi

openapi-check:
	$(TASK) openapi-check

# Coverage report and the covergate thresholds.
cover:
	$(TASK) cover

# Mutation testing on the core packages (gremlins).
mutate:
	$(TASK) mutate

# Benchmarks with benchstat against the stored baseline.
bench:
	$(TASK) bench

bench-baseline:
	$(TASK) bench-baseline

# fmt, lint, test, cover in sequence.
ci:
	$(TASK) ci
