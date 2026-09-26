// Package chaos is the chaos tier of spec 11.6 (milestones M5 and M6): a
// Jepsen-style harness that drives the compose `chaos` profile (three
// chaosnode containers behind per-node Toxiproxy listeners, Postgres, Redis)
// with the eight workloads and fifteen nemeses of the spec, records a
// Jepsen history, and runs every checker.
//
// The package builds only with the `chaos` tag, because it needs Docker, the
// Toxiproxy API, and the stack on localhost. Bring the stack up and run one
// workload with:
//
//	go run ./tools/task chaos-up
//	go run ./tools/task chaos -workload register -seed 1 -duration 60s
//	go run ./tools/task chaos-down
//
// which is the same as
//
//	go test -tags chaos -count=1 -run TestChaos -timeout 40m ./test/chaos/ \
//	    -workload=register -seed=1 -duration=60s
//
// Flags (each with an environment fallback): -workload (CHAOS_WORKLOAD),
// -seed (CHAOS_SEED), -duration (CHAOS_DURATION, the mayhem phase), -nodes
// (CHAOS_NODES), -run-dir (CHAOS_RUN_DIR, the parent of the run directories),
// -recovery-bound (CHAOS_RECOVERY_BOUND, G15), -nemesis-rate
// (CHAOS_NEMESIS_RATE, the mean interval between nemeses; the spec's 5 to
// 15 s is the default 10s), -soak (CHAOS_SOAK, low nemesis rate and invariant
// checks every 5 minutes), -clients (CHAOS_CLIENTS, logical clients per
// node), -rate (CHAOS_RATE, operations per second per client), -check-timeout
// (CHAOS_CHECK_TIMEOUT, the Porcupine budget). The stack's addresses default
// to the compose file: nodes on http://localhost:18081.., Postgres on
// localhost:5432 (app/app/app), Redis on localhost:6379, the Toxiproxy API on
// localhost:8474, and containers named go-api-backend-<service>-1; override
// them with CHAOS_NODE_ADDRS (comma separated), CHAOS_PG_URL,
// CHAOS_REDIS_ADDR, CHAOS_TOXIPROXY_URL, and CHAOS_CONTAINER_PREFIX.
//
// Schedule: reset the stores and restart the nodes, warm up for 10 s, mayhem
// for the duration with a nemesis every 5 to 15 s and at most two active,
// heal (remove every toxic, unpause and start everything, reset clocks,
// relays, and handler placement), quiesce within the recovery bound (L1),
// resolve every retired info operation of a keyed command by re-sending it
// (L2), final reads, then every checker: the workload checker, invariants
// I1 to I7 directly against the stores, fencing monotonicity from the node
// logs, the log scan, and the shutdown check.
//
// Every run writes a directory under test/chaos/runs/ (gitignored) named
// <workload>-seed<seed>-<timestamp> holding:
//
//	seed             the seed, one line
//	config.json      the effective configuration
//	history.jsonl    the Jepsen history (Appendix C), one op per line
//	nemesis.jsonl    every nemesis start and end with its parameters
//	summary.json     seed, workload, duration, op and nemesis counts,
//	                 checker results, phase timings, pass/fail
//	controller.log   the controller's own log
//	<node>.log       docker logs of every node, taken at the end
//	postgres.log, redis.log, toxiproxy.log
//	porcupine.html   the Porcupine visualization, written on failure
//	redis-snapshot/  the Redis data directory copied for redis-restore-old
//
// TestReplay re-runs the offline checkers (the workload checker, fencing,
// log scan, shutdown) on a recorded run directory without Docker:
//
//	go test -tags chaos -count=1 -run TestReplay ./test/chaos/ -args -replay-dir=test/chaos/runs/<dir>
//
// or with CHAOS_REPLAY_DIR set. The database invariants need the live stores
// and are not replayed.
package chaos
