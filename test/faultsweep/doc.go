// Package faultsweep is the fault sweep tier of spec 11.4: every I/O
// operation of the framework fails in every way at least once, in a small
// program with a known end state, and the invariants of spec 11.4 are
// checked against Postgres and Redis after recovery.
//
// The tests are built only with the tags faultsweep and faultinject:
//
//	go test -tags faultsweep,faultinject -count=1 -timeout 60m ./test/faultsweep/...
//	go run ./tools/task test-sweep
//
// The full matrix takes longer than go test's default 10 minute timeout, so
// pass -timeout (or SWEEP_QUICK=1 for a run of a few minutes). Without both
// tags this package holds nothing but this documentation; the enumeration,
// completeness, and dry-run helpers live in the tagless subpackage plan and
// run with plain go test.
//
// # How a sweep runs
//
// Each scenario (SweepCommandAtomicity, SweepRelay, SweepConsumer,
// SweepIdempotency, SweepCache, SweepRemote, SweepLease, SweepShutdown) runs
// once cleanly in a fresh Postgres schema with a fresh Redis key prefix and
// records testkit.Hits(). plan.Enumerate then produces one cell per
// (point, hit, kind) for the points the scenario is about, and each cell is
// a subtest named point#hit/kind that arms exactly one testkit.Schedule,
// runs the scenario, runs recovery, and verifies the scenario's end state
// and the invariants I1 to I7 and the fencing check through
// mediator/testkit/invariants. Recovery rebuilds every node of the cell
// (the nodes of one cell live in one process and are stopped together, as
// one SIGTERM would), terminates the stopped nodes' database backends as a
// process exit would, re-sends the keyed commands the clients had in flight,
// and waits until the relay, the consumers, and the remote server have
// drained. Every wait is bounded; the timeout kind blocks until the nearest
// deadline (2 s for requests and consumer deliveries, 10 s for the consumer
// loop's acknowledgements and claims).
//
// The kinds are the seven of spec 11.4 plus crash-before. The crash kind of
// testkit acts after an operation and only where the code calls
// testkit.FaultAfter (commit, outbox insert, idempotency store, relay mark,
// XADD, dead-letter XADD, RPC XADD, RPC reply); crash-before exits the
// process at every point before the operation runs. Both count as crash for
// the completeness gate. The ambiguous kind is likewise enumerated only at
// FaultAfter points; elsewhere it cannot act and the gate excuses it by
// construction (plan.AfterPoints scans the sources, so adding a FaultAfter
// call demands the cell). Crash cells run the scenario in a child process
// (this test binary re-executed with -test.run TestFaultSweepChild); the
// child exits with code 137 at the point, the parent verifies the database
// and Redis directly, runs a recovery child, waits for it to report idle,
// and verifies again. For the first hit of every FaultAfter point, and of
// pg.tx.commit, a pgrestart sub-cell also restarts Postgres between the
// crash and the recovery (Stop/Start of the testcontainers instance; docker
// restart of go-api-backend-postgres-1 on the env path).
//
// SweepShutdown uses the pseudo-kind shutdown: at every step of every loop
// the runtime context is canceled while the operation is delayed, which is
// how SIGTERM is modeled on a platform that cannot send it to a child. It
// checks that Runtime.Run returns nil, that every acknowledged entry has an
// inbox row, and that no lease survives.
//
// # Resource exhaustion (TestSweepVariants)
//
// pool1 runs the scenario's points with the error and timeout kinds on a
// request pool of size one (the relay and the janitor keep a small pool of
// their own, because the relay hijacks a connection for LISTEN); stmt50 sets
// statement_timeout=50ms and injects a 100 ms delay at every point; body
// sends the atomic command over HTTP with MaxBodyBytes one byte short of the
// request, expects 413 and no rows, then the exact size; oom measures the
// working set of a clean run with MEMORY USAGE, sets maxmemory to used_memory
// plus half of it so the next run fails with OOM midway, and restores
// maxmemory before recovery. The quick mode skips the variants unless
// SWEEP_VARIANTS names them.
//
// TestFaultSweepCompleteness (spec 11.8) compares the accumulated coverage
// with mediator/testkit/faultpoints.txt: every point must be exercised by
// every kind in at least one scenario, except the justified exclusions in
// zz_completeness_test.go, and a stale exclusion fails the test. It runs
// last in the process; set SWEEP_COVERAGE_FILE to persist and merge coverage
// across filtered or split runs.
//
// # Containers
//
// When PG_URL and REDIS_ADDR are both set the tests use them (CI does, after
// go run ./tools/task up); otherwise TestMain starts postgres:18 and redis:8
// through testcontainers, one pair for the package. Every scenario gets a
// fresh schema and a fresh key prefix; every cell truncates the schema and
// rotates the prefix, and child processes reuse the same containers.
//
// # Environment knobs
//
//	SWEEP_SCENARIOS   comma-separated scenario names to run (default all)
//	SWEEP_POINTS      comma-separated point patterns (exact or prefix*) to sweep
//	SWEEP_KINDS       comma-separated kinds; crash selects crash and crash-before
//	SWEEP_MAX_HITS    cap on hits per point (0 = every recorded hit, the default)
//	SWEEP_QUICK=1     first hit of every point only, no resource variants
//	SWEEP_VARIANTS    resource variants to run: pool1,stmt50,body,oom or none
//	SWEEP_PG_RESTART=0  skip the Postgres-restart crash variant
//	SWEEP_COVERAGE_FILE path of a JSON file that accumulates coverage across runs
//	SWEEP_LOG=1       verbose component logs on stderr
//
// CI runs the full matrix (no knobs); SWEEP_QUICK is for local iteration and
// still satisfies the completeness gate, since every kind acts at the first
// hit of every point.
package faultsweep
