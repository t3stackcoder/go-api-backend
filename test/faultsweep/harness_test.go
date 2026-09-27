//go:build faultsweep && faultinject

package faultsweep

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
	"github.com/t3stackcoder/go-api-backend/test/faultsweep/plan"
)

// Timing of the sweep. Every wait is bounded; the timeout kind blocks until
// the nearest deadline, so request deadlines are short.
const (
	sendTimeout     = 3 * time.Second       // client-side deadline of one Send
	behaviorTimeout = 2 * time.Second       // Timeout behavior, requests and consumers
	lockTimeout     = time.Second           // SET LOCAL lock_timeout of every unit of work
	runWait         = 6 * time.Second       // bound of the run phase's quiescence wait
	recoverWait     = 30 * time.Second      // bound of the recovery's quiescence wait
	stopWait        = 25 * time.Second      // bound of a component shutdown
	cellTimeout     = 4 * time.Minute       // bound of one cell
	pollEvery       = 25 * time.Millisecond // state polling interval
	leaseTTL        = 1500 * time.Millisecond
	leaseRenew      = 250 * time.Millisecond
	claimMinIdle    = 500 * time.Millisecond
	readBlock       = 250 * time.Millisecond
	relayPoll       = 300 * time.Millisecond
	defaultDelay    = 300 * time.Millisecond // FaultDelay unless the scenario says otherwise
	partitions      = 1                      // P: one partition keeps every key on one stream
	maxConnsDefault = 8
)

// Pseudo-kinds of the matrix beyond testkit's seven.
const (
	kindCrashBefore = "crash-before" // os.Exit(137) before the operation, at every point
	kindShutdown    = "shutdown"     // runtime context canceled during the step (SweepShutdown)
)

// faultKinds are the kinds a scenario enumerates by default.
var faultKinds = []string{
	string(testkit.FaultError), string(testkit.FaultPermanent), string(testkit.FaultTimeout),
	string(testkit.FaultDelay), string(testkit.FaultAmbiguous), string(testkit.FaultCancel),
	string(testkit.FaultCrash), kindCrashBefore,
}

// gateKinds are the seven kinds of spec 11.4 the completeness gate requires.
var gateKinds = func() []string {
	out := make([]string, len(testkit.AllFaultKinds))
	for i, k := range testkit.AllFaultKinds {
		out[i] = string(k)
	}
	return out
}()

// coverageKind maps a matrix kind to the gate kind it exercises.
func coverageKind(kind string) string {
	if kind == kindCrashBefore {
		return string(testkit.FaultCrash)
	}
	return kind
}

// knobs are the environment switches documented in doc.go.
type knobs struct {
	scenarios    []string
	points       []string
	kinds        []string
	maxHits      int
	quick        bool
	variants     []string
	pgRestart    bool
	coverageFile string
	verbose      bool
}

var opt = loadKnobs()

func loadKnobs() knobs {
	k := knobs{
		scenarios:    plan.SplitList(os.Getenv("SWEEP_SCENARIOS")),
		points:       plan.SplitList(os.Getenv("SWEEP_POINTS")),
		kinds:        plan.SplitList(os.Getenv("SWEEP_KINDS")),
		quick:        os.Getenv("SWEEP_QUICK") == "1",
		variants:     plan.SplitList(os.Getenv("SWEEP_VARIANTS")),
		pgRestart:    os.Getenv("SWEEP_PG_RESTART") != "0",
		coverageFile: os.Getenv("SWEEP_COVERAGE_FILE"),
		verbose:      os.Getenv("SWEEP_LOG") == "1",
	}
	if v := os.Getenv("SWEEP_MAX_HITS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			log := slog.Default()
			log.Warn("SWEEP_MAX_HITS ignored", "value", v)
		} else {
			k.maxHits = n
		}
	} else if k.quick {
		k.maxHits = 1
	}
	return k
}

func (k knobs) scenarioSelected(name string) bool {
	return len(k.scenarios) == 0 || slices.Contains(k.scenarios, name)
}

func (k knobs) kindSelected(kind string) bool {
	if len(k.kinds) == 0 {
		return true
	}
	if slices.Contains(k.kinds, kind) {
		return true
	}
	return kind == kindCrashBefore && slices.Contains(k.kinds, string(testkit.FaultCrash))
}

func (k knobs) variantSelected(name string) bool {
	if k.quick && len(k.variants) == 0 {
		return false
	}
	if len(k.variants) == 0 {
		return true
	}
	if slices.Contains(k.variants, "none") {
		return false
	}
	return slices.Contains(k.variants, name)
}

// Process-wide records for the completeness gate.
var (
	coverage = plan.NewCoverage()
	// sweepsRun counts the scenario sweeps that ran in this process.
	sweepsRun sync.Map
	// afterPointsOnce scans the sources for FaultAfter call sites.
	afterPointsOnce sync.Once
	afterPointsList []string
	afterPointsErr  error
)

// afterPoints returns the points at which ambiguous and crash act.
func afterPoints(t testing.TB) []string {
	t.Helper()
	afterPointsOnce.Do(func() {
		afterPointsList, afterPointsErr = plan.AfterPoints(sourceRoot())
	})
	if afterPointsErr != nil {
		t.Fatalf("scan FaultAfter points: %v", afterPointsErr)
	}
	return afterPointsList
}

// sourceRoot is the module root: go test runs with the package directory
// as the working directory.
func sourceRoot() string { return "../.." }

func isAfterPoint(t testing.TB, point string) bool {
	return slices.Contains(afterPoints(t), point)
}

// scenario is one program of spec 11.4. build creates the nodes and
// components of a cell (not started); run is the body and its bounded wait
// for the run phase to end; recover runs on a fresh build after the faulty
// one was stopped and must leave the system quiescent; verify checks the
// scenario's expected end state; expect lists the commands I1 checks.
type scenario struct {
	name     string
	patterns []string
	kinds    []string
	delay    time.Duration
	groups   []string
	skipDLQ  bool
	// noInvariants skips invariants.All (scenarios without Postgres tables).
	noInvariants bool
	variants     []string
	newState     func() any
	// preflight runs once before the clean cell, before hits are recorded.
	preflight func(ctx context.Context, c *cellRun) error
	build     func(ctx context.Context, c *cellRun) error
	run       func(ctx context.Context, c *cellRun) error
	recover   func(ctx context.Context, c *cellRun) error
	verify    func(ctx context.Context, c *cellRun, v *verifier)
	expect    func(c *cellRun) map[string]invariants.Expect
	// crashCheck runs in the parent between a crash and its recovery, on
	// top of the generic point-in-time checks.
	crashCheck func(ctx context.Context, c *cellRun, v *verifier)
}

func (sc *scenario) enumeratedKinds() []string {
	if sc.kinds != nil {
		return sc.kinds
	}
	return faultKinds
}

func (sc *scenario) faultDelay() time.Duration {
	if sc.delay > 0 {
		return sc.delay
	}
	return defaultDelay
}

// allScenarios is the registry, in the order the tests run.
var allScenarios = []*scenario{
	scenarioAtomicity, scenarioRelay, scenarioConsumer, scenarioIdempotency,
	scenarioCache, scenarioRemote, scenarioLease, scenarioShutdown,
}

func scenarioByName(name string) *scenario {
	for _, sc := range allScenarios {
		if sc.name == name {
			return sc
		}
	}
	return nil
}

// ---------------------------------------------------------------- environment

// scenarioEnv is one scenario's schema and the rotating Redis prefix of its
// cells. Every cell truncates the schema and rotates the prefix, and the
// backends of the previous cell's pools are terminated first so that a
// connection leaked by an injected fault cannot block the truncation or
// hold a relay slot.
type scenarioEnv struct {
	name   string
	schema string
	prefix string
	prev   string
	cellN  int
	// child is set in a child process: the schema exists and the prefix is
	// the parent's; reset is never called.
	child bool
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// newScenarioEnv creates the schema with the framework and workload tables.
func newScenarioEnv(t *testing.T, name string) *scenarioEnv {
	t.Helper()
	ctx := context.Background()
	e := &scenarioEnv{name: name, schema: "fs_" + strings.ToLower(strings.TrimPrefix(name, "Sweep")) + "_" + randomHex(3)}
	r, err := root(ctx)
	if err != nil {
		t.Fatalf("root pool: %v", err)
	}
	if _, err := r.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{e.schema}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	pool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: e.schema, MaxConns: 2, ApplicationName: e.appName("setup")})
	if err != nil {
		t.Fatalf("setup pool: %v", err)
	}
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := workload.Migrate(ctx, pool); err != nil {
		t.Fatalf("workload migrate: %v", err)
	}
	pool.Close()
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		r, err := root(cctx)
		if err != nil {
			return
		}
		_ = terminateBackends(cctx, r, e.appName("%"))
		_, _ = r.Exec(cctx, "DROP SCHEMA "+pgx.Identifier{e.schema}.Sanitize()+" CASCADE")
		if client, err := redisx.NewClient(cctx, redisx.Config{Addr: redisAddr}); err == nil {
			_ = deletePrefix(cctx, client, e.prefix)
			_ = deletePrefix(cctx, client, e.prev)
			_ = client.Close()
		}
	})
	return e
}

// appName is the application_name of a pool of this scenario; the suffix
// identifies the cell so leaked backends can be terminated selectively.
func (e *scenarioEnv) appName(suffix string) string { return "fs-" + e.schema + "-" + suffix }

// terminateBackends kills every backend whose application_name matches the
// LIKE pattern, except the caller's own.
func terminateBackends(ctx context.Context, pool *pgxpool.Pool, pattern string) error {
	_, err := pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name LIKE $1 AND pid <> pg_backend_pid()`, pattern)
	return err
}

// frameworkTables are truncated between cells together with the workload's.
var frameworkTables = []string{"mediator_outbox", "mediator_stream_seq", "mediator_inbox", "mediator_idempotency", "mediator_relay_cursor", "mediator_partition_epoch"}

// reset prepares the schema and a new prefix for the next cell.
func (e *scenarioEnv) reset(ctx context.Context) error {
	if e.child {
		return errors.New("reset in a child process")
	}
	r, err := root(ctx)
	if err != nil {
		return err
	}
	if e.cellN > 0 {
		if err := terminateBackends(ctx, r, e.appName("%")); err != nil {
			return fmt.Errorf("terminate backends: %w", err)
		}
	}
	names := make([]string, 0, len(frameworkTables)+len(workload.Tables))
	for _, t := range append(append([]string{}, frameworkTables...), workload.Tables...) {
		names = append(names, pgx.Identifier{e.schema, t}.Sanitize())
	}
	if _, err := r.Exec(ctx, "TRUNCATE "+strings.Join(names, ", ")+" RESTART IDENTITY"); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}
	bank := pgx.Identifier{e.schema, "wl_bank"}.Sanitize()
	for _, a := range workload.Accounts {
		if _, err := r.Exec(ctx, "INSERT INTO "+bank+" (account, balance) VALUES ($1, $2)", a, workload.InitialBalance); err != nil {
			return fmt.Errorf("seed bank: %w", err)
		}
	}
	client, err := redisx.NewClient(ctx, redisx.Config{Addr: redisAddr})
	if err != nil {
		return err
	}
	defer client.Close()
	// The previous cell's keys survive one more cell so that the oom
	// variant can measure the working set of a clean run; the older ones go.
	if err := deletePrefix(ctx, client, e.prev); err != nil {
		return err
	}
	e.cellN++
	e.prev, e.prefix = e.prefix, fmt.Sprintf("fs%s%d", strings.TrimPrefix(e.schema, "fs_"), e.cellN)
	return nil
}

// deletePrefix unlinks every key under prefix.
func deletePrefix(ctx context.Context, client *redis.Client, prefix string) error {
	if prefix == "" {
		return nil
	}
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+":*", 1000).Result()
		if err != nil {
			return fmt.Errorf("scan %s: %w", prefix, err)
		}
		if len(keys) > 0 {
			if err := client.Unlink(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("unlink: %w", err)
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

// redisConfig is the fast configuration of every node of a cell.
func (e *scenarioEnv) redisConfig(nodeID string) redisx.Config {
	return redisx.Config{
		Addr: redisAddr, Prefix: e.prefix, NodeID: nodeID, PartitionsPerTopic: partitions,
		LeaseTTL: leaseTTL, LeaseRenew: leaseRenew, ClaimMinIdle: claimMinIdle,
		ReadBlock: readBlock, ReadBatch: 16, MaxAttempts: 4, CacheDefaultTTL: time.Minute, ReplyStreamMaxLen: 1000,
	}.WithDefaults()
}

// ------------------------------------------------------------------ verifier

// verifier is a fresh pool and client for reading the end state after every
// node of a cell was stopped.
type verifier struct {
	t      testing.TB
	env    *scenarioEnv
	pool   *pgxpool.Pool
	client *redis.Client
	cfg    redisx.Config
}

func (e *scenarioEnv) verifier(ctx context.Context, t testing.TB) (*verifier, error) {
	pool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: e.schema, MaxConns: 3, ApplicationName: e.appName("verify")})
	if err != nil {
		return nil, fmt.Errorf("verifier pool: %w", err)
	}
	cfg := e.redisConfig("verify")
	client, err := redisx.NewClient(ctx, cfg)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("verifier redis: %w", err)
	}
	return &verifier{t: t, env: e, pool: pool, client: client, cfg: cfg}, nil
}

func (v *verifier) close() {
	_ = v.client.Close()
	v.pool.Close()
}

func (v *verifier) inv(groups []string) invariants.Env {
	if groups == nil {
		groups = []string{}
	}
	return invariants.Env{Pool: v.pool, Redis: v.client, Cfg: v.cfg, Groups: groups,
		Topics: []string{workload.TopicBumped}, Partitions: partitions, Bound: invariants.DefaultBound, Poll: pollEvery}
}

// count runs a scalar query.
func (v *verifier) count(ctx context.Context, sql string, args ...any) int64 {
	v.t.Helper()
	var n int64
	if err := v.pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		v.t.Errorf("verify query %q: %v", sql, err)
	}
	return n
}

// expectCount asserts a scalar query result.
func (v *verifier) expectCount(ctx context.Context, want int64, what, sql string, args ...any) {
	v.t.Helper()
	if got := v.count(ctx, sql, args...); got != want {
		v.t.Errorf("%s: got %d, want %d", what, got, want)
	}
}

// ---------------------------------------------------------------------- node

// nodeOpts configures newNode.
type nodeOpts struct {
	id       string
	deps     workload.Deps
	workload bool
	remote   *redisx.Remote
	register func(m *mediator.Mediator) error
	maxConns int32
	params   map[string]string
	// noStore builds a node with a Redis client only (SweepCache).
	noStore bool
}

// node is one process-in-a-process: pools, store, client, mediator, and the
// components a scenario added. Components start together and stop together
// with a bounded wait; a component that ignores cancellation is reported.
type node struct {
	id      string
	pool    *pgxpool.Pool
	aux     *pgxpool.Pool
	store   *pg.PgStore
	client  *redis.Client
	cfg     redisx.Config
	m       *mediator.Mediator
	logger  *slog.Logger
	comps   []*comp
	closers []func()
	cancel  context.CancelFunc
}

type comp struct {
	name string
	c    mediator.Component
	done chan error
}

func (n *node) add(name string, c mediator.Component) {
	n.comps = append(n.comps, &comp{name: name, c: c, done: make(chan error, 1)})
}

func (n *node) onClose(f func()) { n.closers = append(n.closers, f) }

func (n *node) start(ctx context.Context) {
	cctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n.cancel = cancel
	for _, c := range n.comps {
		go func() {
			defer func() {
				if p := recover(); p != nil {
					c.done <- fmt.Errorf("panic: %v", p)
				}
			}()
			c.done <- c.c.Run(cctx)
		}()
	}
}

// stopComponents cancels the components (the node's SIGTERM).
func (n *node) stopComponents() {
	if n.cancel != nil {
		n.cancel()
	}
}

// awaitComponents waits until the deadline for every component to return,
// then closes the client and the pools without waiting for leaked
// connections. A component that ignores cancellation is reported.
func (n *node) awaitComponents(deadline <-chan time.Time) []error {
	var errs []error
	if n.cancel != nil {
		for _, c := range n.comps {
			select {
			case err := <-c.done:
				if err != nil && !errors.Is(err, context.Canceled) {
					errs = append(errs, fmt.Errorf("component %s returned %w", c.name, err))
				}
			case <-deadline:
				errs = append(errs, fmt.Errorf("component %s did not stop within %s", c.name, stopWait))
			}
		}
		n.cancel = nil
	}
	for _, f := range n.closers {
		f()
	}
	if n.client != nil {
		_ = n.client.Close()
	}
	for _, p := range []*pgxpool.Pool{n.pool, n.aux} {
		if p != nil {
			go p.Close() // may block on a connection leaked by an injected rollback fault
		}
	}
	return errs
}

// ------------------------------------------------------------------- cellRun

// cellRun is the state of one cell: the environment, the armed run, the
// nodes of the current phase, and the scenario's own state.
type cellRun struct {
	t       *testing.T
	env     *scenarioEnv
	sc      *scenario
	run     plan.Run
	variant *variant
	ctx     context.Context
	logger  *slog.Logger
	child   bool
	nodes   []*node
	st      any

	// phase is phaseRun during the faulty run and phaseRecover afterwards.
	phase string

	mu        sync.Mutex
	curCancel context.CancelFunc
	curID     int
	responses map[string][][]byte
	notes     []string
	nodeN     int
	// runtimeCancel is installed by SweepShutdown for the shutdown kind.
	runtimeCancel func()
}

// Phases of a cell.
const (
	phaseRun     = "run"
	phaseRecover = "recover"
)

func newCellRun(t *testing.T, env *scenarioEnv, sc *scenario, run plan.Run, v *variant) *cellRun {
	c := &cellRun{t: t, env: env, sc: sc, run: run, variant: v, logger: newLogger(), responses: map[string][][]byte{}}
	if sc.newState != nil {
		c.st = sc.newState()
	}
	return c
}

func newLogger() *slog.Logger {
	if opt.verbose {
		return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// note records an informational line reported with the cell.
func (c *cellRun) note(format string, args ...any) {
	c.mu.Lock()
	c.notes = append(c.notes, fmt.Sprintf(format, args...))
	c.mu.Unlock()
}

// reqCtx derives a request context with the Send deadline and registers its
// cancel func for the cancel kind.
func (c *cellRun) reqCtx(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(parent, sendTimeout)
	c.mu.Lock()
	c.curID++
	id := c.curID
	c.curCancel = cancel
	c.mu.Unlock()
	return ctx, func() {
		c.mu.Lock()
		if c.curID == id {
			c.curCancel = nil
		}
		c.mu.Unlock()
		cancel()
	}
}

// cancelRequest cancels the in-flight request context, if any.
func (c *cellRun) cancelRequest() {
	c.mu.Lock()
	cancel := c.curCancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// nodeID names a node of the current phase: recovery nodes carry a distinct
// ID, so a lease or a pending entry left by a stopped node is recognizable
// and the takeover path (XAUTOCLAIM after ClaimMinIdle) is exercised.
func (c *cellRun) nodeID(base string) string {
	if c.phase == phaseRecover {
		return base + "r"
	}
	return base
}

// runningNodeIDs lists the IDs of the cell's current nodes.
func (c *cellRun) runningNodeIDs() []string {
	ids := make([]string, 0, len(c.nodes))
	for _, n := range c.nodes {
		ids = append(ids, n.id)
	}
	return ids
}

// send dispatches req on m under a request context.
func (c *cellRun) send(ctx context.Context, m *mediator.Mediator, req any) (any, error) {
	rctx, cancel := c.reqCtx(ctx)
	defer cancel()
	return m.SendAny(rctx, req)
}

// sendKeyed is send with the idempotency key also in the context, which
// remote dispatch needs for the ack timing of spec 7.6.
func (c *cellRun) sendKeyed(ctx context.Context, m *mediator.Mediator, key string, req any) (any, error) {
	return c.send(mediator.WithIdempotencyKey(ctx, key), m, req)
}

// retrySend re-sends a keyed command until it succeeds or attempts run out,
// the client behavior after an error or an ambiguous outcome.
func (c *cellRun) retrySend(ctx context.Context, m *mediator.Mediator, key string, req any, attempts int) (any, error) {
	var res any
	var err error
	for i := 0; i < attempts; i++ {
		if key != "" {
			res, err = c.sendKeyed(ctx, m, key, req)
		} else {
			res, err = c.send(ctx, m, req)
		}
		if err == nil {
			return res, nil
		}
		if ctx.Err() != nil {
			return nil, err
		}
		time.Sleep(150 * time.Millisecond)
	}
	return nil, err
}

// record keeps the JSON of a successful response for the byte-identical
// check of I4.
func (c *cellRun) record(key string, res any) {
	b, err := json.Marshal(res)
	if err != nil {
		c.t.Errorf("encode response for %s: %v", key, err)
		return
	}
	c.mu.Lock()
	c.responses[key] = append(c.responses[key], b)
	c.mu.Unlock()
}

func (c *cellRun) recorded() map[string][][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string][][]byte, len(c.responses))
	for k, v := range c.responses {
		out[k] = append([][]byte(nil), v...)
	}
	return out
}

// newNode builds a node over the cell's schema and prefix.
func (c *cellRun) newNode(ctx context.Context, o nodeOpts) (*node, error) {
	c.nodeN++
	if c.variant != nil {
		if o.maxConns == 0 {
			o.maxConns = c.variant.maxConns
		}
		if len(c.variant.params) > 0 && o.params == nil {
			o.params = c.variant.params
		}
	}
	n := &node{id: o.id, logger: c.logger}
	suffix := fmt.Sprintf("%d-%s", c.env.cellN, o.id)
	if c.child {
		suffix = "child-" + o.id
	}
	n.cfg = c.env.redisConfig(o.id)
	client, err := redisx.NewClient(ctx, n.cfg)
	if err != nil {
		return nil, fmt.Errorf("redis client: %w", err)
	}
	n.client = client
	if o.noStore {
		c.nodes = append(c.nodes, n)
		return n, nil
	}
	maxConns := o.maxConns
	if maxConns <= 0 {
		maxConns = maxConnsDefault
	}
	url := pgURL
	if len(o.params) > 0 {
		sep := "?"
		if strings.Contains(url, "?") {
			sep = "&"
		}
		keys := make([]string, 0, len(o.params))
		for k := range o.params {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			url += sep + k + "=" + o.params[k]
			sep = "&"
		}
	}
	n.pool, err = pg.NewPool(ctx, url, pg.PoolConfig{SearchPath: c.env.schema, MaxConns: maxConns, ApplicationName: c.env.appName(suffix)})
	if err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("pool: %w", err)
	}
	// The relay hijacks a connection for LISTEN and the janitor holds a
	// session lock; they get their own small pool so a pool of size one
	// still serves requests and consumers.
	n.aux, err = pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: c.env.schema, MaxConns: 3, ApplicationName: c.env.appName(suffix + "-aux")})
	if err != nil {
		n.pool.Close()
		_ = client.Close()
		return nil, fmt.Errorf("aux pool: %w", err)
	}
	n.store = pg.NewStore(n.pool, pg.StoreConfig{Partitions: partitions, DefaultLockTimeout: lockTimeout})
	mopts := []mediator.Option{mediator.WithNodeID(o.id), mediator.WithLogger(c.logger)}
	if o.remote != nil {
		mopts = append(mopts, mediator.WithRemote(o.remote))
	}
	m := mediator.New(mopts...)
	if o.workload {
		if err := behavior.UseStandard(m, behavior.Config{
			Logger: c.logger, Store: n.store, DefaultTimeout: behaviorTimeout,
			UnitOfWork: pg.UnitOfWorkConfig{DefaultLockTimeout: lockTimeout, RollbackTimeout: 2 * time.Second, Logger: c.logger},
		}); err != nil {
			return nil, err
		}
		deps := o.deps
		deps.NodeID = o.id
		if deps.Groups == nil {
			deps.Groups = []string{}
		}
		if err := workload.Register(m, deps); err != nil {
			return nil, err
		}
	}
	if o.register != nil {
		if err := o.register(m); err != nil {
			return nil, err
		}
	}
	if err := m.Build(); err != nil {
		return nil, fmt.Errorf("build: %w", err)
	}
	n.m = m
	c.nodes = append(c.nodes, n)
	return n, nil
}

func (c *cellRun) startAll(ctx context.Context) {
	for _, n := range c.nodes {
		n.start(ctx)
	}
}

// stopAll stops every node together (the nodes share this process, so one
// SIGTERM reaches all of them) and reports components that misbehaved. It
// then terminates the nodes' backends: a restart kills a process's
// connections, including one an injected rollback fault left open with its
// locks.
func (c *cellRun) stopAll() []error {
	var errs []error
	for _, n := range c.nodes {
		n.stopComponents()
	}
	deadline := time.NewTimer(stopWait)
	defer deadline.Stop()
	for _, n := range c.nodes {
		errs = append(errs, n.awaitComponents(deadline.C)...)
	}
	c.nodes = nil
	tctx, cancel := context.WithTimeout(context.WithoutCancel(c.ctx), 10*time.Second)
	defer cancel()
	if r, err := root(tctx); err == nil {
		if err := terminateBackends(tctx, r, c.env.appName(c.nodePattern())); err != nil {
			errs = append(errs, fmt.Errorf("terminate backends: %w", err))
		}
	}
	return errs
}

// nodePattern is the LIKE pattern of the application names of this cell's
// nodes.
func (c *cellRun) nodePattern() string {
	if c.child {
		return "child-%"
	}
	return fmt.Sprintf("%d-%%", c.env.cellN)
}

// buildAndStart runs the scenario's build and starts the nodes.
func (c *cellRun) buildAndStart(ctx context.Context) error {
	if err := c.sc.build(ctx, c); err != nil {
		c.stopAll()
		return err
	}
	c.startAll(ctx)
	return nil
}

// schedule builds the testkit schedule of the cell's run.
func (c *cellRun) schedule() (testkit.Schedule, bool) {
	r := c.run
	s := testkit.Schedule{Point: r.Point, Hit: r.Hit}
	switch r.Kind {
	case "":
		return s, false
	case string(testkit.FaultError), string(testkit.FaultPermanent), string(testkit.FaultTimeout), string(testkit.FaultAmbiguous):
		s.Kind = testkit.FaultKind(r.Kind)
	case string(testkit.FaultDelay):
		s.Kind = testkit.FaultDelay
		s.Delay = c.sc.faultDelay()
		if c.variant != nil && c.variant.delay > 0 {
			s.Delay = c.variant.delay
		}
	case string(testkit.FaultCancel):
		s.Kind = testkit.FaultCancel
		s.Cancel = c.cancelRequest
	case string(testkit.FaultCrash):
		s.Kind = testkit.FaultCrash
	case kindCrashBefore:
		s.Kind = testkit.FaultCancel
		s.Cancel = func() {
			fmt.Fprintf(os.Stderr, "faultsweep: injected crash before %s\n", r.Point)
			os.Exit(137)
		}
	case kindShutdown:
		s.Kind = testkit.FaultDelay
		s.Delay = 200 * time.Millisecond
	default:
		c.t.Fatalf("unknown kind %q", r.Kind)
	}
	return s, true
}

// watchShutdown cancels the runtime when the armed point is reached, for
// the shutdown kind. It polls the hit counter while the step is delayed.
func (c *cellRun) watchShutdown(ctx context.Context) {
	if c.run.Kind != kindShutdown {
		return
	}
	go func() {
		for ctx.Err() == nil {
			if testkit.Hits()[c.run.Point] >= c.run.Hit {
				c.mu.Lock()
				f := c.runtimeCancel
				c.mu.Unlock()
				if f != nil {
					f()
				}
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
}

// ------------------------------------------------------------ state helpers

// waitFor polls cond until it holds, ctx ends, or timeout passes. It
// returns errQuiescence on timeout so callers can distinguish it.
var errQuiescence = errors.New("did not quiesce in time")

func waitFor(ctx context.Context, timeout time.Duration, cond func(ctx context.Context) (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := cond(ctx)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return errQuiescence
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollEvery):
		}
	}
}

func unpublished(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx, `SELECT count(*) FROM mediator_outbox WHERE published_at IS NULL`).Scan(&n)
	return n, err
}

// drained reports that every outbox row is published and every group has
// consumed the whole partition stream with nothing pending: the quiescence
// of a relay-and-consumers node. A group that has not read anything yet is
// not drained, even though its pending count is zero.
func drained(pool *pgxpool.Pool, client *redis.Client, cfg redisx.Config, groups []string) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		n, err := unpublished(ctx, pool)
		if err != nil {
			return false, nil // the database may be restarting; keep polling
		}
		if n > 0 {
			return false, nil
		}
		if len(groups) == 0 {
			return true, nil
		}
		return groupsCaughtUp(ctx, client, cfg, groups)
	}
}

// groupsCaughtUp reports whether every group exists on the partition
// stream, has delivered every entry (lag zero, or last-delivered at the
// tail when Redis cannot compute the lag), and has no pending entry. An
// empty or missing stream has nothing to consume.
func groupsCaughtUp(ctx context.Context, client *redis.Client, cfg redisx.Config, groups []string) (bool, error) {
	stream := cfg.Keys().Stream(workload.TopicBumped, 0)
	tail, err := client.XRevRangeN(ctx, stream, "+", "-", 1).Result()
	if err != nil || len(tail) == 0 {
		return true, nil
	}
	infos, err := client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		return false, nil
	}
	for _, g := range groups {
		var info *redis.XInfoGroup
		for i := range infos {
			if infos[i].Name == g {
				info = &infos[i]
			}
		}
		if info == nil || info.Pending != 0 || info.Lag > 0 {
			return false, nil
		}
		if info.Lag < 0 && compareStreamIDs(info.LastDeliveredID, tail[0].ID) < 0 {
			return false, nil
		}
	}
	return true, nil
}

// leasesSettled reports that every lease of the groups is held by a running
// node: a lease a stopped node could not release (an injected fault at the
// release) has expired and, if needed, been taken over.
func leasesSettled(client *redis.Client, cfg redisx.Config, groups, running []string) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		for _, g := range groups {
			leases, err := redisx.LeaseList(ctx, client, cfg, g)
			if err != nil {
				return false, nil
			}
			for _, l := range leases {
				if !slices.Contains(running, l.Node) {
					return false, nil
				}
			}
		}
		return true, nil
	}
}

// drainedAndSettled is drained followed by leasesSettled, the recovery
// condition of every scenario with consumers.
func (c *cellRun) drainedAndSettled(ctx context.Context, n *node, groups []string) error {
	if err := waitFor(ctx, recoverWait, drained(n.pool, n.client, n.cfg, groups)); err != nil {
		return err
	}
	if err := waitFor(ctx, leaseTTL+2*time.Second, leasesSettled(n.client, n.cfg, groups, c.runningNodeIDs())); err != nil {
		return fmt.Errorf("stale leases did not expire: %w", err)
	}
	return nil
}

// rpcPending counts unacknowledged remote requests of the names.
func rpcPending(ctx context.Context, client *redis.Client, cfg redisx.Config, names []string) (int64, error) {
	var n int64
	for _, name := range names {
		p, err := client.XPending(ctx, cfg.Keys().RPC(name), redisx.RPCGroup).Result()
		if err != nil {
			if strings.Contains(err.Error(), "NOGROUP") || strings.Contains(err.Error(), "no such key") {
				continue
			}
			return 0, err
		}
		n += p.Count
	}
	return n, nil
}

// ------------------------------------------------------------------- cells

// runSweep is the sweep loop of one scenario: the clean cell records the
// hits, plan.Enumerate produces the cells, each cell is a subtest.
func runSweep(t *testing.T, sc *scenario) {
	if !opt.scenarioSelected(sc.name) {
		t.Skipf("SWEEP_SCENARIOS excludes %s", sc.name)
	}
	env := newScenarioEnv(t, sc.name)
	start := time.Now()
	hits, ok := executeCell(t, env, sc, plan.Run{}, nil)
	if !ok {
		t.Fatalf("%s: the clean run failed; the sweep is not meaningful", sc.name)
	}
	patterns := sc.patterns
	if len(opt.points) > 0 {
		patterns = intersectPatterns(hits, sc.patterns, opt.points)
	}
	var kinds []string
	for _, k := range sc.enumeratedKinds() {
		if opt.kindSelected(k) {
			kinds = append(kinds, k)
		}
	}
	runs := plan.Enumerate(hits, patterns, kinds, opt.maxHits)
	t.Logf("%s: clean run %s, %d points hit, %d cells", sc.name, time.Since(start).Round(time.Millisecond), len(hits), len(runs))
	cells := 0
	for _, r := range runs {
		r := r
		t.Run(r.String(), func(t *testing.T) {
			switch {
			case r.Kind == string(testkit.FaultAmbiguous) && !isAfterPoint(t, r.Point):
				t.Skip("ambiguous acts only where testkit.FaultAfter is called")
			case r.Kind == string(testkit.FaultCrash) && !isAfterPoint(t, r.Point):
				t.Skip("crash (after) acts only where testkit.FaultAfter is called; see crash-before")
			case r.Kind == string(testkit.FaultCrash) || r.Kind == kindCrashBefore:
				cells++
				executeCrashCell(t, env, sc, r, false)
				if opt.pgRestart && r.Hit == 1 && (r.Kind == string(testkit.FaultCrash) || r.Point == "pg.tx.commit") {
					t.Run("pgrestart", func(t *testing.T) { executeCrashCell(t, env, sc, r, true) })
				}
			default:
				cells++
				executeCell(t, env, sc, r, nil)
			}
		})
	}
	sweepsRun.Store(sc.name, true)
	t.Logf("%s: %d cells in %s", sc.name, cells, time.Since(start).Round(time.Second))
	saveCoverage(t)
}

// intersectPatterns keeps the recorded points matching both the scenario's
// patterns and SWEEP_POINTS, as exact names.
func intersectPatterns(hits map[string]int, scenario, selected []string) []string {
	var out []string
	for p := range hits {
		if plan.MatchAny(scenario, p) && plan.MatchAny(selected, p) {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	if out == nil {
		out = []string{"none.selected"}
	}
	return out
}

// executeCell runs one in-process cell (or the clean run when run is zero)
// and returns the hits recorded through the run phase and the first stop.
func executeCell(t *testing.T, env *scenarioEnv, sc *scenario, run plan.Run, v *variant) (map[string]int, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), cellTimeout)
	defer cancel()
	if err := env.reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	c := newCellRun(t, env, sc, run, v)
	c.ctx = ctx
	failed := func() bool { return t.Failed() }
	if run.Kind == "" && sc.preflight != nil {
		if err := sc.preflight(ctx, c); err != nil {
			t.Errorf("preflight: %v", err)
		}
	}
	testkit.Disarm()
	if s, armed := c.schedule(); armed {
		testkit.Arm(s)
	}
	c.watchShutdown(ctx)
	c.phase = phaseRun
	if v != nil && v.beforeRun != nil {
		if err := v.beforeRun(ctx, c); err != nil {
			t.Fatalf("variant %s: %v", v.name, err)
		}
	}
	if err := c.buildAndStart(ctx); err != nil {
		testkit.Disarm()
		t.Errorf("build: %v", err)
		return nil, false
	}
	if err := sc.run(ctx, c); err != nil {
		c.note("run phase: %v", err)
	}
	stopErrs := c.stopAll()
	hits := testkit.Hits()
	testkit.Disarm()
	for _, err := range stopErrs {
		if run.Kind == "" && v == nil {
			t.Errorf("clean run: %v", err)
		} else {
			c.note("faulty phase: %v", err)
		}
	}
	if v != nil && v.afterRun != nil {
		if err := v.afterRun(ctx, c); err != nil {
			t.Fatalf("variant %s: %v", v.name, err)
		}
	}
	// Recovery on a fresh build.
	c.phase = phaseRecover
	if err := c.buildAndStart(ctx); err != nil {
		t.Errorf("recovery build: %v", err)
		return hits, false
	}
	if err := sc.recover(ctx, c); err != nil {
		t.Errorf("recovery: %v", err)
	}
	for _, err := range c.stopAll() {
		t.Errorf("recovery stop: %v", err)
	}
	c.verifyAll(ctx)
	c.report()
	if run.Kind != "" && run.Kind != kindShutdown {
		if hits[run.Point] >= run.Hit {
			coverage.Mark(run.Point, coverageKind(run.Kind))
		} else {
			t.Logf("point %s was reached %d times, hit %d never armed; not counted as coverage", run.Point, hits[run.Point], run.Hit)
		}
	}
	return hits, !failed()
}

// verifyAll runs the invariants and the scenario's end-state checks.
func (c *cellRun) verifyAll(ctx context.Context) {
	c.t.Helper()
	v, err := c.env.verifier(ctx, c.t)
	if err != nil {
		c.t.Errorf("verifier: %v", err)
		return
	}
	defer v.close()
	if !c.sc.noInvariants {
		var exp map[string]invariants.Expect
		if c.sc.expect != nil {
			exp = c.sc.expect(c)
		}
		rep, err := invariants.All(ctx, v.inv(c.sc.groups), invariants.Options{
			Expect: exp, Responses: c.recorded(), SkipLiveness: true, SkipDLQ: c.sc.skipDLQ,
		})
		if err != nil {
			c.t.Errorf("invariants: %v", err)
		}
		for _, viol := range rep.Violations {
			c.t.Errorf("invariant violated: %s", viol)
		}
	}
	if c.sc.verify != nil {
		c.sc.verify(ctx, c, v)
	}
}

// report logs the cell's notes when verbose or when the cell failed.
func (c *cellRun) report() {
	c.mu.Lock()
	notes := c.notes
	c.mu.Unlock()
	if len(notes) == 0 || (!opt.verbose && !c.t.Failed()) {
		return
	}
	c.t.Logf("cell notes:\n  %s", strings.Join(notes, "\n  "))
}

// saveCoverage merges the process coverage into SWEEP_COVERAGE_FILE.
func saveCoverage(t *testing.T) {
	if opt.coverageFile == "" {
		return
	}
	if err := coverage.Save(opt.coverageFile); err != nil {
		t.Errorf("save coverage: %v", err)
	}
}

// ---------------------------------------------------------- shared checks

// checkDLQ asserts the dead letters of a group: none, or every entry for
// the one expected event of key. Several entries are permitted: an
// ambiguous dead-letter XADD is written twice, and a relay duplicate of a
// poison event is dead-lettered once per stream entry, because a handler
// that always fails never reaches the inbox that would deduplicate it.
func checkDLQ(ctx context.Context, v *verifier, group, key string, wantSome bool) {
	v.t.Helper()
	entries, err := redisx.DLQList(ctx, v.client, v.cfg, group)
	if err != nil {
		v.t.Errorf("dlq list %s: %v", group, err)
		return
	}
	if !wantSome {
		if len(entries) > 0 {
			v.t.Errorf("group %s has %d dead letters, want none", group, len(entries))
		}
		return
	}
	if len(entries) == 0 {
		v.t.Errorf("group %s has no dead letter for key %s", group, key)
		return
	}
	for _, e := range entries {
		if e.DecodeErr != nil || e.Envelope.StreamKey != key || e.Envelope.ID != entries[0].Envelope.ID {
			v.t.Errorf("unexpected dead letter in %s: key=%s event=%s stream_id=%s err=%v", group, e.Envelope.StreamKey, e.Envelope.ID, e.StreamID, e.DecodeErr)
		}
	}
}

// checkLeasesReleased asserts no lease key remains under the prefix.
func checkLeasesReleased(ctx context.Context, v *verifier, groups []string) {
	v.t.Helper()
	for _, g := range groups {
		leases, err := redisx.LeaseList(ctx, v.client, v.cfg, g)
		if err != nil {
			v.t.Errorf("lease list %s: %v", g, err)
			continue
		}
		for _, l := range leases {
			v.t.Errorf("lease still held after shutdown: group=%s partition=%d node=%s epoch=%d ttl=%s", l.Group, l.Partition, l.Node, l.Epoch, l.TTL)
		}
	}
}

// checkAckedImpliesInbox asserts G16's ordering: every entry a group
// acknowledged (delivered and no longer pending) has an inbox row, so the
// acknowledgement followed the commit. A dead letter is the one exception:
// it is acknowledged after its copy was added to the group's DLQ and never
// reaches the inbox (spec 7.3), so the DLQ entry is its durable record.
func checkAckedImpliesInbox(ctx context.Context, v *verifier, groups []string) {
	v.t.Helper()
	stream := v.cfg.Keys().Stream(workload.TopicBumped, 0)
	msgs, err := v.client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return
		}
		v.t.Errorf("xrange %s: %v", stream, err)
		return
	}
	infos, err := v.client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return // the stream was never created: nothing was delivered
		}
		v.t.Errorf("xinfo groups %s: %v", stream, err)
		return
	}
	for _, g := range groups {
		var last string
		found := false
		for _, gi := range infos {
			if gi.Name == g {
				last, found = gi.LastDeliveredID, true
			}
		}
		if !found {
			continue
		}
		pending := map[string]bool{}
		pend, err := v.client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: stream, Group: g, Start: "-", End: "+", Count: 10000}).Result()
		if err != nil {
			v.t.Errorf("xpending %s %s: %v", stream, g, err)
			continue
		}
		for _, p := range pend {
			pending[p.ID] = true
		}
		deadLettered := map[string]bool{}
		if dlq, err := redisx.DLQList(ctx, v.client, v.cfg, g); err != nil {
			v.t.Errorf("dlq list %s: %v", g, err)
		} else {
			for _, e := range dlq {
				deadLettered[e.StreamID] = true
			}
		}
		for _, m := range msgs {
			if compareStreamIDs(m.ID, last) > 0 || pending[m.ID] || deadLettered[m.ID] {
				continue
			}
			env, _, _, derr := redisx.DecodeEntry(m.Values)
			if derr != nil {
				continue
			}
			n := v.count(ctx, `SELECT count(*) FROM mediator_inbox WHERE consumer_group = $1 AND event_id = $2`, g, env.ID)
			if n == 0 {
				v.t.Errorf("G16: group %s acknowledged entry %s (event %s key %s seq %d) without an inbox row", g, m.ID, env.ID, env.StreamKey, env.Seq)
			}
		}
	}
}

// compareStreamIDs orders "<ms>-<seq>" IDs numerically.
func compareStreamIDs(a, b string) int {
	parse := func(s string) (int64, int64) {
		i := strings.IndexByte(s, '-')
		if i < 0 {
			n, _ := strconv.ParseInt(s, 10, 64)
			return n, 0
		}
		ms, _ := strconv.ParseInt(s[:i], 10, 64)
		seq, _ := strconv.ParseInt(s[i+1:], 10, 64)
		return ms, seq
	}
	am, as := parse(a)
	bm, bs := parse(b)
	switch {
	case am != bm:
		if am < bm {
			return -1
		}
		return 1
	case as != bs:
		if as < bs {
			return -1
		}
		return 1
	}
	return 0
}

// checkPartialConsumerState holds after a crash before recovery: no event
// applied twice, nothing applied that is not committed, and every
// acknowledged entry committed first.
func checkPartialConsumerState(ctx context.Context, v *verifier, groups []string) {
	v.t.Helper()
	v.expectCount(ctx, 0, "events applied more than once",
		`SELECT count(*) FROM (SELECT grp, event_id FROM wl_applied GROUP BY grp, event_id HAVING count(*) > 1) d`)
	v.expectCount(ctx, 0, "applied events absent from the outbox",
		`SELECT count(*) FROM wl_applied a WHERE NOT EXISTS (SELECT 1 FROM mediator_outbox o WHERE o.event_id = a.event_id)`)
	v.expectCount(ctx, 0, "inbox rows for events absent from the outbox",
		`SELECT count(*) FROM mediator_inbox i WHERE NOT EXISTS (SELECT 1 FROM mediator_outbox o WHERE o.event_id = i.event_id)`)
	checkAckedImpliesInbox(ctx, v, groups)
}

// checkProjection asserts the read-model projection of a key.
func checkProjection(ctx context.Context, v *verifier, key string, n int64) {
	v.t.Helper()
	var count, last int64
	err := v.pool.QueryRow(ctx, `SELECT count, last_seq FROM wl_projection WHERE grp = $1 AND key = $2`, workload.GroupReadModel, key).Scan(&count, &last)
	if err != nil {
		v.t.Errorf("projection of %s: %v", key, err)
		return
	}
	if count != n || last != n {
		v.t.Errorf("projection of %s: count=%d last_seq=%d, want %d", key, count, last, n)
	}
}

// bumpCommands returns n keyed Bump commands of key with IDs prefix-1..n.
func bumpCommands(key, prefix string, n int) []workload.Bump {
	out := make([]workload.Bump, n)
	for i := range out {
		out[i] = workload.Bump{Key: key, CmdID: fmt.Sprintf("%s-%d", prefix, i+1)}
	}
	return out
}

// expectBumps adds I1 expectations for keyed Bump commands.
func expectBumps(exp map[string]invariants.Expect, cmds ...workload.Bump) {
	for _, b := range cmds {
		if e, ok := invariants.ExpectFor(b); ok {
			exp[b.CmdID] = e
		}
	}
}
