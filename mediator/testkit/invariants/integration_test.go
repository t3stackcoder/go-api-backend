//go:build integration

package invariants_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

var (
	pgURL     string
	redisAddr string
	rootOnce  sync.Once
	rootPool  *pgxpool.Pool
)

// TestMain starts postgres:18 and redis:8 once for the package (spec 11.5).
func TestMain(m *testing.M) {
	ctx := context.Background()
	pc, err := postgres.Run(ctx, "postgres:18",
		postgres.WithDatabase("mediator"), postgres.WithUsername("mediator"), postgres.WithPassword("mediator"),
		postgres.BasicWaitStrategies())
	if err != nil {
		log.Fatalf("start postgres: %v", err)
	}
	pgURL, err = pc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("connection string: %v", err)
	}
	rc, err := tcredis.Run(ctx, "redis:8")
	if err != nil {
		log.Fatalf("start redis: %v", err)
	}
	conn, err := rc.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("redis connection string: %v", err)
	}
	redisAddr = strings.TrimPrefix(conn, "redis://")
	code := m.Run()
	if rootPool != nil {
		rootPool.Close()
	}
	_ = testcontainers.TerminateContainer(rc)
	_ = testcontainers.TerminateContainer(pc)
	os.Exit(code)
}

func root(t *testing.T) *pgxpool.Pool {
	t.Helper()
	rootOnce.Do(func() {
		var err error
		rootPool, err = pg.NewPool(context.Background(), pgURL, pg.PoolConfig{MaxConns: 4})
		if err != nil {
			t.Fatalf("root pool: %v", err)
		}
	})
	return rootPool
}

func unique() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// env is a complete node: schema, mediator with the real behaviors, the
// relay, and the consumers, plus the invariants.Env that reads it.
type env struct {
	pool   *pgxpool.Pool
	store  *pg.PgStore
	client *redis.Client
	cfg    redisx.Config
	m      *mediator.Mediator
	inv    invariants.Env
}

// newEnv starts a node. The relay's advisory locks are database-wide, so
// tests using newEnv must not run in parallel; Cleanup stops the relay and
// consumers before the next test starts.
func newEnv(t *testing.T, deps workload.Deps) *env {
	t.Helper()
	ctx := context.Background()
	schema := "inv_" + unique()
	if _, err := root(t).Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	pool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: schema, MaxConns: 10})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := workload.Migrate(ctx, pool); err != nil {
		t.Fatalf("workload migrate: %v", err)
	}
	if err := workload.SeedBank(ctx, pool); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cfg := redisx.Config{
		Addr: redisAddr, Prefix: "inv" + unique(), NodeID: "n1", PartitionsPerTopic: 4,
		LeaseTTL: 2 * time.Second, LeaseRenew: 200 * time.Millisecond, ClaimMinIdle: time.Second,
		ReadBlock: 200 * time.Millisecond, ReadBatch: 16, MaxAttempts: 3,
	}.WithDefaults()
	client, err := redisx.NewClient(ctx, cfg)
	if err != nil {
		t.Fatalf("redis: %v", err)
	}
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: cfg.PartitionsPerTopic})
	m := mediator.New(mediator.WithNodeID(cfg.NodeID))
	if err := mediator.Use(m, pg.UnitOfWork(store, pg.UnitOfWorkConfig{}), mediator.Requests(), mediator.Consumers()); err != nil {
		t.Fatal(err)
	}
	if err := mediator.Use(m, pg.Idempotency(pg.IdempotencyConfig{}), mediator.Commands()); err != nil {
		t.Fatal(err)
	}
	if err := mediator.Use(m, pg.Inbox(), mediator.Consumers()); err != nil {
		t.Fatal(err)
	}
	if deps.Groups == nil {
		deps.Groups = workload.DefaultGroups
	}
	if err := workload.Register(m, deps); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	groups := deps.Groups
	relay := pg.NewRelay(pool, redisx.NewStreams(client, cfg), pg.RelayConfig{
		Topics: []string{workload.TopicBumped}, Partitions: cfg.PartitionsPerTopic, PollInterval: 200 * time.Millisecond,
		KnownGroups: func() []string { return groups },
	})
	consumers := redisx.NewConsumers(m, client, cfg, store)
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{}, 2)
	go func() { _ = relay.Run(runCtx); done <- struct{}{} }()
	go func() { _ = consumers.Run(runCtx); done <- struct{}{} }()
	t.Cleanup(func() {
		cancel()
		for i := 0; i < 2; i++ {
			select {
			case <-done:
			case <-time.After(15 * time.Second):
				t.Log("component did not stop in time")
			}
		}
		_ = client.Close()
		pool.Close()
		_, _ = root(t).Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
	})
	inv := invariants.Env{Pool: pool, Redis: client, Cfg: cfg, Groups: groups, Topics: []string{workload.TopicBumped},
		Partitions: cfg.PartitionsPerTopic, Bound: 30 * time.Second, Poll: 100 * time.Millisecond}
	return &env{pool: pool, store: store, client: client, cfg: cfg, m: m, inv: inv}
}

func (e *env) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	if _, err := e.pool.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func send[R any](t *testing.T, e *env, req mediator.Request[R]) R {
	t.Helper()
	res, err := mediator.Send(context.Background(), e.m, req)
	if err != nil {
		t.Fatalf("%T: %v", req, err)
	}
	return res
}

// expect records the expectation of a command the scenario sent.
func expect(t *testing.T, m map[string]invariants.Expect, id string, req any) {
	t.Helper()
	e, ok := invariants.ExpectFor(req)
	if !ok {
		t.Fatalf("no expectation for %T", req)
	}
	m[id] = e
}

// result pairs the outputs of a check so assertions can take them as one
// argument.
type result struct {
	vs  []invariants.Violation
	err error
}

func r(vs []invariants.Violation, err error) result { return result{vs, err} }

// assertOnly asserts that the check found violations and that every one
// carries the ID and the message fragment.
func assertOnly(t *testing.T, res result, id, fragment string) {
	t.Helper()
	if res.err != nil {
		t.Fatalf("check error: %v", res.err)
	}
	if len(res.vs) == 0 {
		t.Fatalf("expected a %s violation with %q", id, fragment)
	}
	for _, v := range res.vs {
		if v.ID != id || !strings.Contains(v.Msg, fragment) {
			t.Fatalf("unexpected violation %s (want %s %q)", v, id, fragment)
		}
	}
}

func assertNone(t *testing.T, res result) {
	t.Helper()
	if res.err != nil {
		t.Fatalf("check error: %v", res.err)
	}
	if len(res.vs) != 0 {
		t.Fatalf("unexpected violations:\n%s", invariants.Report{Violations: res.vs}.String())
	}
}

func TestIntegration_ConsistentStateAndCorruptions(t *testing.T) {
	e := newEnv(t, workload.Deps{})
	ctx := context.Background()
	exp := map[string]invariants.Expect{}
	responses := map[string][][]byte{}
	record := func(key string, res any) {
		b, err := json.Marshal(res)
		if err != nil {
			t.Fatal(err)
		}
		responses[key] = append(responses[key], b)
	}

	// The scenario: every command kind, with replays of the keyed ones.
	set1 := workload.SetValue{Key: "k1", Val: 1, CmdID: "c-set1"}
	send(t, e, set1)
	expect(t, exp, set1.CmdID, set1)
	set2 := workload.SetValue{Key: "k2", Val: 2, CmdID: "c-set2", Keyed: true, Invalidate: true}
	send(t, e, set2)
	send(t, e, set2)
	expect(t, exp, set2.CmdID, set2)
	for i := 0; i < 3; i++ {
		app := workload.Append{Key: "list", Val: int64(i), CmdID: fmt.Sprintf("c-app%d", i)}
		record(app.CmdID, send(t, e, app))
		expect(t, exp, app.CmdID, app)
	}
	record("c-app0", send(t, e, workload.Append{Key: "list", Val: 0, CmdID: "c-app0"}))
	tr := workload.Transfer{From: "a", To: "b", Amt: 10, CmdID: "c-tr1", Keyed: true}
	send(t, e, tr)
	send(t, e, tr)
	expect(t, exp, tr.CmdID, tr)
	for _, key := range []string{"e1", "e2", "e3"} {
		for i := 0; i < 4; i++ {
			b := workload.Bump{Key: key, CmdID: fmt.Sprintf("c-bump-%s-%d", key, i)}
			record(b.CmdID, send(t, e, b))
			expect(t, exp, b.CmdID, b)
		}
	}
	record("c-bump-e1-0", send(t, e, workload.Bump{Key: "e1", CmdID: "c-bump-e1-0"}))
	atom := workload.AtomicScenario{CmdID: "c-atom", Key1: "z1", Key2: "z2", Val: 9, CmdKey: "c-atom"}
	send(t, e, atom)
	expect(t, exp, atom.CmdID, atom)
	// A command that failed leaves nothing; its expectation must still hold.
	if _, err := mediator.Send(ctx, e.m, workload.Transfer{From: "a", To: "b", Amt: 10_000, CmdID: "c-fail", Keyed: true}); mediator.CodeOf(err) != mediator.CodePrecondition {
		t.Fatalf("failed transfer: %v", err)
	}
	expect(t, exp, "c-fail", workload.Transfer{CmdID: "c-fail", Keyed: true})
	exp["c-never"] = invariants.Expect{} // a command id nothing ever used

	// Reads and writes for I7, consistent by construction.
	writes := []history.Write{{Key: "k1", Value: 1, Invoke: 1, Return: 2, Definite: true}}
	reads := []history.Read{{Key: "k1", Value: 1, Invoke: 3, Return: 4}}

	// Quiescence, then every check.
	assertNone(t, r(invariants.L1(ctx, e.inv)))
	rep, err := invariants.All(ctx, e.inv, invariants.Options{Expect: exp, Responses: responses, Reads: reads, Writes: writes})
	if err != nil || !rep.OK() {
		t.Fatalf("consistent state: %v\n%s", err, rep)
	}
	// Sanity: the consumers really applied the 14 events in both groups.
	var applied int64
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM wl_applied`).Scan(&applied); err != nil || applied != 28 {
		t.Fatalf("applied = %d (%v)", applied, err)
	}

	t.Run("I1 missing marker", func(t *testing.T) {
		e.exec(t, `UPDATE wl_cmd_log SET cmd_id = 'c-atom-x' WHERE cmd_id = 'c-atom'`)
		defer e.exec(t, `UPDATE wl_cmd_log SET cmd_id = 'c-atom' WHERE cmd_id = 'c-atom-x'`)
		vs, err := invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-atom": exp["c-atom"]})
		assertOnly(t, r(vs, err), invariants.IDI1, "without its wl_cmd_log marker")
		// The zero Expect finds the same partial state.
		vs, err = invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-atom": {}})
		assertOnly(t, r(vs, err), invariants.IDI1, "without its wl_cmd_log marker")
	})
	t.Run("I1 partial commit", func(t *testing.T) {
		e.exec(t, `DELETE FROM wl_side WHERE cmd_id = 'c-atom'`)
		defer e.exec(t, `INSERT INTO wl_side (cmd_id, note) VALUES ('c-atom', 'restored')`)
		vs, err := invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-atom": exp["c-atom"]})
		assertOnly(t, r(vs, err), invariants.IDI1, "committed partially")
		vs, err = invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-atom": {}})
		assertOnly(t, r(vs, err), invariants.IDI1, "committed partially")
	})
	t.Run("I1 unexpected row", func(t *testing.T) {
		e.exec(t, `INSERT INTO wl_side (cmd_id, note) VALUES ('c-set1', 'bogus')`)
		defer e.exec(t, `DELETE FROM wl_side WHERE cmd_id = 'c-set1'`)
		vs, err := invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-set1": exp["c-set1"]})
		assertOnly(t, r(vs, err), invariants.IDI1, "must not write")
		// Without an explicit expectation the extra row is tolerated.
		assertNone(t, r(invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-set1": {}})))
	})
	t.Run("I1 causation and name", func(t *testing.T) {
		e.exec(t, `UPDATE wl_cmd_log SET request_id = $1 WHERE cmd_id = 'c-atom'`, uuid.Nil.String())
		vs, err := invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-atom": exp["c-atom"]})
		assertOnly(t, r(vs, err), invariants.IDI1, "causation id disagree")
		e.exec(t, `UPDATE wl_cmd_log SET request_id = (SELECT headers->>'cause' FROM mediator_outbox WHERE headers->'h'->>'cmd' = 'c-atom' LIMIT 1) WHERE cmd_id = 'c-atom'`)
		assertNone(t, r(invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-atom": exp["c-atom"]})))
		vs, err = invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-atom": {Name: workload.NameBump, Events: 1}})
		if err != nil || len(vs) < 2 {
			t.Fatalf("wrong name and count: %v %v", err, vs)
		}
		// An overwritten register row is unknown, not a violation.
		send(t, e, workload.SetValue{Key: "z1", Val: 1, CmdID: "c-over"})
		assertNone(t, r(invariants.I1(ctx, e.inv, map[string]invariants.Expect{"c-atom": exp["c-atom"]})))
	})
	t.Run("I3 inbox row deleted", func(t *testing.T) {
		var id uuid.UUID
		if err := e.pool.QueryRow(ctx, `SELECT event_id FROM mediator_inbox WHERE consumer_group = $1 LIMIT 1`, workload.GroupReadModel).Scan(&id); err != nil {
			t.Fatal(err)
		}
		e.exec(t, `DELETE FROM mediator_inbox WHERE consumer_group = $1 AND event_id = $2`, workload.GroupReadModel, id)
		defer e.exec(t, `INSERT INTO mediator_inbox (consumer_group, event_id) VALUES ($1, $2)`, workload.GroupReadModel, id)
		assertOnly(t, r(invariants.I3(ctx, e.inv)), invariants.IDI3, "no inbox row")
	})
	t.Run("I3 projection and audit", func(t *testing.T) {
		e.exec(t, `UPDATE wl_projection SET count = count + 1 WHERE key = 'e1'`)
		assertOnly(t, r(invariants.I3(ctx, e.inv)), invariants.IDI3, "differs from the derived state")
		e.exec(t, `UPDATE wl_projection SET count = count - 1 WHERE key = 'e1'`)
		e.exec(t, `INSERT INTO wl_projection (grp, key, count, last_seq) VALUES ($1, 'phantom', 1, 1)`, workload.GroupReadModel)
		assertOnly(t, r(invariants.I3(ctx, e.inv)), invariants.IDI3, "never bumped")
		e.exec(t, `DELETE FROM wl_projection WHERE key = 'phantom'`)
		e.exec(t, `INSERT INTO wl_bumps (key, n) VALUES ('orphan', 1)`)
		vs, err := invariants.I3(ctx, e.inv)
		if err != nil || len(vs) != 2 {
			t.Fatalf("orphan bump: %v %v", err, vs)
		}
		e.exec(t, `DELETE FROM wl_bumps WHERE key = 'orphan'`)
		var id uuid.UUID
		var key string
		var seq int64
		if err := e.pool.QueryRow(ctx, `SELECT event_id, key, seq FROM wl_audit LIMIT 1`).Scan(&id, &key, &seq); err != nil {
			t.Fatal(err)
		}
		e.exec(t, `DELETE FROM wl_audit WHERE event_id = $1`, id)
		assertOnly(t, r(invariants.I3(ctx, e.inv)), invariants.IDI3, "without an audit row")
		e.exec(t, `INSERT INTO wl_audit (grp, event_id, key, seq) VALUES ($1, $2, $3, $4)`, workload.GroupAudit, id, key, seq)
		e.exec(t, `INSERT INTO wl_audit (grp, event_id, key, seq) VALUES ($1, $2, 'x', 1)`, workload.GroupAudit, uuid.New())
		assertOnly(t, r(invariants.I3(ctx, e.inv)), invariants.IDI3, "not in the outbox")
		e.exec(t, `DELETE FROM wl_audit WHERE key = 'x'`)
		// Apply log: a duplicate, an unknown event, and a missing apply.
		e.exec(t, `INSERT INTO wl_applied (grp, event_id, key, seq, fencing, node) SELECT grp, event_id, key, seq, fencing, node FROM wl_applied WHERE grp = $1 AND key = 'e2' AND seq = 1`, workload.GroupAudit)
		assertOnly(t, r(invariants.I3(ctx, e.inv)), invariants.IDI3, "more than once")
		e.exec(t, `DELETE FROM wl_applied WHERE applied = (SELECT max(applied) FROM wl_applied)`)
		e.exec(t, `INSERT INTO wl_applied (grp, event_id, key, seq, fencing, node) VALUES ($1, $2, 'x', 1, 1, 'n1')`, workload.GroupAudit, uuid.New())
		assertOnly(t, r(invariants.I3(ctx, e.inv)), invariants.IDI3, "not in the outbox")
		e.exec(t, `DELETE FROM wl_applied WHERE key = 'x'`)
		e.exec(t, `UPDATE wl_applied SET grp = 'parked' WHERE grp = $1 AND key = 'e3' AND seq = 4`, workload.GroupReadModel)
		assertOnly(t, r(invariants.I3(ctx, e.inv)), invariants.IDI3, "never applied")
		e.exec(t, `UPDATE wl_applied SET grp = $1 WHERE grp = 'parked'`, workload.GroupReadModel)
		assertNone(t, r(invariants.I3(ctx, e.inv)))
	})
	t.Run("I4", func(t *testing.T) {
		e.exec(t, `UPDATE wl_executions SET n = 2 WHERE key = 'c-app1'`)
		assertOnly(t, r(invariants.I4(ctx, e.inv)), invariants.IDI4, "more than once")
		e.exec(t, `UPDATE wl_executions SET n = 1 WHERE key = 'c-app1'`)
		e.exec(t, `DELETE FROM wl_executions WHERE key = 'c-app1'`)
		assertOnly(t, r(invariants.I4(ctx, e.inv)), invariants.IDI4, "without an execution")
		e.exec(t, `INSERT INTO wl_executions (scope, key, n) VALUES ($1, 'c-app1', 1)`, workload.NameAppend)
		assertNone(t, r(invariants.I4(ctx, e.inv)))
	})
	t.Run("I5 reservation and pending", func(t *testing.T) {
		e.exec(t, `INSERT INTO mediator_idempotency (scope, key, request_hash, expires_at) VALUES ($1, 'ghost', '\x00', now() + interval '1 hour')`, workload.NameAppend)
		assertOnly(t, r(invariants.I5(ctx, e.inv)), invariants.IDI5, "NULL response")
		e.exec(t, `DELETE FROM mediator_idempotency WHERE key = 'ghost'`)
		// A ghost group that reads without acknowledging has a stale pending entry.
		stream := e.cfg.Keys().Stream(workload.TopicBumped, mediator.Partition("e1", e.cfg.PartitionsPerTopic))
		if err := e.client.XGroupCreate(ctx, stream, "ghost", "0").Err(); err != nil {
			t.Fatal(err)
		}
		if _, err := e.client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "ghost", Consumer: "c", Streams: []string{stream, ">"}, Count: 1}).Result(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		ghost := e.inv
		ghost.Groups = []string{"ghost"}
		ghost.Bound = time.Millisecond
		assertOnly(t, r(invariants.I5(ctx, ghost)), invariants.IDI5, "idle for longer")
		// Within the bound the same entry is fine, and L1 reports it as pending.
		ghost.Bound = time.Minute
		assertNone(t, r(invariants.I5(ctx, ghost)))
		ghost.Bound = 300 * time.Millisecond
		assertOnly(t, r(invariants.L1(ctx, ghost)), invariants.IDL1, "did not quiesce")
		if err := e.client.XGroupDestroy(ctx, stream, "ghost").Err(); err != nil {
			t.Fatal(err)
		}
		assertNone(t, r(invariants.I5(ctx, e.inv)))
		assertNone(t, r(invariants.I5(ctx, ghost))) // a missing group is empty
	})
	t.Run("I6 shifted seq", func(t *testing.T) {
		swap := `UPDATE wl_applied SET seq = CASE seq WHEN 1 THEN 2 ELSE 1 END WHERE grp = $1 AND key = 'e1' AND seq IN (1, 2)`
		e.exec(t, swap, workload.GroupReadModel)
		assertOnly(t, r(invariants.I6(ctx, e.inv)), invariants.IDI6, "not exactly 1..n")
		e.exec(t, swap, workload.GroupReadModel)
		assertNone(t, r(invariants.I6(ctx, e.inv)))
	})
	t.Run("Fencing", func(t *testing.T) {
		var applied, token int64
		var node string
		// e1's last apply follows e1's earlier applies in the same partition,
		// so a different node with a token that is not greater is caught.
		if err := e.pool.QueryRow(ctx, `SELECT applied, fencing, node FROM wl_applied WHERE grp = $1 AND key = 'e1' ORDER BY applied DESC LIMIT 1`, workload.GroupReadModel).Scan(&applied, &token, &node); err != nil {
			t.Fatal(err)
		}
		if token < 1 {
			t.Fatalf("consumers must record a real fencing token, got %d", token)
		}
		e.exec(t, `UPDATE wl_applied SET node = 'n2', fencing = $2 WHERE applied = $1`, applied, token)
		assertOnly(t, r(invariants.Fencing(ctx, e.inv)), invariants.IDFencing, "new owner")
		e.exec(t, `UPDATE wl_applied SET node = $2, fencing = $3 WHERE applied = $1`, applied, node, token)
		assertNone(t, r(invariants.Fencing(ctx, e.inv)))
	})
	t.Run("I2 and L1 with an unpublished row", func(t *testing.T) {
		for i := 0; i < 25; i++ {
			e.exec(t, `INSERT INTO mediator_outbox (event_id, topic, stream_key, seq, partition, event_type, payload, created_at)
VALUES ($1, 'ghost.topic', 'g', $2, 0, 'wl.Bumped', '{}', now() - interval '2 hours')`, uuid.New(), i+1)
		}
		vs, err := invariants.I2(ctx, e.inv)
		assertOnly(t, r(vs, err), invariants.IDI2, "older than the bound")
		if vs[0].Details["count"] != 25 || len(vs[0].Details["ids"].([]int64)) != 20 {
			t.Fatalf("details must be capped: %v", vs[0])
		}
		short := e.inv
		short.Bound = 300 * time.Millisecond
		assertOnly(t, r(invariants.L1(ctx, short)), invariants.IDL1, "did not quiesce")
		tctx, tcancel := context.WithTimeout(ctx, 250*time.Millisecond)
		defer tcancel()
		if _, err := invariants.L1(tctx, e.inv); err == nil {
			t.Fatal("L1 must return the context error when the context ends first")
		}
		e.exec(t, `DELETE FROM mediator_outbox WHERE topic = 'ghost.topic'`)
		assertNone(t, r(invariants.I2(ctx, e.inv)))
	})
	t.Run("I2 stream entry lost", func(t *testing.T) {
		var id uuid.UUID
		var partition int
		if err := e.pool.QueryRow(ctx, `SELECT event_id, partition FROM mediator_outbox WHERE published_at IS NOT NULL ORDER BY id LIMIT 1`).Scan(&id, &partition); err != nil {
			t.Fatal(err)
		}
		stream := e.cfg.Keys().Stream(workload.TopicBumped, partition)
		msgs, err := e.client.XRange(ctx, stream, "-", "+").Result()
		if err != nil {
			t.Fatal(err)
		}
		var lost redis.XMessage
		for _, m := range msgs {
			if fmt.Sprint(m.Values[redisx.FieldID]) == id.String() {
				lost = m
			}
		}
		if lost.ID == "" {
			t.Fatalf("event %s not found in %s", id, stream)
		}
		if err := e.client.XDel(ctx, stream, lost.ID).Err(); err != nil {
			t.Fatal(err)
		}
		assertOnly(t, r(invariants.I2(ctx, e.inv)), invariants.IDI2, "missing from their partition stream")
		assertOnly(t, r(invariants.I3(ctx, e.inv)), invariants.IDI3, "absent from every stream")
		// Re-adding the entry is what the relay does after data loss; the inbox absorbs the redelivery.
		if err := e.client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: lost.Values}).Err(); err != nil {
			t.Fatal(err)
		}
		assertNone(t, r(invariants.L1(ctx, e.inv)))
		rep, err := invariants.All(ctx, e.inv, invariants.Options{SkipLiveness: true})
		if err != nil || !rep.OK() {
			t.Fatalf("after re-add: %v\n%s", err, rep)
		}
		var applied int64
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM wl_applied`).Scan(&applied); err != nil || applied != 28 {
			t.Fatalf("redelivery must not apply again: %d", applied)
		}
	})
	t.Run("large stream with foreign entries", func(t *testing.T) {
		junk := e.inv
		junk.Topics = []string{workload.TopicBumped, "junk"}
		stream := e.cfg.Keys().Stream("junk", 0)
		pipe := e.client.Pipeline()
		for i := 0; i < 1001; i++ {
			pipe.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{redisx.FieldID: "not-a-uuid", "n": i}})
		}
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"n": "no id"}})
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatal(err)
		}
		assertNone(t, r(invariants.I2(ctx, junk)))
		assertNone(t, r(invariants.I3(ctx, junk)))
		if err := e.client.Del(ctx, stream).Err(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("without Redis", func(t *testing.T) {
		noRedis := e.inv
		noRedis.Redis = nil
		rep, err := invariants.All(ctx, noRedis, invariants.Options{SkipLiveness: true, Expect: exp})
		if err != nil || !rep.OK() {
			t.Fatalf("without redis: %v\n%s", err, rep)
		}
		assertNone(t, r(invariants.L1(ctx, noRedis)))
	})
	t.Run("check errors are returned", func(t *testing.T) {
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := invariants.All(cctx, e.inv, invariants.Options{Expect: exp, Reads: reads}); err == nil {
			t.Fatal("canceled context must surface as an error")
		}
		for _, f := range []func(context.Context, invariants.Env) ([]invariants.Violation, error){
			invariants.I2, invariants.I3, invariants.I4, invariants.I5, invariants.I6, invariants.L1, invariants.Fencing, invariants.DLQEmpty,
		} {
			if _, err := f(cctx, e.inv); err == nil {
				t.Fatal("canceled context must surface as an error")
			}
		}
		if _, err := invariants.I1(cctx, e.inv, exp); err == nil {
			t.Fatal("I1 with a canceled context must fail")
		}
	})
}

func TestIntegration_DeadLetters(t *testing.T) {
	e := newEnv(t, workload.Deps{Groups: workload.AllGroups, PoisonKey: "bad"})
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		send(t, e, workload.Bump{Key: "bad"})
		send(t, e, workload.Bump{Key: "good"})
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		entries, err := redisx.DLQList(ctx, e.client, e.cfg, workload.GroupPoison)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("dead letters: %d", len(entries))
		}
		time.Sleep(100 * time.Millisecond)
	}
	assertNone(t, r(invariants.L1(ctx, e.inv)))
	vs, err := invariants.DLQEmpty(ctx, e.inv)
	if err != nil || len(vs) != 2 || vs[0].ID != invariants.IDDLQ || vs[0].Details["key"] != "bad" {
		t.Fatalf("dlq: %v %v", err, vs)
	}
	// The dead-lettered key is a gap for the poison group only; every other
	// check holds because the checks account for the dead letters.
	rep, err := invariants.All(ctx, e.inv, invariants.Options{SkipLiveness: true, SkipDLQ: true})
	if err != nil || !rep.OK() {
		t.Fatalf("with dead letters: %v\n%s", err, rep)
	}
	rep, err = invariants.All(ctx, e.inv, invariants.Options{SkipLiveness: true})
	if err != nil || rep.OK() || len(rep.Violations) != 2 {
		t.Fatalf("DLQ must be reported: %v\n%s", err, rep)
	}
	var poisonApplied int64
	if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM wl_applied WHERE grp = $1`, workload.GroupPoison).Scan(&poisonApplied); err != nil || poisonApplied != 2 {
		t.Fatalf("poison applied = %d", poisonApplied)
	}
	// A group with dead letters for a key is not held to the projection and
	// order checks for that key: copy the poison group's dead letters to the
	// read-model group and remove the read-model rows of the key.
	dead, err := e.client.XRange(ctx, e.cfg.Keys().DLQ(workload.GroupPoison), "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range dead {
		if err := e.client.XAdd(ctx, &redis.XAddArgs{Stream: e.cfg.Keys().DLQ(workload.GroupReadModel), Values: m.Values}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	e.exec(t, `UPDATE wl_applied SET seq = seq + 10 WHERE grp = $1 AND key = 'bad'`, workload.GroupReadModel)
	e.exec(t, `UPDATE wl_projection SET count = 99 WHERE grp = $1 AND key = 'bad'`, workload.GroupReadModel)
	assertNone(t, r(invariants.I3(ctx, e.inv)))
	assertNone(t, r(invariants.I6(ctx, e.inv)))
	if err := e.client.Del(ctx, e.cfg.Keys().DLQ(workload.GroupReadModel)).Err(); err != nil {
		t.Fatal(err)
	}
	assertOnly(t, r(invariants.I6(ctx, e.inv)), invariants.IDI6, "not exactly 1..n")
}
