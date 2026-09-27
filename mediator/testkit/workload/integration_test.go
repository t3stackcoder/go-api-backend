//go:build integration

package workload_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

var (
	pgURL    string
	rootOnce sync.Once
	rootPool *pgxpool.Pool
)

// TestMain starts postgres:18 once for the package (spec 11.5).
func TestMain(m *testing.M) {
	ctx := context.Background()
	c, err := postgres.Run(ctx, "postgres:18",
		postgres.WithDatabase("mediator"), postgres.WithUsername("mediator"), postgres.WithPassword("mediator"),
		postgres.BasicWaitStrategies())
	if err != nil {
		log.Fatalf("start postgres: %v", err)
	}
	pgURL, err = c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("connection string: %v", err)
	}
	code := m.Run()
	if rootPool != nil {
		rootPool.Close()
	}
	_ = testcontainers.TerminateContainer(c)
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

// newSchema creates a fresh schema with the framework and workload tables
// and returns a pool bound to it.
func newSchema(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "wl_" + hex.EncodeToString(b[:])
	if _, err := root(t).Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	pool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: name, MaxConns: 8})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = root(t).Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE")
	})
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := workload.Migrate(ctx, pool); err != nil {
		t.Fatalf("workload migrate: %v", err)
	}
	if err := workload.Migrate(ctx, pool); err != nil {
		t.Fatalf("workload migrate must be idempotent: %v", err)
	}
	if err := workload.SeedBank(ctx, pool); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := workload.SeedBank(ctx, pool); err != nil {
		t.Fatalf("seed must be idempotent: %v", err)
	}
	return pool
}

// fixture is a mediator with the real unit of work, idempotency, and inbox
// behaviors registered directly (no behavior package) over a fresh schema.
type fixture struct {
	pool  *pgxpool.Pool
	store *pg.PgStore
	m     *mediator.Mediator
}

func newFixture(t *testing.T, deps workload.Deps, extra func(m *mediator.Mediator)) *fixture {
	t.Helper()
	return newFixtureStore(t, deps, extra, pg.StoreConfig{Partitions: 4})
}

// newFixtureStore is newFixture with the store configuration of the test.
func newFixtureStore(t *testing.T, deps workload.Deps, extra func(m *mediator.Mediator), cfg pg.StoreConfig) *fixture {
	t.Helper()
	pool := newSchema(t)
	store := pg.NewStore(pool, cfg)
	m := mediator.New(mediator.WithNodeID("n1"))
	if err := mediator.Use(m, pg.UnitOfWork(store, pg.UnitOfWorkConfig{}), mediator.Requests(), mediator.Consumers()); err != nil {
		t.Fatal(err)
	}
	if err := mediator.Use(m, pg.Idempotency(pg.IdempotencyConfig{}), mediator.Commands()); err != nil {
		t.Fatal(err)
	}
	if err := mediator.Use(m, pg.Inbox(), mediator.Consumers()); err != nil {
		t.Fatal(err)
	}
	if extra != nil {
		extra(m)
	}
	if err := workload.Register(m, deps); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	return &fixture{pool: pool, store: store, m: m}
}

func (f *fixture) count(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := f.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func send[R any](t *testing.T, f *fixture, req mediator.Request[R]) R {
	t.Helper()
	res, err := mediator.Send(context.Background(), f.m, req)
	if err != nil {
		t.Fatalf("%T: %v", req, err)
	}
	return res
}

func TestIntegration_Register(t *testing.T) {
	t.Parallel()
	f := newFixture(t, workload.Deps{Groups: []string{}}, nil)
	ctx := context.Background()
	if v := send(t, f, workload.GetValue{Key: "k"}); v.Found || v.Key != "k" {
		t.Fatalf("missing key: %+v", v)
	}
	send(t, f, workload.SetValue{Key: "k", Val: 7, CmdID: "c1"})
	if v := send(t, f, workload.GetValue{Key: "k"}); !v.Found || v.Val != 7 {
		t.Fatalf("get: %+v", v)
	}
	if v := send(t, f, workload.GetValueCached{Key: "k"}); !v.Found || v.Val != 7 {
		t.Fatalf("get cached: %+v", v)
	}
	// Marker row with the request ID; unkeyed: no idempotency row, no counter.
	var name, key, node, reqID string
	if err := f.pool.QueryRow(ctx, `SELECT name, key, node, request_id FROM wl_cmd_log WHERE cmd_id = 'c1'`).Scan(&name, &key, &node, &reqID); err != nil {
		t.Fatal(err)
	}
	if name != workload.NameSetValue || key != "k" || node != "n1" {
		t.Fatalf("cmd log: %s %s %s", name, key, node)
	}
	if _, err := uuid.Parse(reqID); err != nil {
		t.Fatalf("request id %q: %v", reqID, err)
	}
	if f.count(t, `SELECT count(*) FROM mediator_idempotency`) != 0 || f.count(t, `SELECT count(*) FROM wl_executions`) != 0 {
		t.Fatal("unkeyed command must not reserve or count")
	}
	// An unkeyed retry with the same command ID re-executes and updates the marker.
	send(t, f, workload.SetValue{Key: "k", Val: 8, CmdID: "c1"})
	if v := send(t, f, workload.GetValue{Key: "k"}); v.Val != 8 {
		t.Fatalf("retry: %+v", v)
	}
	// Keyed: executes once, replays return the stored response, counter stays 1.
	for i := 0; i < 3; i++ {
		send(t, f, workload.SetValue{Key: "k", Val: 9, CmdID: "c2", Keyed: true, Invalidate: true})
	}
	if v := send(t, f, workload.GetValue{Key: "k"}); v.Val != 9 {
		t.Fatalf("keyed: %+v", v)
	}
	if n := f.count(t, `SELECT n FROM wl_executions WHERE scope = $1 AND key = 'c2'`, workload.NameSetValue); n != 1 {
		t.Fatalf("executions = %d", n)
	}
	if n := f.count(t, `SELECT hits FROM mediator_idempotency WHERE scope = $1 AND key = 'c2'`, workload.NameSetValue); n != 2 {
		t.Fatalf("hits = %d", n)
	}
	var by string
	if err := f.pool.QueryRow(ctx, `SELECT updated_by FROM wl_register WHERE key = 'k'`).Scan(&by); err != nil || by != "c2" {
		t.Fatalf("updated_by = %q %v", by, err)
	}
	// A payload change under the same key is rejected.
	_, err := mediator.Send(ctx, f.m, workload.SetValue{Key: "k", Val: 10, CmdID: "c2", Keyed: true})
	if mediator.CodeOf(err) != mediator.CodeIdempotencyMismatch {
		t.Fatalf("mismatch: %v", err)
	}
}

func TestIntegration_Bank(t *testing.T) {
	t.Parallel()
	f := newFixture(t, workload.Deps{Groups: []string{}}, nil)
	ctx := context.Background()
	view := send(t, f, workload.ReadAll{})
	if view.Total != workload.BankTotal() || len(view.Balances) != 4 || view.Balances["a"] != 100 {
		t.Fatalf("seed: %+v", view)
	}
	send(t, f, workload.Transfer{From: "a", To: "b", Amt: 30, CmdID: "t1", Keyed: true})
	send(t, f, workload.Transfer{From: "a", To: "b", Amt: 30, CmdID: "t1", Keyed: true}) // replay
	view = send(t, f, workload.ReadAll{})
	if view.Balances["a"] != 70 || view.Balances["b"] != 130 || view.Total != 400 {
		t.Fatalf("after transfer: %+v", view)
	}
	_, err := mediator.Send(ctx, f.m, workload.Transfer{From: "a", To: "c", Amt: 71, CmdID: "t2"})
	if mediator.CodeOf(err) != mediator.CodePrecondition {
		t.Fatalf("insufficient: %v", err)
	}
	_, err = mediator.Send(ctx, f.m, workload.Transfer{From: "a", To: "zz", Amt: 1, CmdID: "t3"})
	if mediator.CodeOf(err) != mediator.CodeNotFound {
		t.Fatalf("unknown account: %v", err)
	}
	view = send(t, f, workload.ReadAll{})
	if view.Balances["a"] != 70 || view.Total != 400 {
		t.Fatalf("failed transfers must not move money: %+v", view)
	}
	if f.count(t, `SELECT count(*) FROM wl_cmd_log WHERE cmd_id IN ('t2', 't3')`) != 0 {
		t.Fatal("failed commands must leave no marker")
	}
	// Concurrent opposite transfers do not deadlock and conserve money.
	var wg sync.WaitGroup
	errs := make([]error, 20)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			from, to := "c", "d"
			if i%2 == 1 {
				from, to = "d", "c"
			}
			_, errs[i] = mediator.Send(ctx, f.m, workload.Transfer{From: from, To: to, Amt: 5, CmdID: "x" + string(rune('a'+i))})
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if view := send(t, f, workload.ReadAll{}); view.Total != 400 || view.Balances["c"] != 100 || view.Balances["d"] != 100 {
		t.Fatalf("concurrent: %+v", view)
	}
}

func TestIntegration_Append(t *testing.T) {
	t.Parallel()
	f := newFixture(t, workload.Deps{Groups: []string{}}, nil)
	ctx := context.Background()
	first := send(t, f, workload.Append{Key: "list", Val: 10, CmdID: "a1"})
	again := send(t, f, workload.Append{Key: "list", Val: 10, CmdID: "a1"})
	if first != again || first.Applied == 0 {
		t.Fatalf("replay must return the stored response: %+v %+v", first, again)
	}
	send(t, f, workload.Append{Key: "list", Val: 20, CmdID: "a2"})
	send(t, f, workload.Append{Key: "other", Val: 30, CmdID: "a3"})
	if list := send(t, f, workload.ReadList{Key: "list"}); len(list) != 2 || list[0] != 10 || list[1] != 20 {
		t.Fatalf("list: %v", list)
	}
	if list := send(t, f, workload.ReadList{Key: "none"}); list == nil || len(list) != 0 {
		t.Fatalf("empty list must be non-nil: %v", list)
	}
	if n := f.count(t, `SELECT n FROM wl_executions WHERE scope = $1 AND key = 'a1'`, workload.NameAppend); n != 1 {
		t.Fatalf("executions = %d", n)
	}
	if f.count(t, `SELECT count(*) FROM wl_appends`) != 3 {
		t.Fatal("rows")
	}
	// Concurrent duplicates of one key: one execution, identical responses.
	var wg sync.WaitGroup
	results := make([]workload.AppendResult, 8)
	errs := make([]error, 8)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = mediator.Send(ctx, f.m, workload.Append{Key: "race", Val: 1, CmdID: "r1"})
		}()
	}
	wg.Wait()
	for i := range results {
		if errs[i] != nil || results[i] != results[0] {
			t.Fatalf("racer %d: %+v %v", i, results[i], errs[i])
		}
	}
	if n := f.count(t, `SELECT n FROM wl_executions WHERE scope = $1 AND key = 'r1'`, workload.NameAppend); n != 1 {
		t.Fatalf("race executions = %d", n)
	}
	// A payload change under a completed key is rejected, not re-executed.
	_, err := mediator.Send(ctx, f.m, workload.Append{Key: "list", Val: 99, CmdID: "a1"})
	if mediator.CodeOf(err) != mediator.CodeIdempotencyMismatch {
		t.Fatalf("payload change: %v", err)
	}
}

func TestIntegration_BumpAndAtomic(t *testing.T) {
	t.Parallel()
	f := newFixture(t, workload.Deps{Groups: []string{}}, nil)
	ctx := context.Background()
	if r := send(t, f, workload.Bump{Key: "k1", CmdID: "b1"}); r.N != 1 {
		t.Fatalf("bump: %+v", r)
	}
	if r := send(t, f, workload.Bump{Key: "k1", CmdID: "b1"}); r.N != 1 {
		t.Fatalf("replayed bump must not increment: %+v", r)
	}
	if r := send(t, f, workload.Bump{Key: "k1"}); r.N != 2 {
		t.Fatalf("unkeyed bump: %+v", r)
	}
	type row struct {
		Topic, Key, Type, Cmd, Cause string
		Seq                          int64
		Partition                    int
	}
	rows, err := f.pool.Query(ctx, `SELECT topic, stream_key, event_type, headers->'h'->>'cmd', headers->>'cause', seq, partition FROM mediator_outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	var out []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.Topic, &r.Key, &r.Type, &r.Cmd, &r.Cause, &r.Seq, &r.Partition); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	rows.Close()
	if len(out) != 2 || out[0].Topic != workload.TopicBumped || out[0].Type != workload.NameBumped || out[0].Key != "k1" || out[0].Seq != 1 || out[1].Seq != 2 || out[0].Cmd != "b1" {
		t.Fatalf("outbox: %+v", out)
	}
	if out[1].Cmd[:4] != "req:" || out[0].Partition != mediator.Partition("k1", 4) {
		t.Fatalf("unkeyed bump header/partition: %+v", out[1])
	}
	var reqID string
	if err := f.pool.QueryRow(ctx, `SELECT request_id FROM wl_cmd_log WHERE cmd_id = 'b1'`).Scan(&reqID); err != nil || reqID != out[0].Cause {
		t.Fatalf("causation %q vs request id %q (%v)", out[0].Cause, reqID, err)
	}
	if n := f.count(t, `SELECT count(*) FROM wl_cmd_log WHERE cmd_id = $1`, out[1].Cmd); n != 1 {
		t.Fatal("unkeyed bump must log under its derived id")
	}

	send(t, f, workload.AtomicScenario{CmdID: "s1", Key1: "z2", Key2: "z1", Val: 5, CmdKey: "s1"})
	send(t, f, workload.AtomicScenario{CmdID: "s1", Key1: "z2", Key2: "z1", Val: 5, CmdKey: "s1"}) // replay
	checks := map[string]int64{
		`SELECT count(*) FROM wl_register WHERE key = 'z2' AND val = 5 AND updated_by = 's1'`:            1,
		`SELECT count(*) FROM mediator_outbox WHERE headers->'h'->>'cmd' = 's1'`:                         2,
		`SELECT count(*) FROM mediator_outbox WHERE headers->'h'->>'cmd' = 's1' AND stream_key = 'z1'`:   1,
		`SELECT count(*) FROM wl_side WHERE cmd_id = 's1'`:                                               1,
		`SELECT count(*) FROM wl_cmd_log WHERE cmd_id = 's1' AND name = 'wl.AtomicScenario'`:             1,
		`SELECT count(*) FROM mediator_idempotency WHERE scope = 'wl.AtomicScenario' AND key = 's1'`:     1,
		`SELECT coalesce(sum(n), 0) FROM wl_executions WHERE scope = 'wl.AtomicScenario' AND key = 's1'`: 1,
		`SELECT coalesce(sum(n), 0) FROM wl_bumps WHERE key IN ('z1', 'z2')`:                             2,
	}
	for sql, want := range checks {
		if got := f.count(t, sql); got != want {
			t.Fatalf("%s = %d, want %d", sql, got, want)
		}
	}

	// A failing in-process handler rolls back everything the command wrote.
	g := newFixture(t, workload.Deps{Groups: []string{}}, func(m *mediator.Mediator) {
		if err := mediator.OnFunc(m, func(context.Context, workload.AtomicDone) error {
			return errors.New("second handler fails")
		}); err != nil {
			t.Fatal(err)
		}
	})
	if _, err := mediator.Send(ctx, g.m, workload.AtomicScenario{CmdID: "s2", Key1: "y1", Key2: "y2", CmdKey: "s2"}); err == nil {
		t.Fatal("must fail")
	}
	for _, sql := range []string{
		`SELECT count(*) FROM wl_register`, `SELECT count(*) FROM mediator_outbox`, `SELECT count(*) FROM wl_side`,
		`SELECT count(*) FROM wl_cmd_log`, `SELECT count(*) FROM mediator_idempotency`, `SELECT count(*) FROM wl_executions`,
		`SELECT count(*) FROM wl_bumps`, `SELECT count(*) FROM mediator_stream_seq`,
	} {
		if n := g.count(t, sql); n != 0 {
			t.Fatalf("%s = %d after rollback", sql, n)
		}
	}
	// A command run as a query cannot publish: the read-only transaction rejects it.
	if err := pg.WithTx(ctx, g.store, pg.TxOptions{ReadOnly: true}, func(ctx context.Context) error {
		return mediator.Publish(ctx, g.m, workload.Bumped{Key: "k", N: 1})
	}); !errors.Is(err, mediator.ErrDurablePublishInQuery) {
		t.Fatalf("read-only publish: %v", err)
	}
}

// outboxEntries reads the outbox rows of the schema as envelopes and
// payloads, in id order.
func outboxEntries(t *testing.T, f *fixture) []pg.OutboxEntry {
	t.Helper()
	rows, err := f.pool.Query(context.Background(), `SELECT id, event_id, topic, stream_key, seq, partition, event_type, payload FROM mediator_outbox ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []pg.OutboxEntry
	for rows.Next() {
		var e pg.OutboxEntry
		if err := rows.Scan(&e.ID, &e.Envelope.ID, &e.Envelope.Topic, &e.Envelope.StreamKey, &e.Envelope.Seq, &e.Envelope.Partition, &e.Envelope.Type, &e.Payload); err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
	return out
}

func TestIntegration_Consumers(t *testing.T) {
	t.Parallel()
	f := newFixture(t, workload.Deps{Groups: workload.AllGroups, StrictProjection: true, PoisonKey: "bad", NestedTouch: true}, nil)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		send(t, f, workload.Bump{Key: "k"})
	}
	send(t, f, workload.Bump{Key: "bad"})
	entries := outboxEntries(t, f)
	if len(entries) != 4 {
		t.Fatalf("outbox rows: %d", len(entries))
	}
	deliver := func(group string, e pg.OutboxEntry, token int64) (*mediator.ConsumerState, error) {
		st := &mediator.ConsumerState{Attempt: 1}
		dctx := mediator.WithConsumerState(mediator.WithFencingToken(ctx, token), st)
		return st, f.m.Deliver(dctx, group, e.Envelope, e.Payload)
	}
	// In order, with increasing fencing tokens, to every group.
	for i, e := range entries[:3] {
		for _, g := range workload.AllGroups {
			if _, err := deliver(g, e, int64(10+i)); err != nil {
				t.Fatalf("%s seq %d: %v", g, e.Envelope.Seq, err)
			}
		}
	}
	var count, last int64
	if err := f.pool.QueryRow(ctx, `SELECT count, last_seq FROM wl_projection WHERE grp = $1 AND key = 'k'`, workload.GroupReadModel).Scan(&count, &last); err != nil || count != 3 || last != 3 {
		t.Fatalf("projection: %d %d %v", count, last, err)
	}
	type applied struct {
		Seq, Fencing int64
		Node         string
	}
	rows, err := f.pool.Query(ctx, `SELECT seq, fencing, node FROM wl_applied WHERE grp = $1 AND key = 'k' ORDER BY applied`, workload.GroupReadModel)
	if err != nil {
		t.Fatal(err)
	}
	var ap []applied
	for rows.Next() {
		var a applied
		if err := rows.Scan(&a.Seq, &a.Fencing, &a.Node); err != nil {
			t.Fatal(err)
		}
		ap = append(ap, a)
	}
	rows.Close()
	if len(ap) != 3 || ap[0].Seq != 1 || ap[2].Seq != 3 || ap[0].Fencing != 10 || ap[2].Fencing != 12 || ap[1].Node != "n1" {
		t.Fatalf("applied: %+v", ap)
	}
	if f.count(t, `SELECT count(*) FROM wl_applied`) != 9 || f.count(t, `SELECT count(*) FROM wl_audit`) != 3 || f.count(t, `SELECT count(*) FROM mediator_inbox`) != 9 {
		t.Fatal("every group applies every event once")
	}
	if f.count(t, `SELECT count(*) FROM wl_cmd_log WHERE name = $1 AND key = 'k'`, workload.NameTouch) != 3 {
		t.Fatal("nested Touch must log once per projection apply")
	}
	// Redelivery is a no-op through the inbox.
	st, err := deliver(workload.GroupReadModel, entries[0], 13)
	if err != nil || !st.Duplicate {
		t.Fatalf("redelivery: %v duplicate=%v", err, st.Duplicate)
	}
	if f.count(t, `SELECT count(*) FROM wl_applied`) != 9 {
		t.Fatal("redelivery must not apply again")
	}
	// The poison consumer fails its key; the others apply it.
	_, err = deliver(workload.GroupPoison, entries[3], 14)
	if mediator.CodeOf(err) != mediator.CodeInternal || mediator.IsTransient(err) {
		t.Fatalf("poison: %v", err)
	}
	if f.count(t, `SELECT count(*) FROM mediator_inbox WHERE consumer_group = $1`, workload.GroupPoison) != 3 {
		t.Fatal("a failed delivery must roll back its inbox row")
	}
	if _, err := deliver(workload.GroupAudit, entries[3], 14); err != nil {
		t.Fatal(err)
	}
	// Strict projection rejects a gap: seq 5 for a key at seq 0.
	gap := entries[0]
	gap.Envelope.ID = mediator.NewID(time.Now())
	gap.Envelope.StreamKey = "fresh"
	gap.Envelope.Seq = 5
	gap.Payload, _ = json.Marshal(workload.Bumped{Key: "fresh", N: 5})
	if _, err := deliver(workload.GroupReadModel, gap, 15); err == nil || mediator.CodeOf(err) != mediator.CodeInternal {
		t.Fatalf("gap: %v", err)
	}
	if f.count(t, `SELECT count(*) FROM wl_projection WHERE key = 'fresh'`) != 0 {
		t.Fatal("rejected event must not project")
	}
	// A non-strict projector applies whatever arrives.
	lax := newFixture(t, workload.Deps{Groups: []string{workload.GroupReadModel}}, nil)
	if err := lax.m.Deliver(mediator.WithFencingToken(ctx, 1), workload.GroupReadModel, gap.Envelope, gap.Payload); err != nil {
		t.Fatal(err)
	}
	if lax.count(t, `SELECT last_seq FROM wl_projection WHERE key = 'fresh'`) != 5 {
		t.Fatal("lax projection")
	}
	// Truncate clears every workload table.
	if err := workload.Truncate(ctx, f.pool); err != nil {
		t.Fatal(err)
	}
	for _, table := range workload.Tables {
		if n := f.count(t, "SELECT count(*) FROM "+table); n != 0 {
			t.Fatalf("%s has %d rows after Truncate", table, n)
		}
	}
	if err := workload.Truncate(ctx, lax.pool); err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_MigrateErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: "does_not_exist", MaxConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := workload.Migrate(ctx, pool); err == nil {
		t.Fatal("migrate into a missing schema must fail")
	}
	if err := workload.SeedBank(ctx, pool); err == nil {
		t.Fatal("seed without tables must fail")
	}
	if err := workload.Truncate(ctx, pool); err == nil {
		t.Fatal("truncate without tables must fail")
	}
}

// TestIntegration_StorageErrors drops one workload table at a time and
// checks that every handler touching it reports the failure instead of
// swallowing it, so a broken schema can never look like a clean run.
func TestIntegration_StorageErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	type step struct {
		table string
		run   func(t *testing.T, f *fixture, entry pg.OutboxEntry) error
	}
	sendErr := func(req any) func(*testing.T, *fixture, pg.OutboxEntry) error {
		return func(_ *testing.T, f *fixture, _ pg.OutboxEntry) error {
			_, err := f.m.SendAny(ctx, req)
			return err
		}
	}
	deliverErr := func(group string) func(*testing.T, *fixture, pg.OutboxEntry) error {
		return func(_ *testing.T, f *fixture, e pg.OutboxEntry) error {
			return f.m.Deliver(mediator.WithFencingToken(ctx, 1), group, e.Envelope, e.Payload)
		}
	}
	steps := []step{
		{"wl_register", sendErr(workload.SetValue{Key: "k", CmdID: "c"})},
		{"wl_register", sendErr(workload.GetValue{Key: "k"})},
		{"wl_register", sendErr(workload.AtomicScenario{CmdID: "c", Key1: "a", Key2: "b"})},
		{"wl_bank", sendErr(workload.Transfer{From: "a", To: "b", Amt: 1, CmdID: "c"})},
		{"wl_bank", sendErr(workload.ReadAll{})},
		{"wl_appends", sendErr(workload.Append{Key: "k", CmdID: "c"})},
		{"wl_appends", sendErr(workload.ReadList{Key: "k"})},
		{"wl_executions", sendErr(workload.Append{Key: "k", CmdID: "c"})},
		{"wl_bumps", sendErr(workload.Bump{Key: "k"})},
		{"wl_bumps", sendErr(workload.AtomicScenario{CmdID: "c", Key1: "a", Key2: "b"})},
		{"mediator_stream_seq", sendErr(workload.Bump{Key: "k"})},
		{"wl_cmd_log", sendErr(workload.SetValue{Key: "k", CmdID: "c"})},
		{"wl_cmd_log", sendErr(workload.Touch{Key: "k", CmdID: "c"})},
		{"wl_side", sendErr(workload.AtomicScenario{CmdID: "c", Key1: "a", Key2: "b"})},
		{"wl_projection", deliverErr(workload.GroupReadModel)},
		{"wl_applied", deliverErr(workload.GroupReadModel)},
		{"wl_applied", deliverErr(workload.GroupAudit)},
		{"wl_applied", deliverErr(workload.GroupPoison)},
		{"wl_audit", deliverErr(workload.GroupAudit)},
		{"mediator_idempotency", sendErr(workload.SetValue{Key: "k", CmdID: "c", Keyed: true})},
		{"mediator_partition_epoch", deliverErr(workload.GroupReadModel)},
		{"mediator_inbox", deliverErr(workload.GroupAudit)},
	}
	for i, s := range steps {
		t.Run(s.table+"/"+string(rune('a'+i)), func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, workload.Deps{Groups: workload.AllGroups, StrictProjection: true}, nil)
			send(t, f, workload.Bump{Key: "k"})
			entry := outboxEntries(t, f)[0]
			if _, err := f.pool.Exec(ctx, "DROP TABLE "+s.table); err != nil {
				t.Fatal(err)
			}
			if err := s.run(t, f, entry); err == nil {
				t.Fatalf("%s dropped: handler must fail", s.table)
			}
		})
	}
}

// TestIntegration_HandlerFailures reaches the handler branches a missing
// table cannot: a row that no longer scans, a query that fails while it
// returns rows, a statement the server rejects, a lock that times out
// after the query started, and a second execution the idempotency
// behavior did not prevent.
func TestIntegration_HandlerFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const (
		boomFn      = `CREATE FUNCTION boom() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$`
		boomTrigger = `CREATE FUNCTION boom_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$`
	)
	type step struct {
		name string
		deps workload.Deps
		prep func(t *testing.T, f *fixture, e pg.OutboxEntry)
		run  func(t *testing.T, f *fixture, e pg.OutboxEntry) error
		code mediator.Code // zero means any error
	}
	exec := func(stmts ...string) func(*testing.T, *fixture, pg.OutboxEntry) {
		return func(t *testing.T, f *fixture, _ pg.OutboxEntry) {
			for _, s := range stmts {
				if _, err := f.pool.Exec(ctx, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
		}
	}
	sendErr := func(req any) func(*testing.T, *fixture, pg.OutboxEntry) error {
		return func(_ *testing.T, f *fixture, _ pg.OutboxEntry) error {
			_, err := f.m.SendAny(ctx, req)
			return err
		}
	}
	deliverErr := func(group string) func(*testing.T, *fixture, pg.OutboxEntry) error {
		return func(_ *testing.T, f *fixture, e pg.OutboxEntry) error {
			return f.m.Deliver(mediator.WithFencingToken(ctx, 1), group, e.Envelope, e.Payload)
		}
	}
	transfer := workload.Transfer{From: "a", To: "b", Amt: 1, CmdID: "c"}
	steps := []step{
		{name: "transfer row does not scan", prep: exec(`ALTER TABLE wl_bank ALTER COLUMN balance TYPE text`), run: sendErr(transfer)},
		{name: "transfer debit rejected", prep: exec(boomTrigger, `CREATE TRIGGER reject BEFORE UPDATE ON wl_bank FOR EACH ROW WHEN (NEW.balance < OLD.balance) EXECUTE FUNCTION boom_trigger()`), run: sendErr(transfer)},
		{name: "transfer credit rejected", prep: exec(boomTrigger, `CREATE TRIGGER reject BEFORE UPDATE ON wl_bank FOR EACH ROW WHEN (NEW.balance > OLD.balance) EXECUTE FUNCTION boom_trigger()`), run: sendErr(transfer)},
		{name: "read all row does not scan", prep: exec(`ALTER TABLE wl_bank ALTER COLUMN balance TYPE text`), run: sendErr(workload.ReadAll{})},
		{name: "read all fails while read", prep: exec(boomFn, `DROP TABLE wl_bank`, `CREATE VIEW wl_bank AS SELECT 'a'::text AS account, boom() AS balance`), run: sendErr(workload.ReadAll{})},
		{name: "append executed twice", prep: exec(`INSERT INTO wl_appends (cmd_id, key, val) VALUES ('c', 'k', 1)`), run: sendErr(workload.Append{Key: "k", CmdID: "c"}), code: mediator.CodeConflict},
		{name: "read list row does not scan", prep: exec(`INSERT INTO wl_appends (cmd_id, key, val) VALUES ('c', 'k', 1)`, `ALTER TABLE wl_appends ALTER COLUMN val TYPE text`), run: sendErr(workload.ReadList{Key: "k"})},
		{name: "read list fails while read", prep: exec(boomFn, `DROP TABLE wl_appends`, `CREATE VIEW wl_appends AS SELECT 'c'::text AS cmd_id, 'k'::text AS key, boom() AS val, 1::bigint AS applied`), run: sendErr(workload.ReadList{Key: "k"})},
		{name: "projection insert fails", deps: workload.Deps{Groups: workload.AllGroups}, prep: exec(`DROP TABLE wl_projection`), run: deliverErr(workload.GroupReadModel)},
		{name: "nested touch fails", deps: workload.Deps{Groups: workload.AllGroups, NestedTouch: true}, prep: exec(`DROP TABLE wl_cmd_log`), run: deliverErr(workload.GroupReadModel)},
		{name: "audit applied twice", deps: workload.Deps{Groups: workload.AllGroups}, prep: func(t *testing.T, f *fixture, e pg.OutboxEntry) {
			if _, err := f.pool.Exec(ctx, `INSERT INTO wl_audit (grp, event_id, key, seq) VALUES ($1, $2, 'k', 1)`, workload.GroupAudit, e.Envelope.ID); err != nil {
				t.Fatal(err)
			}
		}, run: deliverErr(workload.GroupAudit), code: mediator.CodeConflict},
	}
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, s.deps, nil)
			send(t, f, workload.Bump{Key: "k"})
			entry := outboxEntries(t, f)[0]
			s.prep(t, f, entry)
			err := s.run(t, f, entry)
			if err == nil {
				t.Fatal("the handler must fail")
			}
			if s.code != "" && mediator.CodeOf(err) != s.code {
				t.Fatalf("want %s, got %v", s.code, err)
			}
		})
	}

	// A row lock that times out after the query has started: the failure
	// arrives with the rows, not with the query.
	t.Run("transfer waits on a locked row", func(t *testing.T) {
		t.Parallel()
		f := newFixtureStore(t, workload.Deps{}, nil, pg.StoreConfig{Partitions: 4, DefaultLockTimeout: 200 * time.Millisecond})
		holder, err := f.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer holder.Rollback(ctx) //nolint:errcheck // cleanup
		if _, err := holder.Exec(ctx, `SELECT balance FROM wl_bank WHERE account = 'a' FOR UPDATE`); err != nil {
			t.Fatal(err)
		}
		_, err = f.m.SendAny(ctx, transfer)
		if !pg.IsLockTimeout(err) {
			t.Fatalf("want a lock timeout, got %v", err)
		}
	})
}
