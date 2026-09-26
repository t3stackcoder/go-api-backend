//go:build integration

package pg_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

// recordingTrimmer wraps memstore.Streams and records TrimBefore calls.
type recordingTrimmer struct {
	*memstore.Streams
	mu    sync.Mutex
	calls []string
}

func (r *recordingTrimmer) TrimBefore(ctx context.Context, topic string, partition int, minTime time.Time) error {
	r.mu.Lock()
	r.calls = append(r.calls, topic)
	r.mu.Unlock()
	return r.Streams.TrimBefore(ctx, topic, partition, minTime)
}

// seedRetention inserts expired and live rows and returns the tag that
// makes this call's keys and idempotency scope unique.
func seedRetention(t *testing.T, pool *pgxpool.Pool) string {
	t.Helper()
	ctx := context.Background()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
	// Stream keys are unique per call: (topic, stream_key, seq) is a unique index.
	const outbox = `INSERT INTO mediator_outbox (event_id, topic, stream_key, seq, partition, event_type, payload, published_at) VALUES (gen_random_uuid(), 't', $1, $2, 0, 'E', '{}', $3)`
	run := uuid.New().String()
	for i := 1; i <= 3; i++ {
		exec(outbox, "old-"+run, i, time.Now().Add(-8*24*time.Hour))
	}
	for i := 1; i <= 2; i++ {
		exec(outbox, "new-"+run, i, time.Now().Add(-24*time.Hour))
	}
	exec(outbox, "unpublished-"+run, 1, nil)
	exec(`INSERT INTO mediator_inbox (consumer_group, event_id, processed_at) VALUES ('g', gen_random_uuid(), now() - interval '8 days'), ('g', gen_random_uuid(), now() - interval '8 days'), ('g', gen_random_uuid(), now())`)
	exec(`INSERT INTO mediator_idempotency (scope, key, request_hash, expires_at) VALUES ($1, 'e1', '\x01', now() - interval '1 hour'), ($1, 'e2', '\x01', now() - interval '1 minute'), ($1, 'ok', '\x01', now() + interval '1 hour')`, "s-"+run)
	return run
}

// Janitor tests share one global advisory lock, so they do not run in parallel.
func TestIntegration_Janitor(t *testing.T) {
	ctx := context.Background()
	pool, _ := newSchema(t, true)
	seedRetention(t, pool)
	trimmer := &recordingTrimmer{Streams: memstore.NewStreams(nil)}
	_, _ = trimmer.Append(ctx, "t", 0, []pg.OutboxEntry{{ID: 1}})
	j := pg.NewJanitor(pool, pg.JanitorConfig{BatchSize: 2, Trimmer: trimmer, Topics: []string{"t", "u"}, Partitions: 2})
	res, err := j.Sweep(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if res.Outbox != 3 || res.Inbox != 2 || res.Idempotency != 2 || res.Trimmed != 4 || res.Skipped {
		t.Fatalf("sweep: %+v", res)
	}
	if n := count(t, pool, `SELECT count(*) FROM mediator_outbox`); n != 3 {
		t.Fatalf("outbox left %d, want 3 (2 recent published + 1 unpublished)", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM mediator_inbox`); n != 1 {
		t.Fatalf("inbox left %d", n)
	}
	if n := count(t, pool, `SELECT count(*) FROM mediator_idempotency`); n != 1 {
		t.Fatalf("idempotency left %d", n)
	}
	if len(trimmer.calls) != 4 || len(trimmer.Entries("t", 0)) != 1 {
		t.Fatalf("trimmer calls %v, entries %d", trimmer.calls, len(trimmer.Entries("t", 0)))
	}
	if j.Healthy() != nil || j.Sweeps() != 1 {
		t.Fatal("healthy after a clean sweep")
	}

	// Another holder of the janitor lock makes the sweep skip.
	pc, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var got bool
	if err := pc.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('mediator_janitor'))`).Scan(&got); err != nil || !got {
		t.Fatalf("lock: %v %v", got, err)
	}
	res, err = j.Sweep(ctx)
	if err != nil || !res.Skipped {
		t.Fatalf("want skipped, got %+v %v", res, err)
	}
	pc.Hijack().Close(ctx)

	// Misconfigured retention is reported by Sweep and Healthy.
	bad := pg.NewJanitor(pool, pg.JanitorConfig{InboxRetention: time.Hour, OutboxRetention: 2 * time.Hour})
	if _, err := bad.Sweep(ctx); err == nil || bad.Healthy() == nil {
		t.Fatal("inbox retention shorter than outbox retention must fail")
	}

	// Run sweeps immediately and then on the interval until canceled.
	seedRetention(t, pool)
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	fast := pg.NewJanitor(pool, pg.JanitorConfig{Interval: 50 * time.Millisecond})
	go func() { done <- fast.Run(rctx) }()
	eventually(t, 5*time.Second, "periodic sweeps", func() bool { return fast.Sweeps() >= 2 })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if n := count(t, pool, `SELECT count(*) FROM mediator_outbox WHERE published_at < now() - interval '7 days'`); n != 0 {
		t.Fatalf("%d expired rows survived the periodic sweep", n)
	}
}

func TestIntegration_Ops(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool, _ := newSchema(t, true)
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: 2})
	publish(t, store, "orders", []string{"a", "b", "c"}, 2)

	stats, err := pg.OutboxStats(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, s := range stats {
		if s.Topic != "orders" || s.OldestAge < 0 {
			t.Fatalf("stats row: %+v", s)
		}
		total += s.Unpublished
	}
	if total != 6 {
		t.Fatalf("unpublished total %d: %+v", total, stats)
	}

	if err := pg.CheckPartitions(ctx, pool, 2); err != nil {
		t.Fatal(err)
	}
	if err := pg.CheckPartitions(ctx, pool, 1); err == nil {
		t.Fatal("P smaller than an unpublished row's partition must be rejected")
	}
	if err := pg.CheckPartitions(ctx, pool, 0); err == nil {
		t.Fatal("P must be positive")
	}

	// Mark everything published and record a cursor, then replay from an id.
	if _, err := pool.Exec(ctx, `UPDATE mediator_outbox SET published_at = now()`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO mediator_relay_cursor (topic, partition, last_outbox_id, last_stream_id) VALUES ('orders', 1, 6, '1-0')`); err != nil {
		t.Fatal(err)
	}
	if err := pg.CheckPartitions(ctx, pool, 1); err == nil {
		t.Fatal("P smaller than a cursor partition must be rejected")
	}
	sink := memstore.NewStreams(nil)
	pa := mediator.Partition("a", 2)
	rowsInA := count(t, pool, `SELECT count(*) FROM mediator_outbox WHERE partition = $1`, pa)
	n, err := pg.OutboxReplay(ctx, pool, sink, "orders", pa, 1)
	if err != nil || int64(n) != rowsInA || int64(len(sink.Entries("orders", pa))) != rowsInA {
		t.Fatalf("replay: %d %v (rows %d)", n, err, rowsInA)
	}
	var lastID int64
	if err := pool.QueryRow(ctx, `SELECT max(id) FROM mediator_outbox WHERE partition = $1`, pa).Scan(&lastID); err != nil {
		t.Fatal(err)
	}
	if n, _ := pg.OutboxReplay(ctx, pool, sink, "orders", pa, lastID); n != 1 {
		t.Fatalf("replay from the last id must add one row, got %d", n)
	}
	if n, _ := pg.OutboxReplay(ctx, pool, sink, "orders", pa, lastID+1); n != 0 {
		t.Fatalf("replay past the end: %d", n)
	}
	sink.Hooks.Append = func(string, int, []pg.OutboxEntry) error { return context.DeadlineExceeded }
	if _, err := pg.OutboxReplay(ctx, pool, sink, "orders", pa, 1); err == nil {
		t.Fatal("append failure must be reported")
	}

	// Reshard to 5 partitions: every row carries its new partition and the
	// cursors are gone.
	moved, err := pg.OutboxReshard(ctx, pool, 5)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := pool.Query(ctx, `SELECT stream_key, partition FROM mediator_outbox`)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for rows.Next() {
		var key string
		var p int
		if err := rows.Scan(&key, &p); err != nil {
			t.Fatal(err)
		}
		if p != mediator.Partition(key, 5) {
			t.Fatalf("key %s partition %d after reshard", key, p)
		}
		seen++
	}
	rows.Close()
	if seen != 6 || moved < 0 || count(t, pool, `SELECT count(*) FROM mediator_relay_cursor`) != 0 {
		t.Fatalf("reshard: seen=%d moved=%d", seen, moved)
	}
	if _, err := pg.OutboxReshard(ctx, pool, 0); err == nil {
		t.Fatal("reshard needs a positive P")
	}
	if err := pg.CheckPartitions(ctx, pool, 5); err != nil {
		t.Fatal(err)
	}

	// Inbox and idempotency purges, and idem show.
	run := seedRetention(t, pool)
	if n, err := pg.InboxPurge(ctx, pool, 7*24*time.Hour); err != nil || n != 2 {
		t.Fatalf("inbox purge: %d %v", n, err)
	}
	if n, err := pg.IdemPurge(ctx, pool); err != nil || n != 2 {
		t.Fatalf("idem purge: %d %v", n, err)
	}
	info, err := pg.IdemShow(ctx, pool, "s-"+run, "ok")
	if err != nil || info.Scope != "s-"+run || info.Key != "ok" || info.Response != nil || info.Hits != 0 || info.ExpiresAt.Before(time.Now()) || info.CreatedAt.IsZero() || len(info.RequestHash) != 1 {
		t.Fatalf("idem show: %+v %v", info, err)
	}
	if _, err := pg.IdemShow(ctx, pool, "s", "missing"); mediator.CodeOf(err) != mediator.CodeNotFound {
		t.Fatalf("want not found, got %v", err)
	}

	// Operations fail cleanly on a closed pool.
	closed, _ := newSchema(t, true)
	closed.Close()
	if _, err := pg.OutboxStats(ctx, closed); err == nil {
		t.Fatal("stats on a closed pool")
	}
	if _, err := pg.OutboxReshard(ctx, closed, 2); err == nil {
		t.Fatal("reshard on a closed pool")
	}
	if err := pg.CheckPartitions(ctx, closed, 2); err == nil {
		t.Fatal("check on a closed pool")
	}
	if _, err := pg.IdemShow(ctx, closed, "s", "k"); err == nil {
		t.Fatal("show on a closed pool")
	}
	if _, err := pg.MigrationStatus(ctx, closed); err == nil {
		t.Fatal("status on a closed pool")
	}
	if err := pg.Migrate(ctx, closed); err == nil {
		t.Fatal("migrate on a closed pool")
	}
	if _, err := pg.NewJanitor(closed, pg.JanitorConfig{}).Sweep(ctx); err == nil {
		t.Fatal("sweep on a closed pool")
	}
}
