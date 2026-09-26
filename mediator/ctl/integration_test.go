//go:build integration

package ctl_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json/v2"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/t3stackcoder/go-api-backend/mediator/ctl"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

var (
	pgURL     string
	redisAddr string
)

// TestMain starts postgres:18 and redis:8 once for the package (spec 11.5).
func TestMain(m *testing.M) {
	ctx := context.Background()
	pgc, err := postgres.Run(ctx, "postgres:18",
		postgres.WithDatabase("mediator"), postgres.WithUsername("mediator"), postgres.WithPassword("mediator"),
		postgres.BasicWaitStrategies())
	if err != nil {
		log.Fatalf("start postgres: %v", err)
	}
	pgURL, err = pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("postgres connection string: %v", err)
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
	_ = testcontainers.TerminateContainer(rc)
	_ = testcontainers.TerminateContainer(pgc)
	os.Exit(code)
}

// cli runs Main against the containers with captured output.
type cli struct {
	t        *testing.T
	opts     ctl.Options
	out, err bytes.Buffer
}

func newCLI(t *testing.T) *cli {
	t.Helper()
	var b [4]byte
	_, _ = rand.Read(b[:])
	c := &cli{t: t}
	c.opts = ctl.Options{
		Stdout: &c.out, Stderr: &c.err, PGURL: pgURL,
		Redis: redisx.Config{Addr: redisAddr, Prefix: "ctl" + hex.EncodeToString(b[:]), PartitionsPerTopic: 2},
	}
	return c
}

func (c *cli) expect(code int, args ...string) (stdout, stderr string) {
	c.t.Helper()
	c.out.Reset()
	c.err.Reset()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if got := ctl.Main(ctx, args, c.opts); got != code {
		c.t.Fatalf("%v: exit %d, want %d\nstdout: %s\nstderr: %s", args, got, code, c.out.String(), c.err.String())
	}
	return c.out.String(), c.err.String()
}

func (c *cli) json(args ...string) map[string]any {
	c.t.Helper()
	stdout, _ := c.expect(ctl.ExitOK, append(args, "--json")...)
	var v map[string]any
	if err := json.Unmarshal([]byte(stdout), &v); err != nil {
		c.t.Fatalf("%v: invalid json %q: %v", args, stdout, err)
	}
	return v
}

func contains(t *testing.T, s string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Fatalf("output lacks %q:\n%s", sub, s)
		}
	}
}

// TestIntegration_Main drives every command end to end through Main against
// real Postgres and Redis. The steps share one database, so they run in
// order.
func TestIntegration_Main(t *testing.T) {
	ctx := context.Background()
	c := newCLI(t)
	keys := c.opts.Redis.Keys()

	pool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{MaxConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer client.Close()

	t.Run("migrate status before up", func(t *testing.T) {
		stdout, _ := c.expect(ctl.ExitOK, "migrate", "status")
		contains(t, stdout, "init", "false", "current version: 0")
	})
	t.Run("migrate up", func(t *testing.T) {
		stdout, _ := c.expect(ctl.ExitOK, "migrate", "up")
		contains(t, stdout, "applied 0001_init", "current version: 1 (was 0)")
		stdout, _ = c.expect(ctl.ExitOK, "migrate", "up")
		contains(t, stdout, "nothing to do: current version 1")
	})
	t.Run("migrate status after up", func(t *testing.T) {
		v := c.json("migrate", "status")
		ms := v["migrations"].([]any)
		if v["current"] != 1.0 || len(ms) < 1 || ms[0].(map[string]any)["applied"] != true {
			t.Fatalf("status = %v", v)
		}
	})
	t.Run("outbox stats empty", func(t *testing.T) {
		stdout, _ := c.expect(ctl.ExitOK, "outbox", "stats")
		contains(t, stdout, "no unpublished outbox rows")
	})
	t.Run("outbox stats and consumer lag with a backlog", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO mediator_outbox (event_id, topic, stream_key, seq, partition, event_type, payload)
VALUES (gen_random_uuid(), 'orders', 'o-1', 1, 1, 'OrderSubmitted', '{}'::jsonb)`)
		if err != nil {
			t.Fatal(err)
		}
		v := c.json("outbox", "stats")
		ps := v["partitions"].([]any)
		if len(ps) != 1 || ps[0].(map[string]any)["unpublished"] != 1.0 || ps[0].(map[string]any)["partition"] != 1.0 {
			t.Fatalf("stats = %v", v)
		}
		stdout, _ := c.expect(ctl.ExitOK, "consumer", "lag", "--group", "inventory", "--topic", "orders")
		contains(t, stdout, "inventory", "orders", "true")
		v = c.json("consumer", "lag", "--group", "inventory", "--topic", "orders")
		rows := v["rows"].([]any)
		if len(rows) != 2 {
			t.Fatalf("rows = %v", rows)
		}
		p1 := rows[1].(map[string]any)
		if p1["missing"] != true || p1["unpublished"] != 1.0 || p1["pending"] != 0.0 {
			t.Fatalf("row 1 = %v", p1)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM mediator_outbox`); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("outbox replay nothing", func(t *testing.T) {
		stdout, _ := c.expect(ctl.ExitOK, "outbox", "replay", "--topic", "orders", "--partition", "0")
		contains(t, stdout, "replayed 0 rows of orders partition 0 from id 0")
	})
	t.Run("outbox replay published rows", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO mediator_outbox (event_id, topic, stream_key, seq, partition, event_type, payload, published_at)
VALUES (gen_random_uuid(), 'orders', 'o-2', 1, 0, 'OrderSubmitted', '{"a":1}'::jsonb, now())`)
		if err != nil {
			t.Fatal(err)
		}
		v := c.json("outbox", "replay", "--topic", "orders", "--partition", "0", "--from-id", "1")
		if v["replayed"] != 1.0 {
			t.Fatalf("replay = %v", v)
		}
		n, err := client.XLen(ctx, keys.Stream("orders", 0)).Result()
		if err != nil || n != 1 {
			t.Fatalf("stream length %d, %v", n, err)
		}
		if _, err := pool.Exec(ctx, `DELETE FROM mediator_outbox`); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("outbox reshard", func(t *testing.T) {
		_, stderr := c.expect(ctl.ExitUsage, "outbox", "reshard", "--partitions", "4")
		contains(t, stderr, "refusing to reshard without --yes")
		stdout, stderr := c.expect(ctl.ExitOK, "outbox", "reshard", "--partitions", "4", "--yes")
		contains(t, stderr, "warning: "+ctl.ReshardWarning)
		contains(t, stdout, "resharded to 4 partitions: 0 rows moved")
	})
	t.Run("inbox purge", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO mediator_inbox (consumer_group, event_id, processed_at)
VALUES ('inventory', gen_random_uuid(), now() - interval '10 days'), ('inventory', gen_random_uuid(), now() - interval '5 seconds')`)
		if err != nil {
			t.Fatal(err)
		}
		stdout, _ := c.expect(ctl.ExitOK, "inbox", "purge", "--older-than", "7d")
		contains(t, stdout, "deleted 1 rows of mediator_inbox older than 168h0m0s")
		v := c.json("inbox", "purge", "--older-than", "1s")
		if v["deleted"] != 1.0 {
			t.Fatalf("purge = %v", v)
		}
	})
	t.Run("idem show not found", func(t *testing.T) {
		_, stderr := c.expect(ctl.ExitFailure, "idem", "show", "--scope", "CreateOrder", "--key", "k1")
		contains(t, stderr, `error: not_found: no idempotency row for scope "CreateOrder" key "k1"`)
	})
	t.Run("idem show and purge", func(t *testing.T) {
		_, err := pool.Exec(ctx, `INSERT INTO mediator_idempotency (scope, key, request_hash, response, hits, expires_at) VALUES
('CreateOrder', 'k1', '\xdead'::bytea, '{"orderId":"o-1"}'::jsonb, 2, now() + interval '1 hour'),
('CreateOrder', 'old', '\xbeef'::bytea, NULL, 0, now() - interval '1 hour')`)
		if err != nil {
			t.Fatal(err)
		}
		stdout, _ := c.expect(ctl.ExitOK, "idem", "show", "--scope", "CreateOrder", "--key", "k1")
		contains(t, stdout, "dead", `{"orderId": "o-1"}`, "expired")
		v := c.json("idem", "show", "--scope", "CreateOrder", "--key", "k1")
		if v["hits"] != 2.0 || v["expired"] != false || v["response"].(map[string]any)["orderId"] != "o-1" {
			t.Fatalf("show = %v", v)
		}
		v = c.json("idem", "show", "--scope", "CreateOrder", "--key", "old")
		if _, has := v["response"]; has || v["expired"] != true {
			t.Fatalf("reserved row = %v", v)
		}
		stdout, _ = c.expect(ctl.ExitOK, "idem", "purge")
		contains(t, stdout, "deleted 1 expired rows of mediator_idempotency")
		c.expect(ctl.ExitFailure, "idem", "show", "--scope", "CreateOrder", "--key", "old")
		c.expect(ctl.ExitOK, "idem", "show", "--scope", "CreateOrder", "--key", "k1")
	})
	t.Run("dlq list empty", func(t *testing.T) {
		stdout, _ := c.expect(ctl.ExitOK, "dlq", "list", "--group", "inventory")
		contains(t, stdout, "no dead letters in group inventory")
	})
	t.Run("dlq list, requeue, drop, skip", func(t *testing.T) {
		fields := map[string]any{
			redisx.DLQFieldError: "boom", redisx.DLQFieldAttempts: "3", redisx.DLQFieldStreamID: "1699999999999-0",
			redisx.DLQFieldStream: keys.Stream("orders", 1), redisx.DLQFieldGroup: "inventory", redisx.DLQFieldNode: "n1",
			redisx.DLQFieldAt: time.Now().UTC().Format(time.RFC3339Nano),
			redisx.FieldID:    "0192a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b", redisx.FieldType: "OrderSubmitted",
			redisx.FieldTopic: "orders", redisx.FieldKey: "o-1", redisx.FieldSeq: "4", redisx.FieldPartition: "1",
			redisx.FieldAt: time.Now().UTC().Format(time.RFC3339Nano), redisx.FieldSchema: "1",
			redisx.FieldHeaders: "{}", redisx.FieldPayload: `{"orderId":"o-1"}`, redisx.FieldOutboxID: "77",
		}
		add := func() string {
			t.Helper()
			id, err := client.XAdd(ctx, &redis.XAddArgs{Stream: keys.DLQ("inventory"), Values: fields}).Result()
			if err != nil {
				t.Fatal(err)
			}
			return id
		}
		id := add()
		stdout, _ := c.expect(ctl.ExitOK, "dlq", "list", "--group", "inventory")
		contains(t, stdout, id, "OrderSubmitted", "o-1", "boom", "n1")
		v := c.json("dlq", "list", "--group", "inventory")
		e := v["entries"].([]any)[0].(map[string]any)
		if e["id"] != id || e["attempts"] != 3.0 || e["outboxId"] != 77.0 || e["eventId"] != "0192a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b" {
			t.Fatalf("entry = %v", e)
		}
		stdout, _ = c.expect(ctl.ExitOK, "dlq", "requeue", "--group", "inventory", "--id", id)
		contains(t, stdout, "requeued dead letter "+id)
		if n, err := client.XLen(ctx, keys.Stream("orders", 1)).Result(); err != nil || n != 1 {
			t.Fatalf("requeued stream length %d, %v", n, err)
		}
		stdout, _ = c.expect(ctl.ExitOK, "dlq", "list", "--group", "inventory")
		contains(t, stdout, "no dead letters")
		_, stderr := c.expect(ctl.ExitFailure, "dlq", "requeue", "--group", "inventory", "--id", id)
		contains(t, stderr, "dead-letter entry not found")

		id = add()
		stdout, _ = c.expect(ctl.ExitOK, "dlq", "drop", "--group", "inventory", "--id", id)
		contains(t, stdout, "dropped dead letter "+id)
		_, stderr = c.expect(ctl.ExitFailure, "dlq", "drop", "--group", "inventory", "--id", id)
		contains(t, stderr, "dead-letter entry not found")

		// skip acknowledges a pending entry: create the group, read the
		// requeued entry so it is pending, then skip it.
		if err := client.XGroupCreateMkStream(ctx, keys.Stream("orders", 1), "inventory", "0").Err(); err != nil {
			t.Fatal(err)
		}
		res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: "inventory", Consumer: "c1", Streams: []string{keys.Stream("orders", 1), ">"}, Count: 1}).Result()
		if err != nil || len(res) != 1 || len(res[0].Messages) != 1 {
			t.Fatalf("read group: %v %v", res, err)
		}
		pendingID := res[0].Messages[0].ID
		v = c.json("consumer", "lag", "--group", "inventory", "--topic", "orders")
		p1 := v["rows"].([]any)[1].(map[string]any)
		if p1["pending"] != 1.0 || p1["consumers"].(map[string]any)["c1"] != 1.0 || p1["missing"] != false {
			t.Fatalf("lag row = %v", p1)
		}
		stdout, _ = c.expect(ctl.ExitOK, "dlq", "skip", "--group", "inventory", "--topic", "orders", "--partition", "1", "--id", pendingID)
		contains(t, stdout, "acknowledged "+pendingID)
		_, stderr = c.expect(ctl.ExitFailure, "dlq", "skip", "--group", "inventory", "--topic", "orders", "--partition", "1", "--id", pendingID)
		contains(t, stderr, "is not pending")
	})
	t.Run("lease list empty", func(t *testing.T) {
		stdout, _ := c.expect(ctl.ExitOK, "lease", "list", "--group", "inventory")
		contains(t, stdout, "no leases held in group inventory")
	})
	t.Run("lease list and release", func(t *testing.T) {
		if err := client.Set(ctx, keys.Lease("inventory", "orders", 0), redisx.LeaseValue("node-1", 7), 30*time.Second).Err(); err != nil {
			t.Fatal(err)
		}
		stdout, _ := c.expect(ctl.ExitOK, "lease", "list", "--group", "inventory")
		contains(t, stdout, "orders", "node-1", "7")
		v := c.json("lease", "list", "--group", "inventory")
		l := v["leases"].([]any)[0].(map[string]any)
		if l["node"] != "node-1" || l["epoch"] != 7.0 || l["partition"] != 0.0 || l["ttlSeconds"].(float64) <= 0 {
			t.Fatalf("lease = %v", l)
		}
		stdout, _ = c.expect(ctl.ExitOK, "lease", "release", "--group", "inventory", "--topic", "orders", "--partition", "0")
		contains(t, stdout, "released lease of orders partition 0 in group inventory")
		stdout, _ = c.expect(ctl.ExitOK, "lease", "list", "--group", "inventory")
		contains(t, stdout, "no leases held")
	})
	t.Run("names without registry", func(t *testing.T) {
		_, stderr := c.expect(ctl.ExitFailure, "names")
		contains(t, stderr, "error: no registry available")
	})
	t.Run("migrate down and up again", func(t *testing.T) {
		stdout, stderr := c.expect(ctl.ExitOK, "migrate", "down", "--to", "0")
		contains(t, stderr, "warning: migrate down exists for tests")
		contains(t, stdout, "reverted 0001_init", "current version: 0 (was 1)")
		stdout, _ = c.expect(ctl.ExitOK, "migrate", "status")
		contains(t, stdout, "current version: 0")
		stdout, _ = c.expect(ctl.ExitOK, "migrate", "up")
		contains(t, stdout, "applied 0001_init")
	})
	t.Run("connection errors", func(t *testing.T) {
		bad := newCLI(t)
		bad.opts.PGURL = "postgres://mediator:wrong@127.0.0.1:1/mediator?sslmode=disable&connect_timeout=2"
		_, stderr := bad.expect(ctl.ExitFailure, "migrate", "status")
		contains(t, stderr, "error: connect to postgres:")
		bad.opts.Redis.Addr = "127.0.0.1:1"
		_, stderr = bad.expect(ctl.ExitFailure, "dlq", "list", "--group", "g")
		contains(t, stderr, "error: connect to redis:")
	})
}
