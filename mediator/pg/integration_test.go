//go:build integration

package pg_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/pg/storetest"
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

// newSchema creates a fresh schema and returns a pool whose search_path is
// bound to it, migrated when migrate is true. Tests run in parallel because
// each owns a schema.
func newSchema(t *testing.T, migrate bool) (*pgxpool.Pool, string) {
	t.Helper()
	ctx := context.Background()
	var b [6]byte
	_, _ = rand.Read(b[:])
	name := "t_" + hex.EncodeToString(b[:])
	if _, err := root(t).Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	pool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: name, MaxConns: 8})
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(func() {
		// Close waits for acquired connections; a failed test may still hold
		// one, so bound the wait instead of wedging the whole run.
		closed := make(chan struct{})
		go func() {
			pool.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
			t.Logf("pool for schema %s did not close in time; a connection is still held", name)
		}
		_, _ = root(t).Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{name}.Sanitize()+" CASCADE")
	})
	if migrate {
		if err := pg.Migrate(ctx, pool); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}
	return pool, name
}

func tableExists(t *testing.T, pool *pgxpool.Pool, table string) bool {
	t.Helper()
	var reg *string
	if err := pool.QueryRow(context.Background(), "SELECT to_regclass($1)::text", table).Scan(&reg); err != nil {
		t.Fatal(err)
	}
	return reg != nil
}

func count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// eventually polls cond until it holds or the timeout passes.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestIntegration_Migrations_UpDownUp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool, _ := newSchema(t, false)

	status, err := pg.MigrationStatus(ctx, pool)
	if err != nil || len(status) < 1 || status[0].Applied {
		t.Fatalf("status before: %+v %v", status, err)
	}
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	status, _ = pg.MigrationStatus(ctx, pool)
	for _, s := range status {
		if !s.Applied || s.AppliedAt.IsZero() {
			t.Fatalf("not applied: %+v", s)
		}
	}
	if err := pg.Migrate(ctx, pool); err != nil {
		t.Fatalf("second migrate must be a no-op: %v", err)
	}
	seed := func() {
		if _, err := pool.Exec(ctx, `INSERT INTO mediator_outbox (event_id, topic, stream_key, seq, partition, event_type, payload) VALUES (gen_random_uuid(), 't', 'k', 1, 0, 'E', '{}')`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO mediator_inbox (consumer_group, event_id) VALUES ('g', gen_random_uuid())`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO mediator_idempotency (scope, key, request_hash, expires_at) VALUES ('s', 'k', '\x01', now())`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO mediator_relay_cursor (topic, partition, last_outbox_id, last_stream_id) VALUES ('t', 0, 1, '1-0')`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO mediator_partition_epoch (consumer_group, topic, partition, epoch) VALUES ('g', 't', 0, 1)`); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `SELECT nextval('mediator_fencing_seq')`); err != nil {
			t.Fatal(err)
		}
	}
	// Every migration applies, reverts, and re-applies on a database seeded
	// at the previous version (6.7).
	for i := len(status) - 1; i >= 0; i-- {
		v := status[i].Version
		if err := pg.MigrateDown(ctx, pool, v-1); err != nil {
			t.Fatalf("down to %d: %v", v-1, err)
		}
		if v-1 == 0 {
			for _, table := range []string{"mediator_outbox", "mediator_stream_seq", "mediator_inbox", "mediator_idempotency", "mediator_relay_cursor", "mediator_schema_version", "mediator_fencing_seq", "mediator_partition_epoch"} {
				if tableExists(t, pool, table) {
					t.Fatalf("%s still exists after down", table)
				}
			}
		}
		if err := pg.Migrate(ctx, pool); err != nil {
			t.Fatalf("up after down to %d: %v", v-1, err)
		}
		seed()
		if err := pg.MigrateDown(ctx, pool, v-1); err != nil {
			t.Fatalf("down on seeded: %v", err)
		}
		if err := pg.Migrate(ctx, pool); err != nil {
			t.Fatalf("up on seeded: %v", err)
		}
	}
	if n := count(t, pool, `SELECT count(*) FROM mediator_outbox`); n != 0 {
		t.Fatalf("re-applied schema must be empty, got %d rows", n)
	}
	if err := pg.MigrateDown(ctx, pool, 99); err != nil {
		t.Fatalf("down above the current version is a no-op: %v", err)
	}
	status, _ = pg.MigrationStatus(ctx, pool)
	if !status[0].Applied {
		t.Fatal("status after re-apply")
	}
	// Concurrent migrations serialize on the advisory lock and both succeed.
	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = pg.Migrate(ctx, pool)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestIntegration_StoreConformance(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T) pg.Store {
		pool, _ := newSchema(t, true)
		return pg.NewStore(pool, pg.StoreConfig{Partitions: 4})
	})
}

func TestIntegration_StoreSchemaOption(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, name := newSchema(t, true)
	// A pool without search_path reaches the tables through StoreConfig.Schema.
	pool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{MaxConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	store := pg.NewStore(pool, pg.StoreConfig{Schema: name, Partitions: 2})
	if store.Partitions() != 2 || store.Pool() != pool {
		t.Fatal("store config")
	}
	if err := store.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	dctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err = pg.WithTx(dctx, store, pg.TxOptions{}, func(ctx context.Context) error {
		tx, ok := pg.TxFrom(ctx)
		if !ok {
			return fmt.Errorf("TxFrom must expose the pgx transaction")
		}
		var lock, idle string
		if err := tx.QueryRow(ctx, "SELECT current_setting('lock_timeout'), current_setting('idle_in_transaction_session_timeout')").Scan(&lock, &idle); err != nil {
			return err
		}
		if lock != "5s" || idle == "0" {
			return fmt.Errorf("lock_timeout=%s idle=%s", lock, idle)
		}
		_, err := tx.Exec(ctx, "INSERT INTO mediator_inbox (consumer_group, event_id) VALUES ('g', gen_random_uuid())")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(ctx, pg.TxOptions{Isolation: "nope"}); err == nil {
		t.Fatal("bad isolation must fail before touching the pool")
	}
	token, err := store.NextFencingToken(ctx)
	if err != nil || token < 1 {
		t.Fatalf("fencing: %d %v", token, err)
	}
}
