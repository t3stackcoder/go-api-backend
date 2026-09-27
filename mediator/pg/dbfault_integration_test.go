//go:build integration

package pg_test

// The error branches of the driver that a healthy database never takes:
// a statement the server rejects while it runs (a trigger that raises, a
// column that no longer scans, a view that fails while it is read), a
// commit the server refuses, and the connection-level failures that only
// a dropped link produces (a lost COMMIT acknowledgement, a failed
// ROLLBACK, an advisory lock that cannot be taken or released), the last
// through testkit/netfault.

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/netfault"
	"github.com/t3stackcoder/go-api-backend/migrations"
)

const (
	sqlBoomFn      = `CREATE FUNCTION boom() RETURNS bigint LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$`
	sqlBoomTextFn  = `CREATE FUNCTION boom_text() RETURNS text LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$`
	sqlBoomTrigger = `CREATE FUNCTION boom_trigger() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'boom'; END $$`
	sqlOutboxRow   = `INSERT INTO mediator_outbox (event_id, topic, stream_key, seq, partition, event_type, payload) VALUES (gen_random_uuid(), 't', 'k', 1, 0, 'E', '{}')`
	sqlCursorRow   = `INSERT INTO mediator_relay_cursor (topic, partition, last_outbox_id, last_stream_id) VALUES ('t', 0, 1, '1-0')`
	// sqlOutboxView replaces the outbox with a view whose one row fails
	// while it is read, so a query fails after it has started to return.
	sqlOutboxView        = `CREATE VIEW mediator_outbox AS SELECT 1::bigint AS id, gen_random_uuid() AS event_id, 't'::text AS topic, 'k'::text AS stream_key, boom() AS seq, 0::smallint AS partition, 'E'::text AS event_type, 1::int AS schema_ver, '{}'::jsonb AS payload, '{}'::jsonb AS headers, now() AS created_at, now() AS published_at`
	sqlMigrateLockStmt   = `SELECT pg_advisory_lock(hashtext('mediator_migrate'))`
	sqlMigrateUnlockStmt = `SELECT pg_advisory_unlock(hashtext('mediator_migrate'))`
	sqlJanitorLockStmt   = `SELECT pg_try_advisory_lock(hashtext('mediator_janitor'))`
	sqlJanitorUnlockStmt = `SELECT pg_advisory_unlock(hashtext('mediator_janitor'))`
)

func execAll(t *testing.T, pool *pgxpool.Pool, stmts ...string) {
	t.Helper()
	for _, s := range stmts {
		if _, err := pool.Exec(context.Background(), s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
}

func wantErr(t *testing.T, err error, fragment string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), fragment) {
		t.Fatalf("want an error containing %q, got %v", fragment, err)
	}
}

// closePool closes pool with a bound, so a connection a failed test still
// holds cannot wedge the run.
func closePool(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	closed := make(chan struct{})
	go func() {
		pool.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Log("pool did not close in time; a connection is still held")
	}
}

// faultPool opens a pool on schema through a netfault.Dialer and warms its
// connection, so the statement under test runs on a connection that
// already exists rather than in a handshake.
func faultPool(t *testing.T, schema string, opts ...func(*pgxpool.Config)) (*pgxpool.Pool, *netfault.Dialer) {
	t.Helper()
	ctx := context.Background()
	d := &netfault.Dialer{}
	pc, err := d.Config(pgURL)
	if err != nil {
		t.Fatal(err)
	}
	pc.ConnConfig.RuntimeParams["search_path"] = schema
	for _, o := range opts {
		o(pc)
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closePool(t, pool) })
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	return pool, d
}

func newEnvelope() *mediator.Envelope {
	return &mediator.Envelope{ID: mediator.NewID(time.Now()), Topic: "t", StreamKey: "k", Type: "E", OccurredAt: time.Now().UTC()}
}

func TestIntegration_TxStatementFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("commit rejected by the server", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlBoomTrigger, `CREATE CONSTRAINT TRIGGER reject AFTER INSERT ON mediator_inbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION boom_trigger()`)
		store := pg.NewStore(pool, pg.StoreConfig{})
		tx, err := store.Begin(ctx, pg.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.InboxInsert(ctx, "g", mediator.NewID(time.Now())); err != nil {
			t.Fatal(err)
		}
		err = tx.Commit(ctx)
		wantErr(t, err, "pg: commit")
		// A deferred trigger fails the COMMIT itself with a SQLSTATE: the
		// outcome is known, nothing was committed.
		if !pg.IsDefiniteCommitFailure(err) || pg.SQLState(err) != "P0001" {
			t.Fatalf("server-side commit failure must be definite: %v (%s)", err, pg.SQLState(err))
		}
		if n := count(t, pool, `SELECT count(*) FROM mediator_inbox`); n != 0 {
			t.Fatalf("%d rows committed", n)
		}
		_ = tx.Rollback(ctx)
	})

	t.Run("commit acknowledgement lost", func(t *testing.T) {
		t.Parallel()
		pool, schema := newSchema(t, true)
		fp, d := faultPool(t, schema)
		store := pg.NewStore(fp, pg.StoreConfig{})
		tx, err := store.Begin(ctx, pg.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		id := mediator.NewID(time.Now())
		if _, err := tx.InboxInsert(ctx, "g", id); err != nil {
			t.Fatal(err)
		}
		d.DropAfterWrite("commit")
		err = tx.Commit(ctx)
		if err == nil || pg.IsDefiniteCommitFailure(err) {
			t.Fatalf("a lost acknowledgement must be an indefinite failure, got %v", err)
		}
		// The server did commit: the row is visible through a fresh connection.
		eventually(t, 5*time.Second, "committed row visible", func() bool {
			return count(t, pool, `SELECT count(*) FROM mediator_inbox WHERE event_id = $1`, id) == 1
		})
	})

	t.Run("rollback fails and the unit of work logs it", func(t *testing.T) {
		t.Parallel()
		_, schema := newSchema(t, true)
		fp, d := faultPool(t, schema)
		store := pg.NewStore(fp, pg.StoreConfig{})
		h := &recordingHandler{}
		m := build(t, func(m *mediator.Mediator) {
			must(t, mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
				d.FailWrite("rollback")
				return thingResult{}, errors.New("handler failed")
			}))
		}, pg.UnitOfWork(store, pg.UnitOfWorkConfig{Logger: slog.New(h)}))
		_, err := mediator.Send(ctx, m, createThing{})
		wantErr(t, err, "handler failed")
		if !h.has("unit of work: rollback failed") {
			t.Fatalf("rollback failure not logged: %v", h.msgs)
		}
		// The dropped connection is replaced: the pool serves again.
		if err := fp.Ping(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("outbox insert rejected", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlBoomTrigger, `CREATE TRIGGER reject BEFORE INSERT ON mediator_outbox FOR EACH ROW EXECUTE FUNCTION boom_trigger()`)
		store := pg.NewStore(pool, pg.StoreConfig{})
		tx, err := store.Begin(ctx, pg.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck // cleanup
		wantErr(t, tx.OutboxAppend(ctx, newEnvelope(), []byte(`{}`)), "outbox insert")
	})

	t.Run("outbox headers unencodable", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		store := pg.NewStore(pool, pg.StoreConfig{})
		tx, err := store.Begin(ctx, pg.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck // cleanup
		env := newEnvelope()
		env.Headers = map[string]string{"k": "\xff"}
		wantErr(t, tx.OutboxAppend(ctx, env, []byte(`{}`)), "encode outbox headers")
	})
}

func TestIntegration_RelayStoreFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("begin on a closed pool", func(t *testing.T) {
		t.Parallel()
		_, schema := newSchema(t, true)
		p2, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: schema, MaxConns: 1})
		if err != nil {
			t.Fatal(err)
		}
		p2.Close()
		_, err = pg.NewPgSlotStoreForTest(p2).BeginBatch(ctx, "t", 0, 10)
		wantErr(t, err, "relay begin")
	})

	t.Run("select fails", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `DROP TABLE mediator_outbox`)
		_, err := pg.NewPgSlotStoreForTest(pool).BeginBatch(ctx, "t", 0, 10)
		wantErr(t, err, "select outbox")
	})

	t.Run("row does not scan", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlOutboxRow, `ALTER TABLE mediator_outbox ALTER COLUMN seq TYPE text`)
		_, err := pg.NewPgSlotStoreForTest(pool).BeginBatch(ctx, "t", 0, 10)
		wantErr(t, err, "scan outbox row")
	})

	t.Run("headers do not decode", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `INSERT INTO mediator_outbox (event_id, topic, stream_key, seq, partition, event_type, payload, headers) VALUES (gen_random_uuid(), 't', 'k', 1, 0, 'E', '{}', '{"h": 5}')`)
		_, err := pg.NewPgSlotStoreForTest(pool).BeginBatch(ctx, "t", 0, 10)
		wantErr(t, err, "decode outbox headers")
	})

	t.Run("rows fail while read", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlBoomFn, `DROP TABLE mediator_outbox`, sqlOutboxView)
		_, err := pg.NewPgSlotStoreForTest(pool).PublishedAfter(ctx, "t", 0, 0, 10)
		wantErr(t, err, "select outbox")
	})

	t.Run("cursor table missing", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `DROP TABLE mediator_relay_cursor`)
		rs := pg.NewPgSlotStoreForTest(pool)
		_, _, err := rs.Cursor(ctx, "t", 0)
		wantErr(t, err, "relay cursor")
		wantErr(t, rs.SaveCursor(ctx, "t", 0, 1, "1-0"), "relay cursor")
	})

	t.Run("gauges fail", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `DROP TABLE mediator_outbox`)
		_, _, err := pg.NewPgSlotStoreForTest(pool).Gauges(ctx, "t", 0)
		wantErr(t, err, "outbox gauges")
	})

	batchOfOne := func(t *testing.T, pool *pgxpool.Pool) pg.RelayBatch {
		t.Helper()
		b, err := pg.NewPgSlotStoreForTest(pool).BeginBatch(ctx, "t", 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(b.Entries()) != 1 {
			t.Fatalf("%d entries", len(b.Entries()))
		}
		t.Cleanup(func() { _ = b.Rollback(ctx) })
		return b
	}

	t.Run("mark rejected", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlOutboxRow, sqlBoomTrigger, `CREATE TRIGGER reject BEFORE UPDATE ON mediator_outbox FOR EACH ROW EXECUTE FUNCTION boom_trigger()`)
		wantErr(t, batchOfOne(t, pool).Mark(ctx, "1-0"), "relay mark")
	})

	t.Run("cursor upsert rejected", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlOutboxRow, sqlBoomTrigger, `CREATE TRIGGER reject BEFORE INSERT ON mediator_relay_cursor FOR EACH ROW EXECUTE FUNCTION boom_trigger()`)
		wantErr(t, batchOfOne(t, pool).Mark(ctx, "1-0"), "relay cursor")
	})

	t.Run("batch rollback fails", func(t *testing.T) {
		t.Parallel()
		_, schema := newSchema(t, true)
		fp, d := faultPool(t, schema)
		b, err := pg.NewPgSlotStoreForTest(fp).BeginBatch(ctx, "t", 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		d.FailWrite("rollback")
		if err := b.Rollback(ctx); err == nil {
			t.Fatal("a rollback the connection cannot deliver must be reported")
		}
	})
}

// TestIntegration_MigrateFailures swaps the migration source, so it does
// not run in parallel with the tests that migrate their schemas.
func TestIntegration_MigrateFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("lock cannot be taken", func(t *testing.T) {
		_, schema := newSchema(t, false)
		fp, d := faultPool(t, schema)
		d.FailWrite(sqlMigrateLockStmt)
		wantErr(t, pg.Migrate(ctx, fp), "migrate: lock")
	})

	t.Run("lock cannot be released", func(t *testing.T) {
		pool, schema := newSchema(t, false)
		fp, d := faultPool(t, schema)
		d.FailWrite(sqlMigrateUnlockStmt)
		wantErr(t, pg.Migrate(ctx, fp), "migrate: unlock")
		if !tableExists(t, pool, "mediator_outbox") {
			t.Fatal("the migrations ran before the unlock failed")
		}
		// The connection holding the lock was dropped with it: a new
		// migration run takes the lock at once.
		if err := pg.Migrate(ctx, pool); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("version table query fails", func(t *testing.T) {
		_, schema := newSchema(t, false)
		fp, d := faultPool(t, schema)
		d.FailWrite("to_regclass")
		wantErr(t, pg.Migrate(ctx, fp), "version table")
	})

	t.Run("versions query fails", func(t *testing.T) {
		pool, _ := newSchema(t, false)
		execAll(t, pool, `CREATE TABLE mediator_schema_version (version int)`)
		wantErr(t, pg.Migrate(ctx, pool), "migrate: versions")
	})

	t.Run("versions do not scan", func(t *testing.T) {
		pool, _ := newSchema(t, false)
		execAll(t, pool, `CREATE TABLE mediator_schema_version (version text, applied_at text)`, `INSERT INTO mediator_schema_version VALUES ('x', 'y')`)
		wantErr(t, pg.Migrate(ctx, pool), "migrate: versions")
	})

	t.Run("transaction cannot begin", func(t *testing.T) {
		_, schema := newSchema(t, false)
		fp, d := faultPool(t, schema)
		d.FailWrite("begin")
		wantErr(t, pg.Migrate(ctx, fp), "migration 0001_init")
	})

	t.Run("forward SQL fails", func(t *testing.T) {
		pool, _ := newSchema(t, false)
		execAll(t, pool, `CREATE TABLE mediator_outbox (x int)`)
		wantErr(t, pg.Migrate(ctx, pool), "migration 0001_init")
		if tableExists(t, pool, "mediator_schema_version") {
			t.Fatal("a failed migration must leave nothing behind")
		}
	})

	t.Run("no down section", func(t *testing.T) {
		src, err := migrations.FS.ReadFile("0001_init.sql")
		if err != nil {
			t.Fatal(err)
		}
		restore := pg.SetMigrationsFS(fstest.MapFS{
			"0001_init.sql":   {Data: src},
			"0002_nodown.sql": {Data: []byte("CREATE TABLE nodown (x int)")},
		})
		defer restore()
		pool, _ := newSchema(t, false)
		if err := pg.Migrate(ctx, pool); err != nil {
			t.Fatal(err)
		}
		wantErr(t, pg.MigrateDown(ctx, pool, 0), "has no down section")
	})

	t.Run("down: applied versions fail", func(t *testing.T) {
		_, schema := newSchema(t, true)
		fp, d := faultPool(t, schema)
		d.FailWrite("to_regclass")
		wantErr(t, pg.MigrateDown(ctx, fp, 0), "version table")
	})

	t.Run("down: forget fails", func(t *testing.T) {
		_, schema := newSchema(t, true)
		fp, d := faultPool(t, schema)
		d.FailWrite("DELETE FROM mediator_schema_version")
		wantErr(t, pg.MigrateDown(ctx, fp, 0), "revert migration 0002")
	})

	t.Run("down skips versions already reverted", func(t *testing.T) {
		pool, _ := newSchema(t, true)
		if err := pg.MigrateDown(ctx, pool, 1); err != nil {
			t.Fatal(err)
		}
		if err := pg.MigrateDown(ctx, pool, 0); err != nil {
			t.Fatal(err)
		}
		if tableExists(t, pool, "mediator_outbox") {
			t.Fatal("everything must be reverted")
		}
	})

	t.Run("status fails", func(t *testing.T) {
		_, schema := newSchema(t, true)
		fp, d := faultPool(t, schema)
		d.FailWrite("to_regclass")
		_, err := pg.MigrationStatus(ctx, fp)
		wantErr(t, err, "version table")
	})
}

type failingTrimmer struct{}

func (failingTrimmer) TrimBefore(context.Context, string, int, time.Time) error {
	return errors.New("redis down")
}

// TestIntegration_JanitorFailures shares the janitor advisory lock with
// TestIntegration_Janitor, so neither runs in parallel.
func TestIntegration_JanitorFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("lock query fails", func(t *testing.T) {
		_, schema := newSchema(t, true)
		fp, d := faultPool(t, schema)
		d.FailWrite(sqlJanitorLockStmt)
		_, err := pg.NewJanitor(fp, pg.JanitorConfig{}).Sweep(ctx)
		wantErr(t, err, "janitor: lock")
	})

	t.Run("unlock fails and the connection is dropped", func(t *testing.T) {
		_, schema := newSchema(t, true)
		fp, d := faultPool(t, schema)
		d.FailWrite(sqlJanitorUnlockStmt)
		j := pg.NewJanitor(fp, pg.JanitorConfig{})
		if _, err := j.Sweep(ctx); err != nil {
			t.Fatalf("the sweep itself succeeded: %v", err)
		}
		if d.Fired() != 1 {
			t.Fatal("the unlock was never attempted")
		}
		// The lock died with its connection: the next sweep is not skipped.
		res, err := j.Sweep(ctx)
		if err != nil || res.Skipped {
			t.Fatalf("sweep after the dropped unlock: %+v %v", res, err)
		}
	})

	t.Run("deletes fail", func(t *testing.T) {
		pool, _ := newSchema(t, true)
		j := pg.NewJanitor(pool, pg.JanitorConfig{})
		for _, table := range []string{"mediator_idempotency", "mediator_inbox", "mediator_outbox"} {
			execAll(t, pool, "DROP TABLE "+table)
			_, err := j.Sweep(ctx)
			wantErr(t, err, "janitor: delete")
		}
	})

	t.Run("trimmer fails", func(t *testing.T) {
		pool, _ := newSchema(t, true)
		res, err := pg.NewJanitor(pool, pg.JanitorConfig{Trimmer: failingTrimmer{}, Topics: []string{"t"}, Partitions: 2}).Sweep(ctx)
		wantErr(t, err, "janitor: trim t:1")
		if res.Trimmed != 0 {
			t.Fatalf("trimmed %d", res.Trimmed)
		}
	})
}

func TestIntegration_OpsFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("stats do not scan", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlOutboxRow, `ALTER TABLE mediator_outbox ALTER COLUMN partition TYPE text`)
		_, err := pg.OutboxStats(ctx, pool)
		wantErr(t, err, "outbox stats")
	})

	t.Run("reshard query fails", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `DROP TABLE mediator_outbox`)
		_, err := pg.OutboxReshard(ctx, pool, 2)
		wantErr(t, err, "reshard")
	})

	t.Run("reshard key does not scan", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `ALTER TABLE mediator_outbox ALTER COLUMN stream_key DROP NOT NULL`,
			`INSERT INTO mediator_outbox (event_id, topic, stream_key, seq, partition, event_type, payload) VALUES (gen_random_uuid(), 't', NULL, 1, 0, 'E', '{}')`)
		_, err := pg.OutboxReshard(ctx, pool, 2)
		wantErr(t, err, "reshard")
	})

	t.Run("reshard rows fail while read", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlBoomTextFn, `DROP TABLE mediator_outbox`, `CREATE VIEW mediator_outbox AS SELECT boom_text() AS stream_key`)
		_, err := pg.OutboxReshard(ctx, pool, 2)
		wantErr(t, err, "reshard")
	})

	t.Run("reshard update rejected", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `INSERT INTO mediator_outbox (event_id, topic, stream_key, seq, partition, event_type, payload) VALUES (gen_random_uuid(), 't', 'k', 1, 7, 'E', '{}')`,
			sqlBoomTrigger, `CREATE TRIGGER reject BEFORE UPDATE ON mediator_outbox FOR EACH ROW EXECUTE FUNCTION boom_trigger()`)
		_, err := pg.OutboxReshard(ctx, pool, 1)
		wantErr(t, err, "reshard")
	})

	t.Run("reshard cursor reset rejected", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlCursorRow, sqlBoomTrigger, `CREATE TRIGGER reject BEFORE DELETE ON mediator_relay_cursor FOR EACH ROW EXECUTE FUNCTION boom_trigger()`)
		_, err := pg.OutboxReshard(ctx, pool, 1)
		wantErr(t, err, "reshard")
	})

	t.Run("reshard commit rejected", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlCursorRow, sqlBoomTrigger, `CREATE CONSTRAINT TRIGGER reject AFTER DELETE ON mediator_relay_cursor DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION boom_trigger()`)
		_, err := pg.OutboxReshard(ctx, pool, 1)
		wantErr(t, err, "reshard")
		if n := count(t, pool, `SELECT count(*) FROM mediator_relay_cursor`); n != 1 {
			t.Fatalf("the cursor must survive the failed commit, %d rows", n)
		}
	})

	t.Run("check partitions with the outbox missing", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `DROP TABLE mediator_outbox`)
		wantErr(t, pg.CheckPartitions(ctx, pool, 1), "check partitions")
	})
}

// TestIntegration_BehaviorStorageFailures drives the inbox and idempotency
// behaviors over the real store with their tables broken, so a storage
// failure that is not a lock timeout is returned as it is.
func TestIntegration_BehaviorStorageFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	next := func(ctx context.Context, req any) (any, error) { return thingResult{ID: "x"}, nil }
	consumer := &mediator.RequestInfo{Kind: mediator.KindConsumer, Group: "g"}
	command := &mediator.RequestInfo{Kind: mediator.KindCommand, Name: "idemCmd", ResponseType: reflect.TypeFor[thingResult]()}

	t.Run("inbox: partition epoch table missing", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `DROP TABLE mediator_partition_epoch`)
		store := pg.NewStore(pool, pg.StoreConfig{})
		env := mediator.Envelope{ID: mediator.NewID(time.Now()), Topic: "t"}
		err := pg.WithTx(mediator.WithFencingToken(mediator.WithEnvelope(ctx, env), 1), store, pg.TxOptions{}, func(ctx context.Context) error {
			_, err := pg.Inbox().Handle(ctx, nil, consumer, next)
			return err
		})
		wantErr(t, err, "fence partition")
	})

	t.Run("inbox: inbox table missing", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `DROP TABLE mediator_inbox`)
		store := pg.NewStore(pool, pg.StoreConfig{})
		env := mediator.Envelope{ID: mediator.NewID(time.Now()), Topic: "t"}
		err := pg.WithTx(mediator.WithEnvelope(ctx, env), store, pg.TxOptions{}, func(ctx context.Context) error {
			_, err := pg.Inbox().Handle(ctx, nil, consumer, next)
			return err
		})
		wantErr(t, err, "inbox insert")
	})

	t.Run("idempotency: table missing", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, `DROP TABLE mediator_idempotency`)
		store := pg.NewStore(pool, pg.StoreConfig{})
		err := pg.WithTx(ctx, store, pg.TxOptions{}, func(ctx context.Context) error {
			_, err := pg.Idempotency(pg.IdempotencyConfig{}).Handle(ctx, idemCmd{Name: "a", Key: "k"}, command, next)
			return err
		})
		wantErr(t, err, "idempotency reserve")
	})

	t.Run("idempotency: store rejected", func(t *testing.T) {
		t.Parallel()
		pool, _ := newSchema(t, true)
		execAll(t, pool, sqlBoomTrigger, `CREATE TRIGGER reject BEFORE UPDATE ON mediator_idempotency FOR EACH ROW EXECUTE FUNCTION boom_trigger()`)
		store := pg.NewStore(pool, pg.StoreConfig{})
		err := pg.WithTx(ctx, store, pg.TxOptions{}, func(ctx context.Context) error {
			_, err := pg.Idempotency(pg.IdempotencyConfig{}).Handle(ctx, idemCmd{Name: "a", Key: "k"}, command, next)
			return err
		})
		wantErr(t, err, "idempotency store")
	})
}

// TestIntegration_RelaySessionLockFailure drops the LISTEN connection at
// the advisory lock query: the session ends, the relay reconnects, and the
// next session owns the slot.
func TestIntegration_RelaySessionLockFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, schema := newSchema(t, true)
	topic := "evt_" + schema
	fp, d := faultPool(t, schema, func(pc *pgxpool.Config) { pc.MaxConns = 2 })
	h := &recordingHandler{}
	relay := pg.NewRelay(fp, memstore.NewStreams(nil), pg.RelayConfig{
		Topics: []string{topic}, Partitions: 1, PollInterval: 50 * time.Millisecond, MinBackoff: 20 * time.Millisecond, Logger: slog.New(h),
	})
	d.FailWrite("SELECT pg_try_advisory_lock(hashtext($1))")
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	eventually(t, 10*time.Second, "session error counted", func() bool { return relay.Stats().Errors > 0 })
	eventually(t, 10*time.Second, "slot owned by the next session", func() bool {
		st := relay.Stats()
		return st.Listening && len(st.Slots) == 1 && st.Slots[0].Owned
	})
	if !h.has("relay: lock") || d.Fired() != 1 {
		t.Fatalf("lock failure not logged (%v) or not fired (%d)", h.msgs, d.Fired())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestIntegration_RelaySessionListenFailure drops the dedicated connection
// at LISTEN: the session fails before any slot, the relay counts it, and
// the next session listens and owns the slot.
func TestIntegration_RelaySessionListenFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, schema := newSchema(t, true)
	topic := "evt_" + schema
	fp, d := faultPool(t, schema, func(pc *pgxpool.Config) { pc.MaxConns = 2 })
	h := &recordingHandler{}
	relay := pg.NewRelay(fp, memstore.NewStreams(nil), pg.RelayConfig{
		Topics: []string{topic}, Partitions: 1, PollInterval: 50 * time.Millisecond, MinBackoff: 20 * time.Millisecond, Logger: slog.New(h),
	})
	d.FailWrite("LISTEN " + pg.NotifyChannel)
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	eventually(t, 10*time.Second, "session error counted", func() bool { return relay.Stats().Errors > 0 })
	eventually(t, 10*time.Second, "next session listens and owns the slot", func() bool {
		st := relay.Stats()
		return st.Listening && len(st.Slots) == 1 && st.Slots[0].Owned
	})
	if !h.has("relay: session ended; reconnecting") || d.Fired() != 1 {
		t.Fatalf("listen failure not logged (%v) or not fired (%d)", h.msgs, d.Fired())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
