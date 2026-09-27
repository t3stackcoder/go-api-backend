//go:build integration && faultinject

package pg_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

// TestFault_PointsReturnTheInjectedError arms every Postgres fault point of
// the catalogue with the error kind (and the mark point with the ambiguous
// kind) and checks the driver returns the injected error from the call the
// point guards, so a sweep cell fails where its point says. Fault schedules
// are process-wide, so the test does not run in parallel.
func TestFault_PointsReturnTheInjectedError(t *testing.T) {
	ctx := context.Background()
	pool, _ := newSchema(t, true)
	store := pg.NewStore(pool, pg.StoreConfig{})
	t.Cleanup(testkit.Disarm)
	arm := func(point string, kind testkit.FaultKind) {
		testkit.Arm(testkit.Schedule{Point: point, Kind: kind})
	}
	wantInjected := func(t *testing.T, point string, err error) {
		t.Helper()
		if !errors.Is(err, testkit.ErrInjected) {
			t.Fatalf("%s: want the injected error, got %v", point, err)
		}
	}
	inTx := func(t *testing.T, point string, op func(tx pg.Tx) error) {
		t.Helper()
		testkit.Disarm()
		tx, err := store.Begin(ctx, pg.TxOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx) //nolint:errcheck // cleanup
		arm(point, testkit.FaultError)
		wantInjected(t, point, op(tx))
	}
	env := func() *mediator.Envelope {
		return &mediator.Envelope{ID: mediator.NewID(time.Now()), Topic: "t", StreamKey: "k", Type: "E", OccurredAt: time.Now().UTC()}
	}

	inTx(t, "pg.tx.commit", func(tx pg.Tx) error { return tx.Commit(ctx) })
	inTx(t, "pg.outbox.seq", func(tx pg.Tx) error { return tx.OutboxAppend(ctx, env(), []byte(`{}`)) })
	inTx(t, "pg.outbox.insert", func(tx pg.Tx) error { return tx.OutboxAppend(ctx, env(), []byte(`{}`)) })
	inTx(t, "pg.inbox.insert", func(tx pg.Tx) error { _, err := tx.InboxInsert(ctx, "g", mediator.NewID(time.Now())); return err })
	inTx(t, "pg.inbox.fence", func(tx pg.Tx) error { _, err := tx.FencePartition(ctx, "g", "t", 0, 1); return err })
	inTx(t, "pg.idem.reserve", func(tx pg.Tx) error { _, err := tx.IdempotencyReserve(ctx, "s", "k", []byte{1}, time.Hour); return err })
	inTx(t, "pg.idem.store", func(tx pg.Tx) error { return tx.IdempotencyStore(ctx, "s", "k", []byte(`1`)) })
	inTx(t, "pg.outbox.notify", func(tx pg.Tx) error { return tx.Notify(ctx, pg.NotifyChannel, "t:0") })

	arm("pg.tx.begin", testkit.FaultError)
	_, err := store.Begin(ctx, pg.TxOptions{})
	wantInjected(t, "pg.tx.begin", err)
	arm("pg.lease.epoch", testkit.FaultError)
	_, err = store.NextFencingToken(ctx)
	wantInjected(t, "pg.lease.epoch", err)

	rs := pg.NewPgSlotStoreForTest(pool)
	arm("pg.relay.select", testkit.FaultError)
	_, err = rs.BeginBatch(ctx, "t", 0, 10)
	wantInjected(t, "pg.relay.select", err)
	arm("pg.relay.cursor", testkit.FaultError)
	wantInjected(t, "pg.relay.cursor", rs.SaveCursor(ctx, "t", 0, 1, "1-0"))

	// A batch of one row: the mark point before and after the UPDATE, and
	// the cursor point inside Mark. Each batch locks the row FOR UPDATE, so
	// it is rolled back before the next one selects with SKIP LOCKED.
	execAll(t, pool, sqlOutboxRow)
	batch := func(t *testing.T) pg.RelayBatch {
		t.Helper()
		testkit.Disarm()
		b, err := rs.BeginBatch(ctx, "t", 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Rollback(ctx) })
		if len(b.Entries()) != 1 {
			t.Fatalf("%d entries", len(b.Entries()))
		}
		return b
	}
	release := func(t *testing.T, b pg.RelayBatch) {
		t.Helper()
		testkit.Disarm()
		if err := b.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
	b := batch(t)
	arm("pg.relay.mark", testkit.FaultError)
	wantInjected(t, "pg.relay.mark", b.Mark(ctx, "1-0"))
	release(t, b)
	b = batch(t)
	arm("pg.relay.mark", testkit.FaultAmbiguous)
	if err := b.Mark(ctx, "1-0"); !errors.Is(err, testkit.ErrInjectedAmbiguous) {
		t.Fatalf("pg.relay.mark ambiguous: %v", err)
	}
	release(t, b)
	b = batch(t)
	arm("pg.relay.cursor", testkit.FaultError)
	wantInjected(t, "pg.relay.cursor in Mark", b.Mark(ctx, "1-0"))
	release(t, b)

	arm("pg.janitor.delete", testkit.FaultError)
	_, err = pg.NewJanitor(pool, pg.JanitorConfig{}).Sweep(ctx)
	wantInjected(t, "pg.janitor.delete", err)

	arm("pg.migrate.apply", testkit.FaultError)
	wantInjected(t, "pg.migrate.apply (down)", pg.MigrateDown(ctx, pool, 0))
	fresh, _ := newSchema(t, false)
	arm("pg.migrate.apply", testkit.FaultError)
	wantInjected(t, "pg.migrate.apply (up)", pg.Migrate(ctx, fresh))
}

// TestFault_RelayLockFaultIsRetried: a fault at pg.relay.lock skips the
// slot for that pass and the next pass acquires it.
func TestFault_RelayLockFaultIsRetried(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, schema := newSchema(t, true)
	topic := "evt_" + schema
	testkit.Arm(testkit.Schedule{Point: "pg.relay.lock", Kind: testkit.FaultError, Hit: 1})
	t.Cleanup(testkit.Disarm)
	h := &recordingHandler{}
	relay := pg.NewRelay(pool, memstore.NewStreams(nil), pg.RelayConfig{
		Topics: []string{topic}, Partitions: 1, PollInterval: 30 * time.Millisecond, Logger: slog.New(h),
	})
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	eventually(t, 10*time.Second, "slot acquired on a later pass", func() bool {
		st := relay.Stats()
		return len(st.Slots) == 1 && st.Slots[0].Owned
	})
	if !h.has("relay: lock") || testkit.Hits()["pg.relay.lock"] < 2 {
		t.Fatalf("lock fault not logged (%v) or not retried (%v)", h.msgs, testkit.Hits())
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
