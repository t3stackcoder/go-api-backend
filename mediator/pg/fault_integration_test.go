//go:build integration && faultinject

package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// TestFault_RollbackFaultReleasesConnection: an injected failure at
// pg.tx.rollback is what the caller sees, but the transaction is still torn
// down underneath, so the pooled connection comes back. A real failed
// ROLLBACK breaks the connection and pgxpool releases it; a pool of one
// would otherwise wait forever for the abandoned connection
// (SweepConsumer/pool1 at pg.tx.rollback).
func TestFault_RollbackFaultReleasesConnection(t *testing.T) {
	ctx := context.Background()
	_, schema := newSchema(t, true)
	pool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: schema, MaxConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closed := make(chan struct{})
		go func() {
			pool.Close()
			close(closed)
		}()
		select {
		case <-closed:
		case <-time.After(10 * time.Second):
			t.Error("pool of one did not close: a connection is still held")
		}
	})
	t.Cleanup(testkit.Disarm)
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: 1})

	beginPromptly := func(what string) pg.Tx {
		t.Helper()
		bctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		tx, err := store.Begin(bctx, pg.TxOptions{})
		if err != nil {
			t.Fatalf("%s: begin on the pool of one: %v", what, err)
		}
		return tx
	}

	for _, kind := range []testkit.FaultKind{testkit.FaultError, testkit.FaultTimeout} {
		testkit.Arm(testkit.Schedule{Point: "pg.tx.rollback", Kind: kind})
		tx := beginPromptly(string(kind))
		if _, err := tx.InboxInsert(ctx, "g", mediator.NewID(time.Now())); err != nil {
			t.Fatal(err)
		}
		rctx, rcancel := context.WithTimeout(ctx, 200*time.Millisecond)
		err := tx.Rollback(rctx)
		rcancel()
		switch kind {
		case testkit.FaultError:
			if !errors.Is(err, testkit.ErrInjected) {
				t.Fatalf("%s: want the injected error, got %v", kind, err)
			}
		case testkit.FaultTimeout:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s: want the deadline error, got %v", kind, err)
			}
		}
		testkit.Disarm()
		// The connection is back: the next transaction on the pool of one
		// begins at once, and the failed transaction's write is gone.
		next := beginPromptly(string(kind) + " afterwards")
		if err := next.Rollback(ctx); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM mediator_inbox`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("inbox rows after the failed rollbacks: %d %v", n, err)
	}
}
