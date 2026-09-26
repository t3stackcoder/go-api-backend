//go:build integration

package pg_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// counterCmd inserts a row and reports how many times it ran; the sleep
// widens the window in which a concurrent duplicate must block.
type counterCmd struct {
	mediator.Command[thingResult]
	Name string `json:"name"`
	Key  string `json:"key"`
}

func (c counterCmd) IdempotencyKey() string { return c.Key }

type g8Harness struct {
	m     *mediator.Mediator
	calls atomic.Int32
	hold  time.Duration
}

func newG8(t *testing.T, uowCfg pg.UnitOfWorkConfig) (*g8Harness, *pg.PgStore) {
	pool, _ := newSchema(t, true)
	if _, err := pool.Exec(context.Background(), `CREATE TABLE things (name text PRIMARY KEY, n int NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: 2})
	h := &g8Harness{hold: 300 * time.Millisecond}
	h.m = build(t, func(m *mediator.Mediator) {
		must(t, mediator.HandleFunc(m, func(ctx context.Context, c counterCmd) (thingResult, error) {
			n := h.calls.Add(1)
			tx, ok := pg.TxFrom(ctx)
			if !ok {
				return thingResult{}, errors.New("no pgx tx")
			}
			if _, err := tx.Exec(ctx, `INSERT INTO things (name, n) VALUES ($1, $2)`, c.Name, n); err != nil {
				return thingResult{}, err
			}
			time.Sleep(h.hold)
			return thingResult{ID: c.Name}, nil
		}))
	}, pg.UnitOfWork(store, uowCfg), pg.Idempotency(pg.IdempotencyConfig{}))
	return h, store
}

// TestIntegration_IdempotencyRace_G8: two goroutines race on one key; the
// handler executes once, both receive the same response, and the state row
// exists once.
func TestIntegration_IdempotencyRace_G8(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h, store := newG8(t, pg.UnitOfWorkConfig{})
	results := make([]thingResult, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = mediator.Send(ctx, h.m, counterCmd{Name: "a", Key: "k"})
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("goroutine %d: %v", i, err)
		}
	}
	if results[0] != results[1] || results[0].ID != "a" {
		t.Fatalf("responses differ: %+v %+v", results[0], results[1])
	}
	if h.calls.Load() != 1 {
		t.Fatalf("handler ran %d times", h.calls.Load())
	}
	if n := count(t, store.Pool(), `SELECT count(*) FROM things`); n != 1 {
		t.Fatalf("%d state rows", n)
	}
	info, err := pg.IdemShow(ctx, store.Pool(), "counterCmd", "k")
	if err != nil || info.Hits != 1 || string(info.Response) != `{"id": "a"}` {
		t.Fatalf("idempotency row: %+v %v", info, err)
	}
	// A different payload under the same key is rejected without executing.
	if _, err := mediator.Send(ctx, h.m, counterCmd{Name: "b", Key: "k"}); mediator.CodeOf(err) != mediator.CodeIdempotencyMismatch {
		t.Fatalf("want mismatch, got %v", err)
	}
	if h.calls.Load() != 1 {
		t.Fatal("mismatch executed the handler")
	}
}

// TestIntegration_IdempotencyBusy: with a short lock_timeout the second
// attempt gets CodeIdempotencyBusy from a real 55P03.
func TestIntegration_IdempotencyBusy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h, _ := newG8(t, pg.UnitOfWorkConfig{DefaultLockTimeout: 100 * time.Millisecond})
	h.hold = time.Second
	done := make(chan error, 1)
	go func() {
		_, err := mediator.Send(ctx, h.m, counterCmd{Name: "a", Key: "k"})
		done <- err
	}()
	eventually(t, 5*time.Second, "first attempt to enter the handler", func() bool { return h.calls.Load() == 1 })
	_, err := mediator.Send(ctx, h.m, counterCmd{Name: "a", Key: "k"})
	if mediator.CodeOf(err) != mediator.CodeIdempotencyBusy || !pg.IsLockTimeout(err) || pg.SQLState(err) != "55P03" {
		t.Fatalf("want busy from 55P03, got %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := mediator.Send(ctx, h.m, counterCmd{Name: "a", Key: "k"}); err != nil || h.calls.Load() != 1 {
		t.Fatalf("replay after completion: err=%v calls=%d", err, h.calls.Load())
	}
}

// TestIntegration_NotifyOnCommit: the relay wake-up is delivered exactly
// once per (topic, partition) when the transaction commits, and not at all
// when it rolls back.
func TestIntegration_NotifyOnCommit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool, _ := newSchema(t, true)
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: 1})
	fail := false
	m := build(t, func(m *mediator.Mediator) {
		must(t, mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
			for i := 0; i < 3; i++ {
				if err := mediator.Publish(ctx, m, thingCreated{ID: c.Name}); err != nil {
					return thingResult{}, err
				}
			}
			if fail {
				return thingResult{}, errors.New("boom")
			}
			return thingResult{ID: c.Name}, nil
		}))
	}, uow(store))
	pc, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn := pc.Hijack()
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "LISTEN "+pg.NotifyChannel); err != nil {
		t.Fatal(err)
	}
	fail = true
	if _, err := mediator.Send(ctx, m, createThing{Name: "x"}); err == nil {
		t.Fatal("want failure")
	}
	fail = false
	if _, err := mediator.Send(ctx, m, createThing{Name: "x"}); err != nil {
		t.Fatal(err)
	}
	// The channel is database-wide and other tests publish concurrently, so
	// count only this test's payload over a window.
	mine := 0
	deadline := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(deadline) {
		wctx, cancel := context.WithDeadline(ctx, deadline)
		n, err := conn.WaitForNotification(wctx)
		cancel()
		if err != nil {
			break
		}
		if n.Channel == pg.NotifyChannel && n.Payload == "thingCreated:0" {
			mine++
		}
	}
	if mine != 1 {
		t.Fatalf("got %d notifications for the committed transaction, want exactly one per (topic, partition)", mine)
	}
	if n := count(t, pool, `SELECT count(*) FROM mediator_outbox`); n != 3 {
		t.Fatalf("outbox rows %d, want 3 (the failed attempt rolled back)", n)
	}
	if n := count(t, pool, `SELECT max(seq) FROM mediator_outbox WHERE stream_key = 'x'`); n != 3 {
		t.Fatalf("sequences must be dense after the rollback: max %d", n)
	}
}
