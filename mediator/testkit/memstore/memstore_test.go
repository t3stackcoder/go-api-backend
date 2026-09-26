package memstore_test

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"pgregory.net/rapid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/pg/storetest"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

func TestConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) pg.Store {
		return memstore.New(memstore.Config{Partitions: 4})
	})
}

func envelope(topic, key string) *mediator.Envelope {
	return &mediator.Envelope{ID: mediator.NewID(time.Now()), Type: "E", Topic: topic, StreamKey: key, SchemaVersion: 1}
}

// TestPropSeqDense is PropSeqDense of spec 11.3: for any interleaving of
// concurrent publishers on one key, the committed sequences are exactly
// 1..n, and ascending outbox id is ascending seq.
func TestPropSeqDense(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		type plan struct {
			appends int
			commit  bool
			delay   time.Duration
		}
		plans := make([]plan, rapid.IntRange(1, 6).Draw(rt, "publishers"))
		for i := range plans {
			plans[i] = plan{
				appends: rapid.IntRange(1, 3).Draw(rt, "appends"),
				commit:  rapid.Bool().Draw(rt, "commit"),
				delay:   time.Duration(rapid.IntRange(0, 2).Draw(rt, "delay")) * time.Millisecond,
			}
		}
		s := memstore.New(memstore.Config{})
		ctx := context.Background()
		var mu sync.Mutex
		var errs []error
		var wg sync.WaitGroup
		for _, p := range plans {
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(p.delay)
				tx, err := s.Begin(ctx, pg.TxOptions{})
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				for i := 0; i < p.appends; i++ {
					if err := tx.OutboxAppend(ctx, envelope("t", "k"), []byte(`{}`)); err != nil {
						mu.Lock()
						errs = append(errs, err)
						mu.Unlock()
					}
				}
				if p.commit {
					err = tx.Commit(ctx)
				} else {
					err = tx.Rollback(ctx)
				}
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if len(errs) > 0 {
			rt.Fatalf("publisher errors: %v", errs)
		}
		want := 0
		for _, p := range plans {
			if p.commit {
				want += p.appends
			}
		}
		rows := s.Outbox()
		if len(rows) != want {
			rt.Fatalf("committed %d rows, want %d", len(rows), want)
		}
		for i, r := range rows {
			if r.Envelope.Seq != int64(i+1) {
				rt.Fatalf("row %d (id %d) has seq %d, want %d", i, r.ID, r.Envelope.Seq, i+1)
			}
		}
		if s.Committed()+s.RolledBack() != len(plans) {
			rt.Fatalf("committed %d + rolled back %d != %d", s.Committed(), s.RolledBack(), len(plans))
		}
	})
}

func TestHooksAndInspection(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(memstore.Config{Partitions: 2})
	if s.Partitions() != 2 || s.Clock() == nil {
		t.Fatal("config not applied")
	}

	s.Hooks.Begin = func(pg.TxOptions) error { return errors.New("no") }
	if _, err := s.Begin(ctx, pg.TxOptions{}); err == nil || err.Error() != "no" {
		t.Fatalf("begin hook: %v", err)
	}
	s.Hooks.Begin = nil

	s.Hooks.BeforeCommit = func() error { return errors.New("boom") }
	tx, _ := s.Begin(ctx, pg.TxOptions{})
	if err := tx.OutboxAppend(ctx, envelope("t", "k"), nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); !errors.Is(err, pg.ErrTxAborted) {
		t.Fatalf("definite commit failure: want ErrTxAborted, got %v", err)
	}
	if len(s.Outbox()) != 0 || s.RolledBack() != 1 || s.Committed() != 0 {
		t.Fatalf("definite failure must apply nothing: outbox=%d rb=%d c=%d", len(s.Outbox()), s.RolledBack(), s.Committed())
	}
	s.Hooks.BeforeCommit = nil

	s.Hooks.AfterCommit = func() error { return errors.New("lost ack") }
	tx, _ = s.Begin(ctx, pg.TxOptions{})
	if err := tx.OutboxAppend(ctx, envelope("t", "k"), nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err == nil || errors.Is(err, pg.ErrTxAborted) {
		t.Fatalf("ambiguous commit: want a plain error, got %v", err)
	}
	if len(s.Outbox()) != 1 || s.Committed() != 1 {
		t.Fatalf("ambiguous failure must apply the commit: outbox=%d c=%d", len(s.Outbox()), s.Committed())
	}
	s.Hooks.AfterCommit = nil

	s.Hooks.Rollback = func() error { return errors.New("rb") }
	tx, _ = s.Begin(ctx, pg.TxOptions{})
	if err := tx.Rollback(ctx); err == nil || err.Error() != "rb" {
		t.Fatalf("rollback hook: %v", err)
	}
	s.Hooks.Rollback = nil
	if s.RolledBack() != 2 || s.Begun() != 3 {
		t.Fatalf("counters rb=%d begun=%d", s.RolledBack(), s.Begun())
	}

	id := uuid.New()
	hash := bytes.Repeat([]byte{7}, 32)
	tx, _ = s.Begin(ctx, pg.TxOptions{LockTimeout: time.Second})
	mtx := tx.(*memstore.Tx)
	if mtx.Options().LockTimeout != time.Second {
		t.Fatal("Options() lost the lock timeout")
	}
	if err := tx.Notify(ctx, "c", "p"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.InboxInsert(ctx, "g", id); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.IdempotencyReserve(ctx, "S", "k", hash, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := tx.IdempotencyStore(ctx, "S", "k", []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := tx.IdempotencyStore(ctx, "S", "unknown", []byte("1")); err != nil {
		t.Fatal("storing for an unknown key must be a no-op")
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if n := s.Notifications(); len(n) != 1 || n[0] != (memstore.Notification{Channel: "c", Payload: "p"}) {
		t.Fatalf("notifications: %+v", n)
	}
	if got := s.Inbox()["g"]; len(got) != 1 || got[0] != id {
		t.Fatalf("inbox: %v", got)
	}
	rows := s.Idempotency()
	row, ok := rows[memstore.IdemKey{Scope: "S", Key: "k"}]
	if !ok || string(row.Response) != "1" || row.Hits != 0 || !bytes.Equal(row.RequestHash, hash) || row.ExpiresAt.Sub(row.CreatedAt) != time.Hour {
		t.Fatalf("idempotency row: %+v", row)
	}
	if len(rows) != 1 {
		t.Fatalf("unknown key must not create a row: %d rows", len(rows))
	}
	if b := s.Begins(); len(b) != 4 || b[3].LockTimeout != time.Second {
		t.Fatalf("begins: %+v", b)
	}

	// A replay in another transaction updates hits and may overwrite the response.
	tx, _ = s.Begin(ctx, pg.TxOptions{})
	r, err := tx.IdempotencyReserve(ctx, "S", "k", hash, time.Hour)
	if err != nil || r.Hits != 1 || string(r.Response) != "1" {
		t.Fatalf("replay: %+v %v", r, err)
	}
	if err := tx.IdempotencyStore(ctx, "S", "k", []byte("2")); err != nil {
		t.Fatal(err)
	}
	r, err = tx.IdempotencyReserve(ctx, "S", "k", hash, time.Hour)
	if err != nil || r.Hits != 2 || string(r.Response) != "2" {
		t.Fatalf("second reserve in one transaction: %+v %v", r, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	row = s.Idempotency()[memstore.IdemKey{Scope: "S", Key: "k"}]
	if row.Hits != 2 || string(row.Response) != "2" {
		t.Fatalf("after replay: %+v", row)
	}

	// Reserving twice in the inserting transaction bumps the pending row.
	tx, _ = s.Begin(ctx, pg.TxOptions{})
	if _, err := tx.IdempotencyReserve(ctx, "S", "new", hash, time.Hour); err != nil {
		t.Fatal(err)
	}
	if r, err := tx.IdempotencyReserve(ctx, "S", "new", hash, time.Hour); err != nil || r.Hits != 1 {
		t.Fatalf("own pending row: %+v %v", r, err)
	}
	_ = tx.Rollback(ctx)

	if n := s.PurgeIdempotency(time.Now().Add(2 * time.Hour)); n != 1 {
		t.Fatalf("purged %d, want 1", n)
	}
	if len(s.Idempotency()) != 0 {
		t.Fatal("purge left rows")
	}

	// A failed statement aborts the transaction: later statements and the
	// commit fail, and the commit counts as a rollback.
	tx, _ = s.Begin(ctx, pg.TxOptions{ReadOnly: true})
	if err := tx.OutboxAppend(ctx, envelope("t", "k"), nil); !pg.IsReadOnly(err) {
		t.Fatalf("want read-only error, got %v", err)
	}
	if err := tx.Notify(ctx, "c", "p"); !errors.Is(err, pg.ErrTxAborted) {
		t.Fatalf("statement after abort: want ErrTxAborted, got %v", err)
	}
	before := s.RolledBack()
	if err := tx.Commit(ctx); !errors.Is(err, pg.ErrTxAborted) {
		t.Fatalf("commit after abort: want ErrTxAborted, got %v", err)
	}
	if s.RolledBack() != before+1 {
		t.Fatal("aborted commit must count as a rollback")
	}
	if _, err := s.NextFencingToken(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestLockTimeoutWithFakeClock(t *testing.T) {
	ctx := context.Background()
	clock := testkit.NewFakeClock(time.Unix(0, 0))
	s := memstore.New(memstore.Config{Clock: clock, DefaultLockTimeout: time.Second})
	hash := bytes.Repeat([]byte{1}, 32)
	tx1, _ := s.Begin(ctx, pg.TxOptions{})
	if _, err := tx1.IdempotencyReserve(ctx, "S", "k", hash, time.Hour); err != nil {
		t.Fatal(err)
	}
	tx2, _ := s.Begin(ctx, pg.TxOptions{})
	done := make(chan error, 1)
	go func() {
		_, err := tx2.IdempotencyReserve(ctx, "S", "k", hash, time.Hour)
		done <- err
	}()
	for i := 0; i < 200; i++ {
		select {
		case err := <-done:
			if !pg.IsLockTimeout(err) {
				t.Fatalf("want lock timeout, got %v", err)
			}
			_ = tx1.Rollback(ctx)
			return
		default:
		}
		clock.Advance(100 * time.Millisecond)
		time.Sleep(time.Millisecond)
	}
	t.Fatal("waiter never timed out")
}

func TestLockWaitCancelled(t *testing.T) {
	s := memstore.New(memstore.Config{})
	ctx := context.Background()
	tx1, _ := s.Begin(ctx, pg.TxOptions{})
	if _, err := tx1.InboxInsert(ctx, "g", uuid.Nil); err != nil {
		t.Fatal(err)
	}
	tx2, _ := s.Begin(ctx, pg.TxOptions{})
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := tx2.InboxInsert(cctx, "g", uuid.Nil)
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter did not observe cancellation")
	}
	_ = tx1.Rollback(ctx)
}

func TestStreams(t *testing.T) {
	ctx := context.Background()
	clock := testkit.NewFakeClock(time.UnixMilli(1000))
	st := memstore.NewStreams(clock)
	if _, _, ok, err := st.Tail(ctx, "t", 0); ok || err != nil {
		t.Fatalf("empty tail ok=%v err=%v", ok, err)
	}
	last, err := st.Append(ctx, "t", 0, []pg.OutboxEntry{{ID: 1}, {ID: 2}})
	if err != nil || last != "1000-1" {
		t.Fatalf("append: %q %v", last, err)
	}
	if e := st.Entries("t", 0); len(e) != 2 || e[0].ID != "1000-0" || e[1].Entry.ID != 2 {
		t.Fatalf("entries: %+v", e)
	}
	if id, oid, ok, _ := st.Tail(ctx, "t", 0); !ok || id != "1000-1" || oid != 2 {
		t.Fatalf("tail: %s %d %v", id, oid, ok)
	}
	clock.Advance(time.Millisecond)
	if last, _ := st.Append(ctx, "t", 0, []pg.OutboxEntry{{ID: 3}}); last != "1001-0" {
		t.Fatalf("id after advance: %s", last)
	}
	if err := st.EnsureGroups(ctx, "t", 0, []string{"g2", "g1"}); err != nil {
		t.Fatal(err)
	}
	if g := st.Groups("t", 0); len(g) != 2 || g[0] != "g1" || g[1] != "g2" {
		t.Fatalf("groups: %v", g)
	}
	if err := st.EnsureGroups(ctx, "u", 0, []string{"g"}); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := st.Tail(ctx, "u", 0); ok {
		t.Fatal("group creation must not add entries")
	}
	if g := st.Groups("u", 0); len(g) != 1 {
		t.Fatalf("groups on created stream: %v", g)
	}
	if err := st.TrimBefore(ctx, "t", 0, time.UnixMilli(1001)); err != nil {
		t.Fatal(err)
	}
	if e := st.Entries("t", 0); len(e) != 1 || e[0].ID != "1001-0" {
		t.Fatalf("after trim: %+v", e)
	}
	if err := st.TrimBefore(ctx, "missing", 0, time.Now()); err != nil {
		t.Fatal(err)
	}
	st.Truncate("t", 0, 5)
	if len(st.Entries("t", 0)) != 1 {
		t.Fatal("truncate above length must keep everything")
	}
	st.Truncate("t", 0, 0)
	if len(st.Entries("t", 0)) != 0 {
		t.Fatal("truncate to zero")
	}
	st.Truncate("missing", 0, 0)
	st.Drop("t", 0)
	if st.Entries("t", 0) != nil || st.Groups("t", 0) != nil {
		t.Fatal("drop must remove the stream")
	}
	if st.Appends() != 2 {
		t.Fatalf("appends %d, want 2", st.Appends())
	}

	st.Hooks.Append = func(topic string, partition int, entries []pg.OutboxEntry) error { return errors.New("xadd down") }
	if _, err := st.Append(ctx, "t", 0, []pg.OutboxEntry{{ID: 9}}); err == nil {
		t.Fatal("append hook error not returned")
	}
	if st.Appends() != 2 || len(st.Entries("t", 0)) != 0 {
		t.Fatal("failed append must add nothing")
	}
	st.Hooks.Append = nil
	st.Hooks.Tail = func(string, int) error { return errors.New("info down") }
	if _, _, _, err := st.Tail(ctx, "t", 0); err == nil {
		t.Fatal("tail hook error not returned")
	}
	st.Hooks.Tail = nil

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := st.Append(canceled, "t", 0, nil); err == nil {
		t.Fatal("append with canceled context")
	}
	if _, _, _, err := st.Tail(canceled, "t", 0); err == nil {
		t.Fatal("tail with canceled context")
	}
	if err := st.EnsureGroups(canceled, "t", 0, nil); err == nil {
		t.Fatal("ensure groups with canceled context")
	}
	if err := st.TrimBefore(canceled, "t", 0, time.Now()); err == nil {
		t.Fatal("trim with canceled context")
	}
	if memstore.NewStreams(nil) == nil {
		t.Fatal("nil clock must default")
	}
}

func TestOutboxOrderedByID(t *testing.T) {
	s := memstore.New(memstore.Config{})
	ctx := context.Background()
	tx1, _ := s.Begin(ctx, pg.TxOptions{})
	tx2, _ := s.Begin(ctx, pg.TxOptions{})
	if err := tx1.OutboxAppend(ctx, envelope("t", "a"), nil); err != nil {
		t.Fatal(err)
	}
	if err := tx2.OutboxAppend(ctx, envelope("t", "b"), nil); err != nil {
		t.Fatal(err)
	}
	_ = tx2.Commit(ctx)
	_ = tx1.Commit(ctx)
	rows := s.Outbox()
	if !sort.SliceIsSorted(rows, func(i, j int) bool { return rows[i].ID < rows[j].ID }) {
		t.Fatal("Outbox() must be ordered by id")
	}
	if rows[0].Envelope.StreamKey != "a" {
		t.Fatal("ids are assigned at append time")
	}
}
