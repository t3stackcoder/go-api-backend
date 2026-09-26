package pg_test

import (
	"context"
	"errors"
	"io"
	"iter"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

// Request and event types shared by the behavior tests.

type thingResult struct {
	ID string `json:"id"`
}

type createThing struct {
	mediator.Command[thingResult]
	Name string `json:"name"`
}

type thingCreated struct {
	mediator.Event
	ID string `json:"id"`
}

func (e thingCreated) StreamKey() string { return e.ID }

type getThing struct {
	mediator.Query[thingResult]
	ID string `json:"id"`
}

type listThings struct {
	mediator.StreamQuery[int]
	N int `json:"n"`
}

type serializableCmd struct {
	mediator.Command[mediator.Void]
}

func (serializableCmd) TxOptions() pg.TxOptions {
	return pg.TxOptions{Isolation: pgx.Serializable, LockTimeout: 2 * time.Second}
}

type requiresNewCmd struct {
	mediator.Command[mediator.Void]
}

func (requiresNewCmd) TxOptions() pg.TxOptions { return pg.TxOptions{Propagation: pg.RequiresNew} }

// build wires behaviors (requests and consumers) and registrations.
func build(t *testing.T, register func(m *mediator.Mediator), behaviors ...mediator.Behavior) *mediator.Mediator {
	t.Helper()
	m := mediator.New()
	for _, b := range behaviors {
		if err := mediator.Use(m, b, mediator.Requests(), mediator.Consumers()); err != nil {
			t.Fatal(err)
		}
	}
	register(m)
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	return m
}

func uow(store pg.Store) mediator.Behavior { return pg.UnitOfWork(store, pg.UnitOfWorkConfig{}) }

func TestUnitOfWork_CommitsAndNotifiesOncePerPartition(t *testing.T) {
	store := memstore.New(memstore.Config{Partitions: 3})
	m := build(t, func(m *mediator.Mediator) {
		mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
			if _, ok := pg.StoreTxFrom(ctx); !ok {
				t.Error("no store tx in handler context")
			}
			if _, ok := pg.TxFrom(ctx); ok {
				t.Error("TxFrom must be false with the in-memory store")
			}
			for i := 0; i < 2; i++ {
				if err := mediator.Publish(ctx, m, thingCreated{ID: c.Name}); err != nil {
					return thingResult{}, err
				}
			}
			return thingResult{ID: c.Name}, nil
		})
	}, uow(store))
	res, err := mediator.Send(context.Background(), m, createThing{Name: "a"})
	if err != nil || res.ID != "a" {
		t.Fatalf("send: %+v %v", res, err)
	}
	if store.Committed() != 1 || store.RolledBack() != 0 {
		t.Fatalf("committed=%d rolledBack=%d", store.Committed(), store.RolledBack())
	}
	rows := store.Outbox()
	if len(rows) != 2 || rows[0].Envelope.Seq != 1 || rows[1].Envelope.Seq != 2 {
		t.Fatalf("outbox: %+v", rows)
	}
	want := memstore.Notification{Channel: pg.NotifyChannel, Payload: "thingCreated:" + strconv.Itoa(mediator.Partition("a", 3))}
	if n := store.Notifications(); len(n) != 1 || n[0] != want {
		t.Fatalf("notifications %+v, want exactly [%+v]", n, want)
	}
}

func TestUnitOfWork_RollsBackOnError(t *testing.T) {
	store := memstore.New(memstore.Config{})
	boom := errors.New("boom")
	m := build(t, func(m *mediator.Mediator) {
		mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
			if err := mediator.Publish(ctx, m, thingCreated{ID: c.Name}); err != nil {
				return thingResult{}, err
			}
			return thingResult{}, boom
		})
	}, uow(store))
	if _, err := mediator.Send(context.Background(), m, createThing{Name: "a"}); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	if store.RolledBack() != 1 || store.Committed() != 0 || len(store.Outbox()) != 0 || len(store.Notifications()) != 0 {
		t.Fatalf("rollback did not discard: rb=%d c=%d outbox=%d notes=%d", store.RolledBack(), store.Committed(), len(store.Outbox()), len(store.Notifications()))
	}
}

func TestUnitOfWork_RollsBackOnPanic(t *testing.T) {
	store := memstore.New(memstore.Config{})
	m := build(t, func(m *mediator.Mediator) {
		mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
			_ = mediator.Publish(ctx, m, thingCreated{ID: c.Name})
			panic("handler exploded")
		})
	}, uow(store))
	_, err := mediator.Send(context.Background(), m, createThing{Name: "a"})
	var pe *mediator.PanicError
	if !errors.As(err, &pe) || mediator.CodeOf(err) != mediator.CodeInternal {
		t.Fatalf("want recovered panic, got %v", err)
	}
	if store.RolledBack() != 1 || len(store.Outbox()) != 0 {
		t.Fatalf("panic must roll back: rb=%d outbox=%d", store.RolledBack(), len(store.Outbox()))
	}
}

func TestUnitOfWork_JoinsAmbientTransaction(t *testing.T) {
	store := memstore.New(memstore.Config{})
	var outerTx, innerTx pg.Tx
	m := build(t, func(m *mediator.Mediator) {
		mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
			outerTx, _ = pg.StoreTxFrom(ctx)
			if _, err := mediator.Send(ctx, m, getThing{ID: c.Name}); err != nil {
				return thingResult{}, err
			}
			if _, err := mediator.Send(ctx, m, requiresNewCmd{}); err != nil {
				return thingResult{}, err
			}
			return thingResult{ID: c.Name}, nil
		})
		mediator.HandleFunc(m, func(ctx context.Context, q getThing) (thingResult, error) {
			innerTx, _ = pg.StoreTxFrom(ctx)
			return thingResult{ID: q.ID}, nil
		})
		mediator.HandleFunc(m, func(ctx context.Context, _ requiresNewCmd) (mediator.Void, error) {
			tx, _ := pg.StoreTxFrom(ctx)
			if tx == outerTx {
				t.Error("RequiresNew must not join the ambient transaction")
			}
			return mediator.Void{}, nil
		})
	}, uow(store))
	if _, err := mediator.Send(context.Background(), m, createThing{Name: "a"}); err != nil {
		t.Fatal(err)
	}
	if outerTx == nil || innerTx != outerTx {
		t.Fatal("nested query must run in the outer transaction")
	}
	if store.Begun() != 2 || store.Committed() != 2 {
		t.Fatalf("begun=%d committed=%d, want 2 and 2 (outer + RequiresNew)", store.Begun(), store.Committed())
	}
}

func TestUnitOfWork_DefaultsByKind(t *testing.T) {
	store := memstore.New(memstore.Config{})
	m := build(t, func(m *mediator.Mediator) {
		mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) { return thingResult{}, nil })
		mediator.HandleFunc(m, func(ctx context.Context, q getThing) (thingResult, error) {
			return thingResult{}, mediator.Publish(ctx, m, thingCreated{ID: "x"})
		})
		mediator.HandleStreamFunc(m, func(ctx context.Context, q listThings) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) { yield(1, nil) }
		})
		mediator.HandleFunc(m, func(ctx context.Context, _ serializableCmd) (mediator.Void, error) { return mediator.Void{}, nil })
		mediator.ConsumeFunc(m, "g", func(ctx context.Context, e thingCreated) error { return nil })
	}, pg.UnitOfWork(store, pg.UnitOfWorkConfig{DefaultLockTimeout: 7 * time.Second}))
	ctx := context.Background()
	if _, err := mediator.Send(ctx, m, createThing{}); err != nil {
		t.Fatal(err)
	}
	if _, err := mediator.Send(ctx, m, getThing{}); !errors.Is(err, mediator.ErrDurablePublishInQuery) {
		t.Fatalf("durable publish in a query: want ErrDurablePublishInQuery, got %v", err)
	}
	for range mediator.Stream(ctx, m, listThings{N: 1}) {
	}
	if _, err := mediator.Send(ctx, m, serializableCmd{}); err != nil {
		t.Fatal(err)
	}
	env := mediator.Envelope{ID: mediator.NewID(time.Now()), Type: "thingCreated", Topic: "thingCreated", StreamKey: "x"}
	if err := m.Deliver(ctx, "g", env, []byte(`{"id":"x"}`)); err != nil {
		t.Fatal(err)
	}
	b := store.Begins()
	want := []pg.TxOptions{
		{Isolation: pgx.ReadCommitted, LockTimeout: 7 * time.Second},
		{Isolation: pgx.RepeatableRead, ReadOnly: true, LockTimeout: 7 * time.Second},
		{Isolation: pgx.RepeatableRead, ReadOnly: true, LockTimeout: 7 * time.Second},
		{Isolation: pgx.Serializable, LockTimeout: 2 * time.Second},
		{Isolation: pgx.ReadCommitted, LockTimeout: 7 * time.Second},
	}
	if len(b) != len(want) {
		t.Fatalf("begins: %+v", b)
	}
	for i := range want {
		if b[i] != want[i] {
			t.Errorf("begin %d: %+v, want %+v", i, b[i], want[i])
		}
	}
	if store.RolledBack() != 1 {
		t.Fatalf("the failed query must roll back: %d", store.RolledBack())
	}
}

func TestResolveTxOptions(t *testing.T) {
	def := 5 * time.Second
	got := pg.ResolveTxOptions(createThing{}, mediator.KindCommand, def)
	if got != (pg.TxOptions{Isolation: pgx.ReadCommitted, LockTimeout: def}) {
		t.Errorf("command: %+v", got)
	}
	got = pg.ResolveTxOptions(getThing{}, mediator.KindQuery, def)
	if got != (pg.TxOptions{Isolation: pgx.RepeatableRead, ReadOnly: true, LockTimeout: def}) {
		t.Errorf("query: %+v", got)
	}
	got = pg.ResolveTxOptions(requiresNewCmd{}, mediator.KindCommand, def)
	if got != (pg.TxOptions{Isolation: pgx.ReadCommitted, Propagation: pg.RequiresNew, LockTimeout: def}) {
		t.Errorf("trait with empty isolation keeps the default: %+v", got)
	}
	got = pg.ResolveTxOptions(serializableCmd{}, mediator.KindCommand, def)
	if got != (pg.TxOptions{Isolation: pgx.Serializable, LockTimeout: 2 * time.Second}) {
		t.Errorf("trait override: %+v", got)
	}
}

func TestUnitOfWork_HooksOrder(t *testing.T) {
	store := memstore.New(memstore.Config{})
	var log []string
	m := build(t, func(m *mediator.Mediator) {
		mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
			pg.BeforeCommit(ctx, func(context.Context) error {
				log = append(log, "before1")
				return nil
			})
			pg.OnCommit(ctx, func(hctx context.Context) {
				log = append(log, "after1")
				pg.OnCommit(hctx, func(context.Context) { log = append(log, "nested") })
			})
			pg.OnCommit(ctx, func(context.Context) { panic("hook panic") })
			pg.BeforeCommit(ctx, func(context.Context) error {
				if store.Committed() != 0 {
					t.Error("before-commit hook ran after commit")
				}
				log = append(log, "before2")
				return nil
			})
			pg.OnCommit(ctx, func(context.Context) {
				if store.Committed() != 1 {
					t.Error("on-commit hook ran before commit")
				}
				log = append(log, "after2")
			})
			return thingResult{}, nil
		})
	}, uow(store))
	if _, err := mediator.Send(context.Background(), m, createThing{}); err != nil {
		t.Fatal(err)
	}
	want := "before1 before2 after1 after2 nested"
	if got := strings.Join(log, " "); got != want {
		t.Fatalf("hook order %q, want %q", got, want)
	}
}

func TestUnitOfWork_BeforeCommitErrorRollsBack(t *testing.T) {
	store := memstore.New(memstore.Config{})
	veto := errors.New("veto")
	afterRan := false
	m := build(t, func(m *mediator.Mediator) {
		mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
			pg.OnCommit(ctx, func(context.Context) { afterRan = true })
			pg.BeforeCommit(ctx, func(context.Context) error { return veto })
			return thingResult{}, nil
		})
	}, uow(store))
	if _, err := mediator.Send(context.Background(), m, createThing{}); !errors.Is(err, veto) {
		t.Fatalf("want veto, got %v", err)
	}
	if store.RolledBack() != 1 || store.Committed() != 0 || afterRan {
		t.Fatalf("rb=%d c=%d afterRan=%v", store.RolledBack(), store.Committed(), afterRan)
	}
}

func TestUnitOfWork_CommitFailureClassification(t *testing.T) {
	cases := []struct {
		name      string
		hooks     memstore.Hooks
		ambiguous bool
		code      mediator.Code
		committed int
	}{
		{"lost acknowledgement", memstore.Hooks{AfterCommit: func() error { return io.ErrUnexpectedEOF }}, true, mediator.CodeUnavailable, 1},
		{"definite non-transient", memstore.Hooks{BeforeCommit: func() error { return errors.New("disk full") }}, false, mediator.CodeInternal, 0},
		{"definite transient", memstore.Hooks{BeforeCommit: func() error { return &pgconn.PgError{Code: "40001"} }}, false, mediator.CodeUnavailable, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := memstore.New(memstore.Config{})
			store.Hooks = tc.hooks
			afterRan := false
			m := build(t, func(m *mediator.Mediator) {
				mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
					pg.OnCommit(ctx, func(context.Context) { afterRan = true })
					return thingResult{}, nil
				})
			}, uow(store))
			_, err := mediator.Send(context.Background(), m, createThing{})
			if err == nil {
				t.Fatal("want an error")
			}
			if pg.IsAmbiguous(err) != tc.ambiguous || mediator.CodeOf(err) != tc.code {
				t.Fatalf("ambiguous=%v code=%s, want %v %s: %v", pg.IsAmbiguous(err), mediator.CodeOf(err), tc.ambiguous, tc.code, err)
			}
			if store.Committed() != tc.committed || afterRan {
				t.Fatalf("committed=%d afterRan=%v", store.Committed(), afterRan)
			}
		})
	}
}

func TestIsDefiniteCommitFailure(t *testing.T) {
	cases := map[error]bool{
		&pgconn.PgError{Code: "40001"}:               true,
		pg.ErrTxAborted:                              true,
		pg.ErrTxClosed:                               true,
		pgx.ErrTxClosed:                              true,
		pgx.ErrTxCommitRollback:                      true,
		io.ErrUnexpectedEOF:                          false,
		context.Canceled:                             false,
		errors.New("read: connection reset by peer"): false,
	}
	for err, want := range cases {
		if got := pg.IsDefiniteCommitFailure(err); got != want {
			t.Errorf("%v: %v, want %v", err, got, want)
		}
	}
}

func TestUnitOfWork_BeginError(t *testing.T) {
	store := memstore.New(memstore.Config{})
	m := build(t, func(m *mediator.Mediator) {
		mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) { return thingResult{}, nil })
	}, uow(store))
	store.Hooks.Begin = func(pg.TxOptions) error { return errors.New("pool exhausted") }
	if _, err := mediator.Send(context.Background(), m, createThing{}); err == nil || mediator.CodeOf(err) != mediator.CodeInternal {
		t.Fatalf("plain begin error: %v", err)
	}
	store.Hooks.Begin = func(pg.TxOptions) error { return &pgconn.PgError{Code: "08006"} }
	if _, err := mediator.Send(context.Background(), m, createThing{}); mediator.CodeOf(err) != mediator.CodeUnavailable {
		t.Fatalf("transient begin error must be CodeUnavailable: %v", err)
	}
	if store.Begun() != 0 {
		t.Fatal("nothing should have begun")
	}
}

func TestUnitOfWork_Stream(t *testing.T) {
	store := memstore.New(memstore.Config{})
	failAt := 0
	m := build(t, func(m *mediator.Mediator) {
		mediator.HandleStreamFunc(m, func(ctx context.Context, q listThings) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) {
				for i := 1; i <= q.N; i++ {
					if _, ok := pg.StoreTxFrom(ctx); !ok {
						yield(0, errors.New("no tx while streaming"))
						return
					}
					if i == failAt {
						yield(0, errors.New("stream failed"))
						return
					}
					if !yield(i, nil) {
						return
					}
				}
			}
		})
		mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
			for range mediator.Stream(ctx, m, listThings{N: 2}) {
			}
			return thingResult{}, nil
		})
	}, uow(store))
	ctx := context.Background()

	seq := mediator.Stream(ctx, m, listThings{N: 3})
	if store.Begun() != 0 {
		t.Fatal("the transaction must open at the first iteration, not at Stream()")
	}
	var got []int
	for v, err := range seq {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if len(got) != 3 || store.Begun() != 1 || store.Committed() != 1 {
		t.Fatalf("items=%v begun=%d committed=%d", got, store.Begun(), store.Committed())
	}

	for range mediator.Stream(ctx, m, listThings{N: 3}) {
		break
	}
	if store.RolledBack() != 1 {
		t.Fatalf("break must roll back: rb=%d", store.RolledBack())
	}

	failAt = 2
	var last error
	for _, err := range mediator.Stream(ctx, m, listThings{N: 3}) {
		last = err
	}
	if last == nil || store.RolledBack() != 2 {
		t.Fatalf("error must be delivered and roll back: err=%v rb=%d", last, store.RolledBack())
	}
	failAt = 0

	if _, err := mediator.Send(ctx, m, createThing{}); err != nil {
		t.Fatal(err)
	}
	if store.Begun() != 4 || store.Committed() != 2 {
		t.Fatalf("a stream inside a command must join it: begun=%d committed=%d", store.Begun(), store.Committed())
	}

	store.Hooks.Begin = func(pg.TxOptions) error { return errors.New("no conn") }
	n := 0
	for _, err := range mediator.Stream(ctx, m, listThings{N: 3}) {
		n++
		if err == nil {
			t.Fatal("begin failure must be the first and only element")
		}
	}
	if n != 1 {
		t.Fatalf("got %d elements, want 1", n)
	}
	store.Hooks.Begin = nil

	store.Hooks.AfterCommit = func() error { return io.ErrUnexpectedEOF }
	for _, err := range mediator.Stream(ctx, m, listThings{N: 1}) {
		last = err
	}
	if !pg.IsAmbiguous(last) {
		t.Fatalf("commit failure must be the terminal element: %v", last)
	}
	store.Hooks.AfterCommit = nil
}

func TestUnitOfWork_StreamPanicInConsumerRollsBackAndRepanics(t *testing.T) {
	store := memstore.New(memstore.Config{})
	sb := pg.UnitOfWork(store, pg.UnitOfWorkConfig{}).(mediator.StreamBehavior)
	info := &mediator.RequestInfo{Kind: mediator.KindStream}
	next := func(ctx context.Context, req any) iter.Seq2[any, error] {
		return func(yield func(any, error) bool) { yield(1, nil) }
	}
	seq := sb.HandleStream(context.Background(), listThings{}, info, next)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic must propagate after the rollback")
			}
		}()
		seq(func(any, error) bool { panic("consumer body") })
	}()
	if store.RolledBack() != 1 || store.Committed() != 0 {
		t.Fatalf("rb=%d c=%d", store.RolledBack(), store.Committed())
	}

	// Hooks around a stream commit.
	var log []string
	next = func(ctx context.Context, req any) iter.Seq2[any, error] {
		return func(yield func(any, error) bool) {
			pg.BeforeCommit(ctx, func(context.Context) error { log = append(log, "before"); return nil })
			pg.OnCommit(ctx, func(context.Context) { log = append(log, "after") })
			yield(1, nil)
		}
	}
	for range sb.HandleStream(context.Background(), listThings{}, info, next) {
	}
	if strings.Join(log, " ") != "before after" || store.Committed() != 1 {
		t.Fatalf("log=%v committed=%d", log, store.Committed())
	}
	next = func(ctx context.Context, req any) iter.Seq2[any, error] {
		return func(yield func(any, error) bool) {
			pg.BeforeCommit(ctx, func(context.Context) error { return errors.New("veto") })
			yield(1, nil)
		}
	}
	var last error
	for _, err := range sb.HandleStream(context.Background(), listThings{}, info, next) {
		last = err
	}
	if last == nil || last.Error() != "veto" || store.RolledBack() != 2 {
		t.Fatalf("before-commit veto on a stream: err=%v rb=%d", last, store.RolledBack())
	}
	if sb.Name() != mediator.NameUnitOfWork {
		t.Fatal("name")
	}
}

func TestWithTx(t *testing.T) {
	store := memstore.New(memstore.Config{})
	ctx := context.Background()
	err := pg.WithTx(ctx, store, pg.TxOptions{}, func(ctx context.Context) error {
		if _, ok := pg.StoreTxFrom(ctx); !ok {
			t.Error("no tx")
		}
		if _, ok := mediator.UnitOfWorkFrom(ctx); !ok {
			t.Error("no mediator unit of work")
		}
		// Required joins; RequiresNew opens a second transaction.
		if err := pg.WithTx(ctx, store, pg.TxOptions{}, func(context.Context) error { return nil }); err != nil {
			return err
		}
		return pg.WithTx(ctx, store, pg.TxOptions{Propagation: pg.RequiresNew}, func(context.Context) error { return nil })
	})
	if err != nil || store.Begun() != 2 || store.Committed() != 2 {
		t.Fatalf("err=%v begun=%d committed=%d", err, store.Begun(), store.Committed())
	}
	boom := errors.New("boom")
	if err := pg.WithTx(ctx, store, pg.TxOptions{}, func(context.Context) error { return boom }); !errors.Is(err, boom) || store.RolledBack() != 1 {
		t.Fatalf("err=%v rb=%d", err, store.RolledBack())
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic must propagate")
			}
		}()
		_ = pg.WithTx(ctx, store, pg.TxOptions{}, func(context.Context) error { panic("x") })
	}()
	if store.RolledBack() != 2 {
		t.Fatalf("panic must roll back: rb=%d", store.RolledBack())
	}
	if _, ok := pg.StoreTxFrom(ctx); ok {
		t.Fatal("no tx outside")
	}
	if _, ok := pg.TxFrom(ctx); ok {
		t.Fatal("no pgx tx outside")
	}
}

func TestHooksOutsideOrAfterUnitOfWork(t *testing.T) {
	ctx := context.Background()
	ran := 0
	pg.OnCommit(ctx, func(context.Context) { ran++ })
	pg.BeforeCommit(ctx, func(context.Context) error { ran++; return errors.New("logged only") })
	if ran != 2 {
		t.Fatalf("hooks outside a unit of work must run immediately: %d", ran)
	}
	store := memstore.New(memstore.Config{})
	var captured context.Context
	_ = pg.WithTx(ctx, store, pg.TxOptions{}, func(ctx context.Context) error {
		captured = ctx
		return nil
	})
	pg.OnCommit(captured, func(context.Context) { ran++ })
	pg.BeforeCommit(captured, func(context.Context) error { ran++; return nil })
	if ran != 4 {
		t.Fatalf("hooks after commit must run immediately: %d", ran)
	}
	_ = pg.WithTx(ctx, store, pg.TxOptions{}, func(ctx context.Context) error {
		captured = ctx
		return errors.New("fail")
	})
	pg.OnCommit(captured, func(context.Context) { ran++ })
	if ran != 4 {
		t.Fatal("hooks after a rollback must be dropped")
	}
}

func TestBeginSQL(t *testing.T) {
	now := time.Unix(1000, 0)
	cases := []struct {
		name     string
		opts     pg.TxOptions
		schema   string
		deadline time.Time
		want     string
	}{
		{"defaults", pg.TxOptions{}, "", time.Time{},
			"BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE; SET LOCAL lock_timeout = 5000"},
		{"read only repeatable", pg.TxOptions{Isolation: pgx.RepeatableRead, ReadOnly: true, LockTimeout: 250 * time.Millisecond}, "", time.Time{},
			"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL lock_timeout = 250"},
		{"deadline", pg.TxOptions{}, "", now.Add(1500 * time.Millisecond),
			"BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE; SET LOCAL lock_timeout = 5000; SET LOCAL idle_in_transaction_session_timeout = 1500"},
		{"expired deadline", pg.TxOptions{}, "", now.Add(-time.Second),
			"BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE; SET LOCAL lock_timeout = 5000; SET LOCAL idle_in_transaction_session_timeout = 1"},
		{"schema", pg.TxOptions{Isolation: pgx.Serializable}, `te"st`, time.Time{},
			`BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE; SET LOCAL lock_timeout = 5000; SET LOCAL search_path = "te""st"`},
		{"sub-millisecond lock timeout", pg.TxOptions{LockTimeout: time.Microsecond}, "", time.Time{},
			"BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE; SET LOCAL lock_timeout = 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := pg.BeginSQL(tc.opts, 5*time.Second, tc.schema, tc.deadline, !tc.deadline.IsZero(), now)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("\n got %s\nwant %s", got, tc.want)
			}
		})
	}
	if _, err := pg.BeginSQL(pg.TxOptions{Isolation: "bogus; DROP TABLE x"}, time.Second, "", time.Time{}, false, now); err == nil {
		t.Fatal("unknown isolation level must be rejected")
	}
}
