package pg_test

// Failure branches reachable without Postgres: a malformed migration
// source, a relay whose pool is gone, a slot canceled while failing, the
// janitor loop logging a failed sweep, the unit of work logging a failed
// rollback and a hook panic, and the codecs of the idempotency
// behavior.

import (
	"context"
	"errors"
	"io/fs"
	"iter"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

// recordingHandler is a slog.Handler that keeps every message and runs a
// hook per record, so a test can react to a log line.
type recordingHandler struct {
	mu   sync.Mutex
	msgs []string
	on   func(slog.Record)
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.msgs = append(h.msgs, r.Message)
	h.mu.Unlock()
	if h.on != nil {
		h.on(r)
	}
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) has(msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.msgs {
		if m == msg {
			return true
		}
	}
	return false
}

// errFS fails every Open, so listing the migrations fails.
type errFS struct{}

func (errFS) Open(string) (fs.File, error) { return nil, errors.New("disk on fire") }

// unreadableFS lists its files but cannot open one of them.
type unreadableFS struct {
	inner fstest.MapFS
	bad   string
}

func (u unreadableFS) Open(name string) (fs.File, error) {
	if name == u.bad {
		return nil, errors.New("unreadable")
	}
	return u.inner.Open(name)
}

func TestLoadMigrations_SourceFailures(t *testing.T) {
	if _, err := pg.LoadMigrations(errFS{}); err == nil || !strings.Contains(err.Error(), "read migrations") {
		t.Fatalf("unlistable source: %v", err)
	}
	u := unreadableFS{inner: fstest.MapFS{"0001_init.sql": {Data: []byte("CREATE TABLE x (y int)")}}, bad: "0001_init.sql"}
	if _, err := pg.LoadMigrations(u); err == nil || !strings.Contains(err.Error(), "read migration 0001_init.sql") {
		t.Fatalf("unreadable file: %v", err)
	}
	// The three entry points surface the loading error before touching the pool.
	restore := pg.SetMigrationsFS(errFS{})
	defer restore()
	ctx := context.Background()
	if err := pg.Migrate(ctx, nil); err == nil {
		t.Fatal("Migrate must fail on an unlistable source")
	}
	if err := pg.MigrateDown(ctx, nil, 0); err == nil {
		t.Fatal("MigrateDown must fail on an unlistable source")
	}
	if _, err := pg.MigrationStatus(ctx, nil); err == nil {
		t.Fatal("MigrationStatus must fail on an unlistable source")
	}
}

func TestApplyPoolConfig_CreatesRuntimeParams(t *testing.T) {
	pc, err := pgxpool.ParseConfig("postgres://u:p@localhost:5432/db")
	if err != nil {
		t.Fatal(err)
	}
	pc.ConnConfig.RuntimeParams = nil
	pg.ApplyPoolConfig(pc, pg.PoolConfig{SearchPath: "s"})
	if pc.ConnConfig.RuntimeParams["search_path"] != "s" || pc.ConnConfig.RuntimeParams["application_name"] != "mediator" {
		t.Fatalf("runtime params: %v", pc.ConnConfig.RuntimeParams)
	}
}

func TestCompareStreamID_UnparsableFallsBackToStringOrder(t *testing.T) {
	if pg.CompareStreamID("abc-1", "abd-1") >= 0 || pg.CompareStreamID("2-x", "2-1") <= 0 {
		t.Fatal("unparsable IDs compare as strings")
	}
}

func TestEncodeOutboxHeaders_RejectsInvalidUTF8(t *testing.T) {
	_, err := pg.EncodeOutboxHeaders(&mediator.Envelope{Headers: map[string]string{"k": "\xff"}})
	if err == nil || !strings.Contains(err.Error(), "encode outbox headers") {
		t.Fatalf("invalid UTF-8 in a header: %v", err)
	}
}

func TestJanitor_RunLogsFailedSweep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	h := &recordingHandler{on: func(slog.Record) { cancel() }}
	// The retention check fails before the pool is touched, so no pool is needed.
	j := pg.NewJanitor(nil, pg.JanitorConfig{InboxRetention: time.Hour, OutboxRetention: 2 * time.Hour, Interval: time.Hour, Logger: slog.New(h)})
	if err := j.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !h.has("janitor: sweep failed") || j.Healthy() == nil || j.Sweeps() != 1 {
		t.Fatalf("failed sweep: logged=%v healthy=%v sweeps=%d", h.msgs, j.Healthy(), j.Sweeps())
	}
}

func TestRelay_RunRetriesAfterSessionFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pc, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:1/db")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		t.Fatal(err)
	}
	pool.Close() // every Acquire fails at once, without dialing
	clock := testkit.NewFakeClock(time.Now())
	h := &recordingHandler{}
	r := pg.NewRelay(pool, memstore.NewStreams(nil), pg.RelayConfig{Topics: []string{"t"}, Clock: clock, Logger: slog.New(h)})
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for r.Stats().Errors == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the failed session was not counted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if r.Healthy() == nil || !h.has("relay: session ended; reconnecting") {
		t.Fatalf("healthy=%v logged=%v", r.Healthy(), h.msgs)
	}
	cancel() // parked in the backoff wait: cancellation ends Run cleanly
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestSlot_RunStopsWhenCanceledDuringFailure(t *testing.T) {
	t.Run("canceled inside the failing batch", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			store := &fakeSlotStore{failBegin: errors.New("db down")}
			store.onBegin = cancel
			s := newSlot(store, memstore.NewStreams(nil), pg.RelayConfig{})
			s.Run(ctx)
			if s.Errors() != 0 {
				t.Fatal("a failure after cancellation is not counted")
			}
		})
	})
	t.Run("canceled during the backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			store := &fakeSlotStore{failBegin: errors.New("db down"), failCursor: errors.New("cursor gone")}
			s := newSlot(store, memstore.NewStreams(nil), pg.RelayConfig{MinBackoff: time.Hour})
			done := make(chan struct{})
			go func() {
				defer close(done)
				s.Run(ctx)
			}()
			synctest.Wait()
			// The initial check failed on the cursor, then the batch failed.
			if s.Errors() != 2 || !strings.Contains(s.Stats().LastError, "db down") {
				t.Fatalf("errors=%d stats=%+v", s.Errors(), s.Stats())
			}
			cancel()
			<-done
		})
	})
}

func TestSlot_ReplayWithNothingAfterTheTail(t *testing.T) {
	// The stream is gone but every published row is at or below the tail's
	// outbox ID: nothing to replay, the cursor stays, the groups are recreated.
	store := &fakeSlotStore{cursor: &pg.RelayCursor{LastOutboxID: 3, LastStreamID: "50-0"}}
	sink := memstore.NewStreams(nil)
	s := newSlot(store, sink, pg.RelayConfig{KnownGroups: func() []string { return []string{"proj"} }})
	n, err := s.CheckDataLoss(context.Background())
	if err != nil || n != 0 || s.Replayed() != 0 {
		t.Fatalf("replayed %d (%d) %v", n, s.Replayed(), err)
	}
	if store.cursor.LastStreamID != "50-0" {
		t.Fatalf("cursor moved: %+v", store.cursor)
	}
	if g := sink.Groups("t", 0); len(g) != 1 || g[0] != "proj" {
		t.Fatalf("groups: %v", g)
	}
}

func TestUnitOfWork_AppendOutboxRejectsReadOnly(t *testing.T) {
	store := memstore.New(memstore.Config{})
	err := pg.WithTx(context.Background(), store, pg.TxOptions{ReadOnly: true}, func(ctx context.Context) error {
		u, ok := mediator.UnitOfWorkFrom(ctx)
		if !ok {
			return errors.New("no unit of work in context")
		}
		return u.AppendOutbox(ctx, &mediator.Envelope{ID: mediator.NewID(time.Now()), Topic: "t", StreamKey: "k"}, []byte(`{}`))
	})
	if !errors.Is(err, mediator.ErrDurablePublishInQuery) {
		t.Fatalf("durable append in a read-only unit of work: %v", err)
	}
}

func TestUnitOfWork_StreamBeginError(t *testing.T) {
	store := memstore.New(memstore.Config{})
	m := build(t, func(m *mediator.Mediator) {
		must(t, mediator.HandleStreamFunc(m, func(ctx context.Context, q listThings) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) { yield(1, nil) }
		}))
	}, uow(store))
	store.Hooks.Begin = func(pg.TxOptions) error { return &pgconn.PgError{Code: "08006"} }
	var got error
	n := 0
	for _, err := range mediator.Stream(context.Background(), m, listThings{N: 1}) {
		got = err
		n++
	}
	if n != 1 || mediator.CodeOf(got) != mediator.CodeUnavailable {
		t.Fatalf("stream with a failing begin: %d items, %v", n, got)
	}
	if store.Begun() != 0 {
		t.Fatal("nothing should have begun")
	}
}

// unencodableResult cannot be stored by the idempotency behavior once it
// holds a value; the zero value still round-trips, which registration checks.
type unencodableResult struct{ V int }

func (r unencodableResult) MarshalJSON() ([]byte, error) {
	if r.V == 0 {
		return []byte(`{"v":0}`), nil
	}
	return nil, errors.New("cannot encode")
}

func (r *unencodableResult) UnmarshalJSON([]byte) error { return nil }

type unencodableCmd struct {
	mediator.Command[unencodableResult]
	Key string `json:"key"`
}

func (c unencodableCmd) IdempotencyKey() string { return c.Key }

// undecodableResult stores fine and refuses to be read back once it holds
// a value; the zero value still round-trips, which registration checks.
type undecodableResult struct {
	V int `json:"v"`
}

func (r *undecodableResult) UnmarshalJSON(b []byte) error {
	if string(b) == `{"v":0}` {
		return nil
	}
	return errors.New("stored response is unreadable")
}

type undecodableCmd struct {
	mediator.Command[undecodableResult]
	Key string `json:"key"`
}

func (c undecodableCmd) IdempotencyKey() string { return c.Key }

func TestIdempotency_ResponseCodecFailures(t *testing.T) {
	store := memstore.New(memstore.Config{})
	m := build(t, func(m *mediator.Mediator) {
		must(t, mediator.HandleFunc(m, func(ctx context.Context, c unencodableCmd) (unencodableResult, error) {
			return unencodableResult{V: 1}, nil
		}))
		must(t, mediator.HandleFunc(m, func(ctx context.Context, c undecodableCmd) (undecodableResult, error) {
			return undecodableResult{V: 1}, nil
		}))
	}, uow(store), pg.Idempotency(pg.IdempotencyConfig{}))
	ctx := context.Background()
	_, err := mediator.Send(ctx, m, unencodableCmd{Key: "k1"})
	if mediator.CodeOf(err) != mediator.CodeInternal || !strings.Contains(err.Error(), "encode response") {
		t.Fatalf("unencodable response: %v", err)
	}
	if _, err := mediator.Send(ctx, m, undecodableCmd{Key: "k2"}); err != nil {
		t.Fatalf("first execution stores the response: %v", err)
	}
	_, err = mediator.Send(ctx, m, undecodableCmd{Key: "k2"})
	if mediator.CodeOf(err) != mediator.CodeInternal || !strings.Contains(err.Error(), "decode stored response") {
		t.Fatalf("replay of an undecodable response: %v", err)
	}
}

func TestIdempotency_ReplayWithoutResponseType(t *testing.T) {
	// A request info without a response type (an adapter that registers by
	// name) replays as a nil response rather than decoding into nothing.
	store := memstore.New(memstore.Config{})
	ctx := context.Background()
	b := pg.Idempotency(pg.IdempotencyConfig{})
	info := &mediator.RequestInfo{Kind: mediator.KindCommand, Name: "idemCmd"}
	next := func(ctx context.Context, req any) (any, error) { return thingResult{ID: "x"}, nil }
	for i, want := range []any{thingResult{ID: "x"}, nil} {
		err := pg.WithTx(ctx, store, pg.TxOptions{}, func(ctx context.Context) error {
			res, err := b.Handle(ctx, idemCmd{Name: "a", Key: "k"}, info, next)
			if err != nil {
				return err
			}
			if res != want {
				t.Fatalf("call %d: got %v, want %v", i, res, want)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestBeforeCommit_OutsideUnitOfWorkLogsOnlyAFailure: outside a unit of
// work the hook runs at once, and only an error reaches the default logger.
func TestBeforeCommit_OutsideUnitOfWorkLogsOnlyAFailure(t *testing.T) {
	const msg = "unit of work: before-commit hook outside a transaction failed"
	h := &recordingHandler{}
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })
	ran := 0
	pg.BeforeCommit(context.Background(), func(context.Context) error { ran++; return nil })
	if ran != 1 || h.has(msg) {
		t.Fatalf("a succeeding hook must run once and log nothing: ran=%d logged=%v", ran, h.msgs)
	}
	pg.BeforeCommit(context.Background(), func(context.Context) error { ran++; return errors.New("veto") })
	if ran != 2 || !h.has(msg) {
		t.Fatalf("a failing hook must run once and be logged: ran=%d logged=%v", ran, h.msgs)
	}
}

// TestUnitOfWork_LogsRollbackFailure: a ROLLBACK that fails is logged
// through the configured logger; a clean one is not.
func TestUnitOfWork_LogsRollbackFailure(t *testing.T) {
	const msg = "unit of work: rollback failed"
	boom := errors.New("boom")
	cases := []struct {
		name     string
		rollback func() error
		want     bool
	}{
		{"rollback succeeds", nil, false},
		{"rollback fails", func() error { return errors.New("conn lost") }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := memstore.New(memstore.Config{})
			store.Hooks.Rollback = tc.rollback
			h := &recordingHandler{}
			m := build(t, func(m *mediator.Mediator) {
				must(t, mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) { return thingResult{}, boom }))
			}, pg.UnitOfWork(store, pg.UnitOfWorkConfig{Logger: slog.New(h)}))
			if _, err := mediator.Send(context.Background(), m, createThing{}); !errors.Is(err, boom) {
				t.Fatalf("want boom, got %v", err)
			}
			if store.RolledBack() != 1 || h.has(msg) != tc.want {
				t.Fatalf("rolledBack=%d logged=%v, want logged=%v", store.RolledBack(), h.msgs, tc.want)
			}
		})
	}
}

// TestUnitOfWork_LogsOnCommitHookPanic: a hook panic is logged and the
// remaining hooks still run; a hook that returns logs nothing.
func TestUnitOfWork_LogsOnCommitHookPanic(t *testing.T) {
	const msg = "unit of work: on-commit hook panicked"
	cases := []struct {
		name string
		hook func(context.Context)
		want bool
	}{
		{"hook returns", func(context.Context) {}, false},
		{"hook panics", func(context.Context) { panic("hook exploded") }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := memstore.New(memstore.Config{})
			h := &recordingHandler{}
			ran := 0
			m := build(t, func(m *mediator.Mediator) {
				must(t, mediator.HandleFunc(m, func(ctx context.Context, c createThing) (thingResult, error) {
					pg.OnCommit(ctx, tc.hook)
					pg.OnCommit(ctx, func(context.Context) { ran++ })
					return thingResult{}, nil
				}))
			}, pg.UnitOfWork(store, pg.UnitOfWorkConfig{Logger: slog.New(h)}))
			if _, err := mediator.Send(context.Background(), m, createThing{}); err != nil {
				t.Fatal(err)
			}
			if store.Committed() != 1 || ran != 1 || h.has(msg) != tc.want {
				t.Fatalf("committed=%d ran=%d logged=%v, want logged=%v", store.Committed(), ran, h.msgs, tc.want)
			}
		})
	}
}
