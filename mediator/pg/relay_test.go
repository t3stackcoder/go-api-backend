package pg_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

// fakeSlotStore is an in-memory pg.SlotStore: outbox rows with a published
// flag and one relay cursor, plus failure switches.
type fakeSlotStore struct {
	mu         sync.Mutex
	rows       []fakeRow
	cursor     *pg.RelayCursor
	failBegin  error
	failCursor error
	failMark   error
	failAfter  error
	failSave   error
	commitErr  error
	batches    int
	// onBegin runs at the start of every BeginBatch; tests use it to cancel
	// the context under a failing batch.
	onBegin func()
	// gauges counts Gauges reads; gaugesFn, when set, supplies the reading.
	gauges   int
	gaugesFn func() (int64, time.Duration, error)
}

type fakeRow struct {
	entry     pg.OutboxEntry
	published bool
}

func (f *fakeSlotStore) add(ids ...int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		f.rows = append(f.rows, fakeRow{entry: pg.OutboxEntry{ID: id, Envelope: mediator.Envelope{ID: mediator.NewID(time.Now()), Topic: "t", StreamKey: "k", Seq: id}, Payload: []byte(`{}`), CreatedAt: time.Now()}})
	}
}

func (f *fakeSlotStore) markPublished(ids ...int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.rows {
		for _, id := range ids {
			if f.rows[i].entry.ID == id {
				f.rows[i].published = true
			}
		}
	}
}

func (f *fakeSlotStore) unpublished() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []int64
	for _, r := range f.rows {
		if !r.published {
			out = append(out, r.entry.ID)
		}
	}
	return out
}

func (f *fakeSlotStore) BeginBatch(ctx context.Context, topic string, partition, limit int) (pg.RelayBatch, error) {
	if f.onBegin != nil {
		f.onBegin()
	}
	if f.failBegin != nil {
		return nil, f.failBegin
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches++
	var entries []pg.OutboxEntry
	for _, r := range f.rows {
		if !r.published && len(entries) < limit {
			entries = append(entries, r.entry)
		}
	}
	return &fakeBatch{store: f, entries: entries}, nil
}

func (f *fakeSlotStore) Cursor(ctx context.Context, topic string, partition int) (pg.RelayCursor, bool, error) {
	if f.failCursor != nil {
		return pg.RelayCursor{}, false, f.failCursor
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cursor == nil {
		return pg.RelayCursor{}, false, nil
	}
	return *f.cursor, true, nil
}

func (f *fakeSlotStore) PublishedAfter(ctx context.Context, topic string, partition int, afterID int64, limit int) ([]pg.OutboxEntry, error) {
	if f.failAfter != nil {
		return nil, f.failAfter
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []pg.OutboxEntry
	for _, r := range f.rows {
		if r.published && r.entry.ID > afterID && len(out) < limit {
			out = append(out, r.entry)
		}
	}
	return out, nil
}

func (f *fakeSlotStore) SaveCursor(ctx context.Context, topic string, partition int, lastOutboxID int64, lastStreamID string) error {
	if f.failSave != nil {
		return f.failSave
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cursor = &pg.RelayCursor{LastOutboxID: lastOutboxID, LastStreamID: lastStreamID}
	return nil
}

func (f *fakeSlotStore) Gauges(ctx context.Context, topic string, partition int) (int64, time.Duration, error) {
	f.mu.Lock()
	f.gauges++
	fn := f.gaugesFn
	f.mu.Unlock()
	if fn != nil {
		return fn()
	}
	return int64(len(f.unpublished())), 0, nil
}

// gaugeCalls reports how many times Gauges was read.
func (f *fakeSlotStore) gaugeCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gauges
}

// setGauges replaces the Gauges reading; nil restores the backlog count.
func (f *fakeSlotStore) setGauges(fn func() (int64, time.Duration, error)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gaugesFn = fn
}

type fakeBatch struct {
	store   *fakeSlotStore
	entries []pg.OutboxEntry
	marked  bool
	stream  string
}

func (b *fakeBatch) Entries() []pg.OutboxEntry { return b.entries }

func (b *fakeBatch) Mark(ctx context.Context, lastStreamID string) error {
	if b.store.failMark != nil {
		return b.store.failMark
	}
	b.marked, b.stream = true, lastStreamID
	return nil
}

func (b *fakeBatch) Commit(ctx context.Context) error {
	if b.store.commitErr != nil {
		return b.store.commitErr
	}
	if b.marked {
		ids := make([]int64, len(b.entries))
		for i, e := range b.entries {
			ids[i] = e.ID
		}
		b.store.markPublished(ids...)
		b.store.mu.Lock()
		b.store.cursor = &pg.RelayCursor{LastOutboxID: ids[len(ids)-1], LastStreamID: b.stream}
		b.store.mu.Unlock()
	}
	return nil
}

func (b *fakeBatch) Rollback(ctx context.Context) error { return nil }

// failingSink wraps a sink to fail EnsureGroups.
type failingSink struct {
	pg.StreamSink
	ensureErr error
}

func (s failingSink) EnsureGroups(ctx context.Context, topic string, partition int, groups []string) error {
	return s.ensureErr
}

// countingSink wraps a sink to count EnsureGroups calls.
type countingSink struct {
	pg.StreamSink
	ensures atomic.Int32
}

func (s *countingSink) EnsureGroups(ctx context.Context, topic string, partition int, groups []string) error {
	s.ensures.Add(1)
	return s.StreamSink.EnsureGroups(ctx, topic, partition, groups)
}

func newSlot(store pg.SlotStore, sink pg.StreamSink, cfg pg.RelayConfig) *pg.Slot {
	return pg.NewSlotForTest(store, sink, cfg, "t", 0)
}

func TestBackoff(t *testing.T) {
	b := pg.NewBackoff(100*time.Millisecond, time.Second)
	want := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, time.Second, time.Second}
	for i, w := range want {
		if got := b.Next(); got != w {
			t.Fatalf("step %d: %s, want %s", i, got, w)
		}
	}
	b.Reset()
	if got := b.Next(); got != 100*time.Millisecond {
		t.Fatalf("after reset: %s", got)
	}
}

func TestStreamIDOrderAndReplayDecision(t *testing.T) {
	cmp := []struct {
		a, b string
		want int
	}{
		{"1-0", "1-0", 0}, {"1-0", "1-1", -1}, {"2-0", "1-9", 1}, {"10-0", "9-5", 1}, {"abc", "abd", -1}, {"1-x", "1-0", 1},
	}
	for _, c := range cmp {
		if got := pg.CompareStreamID(c.a, c.b); got != c.want {
			t.Errorf("compare(%s,%s)=%d want %d", c.a, c.b, got, c.want)
		}
	}
	if !pg.NeedsReplay("5-0", "", false) {
		t.Error("missing stream needs replay")
	}
	if !pg.NeedsReplay("5-0", "4-9", true) {
		t.Error("tail behind cursor needs replay")
	}
	if pg.NeedsReplay("5-0", "5-0", true) || pg.NeedsReplay("5-0", "6-0", true) {
		t.Error("tail at or ahead of the cursor is normal")
	}
}

func TestSlot_RelayOnce(t *testing.T) {
	ctx := context.Background()
	store := &fakeSlotStore{}
	sink := memstore.NewStreams(nil)
	s := newSlot(store, sink, pg.RelayConfig{BatchSize: 2})
	if n, err := s.RelayOnce(ctx); n != 0 || err != nil {
		t.Fatalf("empty: %d %v", n, err)
	}
	store.add(1, 2, 3)
	n, err := s.RelayOnce(ctx)
	if err != nil || n != 2 {
		t.Fatalf("batch: %d %v", n, err)
	}
	if got := store.unpublished(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("unpublished after batch: %v", got)
	}
	entries := sink.Entries("t", 0)
	if len(entries) != 2 || entries[0].Entry.ID != 1 || entries[1].Entry.Envelope.Seq != 2 {
		t.Fatalf("sink: %+v", entries)
	}
	if store.cursor == nil || store.cursor.LastOutboxID != 2 || store.cursor.LastStreamID != entries[1].ID {
		t.Fatalf("cursor: %+v", store.cursor)
	}
	if n, _ := s.RelayOnce(ctx); n != 1 || s.Published() != 3 {
		t.Fatalf("drain: n=%d published=%d", n, s.Published())
	}
}

func TestSlot_FailuresNeverMarkUnsentRows(t *testing.T) {
	ctx := context.Background()
	t.Run("append fails", func(t *testing.T) {
		store := &fakeSlotStore{}
		store.add(1, 2)
		sink := memstore.NewStreams(nil)
		sink.Hooks.Append = func(string, int, []pg.OutboxEntry) error { return errors.New("redis down") }
		s := newSlot(store, sink, pg.RelayConfig{})
		if _, err := s.RelayOnce(ctx); err == nil {
			t.Fatal("want error")
		}
		if len(store.unpublished()) != 2 || len(sink.Entries("t", 0)) != 0 {
			t.Fatal("nothing may be marked or appended")
		}
		sink.Hooks.Append = nil
		if n, err := s.RelayOnce(ctx); err != nil || n != 2 {
			t.Fatalf("retry: %d %v", n, err)
		}
	})
	t.Run("mark fails after append: duplicates, no row marked", func(t *testing.T) {
		store := &fakeSlotStore{failMark: errors.New("crash")}
		store.add(1, 2)
		sink := memstore.NewStreams(nil)
		s := newSlot(store, sink, pg.RelayConfig{})
		if _, err := s.RelayOnce(ctx); err == nil {
			t.Fatal("want error")
		}
		if len(store.unpublished()) != 2 || len(sink.Entries("t", 0)) != 2 {
			t.Fatalf("unpublished=%v sink=%d", store.unpublished(), len(sink.Entries("t", 0)))
		}
		store.failMark = nil
		if n, err := s.RelayOnce(ctx); err != nil || n != 2 {
			t.Fatalf("retry: %d %v", n, err)
		}
		if len(store.unpublished()) != 0 || len(sink.Entries("t", 0)) != 4 {
			t.Fatal("the retry re-adds the batch (at-least-once) and marks it")
		}
	})
	t.Run("commit fails", func(t *testing.T) {
		store := &fakeSlotStore{commitErr: errors.New("conn lost")}
		store.add(1)
		s := newSlot(store, memstore.NewStreams(nil), pg.RelayConfig{})
		if _, err := s.RelayOnce(ctx); err == nil || len(store.unpublished()) != 1 {
			t.Fatalf("commit failure: %v", err)
		}
	})
	t.Run("select fails", func(t *testing.T) {
		store := &fakeSlotStore{failBegin: errors.New("db down")}
		s := newSlot(store, memstore.NewStreams(nil), pg.RelayConfig{})
		if _, err := s.RelayOnce(ctx); err == nil {
			t.Fatal("want error")
		}
	})
}

func TestSlot_DataLossRecovery(t *testing.T) {
	ctx := context.Background()
	groups := func() []string { return []string{"proj"} }

	t.Run("no cursor yet", func(t *testing.T) {
		sink := memstore.NewStreams(nil)
		sink.Hooks.Tail = func(string, int) error { return errors.New("must not be called") }
		s := newSlot(&fakeSlotStore{}, sink, pg.RelayConfig{KnownGroups: groups})
		if n, err := s.CheckDataLoss(ctx); n != 0 || err != nil {
			t.Fatalf("%d %v", n, err)
		}
	})
	t.Run("stream missing: replay everything and recreate groups", func(t *testing.T) {
		store := &fakeSlotStore{cursor: &pg.RelayCursor{LastOutboxID: 3, LastStreamID: "50-0"}}
		store.add(1, 2, 3)
		store.markPublished(1, 2, 3)
		sink := memstore.NewStreams(nil)
		s := newSlot(store, sink, pg.RelayConfig{KnownGroups: groups, BatchSize: 2})
		n, err := s.CheckDataLoss(ctx)
		if err != nil || n != 3 || s.Replayed() != 3 {
			t.Fatalf("replayed %d (%d) %v", n, s.Replayed(), err)
		}
		entries := sink.Entries("t", 0)
		if len(entries) != 3 || entries[2].Entry.ID != 3 {
			t.Fatalf("sink: %+v", entries)
		}
		if store.cursor.LastOutboxID != 3 || store.cursor.LastStreamID != entries[2].ID {
			t.Fatalf("cursor after replay: %+v", store.cursor)
		}
		if g := sink.Groups("t", 0); len(g) != 1 || g[0] != "proj" {
			t.Fatalf("groups: %v", g)
		}
		if n, _ := s.CheckDataLoss(ctx); n != 0 {
			t.Fatal("a consistent stream must not replay again")
		}
	})
	t.Run("stream behind the cursor: replay from the tail", func(t *testing.T) {
		store := &fakeSlotStore{}
		store.add(1, 2, 3)
		store.markPublished(1, 2, 3)
		sink := memstore.NewStreams(nil)
		s := newSlot(store, sink, pg.RelayConfig{})
		last, _ := sink.Append(ctx, "t", 0, []pg.OutboxEntry{store.rows[0].entry, store.rows[1].entry, store.rows[2].entry})
		store.cursor = &pg.RelayCursor{LastOutboxID: 3, LastStreamID: last}
		sink.Truncate("t", 0, 1) // restored from an old snapshot holding only row 1
		n, err := s.CheckDataLoss(ctx)
		if err != nil || n != 2 {
			t.Fatalf("replayed %d %v", n, err)
		}
		entries := sink.Entries("t", 0)
		if len(entries) != 3 || entries[1].Entry.ID != 2 || entries[2].Entry.ID != 3 {
			t.Fatalf("sink: %+v", entries)
		}
	})
	t.Run("stream ahead of the cursor is normal", func(t *testing.T) {
		store := &fakeSlotStore{cursor: &pg.RelayCursor{LastOutboxID: 1, LastStreamID: "1-0"}}
		store.add(1, 2)
		store.markPublished(1, 2)
		sink := memstore.NewStreams(nil)
		_, _ = sink.Append(ctx, "t", 0, []pg.OutboxEntry{store.rows[0].entry, store.rows[1].entry})
		s := newSlot(store, sink, pg.RelayConfig{})
		if n, err := s.CheckDataLoss(ctx); n != 0 || err != nil {
			t.Fatalf("%d %v", n, err)
		}
	})
	t.Run("errors", func(t *testing.T) {
		store := &fakeSlotStore{cursor: &pg.RelayCursor{LastOutboxID: 1, LastStreamID: "9-0"}}
		store.add(1)
		store.markPublished(1)
		sink := memstore.NewStreams(nil)
		sink.Hooks.Tail = func(string, int) error { return errors.New("info failed") }
		s := newSlot(store, sink, pg.RelayConfig{KnownGroups: groups})
		if _, err := s.CheckDataLoss(ctx); err == nil {
			t.Fatal("tail error")
		}
		sink.Hooks.Tail = nil
		store.failAfter = errors.New("select failed")
		if _, err := s.CheckDataLoss(ctx); err == nil {
			t.Fatal("published-after error")
		}
		store.failAfter = nil
		sink.Hooks.Append = func(string, int, []pg.OutboxEntry) error { return errors.New("xadd failed") }
		if _, err := s.CheckDataLoss(ctx); err == nil {
			t.Fatal("append error")
		}
		sink.Hooks.Append = nil
		store.failSave = errors.New("cursor failed")
		if _, err := s.CheckDataLoss(ctx); err == nil {
			t.Fatal("save cursor error")
		}
		store.failSave = nil
		sink.Drop("t", 0)
		s2 := newSlot(store, failingSink{sink, errors.New("xgroup failed")}, pg.RelayConfig{KnownGroups: groups})
		if _, err := s2.CheckDataLoss(ctx); err == nil {
			t.Fatal("ensure groups error")
		}
	})
}

func TestSlot_RunLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeSlotStore{}
		store.add(1)
		sink := memstore.NewStreams(nil)
		// Streams reads its hooks without locking, so the hook is installed
		// before the slot starts and the failure is switched by an atomic
		// instead of by reassigning the hook while the slot runs.
		var redisDown atomic.Bool
		sink.Hooks.Append = func(string, int, []pg.OutboxEntry) error {
			if redisDown.Load() {
				return errors.New("redis down")
			}
			return nil
		}
		cfg := pg.RelayConfig{BatchSize: 10, PollInterval: time.Second, MinBackoff: 100 * time.Millisecond}
		s := newSlot(store, sink, cfg)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.Run(ctx)
		}()
		t.Cleanup(func() {
			cancel()
			<-done
		})
		synctest.Wait()
		if len(sink.Entries("t", 0)) != 1 || !s.Stats().Owned {
			t.Fatalf("initial drain: %d entries owned=%v", len(sink.Entries("t", 0)), s.Stats().Owned)
		}

		store.add(2)
		s.Wake()
		synctest.Wait()
		if len(sink.Entries("t", 0)) != 2 {
			t.Fatal("wake-up did not relay")
		}

		store.add(3)
		time.Sleep(cfg.PollInterval + time.Millisecond)
		synctest.Wait()
		if len(sink.Entries("t", 0)) != 3 {
			t.Fatal("poll did not relay")
		}

		redisDown.Store(true)
		store.add(4)
		s.Wake()
		synctest.Wait()
		if s.Errors() != 1 || s.Stats().LastError == "" {
			t.Fatalf("failure not recorded: errors=%d stats=%+v", s.Errors(), s.Stats())
		}
		redisDown.Store(false)
		time.Sleep(cfg.MinBackoff + time.Millisecond)
		synctest.Wait()
		if len(sink.Entries("t", 0)) != 4 || s.Stats().LastError != "" {
			t.Fatalf("backoff retry: %d entries, stats %+v", len(sink.Entries("t", 0)), s.Stats())
		}
		if st := s.Stats(); st.Unpublished != 0 || st.Topic != "t" {
			t.Fatalf("gauges: %+v", st)
		}
		cancel()
		<-done
		if s.Stats().Owned {
			t.Fatal("ownership must clear on exit")
		}
	})
}

func TestRelay_ConstructionAndWake(t *testing.T) {
	sink := memstore.NewStreams(nil)
	r := pg.NewRelay(nil, sink, pg.RelayConfig{Topics: []string{"a", "b"}, Partitions: 2})
	slots := r.SlotsForTest()
	if len(slots) != 4 {
		t.Fatalf("slots: %d", len(slots))
	}
	if r.Healthy() == nil {
		t.Fatal("not listening before Run")
	}
	st := r.Stats()
	if len(st.Slots) != 4 || st.Listening || st.Slots[3].Topic != "b" || st.Slots[3].Partition != 1 {
		t.Fatalf("stats: %+v", st)
	}
	// Malformed and unowned payloads are ignored without panicking.
	for _, p := range []string{"", "bad", "a:x", "a:1", "zzz:0", "a:9"} {
		r.WakeForTest(p)
	}
	if pg.RelayLockKey("orders", 3) != "mediator_relay:orders:3" {
		t.Fatal("lock key")
	}
	c := pg.RelayConfig{}.WithDefaults()
	if c.Partitions != 1 || c.BatchSize != 100 || c.PollInterval != time.Second || c.MinBackoff != 100*time.Millisecond || c.MaxBackoff != 10*time.Second || c.Logger == nil || c.Clock == nil {
		t.Fatalf("defaults: %+v", c)
	}
}

// TestSlot_TailCheckPerBatch is 7.7 under load: a loss between two batches
// is detected before the next append, at the cost of one Tail per non-empty
// batch, and a failing check or replay rolls the batch back.
func TestSlot_TailCheckPerBatch(t *testing.T) {
	ctx := context.Background()
	store := &fakeSlotStore{}
	sink := memstore.NewStreams(nil)
	var tails atomic.Int32
	sink.Hooks.Tail = func(string, int) error { tails.Add(1); return nil }
	s := newSlot(store, sink, pg.RelayConfig{BatchSize: 10, KnownGroups: func() []string { return []string{"proj"} }})

	// The first batch has no cursor to compare with: no check.
	store.add(1)
	if n, err := s.RelayOnce(ctx); n != 1 || err != nil || tails.Load() != 0 {
		t.Fatalf("first batch: n=%d err=%v tails=%d", n, err, tails.Load())
	}
	// Every following non-empty batch checks the tail once.
	store.add(2)
	if n, err := s.RelayOnce(ctx); n != 1 || err != nil || tails.Load() != 1 {
		t.Fatalf("second batch: n=%d err=%v tails=%d", n, err, tails.Load())
	}
	if n, err := s.RelayOnce(ctx); n != 0 || err != nil || tails.Load() != 1 {
		t.Fatalf("empty batch must not check: n=%d err=%v tails=%d", n, err, tails.Load())
	}
	// Redis loses everything between two batches: the check before the
	// third append replays rows 1 and 2, recreates the groups, then the
	// batch is appended. checkDataLoss adds its own Tail.
	sink.Drop("t", 0)
	store.add(3)
	if n, err := s.RelayOnce(ctx); n != 1 || err != nil || tails.Load() != 3 {
		t.Fatalf("batch after loss: n=%d err=%v tails=%d", n, err, tails.Load())
	}
	entries := sink.Entries("t", 0)
	if len(entries) != 3 || entries[0].Entry.ID != 1 || entries[1].Entry.ID != 2 || entries[2].Entry.ID != 3 || s.Replayed() != 2 {
		t.Fatalf("sink after loss: %+v replayed=%d", entries, s.Replayed())
	}
	if g := sink.Groups("t", 0); len(g) != 1 || g[0] != "proj" {
		t.Fatalf("groups: %v", g)
	}
	if store.cursor.LastOutboxID != 3 || store.cursor.LastStreamID != entries[2].ID {
		t.Fatalf("cursor: %+v", store.cursor)
	}
	// A restore from an old snapshot (tail behind the cursor) is caught
	// the same way.
	sink.Truncate("t", 0, 1)
	store.add(4)
	if n, err := s.RelayOnce(ctx); n != 1 || err != nil || s.Replayed() != 4 {
		t.Fatalf("batch after restore: n=%d err=%v replayed=%d", n, err, s.Replayed())
	}
	if entries = sink.Entries("t", 0); len(entries) != 4 || entries[3].Entry.ID != 4 {
		t.Fatalf("sink after restore: %+v", entries)
	}
	// The check failing rolls the batch back: nothing marked, nothing appended.
	sink.Hooks.Tail = func(string, int) error { return errors.New("info failed") }
	store.add(5)
	if _, err := s.RelayOnce(ctx); err == nil {
		t.Fatal("tail error must fail the batch")
	}
	if u := store.unpublished(); len(u) != 1 || u[0] != 5 || len(sink.Entries("t", 0)) != 4 {
		t.Fatalf("after tail error: unpublished=%v sink=%d", u, len(sink.Entries("t", 0)))
	}
	// A replay failing inside the batch rolls it back too, and the retry
	// replays and appends.
	sink.Hooks.Tail = nil
	sink.Drop("t", 0)
	store.failAfter = errors.New("select failed")
	if _, err := s.RelayOnce(ctx); err == nil {
		t.Fatal("replay error must fail the batch")
	}
	if u := store.unpublished(); len(u) != 1 || u[0] != 5 {
		t.Fatalf("after replay error: unpublished=%v", u)
	}
	store.failAfter = nil
	if n, err := s.RelayOnce(ctx); n != 1 || err != nil || s.Replayed() != 8 {
		t.Fatalf("retry: n=%d err=%v replayed=%d", n, err, s.Replayed())
	}
	if entries = sink.Entries("t", 0); len(entries) != 5 || entries[4].Entry.ID != 5 {
		t.Fatalf("sink after retry: %+v", entries)
	}
}

// TestSlot_RunLoop_DetectsLossOnWake drives the loop with wake-ups only (the
// poll never comes) and loses the stream between two of them.
func TestSlot_RunLoop_DetectsLossOnWake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeSlotStore{}
		store.add(1)
		sink := memstore.NewStreams(nil)
		cfg := pg.RelayConfig{BatchSize: 10, PollInterval: time.Hour, KnownGroups: func() []string { return []string{"proj"} }}
		s := newSlot(store, sink, cfg)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.Run(ctx)
		}()
		t.Cleanup(func() {
			cancel()
			<-done
		})
		synctest.Wait()
		if len(sink.Entries("t", 0)) != 1 {
			t.Fatal("initial drain")
		}
		sink.Drop("t", 0) // FLUSHALL
		store.add(2)
		s.Wake()
		synctest.Wait()
		entries := sink.Entries("t", 0)
		if len(entries) != 2 || entries[0].Entry.ID != 1 || entries[1].Entry.ID != 2 || s.Replayed() != 1 {
			t.Fatalf("loss undetected on the wake-up path: %+v replayed=%d", entries, s.Replayed())
		}
		if g := sink.Groups("t", 0); len(g) != 1 {
			t.Fatalf("groups not recreated: %v", g)
		}
		if st := s.Stats(); st.LastError != "" || s.Errors() != 0 {
			t.Fatalf("replay must not count as an error: %+v", st)
		}
	})
}

// slotName renders a slot as its notification payload, "<topic>:<partition>".
func slotName(st pg.SlotStats) string { return st.Topic + ":" + strconv.Itoa(st.Partition) }

// TestRelay_WakeTargetsOneOwnedSlot pins the notification router: a
// "<topic>:<partition>" payload wakes exactly the owned slot it names, and
// anything else wakes nothing.
func TestRelay_WakeTargetsOneOwnedSlot(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		unowned string // the slot left unowned, "" for none
		want    string // the slot woken, "" for none
	}{
		{"first partition", "a:0", "", "a:0"},
		{"second partition", "a:1", "", "a:1"},
		{"other topic", "b:1", "", "b:1"},
		{"empty topic: the separator is at index 0", ":1", "", ":1"},
		{"unowned slot", "a:1", "a:1", ""},
		{"unknown partition", "a:9", "", ""},
		{"unknown topic", "zzz:0", "", ""},
		{"non-numeric partition", "a:x", "", ""},
		{"no separator", "a", "", ""},
		{"empty payload", "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := pg.NewRelay(nil, memstore.NewStreams(nil), pg.RelayConfig{Topics: []string{"a", "b", ""}, Partitions: 2})
			for _, s := range r.SlotsForTest() {
				s.SetOwnedForTest(slotName(s.Stats()) != tc.unowned)
			}
			r.WakeForTest(tc.payload)
			var woken []string
			for _, s := range r.SlotsForTest() {
				if s.TakeWake() {
					woken = append(woken, slotName(s.Stats()))
				}
			}
			if got := strings.Join(woken, " "); got != tc.want {
				t.Fatalf("wake(%q) woke %q, want %q", tc.payload, got, tc.want)
			}
		})
	}
}

// TestSlot_RunLoop_FullBatchPollsAgainAtOnce pins the two decisions after a
// successful batch: a batch of exactly BatchSize rows is followed by another
// batch at once (no wake-up, no poll), and the gauges are refreshed only
// when a non-empty batch leaves the slot idle.
func TestSlot_RunLoop_FullBatchPollsAgainAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeSlotStore{}
		store.add(1, 2, 3)
		sink := memstore.NewStreams(nil)
		cfg := pg.RelayConfig{BatchSize: 2, PollInterval: time.Hour}
		s := newSlot(store, sink, cfg)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.Run(ctx)
		}()
		t.Cleanup(func() {
			cancel()
			<-done
		})
		synctest.Wait()
		// The first batch is full (n == BatchSize), so the third row is
		// relayed without waiting for a wake-up or the poll.
		if got := len(sink.Entries("t", 0)); got != 3 {
			t.Fatalf("entries after the initial drain: %d, want 3", got)
		}
		// Gauges: once from the initial check and once when the second,
		// partial batch left the slot idle; the full batch did not refresh.
		if got := store.gaugeCalls(); got != 2 {
			t.Fatalf("gauge reads after the drain: %d, want 2", got)
		}
		// A wake-up that finds nothing leaves the gauges alone.
		s.Wake()
		synctest.Wait()
		if got := store.gaugeCalls(); got != 2 {
			t.Fatalf("gauge reads after an empty batch: %d, want 2", got)
		}
		// A partial batch (n < BatchSize) refreshes once.
		store.add(4)
		s.Wake()
		synctest.Wait()
		if got := len(sink.Entries("t", 0)); got != 4 {
			t.Fatalf("entries after the wake-up: %d, want 4", got)
		}
		if got := store.gaugeCalls(); got != 3 {
			t.Fatalf("gauge reads after a partial batch: %d, want 3", got)
		}
	})
}

// TestSlot_RunLoop_GaugesKeepTheLastGoodReading: a successful Gauges read
// is stored as is; a failed read leaves the previous values in place.
func TestSlot_RunLoop_GaugesKeepTheLastGoodReading(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := &fakeSlotStore{}
		store.setGauges(func() (int64, time.Duration, error) { return 5, 7 * time.Second, nil })
		cfg := pg.RelayConfig{PollInterval: time.Second}
		s := newSlot(store, memstore.NewStreams(nil), cfg)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			s.Run(ctx)
		}()
		t.Cleanup(func() {
			cancel()
			<-done
		})
		synctest.Wait()
		if st := s.Stats(); st.Unpublished != 5 || st.OldestAge != 7*time.Second {
			t.Fatalf("gauges after a successful read: %+v", st)
		}
		store.setGauges(func() (int64, time.Duration, error) { return 0, 0, errors.New("db down") })
		time.Sleep(cfg.PollInterval + time.Millisecond)
		synctest.Wait()
		if st := s.Stats(); st.Unpublished != 5 || st.OldestAge != 7*time.Second {
			t.Fatalf("a failed read must keep the last values: %+v", st)
		}
		if got := store.gaugeCalls(); got != 2 {
			t.Fatalf("gauge reads: %d, want 2 (initial check and one poll)", got)
		}
	})
}

// TestSlot_DataLossRecovery_RecreatesOnlyKnownGroups: after a loss the
// consumer groups are recreated only when KnownGroups names at least one.
func TestSlot_DataLossRecovery_RecreatesOnlyKnownGroups(t *testing.T) {
	cases := []struct {
		name        string
		known       func() []string
		wantEnsures int32
		wantGroups  []string
	}{
		{"no KnownGroups", nil, 0, nil},
		{"KnownGroups returns nil", func() []string { return nil }, 0, nil},
		{"KnownGroups returns an empty slice", func() []string { return []string{} }, 0, nil},
		{"one group", func() []string { return []string{"proj"} }, 1, []string{"proj"}},
		{"two groups", func() []string { return []string{"proj", "audit"} }, 1, []string{"audit", "proj"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeSlotStore{cursor: &pg.RelayCursor{LastOutboxID: 3, LastStreamID: "50-0"}}
			store.add(1, 2, 3)
			store.markPublished(1, 2, 3)
			inner := memstore.NewStreams(nil)
			sink := &countingSink{StreamSink: inner}
			s := newSlot(store, sink, pg.RelayConfig{KnownGroups: tc.known})
			if n, err := s.CheckDataLoss(context.Background()); err != nil || n != 3 {
				t.Fatalf("replayed %d %v", n, err)
			}
			if got := sink.ensures.Load(); got != tc.wantEnsures {
				t.Fatalf("EnsureGroups calls: %d, want %d", got, tc.wantEnsures)
			}
			if got := inner.Groups("t", 0); !slices.Equal(got, tc.wantGroups) {
				t.Fatalf("groups: %v, want %v", got, tc.wantGroups)
			}
		})
	}
}
