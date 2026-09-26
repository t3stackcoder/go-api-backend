package pg

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// RelayConfig configures the relay (6.4, 7.7).
type RelayConfig struct {
	// Topics lists every topic this process relays.
	Topics []string
	// Partitions is P, the same value every node uses.
	Partitions int
	// BatchSize bounds one SELECT ... FOR UPDATE SKIP LOCKED. Default 100.
	BatchSize int
	// PollInterval is how often a slot polls without a notification and
	// runs the data-loss check. Default 1s.
	PollInterval time.Duration
	// MinBackoff and MaxBackoff bound the exponential backoff after a
	// failure. Defaults 100ms and 10s.
	MinBackoff, MaxBackoff time.Duration
	// KnownGroups returns the consumer groups to recreate after Redis data
	// loss (7.7). Nil means none.
	KnownGroups func() []string
	Logger      *slog.Logger
	Clock       testkit.Clock
}

func (c RelayConfig) withDefaults() RelayConfig {
	if c.Partitions <= 0 {
		c.Partitions = 1
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 100
	}
	if c.PollInterval <= 0 {
		c.PollInterval = time.Second
	}
	if c.MinBackoff <= 0 {
		c.MinBackoff = 100 * time.Millisecond
	}
	if c.MaxBackoff <= 0 {
		c.MaxBackoff = 10 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Clock == nil {
		c.Clock = testkit.RealClock{}
	}
	return c
}

// Relay moves outbox rows to the stream sink. There is one relay slot per
// (topic, partition); a process owns a slot through an advisory lock held
// on a dedicated connection, so several relay processes share the slots
// safely and ownership ends with the connection. The same connection
// LISTENs on NotifyChannel so a commit that appended outbox rows wakes the
// slot within milliseconds; every slot also polls at PollInterval. It is a
// mediator.Component.
type Relay struct {
	pool      *pgxpool.Pool
	sink      StreamSink
	cfg       RelayConfig
	slots     []*slot
	counters  relayCounters
	listening atomic.Bool
	hooks     relayHooks
}

type relayCounters struct{ published, replayed, errors atomic.Int64 }

// relayHooks are test seams for crash simulation.
type relayHooks struct {
	beforeMark func(ctx context.Context) error
}

// RelayStats is a snapshot of the relay counters and per-slot gauges.
type RelayStats struct {
	// Published counts rows appended to the sink and marked since start.
	Published int64
	// Replayed counts rows re-appended after data loss was detected (G12).
	Replayed int64
	// Errors counts failed batches, checks, and sessions.
	Errors int64
	// Listening reports whether the dedicated connection is up.
	Listening bool
	Slots     []SlotStats
}

// SlotStats describes one (topic, partition) slot.
type SlotStats struct {
	Topic     string
	Partition int
	// Owned reports whether this process holds the slot's advisory lock.
	Owned bool
	// Unpublished and OldestAge are refreshed on every poll of an owned slot.
	Unpublished int64
	OldestAge   time.Duration
	// LastError is the message of the most recent failure, "" after success.
	LastError string
}

// NewRelay builds a relay for every (topic, partition) of cfg over pool and sink.
func NewRelay(pool *pgxpool.Pool, sink StreamSink, cfg RelayConfig) *Relay {
	cfg = cfg.withDefaults()
	r := &Relay{pool: pool, sink: sink, cfg: cfg}
	store := &pgSlotStore{pool: pool, hooks: &r.hooks}
	for _, topic := range cfg.Topics {
		for p := 0; p < cfg.Partitions; p++ {
			r.slots = append(r.slots, newSlot(store, sink, &r.cfg, &r.counters, topic, p))
		}
	}
	return r
}

// Run owns slots and relays until ctx is canceled. A lost dedicated
// connection ends the session (its locks die with it); the relay backs off
// and starts a new one.
func (r *Relay) Run(ctx context.Context) error {
	bo := newBackoff(r.cfg.MinBackoff, r.cfg.MaxBackoff)
	for {
		err := r.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		r.counters.errors.Add(1)
		r.cfg.Logger.Error("relay: session ended; reconnecting", "error", err)
		select {
		case <-r.cfg.Clock.After(bo.next()):
		case <-ctx.Done():
			return nil
		}
	}
}

// Healthy reports nil while the dedicated connection is listening.
func (r *Relay) Healthy() error {
	if !r.listening.Load() {
		return errors.New("relay: not listening")
	}
	return nil
}

// Stats returns a snapshot of counters and gauges.
func (r *Relay) Stats() RelayStats {
	st := RelayStats{
		Published: r.counters.published.Load(), Replayed: r.counters.replayed.Load(),
		Errors: r.counters.errors.Load(), Listening: r.listening.Load(),
	}
	for _, s := range r.slots {
		st.Slots = append(st.Slots, s.stats())
	}
	return st
}

func (r *Relay) session(ctx context.Context) error {
	pc, err := r.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("relay: acquire connection: %w", err)
	}
	// Hijack so the pool never hands this connection, with its LISTEN and
	// advisory locks, to anyone else; closing it releases everything.
	conn := pc.Hijack()
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = conn.Close(cctx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return fmt.Errorf("relay: listen: %w", err)
	}
	r.listening.Store(true)
	defer r.listening.Store(false)
	sctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	for {
		r.acquireSlots(sctx, conn, &wg)
		wctx, wcancel := context.WithTimeout(ctx, r.cfg.PollInterval)
		n, err := conn.WaitForNotification(wctx)
		wcancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) || pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("relay: listen connection: %w", err)
		}
		r.wake(n.Payload)
	}
}

// acquireSlots tries the advisory lock of every unowned slot and starts a
// worker for each one obtained.
func (r *Relay) acquireSlots(ctx context.Context, conn *pgx.Conn, wg *sync.WaitGroup) {
	for _, s := range r.slots {
		if s.owned.Load() {
			continue
		}
		if err := testkit.Fault(ctx, "pg.relay.lock"); err != nil {
			r.cfg.Logger.Warn("relay: lock", "topic", s.topic, "partition", s.partition, "error", err)
			continue
		}
		var got bool
		if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, relayLockKey(s.topic, s.partition)).Scan(&got); err != nil {
			r.cfg.Logger.Warn("relay: lock", "topic", s.topic, "partition", s.partition, "error", err)
			return
		}
		if !got {
			continue
		}
		s.owned.Store(true)
		r.cfg.Logger.Info("relay: slot acquired", "topic", s.topic, "partition", s.partition)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer s.owned.Store(false)
			s.run(ctx)
		}()
	}
}

// relayLockKey is the advisory lock name of 6.4.
func relayLockKey(topic string, partition int) string {
	return "mediator_relay:" + topic + ":" + strconv.Itoa(partition)
}

// wake delivers a "<topic>:<partition>" notification to its slot.
func (r *Relay) wake(payload string) {
	i := strings.LastIndexByte(payload, ':')
	if i < 0 {
		return
	}
	p, err := strconv.Atoi(payload[i+1:])
	if err != nil {
		return
	}
	topic := payload[:i]
	for _, s := range r.slots {
		if s.topic == topic && s.partition == p && s.owned.Load() {
			select {
			case s.wake <- struct{}{}:
			default:
			}
			return
		}
	}
}

// slotStore is what a slot needs from the database. pgSlotStore implements
// it over pgx; tests use a fake so the loop is verified without Postgres.
type slotStore interface {
	// BeginBatch opens a transaction and selects up to limit unpublished
	// rows FOR UPDATE SKIP LOCKED.
	BeginBatch(ctx context.Context, topic string, partition, limit int) (relayBatch, error)
	// Cursor reads the relay cursor; ok is false when none was written yet.
	Cursor(ctx context.Context, topic string, partition int) (cursor relayCursor, ok bool, err error)
	// PublishedAfter returns published rows with id > afterID in id order.
	PublishedAfter(ctx context.Context, topic string, partition int, afterID int64, limit int) ([]OutboxEntry, error)
	// SaveCursor upserts the cursor outside a batch (after a replay).
	SaveCursor(ctx context.Context, topic string, partition int, lastOutboxID int64, lastStreamID string) error
	// Gauges reports the unpublished backlog of the partition.
	Gauges(ctx context.Context, topic string, partition int) (unpublished int64, oldest time.Duration, err error)
}

// relayBatch is one open SELECT ... FOR UPDATE SKIP LOCKED transaction.
type relayBatch interface {
	Entries() []OutboxEntry
	// Mark sets published_at on the selected rows and upserts the cursor.
	Mark(ctx context.Context, lastStreamID string) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// relayCursor is one row of mediator_relay_cursor.
type relayCursor struct {
	LastOutboxID int64
	LastStreamID string
}

// slot relays one (topic, partition).
type slot struct {
	topic       string
	partition   int
	store       slotStore
	sink        StreamSink
	cfg         *RelayConfig
	counters    *relayCounters
	backoff     backoff
	wake        chan struct{}
	owned       atomic.Bool
	unpublished atomic.Int64
	oldestAge   atomic.Int64
	lastErr     atomic.Pointer[string]
}

func newSlot(store slotStore, sink StreamSink, cfg *RelayConfig, counters *relayCounters, topic string, partition int) *slot {
	return &slot{
		topic: topic, partition: partition, store: store, sink: sink, cfg: cfg, counters: counters,
		backoff: newBackoff(cfg.MinBackoff, cfg.MaxBackoff), wake: make(chan struct{}, 1),
	}
}

func (s *slot) stats() SlotStats {
	st := SlotStats{Topic: s.topic, Partition: s.partition, Owned: s.owned.Load(),
		Unpublished: s.unpublished.Load(), OldestAge: time.Duration(s.oldestAge.Load())}
	if e := s.lastErr.Load(); e != nil {
		st.LastError = *e
	}
	return st
}

// run is the per-slot loop of 6.4: drain batches, then wait for a wake-up
// or the poll interval; the poll also runs the data-loss check of 7.7.
func (s *slot) run(ctx context.Context) {
	s.check(ctx)
	for {
		n, err := s.relayOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.fail(err)
			select {
			case <-s.cfg.Clock.After(s.backoff.next()):
			case <-ctx.Done():
				return
			}
			continue
		}
		s.backoff.reset()
		s.lastErr.Store(nil)
		if n >= s.cfg.BatchSize {
			continue
		}
		if n > 0 {
			s.refreshGauges(ctx) // the slot just went idle; report the true backlog
		}
		select {
		case <-s.wake:
		case <-s.cfg.Clock.After(s.cfg.PollInterval):
			s.check(ctx)
		case <-ctx.Done():
			return
		}
	}
}

// check runs the data-loss check and refreshes the gauges, logging failures.
func (s *slot) check(ctx context.Context) {
	if _, err := s.checkDataLoss(ctx); err != nil && ctx.Err() == nil {
		s.fail(err)
	}
	s.refreshGauges(ctx)
}

func (s *slot) refreshGauges(ctx context.Context) {
	if n, age, err := s.store.Gauges(ctx, s.topic, s.partition); err == nil {
		s.unpublished.Store(n)
		s.oldestAge.Store(int64(age))
	}
}

func (s *slot) fail(err error) {
	s.counters.errors.Add(1)
	msg := err.Error()
	s.lastErr.Store(&msg)
	s.cfg.Logger.Warn("relay: slot error", "topic", s.topic, "partition", s.partition, "error", err)
}

// relayOnce relays one batch: BEGIN and SELECT ... FOR UPDATE SKIP LOCKED,
// append to the sink, UPDATE published_at, upsert the cursor, COMMIT. Any
// failure rolls back so no row is marked that was not appended; entries
// already appended become duplicates the inbox handles.
func (s *slot) relayOnce(ctx context.Context) (int, error) {
	b, err := s.store.BeginBatch(ctx, s.topic, s.partition, s.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	entries := b.Entries()
	if len(entries) == 0 {
		return 0, b.Rollback(ctx)
	}
	lastID, err := s.sink.Append(ctx, s.topic, s.partition, entries)
	if err != nil {
		_ = b.Rollback(ctx)
		return 0, fmt.Errorf("relay: append: %w", err)
	}
	if err := b.Mark(ctx, lastID); err != nil {
		_ = b.Rollback(ctx)
		return 0, err
	}
	if err := b.Commit(ctx); err != nil {
		return 0, fmt.Errorf("relay: commit: %w", err)
	}
	s.counters.published.Add(int64(len(entries)))
	return len(entries), nil
}

// checkDataLoss compares the cursor with the stream tail (7.7). When the
// stream is missing or behind the cursor, every published row after the
// tail's outbox id is re-appended in order, the cursor is moved to the new
// tail, and the known consumer groups are recreated.
func (s *slot) checkDataLoss(ctx context.Context) (int, error) {
	cur, ok, err := s.store.Cursor(ctx, s.topic, s.partition)
	if err != nil || !ok {
		return 0, err
	}
	tailID, tailOutbox, tailOK, err := s.sink.Tail(ctx, s.topic, s.partition)
	if err != nil {
		return 0, fmt.Errorf("relay: stream tail: %w", err)
	}
	if !needsReplay(cur.LastStreamID, tailID, tailOK) {
		return 0, nil
	}
	var after int64
	if tailOK {
		after = tailOutbox
	}
	replayed := 0
	lastStream, lastOutbox := tailID, after
	for {
		rows, err := s.store.PublishedAfter(ctx, s.topic, s.partition, after, s.cfg.BatchSize)
		if err != nil {
			return replayed, err
		}
		if len(rows) == 0 {
			break
		}
		id, err := s.sink.Append(ctx, s.topic, s.partition, rows)
		if err != nil {
			return replayed, fmt.Errorf("relay: replay append: %w", err)
		}
		replayed += len(rows)
		after = rows[len(rows)-1].ID
		lastStream, lastOutbox = id, after
		if len(rows) < s.cfg.BatchSize {
			break
		}
	}
	s.counters.replayed.Add(int64(replayed))
	if replayed > 0 {
		s.cfg.Logger.Warn("relay: stream data loss detected; replayed outbox rows",
			"topic", s.topic, "partition", s.partition, "replayed", replayed, "cursor", cur.LastStreamID, "tail", tailID)
		if err := s.store.SaveCursor(ctx, s.topic, s.partition, lastOutbox, lastStream); err != nil {
			return replayed, err
		}
	}
	if s.cfg.KnownGroups != nil {
		if groups := s.cfg.KnownGroups(); len(groups) > 0 {
			if err := s.sink.EnsureGroups(ctx, s.topic, s.partition, groups); err != nil {
				return replayed, fmt.Errorf("relay: ensure groups: %w", err)
			}
		}
	}
	return replayed, nil
}

// needsReplay is the decision of 7.7: the stream is missing, or its last
// entry is older than the one the cursor recorded.
func needsReplay(cursorStreamID, tailID string, tailOK bool) bool {
	if !tailOK {
		return true
	}
	return compareStreamID(tailID, cursorStreamID) < 0
}

// compareStreamID orders Redis stream IDs ("<ms>-<seq>") numerically, falling
// back to string order for anything else.
func compareStreamID(a, b string) int {
	am, as, aok := parseStreamID(a)
	bm, bs, bok := parseStreamID(b)
	if !aok || !bok {
		return strings.Compare(a, b)
	}
	if am != bm {
		return cmp.Compare(am, bm)
	}
	return cmp.Compare(as, bs)
}

func parseStreamID(s string) (ms, seq uint64, ok bool) {
	i := strings.IndexByte(s, '-')
	if i < 0 {
		return 0, 0, false
	}
	ms, err := strconv.ParseUint(s[:i], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	seq, err = strconv.ParseUint(s[i+1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return ms, seq, true
}

// backoff doubles from min to max and resets on success.
type backoff struct{ min, max, cur time.Duration }

func newBackoff(min, max time.Duration) backoff { return backoff{min: min, max: max} }

func (b *backoff) next() time.Duration {
	if b.cur == 0 {
		b.cur = b.min
	}
	d := b.cur
	b.cur = min(b.cur*2, b.max)
	return d
}

func (b *backoff) reset() { b.cur = 0 }

// pgSlotStore is the slotStore over pgx.
type pgSlotStore struct {
	pool  *pgxpool.Pool
	hooks *relayHooks
}

func (p *pgSlotStore) BeginBatch(ctx context.Context, topic string, partition, limit int) (relayBatch, error) {
	if err := testkit.Fault(ctx, "pg.relay.select"); err != nil {
		return nil, err
	}
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("pg: relay begin: %w", err)
	}
	entries, err := selectOutbox(ctx, tx, sqlRelaySelect, topic, partition, limit)
	if err != nil {
		_ = tx.Rollback(context.WithoutCancel(ctx))
		return nil, err
	}
	return &pgBatch{tx: tx, entries: entries, topic: topic, partition: partition, hooks: p.hooks}, nil
}

func (p *pgSlotStore) Cursor(ctx context.Context, topic string, partition int) (relayCursor, bool, error) {
	var c relayCursor
	err := p.pool.QueryRow(ctx, sqlCursorSelect, topic, partition).Scan(&c.LastOutboxID, &c.LastStreamID)
	if errors.Is(err, pgx.ErrNoRows) {
		return relayCursor{}, false, nil
	}
	if err != nil {
		return relayCursor{}, false, fmt.Errorf("pg: relay cursor: %w", err)
	}
	return c, true, nil
}

func (p *pgSlotStore) PublishedAfter(ctx context.Context, topic string, partition int, afterID int64, limit int) ([]OutboxEntry, error) {
	return selectOutbox(ctx, p.pool, sqlOutboxPublishedAfter, topic, partition, afterID, limit)
}

func (p *pgSlotStore) SaveCursor(ctx context.Context, topic string, partition int, lastOutboxID int64, lastStreamID string) error {
	if err := testkit.Fault(ctx, "pg.relay.cursor"); err != nil {
		return err
	}
	if _, err := p.pool.Exec(ctx, sqlCursorUpsert, topic, partition, lastOutboxID, lastStreamID); err != nil {
		return fmt.Errorf("pg: relay cursor: %w", err)
	}
	return nil
}

func (p *pgSlotStore) Gauges(ctx context.Context, topic string, partition int) (int64, time.Duration, error) {
	var n int64
	var age float64
	if err := p.pool.QueryRow(ctx, sqlOutboxGauges, topic, partition).Scan(&n, &age); err != nil {
		return 0, 0, fmt.Errorf("pg: outbox gauges: %w", err)
	}
	return n, time.Duration(age * float64(time.Second)), nil
}

// pgBatch is the relayBatch over a pgx transaction.
type pgBatch struct {
	tx        pgx.Tx
	entries   []OutboxEntry
	topic     string
	partition int
	hooks     *relayHooks
}

func (b *pgBatch) Entries() []OutboxEntry { return b.entries }

func (b *pgBatch) Mark(ctx context.Context, lastStreamID string) error {
	if b.hooks != nil && b.hooks.beforeMark != nil {
		if err := b.hooks.beforeMark(ctx); err != nil {
			return err
		}
	}
	if err := testkit.Fault(ctx, "pg.relay.mark"); err != nil {
		return err
	}
	ids := make([]int64, len(b.entries))
	for i, e := range b.entries {
		ids[i] = e.ID
	}
	if _, err := b.tx.Exec(ctx, sqlRelayMark, ids); err != nil {
		return fmt.Errorf("pg: relay mark: %w", err)
	}
	if err := testkit.FaultAfter(ctx, "pg.relay.mark"); err != nil {
		return err
	}
	if err := testkit.Fault(ctx, "pg.relay.cursor"); err != nil {
		return err
	}
	if _, err := b.tx.Exec(ctx, sqlCursorUpsert, b.topic, b.partition, ids[len(ids)-1], lastStreamID); err != nil {
		return fmt.Errorf("pg: relay cursor: %w", err)
	}
	return nil
}

func (b *pgBatch) Commit(ctx context.Context) error { return b.tx.Commit(ctx) }

func (b *pgBatch) Rollback(ctx context.Context) error {
	err := b.tx.Rollback(context.WithoutCancel(ctx))
	if err == nil || errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return err
}
