package pg

// Seams that expose unexported internals to the external test package so
// the relay loop, the SQL builders, and the classifiers are unit tested
// without Postgres.

import (
	"context"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type (
	SlotStore   = slotStore
	RelayBatch  = relayBatch
	RelayCursor = relayCursor
	Slot        = slot
	Backoff     = backoff
	Migration   = migration
)

func NewSlotForTest(store SlotStore, sink StreamSink, cfg RelayConfig, topic string, partition int) *Slot {
	c := cfg.withDefaults()
	return newSlot(store, sink, &c, &relayCounters{}, topic, partition)
}

func (s *slot) RelayOnce(ctx context.Context) (int, error)     { return s.relayOnce(ctx) }
func (s *slot) CheckDataLoss(ctx context.Context) (int, error) { return s.checkDataLoss(ctx) }
func (s *slot) Run(ctx context.Context) {
	s.owned.Store(true)
	defer s.owned.Store(false)
	s.run(ctx)
}
func (s *slot) Wake() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *slot) Stats() SlotStats { return s.stats() }
func (s *slot) Published() int64 { return s.counters.published.Load() }
func (s *slot) Replayed() int64  { return s.counters.replayed.Load() }
func (s *slot) Errors() int64    { return s.counters.errors.Load() }

func NeedsReplay(cursor, tail string, ok bool) bool { return needsReplay(cursor, tail, ok) }
func CompareStreamID(a, b string) int               { return compareStreamID(a, b) }
func NewBackoff(min, max time.Duration) Backoff     { return newBackoff(min, max) }
func (b *backoff) Next() time.Duration              { return b.next() }
func (b *backoff) Reset()                           { b.reset() }

func BeginSQL(opts TxOptions, defaultLock time.Duration, schema string, deadline time.Time, hasDeadline bool, now time.Time) (string, error) {
	return beginSQL(opts, defaultLock, schema, deadline, hasDeadline, now)
}
func ResolveTxOptions(req any, kind mediator.Kind, defaultLock time.Duration) TxOptions {
	return resolveTxOptions(req, kind, defaultLock)
}
func IsDefiniteCommitFailure(err error) bool { return isDefiniteCommitFailure(err) }
func IsTransientPg(err error) bool           { return isTransient(err) }

func SetRelayBeforeMark(r *Relay, f func(context.Context) error) { r.hooks.beforeMark = f }
func (r *Relay) WakeForTest(payload string)                      { r.wake(payload) }
func (r *Relay) SlotsForTest() []*Slot                           { return r.slots }
func RelayLockKey(topic string, partition int) string            { return relayLockKey(topic, partition) }

func LoadMigrations(fsys fs.FS) ([]Migration, error) { return loadMigrations(fsys) }
func SplitMigration(src string) (up, down string)    { return splitMigration(src) }
func (m migration) Version() int                     { return m.version }
func (m migration) Name() string                     { return m.name }
func (m migration) Up() string                       { return m.up }
func (m migration) Down() string                     { return m.down }

func EncodeOutboxHeaders(env *mediator.Envelope) ([]byte, error) { return encodeOutboxHeaders(env) }
func DecodeOutboxHeaders(raw []byte, env *mediator.Envelope) error {
	return decodeOutboxHeaders(raw, env)
}
func ApplyPoolConfig(pc *pgxpool.Config, cfg PoolConfig) { applyPoolConfig(pc, cfg) }

func (c RelayConfig) WithDefaults() RelayConfig             { return c.withDefaults() }
func (c JanitorConfig) WithDefaults() JanitorConfig         { return c.withDefaults() }
func (c IdempotencyConfig) WithDefaults() IdempotencyConfig { return c.withDefaults() }
func (c UnitOfWorkConfig) WithDefaults() UnitOfWorkConfig   { return c.withDefaults() }
