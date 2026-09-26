// Package memstore is the in-memory pg.Store used by unit tests. It keeps
// the locking semantics of Postgres for the operations the behaviors use:
// per-(topic, key) sequence row locks held until the transaction ends,
// inbox primary-key semantics, idempotency upserts with row locks and a
// lock timeout, and read-only transactions that reject writes. It does not
// provide snapshot isolation: committed rows are visible as soon as they
// commit. pg/storetest.Run keeps it honest against PgStore.
package memstore

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// Config configures the store.
type Config struct {
	// Partitions is P for partition assignment. Default 1.
	Partitions int
	// Clock is used for timestamps and lock timeouts. Default RealClock.
	Clock testkit.Clock
	// DefaultLockTimeout applies when TxOptions.LockTimeout is zero. Default 5s.
	DefaultLockTimeout time.Duration
}

// Hooks inject failures. Set them before the transaction they target starts.
type Hooks struct {
	// Begin fails Begin with the returned error.
	Begin func(opts pg.TxOptions) error
	// BeforeCommit models a definite commit failure: nothing is applied, the
	// transaction is rolled back, and Commit returns the error wrapped in
	// pg.ErrTxAborted.
	BeforeCommit func() error
	// AfterCommit models a lost acknowledgement: the commit is applied and
	// Commit returns the error unchanged.
	AfterCommit func() error
	// Rollback is returned by Rollback after the transaction is discarded.
	Rollback func() error
}

// Notification is one pg_notify recorded at commit.
type Notification struct{ Channel, Payload string }

// IdemKey identifies an idempotency row.
type IdemKey struct{ Scope, Key string }

// IdemRow is one committed idempotency row.
type IdemRow struct {
	RequestHash []byte
	Response    []byte
	Hits        int
	CreatedAt   time.Time
	ExpiresAt   time.Time
}

type seqKey struct{ topic, key string }

type inboxKey struct {
	group string
	id    uuid.UUID
}

// Store is the in-memory pg.Store.
type Store struct {
	cfg   Config
	Hooks Hooks

	mu         sync.Mutex
	outbox     []pg.OutboxEntry
	nextID     int64
	seq        map[seqKey]int64
	inbox      map[inboxKey]time.Time
	idem       map[IdemKey]*IdemRow
	notes      []Notification
	fencing    int64
	begun      int
	committed  int
	rolledBack int
	begins     []pg.TxOptions
	locks      lockTable
}

// New returns an empty store.
func New(cfg Config) *Store {
	if cfg.Partitions <= 0 {
		cfg.Partitions = 1
	}
	if cfg.Clock == nil {
		cfg.Clock = testkit.RealClock{}
	}
	if cfg.DefaultLockTimeout <= 0 {
		cfg.DefaultLockTimeout = pg.DefaultLockTimeout
	}
	return &Store{cfg: cfg, seq: map[seqKey]int64{}, inbox: map[inboxKey]time.Time{}, idem: map[IdemKey]*IdemRow{}}
}

// Clock returns the store's clock.
func (s *Store) Clock() testkit.Clock { return s.cfg.Clock }

// Partitions returns P.
func (s *Store) Partitions() int { return s.cfg.Partitions }

// Begin opens a transaction.
func (s *Store) Begin(ctx context.Context, opts pg.TxOptions) (pg.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.Hooks.Begin != nil {
		if err := s.Hooks.Begin(opts); err != nil {
			return nil, err
		}
	}
	lt := opts.LockTimeout
	if lt <= 0 {
		lt = s.cfg.DefaultLockTimeout
	}
	s.mu.Lock()
	s.begun++
	s.begins = append(s.begins, opts)
	s.mu.Unlock()
	return &Tx{s: s, opts: opts, lockTimeout: lt}, nil
}

// NextFencingToken returns the next value of a monotonic counter.
func (s *Store) NextFencingToken(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fencing++
	return s.fencing, nil
}

// Ping reports the context error, if any.
func (s *Store) Ping(ctx context.Context) error { return ctx.Err() }

// Outbox returns the committed outbox rows ordered by id.
func (s *Store) Outbox() []pg.OutboxEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]pg.OutboxEntry, len(s.outbox))
	copy(out, s.outbox)
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Inbox returns the committed inbox rows per group, each sorted by event ID.
func (s *Store) Inbox() map[string][]uuid.UUID {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string][]uuid.UUID{}
	for k := range s.inbox {
		out[k.group] = append(out[k.group], k.id)
	}
	for g := range out {
		ids := out[g]
		sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	}
	return out
}

// Idempotency returns copies of the committed idempotency rows.
func (s *Store) Idempotency() map[IdemKey]IdemRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[IdemKey]IdemRow, len(s.idem))
	for k, r := range s.idem {
		out[k] = IdemRow{RequestHash: cloneBytes(r.RequestHash), Response: cloneBytes(r.Response), Hits: r.Hits, CreatedAt: r.CreatedAt, ExpiresAt: r.ExpiresAt}
	}
	return out
}

// Notifications returns every pg_notify delivered by a committed transaction.
func (s *Store) Notifications() []Notification {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Notification, len(s.notes))
	copy(out, s.notes)
	return out
}

// Begun counts transactions opened.
func (s *Store) Begun() int { s.mu.Lock(); defer s.mu.Unlock(); return s.begun }

// Committed counts transactions committed.
func (s *Store) Committed() int { s.mu.Lock(); defer s.mu.Unlock(); return s.committed }

// RolledBack counts transactions rolled back, including aborted commits.
func (s *Store) RolledBack() int { s.mu.Lock(); defer s.mu.Unlock(); return s.rolledBack }

// Begins returns the TxOptions of every Begin in order, so tests can check
// the defaults the unit of work chose.
func (s *Store) Begins() []pg.TxOptions {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]pg.TxOptions, len(s.begins))
	copy(out, s.begins)
	return out
}

// PurgeIdempotency removes rows whose expires_at is before now, like the
// janitor, and returns how many it removed.
func (s *Store) PurgeIdempotency(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for k, r := range s.idem {
		if r.ExpiresAt.Before(now) {
			delete(s.idem, k)
			n++
		}
	}
	return n
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
