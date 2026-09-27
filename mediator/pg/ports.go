// Package pg is the Postgres persistence layer: unit of work, outbox, inbox,
// idempotency, relay, janitor, and migrations. Postgres is the source of
// truth; every durability guarantee reduces to "Postgres committed it".
//
// The behaviors in this package run against the Store and Tx interfaces so
// that testkit/memstore can stand in for Postgres in unit tests. PgStore is
// the real implementation over pgxpool.
package pg

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Propagation says how a unit of work relates to an ambient transaction.
type Propagation uint8

const (
	// Required joins the ambient transaction when there is one (default).
	Required Propagation = iota
	// RequiresNew always opens a new transaction on a new connection.
	RequiresNew
)

// TxOptions configures the transaction of a unit of work. A request type
// declares them through TxOptions() pg.TxOptions.
type TxOptions struct {
	Isolation   pgx.TxIsoLevel // default ReadCommitted for commands, RepeatableRead for queries and streams
	ReadOnly    bool           // default false for commands, true for queries and streams
	LockTimeout time.Duration  // SET LOCAL lock_timeout; default 5s
	Propagation Propagation    // Required (join ambient) or RequiresNew
}

// TxOptioner is the trait a request implements to override TxOptions.
type TxOptioner interface{ TxOptions() TxOptions }

// Store is what the unit of work, inbox, and idempotency behaviors need from
// the database. PgStore and testkit/memstore implement it.
type Store interface {
	// Begin opens a transaction. Fault point pg.tx.begin.
	Begin(ctx context.Context, opts TxOptions) (Tx, error)
	// NextFencingToken returns nextval('mediator_fencing_seq'). Fault point pg.lease.epoch.
	NextFencingToken(ctx context.Context) (int64, error)
	// Ping reports whether the database answers.
	Ping(ctx context.Context) error
}

// Tx is one open transaction. It is bound to one goroutine.
type Tx interface {
	// Commit makes the transaction durable. Fault point pg.tx.commit.
	Commit(ctx context.Context) error
	// Rollback discards the transaction. Fault point pg.tx.rollback.
	Rollback(ctx context.Context) error
	// ReadOnly reports whether the transaction was opened read-only.
	ReadOnly() bool
	// OutboxAppend takes the next per-key sequence number and inserts the
	// row (6.3). It sets env.Seq and env.Partition. Fault points
	// pg.outbox.seq and pg.outbox.insert.
	OutboxAppend(ctx context.Context, env *mediator.Envelope, payload []byte) error
	// InboxInsert records that the group processed the event. It returns
	// false when the row already existed (6.5). Fault point pg.inbox.insert.
	InboxInsert(ctx context.Context, group string, eventID uuid.UUID) (fresh bool, err error)
	// FencePartition records token as the epoch of (group, topic, partition)
	// when it is at least the stored one and reports whether it was accepted;
	// false means a higher token has been applied since, so the caller's lease
	// is stale (spec 7.2, G14). The row lock serializes the transactions of two
	// owners. Fault point pg.inbox.fence.
	FencePartition(ctx context.Context, group, topic string, partition int, token int64) (ok bool, err error)
	// IdempotencyReserve performs the upsert of 6.6 and returns the row.
	// Fault point pg.idem.reserve.
	IdempotencyReserve(ctx context.Context, scope, key string, requestHash []byte, ttl time.Duration) (IdempotencyRow, error)
	// IdempotencyStore records the response of a successful execution.
	// Fault point pg.idem.store.
	IdempotencyStore(ctx context.Context, scope, key string, response []byte) error
	// Notify issues pg_notify inside the transaction; Postgres delivers it
	// at commit. Fault point pg.outbox.notify.
	Notify(ctx context.Context, channel, payload string) error
}

// IdempotencyRow is the result of IdempotencyReserve.
type IdempotencyRow struct {
	Hits        int    // 0 means reserved now
	RequestHash []byte // hash stored by the first attempt
	Response    []byte // nil while NULL, i.e. reserved but not completed
}

// OutboxEntry is one relayed row: the unit of the relay protocol between pg
// and the stream sink.
type OutboxEntry struct {
	ID        int64
	Envelope  mediator.Envelope
	Payload   []byte
	CreatedAt time.Time
}

// StreamSink is the Redis side of the relay (6.4, 7.7). redisx.Streams
// implements it; pg never imports redisx.
type StreamSink interface {
	// Append adds the entries to the partition stream in order with the
	// envelope fields and outbox_id, and returns the stream ID of the last
	// entry. Fault point redis.xadd.
	Append(ctx context.Context, topic string, partition int, entries []OutboxEntry) (lastID string, err error)
	// Tail returns the stream ID and outbox ID of the last entry of the
	// partition stream. ok is false when the stream is missing or empty.
	// Fault point redis.stream.info.
	Tail(ctx context.Context, topic string, partition int) (lastID string, lastOutboxID int64, ok bool, err error)
	// EnsureGroups creates the consumer groups on the stream from ID 0 with
	// MKSTREAM when they do not exist, so consumers replay the retained
	// window after data loss. Fault point redis.stream.replay.
	EnsureGroups(ctx context.Context, topic string, partition int, groups []string) error
}

// StreamTrimmer is the optional Redis side of the janitor (6.8).
type StreamTrimmer interface {
	// TrimBefore removes stream entries older than minTime (XTRIM MINID ~).
	TrimBefore(ctx context.Context, topic string, partition int, minTime time.Time) error
}

// FencingSource issues monotonic lease epochs (7.2). Store satisfies it.
type FencingSource interface {
	NextFencingToken(ctx context.Context) (int64, error)
}

// NotifyChannel is the LISTEN/NOTIFY channel that wakes the relay. The
// payload is "<topic>:<partition>".
const NotifyChannel = "mediator_outbox"
