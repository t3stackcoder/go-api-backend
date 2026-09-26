package pg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// Retention defaults of 6.8.
const (
	DefaultRetention        = 7 * 24 * time.Hour
	DefaultJanitorInterval  = time.Minute
	DefaultJanitorBatchSize = 5000
)

// Retention SQL: each statement deletes one batch in its own short
// transaction, addressing rows by ctid so no large index scan is held.
const (
	sqlJanitorOutbox = `DELETE FROM mediator_outbox WHERE ctid = ANY(ARRAY(
    SELECT ctid FROM mediator_outbox WHERE published_at < now() - $1::interval LIMIT $2))`
	sqlJanitorInbox = `DELETE FROM mediator_inbox WHERE ctid = ANY(ARRAY(
    SELECT ctid FROM mediator_inbox WHERE processed_at < now() - $1::interval LIMIT $2))`
	sqlJanitorIdem = `DELETE FROM mediator_idempotency WHERE ctid = ANY(ARRAY(
    SELECT ctid FROM mediator_idempotency WHERE expires_at < now() LIMIT $1))`
	sqlJanitorLock   = `SELECT pg_try_advisory_lock(hashtext('mediator_janitor'))`
	sqlJanitorUnlock = `SELECT pg_advisory_unlock(hashtext('mediator_janitor'))`
)

// JanitorConfig configures the retention sweeps of 6.8.
type JanitorConfig struct {
	// Interval between sweeps. Default 1m.
	Interval time.Duration
	// OutboxRetention keeps published outbox rows (and, through the
	// trimmer, stream entries) this long. Default 7d.
	OutboxRetention time.Duration
	// InboxRetention keeps inbox rows this long. It must not be shorter
	// than OutboxRetention or an old redelivery could be reprocessed. Default 7d.
	InboxRetention time.Duration
	// BatchSize rows are deleted per statement. Default 5000.
	BatchSize int
	// Trimmer, when set, trims the partition streams of Topics x Partitions
	// at the outbox retention horizon. redisx implements it.
	Trimmer    StreamTrimmer
	Topics     []string
	Partitions int
	Logger     *slog.Logger
	Clock      testkit.Clock
}

func (c JanitorConfig) withDefaults() JanitorConfig {
	if c.Interval <= 0 {
		c.Interval = DefaultJanitorInterval
	}
	if c.OutboxRetention <= 0 {
		c.OutboxRetention = DefaultRetention
	}
	if c.InboxRetention <= 0 {
		c.InboxRetention = DefaultRetention
	}
	if c.BatchSize <= 0 {
		c.BatchSize = DefaultJanitorBatchSize
	}
	if c.Partitions <= 0 {
		c.Partitions = 1
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Clock == nil {
		c.Clock = testkit.RealClock{}
	}
	return c
}

// ValidateRetention rejects an inbox retention shorter than the outbox
// (stream) retention, because a redelivery older than the inbox window
// would be reprocessed (6.8). Build calls it through the behavior package.
func ValidateRetention(inbox, outbox time.Duration) error {
	if inbox < outbox {
		return fmt.Errorf("pg: inbox retention %s must not be shorter than outbox retention %s", inbox, outbox)
	}
	return nil
}

// SweepResult reports what one sweep removed.
type SweepResult struct {
	Outbox      int64
	Inbox       int64
	Idempotency int64
	// Trimmed counts the partition streams trimmed.
	Trimmed int
	// Skipped is true when another node held the janitor lock.
	Skipped bool
}

// Janitor deletes expired outbox, inbox, and idempotency rows every
// Interval, on one node at a time (advisory lock), in batches of BatchSize
// with one short transaction per batch. It is a mediator.Component.
type Janitor struct {
	pool    *pgxpool.Pool
	cfg     JanitorConfig
	lastErr atomic.Pointer[error]
	sweeps  atomic.Int64
}

// NewJanitor returns a janitor over pool.
func NewJanitor(pool *pgxpool.Pool, cfg JanitorConfig) *Janitor {
	return &Janitor{pool: pool, cfg: cfg.withDefaults()}
}

// Run sweeps immediately, then every Interval, until ctx is canceled.
func (j *Janitor) Run(ctx context.Context) error {
	for {
		if _, err := j.Sweep(ctx); err != nil && ctx.Err() == nil {
			j.cfg.Logger.Error("janitor: sweep failed", "error", err)
		}
		select {
		case <-j.cfg.Clock.After(j.cfg.Interval):
		case <-ctx.Done():
			return nil
		}
	}
}

// Healthy returns the error of the last sweep, or nil.
func (j *Janitor) Healthy() error {
	if e := j.lastErr.Load(); e != nil {
		return *e
	}
	return nil
}

// Sweeps counts completed sweeps, including skipped ones.
func (j *Janitor) Sweeps() int64 { return j.sweeps.Load() }

// Sweep runs one retention pass now. It returns Skipped when another node
// holds the lock.
func (j *Janitor) Sweep(ctx context.Context) (SweepResult, error) {
	res, err := j.sweep(ctx)
	j.sweeps.Add(1)
	if err != nil {
		j.lastErr.Store(&err)
	} else {
		j.lastErr.Store(nil)
	}
	return res, err
}

func (j *Janitor) sweep(ctx context.Context) (SweepResult, error) {
	var res SweepResult
	if err := ValidateRetention(j.cfg.InboxRetention, j.cfg.OutboxRetention); err != nil {
		return res, err
	}
	conn, err := j.pool.Acquire(ctx)
	if err != nil {
		return res, fmt.Errorf("janitor: acquire connection: %w", err)
	}
	var got bool
	if err := conn.QueryRow(ctx, sqlJanitorLock).Scan(&got); err != nil {
		conn.Release()
		return res, fmt.Errorf("janitor: lock: %w", err)
	}
	if !got {
		conn.Release()
		res.Skipped = true
		return res, nil
	}
	defer func() {
		// Release the session lock; if that fails, drop the connection so
		// the lock dies with it instead of leaking into the pool.
		if _, err := conn.Exec(context.WithoutCancel(ctx), sqlJanitorUnlock); err != nil {
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = conn.Hijack().Close(cctx)
			return
		}
		conn.Release()
	}()
	if res.Outbox, err = deleteBatches(ctx, conn, sqlJanitorOutbox, j.cfg.BatchSize, j.cfg.OutboxRetention); err != nil {
		return res, err
	}
	if res.Inbox, err = deleteBatches(ctx, conn, sqlJanitorInbox, j.cfg.BatchSize, j.cfg.InboxRetention); err != nil {
		return res, err
	}
	if res.Idempotency, err = deleteBatches(ctx, conn, sqlJanitorIdem, j.cfg.BatchSize); err != nil {
		return res, err
	}
	if j.cfg.Trimmer != nil {
		horizon := j.cfg.Clock.Now().Add(-j.cfg.OutboxRetention)
		var errs []error
		for _, topic := range j.cfg.Topics {
			for p := 0; p < j.cfg.Partitions; p++ {
				if err := j.cfg.Trimmer.TrimBefore(ctx, topic, p, horizon); err != nil {
					errs = append(errs, fmt.Errorf("janitor: trim %s:%d: %w", topic, p, err))
					continue
				}
				res.Trimmed++
			}
		}
		if len(errs) > 0 {
			return res, errors.Join(errs...)
		}
	}
	return res, nil
}

// execer is satisfied by *pgxpool.Pool, *pgxpool.Conn, *pgx.Conn, and pgx.Tx.
type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// deleteBatches runs a batched DELETE (whose last parameter is the batch
// size) until a batch comes back short, and returns the total removed.
func deleteBatches(ctx context.Context, ex execer, sql string, batch int, args ...any) (int64, error) {
	var total int64
	args = append(args, batch)
	for {
		if err := testkit.Fault(ctx, "pg.janitor.delete"); err != nil {
			return total, err
		}
		tag, err := ex.Exec(ctx, sql, args...)
		if err != nil {
			return total, fmt.Errorf("janitor: delete: %w", err)
		}
		n := tag.RowsAffected()
		total += n
		if n < int64(batch) {
			return total, nil
		}
	}
}
