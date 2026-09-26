package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// SQL of the write path, verbatim from spec 6.3, 6.5, and 6.6.
const (
	sqlOutboxSeq = `INSERT INTO mediator_stream_seq (topic, stream_key, next_seq)
VALUES ($1, $2, 1)
ON CONFLICT (topic, stream_key) DO UPDATE SET next_seq = mediator_stream_seq.next_seq + 1
RETURNING next_seq`
	sqlOutboxInsert = `INSERT INTO mediator_outbox
    (event_id, topic, stream_key, seq, partition, event_type, schema_ver, payload, headers)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`
	sqlInboxInsert = `INSERT INTO mediator_inbox (consumer_group, event_id) VALUES ($1, $2)
ON CONFLICT DO NOTHING RETURNING 1`
	sqlIdemReserve = `INSERT INTO mediator_idempotency (scope, key, request_hash, expires_at)
VALUES ($1, $2, $3, now() + $4)
ON CONFLICT (scope, key) DO UPDATE SET hits = mediator_idempotency.hits + 1
RETURNING hits, request_hash, response`
	sqlIdemStore   = `UPDATE mediator_idempotency SET response = $3 WHERE scope = $1 AND key = $2`
	sqlNotify      = `SELECT pg_notify($1, $2)`
	sqlFencingNext = `SELECT nextval('mediator_fencing_seq')`
)

// pgTx is the Tx over a pgx transaction.
type pgTx struct {
	tx         pgx.Tx
	readOnly   bool
	partitions int
}

// pgxTxer is implemented by transactions that wrap a pgx.Tx; TxFrom uses it.
type pgxTxer interface{ PgxTx() pgx.Tx }

// PgxTx returns the underlying pgx transaction.
func (t *pgTx) PgxTx() pgx.Tx { return t.tx }

// ReadOnly reports whether the transaction was opened read-only.
func (t *pgTx) ReadOnly() bool { return t.readOnly }

// Commit makes the transaction durable. Fault point pg.tx.commit before and
// after, so the ambiguous kind models a lost acknowledgement.
func (t *pgTx) Commit(ctx context.Context) error {
	if err := testkit.Fault(ctx, "pg.tx.commit"); err != nil {
		return err
	}
	if err := t.tx.Commit(ctx); err != nil {
		if errors.Is(err, pgx.ErrTxClosed) {
			return ErrTxClosed
		}
		if errors.Is(err, pgx.ErrTxCommitRollback) {
			return fmt.Errorf("%w: %w", ErrTxAborted, err)
		}
		return fmt.Errorf("pg: commit: %w", err)
	}
	return testkit.FaultAfter(ctx, "pg.tx.commit")
}

// Rollback discards the transaction. Rolling back a closed transaction is
// not an error.
func (t *pgTx) Rollback(ctx context.Context) error {
	if err := testkit.Fault(ctx, "pg.tx.rollback"); err != nil {
		return err
	}
	err := t.tx.Rollback(ctx)
	if err == nil || errors.Is(err, pgx.ErrTxClosed) {
		return nil
	}
	return fmt.Errorf("pg: rollback: %w", err)
}

// OutboxAppend takes the next per-key sequence number under the row lock of
// mediator_stream_seq and inserts the outbox row (6.3).
func (t *pgTx) OutboxAppend(ctx context.Context, env *mediator.Envelope, payload []byte) error {
	if err := testkit.Fault(ctx, "pg.outbox.seq"); err != nil {
		return err
	}
	if err := t.tx.QueryRow(ctx, sqlOutboxSeq, env.Topic, env.StreamKey).Scan(&env.Seq); err != nil {
		return txErr("outbox seq", err)
	}
	env.Partition = mediator.Partition(env.StreamKey, t.partitions)
	headers, err := encodeOutboxHeaders(env)
	if err != nil {
		return err
	}
	if err := testkit.Fault(ctx, "pg.outbox.insert"); err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, sqlOutboxInsert,
		env.ID, env.Topic, env.StreamKey, env.Seq, env.Partition, env.Type, env.SchemaVersion, payload, headers); err != nil {
		return txErr("outbox insert", err)
	}
	return testkit.FaultAfter(ctx, "pg.outbox.insert")
}

// InboxInsert records the event for the group; false means it was already
// there (6.5). A concurrent insert of the same key blocks until the first
// transaction ends.
func (t *pgTx) InboxInsert(ctx context.Context, group string, eventID uuid.UUID) (bool, error) {
	if err := testkit.Fault(ctx, "pg.inbox.insert"); err != nil {
		return false, err
	}
	var one int
	err := t.tx.QueryRow(ctx, sqlInboxInsert, group, eventID).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, txErr("inbox insert", err)
	}
	return true, nil
}

// IdempotencyReserve performs the upsert of 6.6 and returns the row.
func (t *pgTx) IdempotencyReserve(ctx context.Context, scope, key string, requestHash []byte, ttl time.Duration) (IdempotencyRow, error) {
	if err := testkit.Fault(ctx, "pg.idem.reserve"); err != nil {
		return IdempotencyRow{}, err
	}
	var row IdempotencyRow
	if err := t.tx.QueryRow(ctx, sqlIdemReserve, scope, key, requestHash, ttl).Scan(&row.Hits, &row.RequestHash, &row.Response); err != nil {
		return IdempotencyRow{}, txErr("idempotency reserve", err)
	}
	return row, nil
}

// IdempotencyStore records the response of a successful execution.
func (t *pgTx) IdempotencyStore(ctx context.Context, scope, key string, response []byte) error {
	if err := testkit.Fault(ctx, "pg.idem.store"); err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, sqlIdemStore, scope, key, response); err != nil {
		return txErr("idempotency store", err)
	}
	return testkit.FaultAfter(ctx, "pg.idem.store")
}

// Notify issues pg_notify inside the transaction; Postgres delivers it at
// commit and drops it on rollback.
func (t *pgTx) Notify(ctx context.Context, channel, payload string) error {
	if err := testkit.Fault(ctx, "pg.outbox.notify"); err != nil {
		return err
	}
	if _, err := t.tx.Exec(ctx, sqlNotify, channel, payload); err != nil {
		return txErr("notify", err)
	}
	return nil
}

// txErr wraps a statement error, mapping a closed pgx transaction to
// ErrTxClosed so both store implementations report it the same way.
func txErr(op string, err error) error {
	if errors.Is(err, pgx.ErrTxClosed) {
		return ErrTxClosed
	}
	return fmt.Errorf("pg: %s: %w", op, err)
}
