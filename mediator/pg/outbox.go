package pg

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// outboxHeaders is the document stored in mediator_outbox.headers. The
// table of 6.2 has no columns for the correlation, causation, trace, and
// occurrence fields of the envelope, so they travel in the headers document
// next to the user headers ("h"); the relay restores them into the Envelope.
type outboxHeaders struct {
	OccurredAt    time.Time         `json:"at"`
	CorrelationID string            `json:"corr,omitempty"`
	CausationID   string            `json:"cause,omitempty"`
	TraceParent   string            `json:"trace,omitempty"`
	Headers       map[string]string `json:"h,omitempty"`
}

func encodeOutboxHeaders(env *mediator.Envelope) ([]byte, error) {
	b, err := json.Marshal(outboxHeaders{
		OccurredAt: env.OccurredAt, CorrelationID: env.CorrelationID, CausationID: env.CausationID,
		TraceParent: env.TraceParent, Headers: env.Headers,
	})
	if err != nil {
		return nil, fmt.Errorf("pg: encode outbox headers: %w", err)
	}
	return b, nil
}

func decodeOutboxHeaders(raw []byte, env *mediator.Envelope) error {
	var h outboxHeaders
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &h); err != nil {
			return fmt.Errorf("pg: decode outbox headers: %w", err)
		}
	}
	env.OccurredAt, env.CorrelationID, env.CausationID, env.TraceParent, env.Headers =
		h.OccurredAt, h.CorrelationID, h.CausationID, h.TraceParent, h.Headers
	return nil
}

// Outbox read SQL shared by the relay and the operations functions.
const (
	sqlOutboxColumns = `id, event_id, topic, stream_key, seq, partition, event_type, schema_ver, payload, headers, created_at`
	sqlRelaySelect   = `SELECT ` + sqlOutboxColumns + `
FROM mediator_outbox
WHERE topic = $1 AND partition = $2 AND published_at IS NULL
ORDER BY id
LIMIT $3
FOR UPDATE SKIP LOCKED`
	sqlOutboxPublishedAfter = `SELECT ` + sqlOutboxColumns + `
FROM mediator_outbox
WHERE topic = $1 AND partition = $2 AND published_at IS NOT NULL AND id > $3
ORDER BY id
LIMIT $4`
	sqlRelayMark    = `UPDATE mediator_outbox SET published_at = now() WHERE id = ANY($1)`
	sqlCursorUpsert = `INSERT INTO mediator_relay_cursor (topic, partition, last_outbox_id, last_stream_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT (topic, partition) DO UPDATE
    SET last_outbox_id = EXCLUDED.last_outbox_id,
        last_stream_id = EXCLUDED.last_stream_id,
        updated_at = now()`
	sqlCursorSelect = `SELECT last_outbox_id, last_stream_id FROM mediator_relay_cursor WHERE topic = $1 AND partition = $2`
	sqlOutboxStats  = `SELECT topic, partition, count(*), extract(epoch FROM now() - min(created_at))::float8
FROM mediator_outbox WHERE published_at IS NULL GROUP BY topic, partition ORDER BY topic, partition`
	sqlOutboxGauges = `SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)), 0)::float8
FROM mediator_outbox WHERE topic = $1 AND partition = $2 AND published_at IS NULL`
)

// querier is satisfied by *pgxpool.Pool, *pgxpool.Conn, *pgx.Conn, and pgx.Tx.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// scanOutboxEntry reads one row of sqlOutboxColumns.
func scanOutboxEntry(rows pgx.Rows) (OutboxEntry, error) {
	var e OutboxEntry
	var headers []byte
	if err := rows.Scan(&e.ID, &e.Envelope.ID, &e.Envelope.Topic, &e.Envelope.StreamKey, &e.Envelope.Seq,
		&e.Envelope.Partition, &e.Envelope.Type, &e.Envelope.SchemaVersion, &e.Payload, &headers, &e.CreatedAt); err != nil {
		return OutboxEntry{}, fmt.Errorf("pg: scan outbox row: %w", err)
	}
	if err := decodeOutboxHeaders(headers, &e.Envelope); err != nil {
		return OutboxEntry{}, err
	}
	return e, nil
}

// selectOutbox runs a sqlOutboxColumns query and scans every row.
func selectOutbox(ctx context.Context, q querier, sql string, args ...any) ([]OutboxEntry, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("pg: select outbox: %w", err)
	}
	defer rows.Close()
	var out []OutboxEntry
	for rows.Next() {
		e, err := scanOutboxEntry(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg: select outbox: %w", err)
	}
	return out, nil
}

// PartitionStats is one row of OutboxStats: the unpublished backlog of one
// (topic, partition).
type PartitionStats struct {
	Topic       string
	Partition   int
	Unpublished int64
	// OldestAge is how long the oldest unpublished row has been waiting.
	OldestAge time.Duration
}

// OutboxStats returns the unpublished count and oldest age per partition
// (mediatorctl outbox stats). Partitions without a backlog are omitted.
func OutboxStats(ctx context.Context, pool *pgxpool.Pool) ([]PartitionStats, error) {
	rows, err := pool.Query(ctx, sqlOutboxStats)
	if err != nil {
		return nil, fmt.Errorf("pg: outbox stats: %w", err)
	}
	defer rows.Close()
	var out []PartitionStats
	for rows.Next() {
		var s PartitionStats
		var age float64
		if err := rows.Scan(&s.Topic, &s.Partition, &s.Unpublished, &age); err != nil {
			return nil, fmt.Errorf("pg: outbox stats: %w", err)
		}
		s.OldestAge = time.Duration(age * float64(time.Second))
		out = append(out, s)
	}
	return out, rows.Err()
}

// OutboxReplay re-appends every published row of the partition with id >=
// fromID to the sink, in id order (mediatorctl outbox replay). It returns
// the number of rows appended. The relay cursor is left alone: a stream
// ahead of its cursor is the normal at-least-once case (7.7).
func OutboxReplay(ctx context.Context, pool *pgxpool.Pool, sink StreamSink, topic string, partition int, fromID int64) (int, error) {
	const batch = 100
	after := fromID - 1
	n := 0
	for {
		rows, err := selectOutbox(ctx, pool, sqlOutboxPublishedAfter, topic, partition, after, batch)
		if err != nil {
			return n, err
		}
		if len(rows) == 0 {
			return n, nil
		}
		if _, err := sink.Append(ctx, topic, partition, rows); err != nil {
			return n, fmt.Errorf("pg: replay append: %w", err)
		}
		n += len(rows)
		after = rows[len(rows)-1].ID
		if len(rows) < batch {
			return n, nil
		}
	}
}

// OutboxReshard recomputes the partition of every outbox row for a new P
// and resets the relay cursors, in one transaction (mediatorctl outbox
// reshard). It returns the number of rows moved. Consumers and relays must
// be stopped while it runs, because partition assignment decides which
// stream carries which key; afterwards start them with the new P and let
// CheckPartitions confirm it.
func OutboxReshard(ctx context.Context, pool *pgxpool.Pool, newPartitions int) (int64, error) {
	if newPartitions <= 0 {
		return 0, errors.New("pg: partitions must be positive")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("pg: reshard: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // closed after commit
	rows, err := tx.Query(ctx, `SELECT DISTINCT stream_key FROM mediator_outbox`)
	if err != nil {
		return 0, fmt.Errorf("pg: reshard: %w", err)
	}
	byPartition := map[int][]string{}
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return 0, fmt.Errorf("pg: reshard: %w", err)
		}
		p := mediator.Partition(key, newPartitions)
		byPartition[p] = append(byPartition[p], key)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("pg: reshard: %w", err)
	}
	var moved int64
	for p, keys := range byPartition {
		tag, err := tx.Exec(ctx, `UPDATE mediator_outbox SET partition = $1 WHERE stream_key = ANY($2) AND partition <> $1`, p, keys)
		if err != nil {
			return 0, fmt.Errorf("pg: reshard: %w", err)
		}
		moved += tag.RowsAffected()
	}
	if _, err := tx.Exec(ctx, `DELETE FROM mediator_relay_cursor`); err != nil {
		return 0, fmt.Errorf("pg: reshard: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("pg: reshard: %w", err)
	}
	return moved, nil
}

// CheckPartitions is the startup check of 6.3: the configured P must not be
// smaller than any partition already recorded in mediator_relay_cursor or
// carried by an unpublished outbox row, because such rows would never be
// relayed. P is not stored; it is derived as max(partition)+1 <= P, so
// shrinking P is allowed only through OutboxReshard.
func CheckPartitions(ctx context.Context, pool *pgxpool.Pool, partitions int) error {
	if partitions <= 0 {
		return errors.New("pg: partitions must be positive")
	}
	var maxCursor, maxUnpublished *int
	if err := pool.QueryRow(ctx, `SELECT max(partition) FROM mediator_relay_cursor`).Scan(&maxCursor); err != nil {
		return fmt.Errorf("pg: check partitions: %w", err)
	}
	if err := pool.QueryRow(ctx, `SELECT max(partition) FROM mediator_outbox WHERE published_at IS NULL`).Scan(&maxUnpublished); err != nil {
		return fmt.Errorf("pg: check partitions: %w", err)
	}
	if maxCursor != nil && *maxCursor >= partitions {
		return fmt.Errorf("pg: configured partitions %d but relay cursor has partition %d; run outbox reshard", partitions, *maxCursor)
	}
	if maxUnpublished != nil && *maxUnpublished >= partitions {
		return fmt.Errorf("pg: configured partitions %d but unpublished outbox rows have partition %d; run outbox reshard", partitions, *maxUnpublished)
	}
	return nil
}
