package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// InboxPurge deletes inbox rows processed more than olderThan ago, in
// batches (mediatorctl inbox purge). It returns the number removed.
func InboxPurge(ctx context.Context, pool *pgxpool.Pool, olderThan time.Duration) (int64, error) {
	return deleteBatches(ctx, pool, sqlJanitorInbox, DefaultJanitorBatchSize, olderThan)
}

// IdemPurge deletes expired idempotency rows in batches (mediatorctl idem
// purge). It returns the number removed.
func IdemPurge(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	return deleteBatches(ctx, pool, sqlJanitorIdem, DefaultJanitorBatchSize)
}

// IdempotencyInfo is one mediator_idempotency row as shown by IdemShow.
type IdempotencyInfo struct {
	Scope       string
	Key         string
	RequestHash []byte
	// Response is the stored JSON, nil while NULL.
	Response  []byte
	Hits      int
	CreatedAt time.Time
	ExpiresAt time.Time
}

// IdemShow returns the idempotency row of (scope, key) (mediatorctl idem
// show), or an error with CodeNotFound.
func IdemShow(ctx context.Context, pool *pgxpool.Pool, scope, key string) (IdempotencyInfo, error) {
	const sql = `SELECT scope, key, request_hash, response, hits, created_at, expires_at
FROM mediator_idempotency WHERE scope = $1 AND key = $2`
	var info IdempotencyInfo
	err := pool.QueryRow(ctx, sql, scope, key).Scan(&info.Scope, &info.Key, &info.RequestHash, &info.Response, &info.Hits, &info.CreatedAt, &info.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return IdempotencyInfo{}, mediator.E(mediator.CodeNotFound, fmt.Sprintf("no idempotency row for scope %q key %q", scope, key))
	}
	if err != nil {
		return IdempotencyInfo{}, fmt.Errorf("pg: idempotency show: %w", err)
	}
	return info, nil
}
