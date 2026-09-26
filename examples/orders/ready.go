package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/examples/orders/orders"
	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// readyMaxLag is the consumer lag above which /readyz fails (spec 9.2).
const readyMaxLag = 60 * time.Second

// readyChecks are the /readyz checks of spec 9.2: Postgres answers SELECT 1,
// Redis answers PING, every component reports Healthy, and no consumer
// group of this service lags more than readyMaxLag. The runtime is passed
// by pointer because the listener that serves /readyz is one of its
// components.
func readyChecks(pool *pgxpool.Pool, client *redis.Client, cfg redisx.Config, topics []string, rt *mediator.Runtime) []func(context.Context) error {
	return []func(context.Context) error{
		func(ctx context.Context) error {
			var one int
			if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
				return fmt.Errorf("postgres: %w", err)
			}
			return nil
		},
		func(ctx context.Context) error {
			if err := client.Ping(ctx).Err(); err != nil {
				return fmt.Errorf("redis: %w", err)
			}
			return nil
		},
		func(context.Context) error { return rt.Healthy() },
		func(ctx context.Context) error {
			lags, err := redisx.ConsumerLag(ctx, client, cfg, orders.Groups, topics)
			if err != nil {
				return fmt.Errorf("consumer lag: %w", err)
			}
			for _, l := range lags {
				if !l.Missing && l.OldestAge > readyMaxLag {
					return fmt.Errorf("consumer lag: %s/%s/p%d is %s behind", l.Group, l.Topic, l.Partition, l.OldestAge.Round(time.Second))
				}
			}
			return nil
		},
	}
}

// stdinEOF returns a context that is also cancelled when standard input
// reaches end of file. Windows cannot deliver SIGTERM to a child process,
// so integration tests start the service with SHUTDOWN_ON_STDIN_EOF=1 and
// close its stdin to request the same graceful shutdown a SIGTERM triggers
// on Unix (design notes, section 6).
func stdinEOF(ctx context.Context) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	return ctx
}
