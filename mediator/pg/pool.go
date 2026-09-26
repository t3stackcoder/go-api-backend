package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PoolConfig tunes the connection pool. Zero values take the defaults noted
// on each field.
type PoolConfig struct {
	// MaxConns is the pool size. 0 keeps the pgxpool default (max of 4 and NumCPU).
	MaxConns int32
	// MinConns is the number of connections kept open. Default 0.
	MinConns int32
	// MaxConnLifetime closes a connection after this age. Default 1h.
	MaxConnLifetime time.Duration
	// MaxConnIdleTime closes an idle connection after this time. Default 30m.
	MaxConnIdleTime time.Duration
	// HealthCheckPeriod is the interval of the idle-connection health check. Default 30s.
	HealthCheckPeriod time.Duration
	// ConnectTimeout bounds each connection attempt. Default 5s.
	ConnectTimeout time.Duration
	// ApplicationName is reported in pg_stat_activity. Default "mediator".
	ApplicationName string
	// SearchPath, when set, becomes the session search_path of every
	// connection. Integration tests use it to isolate a schema per test.
	SearchPath string
}

// NewPool parses url, applies cfg, opens the pool, and pings the database
// once so that a misconfiguration surfaces at startup.
func NewPool(ctx context.Context, url string, cfg PoolConfig) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("pg: parse pool config: %w", err)
	}
	applyPoolConfig(pc, cfg)
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("pg: open pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pg: ping: %w", err)
	}
	return pool, nil
}

// applyPoolConfig copies cfg onto a parsed pgxpool config, filling defaults.
func applyPoolConfig(pc *pgxpool.Config, cfg PoolConfig) {
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	if cfg.MinConns > 0 {
		pc.MinConns = cfg.MinConns
	}
	if cfg.MaxConnLifetime > 0 {
		pc.MaxConnLifetime = cfg.MaxConnLifetime
	}
	if cfg.MaxConnIdleTime > 0 {
		pc.MaxConnIdleTime = cfg.MaxConnIdleTime
	}
	pc.HealthCheckPeriod = 30 * time.Second
	if cfg.HealthCheckPeriod > 0 {
		pc.HealthCheckPeriod = cfg.HealthCheckPeriod
	}
	pc.ConnConfig.ConnectTimeout = 5 * time.Second
	if cfg.ConnectTimeout > 0 {
		pc.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	}
	if pc.ConnConfig.RuntimeParams == nil {
		pc.ConnConfig.RuntimeParams = map[string]string{}
	}
	app := cfg.ApplicationName
	if app == "" {
		app = "mediator"
	}
	pc.ConnConfig.RuntimeParams["application_name"] = app
	if cfg.SearchPath != "" {
		pc.ConnConfig.RuntimeParams["search_path"] = cfg.SearchPath
	}
}
