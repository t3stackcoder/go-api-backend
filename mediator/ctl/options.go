package ctl

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// Environment variables consulted for zero fields of Options. The CLI is the
// application edge; the framework packages themselves never read these.
const (
	EnvPGURL         = "PG_URL"
	EnvRedisAddr     = "REDIS_ADDR"
	EnvRedisUsername = "REDIS_USERNAME"
	EnvRedisPassword = "REDIS_PASSWORD"
	EnvRedisDB       = "REDIS_DB"
	EnvRedisPrefix   = "REDIS_PREFIX"
)

// Options configures Main. Every zero field has a default: Stdout and Stderr
// are os.Stdout and os.Stderr, Now is time.Now, PGURL comes from PG_URL, and
// the Addr, Username, Password, DB, and Prefix of Redis come from REDIS_ADDR,
// REDIS_USERNAME, REDIS_PASSWORD, REDIS_DB, and REDIS_PREFIX.
type Options struct {
	Stdout, Stderr io.Writer
	// PGURL is the Postgres connection string. Default $PG_URL.
	PGURL string
	// Redis configures the client and the key prefix. Zero connection fields
	// default from the environment; the rest through redisx defaults.
	Redis redisx.Config
	// Registry supplies the built mediator for names, openapi export, and
	// the default groups and topics of consumer lag. Nil means those
	// commands report that no registry is linked.
	Registry func() (*mediator.Mediator, error)
	// OpenAPI is the generator configuration of openapi export; --title,
	// --version, and --prefix override its fields.
	OpenAPI openapi.Config
	// Now supplies the clock for age computations in the rendering. Default time.Now.
	Now func() time.Time

	// Seams for tests. Zero values open real connections, run the real
	// generator, and read os.Getenv.
	openPostgres func(ctx context.Context, url string) (Postgres, error)
	openRedis    func(ctx context.Context, cfg redisx.Config) (Redis, error)
	export       func(m *mediator.Mediator, cfg openapi.Config, out string) error
	getenv       func(string) string
}

// withDefaults fills every zero field. The returned error is a malformed
// REDIS_DB; it is reported only when a command opens Redis.
func (o Options) withDefaults() (Options, error) {
	if o.Stdout == nil {
		o.Stdout = os.Stdout
	}
	if o.Stderr == nil {
		o.Stderr = os.Stderr
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.getenv == nil {
		o.getenv = os.Getenv
	}
	if o.openPostgres == nil {
		o.openPostgres = openPostgres
	}
	if o.openRedis == nil {
		o.openRedis = openRedis
	}
	if o.export == nil {
		o.export = exportOpenAPI
	}
	if o.PGURL == "" {
		o.PGURL = o.getenv(EnvPGURL)
	}
	if o.Redis.Addr == "" {
		o.Redis.Addr = o.getenv(EnvRedisAddr)
	}
	if o.Redis.Username == "" {
		o.Redis.Username = o.getenv(EnvRedisUsername)
	}
	if o.Redis.Password == "" {
		o.Redis.Password = o.getenv(EnvRedisPassword)
	}
	if o.Redis.Prefix == "" {
		o.Redis.Prefix = o.getenv(EnvRedisPrefix)
	}
	var err error
	if o.Redis.DB == 0 {
		if s := o.getenv(EnvRedisDB); s != "" {
			o.Redis.DB, err = strconv.Atoi(s)
			if err != nil {
				err = fmt.Errorf("%s=%q is not an integer", EnvRedisDB, s)
			}
		}
	}
	return o, err
}

// Postgres is the set of Postgres operations the commands use. NewPostgres
// adapts a pool; tests substitute a fake.
type Postgres interface {
	Migrate(ctx context.Context) error
	MigrateDown(ctx context.Context, toVersion int) error
	MigrationStatus(ctx context.Context) ([]pg.MigrationInfo, error)
	OutboxStats(ctx context.Context) ([]pg.PartitionStats, error)
	OutboxReplay(ctx context.Context, sink pg.StreamSink, topic string, partition int, fromID int64) (int, error)
	OutboxReshard(ctx context.Context, partitions int) (int64, error)
	InboxPurge(ctx context.Context, olderThan time.Duration) (int64, error)
	IdemPurge(ctx context.Context) (int64, error)
	IdemShow(ctx context.Context, scope, key string) (pg.IdempotencyInfo, error)
	Close() error
}

// Redis is the set of Redis operations the commands use. NewRedis adapts a
// client; tests substitute a fake.
type Redis interface {
	DLQList(ctx context.Context, group string) ([]redisx.DLQEntry, error)
	DLQRequeue(ctx context.Context, group, id string) error
	DLQDrop(ctx context.Context, group, id string) error
	PartitionSkip(ctx context.Context, group, topic string, partition int, id string) error
	LeaseList(ctx context.Context, group string) ([]redisx.LeaseInfo, error)
	LeaseRelease(ctx context.Context, group, topic string, partition int) error
	ConsumerLag(ctx context.Context, groups, topics []string) ([]redisx.LagInfo, error)
	// Sink is the stream sink outbox replay appends to.
	Sink() pg.StreamSink
	Close() error
}

// The adapters satisfy the seams the commands are written against.
var (
	_ Postgres = pgPool{}
	_ Redis    = redisClient{}
)

// pgPool adapts a pgxpool.Pool to Postgres by delegating to package pg.
type pgPool struct{ pool *pgxpool.Pool }

// NewPostgres returns the Postgres operations over pool. Close closes the pool.
func NewPostgres(pool *pgxpool.Pool) Postgres { return pgPool{pool: pool} }

func (p pgPool) Migrate(ctx context.Context) error { return pg.Migrate(ctx, p.pool) }
func (p pgPool) MigrateDown(ctx context.Context, toVersion int) error {
	return pg.MigrateDown(ctx, p.pool, toVersion)
}
func (p pgPool) MigrationStatus(ctx context.Context) ([]pg.MigrationInfo, error) {
	return pg.MigrationStatus(ctx, p.pool)
}
func (p pgPool) OutboxStats(ctx context.Context) ([]pg.PartitionStats, error) {
	return pg.OutboxStats(ctx, p.pool)
}
func (p pgPool) OutboxReplay(ctx context.Context, sink pg.StreamSink, topic string, partition int, fromID int64) (int, error) {
	return pg.OutboxReplay(ctx, p.pool, sink, topic, partition, fromID)
}
func (p pgPool) OutboxReshard(ctx context.Context, partitions int) (int64, error) {
	return pg.OutboxReshard(ctx, p.pool, partitions)
}
func (p pgPool) InboxPurge(ctx context.Context, olderThan time.Duration) (int64, error) {
	return pg.InboxPurge(ctx, p.pool, olderThan)
}
func (p pgPool) IdemPurge(ctx context.Context) (int64, error) { return pg.IdemPurge(ctx, p.pool) }
func (p pgPool) IdemShow(ctx context.Context, scope, key string) (pg.IdempotencyInfo, error) {
	return pg.IdemShow(ctx, p.pool, scope, key)
}
func (p pgPool) Close() error {
	p.pool.Close()
	return nil
}

// redisClient adapts a redis.Client to Redis by delegating to package redisx.
type redisClient struct {
	client *redis.Client
	cfg    redisx.Config
}

// NewRedis returns the Redis operations over client with the prefix and
// partition count of cfg. Close closes the client.
func NewRedis(client *redis.Client, cfg redisx.Config) Redis {
	return redisClient{client: client, cfg: cfg.WithDefaults()}
}

func (r redisClient) DLQList(ctx context.Context, group string) ([]redisx.DLQEntry, error) {
	return redisx.DLQList(ctx, r.client, r.cfg, group)
}
func (r redisClient) DLQRequeue(ctx context.Context, group, id string) error {
	return redisx.DLQRequeue(ctx, r.client, r.cfg, group, id)
}
func (r redisClient) DLQDrop(ctx context.Context, group, id string) error {
	return redisx.DLQDrop(ctx, r.client, r.cfg, group, id)
}
func (r redisClient) PartitionSkip(ctx context.Context, group, topic string, partition int, id string) error {
	return redisx.PartitionSkip(ctx, r.client, r.cfg, group, topic, partition, id)
}
func (r redisClient) LeaseList(ctx context.Context, group string) ([]redisx.LeaseInfo, error) {
	return redisx.LeaseList(ctx, r.client, r.cfg, group)
}
func (r redisClient) LeaseRelease(ctx context.Context, group, topic string, partition int) error {
	return redisx.LeaseRelease(ctx, r.client, r.cfg, group, topic, partition)
}
func (r redisClient) ConsumerLag(ctx context.Context, groups, topics []string) ([]redisx.LagInfo, error) {
	return redisx.ConsumerLag(ctx, r.client, r.cfg, groups, topics)
}
func (r redisClient) Sink() pg.StreamSink { return redisx.NewStreams(r.client, r.cfg) }
func (r redisClient) Close() error        { return r.client.Close() }

// openPostgres is the default Postgres opener: a small pool named after the tool.
func openPostgres(ctx context.Context, url string) (Postgres, error) {
	pool, err := pg.NewPool(ctx, url, pg.PoolConfig{MaxConns: 4, ApplicationName: "mediatorctl"})
	if err != nil {
		return nil, err
	}
	return NewPostgres(pool), nil
}

// openRedis is the default Redis opener.
func openRedis(ctx context.Context, cfg redisx.Config) (Redis, error) {
	client, err := redisx.NewClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return NewRedis(client, cfg), nil
}

// exportOpenAPI is the default exporter: generate the document and write it.
func exportOpenAPI(m *mediator.Mediator, cfg openapi.Config, out string) error {
	doc, err := openapi.Generate(m, cfg)
	if err != nil {
		return err
	}
	return doc.WriteFile(out)
}
