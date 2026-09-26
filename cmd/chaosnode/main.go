// Command chaosnode is the node binary of the chaos tier (spec 11.6): it
// registers the workload request types and consumers on the full standard
// behavior chain over Postgres and Redis, serves them over HTTP, runs the
// relay, consumers, janitor, and remote dispatch, and exposes admin
// endpoints under /chaos/ for the chaos controller: the injectable clock,
// fault points (faultinject builds), the remote server and relay switches,
// and statistics. See node in admin.go for the endpoint list.
//
// Configuration comes from the environment: NODE_ID, PG_URL, REDIS_ADDR,
// HTTP_ADDR, and the optional CHAOS_GROUPS, CHAOS_REMOTE_HANDLERS,
// CHAOS_DISPATCH, CHAOS_STRICT_PROJECTION, CHAOS_PARTITIONS, CHAOS_DRAIN,
// CHAOS_REQUEST_TIMEOUT, CHAOS_READY_MAX_LAG, REDIS_PREFIX,
// REDIS_ASSUME_NOEVICTION, LOG_LEVEL (see config.go).
//
// Remote dispatch: the node builds two mediators over the same store. The
// full mediator handles every workload request locally and backs the
// consumers and the remote server. The proxy mediator Declares the workload
// commands (except those named in CHAOS_REMOTE_HANDLERS) so that Send
// dispatches them to whichever node currently advertises them. PUT
// /chaos/handlers?dispatch=remote switches the HTTP routes to the proxy;
// PUT /chaos/remote?serve=false|true stops or starts this node's remote
// server. The remote-send workload puts every node in remote mode and keeps
// exactly one node serving; handler-rotate moves the serving flag.
//
// Logging is slog text on stdout. Every committed consumer apply logs one
// record "fencing=<n> partition=<p> group=<g> node=<id> topic=<t>", and a
// "goroutines=<n>" marker is logged every 5 s, for the chaos checkers.
// SIGTERM drains and exits 0 on a clean shutdown (G16).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

func main() {
	cfg, err := loadConfig()
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(logger)
	if err != nil {
		logger.Error("configuration", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, cfg, logger)
	stop()
	os.Exit(code)
}

// closerFunc adapts a close function to io.Closer.
type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// retry calls f until it succeeds or ctx ends, with exponential backoff
// from 250 ms to 5 s, logging every failure.
func retry(ctx context.Context, logger *slog.Logger, what string, f func(context.Context) error) error {
	wait := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := f(actx)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		logger.Warn(what+" failed; retrying", "attempt", attempt, "wait", wait.String(), "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: %w", what, ctx.Err())
		case <-time.After(wait):
		}
		if wait *= 2; wait > 5*time.Second {
			wait = 5 * time.Second
		}
	}
}

// migrate applies the framework and workload migrations and seeds the
// bank, under an advisory lock so three nodes starting at once serialize.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := pg.Migrate(ctx, pool); err != nil {
		return err
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext('chaosnode_migrate'))`); err != nil {
		return fmt.Errorf("lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(hashtext('chaosnode_migrate'))`)
	}()
	if err := workload.Migrate(ctx, pool); err != nil {
		return err
	}
	return workload.SeedBank(ctx, pool)
}

func run(ctx context.Context, cfg config, logger *slog.Logger) int {
	logger.Info("chaosnode starting", "node", cfg.NodeID, "http", cfg.HTTPAddr, "pg", cfg.PGURL, "redis", cfg.RedisAddr,
		"groups", describeGroups(cfg.Groups), "dispatch", cfg.Dispatch, "partitions", cfg.Partitions,
		"fault_injection", faultInjectionEnabled, "strict_projection", cfg.StrictProjection)
	clock := testkit.NewOffsetClock(testkit.RealClock{})

	// Postgres: pool, migrations, store.
	var pool *pgxpool.Pool
	err := retry(ctx, logger, "postgres connect", func(ctx context.Context) error {
		p, err := pg.NewPool(ctx, cfg.PGURL, pg.PoolConfig{MaxConns: 32, MinConns: 2, ApplicationName: "chaosnode-" + cfg.NodeID})
		if err != nil {
			return err
		}
		pool = p
		return nil
	})
	if err != nil {
		logger.Error("postgres", "error", err)
		return 1
	}
	if err := retry(ctx, logger, "migrate", func(ctx context.Context) error { return migrate(ctx, pool) }); err != nil {
		logger.Error("migrate", "error", err)
		pool.Close()
		return 1
	}
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: cfg.Partitions})
	if err := retry(ctx, logger, "partition check", func(ctx context.Context) error { return pg.CheckPartitions(ctx, pool, cfg.Partitions) }); err != nil {
		logger.Error("partitions", "error", err)
		pool.Close()
		return 1
	}

	// Redis.
	rcfg := redisx.Config{Addr: cfg.RedisAddr, Prefix: cfg.RedisPrefix, NodeID: cfg.NodeID, PartitionsPerTopic: cfg.Partitions, AssumeNoEviction: cfg.AssumeNoEviction}.WithDefaults()
	var client *redis.Client
	err = retry(ctx, logger, "redis connect", func(ctx context.Context) error {
		c, err := redisx.NewClient(ctx, rcfg)
		if err != nil {
			return err
		}
		client = c
		return nil
	})
	if err != nil {
		logger.Error("redis", "error", err)
		pool.Close()
		return 1
	}
	if cfg.AssumeNoEviction {
		logger.Warn("maxmemory-policy check skipped", "assume_noeviction", true)
	} else {
		var evErr error
		_ = retry(ctx, logger, "maxmemory-policy check", func(ctx context.Context) error {
			evErr = redisx.CheckEviction(ctx, client)
			if errors.Is(evErr, redisx.ErrEvictionPolicy) {
				return nil // definite answer: do not retry
			}
			return evErr
		})
		if evErr != nil {
			logger.Error("maxmemory-policy check failed; refusing to run consumers", "error", evErr)
			_ = client.Close()
			pool.Close()
			return 1
		}
		logger.Info("maxmemory-policy check passed", "policy", "noeviction")
	}
	cache := redisx.NewCache(client, rcfg)
	limiter := redisx.NewLimiter(client, rcfg)

	// Full mediator: every workload handler and the consumers.
	bcfg := behavior.Config{Logger: logger, Clock: clock, DefaultTimeout: cfg.RequestTimeout, Store: store, Cache: cache, Limiter: limiter}
	m := mediator.New(mediator.WithNodeID(cfg.NodeID), mediator.WithClock(clock), mediator.WithLogger(logger))
	if err := behavior.UseStandard(m, bcfg); err != nil {
		logger.Error("behaviors", "error", err)
		return 1
	}
	if err := mediator.Use(m, fencingLog{logger: logger, node: cfg.NodeID}, mediator.Consumers(), mediator.After(behavior.Inbox)); err != nil {
		logger.Error("fencing log behavior", "error", err)
		return 1
	}
	if err := workload.Register(m, workload.Deps{NodeID: cfg.NodeID, Groups: cfg.Groups, StrictProjection: cfg.StrictProjection}); err != nil {
		logger.Error("workload registration", "error", err)
		return 1
	}
	if err := m.OnBuild(httpapi.BuildCheck); err != nil {
		logger.Error("build hook", "error", err)
		return 1
	}
	if err := m.Build(); err != nil {
		logger.Error("build", "error", err)
		return 1
	}

	// Remote dispatch: client, reply reader, server over the full mediator,
	// and the proxy mediator whose commands are Declared.
	remote := redisx.NewRemote(client, rcfg, redisx.WithRemoteClock(clock), redisx.WithRemoteLogger(logger))
	replyReader := redisx.NewReplyReader(remote)
	rs := redisx.NewRemoteServer(m, client, rcfg, redisx.WithServerClock(clock), redisx.WithServerLogger(logger))
	mp := mediator.New(mediator.WithNodeID(cfg.NodeID), mediator.WithClock(clock), mediator.WithLogger(logger), mediator.WithRemote(remote))
	if err := behavior.UseStandard(mp, behavior.Config{Logger: logger, Clock: clock, DefaultTimeout: cfg.RequestTimeout}); err != nil {
		logger.Error("proxy behaviors", "error", err)
		return 1
	}
	remoteNames, err := registerProxy(mp, m, cfg.LocalInRemote)
	if err != nil {
		logger.Error("proxy registration", "error", err)
		return 1
	}
	if err := mp.OnBuild(httpapi.BuildCheck); err != nil {
		logger.Error("proxy build hook", "error", err)
		return 1
	}
	if err := mp.Build(); err != nil {
		logger.Error("proxy build", "error", err)
		return 1
	}

	// Components.
	groups := cfg.Groups
	topics := m.Topics()
	streams := redisx.NewStreams(client, rcfg)
	relay := pg.NewRelay(pool, streams, pg.RelayConfig{
		Topics: topics, Partitions: cfg.Partitions, Logger: logger, Clock: clock,
		KnownGroups: func() []string { return groups },
	})
	var consumers *redisx.Consumers
	if len(groups) > 0 {
		consumers = redisx.NewConsumers(m, client, rcfg, store, redisx.WithClock(clock), redisx.WithLogger(logger))
	}
	janitor := pg.NewJanitor(pool, pg.JanitorConfig{Trimmer: streams, Topics: topics, Partitions: cfg.Partitions, Logger: logger, Clock: clock})

	n := &node{
		cfg: cfg, logger: logger, clock: clock, m: m, mp: mp, remoteNames: remoteNames,
		remoteServer: newSwitchable("remote_server", rs, true, logger),
		relay:        newSwitchable("relay", relay, true, logger),
		relayImpl:    relay, rs: rs, consumers: consumers, remote: remote, groups: groups,
		started: time.Now(),
	}
	n.remoteMode.Store(cfg.Dispatch == DispatchRemote)

	rt := &mediator.Runtime{
		RemoteServer: n.remoteServer,
		Relay:        n.relay,
		ReplyReader:  replyReader,
		Janitor:      janitor,
		Extra:        []mediator.Component{goroutineMarker(logger, 5*time.Second)},
		Closers:      []io.Closer{client, closerFunc(func() error { pool.Close(); return nil })},
		DrainTimeout: cfg.Drain,
		Logger:       logger,
	}
	if consumers != nil {
		rt.Consumers = consumers
	}
	ready := []func(context.Context) error{
		func(ctx context.Context) error { return pool.Ping(ctx) },
		func(ctx context.Context) error { return client.Ping(ctx).Err() },
		func(context.Context) error { return rt.Healthy() },
	}
	if len(groups) > 0 && len(topics) > 0 {
		ready = append(ready, func(ctx context.Context) error {
			lags, err := redisx.ConsumerLag(ctx, client, rcfg, groups, topics)
			if err != nil {
				return err
			}
			for _, l := range lags {
				if l.Pending > 0 && l.OldestAge > cfg.ReadyMaxLag {
					return fmt.Errorf("consumer lag %s on %s/%s/%d exceeds %s", l.OldestAge, l.Group, l.Topic, l.Partition, cfg.ReadyMaxLag)
				}
			}
			return nil
		})
	}
	hcfg := httpapi.Config{Logger: logger, ReadyChecks: ready, ReadyMaxLag: cfg.ReadyMaxLag}
	if n.local, err = httpapi.New(m, hcfg); err != nil {
		logger.Error("http server", "error", err)
		return 1
	}
	if n.proxy, err = httpapi.New(mp, hcfg); err != nil {
		logger.Error("proxy http server", "error", err)
		return 1
	}
	rt.HTTP = newHTTPComponent(cfg.HTTPAddr, n.handler(), cfg.Drain, logger)

	logger.Info("chaosnode ready", "node", cfg.NodeID, "topics", topics, "served", rs.Served(), "declared_in_remote_mode", remoteNames)
	if err := rt.Run(ctx); err != nil {
		logger.Error("runtime stopped with error", "error", err)
		return 1
	}
	logger.Info("chaosnode stopped cleanly", "node", cfg.NodeID)
	return 0
}
