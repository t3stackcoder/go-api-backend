// Command orders runs the example service of spec section 13 as one node:
// Postgres pool and migrations, Redis client, the service mediator with the
// standard behaviors, the HTTP listener with the OpenAPI document, the
// relay, the consumers, remote dispatch, and the janitor, composed by
// mediator.Runtime. It is the template for real services.
//
// Configuration comes from the environment (the application edge reads
// it; the framework never does): PG_URL, REDIS_ADDR, REDIS_PREFIX,
// HTTP_ADDR (default :8080), NODE_ID, JWT_SECRET (auth.go), ORDERS_DEBUG=1
// to add the debug requests, and SHUTDOWN_ON_STDIN_EOF=1 to also stop when
// stdin closes (see stdinEOF in ready.go).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/t3stackcoder/go-api-backend/examples/orders/orders"
	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	motel "github.com/t3stackcoder/go-api-backend/mediator/otel"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

const (
	partitions   = 4
	drainTimeout = 15 * time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	slog.SetDefault(logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if os.Getenv("SHUTDOWN_ON_STDIN_EOF") == "1" {
		ctx = stdinEOF(ctx)
	}
	if err := run(ctx, logger); err != nil {
		logger.Error("orders: exit", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	nodeID := envOr("NODE_ID", redisx.DefaultNodeID())
	pool, err := pg.NewPool(ctx, envOr("PG_URL", "postgres://app:app@localhost:5432/app?sslmode=disable"), pg.PoolConfig{ApplicationName: "orders"})
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pg.Migrate(ctx, pool); err != nil {
		return err
	}
	if err := orders.Migrate(ctx, pool); err != nil {
		return err
	}
	rcfg := redisx.Config{Addr: envOr("REDIS_ADDR", "localhost:6379"), Prefix: os.Getenv("REDIS_PREFIX"), NodeID: nodeID, PartitionsPerTopic: partitions}.WithDefaults()
	client, err := redisx.NewClient(ctx, rcfg)
	if err != nil {
		return err
	}
	defer client.Close()
	if err := redisx.CheckEviction(ctx, client); err != nil {
		return err
	}
	inst, err := motel.NewInstruments(nil)
	if err != nil {
		return err
	}
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: partitions})
	streams := redisx.NewStreams(client, rcfg)
	remote := redisx.NewRemote(client, rcfg)
	m, err := orders.NewMediator(orders.Deps{
		Store: store, Querier: pool, Cache: redisx.NewCache(client, rcfg), Limiter: redisx.NewLimiter(client, rcfg),
		Remote: motel.InstrumentRemote(inst, remote), Logger: logger, NodeID: nodeID,
		Debug: os.Getenv("ORDERS_DEBUG") == "1", ProjectorDelay: envDuration("ORDERS_DEBUG_PROJECTOR_DELAY"),
	})
	if err != nil {
		return err
	}
	doc, err := openapi.Generate(m, orders.OpenAPIConfig())
	if err != nil {
		return err
	}
	relay := pg.NewRelay(pool, streams, pg.RelayConfig{Topics: m.Topics(), Partitions: partitions, KnownGroups: func() []string { return orders.Groups }, Logger: logger})
	reg, err := motel.RelayCollector(inst, relay)
	if err != nil {
		return err
	}
	defer func() { _ = reg.Unregister() }()
	var rt mediator.Runtime
	srv, err := httpapi.New(m, httpapi.Config{
		Authenticator: newAuthenticator(logger),
		Logger:        logger,
		Docs:          openapi.Handler(doc),
		ReadyChecks:   readyChecks(pool, client, rcfg, m.Topics(), &rt),
	})
	if err != nil {
		return err
	}
	rt = mediator.Runtime{
		HTTP:         httpapi.NewListener(srv, envOr("HTTP_ADDR", ":8080"), drainTimeout),
		RemoteServer: redisx.NewRemoteServer(m, client, rcfg),
		Consumers:    redisx.NewConsumers(m, client, rcfg, store, redisx.WithObserver(motel.NewConsumersObserver(inst))),
		Relay:        relay,
		ReplyReader:  redisx.NewReplyReader(remote),
		Janitor:      pg.NewJanitor(pool, pg.JanitorConfig{Trimmer: streams, Topics: m.Topics(), Partitions: partitions, Logger: logger}),
		DrainTimeout: drainTimeout,
		Logger:       logger,
	}
	logger.Info("orders: starting", "node", nodeID, "http", envOr("HTTP_ADDR", ":8080"))
	return rt.Run(ctx)
}

// envOr returns the environment variable key, or def when it is empty.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envDuration parses the environment variable key as a duration, zero when
// it is unset or malformed.
func envDuration(key string) time.Duration {
	d, _ := time.ParseDuration(os.Getenv(key))
	return d
}
