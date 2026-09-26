//go:build faultsweep && faultinject

package faultsweep

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/test/faultsweep/plan"
)

// composePostgres is the compose container name restarted by the
// Postgres-restart crash variant on the env path.
const composePostgres = "go-api-backend-postgres-1"

var (
	// pgURL and redisAddr are the connections every cell and child uses.
	// pgURL changes when the Postgres-restart variant restarts a
	// testcontainers instance (its host port is reassigned).
	pgURL     string
	redisAddr string
	// pgC and redisC are set on the testcontainers path only.
	pgC    *postgres.PostgresContainer
	redisC *tcredis.RedisContainer
	// envPath reports that PG_URL and REDIS_ADDR were used.
	envPath bool
	// childArgs is set in a crash-sweep child process.
	childArgs *plan.ChildArgs

	rootMu   sync.Mutex
	rootPool *pgxpool.Pool
)

// TestMain connects to PG_URL and REDIS_ADDR when both are set, otherwise
// starts postgres:18 and redis:8 once for the package. A child process
// (plan.ChildArgsFromEnv) reuses the parent's connections.
func TestMain(m *testing.M) {
	ctx := context.Background()
	if a, err := plan.ChildArgsFromEnv(os.LookupEnv); err == nil {
		childArgs = &a
		pgURL, redisAddr = a.PGURL, a.RedisAddr
		os.Exit(m.Run())
	} else if !errors.Is(err, plan.ErrNotChild) {
		log.Fatalf("faultsweep: %v", err)
	}
	if u, a := os.Getenv("PG_URL"), os.Getenv("REDIS_ADDR"); u != "" && a != "" {
		pgURL, redisAddr, envPath = u, a, true
	} else {
		var err error
		pgC, err = postgres.Run(ctx, "postgres:18",
			postgres.WithDatabase("mediator"), postgres.WithUsername("mediator"), postgres.WithPassword("mediator"),
			postgres.BasicWaitStrategies())
		if err != nil {
			log.Fatalf("faultsweep: start postgres: %v", err)
		}
		pgURL, err = pgC.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			log.Fatalf("faultsweep: postgres connection string: %v", err)
		}
		redisC, err = tcredis.Run(ctx, "redis:8")
		if err != nil {
			_ = testcontainers.TerminateContainer(pgC)
			log.Fatalf("faultsweep: start redis: %v", err)
		}
		conn, err := redisC.ConnectionString(ctx)
		if err != nil {
			log.Fatalf("faultsweep: redis connection string: %v", err)
		}
		redisAddr = strings.TrimPrefix(conn, "redis://")
	}
	code := m.Run()
	rootMu.Lock()
	if rootPool != nil {
		rootPool.Close()
	}
	rootMu.Unlock()
	if redisC != nil {
		_ = testcontainers.TerminateContainer(redisC)
	}
	if pgC != nil {
		_ = testcontainers.TerminateContainer(pgC)
	}
	os.Exit(code)
}

// root returns the schema-less pool used for CREATE SCHEMA, truncation,
// and terminating leaked backends. It is rebuilt after a Postgres restart.
func root(ctx context.Context) (*pgxpool.Pool, error) {
	rootMu.Lock()
	defer rootMu.Unlock()
	if rootPool != nil {
		return rootPool, nil
	}
	p, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{MaxConns: 4, ApplicationName: "fs-root"})
	if err != nil {
		return nil, err
	}
	rootPool = p
	return p, nil
}

// dropRoot closes the root pool so the next root call reconnects.
func dropRoot() {
	rootMu.Lock()
	defer rootMu.Unlock()
	if rootPool != nil {
		p := rootPool
		rootPool = nil
		go p.Close()
	}
}

// errNoDocker reports that the Postgres-restart variant cannot run.
var errNoDocker = errors.New("docker is not available")

// restartPostgres restarts the Postgres instance between a crash and its
// recovery (spec 11.4, crash sweep). With testcontainers the container is
// stopped and started and the connection string re-read, because the host
// port is reassigned; on the env path the compose container is restarted
// with the docker CLI. It waits until the database answers again.
func restartPostgres(ctx context.Context) error {
	dropRoot()
	if pgC != nil {
		d := 10 * time.Second
		if err := pgC.Stop(ctx, &d); err != nil {
			return fmt.Errorf("stop postgres: %w", err)
		}
		if err := pgC.Start(ctx); err != nil {
			return fmt.Errorf("start postgres: %w", err)
		}
		u, err := pgC.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			return fmt.Errorf("postgres connection string: %w", err)
		}
		pgURL = u
	} else {
		if _, err := exec.LookPath("docker"); err != nil {
			return errNoDocker
		}
		out, err := exec.CommandContext(ctx, "docker", "restart", composePostgres).CombinedOutput()
		if err != nil {
			return fmt.Errorf("docker restart %s: %v: %s", composePostgres, err, strings.TrimSpace(string(out)))
		}
	}
	deadline := time.Now().Add(60 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		p, err := root(pctx)
		if err == nil {
			err = p.Ping(pctx)
		}
		cancel()
		if err == nil {
			return nil
		}
		last = err
		dropRoot()
		time.Sleep(250 * time.Millisecond)
	}
	return fmt.Errorf("postgres did not come back after restart: %w", last)
}
