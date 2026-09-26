//go:build integration

package integration

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json/v2"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// Environment of the package, set by TestMain.
var (
	// pgRootURL connects to the Postgres server as a user that may create
	// databases; every test creates its own database from it.
	pgRootURL string
	// redisAddr is host:port of the shared Redis; every test uses its own
	// key prefix on it.
	redisAddr string
	// ordersBin is the examples/orders binary built once by TestMain.
	ordersBin string
	// rootPool is a small pool on pgRootURL for CREATE and DROP DATABASE.
	rootPool *pgxpool.Pool
)

// ordersPackage is the import path of the example service built by TestMain.
const ordersPackage = "github.com/t3stackcoder/go-api-backend/examples/orders"

// jwtSecret signs the tokens of the tests and is passed to every node.
const jwtSecret = "integration-test-secret"

// TestMain starts postgres:18 and redis:8 once for the package unless PG_URL
// and REDIS_ADDR point at running servers, builds the orders binary once, and
// tears everything down after the run.
func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	ctx := context.Background()
	var cleanups []func()
	defer func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}()

	if u := os.Getenv("PG_URL"); u != "" {
		pgRootURL = u
	} else {
		c, err := postgres.Run(ctx, "postgres:18",
			postgres.WithDatabase("mediator"), postgres.WithUsername("mediator"), postgres.WithPassword("mediator"),
			postgres.BasicWaitStrategies())
		if err != nil {
			log.Printf("start postgres: %v", err)
			return 1
		}
		cleanups = append(cleanups, func() { _ = testcontainers.TerminateContainer(c) })
		pgRootURL, err = c.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			log.Printf("postgres connection string: %v", err)
			return 1
		}
	}

	if a := os.Getenv("REDIS_ADDR"); a != "" {
		redisAddr = a
	} else {
		c, err := tcredis.Run(ctx, "redis:8")
		if err != nil {
			log.Printf("start redis: %v", err)
			return 1
		}
		cleanups = append(cleanups, func() { _ = testcontainers.TerminateContainer(c) })
		conn, err := c.ConnectionString(ctx)
		if err != nil {
			log.Printf("redis connection string: %v", err)
			return 1
		}
		redisAddr = strings.TrimPrefix(conn, "redis://")
	}

	dir, err := os.MkdirTemp("", "orders-integration-")
	if err != nil {
		log.Printf("temp dir: %v", err)
		return 1
	}
	cleanups = append(cleanups, func() { _ = os.RemoveAll(dir) })
	ordersBin = filepath.Join(dir, "orders")
	if runtime.GOOS == "windows" {
		ordersBin += ".exe"
	}
	build := exec.Command("go", "build", "-o", ordersBin, ordersPackage)
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		log.Printf("go build %s: %v", ordersPackage, err)
		return 1
	}

	rootPool, err = pg.NewPool(ctx, pgRootURL, pg.PoolConfig{MaxConns: 4, ApplicationName: "integration-root"})
	if err != nil {
		log.Printf("root pool: %v", err)
		return 1
	}
	cleanups = append(cleanups, rootPool.Close)
	return m.Run()
}

// database is one Postgres database owned by a test.
type database struct {
	name string
	// url connects to the database; it is what the node gets as PG_URL.
	url string
	// pool is the test's own view of the database for assertions.
	pool *pgxpool.Pool
}

// newDatabase creates a database no other test uses and drops it at
// cleanup. A database rather than a schema per test because the relay,
// janitor, and migrations take database-global advisory locks, so two
// nodes in one database would contend for the same relay slots.
func newDatabase(t *testing.T) *database {
	t.Helper()
	ctx := context.Background()
	name := "t_" + randomHex(6)
	if _, err := rootPool.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("create database: %v", err)
	}
	u, err := url.Parse(pgRootURL)
	if err != nil {
		t.Fatalf("parse root url: %v", err)
	}
	u.Path = "/" + name
	db := &database{name: name, url: u.String()}
	t.Cleanup(func() {
		if db.pool != nil {
			db.pool.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := rootPool.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
			t.Logf("drop database %s: %v", name, err)
		}
	})
	db.pool, err = pg.NewPool(ctx, db.url, pg.PoolConfig{MaxConns: 4, ApplicationName: "integration-test"})
	if err != nil {
		t.Fatalf("test pool: %v", err)
	}
	return db
}

// count runs a COUNT-style query and returns the number.
func (db *database) count(t *testing.T, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := db.pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

// randomHex returns n random bytes as hex.
func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// uniquePrefix returns a Redis key prefix no other test uses.
func uniquePrefix() string { return "it" + randomHex(4) }

// mintJWT signs an HS256 token over claims with secret, the way the example
// authenticator (examples/orders/auth.go) verifies it.
func mintJWT(t *testing.T, secret string, claims map[string]any) string {
	t.Helper()
	header, err := json.Marshal(map[string]string{"alg": "HS256", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// token returns a bearer token for subject alice with the permissions.
func token(t *testing.T, permissions ...string) string {
	t.Helper()
	if permissions == nil {
		permissions = []string{}
	}
	return mintJWT(t, jwtSecret, map[string]any{
		"sub": "alice", "roles": []string{"customer"}, "permissions": permissions, "tenant": "t1",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
}

// eventually polls cond until it holds or the timeout passes.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
