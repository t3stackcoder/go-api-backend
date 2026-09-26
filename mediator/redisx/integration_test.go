//go:build integration

package redisx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// redisAddr is the host:port of the redis:8 container started by TestMain.
var redisAddr string

func TestMain(m *testing.M) {
	ctx := context.Background()
	if addr := os.Getenv("REDISX_TEST_ADDR"); addr != "" {
		redisAddr = addr
		os.Exit(m.Run())
	}
	c, err := tcredis.Run(ctx, "redis:8")
	if err != nil {
		log.Fatalf("start redis container: %v", err)
	}
	conn, err := c.ConnectionString(ctx)
	if err != nil {
		log.Fatalf("connection string: %v", err)
	}
	redisAddr = strings.TrimPrefix(conn, "redis://")
	code := m.Run()
	_ = testcontainers.TerminateContainer(c)
	os.Exit(code)
}

// uniquePrefix returns a key prefix no other test uses, so tests run in
// parallel against one Redis.
func uniquePrefix() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return "t" + hex.EncodeToString(b[:])
}

// testConfig returns a fast configuration with a unique prefix.
func testConfig(node string) Config {
	return Config{
		Addr:               redisAddr,
		Prefix:             uniquePrefix(),
		NodeID:             node,
		PartitionsPerTopic: 4,
		LeaseTTL:           2 * time.Second,
		LeaseRenew:         300 * time.Millisecond,
		ClaimMinIdle:       time.Second,
		ReadBlock:          200 * time.Millisecond,
		ReadBatch:          16,
		MaxAttempts:        3,
		CacheDefaultTTL:    time.Minute,
		ReplyStreamMaxLen:  1000,
	}.WithDefaults()
}

// newTestClient connects to the container and closes the client at cleanup.
func newTestClient(t *testing.T, cfg Config) *redis.Client {
	t.Helper()
	client, err := NewClient(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// closedClient returns a client whose every command fails.
func closedClient(t *testing.T, cfg Config) *redis.Client {
	t.Helper()
	client, err := NewClient(context.Background(), cfg)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	_ = client.Close()
	return client
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
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// never asserts that cond stays false for d.
func never(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			t.Fatalf("%s happened but must not", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestClient_ConnectAndEviction(t *testing.T) {
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	if err := CheckEviction(context.Background(), client); err != nil {
		t.Fatalf("container should run with noeviction: %v", err)
	}
	if _, err := NewClient(context.Background(), Config{Addr: "127.0.0.1:1"}); err == nil {
		t.Fatal("expected connection error")
	}
	if err := CheckEviction(context.Background(), closedClient(t, cfg)); err == nil {
		t.Fatal("expected error from closed client")
	}
	// An evicting policy is refused, by CheckEviction and by Consumers.Run.
	if err := client.ConfigSet(context.Background(), "maxmemory-policy", "allkeys-lru").Err(); err != nil {
		t.Fatal(err)
	}
	restore := func() { _ = client.ConfigSet(context.Background(), "maxmemory-policy", "noeviction").Err() }
	defer restore()
	err := CheckEviction(context.Background(), client)
	if err == nil || !strings.Contains(err.Error(), "allkeys-lru") {
		t.Fatalf("got %v", err)
	}
	m, _, _ := buildConsumerFixture(t, nil)
	c := NewConsumers(m, client, cfg, &counterFencing{})
	if err := c.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "noeviction") {
		t.Fatalf("Run against an evicting Redis: %v", err)
	}
	restore()
	c2 := NewConsumers(m, client, cfg, &counterFencing{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c2.Run(ctx) }()
	eventually(t, 5*time.Second, "consumers running", func() bool { return c2.Healthy() == nil })
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	_ = fmt.Sprint
}
