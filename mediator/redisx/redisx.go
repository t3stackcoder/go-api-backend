// Package redisx is the Redis 8 side of the mediator: the stream sink the
// relay writes to, the partition-leased consumers that read those streams,
// the tag-versioned cache, the GCRA rate limiter, remote dispatch over
// request and reply streams, and the operations functions behind
// mediatorctl (spec section 7).
//
// Redis is a transport and a cache, never a source of truth: every structure
// here can be rebuilt from Postgres (spec 7.7). Every I/O call site passes
// through testkit.Fault with the point names of Appendix A so the fault sweep
// can fail each one in every way.
package redisx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Config configures every redisx component (spec 7.8). Zero values take the
// defaults listed on each field; call WithDefaults to apply them.
type Config struct {
	// Addr is the Redis address, host:port. Default "localhost:6379".
	Addr string
	// Username and Password authenticate the connection (ACL or requirepass).
	Username, Password string
	// DB selects the logical database. Default 0.
	DB int
	// Prefix is the first segment of every key (7.1). Default "mediator".
	Prefix string
	// PartitionsPerTopic is P, the number of streams per topic. Default 16.
	PartitionsPerTopic int
	// LeaseTTL is the lifetime of a partition lease. Default 15 s.
	LeaseTTL time.Duration
	// LeaseRenew is the renewal and membership heartbeat interval. Default 5 s.
	LeaseRenew time.Duration
	// ClaimMinIdle is the idle time after which a pending entry of another
	// consumer is claimed with XAUTOCLAIM. Default 30 s.
	ClaimMinIdle time.Duration
	// ReadBlock is the BLOCK duration of stream reads. Default 1 s.
	ReadBlock time.Duration
	// ReadBatch is the COUNT of stream reads. Default 64.
	ReadBatch int
	// MaxAttempts is the delivery attempts before an entry is dead-lettered.
	// Default 10.
	MaxAttempts int
	// CacheDefaultTTL is the cache entry TTL when the query declares none.
	// Default 5 m.
	CacheDefaultTTL time.Duration
	// ReplyStreamMaxLen is the approximate MAXLEN of reply streams. Default 10000.
	ReplyStreamMaxLen int64
	// AssumeNoEviction skips the maxmemory-policy check (7.7) for managed
	// Redis offerings that disable CONFIG.
	AssumeNoEviction bool
	// NodeID names this process in leases, membership, and reply streams.
	// Default is "<hostname>-<pid>-<random>".
	NodeID string
}

// WithDefaults returns a copy of c with every zero field set to its default.
func (c Config) WithDefaults() Config {
	if c.Addr == "" {
		c.Addr = "localhost:6379"
	}
	if c.Prefix == "" {
		c.Prefix = "mediator"
	}
	if c.PartitionsPerTopic <= 0 {
		c.PartitionsPerTopic = 16
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = 15 * time.Second
	}
	if c.LeaseRenew <= 0 {
		c.LeaseRenew = 5 * time.Second
	}
	if c.ClaimMinIdle <= 0 {
		c.ClaimMinIdle = 30 * time.Second
	}
	if c.ReadBlock <= 0 {
		c.ReadBlock = time.Second
	}
	if c.ReadBatch <= 0 {
		c.ReadBatch = 64
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 10
	}
	if c.CacheDefaultTTL <= 0 {
		c.CacheDefaultTTL = 5 * time.Minute
	}
	if c.ReplyStreamMaxLen <= 0 {
		c.ReplyStreamMaxLen = 10000
	}
	if c.NodeID == "" {
		c.NodeID = DefaultNodeID()
	}
	return c
}

// Keys returns the key layout for the configured prefix.
func (c Config) Keys() Keys { return Keys{Prefix: c.Prefix} }

// DefaultNodeID returns "<hostname>-<pid>-<random hex>", unique per process.
func DefaultNodeID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "node"
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}

// NewClient connects to Redis with the options of cfg and verifies the
// connection with PING. Context deadlines are honored by every command,
// including blocking reads.
func NewClient(ctx context.Context, cfg Config) (*redis.Client, error) {
	cfg = cfg.WithDefaults()
	client := redis.NewClient(&redis.Options{
		Addr:                  cfg.Addr,
		Username:              cfg.Username,
		Password:              cfg.Password,
		DB:                    cfg.DB,
		ContextTimeoutEnabled: true,
		MaxRetries:            2,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisx: ping %s: %w", cfg.Addr, err)
	}
	return client, nil
}

// ErrEvictionPolicy is returned by CheckEviction when maxmemory-policy is not
// noeviction. Eviction would silently drop stream entries between relay and
// consumer (spec 7.7).
var ErrEvictionPolicy = errors.New("redisx: maxmemory-policy must be noeviction")

// CheckEviction verifies that Redis is configured with maxmemory-policy
// noeviction (spec 7.7). When CONFIG is disabled the error says so; set
// Config.AssumeNoEviction to skip the check.
func CheckEviction(ctx context.Context, client *redis.Client) error {
	res, err := client.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		return fmt.Errorf("redisx: CONFIG GET maxmemory-policy failed (set AssumeNoEviction when CONFIG is disabled): %w", err)
	}
	policy := res["maxmemory-policy"]
	if policy != "noeviction" {
		return fmt.Errorf("%w, got %q", ErrEvictionPolicy, policy)
	}
	return nil
}

// IsRedisTransient reports whether err is a Redis connection-level error or
// a reply that means "try again later" (LOADING, READONLY, CLUSTERDOWN,
// TRYAGAIN, MASTERDOWN, BUSY). It is registered with mediator.RegisterTransient
// so mediator.IsTransient covers Redis (spec 4.9).
func IsRedisTransient(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, redis.Nil) {
		return false
	}
	if errors.Is(err, redis.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	msg := err.Error()
	for _, prefix := range []string{"LOADING ", "READONLY ", "CLUSTERDOWN ", "TRYAGAIN ", "MASTERDOWN ", "BUSY "} {
		if strings.HasPrefix(msg, prefix) {
			return true
		}
	}
	for _, sub := range []string{
		"connection refused", "connection reset", "broken pipe", "i/o timeout",
		"connection pool timeout", "use of closed network connection", "redis: client is closed",
	} {
		if strings.Contains(msg, sub) {
			return true
		}
	}
	return false
}

func init() {
	mediator.RegisterTransient(IsRedisTransient)
}

// isBusyGroup reports whether err is the BUSYGROUP reply of XGROUP CREATE.
func isBusyGroup(err error) bool { return hasReplyPrefix(err, "BUSYGROUP") }

// isNoGroup reports whether err is the NOGROUP reply of XREADGROUP and friends.
func isNoGroup(err error) bool { return hasReplyPrefix(err, "NOGROUP") }

// hasReplyPrefix matches a Redis error reply by prefix, through wrapping.
func hasReplyPrefix(err error, prefix string) bool {
	if err == nil {
		return false
	}
	return redis.HasErrorPrefix(err, prefix) || strings.HasPrefix(err.Error(), prefix) || strings.Contains(err.Error(), ": "+prefix)
}

// nonNil returns err unless it is redis.Nil, which blocking reads return when
// the block time elapses without data.
func nonNil(err error) error {
	if errors.Is(err, redis.Nil) {
		return nil
	}
	return err
}
