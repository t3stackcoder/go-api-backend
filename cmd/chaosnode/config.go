package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// Dispatch modes of the HTTP routes (see node.dispatchHTTP).
const (
	// DispatchLocal routes every request to the full mediator, whose
	// handlers are all local.
	DispatchLocal = "local"
	// DispatchRemote routes every request to the proxy mediator, where the
	// workload commands not listed in CHAOS_REMOTE_HANDLERS are Declared and
	// therefore dispatched to a serving node over Redis (spec 7.6).
	DispatchRemote = "remote"
)

// config is the node configuration, read from the environment.
type config struct {
	// NodeID names the node (NODE_ID). Default: the hostname.
	NodeID string
	// PGURL is the Postgres URL (PG_URL).
	PGURL string
	// RedisAddr is host:port of Redis (REDIS_ADDR).
	RedisAddr string
	// RedisPrefix is the key prefix (REDIS_PREFIX). Default "mediator".
	RedisPrefix string
	// HTTPAddr is the listen address (HTTP_ADDR). Default ":8080".
	HTTPAddr string
	// Groups are the consumer groups this node runs (CHAOS_GROUPS, comma
	// separated). Unset: every workload group. Set but empty: none.
	Groups []string
	// LocalInRemote lists the commands the proxy mediator still handles
	// locally in remote dispatch mode (CHAOS_REMOTE_HANDLERS, comma
	// separated request names). Every other workload command is Declared.
	LocalInRemote map[string]bool
	// Dispatch is the initial dispatch mode (CHAOS_DISPATCH): local or remote.
	Dispatch string
	// StrictProjection is workload.Deps.StrictProjection (CHAOS_STRICT_PROJECTION).
	StrictProjection bool
	// Partitions is P (CHAOS_PARTITIONS). Default 16.
	Partitions int
	// Drain bounds each shutdown stage (CHAOS_DRAIN). Default 5 s.
	Drain time.Duration
	// RequestTimeout is the default request deadline (CHAOS_REQUEST_TIMEOUT). Default 30 s.
	RequestTimeout time.Duration
	// AssumeNoEviction skips the maxmemory-policy check (REDIS_ASSUME_NOEVICTION).
	AssumeNoEviction bool
	// LogLevel is the slog level (LOG_LEVEL). Default info.
	LogLevel slog.Level
	// ReadyMaxLag fails readiness when a consumer group is further behind
	// (CHAOS_READY_MAX_LAG). Default 60 s.
	ReadyMaxLag time.Duration
}

// loadConfig reads the environment and validates it.
func loadConfig() (config, error) {
	c := config{
		NodeID:         os.Getenv("NODE_ID"),
		PGURL:          os.Getenv("PG_URL"),
		RedisAddr:      os.Getenv("REDIS_ADDR"),
		RedisPrefix:    envOr("REDIS_PREFIX", "mediator"),
		HTTPAddr:       envOr("HTTP_ADDR", ":8080"),
		Dispatch:       envOr("CHAOS_DISPATCH", DispatchLocal),
		LocalInRemote:  map[string]bool{},
		Partitions:     16,
		Drain:          5 * time.Second,
		RequestTimeout: 30 * time.Second,
		ReadyMaxLag:    60 * time.Second,
		LogLevel:       slog.LevelInfo,
	}
	if c.NodeID == "" {
		host, err := os.Hostname()
		if err != nil || host == "" {
			host = "chaosnode"
		}
		c.NodeID = host
	}
	if c.PGURL == "" {
		return c, fmt.Errorf("PG_URL is required")
	}
	if c.RedisAddr == "" {
		return c, fmt.Errorf("REDIS_ADDR is required")
	}
	if raw, ok := os.LookupEnv("CHAOS_GROUPS"); ok {
		c.Groups = splitList(raw)
		if c.Groups == nil {
			c.Groups = []string{}
		}
	} else {
		c.Groups = append([]string(nil), workload.AllGroups...)
	}
	for _, name := range splitList(os.Getenv("CHAOS_REMOTE_HANDLERS")) {
		c.LocalInRemote[name] = true
	}
	switch c.Dispatch {
	case DispatchLocal, DispatchRemote:
	default:
		return c, fmt.Errorf("CHAOS_DISPATCH must be %q or %q, got %q", DispatchLocal, DispatchRemote, c.Dispatch)
	}
	var err error
	if c.StrictProjection, err = envBool("CHAOS_STRICT_PROJECTION", false); err != nil {
		return c, err
	}
	if c.AssumeNoEviction, err = envBool("REDIS_ASSUME_NOEVICTION", false); err != nil {
		return c, err
	}
	if raw := os.Getenv("CHAOS_PARTITIONS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return c, fmt.Errorf("CHAOS_PARTITIONS must be a positive integer, got %q", raw)
		}
		c.Partitions = n
	}
	if c.Drain, err = envDuration("CHAOS_DRAIN", c.Drain); err != nil {
		return c, err
	}
	if c.RequestTimeout, err = envDuration("CHAOS_REQUEST_TIMEOUT", c.RequestTimeout); err != nil {
		return c, err
	}
	if c.ReadyMaxLag, err = envDuration("CHAOS_READY_MAX_LAG", c.ReadyMaxLag); err != nil {
		return c, err
	}
	if raw := os.Getenv("LOG_LEVEL"); raw != "" {
		if err := c.LogLevel.UnmarshalText([]byte(raw)); err != nil {
			return c, fmt.Errorf("LOG_LEVEL: %w", err)
		}
	}
	return c, nil
}

func envOr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func envBool(name string, def bool) (bool, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return def, fmt.Errorf("%s: %w", name, err)
	}
	return b, nil
}

func envDuration(name string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return def, fmt.Errorf("%s: %w", name, err)
	}
	return d, nil
}

// splitList splits a comma-separated list, trimming blanks; nil when empty.
func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
