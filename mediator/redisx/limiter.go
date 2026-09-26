package redisx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// Limiter is the GCRA rate limiter of spec 7.5: one Lua script, Redis TIME
// as the clock so every node agrees, and one string per key holding the
// theoretical arrival time in microseconds.
type Limiter struct {
	client *redis.Client
	keys   Keys
}

// NewLimiter returns a limiter over client with the prefix of cfg.
func NewLimiter(client *redis.Client, cfg Config) *Limiter {
	cfg = cfg.WithDefaults()
	return &Limiter{client: client, keys: cfg.Keys()}
}

// gcraScript: KEYS[1] state key; ARGV[1] emission interval in µs, ARGV[2]
// burst capacity, ARGV[3] key expiry in ms. Returns {allowed, remaining,
// retry_after_ms}.
var gcraScript = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local T = tonumber(ARGV[1])
local cap = tonumber(ARGV[2])
local tau = T * cap
local tat = tonumber(redis.call('GET', KEYS[1]) or '0')
if tat < now then tat = now end
local newtat = tat + T
local allowat = newtat - tau
if allowat > now then
  return {0, 0, math.ceil((allowat - now) / 1000)}
end
redis.call('SET', KEYS[1], string.format('%.0f', newtat), 'PX', ARGV[3])
local remaining = math.floor((now - (newtat - tau)) / T)
return {1, remaining, 0}`)

// ErrInvalidPolicy is returned by Check for a policy with a non-positive
// rate or period.
var ErrInvalidPolicy = errors.New("redisx: invalid rate limit policy")

// limiterArgs derives the script arguments from a policy: emission interval
// T = Period / Rate in microseconds, capacity max(Burst, 1), and the key
// expiry max(Period*Burst, T*capacity, 1s).
func limiterArgs(p ratelimit.Policy) (intervalUS int64, capacity int, expire time.Duration, err error) {
	if !p.Valid() {
		return 0, 0, 0, ErrInvalidPolicy
	}
	interval := p.EmissionInterval()
	if interval < time.Microsecond {
		interval = time.Microsecond
	}
	capacity = p.Burst
	if capacity < 1 {
		capacity = 1
	}
	expire = p.Period * time.Duration(p.Burst)
	if e := interval * time.Duration(capacity); e > expire {
		expire = e
	}
	if expire < time.Second {
		expire = time.Second
	}
	return int64(interval / time.Microsecond), capacity, expire, nil
}

// Check applies one operation against the limiter keyed
// `<prefix>:rl:<name>:<key>`. Fault point redis.rl.check.
func (l *Limiter) Check(ctx context.Context, name, key string, p ratelimit.Policy) (ratelimit.Decision, error) {
	if err := testkit.Fault(ctx, "redis.rl.check"); err != nil {
		return ratelimit.Decision{}, err
	}
	interval, capacity, expire, err := limiterArgs(p)
	if err != nil {
		return ratelimit.Decision{}, err
	}
	res, err := gcraScript.Run(ctx, l.client, []string{l.keys.RateLimit(name, key)}, interval, capacity, expire.Milliseconds()).Int64Slice()
	if err != nil {
		return ratelimit.Decision{}, fmt.Errorf("redisx: rate limit check: %w", err)
	}
	if len(res) != 3 {
		return ratelimit.Decision{}, fmt.Errorf("redisx: rate limit check: unexpected reply %v", res)
	}
	d := ratelimit.Decision{Allowed: res[0] == 1, Remaining: int(res[1])}
	if !d.Allowed {
		d.RetryAfter = time.Duration(res[2]) * time.Millisecond
		if d.RetryAfter <= 0 {
			d.RetryAfter = time.Millisecond
		}
	}
	return d, nil
}

// Reset deletes the limiter state of one key.
func (l *Limiter) Reset(ctx context.Context, name, key string) error {
	return l.client.Del(ctx, l.keys.RateLimit(name, key)).Err()
}
