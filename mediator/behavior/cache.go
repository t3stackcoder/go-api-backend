package behavior

import (
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/singleflight"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	motel "github.com/t3stackcoder/go-api-backend/mediator/otel"
)

// Cache results reported in mediator.cache.requests.
const (
	CacheHit    = "hit"
	CacheMiss   = "miss"
	CacheBypass = "bypass"
	CacheError  = "error"
)

// cacheResult indexes cacheAttrs.results.
type cacheResult uint8

const (
	cacheHit cacheResult = iota
	cacheMiss
	cacheBypass
	cacheError
)

var cacheResultNames = [...]string{CacheHit, CacheMiss, CacheBypass, CacheError}

// CacheKey returns the logical cache key of a request: "<name>:<hex sha256
// of mediator.CanonicalJSON(req)>". It equals redisx.CacheKey; the backend
// (redisx.Cache) prepends "<prefix>:cache:" to it. Two requests that encode
// to the same JSON under any key order share one entry.
func CacheKey(name string, req any) (string, error) {
	h, err := mediator.CanonicalHash(req)
	if err != nil {
		return "", fmt.Errorf("behavior: cache key for %s: %w", name, err)
	}
	return name + ":" + hex.EncodeToString(h[:]), nil
}

// NewCache returns the Cache behavior (5.10, 7.4) for queries that
// implement CacheTags(). A hit is decoded into the response type and
// returned without running the rest of the chain, so it costs no
// transaction. On a miss the tag versions are snapshotted before the query
// runs; the response is then stored against that snapshot with the TTL of
// CacheTTL() on the query, else Config.CacheTTL, else five minutes. Misses
// for one key are coalesced in-process with singleflight. mediator.NoCache
// bypasses the read but still stores. Only successful responses are stored.
//
// No backend failure reaches the caller: a failed read is a miss, a failed
// snapshot skips the store, a failed store is dropped; each is counted as
// result "error" in mediator.cache.requests and logged at warn level at
// most once per key per minute. The outcome is reported to the HTTP
// adapter with httpapi.SetCacheResult.
func NewCache(cfg Config) mediator.Behavior {
	inst, err := cfg.instruments()
	c := &cache{
		backend:  cfg.Cache,
		ttl:      cfg.cacheTTL(),
		logger:   cfg.logger(),
		inst:     inst,
		err:      err,
		limitLog: newLogLimiter(cfg.clock(), time.Minute),
	}
	c.attrs.build = newCacheAttrs
	return c
}

type cache struct {
	backend  CacheBackend
	ttl      time.Duration
	logger   *slog.Logger
	inst     *motel.Instruments
	err      error
	limitLog *logLimiter
	attrs    infoCache[cacheAttrs]
	sf       singleflight.Group
}

type cacheAttrs struct {
	results [4][]metric.AddOption
}

func newCacheAttrs(info *mediator.RequestInfo) *cacheAttrs {
	name := attribute.String(motel.AttrName, info.Name)
	a := &cacheAttrs{}
	for i, r := range cacheResultNames {
		a.results[i] = []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(name, attribute.String(motel.AttrResult, r)))}
	}
	return a
}

func (c *cache) Name() string { return Cache }

// Prepare precomputes the metric attributes of every cached query and
// reports a missing backend or a failed instrument creation.
func (c *cache) Prepare(infos []*mediator.RequestInfo) error {
	for _, info := range infos {
		if info.Kind == mediator.KindQuery && info.Traits.CacheTags {
			c.attrs.get(info)
		}
	}
	if c.backend == nil {
		return fmt.Errorf("behavior: Cache requires a CacheBackend")
	}
	return c.err
}

func (c *cache) count(ctx context.Context, a *cacheAttrs, r cacheResult) {
	c.inst.CacheRequests.Add(ctx, 1, a.results[r]...)
}

// degrade records a backend failure: counted as an error result and logged
// at warn level, rate limited per key.
func (c *cache) degrade(ctx context.Context, info *mediator.RequestInfo, a *cacheAttrs, op, key string, err error) {
	c.count(ctx, a, cacheError)
	if c.limitLog.allow(key) {
		c.logger.LogAttrs(ctx, slog.LevelWarn, "cache degraded",
			slog.String("op", op), slog.String("name", info.Name), slog.String("key", key),
			slog.String("correlation_id", mediator.CorrelationID(ctx)), slog.Any("error", err))
	}
}

func (c *cache) ttlFor(req any, info *mediator.RequestInfo) time.Duration {
	if info.Traits.CacheTTL {
		if d := req.(mediator.CacheTTLer).CacheTTL(); d > 0 {
			return d
		}
	}
	return c.ttl
}

func (c *cache) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	tagger, ok := req.(mediator.CacheTagger)
	if !ok {
		return next(ctx, req)
	}
	a := c.attrs.get(info)
	key, err := CacheKey(info.Name, req)
	if err != nil {
		c.degrade(ctx, info, a, "key", info.Name, err)
		httpapi.SetCacheResult(ctx, httpapi.CacheBypass)
		return next(ctx, req)
	}
	tags := tagger.CacheTags()
	if mediator.NoCache(ctx) {
		c.count(ctx, a, cacheBypass)
		httpapi.SetCacheResult(ctx, httpapi.CacheBypass)
		return c.fill(ctx, req, info, next, a, key, tags)
	}
	body, found, err := c.backend.Get(ctx, key)
	switch {
	case err != nil:
		c.degrade(ctx, info, a, "get", key, err)
	case found:
		res, derr := decodeResponse(body, info.ResponseType)
		if derr == nil {
			c.count(ctx, a, cacheHit)
			httpapi.SetCacheResult(ctx, httpapi.CacheHit)
			return res, nil
		}
		c.degrade(ctx, info, a, "decode", key, derr)
	default:
		c.count(ctx, a, cacheMiss)
	}
	httpapi.SetCacheResult(ctx, httpapi.CacheMiss)
	return c.fill(ctx, req, info, next, a, key, tags)
}

// fill runs the rest of the chain once per key at a time and stores the
// response. Waiters that are canceled while a load is in flight return
// their own context error; the load continues for the others.
func (c *cache) fill(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next, a *cacheAttrs, key string, tags []string) (any, error) {
	ch := c.sf.DoChan(key, func() (any, error) {
		return c.load(ctx, req, info, next, a, key, tags)
	})
	select {
	case r := <-ch:
		return r.Val, r.Err
	case <-ctx.Done():
		return nil, mediator.Wrap(mediator.CodeTimeout, "request ended while waiting for the cache fill", ctx.Err())
	}
}

// load is the miss path of 7.4. A panic inside next is converted here,
// before singleflight sees it, so every waiter of the shared load receives
// the same error instead of a crash.
func (c *cache) load(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next, a *cacheAttrs, key string, tags []string) (res any, err error) {
	defer func() {
		if v := recover(); v != nil {
			res, err = nil, recoveredPanic(ctx, c.logger, info, v)
		}
	}()
	snapshot, serr := c.backend.SnapshotTags(ctx, tags)
	if serr != nil {
		c.degrade(ctx, info, a, "snapshot", key, serr)
		snapshot = nil
	}
	res, err = next(ctx, req)
	if err != nil || snapshot == nil {
		return res, err
	}
	body, merr := json.Marshal(res)
	if merr != nil {
		c.degrade(ctx, info, a, "encode", key, merr)
		return res, nil
	}
	if serr := c.backend.Set(ctx, key, snapshot, body, c.ttlFor(req, info)); serr != nil {
		c.degrade(ctx, info, a, "set", key, serr)
	}
	return res, nil
}

// decodeResponse rebuilds a stored body as a value of the response type.
func decodeResponse(body []byte, rt reflect.Type) (any, error) {
	ptr := reflect.New(rt)
	if err := json.Unmarshal(body, ptr.Interface()); err != nil {
		return nil, err
	}
	return ptr.Elem().Interface(), nil
}
