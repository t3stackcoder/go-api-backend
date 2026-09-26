package behavior

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/t3stackcoder/go-api-backend/mediator"
	motel "github.com/t3stackcoder/go-api-backend/mediator/otel"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/retry"
)

// bumpTimeout bounds one tag bump. The bump runs on a context detached from
// the request so a request that is out of time still invalidates.
const bumpTimeout = 5 * time.Second

// bumpRetry is the backoff of a failed bump: 100 ms doubling to 5 s.
var bumpRetry = retry.Policy{BaseDelay: 100 * time.Millisecond, MaxDelay: 5 * time.Second}

// NewCacheInvalidation returns the CacheInvalidation behavior (7.4, design
// notes 3.4) for commands that implement Invalidates(). It sits inside the
// unit of work after Idempotency, so a replayed command never reaches it.
// After the rest of the chain succeeds it bumps every tag once directly
// (before COMMIT) and registers the second bump with pg.OnCommit; without
// a unit of work the second bump runs immediately.
//
// A bump that fails is retried in a goroutine owned by the behavior with
// exponential backoff for up to Config.CacheTTL, counted per tag in
// mediator.cache.invalidation_failed, and logged at warn level at most once
// per tag set per minute; during that window the staleness bound is the
// TTL (G9). The behavior implements io.Closer: Close stops the retries and
// waits for them.
func NewCacheInvalidation(cfg Config) mediator.Behavior {
	inst, err := cfg.instruments()
	ctx, cancel := context.WithCancel(context.Background())
	b := &cacheInvalidation{
		backend:  cfg.Cache,
		logger:   cfg.logger(),
		clock:    cfg.clock(),
		after:    cfg.after(),
		window:   cfg.cacheTTL(),
		inst:     inst,
		err:      err,
		limitLog: newLogLimiter(cfg.clock(), time.Minute),
		ctx:      ctx,
		cancel:   cancel,
		tagAttrs: map[string][]metric.AddOption{},
	}
	return b
}

type cacheInvalidation struct {
	backend  CacheBackend
	logger   *slog.Logger
	clock    mediator.Clock
	after    afterFunc
	window   time.Duration
	inst     *motel.Instruments
	err      error
	limitLog *logLimiter

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	wg     sync.WaitGroup

	tagMu    sync.Mutex
	tagAttrs map[string][]metric.AddOption
}

func (b *cacheInvalidation) Name() string { return CacheInvalidation }

// Prepare reports a missing backend or a failed instrument creation.
func (b *cacheInvalidation) Prepare([]*mediator.RequestInfo) error {
	if b.backend == nil {
		return fmt.Errorf("behavior: CacheInvalidation requires a CacheBackend")
	}
	return b.err
}

// Close stops pending retries and waits for their goroutines.
func (b *cacheInvalidation) Close() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cancel()
	b.wg.Wait()
	return nil
}

func (b *cacheInvalidation) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	res, err := next(ctx, req)
	if err != nil {
		return nil, err
	}
	inv, ok := req.(mediator.Invalidator)
	if !ok {
		return res, nil
	}
	tags := slices.Clone(inv.Invalidates())
	if len(tags) == 0 {
		return res, nil
	}
	b.bump(ctx, tags, false)
	pg.OnCommit(ctx, func(ctx context.Context) { b.bump(ctx, tags, true) })
	return res, nil
}

// bump performs one bump on a detached context and schedules a retry on
// failure.
func (b *cacheInvalidation) bump(ctx context.Context, tags []string, post bool) {
	bctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bumpTimeout)
	err := b.do(bctx, tags, post)
	cancel()
	if err == nil {
		return
	}
	b.failed(ctx, tags, post, 1, err)
	b.schedule(tags, post)
}

func (b *cacheInvalidation) do(ctx context.Context, tags []string, post bool) error {
	if post {
		return b.backend.BumpTagsPost(ctx, tags)
	}
	return b.backend.BumpTagsPre(ctx, tags)
}

func phase(post bool) string {
	if post {
		return "post-commit"
	}
	return "pre-commit"
}

func (b *cacheInvalidation) tagAttr(tag string) []metric.AddOption {
	b.tagMu.Lock()
	defer b.tagMu.Unlock()
	a, ok := b.tagAttrs[tag]
	if !ok {
		a = []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(attribute.String(motel.AttrTag, tag)))}
		b.tagAttrs[tag] = a
	}
	return a
}

// failed counts and logs one failed bump attempt.
func (b *cacheInvalidation) failed(ctx context.Context, tags []string, post bool, attempt int, err error) {
	for _, t := range tags {
		b.inst.CacheInvalidationFailed.Add(ctx, 1, b.tagAttr(t)...)
	}
	if b.limitLog.allow(strings.Join(tags, ",")) {
		b.logger.LogAttrs(ctx, slog.LevelWarn, "cache invalidation failed; retrying with backoff",
			slog.String("phase", phase(post)), slog.Any("tags", tags), slog.Int("attempt", attempt),
			slog.Duration("window", b.window), slog.String("correlation_id", mediator.CorrelationID(ctx)),
			slog.Any("error", err))
	}
}

// schedule starts a retry goroutine unless the behavior is closed.
func (b *cacheInvalidation) schedule(tags []string, post bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.wg.Add(1)
	go b.retry(tags, post)
}

// retry re-attempts the bump with backoff until it succeeds, the retry
// window (the TTL) has passed, or the behavior is closed.
func (b *cacheInvalidation) retry(tags []string, post bool) {
	defer b.wg.Done()
	deadline := b.clock.Now().Add(b.window)
	for attempt := 1; ; attempt++ {
		select {
		case <-b.after(bumpRetry.Backoff(attempt)):
		case <-b.ctx.Done():
			return
		}
		if b.clock.Now().After(deadline) {
			b.logger.LogAttrs(b.ctx, slog.LevelError, "cache invalidation abandoned; entries expire with the TTL",
				slog.String("phase", phase(post)), slog.Any("tags", tags), slog.Int("attempts", attempt))
			return
		}
		actx, cancel := context.WithTimeout(b.ctx, bumpTimeout)
		err := b.do(actx, tags, post)
		cancel()
		if err == nil {
			b.logger.LogAttrs(b.ctx, slog.LevelInfo, "cache invalidation recovered",
				slog.String("phase", phase(post)), slog.Any("tags", tags), slog.Int("attempts", attempt+1))
			return
		}
		b.failed(b.ctx, tags, post, attempt+1, err)
	}
}
