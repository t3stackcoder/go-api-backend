package behavior

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	motel "github.com/t3stackcoder/go-api-backend/mediator/otel"
	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
)

// Rate limit decision results reported in mediator.ratelimit.decisions.
const (
	RateLimitAllowed  = "allowed"
	RateLimitLimited  = "limited"
	RateLimitDegraded = "degraded"
)

// GlobalRateLimitKey is the limiter key when neither the policy, the
// principal, nor the HTTP adapter supplies one.
const GlobalRateLimitKey = "global"

// NewRateLimit returns the RateLimit behavior (5.9) for requests that
// implement RateLimit(). The key is Policy.Key when set, else the principal
// subject, else the client address recorded by httpapi, else "global". Over
// the limit the call fails with CodeRateLimited and Details["retry_after_ms"].
// When the limiter is unreachable the behavior fails open: the call goes
// through, mediator.ratelimit.degraded is counted, and a warning is logged
// at most once per request name per minute. Config.FailClosed turns that
// into CodeUnavailable. Prepare rejects invalid policies and a nil Limiter.
func NewRateLimit(cfg Config) mediator.Behavior {
	inst, err := cfg.instruments()
	b := &rateLimit{
		limiter:    cfg.Limiter,
		failClosed: cfg.FailClosed,
		logger:     cfg.logger(),
		inst:       inst,
		err:        err,
		limitLog:   newLogLimiter(cfg.clock(), time.Minute),
	}
	b.attrs.build = newRateLimitAttrs
	return b
}

type rateLimit struct {
	limiter    RateLimiter
	failClosed bool
	logger     *slog.Logger
	inst       *motel.Instruments
	err        error
	limitLog   *logLimiter
	attrs      infoCache[rateLimitAttrs]
}

type rateLimitAttrs struct {
	decisions [3][]metric.AddOption // allowed, limited, degraded
	degraded  []metric.AddOption
}

func newRateLimitAttrs(info *mediator.RequestInfo) *rateLimitAttrs {
	name := attribute.String(motel.AttrName, info.Name)
	a := &rateLimitAttrs{degraded: []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(name))}}
	for i, r := range [...]string{RateLimitAllowed, RateLimitLimited, RateLimitDegraded} {
		a.decisions[i] = []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(name, attribute.String(motel.AttrResult, r)))}
	}
	return a
}

func (b *rateLimit) Name() string { return RateLimit }

// Prepare validates the policy of every rate-limited request type.
func (b *rateLimit) Prepare(infos []*mediator.RequestInfo) error {
	var errs []error
	if b.err != nil {
		errs = append(errs, b.err)
	}
	if b.limiter == nil {
		errs = append(errs, errors.New("behavior: RateLimit requires a Limiter"))
	}
	for _, info := range infos {
		if !info.Kind.IsRequest() || !info.Traits.RateLimit {
			continue
		}
		b.attrs.get(info)
		p := reflect.Zero(info.RequestType).Interface().(mediator.RateLimited).RateLimit()
		if !p.Valid() {
			errs = append(errs, fmt.Errorf("behavior: %s has an invalid rate limit policy (rate %v, period %s, burst %d)", info.RequestType, p.Rate, p.Period, p.Burst))
		}
	}
	return errors.Join(errs...)
}

// keyFor resolves the limiter key of one call.
func keyFor(ctx context.Context, req any, p ratelimit.Policy) string {
	if p.Key != nil {
		if k := p.Key(ctx, req); k != "" {
			return k
		}
	}
	if s := authz.PrincipalFrom(ctx).Subject; s != "" {
		return s
	}
	if a := httpapi.RemoteAddr(ctx); a != "" {
		return a
	}
	return GlobalRateLimitKey
}

func (b *rateLimit) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	rl, ok := req.(mediator.RateLimited)
	if !ok {
		return next(ctx, req)
	}
	p := rl.RateLimit()
	a := b.attrs.get(info)
	d, err := b.limiter.Check(ctx, info.Name, keyFor(ctx, req, p), p)
	if err != nil {
		b.inst.RateLimitDegraded.Add(ctx, 1, a.degraded...)
		b.inst.RateLimitDecisions.Add(ctx, 1, a.decisions[2]...)
		if b.limitLog.allow(info.Name) {
			b.logger.LogAttrs(ctx, slog.LevelWarn, "rate limiter unavailable",
				slog.String("name", info.Name), slog.Bool("fail_closed", b.failClosed),
				slog.String("correlation_id", mediator.CorrelationID(ctx)), slog.Any("error", err))
		}
		if b.failClosed {
			return nil, mediator.Wrap(mediator.CodeUnavailable, "rate limiter unavailable", err)
		}
		return next(ctx, req)
	}
	if !d.Allowed {
		b.inst.RateLimitDecisions.Add(ctx, 1, a.decisions[1]...)
		return nil, mediator.E(mediator.CodeRateLimited, "rate limit exceeded").WithDetail("retry_after_ms", d.RetryAfter.Milliseconds())
	}
	b.inst.RateLimitDecisions.Add(ctx, 1, a.decisions[0]...)
	return next(ctx, req)
}
