// Package behavior is the standard behavior set of spec section 5: recovery,
// tracing, logging, metrics, timeout, authorization, rate limiting,
// validation, caching, retry, and, from package pg, the unit of work,
// idempotency, and inbox. Standard returns them in the default order of
// Appendix D with their default scopes; UseStandard registers them.
//
// Names are the constants of package mediator re-exported here so that an
// application can position its own behaviors relative to the standard ones:
//
//	mediator.Use(m, tenantBehavior, mediator.After(behavior.Authorization))
//
// Because the names occupy the bare identifiers, the constructors are
// prefixed with New: NewRecovery, NewTracing, and so on.
package behavior

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"

	"github.com/t3stackcoder/go-api-backend/mediator"
	motel "github.com/t3stackcoder/go-api-backend/mediator/otel"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

// Behavior names of the standard set, in default order (Appendix D).
const (
	Recovery      = mediator.NameRecovery
	Tracing       = mediator.NameTracing
	Logging       = mediator.NameLogging
	Metrics       = mediator.NameMetrics
	Timeout       = mediator.NameTimeout
	Authorization = mediator.NameAuthorization
	RateLimit     = mediator.NameRateLimit
	Validation    = mediator.NameValidation
	Cache         = mediator.NameCache
	Retry         = mediator.NameRetry
	UnitOfWork    = mediator.NameUnitOfWork
	Idempotency   = mediator.NameIdempotency
	Inbox         = mediator.NameInbox
	// CacheInvalidation bumps the tag versions of a command inside its unit
	// of work (design notes 3.4). It has no counterpart in mediator because
	// spec 5.10 folds it into Cache, which sits outside the unit of work.
	CacheInvalidation = "CacheInvalidation"
)

// Defaults of the configuration.
const (
	// DefaultTimeout is the request deadline when Config.DefaultTimeout is zero (5.6).
	DefaultTimeout = 30 * time.Second
	// DefaultCacheTTL is the cache entry lifetime when neither the query nor
	// Config.CacheTTL sets one (5.10).
	DefaultCacheTTL = 5 * time.Minute
	// TracerName and MeterName are the instrumentation scope names used when
	// Config leaves Tracer or Meter nil.
	TracerName = "mediator"
	MeterName  = motel.MeterName
)

// CacheBackend is the tag-version cache protocol of spec 7.4 as the Cache
// and CacheInvalidation behaviors use it. *redisx.Cache satisfies it; the
// tests assert that. Keys passed to Get and Set are the logical keys of
// CacheKey; the backend prefixes them.
type CacheBackend interface {
	// Get returns the body stored under key when every tag version it was
	// built against is still current (Lua cache_get).
	Get(ctx context.Context, key string) (body []byte, ok bool, err error)
	// SnapshotTags returns the current version of each tag (MGET); a missing
	// tag is 0.
	SnapshotTags(ctx context.Context, tags []string) (map[string]int64, error)
	// Set stores body under key for ttl unless a tag moved since snapshot
	// (Lua cache_set).
	Set(ctx context.Context, key string, snapshot map[string]int64, body []byte, ttl time.Duration) error
	// BumpTagsPre increments every tag before the command commits.
	BumpTagsPre(ctx context.Context, tags []string) error
	// BumpTagsPost increments every tag after the command committed.
	BumpTagsPost(ctx context.Context, tags []string) error
}

// RateLimiter is the GCRA limiter of spec 7.5 as the RateLimit behavior uses
// it. *redisx.Limiter satisfies it.
type RateLimiter interface {
	Check(ctx context.Context, name, key string, p ratelimit.Policy) (ratelimit.Decision, error)
}

// Retention is the retention pair Build validates (6.8): the inbox window
// must not be shorter than the outbox (stream) window, or an old redelivery
// could be reprocessed. A zero value means the pg default of 7 days.
type Retention struct {
	Inbox  time.Duration
	Outbox time.Duration
}

// Config configures the standard set (design notes 3.4). Every field has a
// usable zero value except Store, Cache, and Limiter, whose absence omits
// the behaviors that need them.
type Config struct {
	// Logger receives the framework's records. Default slog.Default() under
	// group "mediator".
	Logger *slog.Logger
	// Tracer creates request spans. Default: the global tracer provider.
	Tracer trace.Tracer
	// Meter creates the instruments of spec 9.1. Default: the global meter
	// provider.
	Meter metric.Meter
	// Clock is the time source for durations, deadlines, and log rate
	// limits. Default: the wall clock. A clock that also implements
	// After(time.Duration) <-chan time.Time (testkit.Clock) is used for
	// retry and invalidation backoff sleeps too.
	Clock mediator.Clock
	// DefaultTimeout is the deadline applied by Timeout when the request
	// declares none. Default DefaultTimeout (30 s).
	DefaultTimeout time.Duration
	// LogPayloads logs the request after redaction in request.start (5.4).
	LogPayloads bool
	// RequireAuthByDefault makes Build reject every request type that does
	// not implement Requires() (5.7).
	RequireAuthByDefault bool
	// Validator compiles and checks tag rules. Default validate.New().
	Validator *validate.Validator
	// Store is the database. Nil omits UnitOfWork, Idempotency, and Inbox.
	Store pg.Store
	// UnitOfWork and Idempotency configure the pg behaviors. A nil Logger
	// in either takes Logger; a nil Idempotency.Observer takes the
	// mediator.idempotency.outcome counter.
	UnitOfWork  pg.UnitOfWorkConfig
	Idempotency pg.IdempotencyConfig
	// Cache is the cache backend. Nil omits Cache and CacheInvalidation.
	Cache CacheBackend
	// CacheTTL is the entry lifetime when the query declares none, and the
	// window during which a failed tag bump is retried. Default 5 m.
	CacheTTL time.Duration
	// Limiter is the rate limiter. Nil omits RateLimit.
	Limiter RateLimiter
	// FailClosed rejects requests with CodeUnavailable when the limiter is
	// unreachable instead of letting them through (5.9).
	FailClosed bool
	// Retention is validated at Build with pg.ValidateRetention.
	Retention Retention

	// inst is the instrument set shared by the behaviors of one Standard
	// call so every instrument is created once per meter.
	inst *motel.Instruments
}

// Entry is one behavior of the standard set with its registration options.
type Entry struct {
	Behavior mediator.Behavior
	Options  []mediator.UseOption
}

// BuildHook is implemented by behaviors that need the built registry
// (Timeout reads the consumer HandlerTimeout options). UseStandard registers
// the hook with Mediator.OnBuild.
type BuildHook interface {
	OnBuild(m *mediator.Mediator) error
}

// Standard returns the default set in the order of Appendix D:
//
//	Recovery > Tracing > Logging > Metrics > Timeout > Authorization > RateLimit >
//	Validation > Cache(queries) > Retry(commands) > UnitOfWork > Idempotency(commands) >
//	Inbox(consumers) > CacheInvalidation(commands)
//
// Scopes: Recovery, Tracing, Logging, and Metrics apply everywhere; Timeout
// and UnitOfWork to requests and consumers (UnitOfWork minus NoUnitOfWork
// types); Authorization, RateLimit, Cache, Retry, and CacheInvalidation to
// the requests that carry their trait; Validation to requests; Idempotency
// to commands with a unit of work (it skips at run time without a key);
// Inbox to consumers. A nil Store omits UnitOfWork, Idempotency, and Inbox;
// a nil Cache omits Cache and CacheInvalidation; a nil Limiter omits
// RateLimit.
func Standard(cfg Config) []Entry {
	cfg = cfg.shared()
	everywhere := []mediator.UseOption{mediator.Everywhere()}
	entries := []Entry{
		{NewRecovery(cfg), everywhere},
		{NewTracing(cfg), everywhere},
		{NewLogging(cfg), everywhere},
		{NewMetrics(cfg), everywhere},
		{NewTimeout(cfg), []mediator.UseOption{mediator.Requests(), mediator.Consumers()}},
		{NewAuthorization(cfg), []mediator.UseOption{mediator.Where(hasRequires)}},
	}
	if cfg.Limiter != nil {
		entries = append(entries, Entry{NewRateLimit(cfg), []mediator.UseOption{mediator.Where(hasRateLimit)}})
	}
	entries = append(entries, Entry{NewValidation(cfg), []mediator.UseOption{mediator.Requests()}})
	if cfg.Cache != nil {
		entries = append(entries, Entry{NewCache(cfg), []mediator.UseOption{mediator.Queries(), mediator.Where(hasCacheTags)}})
	}
	entries = append(entries, Entry{NewRetry(cfg), []mediator.UseOption{mediator.Commands(), mediator.Where(hasRetryPolicy)}})
	if cfg.Store != nil {
		uow, idem := cfg.UnitOfWork, cfg.Idempotency
		if uow.Logger == nil {
			uow.Logger = cfg.logger()
		}
		if idem.Logger == nil {
			idem.Logger = cfg.logger()
		}
		if idem.Observer == nil && cfg.inst != nil {
			idem.Observer = motel.NewIdempotencyObserver(cfg.inst)
		}
		entries = append(entries,
			Entry{pg.UnitOfWork(cfg.Store, uow), []mediator.UseOption{mediator.Requests(), mediator.Consumers(), mediator.Where(hasUnitOfWork)}},
			Entry{pg.Idempotency(idem), []mediator.UseOption{mediator.Commands(), mediator.Where(hasUnitOfWork)}},
			Entry{pg.Inbox(), []mediator.UseOption{mediator.Consumers()}},
		)
	}
	if cfg.Cache != nil {
		entries = append(entries, Entry{NewCacheInvalidation(cfg), []mediator.UseOption{mediator.Commands(), mediator.Where(hasInvalidates)}})
	}
	return entries
}

// UseStandard registers Standard(cfg) on m and wires every BuildHook.
func UseStandard(m *mediator.Mediator, cfg Config) error {
	for _, e := range Standard(cfg) {
		if err := mediator.Use(m, e.Behavior, e.Options...); err != nil {
			return err
		}
		if h, ok := e.Behavior.(BuildHook); ok {
			if err := m.OnBuild(h.OnBuild); err != nil {
				return err // covergate:ignore OnBuild fails only once built, and Use fails first then
			}
		}
	}
	return nil
}

// Trait predicates of the default scopes.
func hasRequires(i *mediator.RequestInfo) bool    { return i.Traits.Requires }
func hasRateLimit(i *mediator.RequestInfo) bool   { return i.Traits.RateLimit }
func hasCacheTags(i *mediator.RequestInfo) bool   { return i.Traits.CacheTags }
func hasRetryPolicy(i *mediator.RequestInfo) bool { return i.Traits.RetryPolicy }
func hasInvalidates(i *mediator.RequestInfo) bool { return i.Traits.Invalidates }
func hasUnitOfWork(i *mediator.RequestInfo) bool  { return !i.Traits.NoUnitOfWork }

// validate reports the configuration problems Build must surface (9.4).
// Recovery, present in every standard set, returns it from Prepare.
func (c Config) validate() error {
	var errs []error
	if c.Retention.Inbox != 0 || c.Retention.Outbox != 0 {
		inbox, outbox := c.Retention.Inbox, c.Retention.Outbox
		if inbox == 0 {
			inbox = pg.DefaultRetention
		}
		if outbox == 0 {
			outbox = pg.DefaultRetention
		}
		if err := pg.ValidateRetention(inbox, outbox); err != nil {
			errs = append(errs, err)
		}
	}
	if c.DefaultTimeout < 0 {
		errs = append(errs, errors.New("behavior: DefaultTimeout must not be negative"))
	}
	if c.CacheTTL < 0 {
		errs = append(errs, errors.New("behavior: CacheTTL must not be negative"))
	}
	return errors.Join(errs...)
}

func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default().WithGroup("mediator")
}

func (c Config) tracer() trace.Tracer {
	if c.Tracer != nil {
		return c.Tracer
	}
	return otel.GetTracerProvider().Tracer(TracerName)
}

func (c Config) meter() metric.Meter {
	if c.Meter != nil {
		return c.Meter
	}
	return otel.GetMeterProvider().Meter(MeterName)
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (c Config) clock() mediator.Clock {
	if c.Clock != nil {
		return c.Clock
	}
	return realClock{}
}

// afterFunc is the timer primitive of the sleeping behaviors.
type afterFunc func(time.Duration) <-chan time.Time

// after returns the clock's After when it has one (testkit.Clock), else
// time.After, which is virtual inside a testing/synctest bubble.
func (c Config) after() afterFunc {
	if a, ok := c.Clock.(interface {
		After(time.Duration) <-chan time.Time
	}); ok {
		return a.After
	}
	return time.After
}

func (c Config) timeout() time.Duration {
	if c.DefaultTimeout > 0 {
		return c.DefaultTimeout
	}
	return DefaultTimeout
}

func (c Config) cacheTTL() time.Duration {
	if c.CacheTTL > 0 {
		return c.CacheTTL
	}
	return DefaultCacheTTL
}

func (c Config) validator() *validate.Validator {
	if c.Validator != nil {
		return c.Validator
	}
	return validate.New()
}

// shared creates the instrument set once for a Standard call. A meter that
// refuses an instrument leaves inst nil; each behavior then creates its own
// and reports the error from Prepare.
func (c Config) shared() Config {
	if c.inst == nil {
		if inst, err := motel.NewInstruments(c.meter()); err == nil {
			c.inst = inst
		}
	}
	return c
}

// instruments returns the shared set, or creates one. On error it returns
// a set over the no-op meter together with the error, so the behavior stays
// usable and Build reports the problem.
func (c Config) instruments() (*motel.Instruments, error) {
	if c.inst != nil {
		return c.inst, nil
	}
	inst, err := motel.NewInstruments(c.meter())
	if err != nil {
		inst, _ = motel.NewInstruments(noop.NewMeterProvider().Meter(MeterName))
		return inst, err
	}
	return inst, nil
}
