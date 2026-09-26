package behavior_test

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior/cachemodel"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

var fullOrder = []string{
	behavior.Recovery, behavior.Tracing, behavior.Logging, behavior.Metrics, behavior.Timeout,
	behavior.Authorization, behavior.RateLimit, behavior.Validation, behavior.Cache, behavior.Retry,
	behavior.UnitOfWork, behavior.Idempotency, behavior.Inbox, behavior.CacheInvalidation,
}

// TestStandard_Golden is the G2 golden of Appendix D: the resolved total
// order, the chain per request type, and the behaviors that actually ran
// on every path, recorded by wrapping each standard behavior.
func TestStandard_Golden(t *testing.T) {
	h := newHarness(t, withRecorders())
	if got := h.m.Order(); !slices.Equal(got, fullOrder) {
		t.Fatalf("Order() = %v\nwant %v", got, fullOrder)
	}
	ctx := admin(context.Background())
	head := []string{behavior.Recovery, behavior.Tracing, behavior.Logging, behavior.Metrics}
	cases := []struct {
		name string
		typ  reflect.Type
		run  func() error
		want []string
	}{
		{"command", reflect.TypeFor[plainCmd](), func() error {
			_, err := mediator.Send(ctx, h.m, plainCmd{ID: "a"})
			return err
		}, append(slices.Clone(head), behavior.Timeout, behavior.Validation, behavior.UnitOfWork, behavior.Idempotency)},
		{"command with every trait", reflect.TypeFor[richCmd](), func() error {
			_, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Key: "k1", Tags: []string{"things"}})
			return err
		}, append(slices.Clone(head), behavior.Timeout, behavior.Authorization, behavior.RateLimit, behavior.Validation,
			behavior.Retry, behavior.UnitOfWork, behavior.Idempotency, behavior.CacheInvalidation)},
		{"query with CacheTags", reflect.TypeFor[cachedQuery](), func() error {
			_, err := mediator.Send(ctx, h.m, cachedQuery{ID: "q"})
			return err
		}, append(slices.Clone(head), behavior.Timeout, behavior.Validation, behavior.Cache, behavior.UnitOfWork)},
		{"stream", reflect.TypeFor[numStream](), func() error {
			for _, err := range mediator.Stream(ctx, h.m, numStream{N: 2}) {
				if err != nil {
					return err
				}
			}
			return nil
		}, append(slices.Clone(head), behavior.Timeout, behavior.Validation, behavior.UnitOfWork)},
		{"NoUnitOfWork request", reflect.TypeFor[noUowCmd](), func() error {
			_, err := mediator.Send(ctx, h.m, noUowCmd{ID: "n"})
			return err
		}, append(slices.Clone(head), behavior.Timeout, behavior.Validation)},
		{"notification", reflect.TypeFor[thingEvent](), func() error {
			return mediator.Publish(ctx, h.m, thingEvent{ID: "e"})
		}, head},
		{"consumer", nil, func() error {
			return h.deliver(ctx, "c1", mediator.Envelope{})
		}, append(slices.Clone(head), behavior.Timeout, behavior.UnitOfWork, behavior.Inbox)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h.order.take()
			if err := c.run(); err != nil {
				t.Fatal(err)
			}
			if got := h.order.take(); !slices.Equal(got, c.want) {
				t.Errorf("ran %v\nwant %v", got, c.want)
			}
			if c.typ == nil {
				return
			}
			if got := h.m.ChainFor(c.typ); !slices.Equal(got, c.want) {
				t.Errorf("ChainFor(%s) = %v\nwant %v", c.typ, got, c.want)
			}
		})
	}
}

// TestStandard_Omissions checks which behaviors disappear without a store,
// cache, or limiter, and that nothing else changes.
func TestStandard_Omissions(t *testing.T) {
	cases := []struct {
		name string
		opts []harnessOption
		omit []string
	}{
		{"no store", []harnessOption{withoutStore()}, []string{behavior.UnitOfWork, behavior.Idempotency, behavior.Inbox}},
		{"no cache", []harnessOption{withoutCache()}, []string{behavior.Cache, behavior.CacheInvalidation}},
		{"no limiter", []harnessOption{withoutLimiter()}, []string{behavior.RateLimit}},
		{"nothing", []harnessOption{withoutStore(), withoutCache(), withoutLimiter()},
			[]string{behavior.UnitOfWork, behavior.Idempotency, behavior.Inbox, behavior.Cache, behavior.CacheInvalidation, behavior.RateLimit}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t, c.opts...)
			var want []string
			for _, n := range fullOrder {
				if !slices.Contains(c.omit, n) {
					want = append(want, n)
				}
			}
			if got := h.m.Order(); !slices.Equal(got, want) {
				t.Fatalf("Order() = %v, want %v", got, want)
			}
			ctx := admin(context.Background())
			if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Tags: []string{"t"}}); err != nil {
				t.Fatal(err)
			}
			if _, err := mediator.Send(ctx, h.m, cachedQuery{ID: "q"}); err != nil {
				t.Fatal(err)
			}
			if err := h.deliver(ctx, "c", mediator.Envelope{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestUseStandard_Defaults builds with a zero Config: the global otel
// providers, slog.Default, the wall clock, and validate.New().
func TestUseStandard_Defaults(t *testing.T) {
	m := mediator.New()
	register(t, m, defaultHooks())
	if err := behavior.UseStandard(m, behavior.Config{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	want := []string{behavior.Recovery, behavior.Tracing, behavior.Logging, behavior.Metrics, behavior.Timeout,
		behavior.Authorization, behavior.Validation, behavior.Retry}
	if got := m.Order(); !slices.Equal(got, want) {
		t.Fatalf("Order() = %v, want %v", got, want)
	}
	if _, err := mediator.Send(context.Background(), m, plainCmd{ID: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := mediator.Send(context.Background(), m, plainCmd{}); mediator.CodeOf(err) != mediator.CodeValidation {
		t.Fatalf("want validation error, got %v", err)
	}
}

// TestUseStandard_Errors covers registration failures: a name already
// taken and a mediator that is already built.
func TestUseStandard_Errors(t *testing.T) {
	m := mediator.New()
	must(t, mediator.Use(m, mediator.BehaviorFunc{N: behavior.Timeout, F: nil}))
	if err := behavior.UseStandard(m, behavior.Config{}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("want duplicate name error, got %v", err)
	}
	built := mediator.New()
	must(t, built.Build())
	if err := behavior.UseStandard(built, behavior.Config{}); !errors.Is(err, mediator.ErrAlreadyBuilt) {
		t.Fatalf("want ErrAlreadyBuilt, got %v", err)
	}
}

// TestUseStandard_BuildHookError covers the OnBuild registration failing
// after the behavior itself was accepted.
func TestUseStandard_BuildHookError(t *testing.T) {
	m := mediator.New()
	// The hook registration is refused once built; simulate by building in
	// between through a behavior registered first that builds the mediator.
	// Simpler: OnBuild fails only when built, and Use fails first then, so
	// the hook branch is reached by wiring the entries by hand.
	entries := behavior.Standard(behavior.Config{})
	for _, e := range entries {
		must(t, mediator.Use(m, e.Behavior, e.Options...))
	}
	must(t, m.Build())
	var hookErr error
	for _, e := range entries {
		if h, ok := e.Behavior.(behavior.BuildHook); ok {
			hookErr = m.OnBuild(h.OnBuild)
		}
	}
	if !errors.Is(hookErr, mediator.ErrAlreadyBuilt) {
		t.Fatalf("want ErrAlreadyBuilt from OnBuild, got %v", hookErr)
	}
}

// TestConfig_Validate covers the Build-time configuration checks carried
// by Recovery's Prepare.
func TestConfig_Validate(t *testing.T) {
	cases := []struct {
		name string
		cfg  behavior.Config
		want string
	}{
		{"zero", behavior.Config{}, ""},
		{"retention ok", behavior.Config{Retention: behavior.Retention{Inbox: 10 * 24 * time.Hour, Outbox: 7 * 24 * time.Hour}}, ""},
		{"retention equal", behavior.Config{Retention: behavior.Retention{Inbox: time.Hour, Outbox: time.Hour}}, ""},
		{"inbox shorter", behavior.Config{Retention: behavior.Retention{Inbox: time.Hour, Outbox: 2 * time.Hour}}, "inbox retention"},
		{"outbox longer than default inbox", behavior.Config{Retention: behavior.Retention{Outbox: 8 * 24 * time.Hour}}, "inbox retention"},
		{"inbox shorter than default outbox", behavior.Config{Retention: behavior.Retention{Inbox: time.Hour}}, "inbox retention"},
		{"negative timeout", behavior.Config{DefaultTimeout: -1}, "DefaultTimeout"},
		{"negative ttl", behavior.Config{CacheTTL: -1}, "CacheTTL"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := mediator.New()
			must(t, mediator.HandleFunc(m, func(_ context.Context, c plainCmd) (cmdResult, error) { return cmdResult{}, nil }))
			must(t, behavior.UseStandard(m, c.cfg))
			err := m.Build()
			if c.want == "" {
				if err != nil {
					t.Fatalf("unexpected build error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want error containing %q, got %v", c.want, err)
			}
			if !strings.Contains(err.Error(), behavior.Recovery) {
				t.Fatalf("error should name the reporting behavior: %v", err)
			}
		})
	}
}

// TestStandard_PgLoggersAndObserver checks that the pg behaviors get the
// configured logger and the idempotency counter when they have none.
func TestStandard_PgLoggersAndObserver(t *testing.T) {
	h := newHarness(t)
	ctx := admin(context.Background())
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Key: "k"}); err != nil {
		t.Fatal(err)
	}
	rm := h.tel.collect(t)
	if p := point(t, rm, "mediator.idempotency.outcome", map[string]string{"name": "richCmd", "outcome": pg.OutcomeExecuted}); p.Int != 1 {
		t.Fatalf("executed = %d", p.Int)
	}
	if p := point(t, rm, "mediator.idempotency.outcome", map[string]string{"name": "richCmd", "outcome": pg.OutcomeReplayed}); p.Int != 1 {
		t.Fatalf("replayed = %d", p.Int)
	}

	// Explicit pg configuration is kept.
	var seen []string
	sink := newLogSink(slog.LevelDebug)
	entries := behavior.Standard(behavior.Config{
		Store:       memstore.New(memstore.Config{}),
		UnitOfWork:  pg.UnitOfWorkConfig{Logger: slog.New(sink)},
		Idempotency: pg.IdempotencyConfig{Logger: slog.New(sink), Observer: func(name, outcome string) { seen = append(seen, name+":"+outcome) }},
	})
	m := mediator.New()
	register(t, m, defaultHooks())
	for _, e := range entries {
		must(t, mediator.Use(m, e.Behavior, e.Options...))
	}
	must(t, m.Build())
	if _, err := mediator.Send(ctx, m, richCmd{ID: "a", Key: "k"}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seen, []string{"richCmd:executed"}) {
		t.Fatalf("observer saw %v", seen)
	}
}

// TestStandard_Names checks the re-exported constants against the core.
func TestStandard_Names(t *testing.T) {
	pairs := map[string]string{
		behavior.Recovery: mediator.NameRecovery, behavior.Tracing: mediator.NameTracing, behavior.Logging: mediator.NameLogging,
		behavior.Metrics: mediator.NameMetrics, behavior.Timeout: mediator.NameTimeout, behavior.Authorization: mediator.NameAuthorization,
		behavior.RateLimit: mediator.NameRateLimit, behavior.Validation: mediator.NameValidation, behavior.Cache: mediator.NameCache,
		behavior.Retry: mediator.NameRetry, behavior.UnitOfWork: mediator.NameUnitOfWork, behavior.Idempotency: mediator.NameIdempotency,
		behavior.Inbox: mediator.NameInbox,
	}
	for a, b := range pairs {
		if a != b {
			t.Errorf("%s != %s", a, b)
		}
	}
	for _, e := range behavior.Standard(behavior.Config{Store: memstore.New(memstore.Config{}), Cache: cachemodel.NewMemory(), Limiter: newFakeLimiter()}) {
		if !slices.Contains(fullOrder, e.Behavior.Name()) {
			t.Errorf("unexpected behavior %q", e.Behavior.Name())
		}
	}
}
