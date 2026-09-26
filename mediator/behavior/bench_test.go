package behavior_test

import (
	"context"
	"log/slog"
	"testing"

	mnoop "go.opentelemetry.io/otel/metric/noop"
	tnoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior/cachemodel"
)

// Allocation budgets of the default chain with no I/O behaviors active,
// measured by TestSend_DefaultChainAllocations (G17). Spec 4.12 targets 6
// and the chain meets it: the core spends 4 (request box, scope, WithValue,
// response box; a generated correlation ID is kept as a UUID and formatted
// only when read), the Timeout behavior's deadlineCtx costs 1 (its timer and
// Done channel are created on the first Done call, which no-I/O handlers
// never make), and the no-op tracer's ContextWithSpan costs 1; every other
// behavior allocates nothing. The numbers below are the measured floor of
// this chain and gate regressions. Before the G17 work they were 9 warm and
// 10 cold: context.WithTimeout cost 4 and the cold correlation string 1.
const (
	defaultChainAllocsWarm = 6
	defaultChainAllocsCold = 6
)

// benchConfig is the no-I/O configuration: discard logger, no-op tracer and
// meter, nil Store, Cache, and Limiter.
func benchConfig() behavior.Config {
	return behavior.Config{
		Logger: slog.New(slog.DiscardHandler),
		Tracer: tnoop.NewTracerProvider().Tracer("bench"),
		Meter:  mnoop.NewMeterProvider().Meter("bench"),
	}
}

func benchMediator(b testing.TB, entries []behavior.Entry) *mediator.Mediator {
	b.Helper()
	m := mediator.New(mediator.WithLogger(slog.New(slog.DiscardHandler)))
	register(b, m, defaultHooks())
	for _, e := range entries {
		must(b, mediator.Use(m, e.Behavior, e.Options...))
		if h, ok := e.Behavior.(behavior.BuildHook); ok {
			must(b, m.OnBuild(h.OnBuild))
		}
	}
	must(b, m.Build())
	return m
}

func runSend(b *testing.B, m *mediator.Mediator, ctx context.Context, req any) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := m.SendAny(ctx, req); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSend_DefaultChain is the G17 benchmark: the full standard set
// with no I/O behaviors active, over an ambient correlation ID as the HTTP
// adapter always provides.
func BenchmarkSend_DefaultChain(b *testing.B) {
	m := benchMediator(b, behavior.Standard(benchConfig()))
	runSend(b, m, mediator.WithCorrelationID(context.Background(), "corr"), plainCmd{ID: "c"})
}

func BenchmarkSend_DefaultChain_ColdContext(b *testing.B) {
	m := benchMediator(b, behavior.Standard(benchConfig()))
	runSend(b, m, context.Background(), plainCmd{ID: "c"})
}

// BenchmarkSend_Behavior_* measure each behavior in isolation (one behavior
// registered, over the request type that exercises it).
func benchOne(b *testing.B, bh mediator.Behavior, req any, opts ...mediator.UseOption) {
	b.Helper()
	m := benchMediator(b, []behavior.Entry{{Behavior: bh, Options: opts}})
	runSend(b, m, mediator.WithCorrelationID(admin(context.Background()), "corr"), req)
}

func BenchmarkSend_Behavior_None(b *testing.B) {
	m := benchMediator(b, nil)
	runSend(b, m, mediator.WithCorrelationID(context.Background(), "corr"), plainCmd{ID: "c"})
}

func BenchmarkSend_Behavior_Recovery(b *testing.B) {
	benchOne(b, behavior.NewRecovery(benchConfig()), plainCmd{ID: "c"})
}

func BenchmarkSend_Behavior_Tracing(b *testing.B) {
	benchOne(b, behavior.NewTracing(benchConfig()), plainCmd{ID: "c"})
}

func BenchmarkSend_Behavior_Logging(b *testing.B) {
	benchOne(b, behavior.NewLogging(benchConfig()), plainCmd{ID: "c"})
}

func BenchmarkSend_Behavior_Metrics(b *testing.B) {
	benchOne(b, behavior.NewMetrics(benchConfig()), plainCmd{ID: "c"})
}

func BenchmarkSend_Behavior_Timeout(b *testing.B) {
	benchOne(b, behavior.NewTimeout(benchConfig()), plainCmd{ID: "c"})
}

func BenchmarkSend_Behavior_Authorization(b *testing.B) {
	benchOne(b, behavior.NewAuthorization(benchConfig()), richCmd{ID: "c"})
}

func BenchmarkSend_Behavior_RateLimit(b *testing.B) {
	cfg := benchConfig()
	cfg.Limiter = newFakeLimiter()
	benchOne(b, behavior.NewRateLimit(cfg), richCmd{ID: "c"})
}

func BenchmarkSend_Behavior_Validation(b *testing.B) {
	benchOne(b, behavior.NewValidation(benchConfig()), plainCmd{ID: "c"})
}

func BenchmarkSend_Behavior_Retry(b *testing.B) {
	benchOne(b, behavior.NewRetry(benchConfig()), richCmd{ID: "c"})
}

func BenchmarkSend_Behavior_CacheInvalidation(b *testing.B) {
	cfg := benchConfig()
	cfg.Cache = cachemodel.NewMemory()
	benchOne(b, behavior.NewCacheInvalidation(cfg), richCmd{ID: "c", Tags: []string{"t"}})
}

// BenchmarkCache_Hit measures a cached query served from the model backend.
func BenchmarkCache_Hit(b *testing.B) {
	cfg := benchConfig()
	cfg.Cache = cachemodel.NewMemory()
	m := benchMediator(b, []behavior.Entry{{Behavior: behavior.NewCache(cfg), Options: []mediator.UseOption{mediator.Queries()}}})
	ctx := mediator.WithCorrelationID(context.Background(), "corr")
	req := cachedQuery{ID: "hot"}
	if _, err := mediator.Send(ctx, m, req); err != nil {
		b.Fatal(err)
	}
	runSend(b, m, ctx, req)
}

// TestSend_DefaultChainAllocations gates the allocation budget of the
// default chain and reports the measured numbers.
func TestSend_DefaultChainAllocations(t *testing.T) {
	m := benchMediator(t, behavior.Standard(benchConfig()))
	req := plainCmd{ID: "c"}
	cases := []struct {
		name   string
		ctx    context.Context
		budget float64
	}{
		{"ambient correlation", mediator.WithCorrelationID(context.Background(), "corr"), defaultChainAllocsWarm},
		{"cold context", context.Background(), defaultChainAllocsCold},
	}
	for _, c := range cases {
		got := testing.AllocsPerRun(2000, func() {
			if _, err := mediator.Send(c.ctx, m, req); err != nil {
				t.Fatal(err)
			}
		})
		t.Logf("%s: %v allocs per Send (budget %v)", c.name, got, c.budget)
		if got > c.budget {
			t.Errorf("%s: %v allocs per Send, budget %v", c.name, got, c.budget)
		}
	}
}
