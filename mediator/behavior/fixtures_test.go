package behavior_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior/cachemodel"
	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
	"github.com/t3stackcoder/go-api-backend/mediator/retry"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

// ---------------------------------------------------------------- request types

type cmdResult struct {
	ID string `json:"id"`
	N  int    `json:"n"`
}

type queryResult struct {
	ID    string `json:"id"`
	Value int64  `json:"value"`
}

// plainCmd carries no trait.
type plainCmd struct {
	mediator.Command[cmdResult]
	ID string `json:"id" validate:"required"`
}

// richCmd carries every command trait.
type richCmd struct {
	mediator.Command[cmdResult]
	ID     string   `json:"id"`
	Key    string   `json:"key,omitempty"`
	Tags   []string `json:"tags,omitempty"`
	Secret string   `json:"secret,omitempty" log:"redact"`
}

var (
	richPolicy      = ratelimit.Policy{Rate: 10, Period: time.Second, Burst: 5}
	richRetryPolicy = retry.Policy{MaxAttempts: 3, BaseDelay: 10 * time.Millisecond, MaxDelay: 50 * time.Millisecond}
)

func (richCmd) Requires() authz.Requirement    { return authz.Role("admin") }
func (richCmd) RateLimit() ratelimit.Policy    { return richPolicy }
func (richCmd) RetryPolicy() retry.Policy      { return richRetryPolicy }
func (c richCmd) Invalidates() []string        { return c.Tags }
func (c richCmd) IdempotencyKey() string       { return c.Key }
func (richCmd) Timeout() time.Duration         { return 2 * time.Second }
func (richCmd) Validate(context.Context) error { return nil }

// cachedQuery is cached under tag "things".
type cachedQuery struct {
	mediator.Query[queryResult]
	ID string `json:"id"`
	// TTLSeconds overrides the TTL; a time.Duration field would need a
	// json format tag under encoding/json/v2.
	TTLSeconds int `json:"ttlSeconds,omitempty"`
}

func (cachedQuery) CacheTags() []string       { return []string{"things"} }
func (q cachedQuery) CacheTTL() time.Duration { return time.Duration(q.TTLSeconds) * time.Second }

type plainQuery struct {
	mediator.Query[queryResult]
	ID string `json:"id"`
}

type numStream struct {
	mediator.StreamQuery[int]
	N int `json:"n"`
}

type noUowCmd struct {
	mediator.Command[mediator.Void]
	ID string `json:"id"`
}

func (noUowCmd) NoUnitOfWork() {}

type thingEvent struct {
	mediator.Event
	ID string `json:"id"`
}

type thingStored struct {
	mediator.Event
	ID string `json:"id"`
}

func (e thingStored) StreamKey() string { return e.ID }

const consumerGroup = "projector"

// ---------------------------------------------------------------- hooks

// hooks are the handler bodies; tests replace them per case.
type hooks struct {
	plain   func(context.Context, plainCmd) (cmdResult, error)
	rich    func(context.Context, richCmd) (cmdResult, error)
	cached  func(context.Context, cachedQuery) (queryResult, error)
	query   func(context.Context, plainQuery) (queryResult, error)
	stream  func(context.Context, numStream) iter.Seq2[int, error]
	nouow   func(context.Context, noUowCmd) (mediator.Void, error)
	event   func(context.Context, thingEvent) error
	consume func(context.Context, thingStored) error
}

func defaultHooks() *hooks {
	return &hooks{
		plain: func(_ context.Context, c plainCmd) (cmdResult, error) { return cmdResult{ID: c.ID}, nil },
		rich:  func(_ context.Context, c richCmd) (cmdResult, error) { return cmdResult{ID: c.ID}, nil },
		cached: func(_ context.Context, q cachedQuery) (queryResult, error) {
			return queryResult{ID: q.ID, Value: 1}, nil
		},
		query: func(_ context.Context, q plainQuery) (queryResult, error) { return queryResult{ID: q.ID}, nil },
		stream: func(_ context.Context, s numStream) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) {
				for i := 0; i < s.N; i++ {
					if !yield(i, nil) {
						return
					}
				}
			}
		},
		nouow:   func(context.Context, noUowCmd) (mediator.Void, error) { return mediator.Void{}, nil },
		event:   func(context.Context, thingEvent) error { return nil },
		consume: func(context.Context, thingStored) error { return nil },
	}
}

func register(t testing.TB, m *mediator.Mediator, h *hooks) {
	t.Helper()
	must(t, mediator.HandleFunc(m, func(ctx context.Context, c plainCmd) (cmdResult, error) { return h.plain(ctx, c) }))
	must(t, mediator.HandleFunc(m, func(ctx context.Context, c richCmd) (cmdResult, error) { return h.rich(ctx, c) }))
	must(t, mediator.HandleFunc(m, func(ctx context.Context, q cachedQuery) (queryResult, error) { return h.cached(ctx, q) }))
	must(t, mediator.HandleFunc(m, func(ctx context.Context, q plainQuery) (queryResult, error) { return h.query(ctx, q) }))
	must(t, mediator.HandleStreamFunc(m, func(ctx context.Context, s numStream) iter.Seq2[int, error] { return h.stream(ctx, s) }))
	must(t, mediator.HandleFunc(m, func(ctx context.Context, c noUowCmd) (mediator.Void, error) { return h.nouow(ctx, c) }))
	must(t, mediator.OnFunc(m, func(ctx context.Context, e thingEvent) error { return h.event(ctx, e) }))
	must(t, mediator.ConsumeFunc(m, consumerGroup, func(ctx context.Context, e thingStored) error { return h.consume(ctx, e) }))
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------- log sink

type logRecord struct {
	Level slog.Level
	Msg   string
	Attrs map[string]any
}

// logSink is a slog.Handler that keeps every record with its attributes
// flattened (group.key).
type logSink struct {
	mu      sync.Mutex
	level   slog.Level
	records []logRecord
	attrs   []slog.Attr
	groups  []string
}

func newLogSink(level slog.Level) *logSink { return &logSink{level: level} }

func (s *logSink) Enabled(_ context.Context, l slog.Level) bool { return l >= s.level }

func (s *logSink) Handle(_ context.Context, r slog.Record) error {
	rec := logRecord{Level: r.Level, Msg: r.Message, Attrs: map[string]any{}}
	prefix := strings.Join(s.groups, ".")
	if prefix != "" {
		prefix += "."
	}
	for _, a := range s.attrs {
		flatten(rec.Attrs, prefix, a)
	}
	r.Attrs(func(a slog.Attr) bool {
		flatten(rec.Attrs, prefix, a)
		return true
	})
	s.mu.Lock()
	s.records = append(s.records, rec)
	s.mu.Unlock()
	return nil
}

func flatten(out map[string]any, prefix string, a slog.Attr) {
	v := a.Value.Resolve()
	if v.Kind() == slog.KindGroup {
		for _, sub := range v.Group() {
			flatten(out, prefix+a.Key+".", sub)
		}
		return
	}
	out[prefix+a.Key] = v.Any()
}

func (s *logSink) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logSink{level: s.level, records: s.records, attrs: append(slices.Clone(s.attrs), attrs...), groups: s.groups, mu: sync.Mutex{}}
}

func (s *logSink) WithGroup(name string) slog.Handler {
	return &logSink{level: s.level, attrs: s.attrs, groups: append(slices.Clone(s.groups), name)}
}

func (s *logSink) all() []logRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.records)
}

func (s *logSink) find(msg string) []logRecord {
	var out []logRecord
	for _, r := range s.all() {
		if r.Msg == msg {
			out = append(out, r)
		}
	}
	return out
}

func (s *logSink) reset() {
	s.mu.Lock()
	s.records = nil
	s.mu.Unlock()
}

// ---------------------------------------------------------------- fakes

type limiterCall struct {
	Name, Key string
	Policy    ratelimit.Policy
}

// fakeLimiter returns a scripted decision or error.
type fakeLimiter struct {
	mu       sync.Mutex
	decision ratelimit.Decision
	err      error
	calls    []limiterCall
}

func newFakeLimiter() *fakeLimiter {
	return &fakeLimiter{decision: ratelimit.Decision{Allowed: true, Remaining: 1}}
}

func (l *fakeLimiter) Check(_ context.Context, name, key string, p ratelimit.Policy) (ratelimit.Decision, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, limiterCall{Name: name, Key: key, Policy: p})
	return l.decision, l.err
}

func (l *fakeLimiter) set(d ratelimit.Decision, err error) {
	l.mu.Lock()
	l.decision, l.err = d, err
	l.mu.Unlock()
}

func (l *fakeLimiter) lastCall() limiterCall {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls[len(l.calls)-1]
}

// transientErr classifies itself as transient (mediator.Transienter).
type transientErr struct{ msg string }

func (e transientErr) Error() string   { return e.msg }
func (e transientErr) Transient() bool { return true }

// ---------------------------------------------------------------- telemetry

type telemetry struct {
	reader *sdkmetric.ManualReader
	spans  *tracetest.InMemoryExporter
	tp     *sdktrace.TracerProvider
	mp     *sdkmetric.MeterProvider
}

func newTelemetry(t testing.TB) *telemetry {
	t.Helper()
	tel := &telemetry{reader: sdkmetric.NewManualReader(), spans: tracetest.NewInMemoryExporter()}
	tel.mp = sdkmetric.NewMeterProvider(sdkmetric.WithReader(tel.reader))
	tel.tp = sdktrace.NewTracerProvider(sdktrace.WithSyncer(tel.spans))
	t.Cleanup(func() {
		_ = tel.mp.Shutdown(context.Background())
		_ = tel.tp.Shutdown(context.Background())
	})
	return tel
}

func (tel *telemetry) collect(t testing.TB) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := tel.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

// metricPoint is one data point flattened for assertions.
type metricPoint struct {
	Attrs map[string]string
	Int   int64
	Float float64
	Count uint64 // histogram count
}

func (p metricPoint) matches(want map[string]string) bool {
	for k, v := range want {
		if p.Attrs[k] != v {
			return false
		}
	}
	return true
}

func attrMap(set attribute.Set) map[string]string {
	out := map[string]string{}
	for _, kv := range set.ToSlice() {
		out[string(kv.Key)] = kv.Value.Emit()
	}
	return out
}

// points returns every data point of the named instrument.
func points(rm metricdata.ResourceMetrics, name string) []metricPoint {
	var out []metricPoint
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					out = append(out, metricPoint{Attrs: attrMap(dp.Attributes), Int: dp.Value})
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					out = append(out, metricPoint{Attrs: attrMap(dp.Attributes), Int: dp.Value})
				}
			case metricdata.Gauge[float64]:
				for _, dp := range d.DataPoints {
					out = append(out, metricPoint{Attrs: attrMap(dp.Attributes), Float: dp.Value})
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					out = append(out, metricPoint{Attrs: attrMap(dp.Attributes), Count: dp.Count, Float: dp.Sum})
				}
			case metricdata.Histogram[int64]:
				for _, dp := range d.DataPoints {
					out = append(out, metricPoint{Attrs: attrMap(dp.Attributes), Count: dp.Count, Int: dp.Sum})
				}
			}
		}
	}
	return out
}

// point returns the single data point of name matching attrs, failing when
// there is none or more than one.
func point(t testing.TB, rm metricdata.ResourceMetrics, name string, attrs map[string]string) metricPoint {
	t.Helper()
	var found []metricPoint
	for _, p := range points(rm, name) {
		if p.matches(attrs) {
			found = append(found, p)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%s %v: %d matching points in %v", name, attrs, len(found), points(rm, name))
	}
	return found[0]
}

func hasPoint(rm metricdata.ResourceMetrics, name string, attrs map[string]string) bool {
	for _, p := range points(rm, name) {
		if p.matches(attrs) {
			return true
		}
	}
	return false
}

func spanNamed(t testing.TB, tel *telemetry, name string) tracetest.SpanStub {
	t.Helper()
	for _, s := range tel.spans.GetSpans() {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no span %q in %v", name, spanNames(tel))
	return tracetest.SpanStub{}
}

func spanNames(tel *telemetry) []string {
	var out []string
	for _, s := range tel.spans.GetSpans() {
		out = append(out, s.Name)
	}
	return out
}

func spanAttr(s tracetest.SpanStub, key attribute.Key) (string, bool) {
	for _, kv := range s.Attributes {
		if kv.Key == key {
			return kv.Value.Emit(), true
		}
	}
	return "", false
}

// ---------------------------------------------------------------- harness

// harness is a built mediator with the full standard set over fakes.
type harness struct {
	m        *mediator.Mediator
	hooks    *hooks
	store    *memstore.Store
	backend  *cachemodel.Memory
	limiter  *fakeLimiter
	logs     *logSink
	tel      *telemetry
	clock    *testkit.FakeClock
	entries  []behavior.Entry
	order    *callLog
	preBuild []func(*mediator.Mediator) error
}

// callLog records the behaviors invoked per call, outermost first.
type callLog struct {
	mu    sync.Mutex
	names []string
}

func (c *callLog) add(name string) {
	c.mu.Lock()
	c.names = append(c.names, name)
	c.mu.Unlock()
}

func (c *callLog) take() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.names
	c.names = nil
	return out
}

// recorder wraps a standard behavior and records its name on every call.
// It forwards HandleStream, Prepare, and OnBuild so the wrapped behavior
// keeps its full role in the chain.
type recorder struct {
	inner mediator.Behavior
	log   *callLog
}

func (r recorder) Name() string { return r.inner.Name() }

func (r recorder) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	r.log.add(r.inner.Name())
	return r.inner.Handle(ctx, req, info, next)
}

func (r recorder) HandleStream(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.StreamNext) iter.Seq2[any, error] {
	r.log.add(r.inner.Name())
	if sb, ok := r.inner.(mediator.StreamBehavior); ok {
		return sb.HandleStream(ctx, req, info, next)
	}
	res, err := r.inner.Handle(ctx, req, info, func(ctx context.Context, req any) (any, error) {
		return next(ctx, req), nil
	})
	if err != nil {
		return func(yield func(any, error) bool) { yield(nil, err) }
	}
	return res.(iter.Seq2[any, error])
}

func (r recorder) Prepare(infos []*mediator.RequestInfo) error {
	if p, ok := r.inner.(mediator.Preparer); ok {
		return p.Prepare(infos)
	}
	return nil
}

func (r recorder) OnBuild(m *mediator.Mediator) error {
	if h, ok := r.inner.(behavior.BuildHook); ok {
		return h.OnBuild(m)
	}
	return nil
}

type harnessOption func(*harness, *behavior.Config)

func withRecorders() harnessOption {
	return func(h *harness, _ *behavior.Config) { h.order = &callLog{} }
}

func withConfig(f func(*behavior.Config)) harnessOption {
	return func(_ *harness, cfg *behavior.Config) { f(cfg) }
}

func withoutStore() harnessOption {
	return func(h *harness, cfg *behavior.Config) { h.store = nil; cfg.Store = nil }
}

func withoutCache() harnessOption {
	return func(h *harness, cfg *behavior.Config) { h.backend = nil; cfg.Cache = nil }
}

func withoutLimiter() harnessOption {
	return func(h *harness, cfg *behavior.Config) { h.limiter = nil; cfg.Limiter = nil }
}

// withPreBuild runs f on the mediator after the standard set is registered
// and before Build, so a test can add behaviors of its own.
func withPreBuild(f func(m *mediator.Mediator) error) harnessOption {
	return func(h *harness, _ *behavior.Config) { h.preBuild = append(h.preBuild, f) }
}

// newHarness builds the standard set over memstore, the model cache, a fake
// limiter, a recording log sink, and the otel SDK test providers, and
// registers every fixture type.
func newHarness(t testing.TB, opts ...harnessOption) *harness {
	t.Helper()
	h := &harness{
		hooks:   defaultHooks(),
		backend: cachemodel.NewMemory(),
		limiter: newFakeLimiter(),
		logs:    newLogSink(slog.LevelDebug),
		tel:     newTelemetry(t),
		clock:   testkit.NewFakeClock(time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)),
	}
	h.store = memstore.New(memstore.Config{Clock: h.clock})
	cfg := behavior.Config{
		Logger:  slog.New(h.logs),
		Tracer:  h.tel.tp.Tracer("test"),
		Meter:   h.tel.mp.Meter("test"),
		Clock:   h.clock,
		Store:   h.store,
		Cache:   h.backend,
		Limiter: h.limiter,
	}
	for _, o := range opts {
		o(h, &cfg)
	}
	h.m = mediator.New(mediator.WithLogger(slog.New(h.logs)))
	register(t, h.m, h.hooks)
	h.entries = behavior.Standard(cfg)
	for _, e := range h.entries {
		b := e.Behavior
		if h.order != nil {
			b = recorder{inner: b, log: h.order}
		}
		must(t, mediator.Use(h.m, b, e.Options...))
		if hook, ok := b.(behavior.BuildHook); ok {
			must(t, h.m.OnBuild(hook.OnBuild))
		}
	}
	for _, f := range h.preBuild {
		must(t, f(h.m))
	}
	must(t, h.m.Build())
	t.Cleanup(func() {
		for _, e := range h.entries {
			if c, ok := e.Behavior.(interface{ Close() error }); ok {
				_ = c.Close()
			}
		}
	})
	return h
}

// deliver runs the consumer chain for one thingStored event.
func (h *harness) deliver(ctx context.Context, id string, env mediator.Envelope) error {
	if env.Type == "" {
		env.Type = "thingStored"
	}
	if env.ID == [16]byte{} {
		env.ID = mediator.NewID(h.clock.Now())
	}
	env.StreamKey = id
	return h.m.Deliver(ctx, consumerGroup, env, []byte(fmt.Sprintf(`{"id":%q}`, id)))
}

func admin(ctx context.Context) context.Context {
	return authz.WithPrincipal(ctx, authz.Principal{Subject: "alice", Roles: []string{"admin"}})
}

func codeIs(t testing.TB, err error, code mediator.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error with code %s, got nil", code)
	}
	if got := mediator.CodeOf(err); got != code {
		t.Fatalf("want code %s, got %s (%v)", code, got, err)
	}
}

func isPanicError(err error) bool {
	var pe *mediator.PanicError
	return errors.As(err, &pe)
}
