package behavior_test

import (
	"context"
	"errors"
	"iter"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
)

func TestMetrics_RequestOutcomes(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	var mode string
	h.hooks.plain = func(ctx context.Context, c plainCmd) (cmdResult, error) {
		h.clock.Advance(250 * time.Millisecond)
		switch mode {
		case "error":
			return cmdResult{}, mediator.E(mediator.CodeNotFound, "x")
		case "timeout":
			return cmdResult{}, mediator.Wrap(mediator.CodeTimeout, "t", context.DeadlineExceeded)
		case "panic":
			panic("p")
		}
		return cmdResult{ID: c.ID}, nil
	}
	for _, mode = range []string{"ok", "error", "timeout", "panic", "ok"} {
		_, _ = mediator.Send(ctx, h.m, plainCmd{ID: "a"})
	}
	rm := h.tel.collect(t)
	for outcome, n := range map[string]uint64{behavior.OutcomeOK: 2, behavior.OutcomeError: 1, behavior.OutcomeTimeout: 1, behavior.OutcomePanic: 1} {
		p := point(t, rm, "mediator.request.duration", map[string]string{"name": "plainCmd", "kind": "command", "outcome": outcome})
		if p.Count != n {
			t.Errorf("%s: count %d, want %d", outcome, p.Count, n)
		}
		if p.Float < 0.25*float64(n) {
			t.Errorf("%s: sum %v seconds", outcome, p.Float)
		}
	}
	if p := point(t, rm, "mediator.request.inflight", map[string]string{"name": "plainCmd", "kind": "command"}); p.Int != 0 {
		t.Errorf("inflight %d after completion", p.Int)
	}
}

func TestMetrics_Inflight(t *testing.T) {
	h := newHarness(t)
	h.hooks.plain = func(context.Context, plainCmd) (cmdResult, error) {
		rm := h.tel.collect(t)
		if p := point(t, rm, "mediator.request.inflight", map[string]string{"name": "plainCmd"}); p.Int != 1 {
			t.Errorf("inflight %d inside the handler", p.Int)
		}
		return cmdResult{}, nil
	}
	if _, err := mediator.Send(context.Background(), h.m, plainCmd{ID: "a"}); err != nil {
		t.Fatal(err)
	}
}

func TestMetrics_NotificationHandlers(t *testing.T) {
	h := newHarness(t)
	if err := mediator.Publish(context.Background(), h.m, thingEvent{ID: "e"}); err != nil {
		t.Fatal(err)
	}
	rm := h.tel.collect(t)
	p := point(t, rm, "mediator.notification.handlers", map[string]string{"event": "thingEvent"})
	if p.Count != 1 || p.Int != 1 {
		t.Fatalf("handlers point %+v", p)
	}
	if !hasPoint(rm, "mediator.request.duration", map[string]string{"name": "thingEvent", "kind": "notification", "outcome": behavior.OutcomeOK}) {
		t.Fatal("notification duration missing")
	}
}

// TestMetrics_Consumer: deliveries are recorded as requests of kind
// consumer; mediator.consumer.processed is left to the transport observer
// (otel.NewConsumersObserver), so a duplicate or a failure shows up in the
// duration histogram only.
func TestMetrics_Consumer(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	env := mediator.Envelope{ID: mediator.NewID(h.clock.Now())}
	if err := h.deliver(ctx, "c1", env); err != nil {
		t.Fatal(err)
	}
	if err := h.deliver(ctx, "c1", env); err != nil { // same event ID: inbox duplicate
		t.Fatal(err)
	}
	h.hooks.consume = func(context.Context, thingStored) error { return errors.New("fail") }
	if err := h.deliver(ctx, "c2", mediator.Envelope{}); err == nil {
		t.Fatal("want error")
	}
	rm := h.tel.collect(t)
	if pts := points(rm, "mediator.consumer.processed"); len(pts) != 0 {
		t.Fatalf("the behavior must not emit mediator.consumer.processed (the transport observer does): %v", pts)
	}
	consumer := map[string]string{"name": "thingStored", "kind": "consumer"}
	if p := point(t, rm, "mediator.request.duration", map[string]string{"name": "thingStored", "kind": "consumer", "outcome": behavior.OutcomeOK}); p.Count != 2 {
		t.Errorf("ok count %d, want 2 (the inbox duplicate is an ok delivery)", p.Count)
	}
	if p := point(t, rm, "mediator.request.duration", map[string]string{"name": "thingStored", "kind": "consumer", "outcome": behavior.OutcomeError}); p.Count != 1 {
		t.Errorf("error count %d, want 1", p.Count)
	}
	if p := point(t, rm, "mediator.request.inflight", consumer); p.Int != 0 {
		t.Errorf("inflight %d after completion", p.Int)
	}
}

func TestMetrics_Stream(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	for range mediator.Stream(ctx, h.m, numStream{N: 2}) {
	}
	for v := range mediator.Stream(ctx, h.m, numStream{N: 2}) {
		if v == 0 {
			break
		}
	}
	h.hooks.stream = func(context.Context, numStream) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) { yield(0, mediator.E(mediator.CodeUnavailable, "x")) }
	}
	for range mediator.Stream(ctx, h.m, numStream{N: 1}) {
	}
	h.hooks.stream = func(context.Context, numStream) iter.Seq2[int, error] {
		return func(func(int, error) bool) { panic("it") }
	}
	for range mediator.Stream(ctx, h.m, numStream{N: 1}) {
	}
	rm := h.tel.collect(t)
	for outcome, n := range map[string]uint64{behavior.OutcomeOK: 2, behavior.OutcomeError: 1, behavior.OutcomePanic: 1} {
		p := point(t, rm, "mediator.request.duration", map[string]string{"name": "numStream", "kind": "stream", "outcome": outcome})
		if p.Count != n {
			t.Errorf("%s: count %d, want %d", outcome, p.Count, n)
		}
	}
	if p := point(t, rm, "mediator.request.inflight", map[string]string{"name": "numStream"}); p.Int != 0 {
		t.Errorf("inflight %d", p.Int)
	}
}

// failingMeter refuses one instrument so instrument creation fails.
type failingMeter struct {
	metric.Meter
}

func (failingMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, errors.New("no counters here")
}

// TestMetrics_InstrumentFailure: a meter that refuses an instrument makes
// Build fail through Prepare while the behavior itself keeps working over
// no-op instruments.
func TestMetrics_InstrumentFailure(t *testing.T) {
	cfg := behavior.Config{Meter: failingMeter{noop.NewMeterProvider().Meter("x")}}
	for name, b := range map[string]mediator.Behavior{
		"metrics":      behavior.NewMetrics(cfg),
		"ratelimit":    behavior.NewRateLimit(behavior.Config{Meter: cfg.Meter, Limiter: newFakeLimiter()}),
		"cache":        behavior.NewCache(behavior.Config{Meter: cfg.Meter, Cache: newHarness(t).backend}),
		"invalidation": behavior.NewCacheInvalidation(behavior.Config{Meter: cfg.Meter, Cache: newHarness(t).backend}),
	} {
		err := b.(mediator.Preparer).Prepare(nil)
		if err == nil || !strings.Contains(err.Error(), "no counters here") {
			t.Errorf("%s: Prepare = %v", name, err)
		}
	}
	m := behavior.NewMetrics(cfg)
	info := &mediator.RequestInfo{Name: "x", Kind: mediator.KindCommand}
	if _, err := m.Handle(context.Background(), nil, info, func(context.Context, any) (any, error) { return nil, nil }); err != nil {
		t.Fatal(err)
	}
	// Standard with such a meter still returns a usable set; Build reports it.
	med := mediator.New()
	must(t, mediator.HandleFunc(med, func(context.Context, plainCmd) (cmdResult, error) { return cmdResult{}, nil }))
	must(t, behavior.UseStandard(med, cfg))
	if err := med.Build(); err == nil || !strings.Contains(err.Error(), "no counters here") {
		t.Fatalf("Build = %v", err)
	}
}

// TestMetrics_UnpreparedInfo covers the slow path of the attribute cache.
func TestMetrics_UnpreparedInfo(t *testing.T) {
	tel := newTelemetry(t)
	b := behavior.NewMetrics(behavior.Config{Meter: tel.mp.Meter("t")})
	info := &mediator.RequestInfo{Name: "late", Kind: mediator.KindQuery}
	for i := 0; i < 2; i++ {
		if _, err := b.Handle(context.Background(), nil, info, func(context.Context, any) (any, error) { return nil, nil }); err != nil {
			t.Fatal(err)
		}
	}
	rm := tel.collect(t)
	if p := point(t, rm, "mediator.request.duration", map[string]string{"name": "late", "kind": "query", "outcome": "ok"}); p.Count != 2 {
		t.Fatalf("count %d", p.Count)
	}
}
