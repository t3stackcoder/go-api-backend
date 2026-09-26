package otel_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/t3stackcoder/go-api-backend/mediator"
	motel "github.com/t3stackcoder/go-api-backend/mediator/otel"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// Every instrument of spec 9.1 with its unit.
var spec91 = map[string]string{
	"mediator.request.duration":              "s",
	"mediator.request.inflight":              "{request}",
	"mediator.notification.handlers":         "{handler}",
	"mediator.outbox.unpublished":            "{row}",
	"mediator.outbox.oldest_unpublished_age": "s",
	"mediator.relay.published":               "{row}",
	"mediator.relay.replayed":                "{row}",
	"mediator.consumer.pending":              "{entry}",
	"mediator.consumer.lag":                  "s",
	"mediator.consumer.processed":            "{delivery}",
	"mediator.consumer.halted":               "{partition}",
	"mediator.lease.owned":                   "{partition}",
	"mediator.lease.handovers":               "{handover}",
	"mediator.idempotency.outcome":           "{command}",
	"mediator.cache.requests":                "{request}",
	"mediator.cache.invalidation_failed":     "{bump}",
	"mediator.ratelimit.decisions":           "{decision}",
	"mediator.ratelimit.degraded":            "{check}",
	"mediator.remote.duration":               "s",
	"mediator.dlq.size":                      "{entry}",
}

type harness struct {
	reader *sdkmetric.ManualReader
	inst   *motel.Instruments
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	inst, err := motel.NewInstruments(mp.Meter(motel.MeterName))
	if err != nil {
		t.Fatal(err)
	}
	return &harness{reader: reader, inst: inst}
}

type point struct {
	attrs map[string]string
	i     int64
	f     float64
	n     uint64
}

func (h *harness) collect(t *testing.T) (map[string]metricdata.Metrics, map[string][]point) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	metrics := map[string]metricdata.Metrics{}
	pts := map[string][]point{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metrics[m.Name] = m
			add := func(set attribute.Set, p point) {
				p.attrs = map[string]string{}
				for _, kv := range set.ToSlice() {
					p.attrs[string(kv.Key)] = kv.Value.String()
				}
				pts[m.Name] = append(pts[m.Name], p)
			}
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, point{i: dp.Value})
				}
			case metricdata.Gauge[int64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, point{i: dp.Value})
				}
			case metricdata.Gauge[float64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, point{f: dp.Value})
				}
			case metricdata.Histogram[float64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, point{n: dp.Count, f: dp.Sum})
				}
			case metricdata.Histogram[int64]:
				for _, dp := range d.DataPoints {
					add(dp.Attributes, point{n: dp.Count, i: dp.Sum})
				}
			}
		}
	}
	return metrics, pts
}

func find(t *testing.T, pts map[string][]point, name string, attrs map[string]string) point {
	t.Helper()
	for _, p := range pts[name] {
		ok := true
		for k, v := range attrs {
			ok = ok && p.attrs[k] == v
		}
		if ok {
			return p
		}
	}
	t.Fatalf("%s %v not found in %v", name, attrs, pts[name])
	return point{}
}

type fakeRelay struct{ st pg.RelayStats }

func (f fakeRelay) Stats() pg.RelayStats { return f.st }

// TestInstruments_Complete records every instrument once and checks that
// the collected names and units match spec 9.1 exactly.
func TestInstruments_Complete(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	i := h.inst
	i.RequestDuration.Record(ctx, 0.5)
	i.RequestInflight.Add(ctx, 1)
	i.NotificationHandlers.Record(ctx, 2)
	i.ConsumerPending.Record(ctx, 1)
	i.ConsumerLag.Record(ctx, 1)
	i.ConsumerProcessed.Add(ctx, 1)
	i.ConsumerHalted.Record(ctx, 1)
	i.LeaseOwned.Record(ctx, 1)
	i.LeaseHandovers.Add(ctx, 1)
	i.IdempotencyOutcome.Add(ctx, 1)
	i.CacheRequests.Add(ctx, 1)
	i.CacheInvalidationFailed.Add(ctx, 1)
	i.RateLimitDecisions.Add(ctx, 1)
	i.RateLimitDegraded.Add(ctx, 1)
	i.RemoteDuration.Record(ctx, 1)
	i.DLQSize.Record(ctx, 1)
	reg, err := motel.RelayCollector(i, fakeRelay{st: pg.RelayStats{Slots: []pg.SlotStats{{Topic: "t", Partition: 0, Owned: true}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reg.Unregister(); err != nil {
			t.Error(err)
		}
	}()

	metrics, _ := h.collect(t)
	var names []string
	for name := range metrics {
		names = append(names, name)
	}
	slices.Sort(names)
	var want []string
	for name := range spec91 {
		want = append(want, name)
	}
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Fatalf("instruments %v\nwant %v", names, want)
	}
	for name, unit := range spec91 {
		if metrics[name].Unit != unit {
			t.Errorf("%s unit %q, want %q", name, metrics[name].Unit, unit)
		}
		if metrics[name].Description == "" {
			t.Errorf("%s has no description", name)
		}
	}
	if i.Meter() == nil {
		t.Fatal("Meter()")
	}
}

func TestInstruments_DefaultMeterAndErrors(t *testing.T) {
	if inst, err := motel.NewInstruments(nil); err != nil || inst == nil || inst.Meter() == nil {
		t.Fatal(inst, err)
	}
	_, err := motel.NewInstruments(failingMeter{noop.NewMeterProvider().Meter("x")})
	if err == nil || strings.Count(err.Error(), "refused") != 9 {
		t.Fatalf("want every failure joined (7 counters, 2 histograms), got %v", err)
	}
}

type failingMeter struct{ metric.Meter }

func (failingMeter) Int64Counter(string, ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	return nil, errors.New("refused")
}

func (failingMeter) Float64Histogram(string, ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	return nil, errors.New("refused")
}

func TestConsumersObserver(t *testing.T) {
	h := newHarness(t)
	obs := motel.NewConsumersObserver(h.inst)
	obs.LeaseAcquired("g", "t", 0, 7)
	obs.LeaseAcquired("g", "t", 1, 8)
	obs.LeaseEnded("g", "t", 0, "released")
	obs.LeasesOwned("g", "t", 2)
	obs.Processed("g", redisx.OutcomeOK)
	obs.Processed("g", redisx.OutcomeOK)
	obs.Processed("g", redisx.OutcomeDLQ)
	obs.Halted("g", "t", 1, true)
	obs.Halted("g", "t", 0, false)
	obs.Pending("g", "t", 1, 5, 1500*time.Millisecond)
	obs.DLQSize("g", 3)
	_, pts := h.collect(t)
	scope := map[string]string{"group": "g", "topic": "t"}
	if p := find(t, pts, "mediator.lease.handovers", scope); p.i != 2 {
		t.Errorf("handovers %d", p.i)
	}
	if p := find(t, pts, "mediator.lease.owned", scope); p.i != 2 {
		t.Errorf("owned %d", p.i)
	}
	if p := find(t, pts, "mediator.consumer.processed", map[string]string{"group": "g", "outcome": "ok"}); p.i != 2 {
		t.Errorf("processed ok %d", p.i)
	}
	if p := find(t, pts, "mediator.consumer.processed", map[string]string{"group": "g", "outcome": "dlq"}); p.i != 1 {
		t.Errorf("processed dlq %d", p.i)
	}
	if p := find(t, pts, "mediator.consumer.halted", map[string]string{"group": "g", "topic": "t", "partition": "1"}); p.i != 1 {
		t.Errorf("halted %d", p.i)
	}
	if p := find(t, pts, "mediator.consumer.halted", map[string]string{"partition": "0"}); p.i != 0 {
		t.Errorf("halted %d", p.i)
	}
	if p := find(t, pts, "mediator.consumer.pending", map[string]string{"group": "g", "topic": "t", "partition": "1"}); p.i != 5 {
		t.Errorf("pending %d", p.i)
	}
	if p := find(t, pts, "mediator.consumer.lag", map[string]string{"partition": "1"}); p.f != 1.5 {
		t.Errorf("lag %v", p.f)
	}
	if p := find(t, pts, "mediator.dlq.size", map[string]string{"group": "g"}); p.i != 3 {
		t.Errorf("dlq %d", p.i)
	}
}

func TestIdempotencyObserver(t *testing.T) {
	h := newHarness(t)
	obs := motel.NewIdempotencyObserver(h.inst)
	obs("CreateOrder", pg.OutcomeExecuted)
	obs("CreateOrder", pg.OutcomeReplayed)
	obs("CreateOrder", pg.OutcomeReplayed)
	_, pts := h.collect(t)
	if p := find(t, pts, "mediator.idempotency.outcome", map[string]string{"name": "CreateOrder", "outcome": "replayed"}); p.i != 2 {
		t.Errorf("replayed %d", p.i)
	}
	if p := find(t, pts, "mediator.idempotency.outcome", map[string]string{"name": "CreateOrder", "outcome": "executed"}); p.i != 1 {
		t.Errorf("executed %d", p.i)
	}
}

func TestRelayCollector(t *testing.T) {
	h := newHarness(t)
	relay := fakeRelay{st: pg.RelayStats{
		Published: 10, Replayed: 2,
		Slots: []pg.SlotStats{
			{Topic: "orders", Partition: 0, Owned: true, Unpublished: 4, OldestAge: 2500 * time.Millisecond},
			{Topic: "orders", Partition: 1, Owned: false, Unpublished: 99},
		},
	}}
	reg, err := motel.RelayCollector(h.inst, relay)
	if err != nil {
		t.Fatal(err)
	}
	_, pts := h.collect(t)
	if p := find(t, pts, "mediator.relay.published", nil); p.i != 10 {
		t.Errorf("published %d", p.i)
	}
	if p := find(t, pts, "mediator.relay.replayed", nil); p.i != 2 {
		t.Errorf("replayed %d", p.i)
	}
	owned := map[string]string{"topic": "orders", "partition": "0"}
	if p := find(t, pts, "mediator.outbox.unpublished", owned); p.i != 4 {
		t.Errorf("unpublished %d", p.i)
	}
	if p := find(t, pts, "mediator.outbox.oldest_unpublished_age", owned); p.f != 2.5 {
		t.Errorf("age %v", p.f)
	}
	if len(pts["mediator.outbox.unpublished"]) != 1 {
		t.Errorf("only owned slots are observed: %v", pts["mediator.outbox.unpublished"])
	}
	if err := reg.Unregister(); err != nil {
		t.Fatal(err)
	}
	_, pts = h.collect(t)
	if len(pts["mediator.relay.published"]) != 0 {
		t.Error("unregistered callback still observed")
	}
	// A real relay satisfies the interface.
	var _ motel.RelayStats = (*pg.Relay)(nil)
}

type fakeDispatcher struct{ err error }

func (f fakeDispatcher) Send(context.Context, *mediator.RequestInfo, any) (any, error) {
	return "res", f.err
}

func TestInstrumentRemote(t *testing.T) {
	h := newHarness(t)
	info := &mediator.RequestInfo{Name: "Ping"}
	cases := []struct {
		err     error
		outcome string
	}{
		{nil, motel.RemoteOutcomeOK},
		{mediator.E(mediator.CodeUnavailable, "x"), motel.RemoteOutcomeError},
		{mediator.Wrap(mediator.CodeTimeout, "t", context.DeadlineExceeded), motel.RemoteOutcomeTimeout},
	}
	for _, c := range cases {
		d := motel.InstrumentRemote(h.inst, fakeDispatcher{err: c.err})
		res, err := d.Send(context.Background(), info, nil)
		if res != "res" || !errors.Is(err, c.err) {
			t.Fatalf("pass-through: %v %v", res, err)
		}
	}
	_, pts := h.collect(t)
	for _, c := range cases {
		if p := find(t, pts, "mediator.remote.duration", map[string]string{"name": "Ping", "outcome": c.outcome}); p.n != 1 {
			t.Errorf("%s: count %d", c.outcome, p.n)
		}
	}
}
