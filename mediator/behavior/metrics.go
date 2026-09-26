package behavior

import (
	"context"
	"iter"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/t3stackcoder/go-api-backend/mediator"
	motel "github.com/t3stackcoder/go-api-backend/mediator/otel"
)

// NewMetrics returns the Metrics behavior (5.5, 9.1): mediator.request.duration
// (seconds) and mediator.request.inflight with name and kind, the duration
// carrying outcome ok, error, panic, or timeout; mediator.notification.handlers
// on the publish path; and mediator.consumer.processed with outcome ok,
// dedup, or error on the consumer path. Instruments are created once here;
// attribute sets are precomputed per request type at Prepare so a call
// allocates nothing for them. A meter that refuses an instrument leaves the
// behavior recording into no-op instruments and Prepare reports the error.
//
// Note that redisx.Consumers also reports mediator.consumer.processed
// through its Observer (with the dlq outcome the behavior cannot see); wire
// one or the other into the same meter, not both.
func NewMetrics(cfg Config) mediator.Behavior {
	inst, err := cfg.instruments()
	m := &metrics{inst: inst, clock: cfg.clock(), err: err}
	m.attrs.build = newMetricAttrs
	return m
}

type metrics struct {
	inst  *motel.Instruments
	clock mediator.Clock
	err   error
	attrs infoCache[metricAttrs]
}

// consumerOutcome indexes metricAttrs.consumer.
type consumerOutcome uint8

const (
	consumerOK consumerOutcome = iota
	consumerDedup
	consumerError
)

var consumerOutcomeNames = [...]string{motel.ConsumerOutcomeOK, motel.ConsumerOutcomeDedup, motel.ConsumerOutcomeError}

// metricAttrs holds the precomputed attribute options of one type.
type metricAttrs struct {
	inflight []metric.AddOption
	outcome  [4][]metric.RecordOption
	handlers []metric.RecordOption
	consumer [3][]metric.AddOption
}

func newMetricAttrs(info *mediator.RequestInfo) *metricAttrs {
	name := attribute.String(motel.AttrName, info.Name)
	kind := attribute.String(motel.AttrKind, info.Kind.String())
	a := &metricAttrs{
		inflight: []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(name, kind))},
		handlers: []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(attribute.String(motel.AttrEvent, info.Name)))},
	}
	for i, o := range outcomeNames {
		a.outcome[i] = []metric.RecordOption{metric.WithAttributeSet(attribute.NewSet(name, kind, attribute.String(motel.AttrOutcome, o)))}
	}
	group := attribute.String(motel.AttrGroup, info.Group)
	for i, o := range consumerOutcomeNames {
		a.consumer[i] = []metric.AddOption{metric.WithAttributeSet(attribute.NewSet(group, attribute.String(motel.AttrOutcome, o)))}
	}
	return a
}

func (m *metrics) Name() string { return Metrics }

// Prepare precomputes the attribute sets of every type and reports an
// instrument creation failure.
func (m *metrics) Prepare(infos []*mediator.RequestInfo) error {
	m.attrs.prepare(infos)
	return m.err
}

// finish records the end of one call. o is the outcome; for the consumer
// path the delivery outcome is derived from it and the consumer state.
func (m *metrics) finish(ctx context.Context, info *mediator.RequestInfo, a *metricAttrs, secs float64, o outcome) {
	m.inst.RequestInflight.Add(ctx, -1, a.inflight...)
	m.inst.RequestDuration.Record(ctx, secs, a.outcome[o]...)
	switch info.Kind {
	case mediator.KindNotification:
		m.inst.NotificationHandlers.Record(ctx, int64(info.Handlers), a.handlers...)
	case mediator.KindConsumer:
		co := consumerOK
		if o != outcomeOK {
			co = consumerError
		} else if st, ok := mediator.ConsumerStateFrom(ctx); ok && st.Duplicate {
			co = consumerDedup
		}
		m.inst.ConsumerProcessed.Add(ctx, 1, a.consumer[co]...)
	default:
	}
}

func (m *metrics) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (res any, err error) {
	a := m.attrs.get(info)
	m.inst.RequestInflight.Add(ctx, 1, a.inflight...)
	start := m.clock.Now()
	done := false
	defer func() {
		o := outcomePanic
		if done {
			o = outcomeOf(err)
		}
		m.finish(ctx, info, a, m.clock.Now().Sub(start).Seconds(), o)
	}()
	res, err = next(ctx, req)
	done = true
	return res, err
}

func (m *metrics) HandleStream(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.StreamNext) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		a := m.attrs.get(info)
		m.inst.RequestInflight.Add(ctx, 1, a.inflight...)
		start := m.clock.Now()
		o := outcomePanic
		defer func() {
			m.finish(ctx, info, a, m.clock.Now().Sub(start).Seconds(), o)
		}()
		for v, err := range next(ctx, req) {
			if err != nil {
				o = outcomeOf(err)
				yield(nil, err)
				return
			}
			if !yield(v, nil) {
				o = outcomeOK
				return
			}
		}
		o = outcomeOK
	}
}
