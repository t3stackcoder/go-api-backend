package behavior

import (
	"context"
	"iter"
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Span attribute keys of the Tracing behavior (5.3).
const (
	AttrRequestName   = attribute.Key("mediator.request.name")
	AttrRequestKind   = attribute.Key("mediator.request.kind")
	AttrCorrelationID = attribute.Key("mediator.correlation_id")
	AttrCausationID   = attribute.Key("mediator.causation_id")
	AttrDepth         = attribute.Key("mediator.depth")
	AttrErrorCode     = attribute.Key("mediator.error.code")
	AttrStreamItems   = attribute.Key("mediator.stream.items")
	AttrConsumerGroup = attribute.Key("mediator.consumer.group")
)

// NewTracing returns the Tracing behavior (5.3): one span named
// "mediator.{kind} {name}" per call with the request attributes, the span
// status derived from the error code (Error for codes that map to a 5xx
// status, Unset otherwise), and mediator.error.code on every failure. On
// the consumer path the span is linked to the producer span through the
// envelope's TraceParent. On a stream the span ends when the sequence ends
// and records mediator.stream.items.
func NewTracing(cfg Config) mediator.Behavior {
	t := &tracing{tracer: cfg.tracer()}
	t.infos.build = newSpanInfo
	return t
}

type tracing struct {
	tracer trace.Tracer
	infos  infoCache[spanInfo]
}

// spanInfo is the per-type part of a span, computed once at Prepare so the
// hot path formats nothing.
type spanInfo struct {
	name string
	opts []trace.SpanStartOption
}

func newSpanInfo(info *mediator.RequestInfo) *spanInfo {
	attrs := []attribute.KeyValue{AttrRequestName.String(info.Name), AttrRequestKind.String(info.Kind.String())}
	kind := trace.SpanKindInternal
	if info.Kind == mediator.KindConsumer {
		attrs = append(attrs, AttrConsumerGroup.String(info.Group))
		kind = trace.SpanKindConsumer
	}
	return &spanInfo{
		name: "mediator." + info.Kind.String() + " " + info.Name,
		opts: []trace.SpanStartOption{trace.WithSpanKind(kind), trace.WithAttributes(attrs...)},
	}
}

func (t *tracing) Name() string { return Tracing }

// Prepare precomputes the span name and static attributes of every type.
func (t *tracing) Prepare(infos []*mediator.RequestInfo) error {
	t.infos.prepare(infos)
	return nil
}

func (t *tracing) start(ctx context.Context, info *mediator.RequestInfo) (context.Context, trace.Span) {
	si := t.infos.get(info)
	opts := si.opts
	if info.Kind == mediator.KindConsumer {
		if env, ok := mediator.EnvelopeFrom(ctx); ok {
			if link, ok := linkFromTraceParent(env.TraceParent); ok {
				opts = append(slices.Clone(opts), trace.WithLinks(link))
			}
		}
	}
	ctx, span := t.tracer.Start(ctx, si.name, opts...)
	if span.IsRecording() {
		span.SetAttributes(
			AttrCorrelationID.String(mediator.CorrelationID(ctx)),
			AttrCausationID.String(mediator.CausationID(ctx).String()),
			AttrDepth.Int(mediator.Depth(ctx)))
	}
	return ctx, span
}

// linkFromTraceParent extracts the producer span context from a W3C
// traceparent value.
func linkFromTraceParent(tp string) (trace.Link, bool) {
	if tp == "" {
		return trace.Link{}, false
	}
	carrier := propagation.MapCarrier{"traceparent": tp}
	sc := trace.SpanContextFromContext(propagation.TraceContext{}.Extract(context.Background(), carrier))
	if !sc.IsValid() {
		return trace.Link{}, false
	}
	return trace.Link{SpanContext: sc}, true
}

// recordError sets mediator.error.code and, for server-side codes, the
// Error status with the error recorded as an event.
func recordError(span trace.Span, err error) {
	if !span.IsRecording() {
		return
	}
	code := mediator.CodeOf(err)
	span.SetAttributes(AttrErrorCode.String(string(code)))
	if mediator.StatusOf(code) >= 500 {
		span.SetStatus(codes.Error, string(code))
		span.RecordError(err)
	}
}

func recordPanic(span trace.Span) {
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(AttrErrorCode.String(string(mediator.CodeInternal)))
	span.SetStatus(codes.Error, OutcomePanic)
}

func (t *tracing) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (res any, err error) {
	ctx, span := t.start(ctx, info)
	done := false
	defer func() {
		if !done {
			recordPanic(span)
		}
		span.End()
	}()
	res, err = next(ctx, req)
	done = true
	if err != nil {
		recordError(span, err)
	}
	return res, err
}

func (t *tracing) HandleStream(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.StreamNext) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		ctx, span := t.start(ctx, info)
		items := 0
		done := false
		defer func() {
			if span.IsRecording() {
				span.SetAttributes(AttrStreamItems.Int(items))
			}
			if !done {
				recordPanic(span)
			}
			span.End()
		}()
		for v, err := range next(ctx, req) {
			if err != nil {
				done = true
				recordError(span, err)
				yield(nil, err)
				return
			}
			items++
			if !yield(v, nil) {
				done = true
				return
			}
		}
		done = true
	}
}
