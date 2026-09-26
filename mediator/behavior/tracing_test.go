package behavior_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
)

func TestTracing_RequestSpan(t *testing.T) {
	h := newHarness(t)
	ctx := mediator.WithCorrelationID(context.Background(), "corr-1")
	if _, err := mediator.Send(ctx, h.m, plainCmd{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	s := spanNamed(t, h.tel, "mediator.command plainCmd")
	want := map[string]string{
		string(behavior.AttrRequestName):   "plainCmd",
		string(behavior.AttrRequestKind):   "command",
		string(behavior.AttrCorrelationID): "corr-1",
		string(behavior.AttrDepth):         "1",
	}
	for k, v := range want {
		if got, _ := spanAttr(s, attribute.Key(k)); got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
	}
	if got, ok := spanAttr(s, behavior.AttrCausationID); !ok || got == "" {
		t.Errorf("causation id attribute missing: %q %v", got, ok)
	}
	if s.Status.Code != codes.Unset || s.SpanKind != trace.SpanKindInternal {
		t.Errorf("status %v kind %v", s.Status, s.SpanKind)
	}
	if _, ok := spanAttr(s, behavior.AttrErrorCode); ok {
		t.Error("success must not record an error code")
	}
}

func TestTracing_Status(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status codes.Code
		code   string
	}{
		{"client error", mediator.E(mediator.CodeNotFound, "nope"), codes.Unset, "not_found"},
		{"server error", mediator.E(mediator.CodeUnavailable, "down"), codes.Error, "unavailable"},
		{"unknown error", errors.New("plain"), codes.Error, "internal"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			h.hooks.plain = func(context.Context, plainCmd) (cmdResult, error) { return cmdResult{}, c.err }
			_, err := mediator.Send(context.Background(), h.m, plainCmd{ID: "a"})
			if err == nil {
				t.Fatal("want error")
			}
			s := spanNamed(t, h.tel, "mediator.command plainCmd")
			if s.Status.Code != c.status {
				t.Errorf("status %v, want %v", s.Status.Code, c.status)
			}
			if got, _ := spanAttr(s, behavior.AttrErrorCode); got != c.code {
				t.Errorf("error code %q, want %q", got, c.code)
			}
			if c.status == codes.Error && len(s.Events) == 0 {
				t.Error("server error must be recorded as an event")
			}
		})
	}
}

func TestTracing_Panic(t *testing.T) {
	h := newHarness(t)
	h.hooks.plain = func(context.Context, plainCmd) (cmdResult, error) { panic("x") }
	_, _ = mediator.Send(context.Background(), h.m, plainCmd{ID: "a"})
	s := spanNamed(t, h.tel, "mediator.command plainCmd")
	if s.Status.Code != codes.Error || s.Status.Description != behavior.OutcomePanic {
		t.Fatalf("status %+v", s.Status)
	}
	if got, _ := spanAttr(s, behavior.AttrErrorCode); got != "internal" {
		t.Fatalf("error code %q", got)
	}
}

func TestTracing_NestedSendIsChild(t *testing.T) {
	h := newHarness(t)
	h.hooks.plain = func(ctx context.Context, c plainCmd) (cmdResult, error) {
		_, err := mediator.Send(ctx, h.m, plainQuery{ID: c.ID})
		return cmdResult{ID: c.ID}, err
	}
	if _, err := mediator.Send(context.Background(), h.m, plainCmd{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	outer := spanNamed(t, h.tel, "mediator.command plainCmd")
	inner := spanNamed(t, h.tel, "mediator.query plainQuery")
	if inner.Parent.SpanID() != outer.SpanContext.SpanID() {
		t.Fatal("nested Send must produce a child span")
	}
	if got, _ := spanAttr(inner, behavior.AttrDepth); got != "2" {
		t.Fatalf("depth %q", got)
	}
}

func TestTracing_ConsumerLinkedToProducer(t *testing.T) {
	h := newHarness(t)
	// Produce a traceparent from a real span.
	pctx, producer := h.tel.tp.Tracer("test").Start(context.Background(), "producer")
	producer.End()
	tp := "00-" + producer.SpanContext().TraceID().String() + "-" + producer.SpanContext().SpanID().String() + "-01"
	_ = pctx

	if err := h.deliver(context.Background(), "c1", mediator.Envelope{TraceParent: tp}); err != nil {
		t.Fatal(err)
	}
	s := spanNamed(t, h.tel, "mediator.consumer thingStored")
	if s.SpanKind != trace.SpanKindConsumer {
		t.Fatalf("kind %v", s.SpanKind)
	}
	if got, _ := spanAttr(s, behavior.AttrConsumerGroup); got != consumerGroup {
		t.Fatalf("group %q", got)
	}
	if len(s.Links) != 1 || s.Links[0].SpanContext.SpanID() != producer.SpanContext().SpanID() {
		t.Fatalf("links %+v", s.Links)
	}

	// No traceparent, or a malformed one: no link.
	for _, bad := range []string{"", "garbage"} {
		h.tel.spans.Reset()
		if err := h.deliver(context.Background(), "c2", mediator.Envelope{TraceParent: bad}); err != nil {
			t.Fatal(err)
		}
		if s := spanNamed(t, h.tel, "mediator.consumer thingStored"); len(s.Links) != 0 {
			t.Fatalf("traceparent %q: links %+v", bad, s.Links)
		}
	}
}

func TestTracing_Notification(t *testing.T) {
	h := newHarness(t)
	if err := mediator.Publish(context.Background(), h.m, thingEvent{ID: "e"}); err != nil {
		t.Fatal(err)
	}
	s := spanNamed(t, h.tel, "mediator.notification thingEvent")
	if got, _ := spanAttr(s, behavior.AttrRequestKind); got != "notification" {
		t.Fatalf("kind attr %q", got)
	}
}

func TestTracing_Stream(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	for range mediator.Stream(ctx, h.m, numStream{N: 3}) {
	}
	s := spanNamed(t, h.tel, "mediator.stream numStream")
	if got, _ := spanAttr(s, behavior.AttrStreamItems); got != "3" {
		t.Fatalf("items %q", got)
	}
	if s.Status.Code != codes.Unset {
		t.Fatalf("status %v", s.Status)
	}

	// Terminal error after two items.
	h.tel.spans.Reset()
	h.hooks.stream = func(context.Context, numStream) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			_ = yield(1, nil) && yield(2, nil) && yield(0, mediator.E(mediator.CodeUnavailable, "cut"))
		}
	}
	for range mediator.Stream(ctx, h.m, numStream{N: 3}) {
	}
	s = spanNamed(t, h.tel, "mediator.stream numStream")
	if got, _ := spanAttr(s, behavior.AttrStreamItems); got != "2" || s.Status.Code != codes.Error {
		t.Fatalf("items %q status %v", got, s.Status)
	}

	// Consumer stops early: span ends with the items seen so far.
	h.tel.spans.Reset()
	h.hooks.stream = defaultHooks().stream
	for v := range mediator.Stream(ctx, h.m, numStream{N: 5}) {
		if v == 1 {
			break
		}
	}
	s = spanNamed(t, h.tel, "mediator.stream numStream")
	if got, _ := spanAttr(s, behavior.AttrStreamItems); got != "2" {
		t.Fatalf("items %q", got)
	}

	// Iterator panic: status Error "panic".
	h.tel.spans.Reset()
	h.hooks.stream = func(context.Context, numStream) iter.Seq2[int, error] {
		return func(func(int, error) bool) { panic("it") }
	}
	for range mediator.Stream(ctx, h.m, numStream{N: 1}) {
	}
	s = spanNamed(t, h.tel, "mediator.stream numStream")
	if s.Status.Code != codes.Error || s.Status.Description != behavior.OutcomePanic {
		t.Fatalf("status %+v", s.Status)
	}
}

// TestTracing_NoopTracer: with the global (no-op) tracer nothing is
// recorded and the behavior still works, including the consumer link path.
func TestTracing_NoopTracer(t *testing.T) {
	b := behavior.NewTracing(behavior.Config{})
	info := &mediator.RequestInfo{Name: "x", Kind: mediator.KindConsumer, Group: "g"}
	ctx := mediator.WithEnvelope(context.Background(), mediator.Envelope{TraceParent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"})
	res, err := b.Handle(ctx, nil, info, func(ctx context.Context, _ any) (any, error) {
		if trace.SpanFromContext(ctx).IsRecording() {
			t.Fatal("no-op span must not record")
		}
		return "ok", nil
	})
	if err != nil || res != "ok" {
		t.Fatal(res, err)
	}
	if _, err := b.Handle(ctx, nil, info, func(context.Context, any) (any, error) {
		return nil, mediator.E(mediator.CodeInternal, "x")
	}); err == nil {
		t.Fatal("error must pass through")
	}
	func() {
		defer func() {
			if v := recover(); v != "p" {
				t.Fatalf("the panic must propagate to Recovery: %v", v)
			}
		}()
		_, _ = b.Handle(ctx, nil, info, func(context.Context, any) (any, error) { panic("p") })
	}()
}
