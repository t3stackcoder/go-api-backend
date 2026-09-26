package mediator_test

// This file compiles every registration and send shape spec.md documents in
// 4.2, 4.4, 4.5, and 4.7. It exists so that a regression in generic type
// inference fails the build rather than a test.

import (
	"context"
	"iter"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type infCreate struct {
	mediator.Command[infResult]
	ID string `json:"id"`
}

type infResult struct {
	ID string `json:"id"`
}

type infGet struct {
	mediator.Query[infView]
	ID string `json:"id"`
}

type infView struct {
	ID string `json:"id"`
}

type infVoid struct {
	mediator.Command[mediator.Void]
}

// infRemote has no local handler; Declare registers it for remote dispatch.
type infRemote struct{ mediator.Query[infView] }

type infTail struct {
	mediator.StreamQuery[infView]
	N int `json:"n"`
}

type infEvent struct {
	mediator.Event
	ID string `json:"id"`
}

type infDurable struct {
	mediator.Event
	ID string `json:"id"`
}

func (e infDurable) StreamKey() string { return e.ID }

// Struct handlers: type arguments are inferred from the method set.
type infCreateHandler struct{}

func (infCreateHandler) Handle(_ context.Context, c infCreate) (infResult, error) {
	return infResult{ID: c.ID}, nil
}

type infGetHandler struct{ views int }

func (h *infGetHandler) Handle(_ context.Context, q infGet) (infView, error) {
	h.views++
	return infView{ID: q.ID}, nil
}

type infTailHandler struct{}

func (infTailHandler) Handle(_ context.Context, q infTail) iter.Seq2[infView, error] {
	return func(yield func(infView, error) bool) {
		for i := 0; i < q.N; i++ {
			if !yield(infView{ID: "v"}, nil) {
				return
			}
		}
	}
}

type infEventHandler struct{}

func (infEventHandler) Handle(context.Context, infEvent) error { return nil }

type infConsumer struct{}

func (*infConsumer) Handle(context.Context, infDurable) error { return nil }

type infTyped struct{}

func (infTyped) Handle(ctx context.Context, c infCreate, next func(context.Context, infCreate) (infResult, error)) (infResult, error) {
	return next(ctx, c)
}

type infPre struct{}

func (infPre) Process(context.Context, infCreate) error { return nil }

type infPost struct{}

func (infPost) Process(context.Context, infCreate, infResult) error { return nil }

type infOnError struct{}

func (infOnError) Handle(_ context.Context, _ infCreate, err error) (infResult, bool, error) {
	return infResult{}, false, err
}

func TestInference_Compiles(t *testing.T) {
	m := mediator.New()
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}

	// 4.2 Handle with a struct handler (value and pointer receivers), inferring
	// both type arguments from the method set.
	must(mediator.Handle(m, infCreateHandler{}))
	getH := &infGetHandler{}
	must(mediator.Handle(m, getH))
	// 4.2 HandleFunc with a literal infers both type arguments.
	must(mediator.HandleFunc(m, func(_ context.Context, c infVoid) (mediator.Void, error) { return mediator.Void{}, nil }))
	// 4.2 explicit instantiation and HandlerFunc adapter.
	m2 := mediator.New()
	must(mediator.Handle[infCreate, infResult](m2, infCreateHandler{}))
	must(mediator.Handle(m2, mediator.HandlerFunc[infGet, infView](getH.Handle)))
	mediator.MustHandle(m2, mediator.HandlerFunc[infVoid, mediator.Void](func(context.Context, infVoid) (mediator.Void, error) {
		return mediator.Void{}, nil
	}))
	must(mediator.Declare[infRemote, infView](m2))

	// 4.4 On with a struct handler, OnFunc with a literal, Consume and
	// ConsumeFunc for durable events, RegisterEvent.
	must(mediator.On(m, infEventHandler{}))
	must(mediator.OnFunc(m, func(context.Context, infEvent) error { return nil }))
	must(mediator.On(m, mediator.NotificationHandlerFunc[infDurable](func(context.Context, infDurable) error { return nil })))
	must(mediator.Consume(m, "projection", &infConsumer{}))
	must(mediator.ConsumeFunc(m, "audit", func(context.Context, infDurable) error { return nil }, mediator.HandlerTimeout(0), mediator.StrictOrder(false), mediator.MaxAttempts(0)))
	must(mediator.RegisterEvent[infEvent](m2))

	// 4.5 HandleStream with a struct handler and HandleStreamFunc with a literal.
	must(mediator.HandleStream(m, infTailHandler{}))
	must(mediator.HandleStreamFunc(m2, func(_ context.Context, q infTail) iter.Seq2[infView, error] { return nil }))
	_ = mediator.StreamHandlerFunc[infTail, infView](infTailHandler{}.Handle)

	// 4.7 typed extension points with struct implementations and func adapters.
	must(mediator.UseFor(m, infTyped{}))
	must(mediator.UseFor(m, mediator.TypedBehaviorFunc[infCreate, infResult](infTyped{}.Handle)))
	must(mediator.Pre(m, infPre{}))
	must(mediator.Pre(m, mediator.PreProcessorFunc[infCreate, infResult](infPre{}.Process)))
	must(mediator.Post(m, infPost{}))
	must(mediator.Post(m, mediator.PostProcessorFunc[infCreate, infResult](infPost{}.Process)))
	must(mediator.OnError(m, infOnError{}))
	must(mediator.OnError(m, mediator.ErrorHandlerFunc[infCreate, infResult](infOnError{}.Handle)))

	// 4.6 untyped behavior with options.
	must(mediator.Use(m, mediator.BehaviorFunc{N: "b", F: func(ctx context.Context, req any, _ *mediator.RequestInfo, next mediator.Next) (any, error) {
		return next(ctx, req)
	}}, mediator.Commands(), mediator.Queries()))
	must(m.Build())
	must(m2.Build())

	// 4.3 Send with a value and with a pointer; the result is typed.
	var res infResult
	res, err := mediator.Send(ctx, m, infCreate{ID: "a"})
	must(err)
	if res.ID != "a" {
		t.Fatal(res)
	}
	res, err = mediator.Send(ctx, m, &infCreate{ID: "b"})
	must(err)
	if res.ID != "b" {
		t.Fatal(res)
	}
	var view infView
	view, err = mediator.Send(ctx, m, infGet{ID: "g"})
	must(err)
	var void mediator.Void
	void, err = mediator.Send(ctx, m, infVoid{})
	must(err)
	_, _ = view, void

	// 4.5 Stream with a value and a pointer yields typed items.
	seq := mediator.Stream(ctx, m, infTail{N: 2})
	n := 0
	for v, err := range seq {
		must(err)
		n++
		_ = v.ID
	}
	for v, err := range mediator.Stream(ctx, m, &infTail{N: 1}) {
		must(err)
		n++
		_ = v.ID
	}
	if n != 3 {
		t.Fatal(n)
	}

	// 4.4 Publish with a value, a pointer, and a mixed batch.
	must(mediator.Publish(ctx, m, infEvent{ID: "e"}))
	must(mediator.Publish(ctx, m, &infEvent{ID: "e"}, mediator.Strategy(mediator.ContinueOnError)))
	must(mediator.PublishAll(ctx, m, []mediator.Notification{infEvent{}, &infEvent{}}))
	if getH.views != 1 {
		t.Fatal("struct handler with pointer receiver was not used")
	}
}
