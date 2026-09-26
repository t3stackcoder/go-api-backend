package behavior_test

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// boom is a behavior that panics on every path.
func boom() mediator.Behavior {
	return mediator.BehaviorFunc{N: "boom", F: func(context.Context, any, *mediator.RequestInfo, mediator.Next) (any, error) {
		panic("boom")
	}}
}

// assertPanicResult checks the contract of G3 for one path: the error is
// Internal, carries the PanicError, and, when Recovery was outside the
// panic, an error record with the stack was logged.
func assertPanicResult(t *testing.T, h *harness, err error, logged bool) {
	t.Helper()
	codeIs(t, err, mediator.CodeInternal)
	if !isPanicError(err) {
		t.Fatalf("want *mediator.PanicError in chain, got %v", err)
	}
	recs := h.logs.find("panic recovered")
	if !logged {
		if len(recs) != 0 {
			t.Fatalf("Recovery did not run; want no log, got %v", recs)
		}
		return
	}
	if len(recs) == 0 {
		t.Fatal("want a 'panic recovered' record")
	}
	r := recs[0]
	stack, _ := r.Attrs["stack"].(string)
	if r.Level != slog.LevelError || !strings.Contains(stack, "goroutine") || r.Attrs["panic"] == nil {
		t.Fatalf("bad panic record: %+v", r)
	}
}

// TestRecovery_PanicInEveryPosition places a panicking behavior before
// every standard behavior, and after the last one, and exercises every path
// (command, query, stream, notification, consumer). No panic escapes.
func TestRecovery_PanicInEveryPosition(t *testing.T) {
	for i, name := range append(fullOrder, "") {
		pos := mediator.Before(name)
		label := "before " + name
		if name == "" {
			pos = mediator.After(behavior.CacheInvalidation)
			label = "innermost"
		}
		t.Run(label, func(t *testing.T) {
			h := newHarness(t, withPreBuild(func(m *mediator.Mediator) error {
				return mediator.Use(m, boom(), pos, mediator.Everywhere())
			}))
			logged := i > 0 // before Recovery, only the core's guard sees the panic
			ctx := admin(context.Background())

			h.logs.reset()
			_, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Tags: []string{"t"}})
			assertPanicResult(t, h, err, logged)

			h.logs.reset()
			_, err = mediator.Send(ctx, h.m, cachedQuery{ID: "q"})
			assertPanicResult(t, h, err, logged)

			h.logs.reset()
			var last error
			for _, err := range mediator.Stream(ctx, h.m, numStream{N: 3}) {
				last = err
			}
			assertPanicResult(t, h, last, logged)

			h.logs.reset()
			assertPanicResult(t, h, mediator.Publish(ctx, h.m, thingEvent{ID: "e"}), logged)

			h.logs.reset()
			assertPanicResult(t, h, h.deliver(ctx, "c", mediator.Envelope{}), logged)

			// Metrics counted the panic when it ran outside the panic.
			if i > 3 {
				rm := h.tel.collect(t)
				if !hasPoint(rm, "mediator.request.duration", map[string]string{"name": "richCmd", "outcome": behavior.OutcomePanic}) {
					t.Fatal("metrics did not record outcome=panic")
				}
			}
		})
	}
}

// TestRecovery_PanicInHandlers covers panics raised by the handlers
// themselves: command, query, stream iterator (before and after the first
// item), notification handler, consumer handler, and a post-commit hook.
func TestRecovery_PanicInHandlers(t *testing.T) {
	h := newHarness(t)
	ctx := admin(context.Background())
	h.hooks.plain = func(context.Context, plainCmd) (cmdResult, error) { panic("handler") }
	h.hooks.cached = func(context.Context, cachedQuery) (queryResult, error) { panic(errors.New("query")) }
	h.hooks.event = func(context.Context, thingEvent) error { panic("event") }
	h.hooks.consume = func(context.Context, thingStored) error { panic("consumer") }

	h.logs.reset()
	_, err := mediator.Send(ctx, h.m, plainCmd{ID: "a"})
	assertPanicResult(t, h, err, true)
	if h.store.RolledBack() != 1 {
		t.Fatalf("the unit of work must roll back on a panic: rolled back %d", h.store.RolledBack())
	}

	h.logs.reset()
	_, err = mediator.Send(ctx, h.m, cachedQuery{ID: "q"})
	assertPanicResult(t, h, err, true)
	if _, ok := h.backend.Entry("cachedQuery:"); ok {
		t.Fatal("a panicking query must not be cached")
	}

	// The core's fan-out converts a notification handler's panic itself
	// (callHandler), so Recovery sees an error, not a panic; the logging
	// behavior still reports outcome=panic through the PanicError.
	h.logs.reset()
	assertPanicResult(t, h, mediator.Publish(ctx, h.m, thingEvent{ID: "e"}), false)
	if ends := h.logs.find("request.end"); len(ends) != 1 || ends[0].Attrs["outcome"] != behavior.OutcomePanic {
		t.Fatalf("notification end record %v", ends)
	}

	h.logs.reset()
	assertPanicResult(t, h, h.deliver(ctx, "c", mediator.Envelope{}), true)

	// Iterator panics: before any item and after the first item.
	for _, after := range []int{0, 1} {
		h.hooks.stream = func(context.Context, numStream) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) {
				for i := 0; i < after; i++ {
					if !yield(i, nil) {
						return
					}
				}
				panic("iterator")
			}
		}
		h.logs.reset()
		var got []int
		var last error
		for v, err := range mediator.Stream(ctx, h.m, numStream{N: 3}) {
			if err != nil {
				last = err
				continue
			}
			got = append(got, v)
		}
		if len(got) != after {
			t.Fatalf("after=%d: got items %v", after, got)
		}
		assertPanicResult(t, h, last, true)
	}

	// A post-commit hook panic is logged by the unit of work and the
	// command still succeeds: the transaction is durable.
	h.hooks.plain = func(ctx context.Context, c plainCmd) (cmdResult, error) {
		pg.OnCommit(ctx, func(context.Context) { panic("hook") })
		return cmdResult{ID: c.ID}, nil
	}
	h.logs.reset()
	if _, err := mediator.Send(ctx, h.m, plainCmd{ID: "a"}); err != nil {
		t.Fatalf("hook panic must not fail the command: %v", err)
	}
	if len(h.logs.find("unit of work: on-commit hook panicked")) != 1 {
		t.Fatalf("hook panic not logged: %v", h.logs.all())
	}
}

// TestRecovery_StreamPanicAfterConsumerStopped: an iterator that panics
// after the consumer broke out of the loop has nobody to deliver to; the
// panic is logged and swallowed.
func TestRecovery_StreamPanicAfterConsumerStopped(t *testing.T) {
	h := newHarness(t)
	h.hooks.stream = func(context.Context, numStream) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			yield(1, nil)
			panic("late")
		}
	}
	h.logs.reset()
	for v := range mediator.Stream(context.Background(), h.m, numStream{N: 1}) {
		if v == 1 {
			break
		}
	}
	if len(h.logs.find("panic recovered")) != 1 {
		t.Fatalf("late panic must be logged once: %v", h.logs.all())
	}
}

// TestRecovery_StreamCallerPanicPropagates: a panic in the caller's loop
// body is not the framework's to recover.
func TestRecovery_StreamCallerPanicPropagates(t *testing.T) {
	h := newHarness(t)
	defer func() {
		if v := recover(); v != "body" {
			t.Fatalf("recovered %v, want the caller's panic", v)
		}
		if h.store.RolledBack() != 1 {
			t.Fatalf("the stream's unit of work must roll back: %d", h.store.RolledBack())
		}
	}()
	for range mediator.Stream(context.Background(), h.m, numStream{N: 3}) {
		panic("body")
	}
}

// TestRecovery_StreamBehaviorDirect covers HandleStream outside a mediator:
// a panic while the inner request-level behaviors run at Stream time, and a
// nil error pass-through of a terminal error.
func TestRecovery_StreamBehaviorDirect(t *testing.T) {
	sink := newLogSink(slog.LevelDebug)
	b := behavior.NewRecovery(behavior.Config{Logger: slog.New(sink)}).(mediator.StreamBehavior)
	info := &mediator.RequestInfo{Name: "s", Kind: mediator.KindStream}

	seq := b.HandleStream(context.Background(), nil, info, func(context.Context, any) iter.Seq2[any, error] {
		panic("eager")
	})
	var errs []error
	for _, err := range seq {
		errs = append(errs, err)
	}
	if len(errs) != 1 || !isPanicError(errs[0]) {
		t.Fatalf("eager panic: %v", errs)
	}

	terminal := errors.New("terminal")
	seq = b.HandleStream(context.Background(), nil, info, func(context.Context, any) iter.Seq2[any, error] {
		return func(yield func(any, error) bool) {
			if !yield(1, nil) {
				return
			}
			yield(nil, terminal)
		}
	})
	var got []any
	var last error
	for v, err := range seq {
		if err != nil {
			last = err
			continue
		}
		got = append(got, v)
	}
	if len(got) != 1 || !errors.Is(last, terminal) {
		t.Fatalf("got %v, last %v", got, last)
	}
	if b.Name() != behavior.Recovery {
		t.Fatal(b.Name())
	}
}
