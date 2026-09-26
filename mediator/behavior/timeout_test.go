package behavior_test

import (
	"context"
	"errors"
	"iter"
	"testing"
	"testing/synctest"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
)

// timeoutMediator registers the fixture handlers behind Timeout alone.
func timeoutMediator(t *testing.T, h *hooks, cfg behavior.Config) *mediator.Mediator {
	t.Helper()
	m := mediator.New()
	register(t, m, h)
	must(t, mediator.Use(m, behavior.NewTimeout(cfg), mediator.Requests(), mediator.Consumers()))
	must(t, m.Build())
	return m
}

func deadlineIn(ctx context.Context) time.Duration {
	d, ok := ctx.Deadline()
	if !ok {
		return -1
	}
	return time.Until(d)
}

func TestTimeout_Deadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := defaultHooks()
		var seen time.Duration
		h.plain = func(ctx context.Context, c plainCmd) (cmdResult, error) {
			seen = deadlineIn(ctx)
			return cmdResult{}, nil
		}
		h.rich = func(ctx context.Context, c richCmd) (cmdResult, error) {
			seen = deadlineIn(ctx)
			return cmdResult{}, nil
		}
		m := timeoutMediator(t, h, behavior.Config{})
		ctx := context.Background()

		if _, err := mediator.Send(ctx, m, plainCmd{ID: "a"}); err != nil || seen != behavior.DefaultTimeout {
			t.Fatalf("default: err=%v deadline in %s", err, seen)
		}
		if _, err := mediator.Send(ctx, m, richCmd{ID: "a"}); err != nil || seen != 2*time.Second {
			t.Fatalf("trait: err=%v deadline in %s", err, seen)
		}
		early, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		if _, err := mediator.Send(early, m, richCmd{ID: "a"}); err != nil || seen != time.Second {
			t.Fatalf("earlier incoming deadline: err=%v deadline in %s", err, seen)
		}

		m2 := timeoutMediator(t, h, behavior.Config{DefaultTimeout: 7 * time.Second})
		if _, err := mediator.Send(ctx, m2, plainCmd{ID: "a"}); err != nil || seen != 7*time.Second {
			t.Fatalf("configured default: err=%v deadline in %s", err, seen)
		}
	})
}

func TestTimeout_LateResultDiscarded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := defaultHooks()
		var ret error
		h.plain = func(ctx context.Context, c plainCmd) (cmdResult, error) {
			time.Sleep(behavior.DefaultTimeout + time.Second)
			return cmdResult{ID: "late"}, ret
		}
		m := timeoutMediator(t, h, behavior.Config{})
		ctx := context.Background()

		res, err := mediator.Send(ctx, m, plainCmd{ID: "a"})
		codeIs(t, err, mediator.CodeTimeout)
		if res.ID != "" || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("late result must be discarded: %+v %v", res, err)
		}

		ret = mediator.E(mediator.CodeConflict, "late conflict")
		_, err = mediator.Send(ctx, m, plainCmd{ID: "a"})
		codeIs(t, err, mediator.CodeTimeout)
		if !errors.Is(err, ret) {
			t.Fatalf("the late error must stay in the chain: %v", err)
		}

		ambiguous := mediator.MarkAmbiguous(mediator.E(mediator.CodeUnavailable, "commit outcome unknown"))
		ret = ambiguous
		_, err = mediator.Send(ctx, m, plainCmd{ID: "a"})
		if mediator.CodeOf(err) != mediator.CodeTimeout || !mediator.IsAmbiguous(err) {
			t.Fatalf("ambiguity must survive the wrap: %v", err)
		}

		ret = mediator.Wrap(mediator.CodeTimeout, "own", context.DeadlineExceeded)
		_, err = mediator.Send(ctx, m, plainCmd{ID: "a"})
		if !errors.Is(err, ret) {
			t.Fatalf("a timeout error is returned unchanged: %v", err)
		}
	})
}

func TestTimeout_CancelledCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := defaultHooks()
		h.plain = func(ctx context.Context, c plainCmd) (cmdResult, error) {
			<-ctx.Done()
			return cmdResult{ID: "x"}, nil
		}
		m := timeoutMediator(t, h, behavior.Config{})
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Second)
			cancel()
		}()
		_, err := mediator.Send(ctx, m, plainCmd{ID: "a"})
		codeIs(t, err, mediator.CodeTimeout)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("want cancellation cause: %v", err)
		}
		var e *mediator.Error
		if !errors.As(err, &e) || e.Message != "request canceled" {
			t.Fatalf("message: %v", err)
		}
	})
}

func TestTimeout_Stream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := defaultHooks()
		// Two items, then the sequence outlives the deadline.
		h.stream = func(ctx context.Context, s numStream) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) {
				if !yield(1, nil) || !yield(2, nil) {
					return
				}
				time.Sleep(behavior.DefaultTimeout + time.Second)
				if s.N == 99 {
					yield(0, mediator.E(mediator.CodeUnavailable, "late failure"))
					return
				}
				yield(3, nil)
			}
		}
		m := timeoutMediator(t, h, behavior.Config{})
		ctx := context.Background()

		var got []int
		var last error
		for v, err := range mediator.Stream(ctx, m, numStream{N: 3}) {
			if err != nil {
				last = err
				continue
			}
			got = append(got, v)
		}
		if len(got) != 2 {
			t.Fatalf("items past the deadline must not be delivered: %v", got)
		}
		codeIs(t, last, mediator.CodeTimeout)

		got, last = nil, nil
		for v, err := range mediator.Stream(ctx, m, numStream{N: 99}) {
			if err != nil {
				last = err
				continue
			}
			got = append(got, v)
		}
		codeIs(t, last, mediator.CodeTimeout)
		if len(got) != 2 || mediator.CodeOf(errors.Unwrap(last)) != mediator.CodeUnavailable {
			t.Fatalf("late error must be wrapped as a timeout with the cause kept: %v %v", got, last)
		}

		// An error before the deadline passes through; early stop cancels.
		h.stream = func(ctx context.Context, s numStream) iter.Seq2[int, error] {
			return func(yield func(int, error) bool) {
				if !yield(1, nil) {
					return
				}
				yield(0, mediator.E(mediator.CodeConflict, "early"))
			}
		}
		for _, err := range mediator.Stream(ctx, m, numStream{N: 1}) {
			last = err
		}
		codeIs(t, last, mediator.CodeConflict)

		var streamCtx context.Context
		h.stream = func(ctx context.Context, s numStream) iter.Seq2[int, error] {
			streamCtx = ctx
			return defaultHooks().stream(ctx, s)
		}
		for v := range mediator.Stream(ctx, m, numStream{N: 5}) {
			if v == 0 {
				break
			}
		}
		if streamCtx.Err() == nil {
			t.Fatal("the stream context must be canceled when the consumer stops")
		}
	})
}

// TestTimeout_Consumer: the consumer path uses HandlerTimeout when the
// registration sets it, read at Build through OnBuild; otherwise the
// default. A trait timeout of zero falls back too.
func TestTimeout_Consumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var seen time.Duration
		m := mediator.New()
		must(t, mediator.ConsumeFunc(m, "fast", func(ctx context.Context, e thingStored) error { seen = deadlineIn(ctx); return nil }, mediator.HandlerTimeout(5*time.Second)))
		must(t, mediator.ConsumeFunc(m, "slow", func(ctx context.Context, e thingStored) error { seen = deadlineIn(ctx); return nil }))
		must(t, mediator.HandleFunc(m, func(ctx context.Context, c zeroTimeoutCmd) (mediator.Void, error) {
			seen = deadlineIn(ctx)
			return mediator.Void{}, nil
		}))
		must(t, behavior.UseStandard(m, behavior.Config{}))
		must(t, m.Build())
		env := mediator.Envelope{Type: "thingStored", ID: mediator.NewID(time.Now()), StreamKey: "k"}
		if err := m.Deliver(context.Background(), "fast", env, []byte(`{"id":"k"}`)); err != nil || seen != 5*time.Second {
			t.Fatalf("fast: err=%v deadline in %s", err, seen)
		}
		if err := m.Deliver(context.Background(), "slow", env, []byte(`{"id":"k"}`)); err != nil || seen != behavior.DefaultTimeout {
			t.Fatalf("slow: err=%v deadline in %s", err, seen)
		}
		if _, err := mediator.Send(context.Background(), m, zeroTimeoutCmd{}); err != nil || seen != behavior.DefaultTimeout {
			t.Fatalf("zero trait: err=%v deadline in %s", err, seen)
		}
	})
}

type zeroTimeoutCmd struct {
	mediator.Command[mediator.Void]
}

func (zeroTimeoutCmd) Timeout() time.Duration { return 0 }

// TestTimeout_OnBuildMerges: a Timeout shared by two mediators keeps the
// registrations of both.
func TestTimeout_OnBuildMerges(t *testing.T) {
	b := behavior.NewTimeout(behavior.Config{})
	hook := b.(behavior.BuildHook)
	build := func(group string, d time.Duration) *mediator.Mediator {
		m := mediator.New()
		must(t, mediator.ConsumeFunc(m, group, func(context.Context, thingStored) error { return nil }, mediator.HandlerTimeout(d)))
		must(t, mediator.Use(m, b, mediator.Consumers()))
		must(t, m.OnBuild(hook.OnBuild))
		must(t, m.Build())
		return m
	}
	m1 := build("g1", time.Second)
	m2 := build("g2", 2*time.Second)
	for _, m := range []*mediator.Mediator{m1, m2} {
		for _, c := range m.ConsumerRegistrations() {
			var seen time.Duration
			res, err := b.Handle(context.Background(), thingStored{}, c.Info, func(ctx context.Context, _ any) (any, error) {
				seen = deadlineIn(ctx)
				return nil, nil
			})
			if err != nil || res != nil || seen > c.HandlerTimeout || seen < c.HandlerTimeout-time.Second/10 {
				t.Fatalf("%s: err=%v seen=%s want %s", c.Group, err, seen, c.HandlerTimeout)
			}
		}
	}
	if b.Name() != behavior.Timeout {
		t.Fatal(b.Name())
	}
}
