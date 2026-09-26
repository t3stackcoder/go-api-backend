package behavior_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/retry"
)

// retryCmd retries on transient errors; keyedRetryCmd also carries a key.
type retryCmd struct {
	mediator.Command[cmdResult]
	ID string `json:"id"`
}

func (retryCmd) RetryPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 3, BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
}

type keyedRetryCmd struct {
	mediator.Command[cmdResult]
	Key string `json:"key"`
}

func (keyedRetryCmd) RetryPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 2, BaseDelay: time.Millisecond}
}
func (c keyedRetryCmd) IdempotencyKey() string { return c.Key }

// customRetryCmd retries on its own predicate only.
type customRetryCmd struct {
	mediator.Command[cmdResult]
}

var errRetryMe = errors.New("retry me")

func (customRetryCmd) RetryPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 4, RetryIf: func(err error) bool { return errors.Is(err, errRetryMe) }}
}

type retryQuery struct {
	mediator.Query[cmdResult]
}

func (retryQuery) RetryPolicy() retry.Policy { return retry.Policy{MaxAttempts: 2} }

type retryNoUow struct {
	mediator.Command[cmdResult]
}

func (retryNoUow) RetryPolicy() retry.Policy { return retry.Policy{MaxAttempts: 2} }
func (retryNoUow) NoUnitOfWork()             {}

// retryMediator wires the fixture commands behind Retry alone with the
// given clock, and counts attempts.
func retryMediator(t *testing.T, clock mediator.Clock, fail func(attempt int) error) (*mediator.Mediator, *int) {
	t.Helper()
	attempts := new(int)
	handler := func(context.Context) (cmdResult, error) {
		*attempts++
		if err := fail(*attempts); err != nil {
			return cmdResult{}, err
		}
		return cmdResult{N: *attempts}, nil
	}
	m := mediator.New()
	must(t, mediator.HandleFunc(m, func(ctx context.Context, _ retryCmd) (cmdResult, error) { return handler(ctx) }))
	must(t, mediator.HandleFunc(m, func(ctx context.Context, _ keyedRetryCmd) (cmdResult, error) { return handler(ctx) }))
	must(t, mediator.HandleFunc(m, func(ctx context.Context, _ customRetryCmd) (cmdResult, error) { return handler(ctx) }))
	must(t, mediator.Use(m, behavior.NewRetry(behavior.Config{Clock: clock}), mediator.Commands(), mediator.Where(func(i *mediator.RequestInfo) bool { return i.Traits.RetryPolicy })))
	must(t, m.Build())
	return m, attempts
}

func TestRetry_Transient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, attempts := retryMediator(t, nil, func(n int) error {
			if n < 3 {
				return transientErr{"try again"}
			}
			return nil
		})
		start := time.Now()
		res, err := mediator.Send(context.Background(), m, retryCmd{ID: "a"})
		if err != nil || res.N != 3 || *attempts != 3 {
			t.Fatalf("res %+v err %v attempts %d", res, err, *attempts)
		}
		if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
			t.Fatalf("delays are bounded by the backoff: %s", elapsed)
		}
	})
}

func TestRetry_Stops(t *testing.T) {
	permanent := mediator.E(mediator.CodeConflict, "no")
	transient := transientErr{"again"}
	ambiguous := mediator.MarkAmbiguous(mediator.Wrap(mediator.CodeUnavailable, "commit outcome unknown", transient))
	cases := []struct {
		name     string
		req      any
		ctx      func(context.Context) context.Context
		err      error
		attempts int
	}{
		{"non-transient", retryCmd{}, nil, permanent, 1},
		{"exhausted", retryCmd{}, nil, transient, 3},
		{"ambiguous without key", retryCmd{}, nil, ambiguous, 1},
		{"ambiguous with trait key", keyedRetryCmd{Key: "k"}, nil, ambiguous, 2},
		{"ambiguous with empty trait key", keyedRetryCmd{}, nil, ambiguous, 1},
		{"ambiguous with context key", retryCmd{}, func(ctx context.Context) context.Context { return mediator.WithIdempotencyKey(ctx, "k") }, ambiguous, 3},
		{"custom predicate rejects transient", customRetryCmd{}, nil, transient, 1},
		{"custom predicate accepts", customRetryCmd{}, nil, errRetryMe, 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m, attempts := retryMediator(t, nil, func(int) error { return c.err })
				ctx := context.Background()
				if c.ctx != nil {
					ctx = c.ctx(ctx)
				}
				_, err := m.SendAny(ctx, c.req)
				if !errors.Is(err, c.err) || *attempts != c.attempts {
					t.Fatalf("err %v attempts %d, want %d", err, *attempts, c.attempts)
				}
			})
		})
	}
}

// farClock reports a time far ahead of the bubble, so every delay lands
// past any deadline.
type farClock struct{}

func (farClock) Now() time.Time { return time.Now().Add(time.Hour) }

func TestRetry_Deadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, attempts := retryMediator(t, farClock{}, func(int) error { return transientErr{"again"} })
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := mediator.Send(ctx, m, retryCmd{ID: "a"}); err == nil || *attempts != 1 {
			t.Fatalf("a retry that cannot finish before the deadline is not attempted: %v %d", err, *attempts)
		}

		// Without a deadline the far clock does not matter.
		*attempts = 0
		if _, err := mediator.Send(context.Background(), m, retryCmd{ID: "a"}); err == nil || *attempts != 3 {
			t.Fatalf("attempts %d", *attempts)
		}
	})
}

func TestRetry_Cancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Canceled while sleeping between attempts.
		ctx, cancel := context.WithCancel(context.Background())
		m, attempts := retryMediator(t, nil, func(int) error { return transientErr{"again"} })
		go func() {
			time.Sleep(time.Millisecond)
			cancel()
		}()
		_, err := mediator.Send(ctx, m, retryCmd{ID: "a"})
		if !errors.Is(err, transientErr{"again"}) || *attempts != 1 {
			t.Fatalf("err %v attempts %d", err, *attempts)
		}

		// Canceled before the handler returned: no retry, no sleep.
		ctx, cancel = context.WithCancel(context.Background())
		m, attempts = retryMediator(t, nil, func(int) error { cancel(); return transientErr{"again"} })
		if _, err := mediator.Send(ctx, m, retryCmd{ID: "a"}); err == nil || *attempts != 1 {
			t.Fatalf("attempts %d", *attempts)
		}
	})
}

// TestRetry_FreshUnitOfWork: through the standard set each attempt runs in
// its own transaction.
func TestRetry_FreshUnitOfWork(t *testing.T) {
	// The fake clock's After never fires on its own; sleep on real time.
	h := newHarness(t, withConfig(func(cfg *behavior.Config) { cfg.Clock = bubbleClock{} }), withPreBuild(func(m *mediator.Mediator) error {
		n := 0
		return mediator.HandleFunc(m, func(ctx context.Context, c retryCmd) (cmdResult, error) {
			n++
			if n == 1 {
				return cmdResult{}, mediator.E(mediator.CodeUnavailable, "first")
			}
			return cmdResult{N: n}, nil
		})
	}))
	res, err := mediator.Send(context.Background(), h.m, retryCmd{ID: "a"})
	if err != nil || res.N != 2 {
		t.Fatalf("%+v %v", res, err)
	}
	if h.store.Begun() != 2 || h.store.RolledBack() != 1 || h.store.Committed() != 1 {
		t.Fatalf("begun %d rolled back %d committed %d", h.store.Begun(), h.store.RolledBack(), h.store.Committed())
	}
	if recs := h.logs.find("retrying after failure"); len(recs) != 1 {
		t.Fatalf("retry log %v", recs)
	}
}

func TestRetry_Prepare(t *testing.T) {
	b := behavior.NewRetry(behavior.Config{}).(mediator.Preparer)
	info := func(kind mediator.Kind, typ reflect.Type, traits mediator.Traits) *mediator.RequestInfo {
		traits.RetryPolicy = true
		return &mediator.RequestInfo{Kind: kind, RequestType: typ, Traits: traits}
	}
	cases := []struct {
		name string
		info *mediator.RequestInfo
		want string
	}{
		{"query", info(mediator.KindQuery, reflect.TypeFor[retryQuery](), mediator.Traits{}), "commands only"},
		{"no unit of work without key", info(mediator.KindCommand, reflect.TypeFor[retryNoUow](), mediator.Traits{NoUnitOfWork: true}), "remove NoUnitOfWork or RetryPolicy"},
		{"no unit of work with key", info(mediator.KindCommand, reflect.TypeFor[retryNoUow](), mediator.Traits{NoUnitOfWork: true, IdempotencyKey: true}), "remove NoUnitOfWork or RetryPolicy"},
		{"valid", info(mediator.KindCommand, reflect.TypeFor[retryCmd](), mediator.Traits{}), ""},
		{"without trait", &mediator.RequestInfo{Kind: mediator.KindCommand}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := b.Prepare([]*mediator.RequestInfo{c.info})
			if c.want == "" && err != nil {
				t.Fatalf("unexpected %v", err)
			}
			if c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)) {
				t.Fatalf("want %q, got %v", c.want, err)
			}
		})
	}
}

func TestRetry_PolicyValidation(t *testing.T) {
	for _, c := range []struct {
		name string
		typ  reflect.Type
		want string
	}{
		{"zero attempts", reflect.TypeFor[zeroAttemptsCmd](), "MaxAttempts"},
		{"negative delay", reflect.TypeFor[negativeDelayCmd](), "negative"},
		{"max below base", reflect.TypeFor[maxBelowBaseCmd](), "shorter than BaseDelay"},
	} {
		t.Run(c.name, func(t *testing.T) {
			b := behavior.NewRetry(behavior.Config{}).(mediator.Preparer)
			err := b.Prepare([]*mediator.RequestInfo{{Kind: mediator.KindCommand, RequestType: c.typ, Traits: mediator.Traits{RetryPolicy: true}}})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want %q, got %v", c.want, err)
			}
		})
	}
	m := mediator.New()
	must(t, mediator.HandleFunc(m, func(context.Context, zeroAttemptsCmd) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(t, behavior.UseStandard(m, behavior.Config{}))
	if err := m.Build(); err == nil || !strings.Contains(err.Error(), "MaxAttempts") {
		t.Fatalf("Build = %v", err)
	}
}

// keyedRetryNoUow passes the core's Build check (RetryPolicy with
// NoUnitOfWork is allowed there when IdempotencyKey exists) and must still
// be rejected by the Retry behavior's Prepare through the standard set.
type keyedRetryNoUow struct {
	mediator.Command[mediator.Void]
	Key string `json:"key"`
}

func (keyedRetryNoUow) RetryPolicy() retry.Policy { return retry.Policy{MaxAttempts: 2} }
func (keyedRetryNoUow) NoUnitOfWork()             {}
func (c keyedRetryNoUow) IdempotencyKey() string  { return c.Key }

func TestRetry_RejectsNoUnitOfWorkAtBuild(t *testing.T) {
	m := mediator.New()
	must(t, mediator.HandleFunc(m, func(context.Context, keyedRetryNoUow) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(t, behavior.UseStandard(m, behavior.Config{}))
	err := m.Build()
	if err == nil || !strings.Contains(err.Error(), "keyedRetryNoUow has RetryPolicy() and NoUnitOfWork(): retry without a unit of work can repeat effects; remove NoUnitOfWork or RetryPolicy") {
		t.Fatalf("Build = %v", err)
	}
	if strings.Contains(err.Error(), "no IdempotencyKey") {
		t.Fatalf("the core rule must not fire for a keyed request: %v", err)
	}
}

type zeroAttemptsCmd struct {
	mediator.Command[mediator.Void]
}

func (zeroAttemptsCmd) RetryPolicy() retry.Policy { return retry.Policy{} }

type negativeDelayCmd struct {
	mediator.Command[mediator.Void]
}

func (negativeDelayCmd) RetryPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 2, BaseDelay: -1}
}

type maxBelowBaseCmd struct {
	mediator.Command[mediator.Void]
}

func (maxBelowBaseCmd) RetryPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: time.Millisecond}
}

func TestRetry_Direct(t *testing.T) {
	b := behavior.NewRetry(behavior.Config{})
	res, err := b.Handle(context.Background(), plainCmd{}, &mediator.RequestInfo{}, func(context.Context, any) (any, error) { return 1, nil })
	if err != nil || res != 1 || b.Name() != behavior.Retry {
		t.Fatal(res, err)
	}
}
