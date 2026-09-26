package behavior_test

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
)

// keyedCmd derives its limiter key from the request.
type keyedCmd struct {
	mediator.Command[mediator.Void]
	Tenant string `json:"tenant"`
}

func (keyedCmd) RateLimit() ratelimit.Policy {
	return ratelimit.Policy{Rate: 1, Period: time.Second, Burst: 1, Key: func(_ context.Context, req any) string {
		return req.(keyedCmd).Tenant
	}}
}

type badPolicyCmd struct {
	mediator.Command[mediator.Void]
}

func (badPolicyCmd) RateLimit() ratelimit.Policy { return ratelimit.Policy{Rate: 0} }

func rateLimitHarness(t *testing.T, opts ...harnessOption) *harness {
	t.Helper()
	opts = append(opts, withPreBuild(func(m *mediator.Mediator) error {
		return mediator.HandleFunc(m, func(context.Context, keyedCmd) (mediator.Void, error) { return mediator.Void{}, nil })
	}))
	return newHarness(t, opts...)
}

func TestRateLimit_Keys(t *testing.T) {
	h := rateLimitHarness(t)
	base := context.Background()
	cases := []struct {
		name string
		ctx  context.Context
		req  any
		key  string
	}{
		{"policy key", base, keyedCmd{Tenant: "acme"}, "acme"},
		{"policy key empty falls through", authz.WithPrincipal(base, authz.Principal{Subject: "s"}), keyedCmd{}, "s"},
		{"principal subject", admin(base), richCmd{ID: "a"}, "alice"},
		{"remote address", httpapi.WithRemoteAddr(admin(base), "10.0.0.1"), richCmd{ID: "a"}, "alice"},
		{"remote address without principal", httpapi.WithRemoteAddr(base, "10.0.0.1"), keyedCmd{}, "10.0.0.1"},
		{"global", base, keyedCmd{}, behavior.GlobalRateLimitKey},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := h.m.SendAny(c.ctx, c.req); err != nil {
				t.Fatal(err)
			}
			call := h.limiter.lastCall()
			if call.Key != c.key || !call.Policy.Valid() {
				t.Fatalf("call %+v, want key %q", call, c.key)
			}
		})
	}
	if call := h.limiter.lastCall(); call.Name != "keyedCmd" {
		t.Fatalf("name %q", call.Name)
	}
}

func TestRateLimit_Decisions(t *testing.T) {
	h := rateLimitHarness(t)
	ctx := admin(context.Background())
	h.limiter.set(ratelimit.Decision{Allowed: false, RetryAfter: 1500 * time.Millisecond}, nil)
	_, err := mediator.Send(ctx, h.m, richCmd{ID: "a"})
	codeIs(t, err, mediator.CodeRateLimited)
	var e *mediator.Error
	if !errors.As(err, &e) || e.Details["retry_after_ms"] != int64(1500) {
		t.Fatalf("details %v", err)
	}
	h.limiter.set(ratelimit.Decision{Allowed: true}, nil)
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	rm := h.tel.collect(t)
	if p := point(t, rm, "mediator.ratelimit.decisions", map[string]string{"name": "richCmd", "result": behavior.RateLimitLimited}); p.Int != 1 {
		t.Fatalf("limited = %d", p.Int)
	}
	if p := point(t, rm, "mediator.ratelimit.decisions", map[string]string{"name": "richCmd", "result": behavior.RateLimitAllowed}); p.Int != 1 {
		t.Fatalf("allowed = %d", p.Int)
	}
	if hasPoint(rm, "mediator.ratelimit.degraded", nil) {
		t.Fatal("degraded counted without an error")
	}
}

func TestRateLimit_FailOpen(t *testing.T) {
	h := rateLimitHarness(t)
	ctx := admin(context.Background())
	h.limiter.set(ratelimit.Decision{}, errors.New("redis down"))
	for i := 0; i < 3; i++ {
		if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a"}); err != nil {
			t.Fatalf("fail open: %v", err)
		}
	}
	rm := h.tel.collect(t)
	if p := point(t, rm, "mediator.ratelimit.degraded", map[string]string{"name": "richCmd"}); p.Int != 3 {
		t.Fatalf("degraded = %d", p.Int)
	}
	if p := point(t, rm, "mediator.ratelimit.decisions", map[string]string{"name": "richCmd", "result": behavior.RateLimitDegraded}); p.Int != 3 {
		t.Fatalf("degraded decisions = %d", p.Int)
	}
	warns := h.logs.find("rate limiter unavailable")
	if len(warns) != 1 || warns[0].Level != slog.LevelWarn || warns[0].Attrs["fail_closed"] != false {
		t.Fatalf("want one warning per name per minute, got %v", warns)
	}
	h.clock.Advance(time.Minute)
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	if len(h.logs.find("rate limiter unavailable")) != 2 {
		t.Fatal("the warning must be logged again after the window")
	}
}

func TestRateLimit_FailClosed(t *testing.T) {
	h := rateLimitHarness(t, withConfig(func(cfg *behavior.Config) { cfg.FailClosed = true }))
	h.limiter.set(ratelimit.Decision{}, errors.New("redis down"))
	_, err := mediator.Send(admin(context.Background()), h.m, richCmd{ID: "a"})
	codeIs(t, err, mediator.CodeUnavailable)
	if warns := h.logs.find("rate limiter unavailable"); len(warns) != 1 || warns[0].Attrs["fail_closed"] != true {
		t.Fatalf("warnings %v", warns)
	}
}

func TestRateLimit_Prepare(t *testing.T) {
	m := mediator.New()
	must(t, mediator.HandleFunc(m, func(context.Context, badPolicyCmd) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(t, behavior.UseStandard(m, behavior.Config{Limiter: newFakeLimiter()}))
	if err := m.Build(); err == nil || !strings.Contains(err.Error(), "invalid rate limit policy") {
		t.Fatalf("Build = %v", err)
	}

	b := behavior.NewRateLimit(behavior.Config{})
	if err := b.(mediator.Preparer).Prepare(nil); err == nil || !strings.Contains(err.Error(), "requires a Limiter") {
		t.Fatalf("Prepare without limiter = %v", err)
	}
	res, err := b.Handle(context.Background(), plainCmd{}, &mediator.RequestInfo{}, func(context.Context, any) (any, error) { return 1, nil })
	if err != nil || res != 1 || b.Name() != behavior.RateLimit {
		t.Fatal(res, err)
	}
}
