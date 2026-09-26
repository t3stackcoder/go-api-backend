//go:build integration

package redisx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
)

func TestLimiter_BurstThenSteadyRate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	l := NewLimiter(client, cfg)
	p := ratelimit.Policy{Rate: 10, Period: time.Second, Burst: 5}

	// Burst: five immediate operations pass with a shrinking remaining count.
	for i := 0; i < 5; i++ {
		d, err := l.Check(ctx, "CreateOrder", "u1", p)
		if err != nil || !d.Allowed || d.Remaining != 4-i || d.RetryAfter != 0 {
			t.Fatalf("burst %d: %+v %v", i, d, err)
		}
	}
	d, err := l.Check(ctx, "CreateOrder", "u1", p)
	if err != nil || d.Allowed || d.RetryAfter <= 0 || d.RetryAfter > 110*time.Millisecond {
		t.Fatalf("over burst: %+v %v", d, err)
	}
	if ttl := client.PTTL(ctx, cfg.Keys().RateLimit("CreateOrder", "u1")).Val(); ttl <= 0 || ttl > 5*time.Second {
		t.Fatalf("state ttl %s", ttl)
	}
	// Other keys and names are independent.
	if d, _ := l.Check(ctx, "CreateOrder", "u2", p); !d.Allowed {
		t.Fatal("other key limited")
	}
	if d, _ := l.Check(ctx, "Other", "u1", p); !d.Allowed {
		t.Fatal("other name limited")
	}
	// Waiting RetryAfter admits exactly one more.
	time.Sleep(d.RetryAfter + 5*time.Millisecond)
	if d, _ := l.Check(ctx, "CreateOrder", "u1", p); !d.Allowed {
		t.Fatalf("after retry-after: %+v", d)
	}
	if d, _ := l.Check(ctx, "CreateOrder", "u1", p); d.Allowed {
		t.Fatalf("second after retry-after: %+v", d)
	}

	// Steady state: against Redis TIME, about Rate operations per Period.
	if err := l.Reset(ctx, "CreateOrder", "u1"); err != nil {
		t.Fatal(err)
	}
	steady := ratelimit.Policy{Rate: 20, Period: time.Second, Burst: 1}
	allowed := 0
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		d, err := l.Check(ctx, "Steady", "k", steady)
		if err != nil {
			t.Fatal(err)
		}
		if d.Allowed {
			allowed++
		}
		time.Sleep(5 * time.Millisecond)
	}
	if allowed < 17 || allowed > 22 {
		t.Fatalf("steady rate admitted %d in one second, want about 20", allowed)
	}

	if _, err := l.Check(ctx, "x", "k", ratelimit.Policy{}); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("invalid policy: %v", err)
	}
	closed := NewLimiter(closedClient(t, cfg), cfg)
	if _, err := closed.Check(ctx, "x", "k", p); err == nil {
		t.Fatal("closed client")
	}
	if err := closed.Reset(ctx, "x", "k"); err == nil {
		t.Fatal("closed reset")
	}
}
