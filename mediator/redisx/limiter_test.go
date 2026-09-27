package redisx

import (
	"errors"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
)

func TestLimiterArgs(t *testing.T) {
	if _, _, _, err := limiterArgs(ratelimit.Policy{}); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("invalid policy: %v", err)
	}
	// 10 per second, burst 5: T = 100 ms, capacity 5, expiry max(5 s, 500 ms, 1 s) = 5 s.
	iv, capacity, exp, err := limiterArgs(ratelimit.Policy{Rate: 10, Period: time.Second, Burst: 5})
	if err != nil || iv != 100_000 || capacity != 5 || exp != 5*time.Second {
		t.Fatalf("got %d %d %s %v", iv, capacity, exp, err)
	}
	// Burst 0: capacity 1, expiry floor 1 s.
	iv, capacity, exp, err = limiterArgs(ratelimit.Policy{Rate: 100, Period: time.Second, Burst: 0})
	if err != nil || iv != 10_000 || capacity != 1 || exp != time.Second {
		t.Fatalf("got %d %d %s %v", iv, capacity, exp, err)
	}
	// Rate below one per period: T exceeds Period*Burst, expiry follows T*capacity.
	iv, capacity, exp, err = limiterArgs(ratelimit.Policy{Rate: 0.5, Period: time.Minute, Burst: 1})
	if err != nil || iv != int64(2*time.Minute/time.Microsecond) || capacity != 1 || exp != 2*time.Minute {
		t.Fatalf("got %d %d %s %v", iv, capacity, exp, err)
	}
	// Rate below one per period with a burst above one: the expiry is the
	// interval times the capacity (6 min), above Period*Burst (3 min).
	iv, capacity, exp, err = limiterArgs(ratelimit.Policy{Rate: 0.5, Period: time.Minute, Burst: 3})
	if err != nil || iv != int64(2*time.Minute/time.Microsecond) || capacity != 3 || exp != 6*time.Minute {
		t.Fatalf("got %d %d %s %v", iv, capacity, exp, err)
	}
	// Sub-microsecond intervals clamp to 1 µs.
	iv, _, _, _ = limiterArgs(ratelimit.Policy{Rate: 1e9, Period: time.Millisecond, Burst: 1})
	if iv != 1 {
		t.Fatalf("interval clamp: %d", iv)
	}
}
