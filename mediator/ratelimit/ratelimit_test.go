package ratelimit_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
)

func TestValid(t *testing.T) {
	cases := []struct {
		name string
		p    ratelimit.Policy
		want bool
	}{
		{"zero", ratelimit.Policy{}, false},
		{"minimal", ratelimit.Policy{Rate: math.SmallestNonzeroFloat64, Period: 1}, true},
		{"typical", ratelimit.Policy{Rate: 10, Period: time.Second, Burst: 5}, true},
		{"zero burst", ratelimit.Policy{Rate: 1, Period: time.Second}, true},
		{"negative burst", ratelimit.Policy{Rate: 1, Period: time.Second, Burst: -1}, false},
		{"zero rate", ratelimit.Policy{Period: time.Second}, false},
		{"negative rate", ratelimit.Policy{Rate: -1, Period: time.Second}, false},
		{"nan rate", ratelimit.Policy{Rate: math.NaN(), Period: time.Second}, false},
		{"zero period", ratelimit.Policy{Rate: 1}, false},
		{"negative period", ratelimit.Policy{Rate: 1, Period: -time.Second}, false},
		{"max", ratelimit.Policy{Rate: math.MaxFloat64, Period: time.Duration(math.MaxInt64), Burst: math.MaxInt}, true},
		{"with key func", ratelimit.Policy{Rate: 1, Period: time.Second, Key: func(context.Context, any) string { return "k" }}, true},
	}
	for _, c := range cases {
		if got := c.p.Valid(); got != c.want {
			t.Errorf("%s: Valid() = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestEmissionInterval(t *testing.T) {
	cases := []struct {
		name string
		p    ratelimit.Policy
		want time.Duration
	}{
		{"zero", ratelimit.Policy{}, 0},
		{"zero rate", ratelimit.Policy{Period: time.Second}, 0},
		{"negative rate", ratelimit.Policy{Rate: -5, Period: time.Second}, 0},
		{"zero period", ratelimit.Policy{Rate: 5}, 0},
		{"negative period", ratelimit.Policy{Rate: 5, Period: -time.Second}, 0},
		{"nan rate zero period", ratelimit.Policy{Rate: math.NaN()}, 0},
		{"one per second", ratelimit.Policy{Rate: 1, Period: time.Second}, time.Second},
		{"ten per second", ratelimit.Policy{Rate: 10, Period: time.Second}, 100 * time.Millisecond},
		{"fractional rate", ratelimit.Policy{Rate: 0.5, Period: time.Second}, 2 * time.Second},
		{"per minute", ratelimit.Policy{Rate: 120, Period: time.Minute}, 500 * time.Millisecond},
		{"very high rate", ratelimit.Policy{Rate: 1e9, Period: time.Second}, time.Nanosecond},
		{"sub-nanosecond truncates", ratelimit.Policy{Rate: 1e12, Period: time.Second}, 0},
		{"burst is irrelevant", ratelimit.Policy{Rate: 2, Period: time.Second, Burst: 100}, 500 * time.Millisecond},
	}
	for _, c := range cases {
		if got := c.p.EmissionInterval(); got != c.want {
			t.Errorf("%s: EmissionInterval() = %v, want %v", c.name, got, c.want)
		}
	}
	d := ratelimit.Decision{Allowed: true, Remaining: 3}
	if !d.Allowed || d.Remaining != 3 || d.RetryAfter != 0 {
		t.Fatal(d)
	}
}
