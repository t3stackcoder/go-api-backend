package retry_test

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/retry"
)

func TestAttempts(t *testing.T) {
	for _, c := range []struct {
		in, want int
	}{
		{math.MinInt, 1}, {-1, 1}, {0, 1}, {1, 1}, {2, 2}, {100, 100}, {math.MaxInt, math.MaxInt},
	} {
		if got := (retry.Policy{MaxAttempts: c.in}).Attempts(); got != c.want {
			t.Errorf("MaxAttempts %d: Attempts() = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestBackoff(t *testing.T) {
	const huge = time.Duration(math.MaxInt64)
	cases := []struct {
		name    string
		p       retry.Policy
		attempt int
		want    time.Duration
	}{
		{"zero policy", retry.Policy{}, 1, 0},
		{"zero policy later attempt", retry.Policy{}, 5, 0},
		{"zero base with cap stays zero", retry.Policy{MaxDelay: time.Second}, 2, 0},
		{"attempt below 1 is 1", retry.Policy{BaseDelay: time.Second}, 0, time.Second},
		{"attempt negative is 1", retry.Policy{BaseDelay: time.Second}, -7, time.Second},
		{"first", retry.Policy{BaseDelay: time.Second}, 1, time.Second},
		{"second doubles", retry.Policy{BaseDelay: time.Second}, 2, 2 * time.Second},
		{"fourth", retry.Policy{BaseDelay: 100 * time.Millisecond}, 4, 800 * time.Millisecond},
		{"capped exactly", retry.Policy{BaseDelay: time.Second, MaxDelay: 4 * time.Second}, 3, 4 * time.Second},
		{"capped below", retry.Policy{BaseDelay: time.Second, MaxDelay: 3 * time.Second}, 3, 3 * time.Second},
		{"cap applies to the base", retry.Policy{BaseDelay: 10 * time.Second, MaxDelay: time.Second}, 1, time.Second},
		{"cap not reached", retry.Policy{BaseDelay: time.Second, MaxDelay: time.Minute}, 3, 4 * time.Second},
		{"overflow without cap keeps the last value", retry.Policy{BaseDelay: huge / 2}, 3, huge - 1},
		{"overflow without cap on large base", retry.Policy{BaseDelay: huge}, 2, huge},
		{"overflow with cap returns the cap", retry.Policy{BaseDelay: huge / 2, MaxDelay: time.Hour}, 3, time.Hour},
		{"overflow on the first doubling with cap", retry.Policy{BaseDelay: huge/2 + 1, MaxDelay: time.Hour}, 2, time.Hour},
		{"overflow on the first doubling without cap", retry.Policy{BaseDelay: huge/2 + 1}, 2, huge/2 + 1},
		{"many attempts with cap", retry.Policy{BaseDelay: time.Millisecond, MaxDelay: time.Second}, 1000, time.Second},
		{"many attempts without cap", retry.Policy{BaseDelay: time.Millisecond}, 1000, time.Millisecond << 43},
		{"negative base stops doubling", retry.Policy{BaseDelay: -time.Second}, 2, -time.Second},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.p.Backoff(c.attempt); got != c.want {
				t.Fatalf("Backoff(%d) = %v, want %v", c.attempt, got, c.want)
			}
		})
	}
	// Monotonic in the attempt number up to the cap.
	p := retry.Policy{BaseDelay: time.Millisecond, MaxDelay: time.Second}
	prev := time.Duration(-1)
	for a := 1; a <= 20; a++ {
		d := p.Backoff(a)
		if d < prev || d > time.Second {
			t.Fatalf("attempt %d: %v after %v", a, d, prev)
		}
		prev = d
	}
}

func TestDelay(t *testing.T) {
	p := retry.Policy{BaseDelay: 100 * time.Millisecond, MaxDelay: time.Second}
	rnd := rand.New(rand.NewPCG(1, 2))
	for attempt := 0; attempt <= 6; attempt++ {
		upper := p.Backoff(attempt)
		var lo, hi time.Duration = math.MaxInt64, -1
		for range 500 {
			d := p.Delay(attempt, rnd)
			if d < 0 || d > upper {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", attempt, d, upper)
			}
			lo, hi = min(lo, d), max(hi, d)
		}
		// Full jitter spreads over the range.
		if hi-lo < upper/2 {
			t.Fatalf("attempt %d: delays %v..%v do not cover [0, %v]", attempt, lo, hi, upper)
		}
	}
	// Seeded generators are reproducible.
	a := p.Delay(3, rand.New(rand.NewPCG(7, 7)))
	b := p.Delay(3, rand.New(rand.NewPCG(7, 7)))
	if a != b {
		t.Fatal("seeded delay must be deterministic")
	}
	// Nil uses the package generator.
	for range 100 {
		if d := p.Delay(2, nil); d < 0 || d > 200*time.Millisecond {
			t.Fatal(d)
		}
	}
	// A non-positive upper bound yields zero.
	if (retry.Policy{}).Delay(3, rnd) != 0 || (retry.Policy{BaseDelay: -time.Second}).Delay(1, nil) != 0 {
		t.Fatal("zero")
	}
	// The maximum duration does not overflow the random range.
	if d := (retry.Policy{BaseDelay: time.Duration(math.MaxInt64)}).Delay(1, rnd); d < 0 {
		t.Fatal(d)
	}
	// The bound is inclusive: a 1ns bound yields both 0 and 1ns, and never
	// panics on an empty range.
	seen := map[time.Duration]bool{}
	for range 64 {
		seen[(retry.Policy{BaseDelay: time.Nanosecond}).Delay(1, rnd)] = true
	}
	if len(seen) != 2 || !seen[0] || !seen[time.Nanosecond] {
		t.Fatalf("1ns bound: saw %v, want {0, 1ns}", seen)
	}
	// A non-positive bound returns without drawing from the generator, so a
	// zero-delay policy does not disturb a generator it shares with another.
	r1, r2 := rand.New(rand.NewPCG(3, 4)), rand.New(rand.NewPCG(3, 4))
	if (retry.Policy{}).Delay(1, r1) != 0 {
		t.Fatal("zero")
	}
	if a, b := p.Delay(3, r1), p.Delay(3, r2); a != b {
		t.Fatalf("a zero bound consumed randomness: %v != %v", a, b)
	}
	if (retry.Policy{RetryIf: nil}).RetryIf != nil {
		t.Fatal("RetryIf")
	}
}
