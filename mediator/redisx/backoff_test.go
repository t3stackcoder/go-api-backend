package redisx

import (
	"math"
	"testing"
	"time"
)

func TestBackoff_Schedule(t *testing.T) {
	b := defaultBackoff
	want := []time.Duration{
		200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond,
		3200 * time.Millisecond, 6400 * time.Millisecond, 12800 * time.Millisecond, 25600 * time.Millisecond,
		30 * time.Second, 30 * time.Second,
	}
	for i, w := range want {
		if got := b.delay(i + 1); got != w {
			t.Errorf("delay(%d) = %s, want %s", i+1, got, w)
		}
	}
	if b.delay(0) != 200*time.Millisecond || b.delay(-5) != 200*time.Millisecond {
		t.Error("retry < 1 should be the minimum")
	}
	if b.delay(1000) != 30*time.Second {
		t.Error("overflow")
	}
	if (backoff{min: time.Minute, max: time.Second}).delay(1) != time.Second {
		t.Error("min above max should clamp")
	}
	for i := 1; i < 20; i++ {
		d := b.delay(i)
		j := b.jittered(i)
		if j < time.Duration(float64(d)*0.9)-time.Nanosecond || j > time.Duration(float64(d)*1.1)+time.Nanosecond {
			t.Errorf("jittered(%d) = %s outside 10%% of %s", i, j, d)
		}
	}
	// The factor is uniform in [0.9, 1.1]: over many samples no delay may
	// leave that band (a divisor instead of a factor would reach 1.11 d).
	d := b.delay(5)
	lo, hi := time.Duration(float64(d)*0.9)-time.Nanosecond, time.Duration(float64(d)*1.1)+time.Nanosecond
	for n := 0; n < 2000; n++ {
		if j := b.jittered(5); j < lo || j > hi {
			t.Fatalf("jittered(5) sample %d = %s outside [%s, %s]", n, j, lo, hi)
		}
	}
	if (backoff{}).jittered(1) != 0 {
		t.Error("zero backoff should be zero")
	}
}

// TestBackoff_CapBoundaries pins the cap guard of delay at the values where
// its comparisons meet: a delay at exactly half the cap, a doubling that
// lands exactly on the cap, a cap equal to the minimum, and a cap at the
// Duration maximum, which the guard must not overflow.
func TestBackoff_CapBoundaries(t *testing.T) {
	cases := []struct {
		name  string
		b     backoff
		retry int
		want  time.Duration
	}{
		{"first retry is the minimum", backoff{min: 3, max: 7}, 1, 3},
		// d == max/2 with an odd cap: the guard returns the cap rather than
		// doubling to 6 (the cap comparison is at or above half the cap).
		{"odd cap, delay at half the cap returns the cap", backoff{min: 3, max: 7}, 2, 7},
		{"odd cap stays capped", backoff{min: 3, max: 7}, 3, 7},
		{"even cap, doubling lands exactly on the cap", backoff{min: time.Second, max: 4 * time.Second}, 3, 4 * time.Second},
		{"even cap, one step below the cap", backoff{min: time.Second, max: 4 * time.Second}, 2, 2 * time.Second},
		{"cap equal to the minimum, first retry", backoff{min: time.Second, max: time.Second}, 1, time.Second},
		{"cap equal to the minimum, second retry", backoff{min: time.Second, max: time.Second}, 2, time.Second},
		{"cap at the Duration maximum does not overflow the guard", backoff{min: time.Second, max: math.MaxInt64}, 2, 2 * time.Second},
		{"cap at the Duration maximum keeps doubling", backoff{min: time.Second, max: math.MaxInt64}, 11, 1024 * time.Second},
		{"zero schedule", backoff{}, 3, 0},
	}
	for _, c := range cases {
		if got := c.b.delay(c.retry); got != c.want {
			t.Errorf("%s: delay(%d) = %d, want %d", c.name, c.retry, got, c.want)
		}
	}
}

func TestCeilDiv(t *testing.T) {
	cases := [][3]int{{16, 1, 16}, {16, 2, 8}, {16, 3, 6}, {16, 5, 4}, {16, 16, 1}, {16, 17, 1}, {4, 3, 2}, {0, 3, 0}, {5, 0, 5}}
	for _, c := range cases {
		if got := ceilDiv(c[0], c[1]); got != c[2] {
			t.Errorf("ceilDiv(%d,%d) = %d, want %d", c[0], c[1], got, c[2])
		}
	}
}
