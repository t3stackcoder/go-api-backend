package redisx

import (
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
	if (backoff{}).jittered(1) != 0 {
		t.Error("zero backoff should be zero")
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
