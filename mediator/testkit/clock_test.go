package testkit_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

var start = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

func fired(ch <-chan time.Time) (time.Time, bool) {
	select {
	case v := <-ch:
		return v, true
	default:
		return time.Time{}, false
	}
}

func TestFakeClock_NowAndAfter(t *testing.T) {
	c := testkit.NewFakeClock(start)
	first, again := c.Now(), c.Now()
	if !first.Equal(start) || !again.Equal(start) {
		t.Fatal("frozen")
	}
	// Zero and negative durations fire immediately with the current time.
	for _, d := range []time.Duration{0, -time.Second} {
		v, ok := fired(c.After(d))
		if !ok || !v.Equal(start) {
			t.Fatalf("After(%v) = %v %v", d, v, ok)
		}
	}
	short := c.After(time.Second)
	long := c.After(3 * time.Second)
	if _, ok := fired(short); ok {
		t.Fatal("must not fire before Advance")
	}
	c.Advance(999 * time.Millisecond)
	if _, ok := fired(short); ok {
		t.Fatal("must not fire before due")
	}
	c.Advance(time.Millisecond)
	if v, ok := fired(short); !ok || !v.Equal(start.Add(time.Second)) {
		t.Fatalf("short fired %v %v", v, ok)
	}
	if _, ok := fired(long); ok {
		t.Fatal("long fired early")
	}
	// A large advance fires everything due, with the new time.
	later := c.After(time.Second)
	c.Advance(time.Hour)
	if v, ok := fired(long); !ok || !v.Equal(start.Add(time.Hour+time.Second)) {
		t.Fatalf("long fired %v %v", v, ok)
	}
	if _, ok := fired(later); !ok {
		t.Fatal("later")
	}
	if _, ok := fired(short); ok {
		t.Fatal("a timer fires once")
	}
	if !c.Now().Equal(start.Add(time.Hour + time.Second)) {
		t.Fatal(c.Now())
	}
	// Advance with nothing pending is fine; zero advance keeps time.
	c.Advance(0)
	if !c.Now().Equal(start.Add(time.Hour + time.Second)) {
		t.Fatal(c.Now())
	}
}

func TestFakeClock_Set(t *testing.T) {
	c := testkit.NewFakeClock(start)
	ch := c.After(time.Minute)
	c.Set(start.Add(30 * time.Second))
	if _, ok := fired(ch); ok {
		t.Fatal("early")
	}
	// Backwards: time moves, nothing fires.
	c.Set(start.Add(-time.Hour))
	if !c.Now().Equal(start.Add(-time.Hour)) {
		t.Fatal(c.Now())
	}
	if _, ok := fired(ch); ok {
		t.Fatal("backwards fired")
	}
	// Same time: no-op.
	c.Set(c.Now())
	// Forward past the timer (its due time is still start+1m).
	c.Set(start.Add(time.Minute))
	if v, ok := fired(ch); !ok || !v.Equal(start.Add(time.Minute)) {
		t.Fatalf("fired %v %v", v, ok)
	}
	// Far forward.
	c.Set(start.Add(24 * time.Hour))
	if !c.Now().Equal(start.Add(24 * time.Hour)) {
		t.Fatal(c.Now())
	}
}

func TestFakeClock_Sleep(t *testing.T) {
	c := testkit.NewFakeClock(start)
	c.Sleep(0)
	c.Sleep(-time.Second)
	synctest.Test(t, func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			c.Sleep(time.Second)
			close(done)
		}()
		synctest.Wait() // the sleeper is blocked on its timer
		select {
		case <-done:
			t.Fatal("Sleep returned before Advance")
		default:
		}
		c.Advance(time.Second)
		<-done
	})
}

func TestOffsetClock(t *testing.T) {
	base := testkit.NewFakeClock(start)
	c := testkit.NewOffsetClock(base)
	if c.Offset() != 0 || !c.Now().Equal(start) {
		t.Fatal("zero offset")
	}
	c.SetOffset(-time.Minute)
	if c.Offset() != -time.Minute || !c.Now().Equal(start.Add(-time.Minute)) {
		t.Fatal("negative offset")
	}
	c.SetOffset(time.Hour)
	base.Advance(time.Second)
	if !c.Now().Equal(start.Add(time.Hour + time.Second)) {
		t.Fatal(c.Now())
	}
	// After and Sleep delegate to the base clock and ignore the offset.
	if v, ok := fired(c.After(0)); !ok || !v.Equal(start.Add(time.Second)) {
		t.Fatalf("After = %v %v", v, ok)
	}
	c.Sleep(0)
	var _ testkit.Clock = c
	var _ testkit.Clock = base
}

func TestRealClock(t *testing.T) {
	var c testkit.Clock = testkit.RealClock{}
	before := time.Now()
	now := c.Now()
	if now.Before(before) || time.Since(now) > time.Minute {
		t.Fatal(now)
	}
	synctest.Test(t, func(t *testing.T) {
		ch := c.After(time.Hour)
		start := time.Now()
		<-ch
		c.Sleep(time.Minute)
		if el := time.Since(start); el != time.Hour+time.Minute {
			t.Fatalf("elapsed %v", el)
		}
	})
}
