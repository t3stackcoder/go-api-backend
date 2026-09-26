package testkit

import (
	"sync"
	"time"
)

// Clock is the injectable time source used by every component with timers.
// Production code uses RealClock; tests use *FakeClock or run inside a
// testing/synctest bubble with RealClock, where time is virtual.
type Clock interface {
	Now() time.Time
	// NewTimer returns a timer channel that fires after d.
	After(d time.Duration) <-chan time.Time
	// Sleep blocks for d.
	Sleep(d time.Duration)
}

// RealClock is the wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time                         { return time.Now() }
func (RealClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (RealClock) Sleep(d time.Duration)                  { time.Sleep(d) }

// FakeClock is a manually advanced clock. It also supports an offset so chaos
// nodes can skew their clock through the admin endpoint.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

// NewFakeClock returns a clock frozen at start.
func NewFakeClock(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the current fake time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After returns a channel that fires once Advance moves the clock past d.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{at: c.now.Add(d), ch: make(chan time.Time, 1)}
	if d <= 0 {
		t.ch <- c.now
		return t.ch
	}
	c.timers = append(c.timers, t)
	return t.ch
}

// Sleep on a fake clock returns immediately; callers that need to wait use After.
func (c *FakeClock) Sleep(d time.Duration) {
	<-c.After(d)
}

// Advance moves the clock forward and fires every timer that became due.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var keep []*fakeTimer
	var fire []*fakeTimer
	for _, t := range c.timers {
		if !t.at.After(now) {
			fire = append(fire, t)
		} else {
			keep = append(keep, t)
		}
	}
	c.timers = keep
	c.mu.Unlock()
	for _, t := range fire {
		t.ch <- now
	}
}

// Set jumps the clock to t, firing due timers.
func (c *FakeClock) Set(t time.Time) {
	c.mu.Lock()
	d := t.Sub(c.now)
	c.mu.Unlock()
	if d > 0 {
		c.Advance(d)
		return
	}
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

// OffsetClock wraps a clock and shifts it by a mutable offset. Chaos nodes use
// it for the clock-skew nemesis.
type OffsetClock struct {
	Base   Clock
	mu     sync.RWMutex
	offset time.Duration
}

// NewOffsetClock wraps base with a zero offset.
func NewOffsetClock(base Clock) *OffsetClock { return &OffsetClock{Base: base} }

// SetOffset changes the skew applied to Now.
func (c *OffsetClock) SetOffset(d time.Duration) {
	c.mu.Lock()
	c.offset = d
	c.mu.Unlock()
}

// Offset returns the current skew.
func (c *OffsetClock) Offset() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.offset
}

func (c *OffsetClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Base.Now().Add(c.offset)
}

func (c *OffsetClock) After(d time.Duration) <-chan time.Time { return c.Base.After(d) }
func (c *OffsetClock) Sleep(d time.Duration)                  { c.Base.Sleep(d) }
