package behavior

import (
	"errors"
	"iter"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Outcome values shared by the logging and metrics behaviors (5.4, 5.5).
const (
	OutcomeOK      = "ok"
	OutcomeError   = "error"
	OutcomePanic   = "panic"
	OutcomeTimeout = "timeout"
)

// outcome indexes into per-info attribute tables.
type outcome uint8

const (
	outcomeOK outcome = iota
	outcomeError
	outcomePanic
	outcomeTimeout
)

var outcomeNames = [...]string{OutcomeOK, OutcomeError, OutcomePanic, OutcomeTimeout}

func (o outcome) String() string { return outcomeNames[o] }

// outcomeOf classifies the error of a completed call. A *mediator.PanicError
// in the chain is a panic that an inner behavior already converted (the
// cache fill does that so waiters of a shared load see an error).
func outcomeOf(err error) outcome {
	if err == nil {
		return outcomeOK
	}
	var pe *mediator.PanicError
	if errors.As(err, &pe) {
		return outcomePanic
	}
	if mediator.CodeOf(err) == mediator.CodeTimeout {
		return outcomeTimeout
	}
	return outcomeError
}

// errSeq is a sequence that yields one error.
func errSeq(err error) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) { yield(nil, err) }
}

// infoCache memoizes one value per RequestInfo. Reads are lock-free; the
// first computation of an info takes the mutex and publishes a new map, so
// Prepare warms it at Build and Handle never pays for it again. Behaviors
// used without Prepare (or shared by two mediators) fall back to the slow
// path once per info.
type infoCache[T any] struct {
	build func(*mediator.RequestInfo) *T
	mu    sync.Mutex
	m     atomic.Pointer[map[*mediator.RequestInfo]*T]
}

func (c *infoCache[T]) get(info *mediator.RequestInfo) *T {
	if m := c.m.Load(); m != nil {
		if v := (*m)[info]; v != nil {
			return v
		}
	}
	return c.slow(info)
}

func (c *infoCache[T]) slow(info *mediator.RequestInfo) *T {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := map[*mediator.RequestInfo]*T{}
	if old := c.m.Load(); old != nil {
		if v := (*old)[info]; v != nil {
			return v
		}
		next = maps.Clone(*old)
	}
	v := c.build(info)
	next[info] = v
	c.m.Store(&next)
	return v
}

func (c *infoCache[T]) prepare(infos []*mediator.RequestInfo) {
	for _, info := range infos {
		c.get(info)
	}
}

// logLimiter allows one log line per key per window (5.10: warn logs of a
// degraded cache are rate limited). The map is bounded: once it holds
// maxKeys entries, expired ones are swept before a new key is added.
type logLimiter struct {
	clock  mediator.Clock
	window time.Duration
	mu     sync.Mutex
	last   map[string]time.Time
}

const logLimiterMaxKeys = 1024

func newLogLimiter(clock mediator.Clock, window time.Duration) *logLimiter {
	return &logLimiter{clock: clock, window: window, last: map[string]time.Time{}}
}

func (l *logLimiter) allow(key string) bool {
	now := l.clock.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if t, ok := l.last[key]; ok && now.Sub(t) < l.window {
		return false
	}
	if len(l.last) >= logLimiterMaxKeys {
		for k, t := range l.last {
			if now.Sub(t) >= l.window {
				delete(l.last, k)
			}
		}
	}
	l.last[key] = now
	return true
}
