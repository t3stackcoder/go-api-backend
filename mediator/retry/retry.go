// Package retry defines the retry policy a command opts into by implementing
// RetryPolicy() retry.Policy. It is a leaf package so request types can import
// it without pulling in the mediator core.
package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// Policy describes how many attempts a command gets and how long to wait
// between them. Delays grow exponentially from BaseDelay with full jitter and
// are capped by MaxDelay.
type Policy struct {
	MaxAttempts int              // including the first; values below 1 mean 1
	BaseDelay   time.Duration    // exponential with full jitter, capped by MaxDelay
	MaxDelay    time.Duration    // zero means no cap
	RetryIf     func(error) bool // nil means mediator.IsTransient
}

// Attempts returns the effective attempt count, never below 1.
func (p Policy) Attempts() int {
	if p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

// Backoff returns the jitter-free upper bound of the delay before the given
// retry. attempt is 1 for the delay after the first failure.
func (p Policy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.BaseDelay
	for i := 1; i < attempt; i++ {
		next := d * 2
		if next < d { // overflow
			if p.MaxDelay > 0 {
				return p.MaxDelay
			}
			return d
		}
		d = next
		if p.MaxDelay > 0 && d >= p.MaxDelay {
			return p.MaxDelay
		}
	}
	if p.MaxDelay > 0 && d > p.MaxDelay {
		return p.MaxDelay
	}
	return d
}

// Delay returns the jittered delay before the given retry using rnd, which
// may be nil to use the package-level generator. Full jitter means a uniform
// value in [0, Backoff(attempt)].
func (p Policy) Delay(attempt int, rnd *rand.Rand) time.Duration {
	upper := p.Backoff(attempt)
	if upper <= 0 {
		return 0
	}
	n := int64(upper)
	if n < math.MaxInt64 {
		n++ // inclusive upper bound; at MaxInt64 the bound itself is excluded
	}
	if rnd == nil {
		return time.Duration(rand.Int64N(n)) //nolint:gosec // G404: retry jitter is not security-sensitive
	}
	return time.Duration(rnd.Int64N(n))
}
