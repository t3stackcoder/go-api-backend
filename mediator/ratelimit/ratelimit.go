// Package ratelimit defines the policy a request opts into by implementing
// RateLimit() ratelimit.Policy. The limiter itself (GCRA in Redis) lives in
// mediator/redisx; this package is a leaf so request types can import it.
package ratelimit

import (
	"context"
	"time"
)

// Policy describes a GCRA limit: Rate operations per Period with a Burst
// allowance. Key derives the limiter key from the request context; when nil
// the behavior uses the principal subject, else the remote IP, else "global".
type Policy struct {
	Rate   float64
	Period time.Duration
	Burst  int
	Key    func(ctx context.Context, req any) string
}

// Valid reports whether the policy can be enforced.
func (p Policy) Valid() bool {
	return p.Rate > 0 && p.Period > 0 && p.Burst >= 0
}

// EmissionInterval is the GCRA "T": the time between two permitted operations
// at the steady rate.
func (p Policy) EmissionInterval() time.Duration {
	if p.Rate <= 0 || p.Period <= 0 {
		return 0
	}
	return time.Duration(float64(p.Period) / p.Rate)
}

// Decision is the outcome of one limiter check.
type Decision struct {
	Allowed    bool
	Remaining  int
	RetryAfter time.Duration // zero when allowed
}
