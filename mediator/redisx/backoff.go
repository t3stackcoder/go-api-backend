package redisx

import (
	"math/rand/v2"
	"time"
)

// backoff is the exponential retry schedule of the consumer loop (spec 7.3):
// min, 2*min, 4*min, ... capped at max.
type backoff struct {
	min, max time.Duration
}

// defaultBackoff is 200 ms doubling to 30 s.
var defaultBackoff = backoff{min: 200 * time.Millisecond, max: 30 * time.Second}

// delay returns the jitter-free delay before the given retry; retry 1 is the
// delay after the first failure. It never overflows.
func (b backoff) delay(retry int) time.Duration {
	if retry < 1 {
		retry = 1
	}
	d := b.min
	for i := 1; i < retry; i++ {
		if d >= b.max/2 {
			return b.max
		}
		d *= 2
	}
	if d > b.max {
		return b.max
	}
	return d
}

// jittered returns delay(retry) scaled by a uniform factor in [0.9, 1.1] so
// that partitions retrying in lockstep spread out.
func (b backoff) jittered(retry int) time.Duration {
	d := b.delay(retry)
	if d <= 0 {
		return 0
	}
	f := 0.9 + rand.Float64()*0.2
	return time.Duration(float64(d) * f)
}

// ceilDiv returns ceil(a / b) for positive b.
func ceilDiv(a, b int) int {
	if b <= 0 {
		return a
	}
	return (a + b - 1) / b
}
