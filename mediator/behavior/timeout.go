package behavior

import (
	"context"
	"iter"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// NewTimeout returns the Timeout behavior (5.6): the call runs under
// context.WithTimeout with the request's Timeout() when it declares one,
// else Config.DefaultTimeout (30 s). An earlier deadline on the incoming
// context wins. A result returned after the deadline is discarded and
// CodeTimeout returned; an error returned after the deadline that is not
// already a timeout is wrapped as one so the outcome is unambiguous. A
// stream keeps its deadline for the whole sequence and is canceled when it
// ends.
//
// On the consumer path the deadline is the consumer's HandlerTimeout option
// when set. It is read from the registrations at Build through OnBuild,
// which UseStandard wires; a Timeout registered by hand uses the default.
func NewTimeout(cfg Config) mediator.Behavior {
	return &timeout{def: cfg.timeout()}
}

type timeout struct {
	def      time.Duration
	mu       sync.Mutex
	consumer atomic.Pointer[map[*mediator.RequestInfo]time.Duration]
}

func (t *timeout) Name() string { return Timeout }

// OnBuild records the HandlerTimeout of every consumer registration.
func (t *timeout) OnBuild(m *mediator.Mediator) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	next := map[*mediator.RequestInfo]time.Duration{}
	if old := t.consumer.Load(); old != nil {
		next = maps.Clone(*old)
	}
	for _, c := range m.ConsumerRegistrations() {
		if c.HandlerTimeout > 0 {
			next[c.Info] = c.HandlerTimeout
		}
	}
	t.consumer.Store(&next)
	return nil
}

func (t *timeout) durationFor(req any, info *mediator.RequestInfo) time.Duration {
	if info.Traits.Timeout {
		if d := req.(mediator.Timeouter).Timeout(); d > 0 {
			return d
		}
	}
	if info.Kind == mediator.KindConsumer {
		if m := t.consumer.Load(); m != nil {
			if d := (*m)[info]; d > 0 {
				return d
			}
		}
	}
	return t.def
}

// timeoutError normalizes the result of a call whose context ended: a
// result is discarded, a timeout error is kept, anything else is wrapped so
// the caller sees CodeTimeout while the cause (including an ambiguous
// commit marker) stays in the chain.
func timeoutError(err error, cause error) error {
	if err != nil && mediator.CodeOf(err) == mediator.CodeTimeout {
		return err
	}
	msg := "request deadline exceeded"
	if cause == context.Canceled {
		msg = "request canceled"
	}
	if err == nil {
		err = cause
	}
	return mediator.Wrap(mediator.CodeTimeout, msg, err)
}

func (t *timeout) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	tctx, cancel := context.WithTimeout(ctx, t.durationFor(req, info))
	defer cancel()
	res, err := next(tctx, req)
	if cause := tctx.Err(); cause != nil {
		return nil, timeoutError(err, cause)
	}
	return res, err
}

func (t *timeout) HandleStream(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.StreamNext) iter.Seq2[any, error] {
	d := t.durationFor(req, info)
	return func(yield func(any, error) bool) {
		tctx, cancel := context.WithTimeout(ctx, d)
		defer cancel()
		for v, err := range next(tctx, req) {
			if err != nil {
				if cause := tctx.Err(); cause != nil {
					err = timeoutError(err, cause)
				}
				yield(nil, err)
				return
			}
			if cause := tctx.Err(); cause != nil {
				yield(nil, timeoutError(nil, cause))
				return
			}
			if !yield(v, nil) {
				return
			}
		}
	}
}
