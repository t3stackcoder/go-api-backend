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

// NewTimeout returns the Timeout behavior (5.6): the call runs under a
// deadline of the request's Timeout() when it declares one, else
// Config.DefaultTimeout (30 s). An earlier deadline on the incoming context
// wins. A result returned after the deadline is discarded and CodeTimeout
// returned; an error returned after the deadline that is not already a
// timeout is wrapped as one so the outcome is unambiguous. A stream keeps
// its deadline for the whole sequence and is canceled when it ends.
//
// The deadline context is deadlineCtx, which behaves like
// context.WithTimeout but allocates once and creates the timer only when
// something waits on Done (G17); see its documentation for the contract.
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
	tctx := withDeadline(ctx, t.durationFor(req, info))
	defer tctx.cancel()
	res, err := next(tctx, req)
	if cause := tctx.Err(); cause != nil {
		return nil, timeoutError(err, cause)
	}
	return res, err
}

func (t *timeout) HandleStream(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.StreamNext) iter.Seq2[any, error] {
	d := t.durationFor(req, info)
	return func(yield func(any, error) bool) {
		tctx := withDeadline(ctx, d)
		defer tctx.cancel()
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

// deadlineCtx is the deadline context of the Timeout behavior. It is
// observably equivalent to context.WithTimeout(parent, d) followed by the
// returned CancelFunc, with one difference in cost: creating it allocates
// once (the struct), while context.WithTimeout allocates four times (timer
// context, timer, timer callback, cancel closure). The timer and the Done
// channel come into existence on the first Done call, which only I/O paths
// make; a handler that never blocks never pays for them. Err reports the
// deadline from the clock until then, so a late result is still classified
// as a timeout.
//
// Children derived from it with the standard library (context.WithCancel,
// WithTimeout, WithValue, AfterFunc, errgroup) attach to the inner context
// without a watcher goroutine: Done returns the inner context's channel and
// Value hands the standard library's private cancel key to the inner
// context, so context's parentCancelCtx recognizes it as one of its own.
// TestDeadlineCtx_NoWatcherGoroutines pins that property.
//
// Cause: context.Cause(ctx) follows the inner context once Done was called;
// before that it reports the nearest standard-library ancestor's cause,
// which is nil while Err already reports context.DeadlineExceeded. The
// framework classifies outcomes through Err, never Cause.
type deadlineCtx struct {
	parent   context.Context
	deadline time.Time

	// inner is context.WithDeadline(parent, deadline), created by Done. It
	// is read lock-free by Value on the hot path.
	inner atomic.Pointer[innerCtx]

	mu       sync.Mutex
	err      error              // sticky result of Err
	cancelFn context.CancelFunc // inner's cancel
}

// innerCtx boxes the standard-library context so the atomic pointer has a
// concrete type to point at.
type innerCtx struct{ context.Context }

// withDeadline returns a deadlineCtx that ends d from now, or at the
// parent's deadline when that is earlier.
func withDeadline(parent context.Context, d time.Duration) *deadlineCtx {
	deadline := time.Now().Add(d)
	if pd, ok := parent.Deadline(); ok && pd.Before(deadline) {
		deadline = pd
	}
	return &deadlineCtx{parent: parent, deadline: deadline}
}

// closedChan is the Done channel of a context that ended before anything
// waited on it.
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Deadline reports the effective deadline, always present.
func (c *deadlineCtx) Deadline() (time.Time, bool) { return c.deadline, true }

// Done returns the channel of the inner context, creating it on first use.
// After cancel, or once Err reported an expired deadline, no inner context
// is created and a closed channel is returned instead.
func (c *deadlineCtx) Done() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if in := c.inner.Load(); in != nil {
		return in.Done()
	}
	if c.errLocked() != nil {
		return closedChan
	}
	ctx, cancel := context.WithDeadline(c.parent, c.deadline)
	c.cancelFn = cancel
	c.inner.Store(&innerCtx{ctx})
	return ctx.Done()
}

// Err reports why the context ended, or nil. The first non-nil answer is
// remembered so later calls agree with it.
func (c *deadlineCtx) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.errLocked()
}

func (c *deadlineCtx) errLocked() error {
	if c.err != nil {
		return c.err
	}
	switch in := c.inner.Load(); {
	case in != nil:
		c.err = in.Err()
	case c.parent.Err() != nil:
		c.err = c.parent.Err()
	case !time.Now().Before(c.deadline):
		c.err = context.DeadlineExceeded
	}
	return c.err
}

// Value delegates to the inner context once it exists (so the standard
// library finds its cancel context there), else to the parent.
func (c *deadlineCtx) Value(key any) any {
	if in := c.inner.Load(); in != nil {
		return in.Value(key)
	}
	return c.parent.Value(key)
}

// cancel ends the context: the inner context is canceled when it exists,
// and Err reports context.Canceled unless it already reported something.
func (c *deadlineCtx) cancel() {
	c.mu.Lock()
	if c.errLocked() == nil {
		c.err = context.Canceled
	}
	cancelFn := c.cancelFn
	c.mu.Unlock()
	if cancelFn != nil {
		cancelFn()
	}
}
