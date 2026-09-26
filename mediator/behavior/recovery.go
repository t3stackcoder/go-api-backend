package behavior

import (
	"context"
	"iter"
	"log/slog"
	"runtime/debug"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// NewRecovery returns the Recovery behavior (5.2). It converts a panic from
// anything inside it into Wrap(CodeInternal, "panic in handler",
// &PanicError{Value, Stack}) and logs it at error level with the stack. On
// a stream it also converts a panic raised by the iterator into the
// terminal error of the sequence; a panic raised by the caller's own loop
// body propagates, because Go forbids resuming a range-over-func after its
// body panicked. It never re-panics: the runtime.Error values spec 5.2
// reserves for that do not occur in practice.
//
// Recovery is in every standard set, so it also carries the configuration
// checks of Config that belong to Build (retention ordering, 9.4) and
// reports them from Prepare.
func NewRecovery(cfg Config) mediator.Behavior {
	return &recovery{logger: cfg.logger(), cfgErr: cfg.validate()}
}

type recovery struct {
	logger *slog.Logger
	cfgErr error
}

func (r *recovery) Name() string { return Recovery }

// Prepare reports the configuration errors of Config.
func (r *recovery) Prepare([]*mediator.RequestInfo) error { return r.cfgErr }

func (r *recovery) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (res any, err error) {
	defer func() {
		if v := recover(); v != nil {
			res, err = nil, r.recovered(ctx, info, v)
		}
	}()
	return next(ctx, req)
}

// recovered logs the panic with its stack and returns the error of 5.2.
func (r *recovery) recovered(ctx context.Context, info *mediator.RequestInfo, v any) error {
	return recoveredPanic(ctx, r.logger, info, v)
}

// recoveredPanic is shared with the cache fill, which must convert a panic
// before singleflight sees it.
func recoveredPanic(ctx context.Context, logger *slog.Logger, info *mediator.RequestInfo, v any) error {
	pe := &mediator.PanicError{Value: v, Stack: debug.Stack()}
	logger.LogAttrs(ctx, slog.LevelError, "panic recovered",
		slog.String("name", info.Name),
		slog.String("kind", info.Kind.String()),
		slog.String("correlation_id", mediator.CorrelationID(ctx)),
		slog.Any("panic", v),
		slog.String("stack", string(pe.Stack)))
	return mediator.Wrap(mediator.CodeInternal, "panic in handler", pe)
}

func (r *recovery) HandleStream(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.StreamNext) iter.Seq2[any, error] {
	seq, err := r.safeStart(ctx, req, info, next)
	if err != nil {
		return errSeq(err)
	}
	return func(yield func(any, error) bool) {
		done, inYield := false, false
		emit := func(v any, err error) bool {
			inYield = true
			ok := yield(v, err)
			inYield = false
			return ok
		}
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			if inYield {
				panic(v)
			}
			err := r.recovered(ctx, info, v)
			if done {
				// The consumer stopped before the iterator panicked (for
				// example in its cleanup); there is nobody to deliver to.
				return
			}
			done = true
			yield(nil, err)
		}()
		for v, e := range seq {
			if e != nil {
				done = true
				emit(nil, e)
				return
			}
			if !emit(v, nil) {
				done = true
				return
			}
		}
		done = true
	}
}

// safeStart builds the sequence, converting a panic raised while the inner
// request-level behaviors run.
func (r *recovery) safeStart(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.StreamNext) (seq iter.Seq2[any, error], err error) {
	defer func() {
		if v := recover(); v != nil {
			seq, err = nil, r.recovered(ctx, info, v)
		}
	}()
	return next(ctx, req), nil
}
