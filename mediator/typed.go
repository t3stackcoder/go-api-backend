package mediator

import (
	"context"
	"errors"
	"fmt"
	"reflect"
)

// TypedBehavior wraps the handler of one request type with full types.
type TypedBehavior[Q Request[R], R any] interface {
	Handle(ctx context.Context, req Q, next func(context.Context, Q) (R, error)) (R, error)
}

// TypedBehaviorFunc adapts a function to TypedBehavior.
type TypedBehaviorFunc[Q Request[R], R any] func(ctx context.Context, req Q, next func(context.Context, Q) (R, error)) (R, error)

func (f TypedBehaviorFunc[Q, R]) Handle(ctx context.Context, req Q, next func(context.Context, Q) (R, error)) (R, error) {
	return f(ctx, req, next)
}

func typedNext[Q Request[R], R any](inner Next) func(context.Context, Q) (R, error) {
	return func(ctx context.Context, q Q) (R, error) {
		res, err := inner(ctx, q)
		if err != nil {
			var zero R
			return zero, err
		}
		r, _ := res.(R)
		return r, nil
	}
}

// UseFor registers a typed behavior for Q. Without position options it sits
// immediately around the handler, inside every untyped behavior; with Before
// or After it takes part in the global order like an untyped behavior. A
// typed behavior that implements Named supplies its own name; otherwise the
// name is "typed:<request>:<n>".
func UseFor[Q Request[R], R any](m *Mediator, b TypedBehavior[Q, R], opts ...UseOption) error {
	if b == nil {
		return errors.New("mediator: nil typed behavior")
	}
	qt := reflect.TypeFor[Q]()
	var cfg useConfig
	for _, o := range opts {
		o(&cfg)
	}
	wrap := func(inner Next) Next {
		next := typedNext[Q, R](inner)
		return func(ctx context.Context, req any) (any, error) {
			res, err := b.Handle(ctx, req.(Q), next)
			return res, err
		}
	}
	m.mu.Lock()
	r := m.requests[qt]
	built := m.built.Load()
	m.mu.Unlock()
	if built {
		return ErrAlreadyBuilt
	}
	if r == nil {
		return fmt.Errorf("mediator: typed behavior for unregistered request %s; register the handler first", qt)
	}
	m.mu.Lock()
	name := fmt.Sprintf("typed:%s:%d", qt.Name(), r.typedN)
	m.mu.Unlock()
	if n, ok := b.(Named); ok {
		name = n.Name()
	}
	if len(cfg.before) == 0 && len(cfg.after) == 0 {
		m.mu.Lock()
		defer m.mu.Unlock()
		r.typed = append(r.typed, typedReg{name: name, wrap: wrap})
		r.typedN++
		return nil
	}
	cfg.types = []reflect.Type{qt}
	cfg.kinds = 0
	adapted := BehaviorFunc{N: name, F: func(ctx context.Context, req any, info *RequestInfo, next Next) (any, error) {
		return wrap(next)(ctx, req)
	}}
	if err := m.registerBehavior(adapted, cfg); err != nil {
		return err
	}
	m.mu.Lock()
	r.typedN++
	m.mu.Unlock()
	return nil
}

// PreProcessor runs after validation and before the handler.
type PreProcessor[Q Request[R], R any] interface {
	Process(ctx context.Context, req Q) error
}

// PreProcessorFunc adapts a function to PreProcessor.
type PreProcessorFunc[Q Request[R], R any] func(context.Context, Q) error

func (f PreProcessorFunc[Q, R]) Process(ctx context.Context, q Q) error { return f(ctx, q) }

// Pre registers a pre-processor for Q.
func Pre[Q Request[R], R any](m *Mediator, p PreProcessor[Q, R]) error {
	if p == nil {
		return errors.New("mediator: nil pre-processor")
	}
	return m.withReg(reflect.TypeFor[Q](), func(r *requestReg) {
		r.pre = append(r.pre, func(ctx context.Context, req any) error { return p.Process(ctx, req.(Q)) })
	})
}

// PostProcessor runs after a successful handler, before the unit of work commits.
type PostProcessor[Q Request[R], R any] interface {
	Process(ctx context.Context, req Q, res R) error
}

// PostProcessorFunc adapts a function to PostProcessor.
type PostProcessorFunc[Q Request[R], R any] func(context.Context, Q, R) error

func (f PostProcessorFunc[Q, R]) Process(ctx context.Context, q Q, r R) error { return f(ctx, q, r) }

// Post registers a post-processor for Q.
func Post[Q Request[R], R any](m *Mediator, p PostProcessor[Q, R]) error {
	if p == nil {
		return errors.New("mediator: nil post-processor")
	}
	return m.withReg(reflect.TypeFor[Q](), func(r *requestReg) {
		r.post = append(r.post, func(ctx context.Context, req any, res any) error {
			rr, _ := res.(R)
			return p.Process(ctx, req.(Q), rr)
		})
	})
}

// ErrorHandler may translate or recover an error into a response. It sees
// errors from the handler and typed behaviors only. Return handled=true to
// stop with (res, out); return handled=false and a non-nil out to replace the
// error for the next handler.
type ErrorHandler[Q Request[R], R any] interface {
	Handle(ctx context.Context, req Q, err error) (res R, handled bool, out error)
}

// ErrorHandlerFunc adapts a function to ErrorHandler.
type ErrorHandlerFunc[Q Request[R], R any] func(ctx context.Context, req Q, err error) (R, bool, error)

func (f ErrorHandlerFunc[Q, R]) Handle(ctx context.Context, req Q, err error) (R, bool, error) {
	return f(ctx, req, err)
}

// OnError registers an error handler for Q.
func OnError[Q Request[R], R any](m *Mediator, h ErrorHandler[Q, R]) error {
	if h == nil {
		return errors.New("mediator: nil error handler")
	}
	return m.withReg(reflect.TypeFor[Q](), func(r *requestReg) {
		r.errh = append(r.errh, func(ctx context.Context, req any, err error) (any, bool, error) {
			res, handled, out := h.Handle(ctx, req.(Q), err)
			return res, handled, out
		})
	})
}

func (m *Mediator) withReg(qt reflect.Type, f func(*requestReg)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.built.Load() {
		return ErrAlreadyBuilt
	}
	r := m.requests[qt]
	if r == nil {
		return fmt.Errorf("mediator: %s is not registered; register the handler first", qt)
	}
	if r.info.Kind == KindStream {
		return fmt.Errorf("mediator: %s is a stream request; processors and error handlers apply to commands and queries", qt)
	}
	f(r)
	return nil
}
