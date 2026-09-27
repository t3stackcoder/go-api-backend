package mediator

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"reflect"
	"sort"
	"sync"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Send dispatches req through the pipeline to its handler and returns the
// typed response. A pointer to a registered request type is accepted and
// dereferenced.
func Send[R any](ctx context.Context, m *Mediator, req Request[R]) (R, error) {
	var zero R
	res, err := m.SendAny(ctx, req)
	if err != nil {
		return zero, err
	}
	if res == nil {
		return zero, nil
	}
	r, ok := res.(R)
	if !ok {
		return zero, E(CodeInternal, fmt.Sprintf("handler returned %T, want %T", res, zero))
	}
	return r, nil
}

// SendAny is the type-erased Send used by adapters that decode requests by
// name. It returns the boxed response.
func (m *Mediator) SendAny(ctx context.Context, req any) (any, error) {
	if !m.built.Load() {
		return nil, ErrNotBuilt
	}
	if req == nil {
		return nil, E(CodeBadRequest, "nil request")
	}
	t := reflect.TypeOf(req)
	if t.Kind() == reflect.Pointer {
		v := reflect.ValueOf(req)
		if v.IsNil() {
			return nil, E(CodeBadRequest, "nil request")
		}
		req = v.Elem().Interface()
		t = t.Elem()
	}
	r := m.requests[t]
	if r == nil {
		return nil, fmt.Errorf("%w: no handler for %s", ErrHandlerNotFound, t)
	}
	if r.info.Kind == KindStream {
		return nil, E(CodeInternal, fmt.Sprintf("%s is a stream request; use Stream", r.info.Name))
	}
	ctx, err := m.enter(ctx)
	if err != nil {
		return nil, err
	}
	if r.chain == nil {
		if m.remote == nil {
			return nil, fmt.Errorf("%w: %s has no local handler and remote dispatch is not enabled", ErrHandlerNotFound, r.info.Name)
		}
		return m.remote.Send(ctx, r.info, req)
	}
	return safeCall(ctx, r.chain, req)
}

// enter derives the call scope for a Send: correlation ID (kept or created),
// causation (the enclosing request or event), a fresh request ID, and depth.
// A created correlation ID equals the request ID; it is stored as the UUID
// and formatted by CorrelationID on first read.
func (m *Mediator) enter(ctx context.Context) (context.Context, error) {
	parent := scopeFrom(ctx)
	next := &scope{}
	if parent != nil {
		if parent.depth >= m.maxDepth {
			return nil, ErrDepthExceeded
		}
		next.correlation, next.corrID = parent.correlation, parent.corrID
		next.parent = parent.current
		next.depth = parent.depth + 1
	} else {
		next.depth = 1
	}
	next.current = NewID(m.clock.Now())
	if next.correlation == "" && next.corrID == uuid.Nil {
		next.corrID = next.current
	}
	return context.WithValue(ctx, scopeKey{}, next), nil
}

// Stream dispatches a stream request and returns its typed sequence. The
// request-level behaviors run when Stream is called; errors from them are
// delivered as the first and only element of the sequence.
func Stream[T any](ctx context.Context, m *Mediator, req StreamRequest[T]) iter.Seq2[T, error] {
	seq := m.StreamAny(ctx, req)
	return func(yield func(T, error) bool) {
		var zero T
		for v, err := range seq {
			if err != nil {
				yield(zero, err)
				return
			}
			t, ok := v.(T)
			if !ok && v != nil {
				yield(zero, E(CodeInternal, fmt.Sprintf("stream yielded %T, want %T", v, zero)))
				return
			}
			if !yield(t, nil) {
				return
			}
		}
	}
}

// StreamAny is the type-erased Stream used by adapters.
func (m *Mediator) StreamAny(ctx context.Context, req any) iter.Seq2[any, error] {
	if !m.built.Load() {
		return errSeq(ErrNotBuilt)
	}
	if req == nil {
		return errSeq(E(CodeBadRequest, "nil request"))
	}
	t := reflect.TypeOf(req)
	if t.Kind() == reflect.Pointer {
		v := reflect.ValueOf(req)
		if v.IsNil() {
			return errSeq(E(CodeBadRequest, "nil request"))
		}
		req = v.Elem().Interface()
		t = t.Elem()
	}
	r := m.requests[t]
	if r == nil {
		return errSeq(fmt.Errorf("%w: no handler for %s", ErrHandlerNotFound, t))
	}
	if r.info.Kind != KindStream {
		return errSeq(E(CodeInternal, fmt.Sprintf("%s is a %s; use Send", r.info.Name, r.info.Kind)))
	}
	if r.schain == nil {
		return errSeq(fmt.Errorf("%w: %s has no local stream handler", ErrHandlerNotFound, r.info.Name))
	}
	ctx, err := m.enter(ctx)
	if err != nil {
		return errSeq(err)
	}
	seq, err := safeStream(ctx, r.schain, req)
	if err != nil {
		return errSeq(err)
	}
	return guardSeq(ctx, m.logger, seq)
}

func safeStream(ctx context.Context, next StreamNext, req any) (seq iter.Seq2[any, error], err error) {
	defer func() {
		if v := recover(); v != nil {
			seq, err = nil, recovered(v)
		}
	}()
	return next(ctx, req), nil
}

// guardSeq stops the sequence on context cancellation and on the first error,
// and converts a panic inside the iterator body to a terminal error.
func guardSeq(ctx context.Context, logger *slog.Logger, seq iter.Seq2[any, error]) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) {
		done := false
		inYield := false
		defer func() {
			v := recover()
			if v == nil {
				return
			}
			// A panic raised by the caller's own loop body (inside yield) is
			// the caller's and must propagate: Go forbids resuming iteration
			// after the body panicked. A panic raised by the handler's
			// iterator is converted to a terminal error, or, when the
			// consumer has already stopped, logged: there is nobody left to
			// deliver it to.
			if inYield {
				panic(v)
			}
			if done {
				logger.Error("panic in stream iterator after the consumer stopped", "error", recovered(v))
				return
			}
			done = true
			yield(nil, recovered(v))
		}()
		for v, err := range seq {
			if err != nil {
				done = true
				yield(nil, err)
				return
			}
			if ctx.Err() != nil {
				done = true
				yield(nil, Wrap(CodeTimeout, "stream canceled", ctx.Err()))
				return
			}
			inYield = true
			ok := yield(v, nil)
			inYield = false
			if !ok {
				done = true
				return
			}
		}
		done = true
	}
}

// PublishStrategy selects how in-process handlers run.
type PublishStrategy uint8

const (
	// StopOnFirstError runs handlers in order and stops at the first error (MediatR default).
	StopOnFirstError PublishStrategy = iota
	// ContinueOnError runs every handler and returns errors.Join of the failures.
	ContinueOnError
	// Parallel runs handlers in goroutines and waits for all.
	Parallel
)

func (s PublishStrategy) String() string {
	switch s {
	case StopOnFirstError:
		return "stop_on_first_error"
	case ContinueOnError:
		return "continue_on_error"
	case Parallel:
		return "parallel"
	default:
		return "unknown"
	}
}

// PublishOption configures one Publish call.
type PublishOption func(*publishOptions)

type publishOptions struct {
	strategy    PublishStrategy
	hasStrategy bool
	headers     map[string]string
}

// Strategy overrides the fan-out strategy for one call.
func Strategy(s PublishStrategy) PublishOption {
	return func(o *publishOptions) { o.strategy, o.hasStrategy = s, true }
}

// Headers adds envelope headers to a durable event.
func Headers(h map[string]string) PublishOption {
	return func(o *publishOptions) {
		if o.headers == nil {
			o.headers = map[string]string{}
		}
		for k, v := range h {
			o.headers[k] = v
		}
	}
}

type publishOptsKey struct{}

// noPublishOptions is the zero option set (no strategy override, no headers)
// that a nested Publish without options installs in place of the enclosing
// call's, so the mask costs the context frame and nothing else.
var noPublishOptions publishOptions

// Publish runs the in-process handlers of e and, when e is Durable, appends
// one outbox row in the ambient unit of work. Options apply to this call only:
// a Publish made from one of its handlers does not inherit them.
func Publish(ctx context.Context, m *Mediator, e Notification, opts ...PublishOption) error {
	if !m.built.Load() {
		return ErrNotBuilt
	}
	if e == nil {
		return E(CodeBadRequest, "nil notification")
	}
	ev := any(e)
	t := reflect.TypeOf(ev)
	if t.Kind() == reflect.Pointer {
		v := reflect.ValueOf(ev)
		if v.IsNil() {
			return E(CodeBadRequest, "nil notification")
		}
		ev = v.Elem().Interface()
		t = t.Elem()
	}
	switch {
	case len(opts) > 0:
		var o publishOptions
		for _, opt := range opts {
			opt(&o)
		}
		ctx = context.WithValue(ctx, publishOptsKey{}, &o)
	case ctx.Value(publishOptsKey{}) != nil:
		// A nested Publish without options must not inherit the enclosing
		// call's strategy and headers: an option configures one call.
		ctx = context.WithValue(ctx, publishOptsKey{}, &noPublishOptions)
	}
	n := m.notifications[t]
	if n == nil {
		// Unregistered event: no in-process handlers and no chain; a durable
		// event is still appended to the outbox, with the call's headers.
		var headers map[string]string
		if o, ok := ctx.Value(publishOptsKey{}).(*publishOptions); ok {
			headers = o.headers
		}
		return m.appendDurable(ctx, ev, headers)
	}
	_, err := safeCall(ctx, n.chain, ev)
	return err
}

// PublishAll publishes several events. Durable events are appended in
// (topic, stream key) order so that two transactions publishing the same keys
// take their sequence locks in the same order and cannot deadlock.
func PublishAll(ctx context.Context, m *Mediator, events []Notification, opts ...PublishOption) error {
	sorted := make([]Notification, len(events))
	copy(sorted, events)
	sort.SliceStable(sorted, func(i, j int) bool {
		di, iok := sorted[i].(Durable)
		dj, jok := sorted[j].(Durable)
		if iok != jok {
			return !iok // non-durable first, they take no locks
		}
		if !iok {
			return false
		}
		ti, tj := topicOf(m, sorted[i]), topicOf(m, sorted[j])
		if ti != tj {
			return ti < tj
		}
		return di.StreamKey() < dj.StreamKey()
	})
	for _, e := range sorted {
		if err := Publish(ctx, m, e, opts...); err != nil {
			return err
		}
	}
	return nil
}

func topicOf(m *Mediator, e Notification) string {
	t := reflect.TypeOf(e)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if n := m.notifications[t]; n != nil {
		return n.info.Topic
	}
	if tp, ok := e.(Topicer); ok {
		return tp.Topic()
	}
	name, _ := deriveName(t)
	return name
}

// fanout runs in-process handlers according to the strategy, then appends
// the durable row. It is the innermost element of the notification chain.
func (m *Mediator) fanout(ctx context.Context, e any, handlers []func(context.Context, any) error) error {
	strategy := m.strategy
	var headers map[string]string
	if o, ok := ctx.Value(publishOptsKey{}).(*publishOptions); ok {
		if o.hasStrategy {
			strategy = o.strategy
		}
		headers = o.headers
	}
	if len(handlers) > 0 {
		switch strategy {
		case ContinueOnError:
			var errs []error
			for _, h := range handlers {
				if err := callHandler(ctx, h, e); err != nil {
					errs = append(errs, err)
				}
			}
			if len(errs) > 0 {
				return errors.Join(errs...)
			}
		case Parallel:
			errs := make([]error, len(handlers))
			var wg sync.WaitGroup
			for i, h := range handlers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs[i] = callHandler(ctx, h, e)
				}()
			}
			wg.Wait()
			if err := errors.Join(errs...); err != nil {
				return err
			}
		default:
			for _, h := range handlers {
				if err := callHandler(ctx, h, e); err != nil {
					return err
				}
			}
		}
	}
	return m.appendDurable(ctx, e, headers)
}

func callHandler(ctx context.Context, h func(context.Context, any) error, e any) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = recovered(v)
		}
	}()
	return h(ctx, e)
}

// appendDurable writes the outbox row for a durable event in the ambient
// unit of work.
func (m *Mediator) appendDurable(ctx context.Context, e any, headers map[string]string) error {
	d, ok := e.(Durable)
	if !ok {
		return nil
	}
	uow, ok := UnitOfWorkFrom(ctx)
	if !ok {
		return ErrNoUnitOfWork
	}
	if uow.ReadOnly() {
		return ErrDurablePublishInQuery
	}
	env, err := m.envelope(ctx, e, d, headers)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return Wrap(CodeInternal, "encode event", err)
	}
	return uow.AppendOutbox(ctx, &env, payload)
}

func (m *Mediator) envelope(ctx context.Context, e any, d Durable, headers map[string]string) (Envelope, error) {
	t := reflect.TypeOf(e)
	var name, topic string
	if n := m.notifications[t]; n != nil {
		name, topic = n.info.Name, n.info.Topic
	} else {
		var err error
		name, err = deriveName(t)
		if err != nil {
			return Envelope{}, err
		}
		topic = name
		if tp, ok := e.(Topicer); ok {
			topic = tp.Topic()
		}
	}
	key := d.StreamKey()
	if key == "" {
		return Envelope{}, E(CodeInternal, fmt.Sprintf("event %s has an empty stream key", name))
	}
	now := m.clock.Now()
	env := Envelope{
		ID:            NewID(now),
		Type:          name,
		Topic:         topic,
		StreamKey:     key,
		OccurredAt:    now,
		CorrelationID: CorrelationID(ctx),
		SchemaVersion: 1,
		Headers:       headers,
	}
	if id := RequestID(ctx); id != uuid.Nil {
		env.CausationID = id.String()
	}
	if sv, ok := e.(SchemaVersioner); ok {
		env.SchemaVersion = sv.SchemaVersion()
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	env.TraceParent = carrier.Get("traceparent")
	return env, nil
}

// Deliver runs the consumer pipeline of group for the event described by env,
// decoded from payload. Transports call it once per delivery. A nil error
// means the delivery may be acknowledged: either the handler's effects
// committed, or the inbox reported a duplicate (see ConsumerStateFrom).
func (m *Mediator) Deliver(ctx context.Context, group string, env Envelope, payload []byte) error {
	if !m.built.Load() {
		return ErrNotBuilt
	}
	c := m.consumerIndex[consumerKey{group, env.Type}]
	if c == nil {
		return fmt.Errorf("%w: no consumer for event %q in group %q", ErrHandlerNotFound, env.Type, group)
	}
	ptr := reflect.New(c.info.RequestType)
	if err := json.Unmarshal(payload, ptr.Interface()); err != nil {
		return Wrap(CodeBadRequest, fmt.Sprintf("decode event %s", env.Type), err)
	}
	e := ptr.Elem().Interface()
	var cause uuid.UUID
	if env.CausationID != "" {
		cause, _ = uuid.Parse(env.CausationID)
	}
	ctx = context.WithValue(ctx, scopeKey{}, &scope{correlation: env.CorrelationID, current: env.ID, parent: cause, depth: 1})
	ctx = WithEnvelope(ctx, env)
	if _, ok := ConsumerStateFrom(ctx); !ok {
		ctx = WithConsumerState(ctx, &ConsumerState{Attempt: 1})
	}
	_, err := safeCall(ctx, c.chain, e)
	return err
}
