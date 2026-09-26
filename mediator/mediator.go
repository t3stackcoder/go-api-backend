// Package mediator is a MediatR-style CQRS core for Go: every use case is a
// typed request with exactly one handler, cross-cutting concerns are composable
// pipeline behaviors, and notifications fan out to zero or more handlers.
//
// The Mediator is immutable after Build. Registration functions are generic
// so that a handler with the wrong signature fails to compile; internally the
// pipeline is type-erased.
package mediator

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Mediator holds the registry and the compiled pipelines.
type Mediator struct {
	logger   *slog.Logger
	clock    Clock
	strategy PublishStrategy
	remote   RemoteDispatcher
	maxDepth int
	nodeID   string

	mu       sync.Mutex
	built    atomic.Bool
	buildErr error

	requests      map[reflect.Type]*requestReg
	notifications map[reflect.Type]*notificationReg
	consumers     []*consumerReg
	consumerIndex map[consumerKey]*consumerReg
	behaviors     []*behaviorReg
	onBuild       []func(*Mediator) error
	byName        map[string]*RequestInfo
	order         []string
}

// Option configures a Mediator at construction.
type Option func(*Mediator)

// WithLogger sets the logger. Default is slog.Default() under group "mediator".
func WithLogger(l *slog.Logger) Option { return func(m *Mediator) { m.logger = l } }

// WithClock sets the time source used for IDs and envelopes.
func WithClock(c Clock) Option { return func(m *Mediator) { m.clock = c } }

// WithPublishStrategy sets the default in-process fan-out strategy.
func WithPublishStrategy(s PublishStrategy) Option { return func(m *Mediator) { m.strategy = s } }

// WithRemote enables remote dispatch for request types without a local handler.
func WithRemote(r RemoteDispatcher) Option { return func(m *Mediator) { m.remote = r } }

// WithMaxDepth caps nested Send depth. Default 32.
func WithMaxDepth(n int) Option { return func(m *Mediator) { m.maxDepth = n } }

// WithNodeID names this process for logs, leases, and remote dispatch.
func WithNodeID(id string) Option { return func(m *Mediator) { m.nodeID = id } }

// New returns an empty mediator ready for registration.
func New(opts ...Option) *Mediator {
	m := &Mediator{
		logger:        slog.Default().WithGroup("mediator"),
		clock:         realClock{},
		maxDepth:      32,
		requests:      map[reflect.Type]*requestReg{},
		notifications: map[reflect.Type]*notificationReg{},
		consumerIndex: map[consumerKey]*consumerReg{},
		byName:        map[string]*RequestInfo{},
	}
	for _, o := range opts {
		o(m)
	}
	if m.logger == nil {
		m.logger = slog.Default().WithGroup("mediator")
	}
	if m.clock == nil {
		m.clock = realClock{}
	}
	if m.maxDepth <= 0 {
		m.maxDepth = 32
	}
	return m
}

// Logger returns the mediator's logger.
func (m *Mediator) Logger() *slog.Logger { return m.logger }

// Clock returns the mediator's time source.
func (m *Mediator) Clock() Clock { return m.clock }

// NodeID returns the configured node ID, or "" when unset.
func (m *Mediator) NodeID() string { return m.nodeID }

// Built reports whether Build succeeded.
func (m *Mediator) Built() bool { return m.built.Load() }

// Remote returns the configured remote dispatcher, if any.
func (m *Mediator) Remote() RemoteDispatcher { return m.remote }

// RequestInfo describes one registered message type. Behaviors receive it
// with every call; it is immutable after Build.
type RequestInfo struct {
	Name         string
	Kind         Kind
	RequestType  reflect.Type // the request or event struct type (never a pointer)
	ResponseType reflect.Type // response type; for streams the item type; nil for events
	Traits       Traits
	// Group is the consumer group on the consumer path, "" elsewhere.
	Group string
	// Topic is the stream topic of a durable event, "" for requests.
	Topic string
	// Local reports whether a handler is registered in this process. False
	// for types registered with Declare for remote dispatch.
	Local bool
	// Handlers is the number of in-process handlers on the notification path.
	Handlers int
}

// Implements reports whether the request type implements the interface type.
// Packages that define their own traits (pg.TxOptions, httpapi.Route,
// openapi.Operation) use it at Build.
func (i *RequestInfo) Implements(iface reflect.Type) bool {
	return i.RequestType.Implements(iface)
}

func (i *RequestInfo) String() string {
	if i.Group != "" {
		return fmt.Sprintf("%s %s[%s]", i.Kind, i.Name, i.Group)
	}
	return fmt.Sprintf("%s %s", i.Kind, i.Name)
}

type requestReg struct {
	info    *RequestInfo
	handler func(context.Context, any) (any, error)
	stream  func(context.Context, any) iter.Seq2[any, error]
	typed   []typedReg
	typedN  int // typed behaviors registered so far, positioned or not, for naming
	pre     []func(context.Context, any) error
	post    []func(context.Context, any, any) error
	errh    []func(context.Context, any, error) (any, bool, error)
	chain   Next
	schain  StreamNext
}

type typedReg struct {
	name string
	wrap func(Next) Next
}

type notificationReg struct {
	info     *RequestInfo
	handlers []func(context.Context, any) error
	chain    Next
}

type consumerKey struct {
	group string
	event string
}

type consumerReg struct {
	info    *RequestInfo
	group   string
	handler func(context.Context, any) error
	opts    consumeOptions
	chain   Next
}

// Handler handles one request type.
type Handler[Q Request[R], R any] interface {
	Handle(ctx context.Context, req Q) (R, error)
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc[Q Request[R], R any] func(context.Context, Q) (R, error)

func (f HandlerFunc[Q, R]) Handle(ctx context.Context, q Q) (R, error) { return f(ctx, q) }

// Handle registers h as the single handler for Q. Returns an error if Q is
// already registered, if Q is a pointer type, or if the mediator is built.
func Handle[Q Request[R], R any](m *Mediator, h Handler[Q, R]) error {
	if h == nil {
		return errors.New("mediator: nil handler")
	}
	return m.registerRequest(reflect.TypeFor[Q](), reflect.TypeFor[R](), kindOf[Q, R](),
		func(ctx context.Context, req any) (any, error) {
			res, err := h.Handle(ctx, req.(Q))
			return res, err
		}, nil, true)
}

// HandleFunc is the closure form of Handle. Type arguments are inferred from
// the function signature.
func HandleFunc[Q Request[R], R any](m *Mediator, f func(context.Context, Q) (R, error)) error {
	if f == nil {
		return errors.New("mediator: nil handler")
	}
	return Handle[Q, R](m, HandlerFunc[Q, R](f))
}

// MustHandle is Handle that panics on error, for wiring code.
func MustHandle[Q Request[R], R any](m *Mediator, h Handler[Q, R]) {
	if err := Handle(m, h); err != nil {
		panic(err)
	}
}

// Declare registers Q without a handler so that Send dispatches it remotely
// (7.6). The response still round-trips through JSON locally.
func Declare[Q Request[R], R any](m *Mediator) error {
	return m.registerRequest(reflect.TypeFor[Q](), reflect.TypeFor[R](), kindOf[Q, R](), nil, nil, false)
}

// kindOf returns the kind of Q from its marker. A pointer Q is rejected by
// registerRequest, so its method is never invoked on a nil pointer.
func kindOf[Q Request[R], R any]() Kind {
	if reflect.TypeFor[Q]().Kind() == reflect.Pointer {
		return 0
	}
	var zero Q
	return zero.mediatorKind()
}

// StreamHandler handles one stream request type.
type StreamHandler[Q StreamRequest[T], T any] interface {
	Handle(ctx context.Context, req Q) iter.Seq2[T, error]
}

// StreamHandlerFunc adapts a function to StreamHandler.
type StreamHandlerFunc[Q StreamRequest[T], T any] func(context.Context, Q) iter.Seq2[T, error]

func (f StreamHandlerFunc[Q, T]) Handle(ctx context.Context, q Q) iter.Seq2[T, error] {
	return f(ctx, q)
}

// HandleStream registers the single handler for stream request Q.
func HandleStream[Q StreamRequest[T], T any](m *Mediator, h StreamHandler[Q, T]) error {
	if h == nil {
		return errors.New("mediator: nil stream handler")
	}
	return m.registerRequest(reflect.TypeFor[Q](), reflect.TypeFor[T](), KindStream, nil,
		func(ctx context.Context, req any) iter.Seq2[any, error] {
			seq := h.Handle(ctx, req.(Q))
			return func(yield func(any, error) bool) {
				if seq == nil {
					return
				}
				for v, err := range seq {
					if !yield(v, err) {
						return
					}
				}
			}
		}, true)
}

// HandleStreamFunc is the closure form of HandleStream.
func HandleStreamFunc[Q StreamRequest[T], T any](m *Mediator, f func(context.Context, Q) iter.Seq2[T, error]) error {
	if f == nil {
		return errors.New("mediator: nil stream handler")
	}
	return HandleStream[Q, T](m, StreamHandlerFunc[Q, T](f))
}

func (m *Mediator) registerRequest(qt, rt reflect.Type, kind Kind,
	h func(context.Context, any) (any, error),
	s func(context.Context, any) iter.Seq2[any, error], local bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.built.Load() {
		return ErrAlreadyBuilt
	}
	if qt.Kind() == reflect.Pointer {
		return fmt.Errorf("mediator: request type %s must be a struct, not a pointer", qt)
	}
	if qt.Kind() != reflect.Struct {
		return fmt.Errorf("mediator: request type %s must be a struct", qt)
	}
	if _, dup := m.requests[qt]; dup {
		return fmt.Errorf("mediator: request %s already registered", qt)
	}
	info := &RequestInfo{Kind: kind, RequestType: qt, ResponseType: rt, Traits: traitsOf(qt), Local: local}
	m.requests[qt] = &requestReg{info: info, handler: h, stream: s}
	return nil
}

func (m *Mediator) lookupReg(t reflect.Type) *requestReg {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.requests[t]
}

// NotificationHandler handles one event type.
type NotificationHandler[E Notification] interface {
	Handle(ctx context.Context, e E) error
}

// NotificationHandlerFunc adapts a function to NotificationHandler.
type NotificationHandlerFunc[E Notification] func(context.Context, E) error

func (f NotificationHandlerFunc[E]) Handle(ctx context.Context, e E) error { return f(ctx, e) }

// On registers an in-process handler. It runs synchronously inside Publish,
// in the goroutine and unit of work of the publisher.
func On[E Notification](m *Mediator, h NotificationHandler[E]) error {
	if h == nil {
		return errors.New("mediator: nil notification handler")
	}
	return m.registerOn(reflect.TypeFor[E](), func(ctx context.Context, e any) error {
		return h.Handle(ctx, e.(E))
	})
}

// OnFunc is the closure form of On.
func OnFunc[E Notification](m *Mediator, f func(context.Context, E) error) error {
	if f == nil {
		return errors.New("mediator: nil notification handler")
	}
	return On[E](m, NotificationHandlerFunc[E](f))
}

// RegisterEvent declares an event type that has no in-process handler on this
// node so its name is derived at Build and Publish runs the notification
// pipeline for it. Durable events published without any registration still
// work; they simply bypass the notification behaviors.
func RegisterEvent[E Notification](m *Mediator) error {
	return m.registerOn(reflect.TypeFor[E](), nil)
}

func (m *Mediator) registerOn(et reflect.Type, h func(context.Context, any) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.built.Load() {
		return ErrAlreadyBuilt
	}
	if et.Kind() == reflect.Pointer {
		return fmt.Errorf("mediator: event type %s must be a struct, not a pointer", et)
	}
	if et.Kind() != reflect.Struct {
		return fmt.Errorf("mediator: event type %s must be a struct", et)
	}
	n := m.notifications[et]
	if n == nil {
		n = &notificationReg{info: &RequestInfo{Kind: KindNotification, RequestType: et, Traits: traitsOf(et), Local: true}}
		m.notifications[et] = n
	}
	if h != nil {
		n.handlers = append(n.handlers, h)
	}
	return nil
}

// ConsumeOption configures a durable consumer.
type ConsumeOption func(*consumeOptions)

type consumeOptions struct {
	handlerTimeout time.Duration
	strictOrder    bool
	maxAttempts    int
}

// HandlerTimeout bounds one delivery. Default 30 s.
func HandlerTimeout(d time.Duration) ConsumeOption {
	return func(o *consumeOptions) { o.handlerTimeout = d }
}

// StrictOrder disables dead-lettering: a poison entry halts the partition.
func StrictOrder(on bool) ConsumeOption { return func(o *consumeOptions) { o.strictOrder = on } }

// MaxAttempts overrides the delivery attempts before dead-lettering. Default
// comes from the transport configuration (10).
func MaxAttempts(n int) ConsumeOption { return func(o *consumeOptions) { o.maxAttempts = n } }

// Consume registers a durable consumer in the named group. Delivery is
// at-least-once; effect is exactly-once when the effects of the handler are
// inside the unit of work.
func Consume[E Durable](m *Mediator, group string, h NotificationHandler[E], opts ...ConsumeOption) error {
	if h == nil {
		return errors.New("mediator: nil consumer handler")
	}
	if !NamePattern.MatchString(group) {
		return fmt.Errorf("mediator: consumer group %q does not match %s", group, NamePattern)
	}
	var o consumeOptions
	for _, opt := range opts {
		opt(&o)
	}
	et := reflect.TypeFor[E]()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.built.Load() {
		return ErrAlreadyBuilt
	}
	if et.Kind() != reflect.Struct {
		return fmt.Errorf("mediator: event type %s must be a struct", et)
	}
	for _, c := range m.consumers {
		if c.group == group && c.info.RequestType == et {
			return fmt.Errorf("mediator: consumer for %s in group %q already registered", et, group)
		}
	}
	m.consumers = append(m.consumers, &consumerReg{
		info:    &RequestInfo{Kind: KindConsumer, RequestType: et, Traits: traitsOf(et), Group: group, Local: true},
		group:   group,
		handler: func(ctx context.Context, e any) error { return h.Handle(ctx, e.(E)) },
		opts:    o,
	})
	return nil
}

// ConsumeFunc is the closure form of Consume.
func ConsumeFunc[E Durable](m *Mediator, group string, f func(context.Context, E) error, opts ...ConsumeOption) error {
	if f == nil {
		return errors.New("mediator: nil consumer handler")
	}
	return Consume[E](m, group, NotificationHandlerFunc[E](f), opts...)
}

// OnBuild registers a check that runs at Build after the registry is frozen.
// Packages such as httpapi use it to validate their own traits. All errors are
// reported together.
func (m *Mediator) OnBuild(f func(*Mediator) error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.built.Load() {
		return ErrAlreadyBuilt
	}
	m.onBuild = append(m.onBuild, f)
	return nil
}

// Build freezes the registry, validates every registration, resolves the
// behavior order, and compiles one chain per type. It reports every problem
// at once. After a successful Build the mediator is immutable.
func (m *Mediator) Build() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.built.Load() {
		return ErrAlreadyBuilt
	}
	var errs []error
	fail := func(err error) { errs = append(errs, err) }
	// A failed Build leaves the registry open; start each attempt from a clean index.
	clear(m.consumerIndex)

	// Names and per-type checks.
	names := map[string]reflect.Type{}
	claim := func(name string, t reflect.Type) {
		if prev, dup := names[name]; dup && prev != t {
			fail(fmt.Errorf("mediator: name %q is used by both %s and %s", name, prev, t))
			return
		}
		names[name] = t
	}
	for t, r := range m.requests {
		name, err := deriveName(t)
		if err != nil {
			fail(err)
		}
		r.info.Name = name
		claim(name, t)
		if n := countMarkers(t, map[reflect.Type]bool{}); n != 1 {
			fail(fmt.Errorf("mediator: %s embeds %d markers; exactly one of Command, Query, or StreamQuery is required", t, n))
		}
		if err := roundTrips(r.info.ResponseType); err != nil {
			fail(fmt.Errorf("mediator: response type %s of %s does not round-trip through JSON: %w", r.info.ResponseType, t, err))
		}
		if r.info.Traits.RetryPolicy && r.info.Traits.NoUnitOfWork && !r.info.Traits.IdempotencyKey {
			fail(fmt.Errorf("mediator: %s has RetryPolicy() and NoUnitOfWork() but no IdempotencyKey(); retry cannot know whether effects happened", t))
		}
		if r.info.Traits.RetryPolicy && r.info.Kind != KindCommand {
			fail(fmt.Errorf("mediator: %s is a %s; RetryPolicy() applies to commands only", t, r.info.Kind))
		}
		if (r.info.Traits.CacheTags || r.info.Traits.CacheTTL) && r.info.Kind != KindQuery {
			fail(fmt.Errorf("mediator: %s is a %s; CacheTags() applies to queries only", t, r.info.Kind))
		}
		if r.info.Traits.Invalidates && r.info.Kind != KindCommand {
			fail(fmt.Errorf("mediator: %s is a %s; Invalidates() applies to commands only", t, r.info.Kind))
		}
		if r.info.Traits.IdempotencyKey && r.info.Kind != KindCommand {
			fail(fmt.Errorf("mediator: %s is a %s; IdempotencyKey() applies to commands only", t, r.info.Kind))
		}
	}
	eventName := func(info *RequestInfo) {
		name, err := deriveName(info.RequestType)
		if err != nil {
			fail(err)
		}
		info.Name = name
		info.Topic = name
		if info.Traits.Topic {
			info.Topic = reflect.Zero(info.RequestType).Interface().(Topicer).Topic()
			if !NamePattern.MatchString(info.Topic) {
				fail(fmt.Errorf("mediator: topic %q of %s does not match %s", info.Topic, info.RequestType, NamePattern))
			}
		}
		if n := countMarkers(info.RequestType, map[reflect.Type]bool{}); n != 1 {
			fail(fmt.Errorf("mediator: %s embeds %d markers; exactly one Event is required", info.RequestType, n))
		}
	}
	for _, n := range m.notifications {
		eventName(n.info)
		n.info.Handlers = len(n.handlers)
	}
	for _, c := range m.consumers {
		eventName(c.info)
		if !c.info.Traits.Durable {
			fail(fmt.Errorf("mediator: consumer event %s must implement Durable", c.info.RequestType))
		}
		key := consumerKey{c.group, c.info.Name}
		if _, dup := m.consumerIndex[key]; dup {
			fail(fmt.Errorf("mediator: two consumers in group %q handle event name %q", c.group, c.info.Name))
		}
		m.consumerIndex[key] = c
	}
	// Events with the same name but different types collide in the outbox.
	eventTypes := map[string]reflect.Type{}
	checkEvent := func(info *RequestInfo) {
		if prev, ok := eventTypes[info.Name]; ok && prev != info.RequestType {
			fail(fmt.Errorf("mediator: event name %q is used by both %s and %s", info.Name, prev, info.RequestType))
		}
		eventTypes[info.Name] = info.RequestType
	}
	for _, n := range m.notifications {
		checkEvent(n.info)
	}
	for _, c := range m.consumers {
		checkEvent(c.info)
	}

	// Behavior order.
	ordered, err := resolveOrder(m.behaviors)
	if err != nil {
		fail(err)
	}

	infos := m.allInfos()
	for _, b := range m.behaviors { // registration order: Prepare runs even when ordering failed
		if p, ok := b.b.(Preparer); ok {
			if err := p.Prepare(infos); err != nil {
				fail(fmt.Errorf("mediator: behavior %s: %w", b.name, err))
			}
		}
	}
	// OnBuild hooks enumerate the registry through the public accessors,
	// which take m.mu, so release it while they run. Registration is still
	// rejected concurrently because hooks run only after every check above
	// assigned names, and a racing registration would be a caller bug.
	hooks := slices.Clone(m.onBuild)
	m.mu.Unlock()
	for _, f := range hooks {
		if err := f(m); err != nil {
			fail(err)
		}
	}
	m.mu.Lock()
	if len(errs) > 0 {
		m.buildErr = errors.Join(errs...)
		return m.buildErr
	}

	// Compile.
	m.order = make([]string, len(ordered))
	for i, b := range ordered {
		m.order[i] = b.name
	}
	for _, r := range m.requests {
		m.byName[r.info.Name] = r.info
		m.compileRequest(r, ordered)
	}
	for _, n := range m.notifications {
		m.compileNotification(n, ordered)
	}
	for _, c := range m.consumers {
		m.compileConsumer(c, ordered)
	}
	m.built.Store(true)
	for _, e := range m.Names() {
		m.logger.Debug("registered", "kind", e.Kind.String(), "name", e.Name, "type", e.GoType, "group", e.Group, "topic", e.Topic)
	}
	return nil
}

func roundTrips(rt reflect.Type) error {
	if rt == nil {
		return nil
	}
	zero := reflect.Zero(rt).Interface()
	b, err := json.Marshal(zero)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, reflect.New(rt).Interface())
}

// allInfos returns every RequestInfo: requests sorted by name, then events,
// then consumers.
func (m *Mediator) allInfos() []*RequestInfo {
	var out []*RequestInfo
	for _, r := range m.requests {
		out = append(out, r.info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	var ev []*RequestInfo
	for _, n := range m.notifications {
		ev = append(ev, n.info)
	}
	sort.Slice(ev, func(i, j int) bool { return ev[i].Name < ev[j].Name })
	out = append(out, ev...)
	for _, c := range m.consumers {
		out = append(out, c.info)
	}
	return out
}

// Requests returns the info of every registered request type sorted by name.
func (m *Mediator) Requests() []*RequestInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*RequestInfo
	for _, r := range m.requests {
		out = append(out, r.info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Events returns the info of every registered event type sorted by name.
func (m *Mediator) Events() []*RequestInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*RequestInfo
	for _, n := range m.notifications {
		out = append(out, n.info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Lookup returns the request info for a persisted name. Only valid after Build.
func (m *Mediator) Lookup(name string) (*RequestInfo, bool) {
	if !m.built.Load() {
		return nil, false
	}
	info, ok := m.byName[name]
	return info, ok
}

// InfoOf returns the request info for a request type (pointer or value).
func (m *Mediator) InfoOf(t reflect.Type) (*RequestInfo, bool) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	r := m.lookupReg(t)
	if r == nil {
		return nil, false
	}
	return r.info, true
}

// NewRequest returns a pointer to a zero value of the named request type, for
// decoders. Only valid after Build.
func (m *Mediator) NewRequest(name string) (any, bool) {
	info, ok := m.Lookup(name)
	if !ok {
		return nil, false
	}
	return reflect.New(info.RequestType).Interface(), true
}

// Order returns the resolved behavior order. Only valid after Build.
func (m *Mediator) Order() []string { return slices.Clone(m.order) }

// ChainFor returns the names of the behaviors that apply to the type, in
// order. Only valid after Build. Tests use it as a golden of the resolved
// order per type.
func (m *Mediator) ChainFor(t reflect.Type) []string {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	var info *RequestInfo
	if r := m.lookupReg(t); r != nil {
		info = r.info
	} else if n := m.notifications[t]; n != nil {
		info = n.info
	} else {
		return nil
	}
	var out []string
	for _, b := range m.behaviors {
		if b.cfg.applies(info) {
			out = append(out, b.name)
		}
	}
	// Reorder by resolved order.
	rank := map[string]int{}
	for i, n := range m.order {
		rank[n] = i
	}
	sort.SliceStable(out, func(i, j int) bool { return rank[out[i]] < rank[out[j]] })
	return out
}

// ConsumerInfo describes one durable consumer registration.
type ConsumerInfo struct {
	Group          string
	Topic          string
	EventName      string
	EventType      reflect.Type
	HandlerTimeout time.Duration // zero means the transport default
	StrictOrder    bool
	MaxAttempts    int // zero means the transport default
	Info           *RequestInfo
}

// ConsumerRegistrations lists every durable consumer registered on this node.
func (m *Mediator) ConsumerRegistrations() []ConsumerInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]ConsumerInfo, 0, len(m.consumers))
	for _, c := range m.consumers {
		out = append(out, ConsumerInfo{
			Group: c.group, Topic: c.info.Topic, EventName: c.info.Name, EventType: c.info.RequestType,
			HandlerTimeout: c.opts.handlerTimeout, StrictOrder: c.opts.strictOrder, MaxAttempts: c.opts.maxAttempts,
			Info: c.info,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Group != out[j].Group {
			return out[i].Group < out[j].Group
		}
		return out[i].EventName < out[j].EventName
	})
	return out
}

// Topics returns the distinct topics consumed on this node, sorted.
func (m *Mediator) Topics() []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range m.ConsumerRegistrations() {
		if !seen[c.Topic] {
			seen[c.Topic] = true
			out = append(out, c.Topic)
		}
	}
	sort.Strings(out)
	return out
}

func joinNames(ss []string) string { return strings.Join(ss, " > ") }
