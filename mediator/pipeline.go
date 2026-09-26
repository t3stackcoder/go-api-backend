package mediator

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"runtime/debug"
	"slices"
)

// Next invokes the rest of the pipeline.
type Next func(ctx context.Context, req any) (any, error)

// StreamNext invokes the rest of a stream pipeline.
type StreamNext func(ctx context.Context, req any) iter.Seq2[any, error]

// Behavior is middleware around a handler. Behaviors are ordered and may
// short-circuit by not calling next.
type Behavior interface {
	Name() string
	Handle(ctx context.Context, req any, info *RequestInfo, next Next) (any, error)
}

// StreamBehavior is a behavior that also wraps the item sequence of a stream
// request, so it can observe item counts, duration, and the terminal error,
// and keep resources such as a transaction open for the life of the sequence.
// A behavior that derives a cancellable context must implement it, because a
// plain Behavior's context ends when Handle returns.
type StreamBehavior interface {
	Behavior
	HandleStream(ctx context.Context, req any, info *RequestInfo, next StreamNext) iter.Seq2[any, error]
}

// Preparer is implemented by behaviors that validate configuration at Build.
// It receives every registered RequestInfo and reports all problems at once.
type Preparer interface {
	Prepare(infos []*RequestInfo) error
}

// BehaviorFunc adapts a function to Behavior.
type BehaviorFunc struct {
	N string
	F func(ctx context.Context, req any, info *RequestInfo, next Next) (any, error)
}

func (b BehaviorFunc) Name() string { return b.N }

func (b BehaviorFunc) Handle(ctx context.Context, req any, info *RequestInfo, next Next) (any, error) {
	return b.F(ctx, req, info, next)
}

// UseOption positions or scopes a behavior.
type UseOption func(*useConfig)

type useConfig struct {
	before []string
	after  []string
	kinds  uint8
	types  []reflect.Type
	preds  []func(*RequestInfo) bool
}

func kindBit(k Kind) uint8 { return 1 << uint8(k) }

const requestKinds = 1<<uint8(KindCommand) | 1<<uint8(KindQuery) | 1<<uint8(KindStream)

func (c *useConfig) applies(info *RequestInfo) bool {
	kinds := c.kinds
	if kinds == 0 {
		kinds = requestKinds
	}
	if kinds&kindBit(info.Kind) == 0 {
		return false
	}
	if len(c.types) > 0 && !slices.Contains(c.types, info.RequestType) {
		return false
	}
	for _, p := range c.preds {
		if !p(info) {
			return false
		}
	}
	return true
}

// Before places the behavior immediately before the named one.
func Before(name string) UseOption { return func(c *useConfig) { c.before = append(c.before, name) } }

// After places the behavior immediately after the named one.
func After(name string) UseOption { return func(c *useConfig) { c.after = append(c.after, name) } }

// Commands scopes the behavior to commands. Scope options accumulate.
func Commands() UseOption { return func(c *useConfig) { c.kinds |= kindBit(KindCommand) } }

// Queries scopes the behavior to queries.
func Queries() UseOption { return func(c *useConfig) { c.kinds |= kindBit(KindQuery) } }

// Streams scopes the behavior to stream requests.
func Streams() UseOption { return func(c *useConfig) { c.kinds |= kindBit(KindStream) } }

// Notifications scopes the behavior to the in-process publish path.
func Notifications() UseOption { return func(c *useConfig) { c.kinds |= kindBit(KindNotification) } }

// Consumers scopes the behavior to the durable consumer path.
func Consumers() UseOption { return func(c *useConfig) { c.kinds |= kindBit(KindConsumer) } }

// Requests scopes the behavior to commands, queries, and streams (the default).
func Requests() UseOption { return func(c *useConfig) { c.kinds |= requestKinds } }

// Everywhere scopes the behavior to every path.
func Everywhere() UseOption {
	return func(c *useConfig) {
		c.kinds |= requestKinds | kindBit(KindNotification) | kindBit(KindConsumer)
	}
}

// For restricts the behavior to the given request types.
func For(types ...reflect.Type) UseOption {
	return func(c *useConfig) {
		for _, t := range types {
			if t.Kind() == reflect.Pointer {
				t = t.Elem()
			}
			c.types = append(c.types, t)
		}
	}
}

// Where restricts the behavior to types the predicate accepts. It is
// evaluated once per type at Build.
func Where(pred func(*RequestInfo) bool) UseOption {
	return func(c *useConfig) { c.preds = append(c.preds, pred) }
}

type behaviorReg struct {
	b     Behavior
	sb    StreamBehavior
	name  string
	index int
	cfg   useConfig
	// afterAnchor is the name this behavior was placed after, for keeping
	// registration order among siblings.
	afterAnchor string
}

// Use registers a behavior. Order is registration order unless a position
// option is given. Names must be unique.
func Use(m *Mediator, b Behavior, opts ...UseOption) error {
	if b == nil {
		return errors.New("mediator: nil behavior")
	}
	var cfg useConfig
	for _, o := range opts {
		o(&cfg)
	}
	return m.registerBehavior(b, cfg)
}

func (m *Mediator) registerBehavior(b Behavior, cfg useConfig) error {
	name := b.Name()
	if name == "" {
		return errors.New("mediator: behavior has an empty name")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.built.Load() {
		return ErrAlreadyBuilt
	}
	for _, r := range m.behaviors {
		if r.name == name {
			return fmt.Errorf("mediator: behavior %q already registered", name)
		}
	}
	reg := &behaviorReg{b: b, name: name, index: len(m.behaviors), cfg: cfg}
	if sb, ok := b.(StreamBehavior); ok {
		reg.sb = sb
	}
	m.behaviors = append(m.behaviors, reg)
	return nil
}

// resolveOrder turns registration order plus Before/After constraints into one
// total order. A behavior with After(x) is inserted immediately after x (and
// after earlier-registered behaviors that were also placed after x); Before(x)
// immediately before x. Unknown anchors, cycles, and contradictions are errors.
func resolveOrder(regs []*behaviorReg) ([]*behaviorReg, error) {
	byName := make(map[string]*behaviorReg, len(regs))
	for _, r := range regs {
		byName[r.name] = r
	}
	var errs []error
	for _, r := range regs {
		for _, a := range append(append([]string{}, r.cfg.before...), r.cfg.after...) {
			if _, ok := byName[a]; !ok {
				errs = append(errs, fmt.Errorf("mediator: behavior %q is positioned relative to unknown behavior %q", r.name, a))
			}
			if a == r.name {
				errs = append(errs, fmt.Errorf("mediator: behavior %q is positioned relative to itself", r.name))
			}
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	placed := make([]*behaviorReg, 0, len(regs))
	pos := func(name string) int {
		for i, p := range placed {
			if p.name == name {
				return i
			}
		}
		return -1
	}
	pending := slices.Clone(regs)
	for len(pending) > 0 {
		progress := false
		var still []*behaviorReg
		for _, r := range pending {
			if len(r.cfg.before) == 0 && len(r.cfg.after) == 0 {
				placed = append(placed, r)
				progress = true
				continue
			}
			ready := true
			for _, a := range r.cfg.after {
				if pos(a) < 0 {
					ready = false
				}
			}
			for _, b := range r.cfg.before {
				if pos(b) < 0 {
					ready = false
				}
			}
			if !ready {
				still = append(still, r)
				continue
			}
			lo := -1 // must be inserted at index > lo
			var anchor string
			for _, a := range r.cfg.after {
				if p := pos(a); p > lo {
					lo = p
					anchor = a
				}
			}
			hi := len(placed) // must be inserted at index <= hi
			for _, b := range r.cfg.before {
				if p := pos(b); p < hi {
					hi = p
				}
			}
			at := lo + 1
			if anchor != "" {
				// Keep registration order among siblings placed after the same
				// anchor, wherever later insertions left them, but never step
				// past an explicit Before anchor.
				for i := lo + 1; i < hi; i++ {
					if placed[i].afterAnchor == anchor {
						at = i + 1
					}
				}
				r.afterAnchor = anchor
			}
			if len(r.cfg.after) == 0 {
				at = hi
			}
			if at > hi {
				return nil, fmt.Errorf("mediator: behavior %q cannot be both after %v and before %v", r.name, r.cfg.after, r.cfg.before)
			}
			placed = slices.Insert(placed, at, r)
			progress = true
		}
		if !progress {
			names := make([]string, 0, len(still))
			for _, r := range still {
				names = append(names, r.name)
			}
			return nil, fmt.Errorf("mediator: behaviors %v have cyclic position constraints", names)
		}
		pending = still
	}
	// Verify every constraint holds in the final order. Insertion places each
	// behavior strictly after its After anchors and at or before the index of
	// its Before anchors, and later insertions only shift, so these checks are
	// defensive.
	for _, r := range placed {
		p := pos(r.name)
		for _, a := range r.cfg.after {
			if pos(a) > p {
				errs = append(errs, fmt.Errorf("mediator: behavior %q could not be placed after %q", r.name, a)) // covergate:ignore defensive, see above
			}
		}
		for _, b := range r.cfg.before {
			if pos(b) < p {
				errs = append(errs, fmt.Errorf("mediator: behavior %q could not be placed before %q", r.name, b)) // covergate:ignore defensive, see above
			}
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...) // covergate:ignore defensive, see above
	}
	return placed, nil
}

func (m *Mediator) applicable(info *RequestInfo, ordered []*behaviorReg) []*behaviorReg {
	var out []*behaviorReg
	for _, b := range ordered {
		if b.cfg.applies(info) {
			out = append(out, b)
		}
	}
	return out
}

func (m *Mediator) compileRequest(r *requestReg, ordered []*behaviorReg) {
	info := r.info
	if info.Kind == KindStream {
		if r.stream == nil {
			return
		}
		r.schain = m.compileStream(r, ordered)
		return
	}
	if r.handler == nil {
		return
	}
	// Innermost: pre-processors, handler, post-processors.
	pre, post, handler := r.pre, r.post, r.handler
	var core Next = func(ctx context.Context, req any) (any, error) {
		for _, p := range pre {
			if err := p(ctx, req); err != nil {
				return nil, err
			}
		}
		res, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, p := range post {
			if err := p(ctx, req, res); err != nil {
				return nil, err
			}
		}
		return res, nil
	}
	if len(pre) == 0 && len(post) == 0 {
		core = handler
	}
	// Typed behaviors: first registered is outermost.
	for i := len(r.typed) - 1; i >= 0; i-- {
		core = r.typed[i].wrap(core)
	}
	// Error handlers wrap the typed behaviors and the handler.
	if len(r.errh) > 0 {
		inner, errh := core, r.errh
		core = func(ctx context.Context, req any) (any, error) {
			res, err := inner(ctx, req)
			if err == nil {
				return res, nil
			}
			for _, h := range errh {
				r2, handled, out := h(ctx, req, err)
				if handled {
					return r2, out
				}
				if out != nil {
					err = out
				}
			}
			return nil, err
		}
	}
	next := core
	app := m.applicable(info, ordered)
	for i := len(app) - 1; i >= 0; i-- {
		b, inner := app[i].b, next
		next = func(ctx context.Context, req any) (any, error) {
			return b.Handle(ctx, req, info, inner)
		}
	}
	r.chain = next
}

func (m *Mediator) compileStream(r *requestReg, ordered []*behaviorReg) StreamNext {
	info := r.info
	next := StreamNext(r.stream)
	app := m.applicable(info, ordered)
	for i := len(app) - 1; i >= 0; i-- {
		reg, inner := app[i], next
		if reg.sb != nil {
			sb := reg.sb
			next = func(ctx context.Context, req any) iter.Seq2[any, error] {
				return sb.HandleStream(ctx, req, info, inner)
			}
			continue
		}
		b := reg.b
		next = func(ctx context.Context, req any) iter.Seq2[any, error] {
			res, err := b.Handle(ctx, req, info, func(ctx context.Context, req any) (any, error) {
				return inner(ctx, req), nil
			})
			if err != nil {
				return errSeq(err)
			}
			seq, ok := res.(iter.Seq2[any, error])
			if !ok {
				return errSeq(E(CodeInternal, fmt.Sprintf("behavior %s returned %T instead of a stream", b.Name(), res)))
			}
			return seq
		}
	}
	return next
}

func (m *Mediator) compileNotification(n *notificationReg, ordered []*behaviorReg) {
	info := n.info
	handlers := n.handlers
	core := func(ctx context.Context, e any) (any, error) {
		return nil, m.fanout(ctx, e, handlers)
	}
	next := Next(core)
	app := m.applicable(info, ordered)
	for i := len(app) - 1; i >= 0; i-- {
		b, inner := app[i].b, next
		next = func(ctx context.Context, req any) (any, error) {
			return b.Handle(ctx, req, info, inner)
		}
	}
	n.chain = next
}

func (m *Mediator) compileConsumer(c *consumerReg, ordered []*behaviorReg) {
	info := c.info
	handler := c.handler
	next := Next(func(ctx context.Context, e any) (any, error) {
		return nil, handler(ctx, e)
	})
	app := m.applicable(info, ordered)
	for i := len(app) - 1; i >= 0; i-- {
		b, inner := app[i].b, next
		next = func(ctx context.Context, req any) (any, error) {
			return b.Handle(ctx, req, info, inner)
		}
	}
	c.chain = next
}

// errSeq returns a sequence that yields one error.
func errSeq(err error) iter.Seq2[any, error] {
	return func(yield func(any, error) bool) { yield(nil, err) }
}

// recovered converts a recovered panic value to an error.
func recovered(v any) error {
	if v == nil {
		return nil
	}
	return Wrap(CodeInternal, "panic in handler", &PanicError{Value: v, Stack: debug.Stack()})
}

// safeCall runs next and converts a panic to an error so no panic escapes.
func safeCall(ctx context.Context, next Next, req any) (res any, err error) {
	defer func() {
		if v := recover(); v != nil {
			res, err = nil, recovered(v)
		}
	}()
	return next(ctx, req)
}
