package mediator_test

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/retry"
)

// ---- fixtures ------------------------------------------------------------

type bCmd struct {
	mediator.Command[mediator.Void]
}
type bQuery struct{ mediator.Query[int] }
type bStream struct{ mediator.StreamQuery[int] }

type bEvent struct {
	mediator.Event
	K string `json:"k"`
}

func (e bEvent) StreamKey() string { return e.K }

type bPlainEvent struct{ mediator.Event }

type badNamed struct {
	mediator.Command[mediator.Void]
}

func (badNamed) Name() string { return "1 bad name" }

type dupA struct {
	mediator.Command[mediator.Void]
}

func (dupA) Name() string { return "Dup" }

type dupB struct{ mediator.Query[int] }

func (dupB) Name() string { return "Dup" }

type innerQuery struct{ mediator.Query[int] }

// twoMarkers embeds Command at depth 1 and Query at depth 2; the shallower
// method set wins so it still satisfies Request[Void], and Build must reject it.
type twoMarkers struct {
	mediator.Command[mediator.Void]
	innerQuery
}

type twoMarkerEvent struct {
	mediator.Event
	bEvent
}

type ptrMarkerEvent struct{ *mediator.Event }

type sharedBase struct{ X int }
type viaBase struct{ sharedBase }

// seenTwice embeds sharedBase twice at different depths so countMarkers hits
// its visited set.
type seenTwice struct {
	mediator.Command[mediator.Void]
	sharedBase
	viaBase
	Plain string
}

type chanResp struct{ mediator.Query[chan int] }

// noUnmarshal marshals its zero value but refuses to unmarshal it, so only the
// second half of Build's round-trip check can reject it.
type noUnmarshal struct{ V int }

func (*noUnmarshal) UnmarshalJSON([]byte) error { return errors.New("no way back") }

type noUnmarshalQuery struct{ mediator.Query[noUnmarshal] }

type retryNoUow struct {
	mediator.Command[mediator.Void]
}

func (retryNoUow) RetryPolicy() retry.Policy { return retry.Policy{MaxAttempts: 2} }
func (retryNoUow) NoUnitOfWork()             {}

type retryNoUowKeyed struct {
	mediator.Command[mediator.Void]
}

func (retryNoUowKeyed) RetryPolicy() retry.Policy { return retry.Policy{MaxAttempts: 2} }
func (retryNoUowKeyed) NoUnitOfWork()             {}
func (retryNoUowKeyed) IdempotencyKey() string    { return "k" }

type retryQuery struct{ mediator.Query[int] }

func (retryQuery) RetryPolicy() retry.Policy { return retry.Policy{} }

type cacheCmd struct {
	mediator.Command[mediator.Void]
}

func (cacheCmd) CacheTags() []string { return []string{"t"} }

type cacheTTLCmd struct {
	mediator.Command[mediator.Void]
}

func (cacheTTLCmd) CacheTTL() time.Duration { return time.Second }

type invalidatesQuery struct{ mediator.Query[int] }

func (invalidatesQuery) Invalidates() []string { return []string{"t"} }

type idemQuery struct{ mediator.Query[int] }

func (idemQuery) IdempotencyKey() string { return "k" }

type evA struct{ mediator.Event }

func (evA) Name() string { return "SameEv" }

type evB struct{ mediator.Event }

func (evB) Name() string      { return "SameEv" }
func (evB) StreamKey() string { return "b" }

type evC struct{ mediator.Event }

func (evC) Name() string      { return "SameEv" }
func (evC) StreamKey() string { return "c" }

type badTopicEvent struct{ mediator.Event }

func (badTopicEvent) StreamKey() string { return "k" }
func (badTopicEvent) Topic() string     { return "bad topic!" }

type goodTopicEvent struct{ mediator.Event }

func (goodTopicEvent) StreamKey() string { return "k" }
func (goodTopicEvent) Topic() string     { return "orders.v1" }

type badNamedEvent struct{ mediator.Event }

func (badNamedEvent) Name() string { return "" }

type genericReq[T any] struct {
	mediator.Command[mediator.Void]
	V T
}

type preparer struct {
	name string
	err  error
	seen []*mediator.RequestInfo
}

func (p *preparer) Name() string { return p.name }
func (p *preparer) Handle(ctx context.Context, req any, _ *mediator.RequestInfo, next mediator.Next) (any, error) {
	return next(ctx, req)
}
func (p *preparer) Prepare(infos []*mediator.RequestInfo) error {
	p.seen = infos
	return p.err
}

func voidHandler[Q mediator.Request[mediator.Void]]() mediator.HandlerFunc[Q, mediator.Void] {
	return func(context.Context, Q) (mediator.Void, error) { return mediator.Void{}, nil }
}

func intHandler[Q mediator.Request[int]]() mediator.HandlerFunc[Q, int] {
	return func(context.Context, Q) (int, error) { return 1, nil }
}

func noopEvent[E mediator.Notification](context.Context, E) error { return nil }

func mustNil(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func wantContains(t *testing.T, err error, subs ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error containing %q, got nil", subs)
	}
	for _, s := range subs {
		if !strings.Contains(err.Error(), s) {
			t.Errorf("error missing %q:\n%v", s, err)
		}
	}
}

// ---- construction options -------------------------------------------------

func TestNew_Options(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		m := mediator.New()
		if m.Logger() == nil || m.Clock() == nil || m.NodeID() != "" || m.Remote() != nil || m.Built() {
			t.Fatal("bad defaults")
		}
		if d := time.Since(m.Clock().Now()); d < 0 || d > time.Minute {
			t.Fatalf("real clock skew %v", d)
		}
	})
	t.Run("nil values fall back to defaults", func(t *testing.T) {
		m := mediator.New(mediator.WithLogger(nil), mediator.WithClock(nil), mediator.WithMaxDepth(0))
		if m.Logger() == nil || m.Clock() == nil {
			t.Fatal("nil option must fall back")
		}
		// MaxDepth 0 means the default 32: 32 nested sends work, the 33rd fails.
		depth := 0
		mediator.MustHandle(m, mediator.HandlerFunc[bCmd, mediator.Void](func(ctx context.Context, c bCmd) (mediator.Void, error) {
			depth = mediator.Depth(ctx)
			if depth == 40 {
				return mediator.Void{}, nil
			}
			return mediator.Send(ctx, m, bCmd{})
		}))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, bCmd{})
		if !errors.Is(err, mediator.ErrDepthExceeded) || depth != 32 {
			t.Fatalf("depth=%d err=%v", depth, err)
		}
	})
	t.Run("negative depth falls back", func(t *testing.T) {
		m := mediator.New(mediator.WithMaxDepth(-5))
		build(t, m)
		if !m.Built() {
			t.Fatal("not built")
		}
	})
	t.Run("explicit values", func(t *testing.T) {
		rd := &fakeRemote{}
		m := mediator.New(mediator.WithNodeID("n1"), mediator.WithRemote(rd), mediator.WithPublishStrategy(mediator.Parallel))
		if m.NodeID() != "n1" || m.Remote() != rd {
			t.Fatal("options not applied")
		}
	})
}

// ---- registration error paths ------------------------------------------

func TestRegister_NilHandlers(t *testing.T) {
	m := mediator.New()
	var nilFunc func(context.Context, bCmd) (mediator.Void, error)
	var nilStreamFunc func(context.Context, bStream) iter.Seq2[int, error]
	var nilEvFunc func(context.Context, bEvent) error
	cases := []struct {
		name string
		err  error
	}{
		{"Handle", mediator.Handle[bCmd, mediator.Void](m, nil)},
		{"HandleFunc", mediator.HandleFunc(m, nilFunc)},
		{"HandleStream", mediator.HandleStream[bStream, int](m, nil)},
		{"HandleStreamFunc", mediator.HandleStreamFunc(m, nilStreamFunc)},
		{"On", mediator.On[bEvent](m, nil)},
		{"OnFunc", mediator.OnFunc(m, nilEvFunc)},
		{"Consume", mediator.Consume[bEvent](m, "g", nil)},
		{"ConsumeFunc", mediator.ConsumeFunc(m, "g", nilEvFunc)},
		{"Use", mediator.Use(m, nil)},
		{"UseFor", mediator.UseFor[bCmd, mediator.Void](m, nil)},
		{"Pre", mediator.Pre[bCmd, mediator.Void](m, nil)},
		{"Post", mediator.Post[bCmd, mediator.Void](m, nil)},
		{"OnError", mediator.OnError[bCmd, mediator.Void](m, nil)},
	}
	for _, c := range cases {
		if c.err == nil || !strings.Contains(c.err.Error(), "nil") {
			t.Errorf("%s: want nil-handler error, got %v", c.name, c.err)
		}
	}
	// Nothing was registered by the failed calls.
	build(t, m)
	if len(m.Requests()) != 0 || len(m.Events()) != 0 || len(m.ConsumerRegistrations()) != 0 || len(m.Order()) != 0 {
		t.Fatal("failed registrations must not register anything")
	}
}

func TestRegister_PointerTypes(t *testing.T) {
	m := mediator.New()
	err := mediator.Handle(m, mediator.HandlerFunc[*bCmd, mediator.Void](func(context.Context, *bCmd) (mediator.Void, error) {
		return mediator.Void{}, nil
	}))
	wantContains(t, err, "must be a struct, not a pointer")
	err = mediator.HandleStreamFunc(m, func(context.Context, *bStream) iter.Seq2[int, error] { return nil })
	wantContains(t, err, "must be a struct, not a pointer")
	err = mediator.Declare[*bQuery, int](m)
	wantContains(t, err, "must be a struct, not a pointer")
	err = mediator.OnFunc(m, func(context.Context, *bPlainEvent) error { return nil })
	wantContains(t, err, "must be a struct, not a pointer")
	err = mediator.RegisterEvent[*bPlainEvent](m)
	wantContains(t, err, "must be a struct, not a pointer")
	err = mediator.ConsumeFunc(m, "g", func(context.Context, *bEvent) error { return nil })
	wantContains(t, err, "must be a struct")
}

func TestRegister_Duplicates(t *testing.T) {
	m := mediator.New()
	mustNil(t, mediator.Handle(m, voidHandler[bCmd]()))
	wantContains(t, mediator.Handle(m, voidHandler[bCmd]()), "already registered")
	wantContains(t, mediator.Declare[bCmd, mediator.Void](m), "already registered")
	mustNil(t, mediator.HandleStreamFunc(m, func(context.Context, bStream) iter.Seq2[int, error] { return nil }))
	wantContains(t, mediator.HandleStreamFunc(m, func(context.Context, bStream) iter.Seq2[int, error] { return nil }), "already registered")
	// Several in-process handlers per event are allowed; the same consumer twice is not.
	mustNil(t, mediator.OnFunc(m, noopEvent[bEvent]))
	mustNil(t, mediator.OnFunc(m, noopEvent[bEvent]))
	mustNil(t, mediator.RegisterEvent[bEvent](m))
	mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[bEvent]))
	wantContains(t, mediator.ConsumeFunc(m, "g", noopEvent[bEvent]), "already registered")
	mustNil(t, mediator.ConsumeFunc(m, "g2", noopEvent[bEvent]))
	build(t, m)
	if ev := m.Events(); len(ev) != 1 || ev[0].Handlers != 2 {
		t.Fatalf("events = %+v", ev)
	}
}

func TestRegister_MustHandlePanics(t *testing.T) {
	m := mediator.New()
	mediator.MustHandle(m, voidHandler[bCmd]())
	defer func() {
		if v := recover(); v == nil || !strings.Contains(v.(error).Error(), "already registered") {
			t.Fatalf("want panic, got %v", v)
		}
	}()
	mediator.MustHandle(m, voidHandler[bCmd]())
}

func TestRegister_ConsumerGroupNames(t *testing.T) {
	for _, group := range []string{"", "1abc", "has space", "bad!", "a-b", strings.Repeat("g", 129)} {
		m := mediator.New()
		err := mediator.ConsumeFunc(m, group, noopEvent[bEvent])
		wantContains(t, err, "consumer group")
	}
	for _, group := range []string{"g", "G", "group.v1", "g_2", strings.Repeat("g", 128)} {
		m := mediator.New()
		mustNil(t, mediator.ConsumeFunc(m, group, noopEvent[bEvent]))
	}
}

type innerStream struct{ mediator.StreamQuery[int] }

// hybridStream is both a Request (Command at depth 1) and a StreamRequest
// (StreamQuery at depth 2), so it can be registered as a stream and then
// offered to the typed registration functions.
type hybridStream struct {
	mediator.Command[mediator.Void]
	innerStream
}

func TestRegister_TypedForUnregisteredOrStream(t *testing.T) {
	m := mediator.New()
	tb := mediator.TypedBehaviorFunc[bCmd, mediator.Void](func(ctx context.Context, c bCmd, next func(context.Context, bCmd) (mediator.Void, error)) (mediator.Void, error) {
		return next(ctx, c)
	})
	wantContains(t, mediator.UseFor(m, tb), "unregistered")
	wantContains(t, mediator.Pre(m, mediator.PreProcessorFunc[bCmd, mediator.Void](func(context.Context, bCmd) error { return nil })), "not registered")
	wantContains(t, mediator.Post(m, mediator.PostProcessorFunc[bCmd, mediator.Void](func(context.Context, bCmd, mediator.Void) error { return nil })), "not registered")
	wantContains(t, mediator.OnError(m, mediator.ErrorHandlerFunc[bCmd, mediator.Void](func(context.Context, bCmd, error) (mediator.Void, bool, error) {
		return mediator.Void{}, false, nil
	})), "not registered")

	mustNil(t, mediator.HandleStreamFunc(m, func(context.Context, hybridStream) iter.Seq2[int, error] { return nil }))
	wantContains(t, mediator.Pre(m, mediator.PreProcessorFunc[hybridStream, mediator.Void](func(context.Context, hybridStream) error { return nil })), "stream request")
	wantContains(t, mediator.Post(m, mediator.PostProcessorFunc[hybridStream, mediator.Void](func(context.Context, hybridStream, mediator.Void) error { return nil })), "stream request")
	wantContains(t, mediator.OnError(m, mediator.ErrorHandlerFunc[hybridStream, mediator.Void](func(context.Context, hybridStream, error) (mediator.Void, bool, error) {
		return mediator.Void{}, false, nil
	})), "stream request")
}

type bCmd2 struct {
	mediator.Command[mediator.Void]
}

func TestRegister_AfterBuild(t *testing.T) {
	m := mediator.New()
	mustNil(t, mediator.Handle(m, voidHandler[bCmd]()))
	build(t, m)
	tb := mediator.TypedBehaviorFunc[bCmd, mediator.Void](func(ctx context.Context, c bCmd, next func(context.Context, bCmd) (mediator.Void, error)) (mediator.Void, error) {
		return next(ctx, c)
	})
	cases := []struct {
		name string
		err  error
	}{
		{"Handle", mediator.Handle(m, voidHandler[bCmd2]())},
		{"HandleFunc", mediator.HandleFunc(m, func(context.Context, bQuery) (int, error) { return 0, nil })},
		{"Declare", mediator.Declare[bQuery, int](m)},
		{"HandleStream", mediator.HandleStreamFunc(m, func(context.Context, bStream) iter.Seq2[int, error] { return nil })},
		{"On", mediator.OnFunc(m, noopEvent[bEvent])},
		{"RegisterEvent", mediator.RegisterEvent[bEvent](m)},
		{"Consume", mediator.ConsumeFunc(m, "g", noopEvent[bEvent])},
		{"Use", mediator.Use(m, recorder{name: "x"})},
		{"UseFor", mediator.UseFor(m, tb)},
		{"UseFor positioned", mediator.UseFor(m, tb, mediator.After("x"))},
		{"Pre", mediator.Pre(m, mediator.PreProcessorFunc[bCmd, mediator.Void](func(context.Context, bCmd) error { return nil }))},
		{"OnBuild", m.OnBuild(func(*mediator.Mediator) error { return nil })},
		{"Build", m.Build()},
	}
	for _, c := range cases {
		if !errors.Is(c.err, mediator.ErrAlreadyBuilt) {
			t.Errorf("%s: want ErrAlreadyBuilt, got %v", c.name, c.err)
		}
	}
}

// ---- Build error reporting ----------------------------------------------

func TestBuild_ReportsEveryProblemAtOnce(t *testing.T) {
	m := mediator.New()
	mustNil(t, mediator.Handle(m, voidHandler[badNamed]()))
	mustNil(t, mediator.Handle(m, voidHandler[dupA]()))
	mustNil(t, mediator.Handle(m, intHandler[dupB]()))
	mustNil(t, mediator.Handle(m, voidHandler[twoMarkers]()))
	mustNil(t, mediator.HandleFunc(m, func(context.Context, chanResp) (chan int, error) { return nil, nil }))
	mustNil(t, mediator.HandleFunc(m, func(context.Context, noUnmarshalQuery) (noUnmarshal, error) { return noUnmarshal{}, nil }))
	mustNil(t, mediator.Handle(m, voidHandler[retryNoUow]()))
	mustNil(t, mediator.Handle(m, intHandler[retryQuery]()))
	mustNil(t, mediator.Handle(m, voidHandler[cacheCmd]()))
	mustNil(t, mediator.Handle(m, voidHandler[cacheTTLCmd]()))
	mustNil(t, mediator.Handle(m, intHandler[invalidatesQuery]()))
	mustNil(t, mediator.Handle(m, intHandler[idemQuery]()))
	mustNil(t, mediator.OnFunc(m, noopEvent[evA]))
	mustNil(t, mediator.OnFunc(m, noopEvent[twoMarkerEvent]))
	mustNil(t, mediator.OnFunc(m, noopEvent[badNamedEvent]))
	mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[evB]))
	mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[evC]))
	mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[badTopicEvent]))
	mustNil(t, mediator.Use(m, recorder{name: "A"}, mediator.After("nope")))
	mustNil(t, mediator.Use(m, recorder{name: "Self"}, mediator.Before("Self")))
	mustNil(t, mediator.Use(m, &preparer{name: "P", err: errors.New("prepare failed")}))
	mustNil(t, m.OnBuild(func(*mediator.Mediator) error { return errors.New("onbuild failed") }))

	problems := []string{
		`name "1 bad name"`,
		`name "Dup" is used by both`,
		"embeds 2 markers; exactly one of Command",
		"does not round-trip",
		"of mediator_test.noUnmarshalQuery does not round-trip through JSON",
		"no way back",
		"RetryPolicy() and NoUnitOfWork() but no IdempotencyKey()",
		"RetryPolicy() applies to commands only",
		"CacheTags() applies to queries only",
		"Invalidates() applies to commands only",
		"IdempotencyKey() applies to commands only",
		"embeds 2 markers; exactly one Event",
		`name "" of`,
		`event name "SameEv" is used by both`,
		`two consumers in group "g" handle event name "SameEv"`,
		`topic "bad topic!"`,
		"unknown behavior",
		"relative to itself",
		"behavior P: prepare failed",
		"onbuild failed",
	}
	err := m.Build()
	wantContains(t, err, problems...)
	if m.Built() {
		t.Fatal("must not be built")
	}
	// A second Build reports the same problems, not new ones caused by the
	// state the first attempt left behind. Messages are compared as a set
	// because the registry maps iterate in random order.
	err2 := m.Build()
	wantContains(t, err2, problems...)
	if a, b := sortedLines(err2), sortedLines(err); len(a) != len(b) {
		t.Fatalf("second Build reports %d problems, first %d:\n%v\n---\n%v", len(a), len(b), a, b)
	}
	if _, err := mediator.Send(context.Background(), m, dupA{}); !errors.Is(err, mediator.ErrNotBuilt) {
		t.Fatalf("want ErrNotBuilt, got %v", err)
	}
	if _, ok := m.Lookup("Dup"); ok {
		t.Fatal("Lookup before a successful Build must fail")
	}
	if _, ok := m.NewRequest("Dup"); ok {
		t.Fatal("NewRequest before a successful Build must fail")
	}
}

func TestBuild_PositionErrors(t *testing.T) {
	cases := []struct {
		name string
		reg  func(m *mediator.Mediator)
		want string
	}{
		{"unknown after", func(m *mediator.Mediator) {
			_ = mediator.Use(m, recorder{name: "A"}, mediator.After("missing"))
		}, "unknown behavior"},
		{"unknown before", func(m *mediator.Mediator) {
			_ = mediator.Use(m, recorder{name: "A"}, mediator.Before("missing"))
		}, "unknown behavior"},
		{"self", func(m *mediator.Mediator) {
			_ = mediator.Use(m, recorder{name: "A"}, mediator.After("A"))
		}, "relative to itself"},
		{"two-cycle", func(m *mediator.Mediator) {
			_ = mediator.Use(m, recorder{name: "B"}, mediator.After("C"))
			_ = mediator.Use(m, recorder{name: "C"}, mediator.After("B"))
		}, "cyclic"},
		{"three-cycle with before", func(m *mediator.Mediator) {
			_ = mediator.Use(m, recorder{name: "A"}, mediator.Before("B"))
			_ = mediator.Use(m, recorder{name: "B"}, mediator.Before("C"))
			_ = mediator.Use(m, recorder{name: "C"}, mediator.Before("A"))
		}, "cyclic"},
		{"contradiction", func(m *mediator.Mediator) {
			_ = mediator.Use(m, recorder{name: "A"})
			_ = mediator.Use(m, recorder{name: "B"})
			_ = mediator.Use(m, recorder{name: "X"}, mediator.After("B"), mediator.Before("A"))
		}, "cannot be both"},
		{"after and before the same anchor", func(m *mediator.Mediator) {
			_ = mediator.Use(m, recorder{name: "A"})
			_ = mediator.Use(m, recorder{name: "X"}, mediator.After("A"), mediator.Before("A"))
		}, "cannot be both"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := mediator.New()
			c.reg(m)
			wantContains(t, m.Build(), c.want)
		})
	}
}

func TestBuild_AcceptsValidTraits(t *testing.T) {
	m := mediator.New()
	mustNil(t, mediator.Handle(m, voidHandler[retryNoUowKeyed]()))
	mustNil(t, mediator.Handle(m, voidHandler[seenTwice]()))
	mustNil(t, mediator.Handle(m, voidHandler[genericReq[int]]()))
	mustNil(t, mediator.OnFunc(m, noopEvent[ptrMarkerEvent]))
	mustNil(t, mediator.OnFunc(m, noopEvent[goodTopicEvent]))
	mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[goodTopicEvent], mediator.HandlerTimeout(time.Second), mediator.StrictOrder(true), mediator.MaxAttempts(7)))
	p := &preparer{name: "P"}
	mustNil(t, mediator.Use(m, p))
	onBuildSeen := false
	mustNil(t, m.OnBuild(func(mm *mediator.Mediator) error {
		onBuildSeen = mm == m
		return nil
	}))
	build(t, m)
	if !onBuildSeen {
		t.Fatal("OnBuild hook did not run")
	}
	// Preparer sees requests sorted by name, then events sorted, then consumers.
	var names []string
	for _, i := range p.seen {
		names = append(names, i.String())
	}
	want := []string{"command genericReq", "command retryNoUowKeyed", "command seenTwice",
		"notification goodTopicEvent", "notification ptrMarkerEvent", "consumer goodTopicEvent[g]"}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("prepare infos = %v, want %v", names, want)
	}
	info, ok := m.Lookup("retryNoUowKeyed")
	if !ok || !info.Traits.RetryPolicy || !info.Traits.NoUnitOfWork || !info.Traits.IdempotencyKey {
		t.Fatalf("traits = %+v", info)
	}
	if info, ok := m.Lookup("genericReq"); !ok || info.RequestType != reflect.TypeFor[genericReq[int]]() {
		t.Fatal("generic instantiation name must strip the type arguments")
	}
	regs := m.ConsumerRegistrations()
	if len(regs) != 1 || regs[0].Topic != "orders.v1" || regs[0].HandlerTimeout != time.Second || !regs[0].StrictOrder || regs[0].MaxAttempts != 7 || regs[0].Group != "g" || regs[0].EventName != "goodTopicEvent" || regs[0].EventType != reflect.TypeFor[goodTopicEvent]() || regs[0].Info.Group != "g" {
		t.Fatalf("registrations = %+v", regs)
	}
	if !reflect.DeepEqual(m.Topics(), []string{"orders.v1"}) {
		t.Fatalf("topics = %v", m.Topics())
	}
}

// ---- registry accessors ---------------------------------------------------

func TestRegistry_Accessors(t *testing.T) {
	m := mediator.New()
	mustNil(t, mediator.Handle(m, voidHandler[bCmd]()))
	mustNil(t, mediator.Handle(m, intHandler[bQuery]()))
	mustNil(t, mediator.Declare[dupA, mediator.Void](m))
	mustNil(t, mediator.HandleStreamFunc(m, func(context.Context, bStream) iter.Seq2[int, error] { return nil }))
	mustNil(t, mediator.OnFunc(m, noopEvent[bEvent]))
	mustNil(t, mediator.OnFunc(m, noopEvent[bPlainEvent]))
	mustNil(t, mediator.ConsumeFunc(m, "zeta", noopEvent[bEvent]))
	mustNil(t, mediator.ConsumeFunc(m, "alpha", noopEvent[bEvent]))
	mustNil(t, mediator.ConsumeFunc(m, "alpha", noopEvent[goodTopicEvent]))
	mustNil(t, mediator.Use(m, recorder{name: "B"}))
	mustNil(t, mediator.Use(m, recorder{name: "A"}, mediator.Before("B")))
	mustNil(t, mediator.Use(m, recorder{name: "N"}, mediator.Notifications()))

	// Before Build.
	if _, ok := m.Lookup("bCmd"); ok {
		t.Fatal("Lookup must fail before Build")
	}
	if info, ok := m.InfoOf(reflect.TypeFor[*bCmd]()); !ok || info.Kind != mediator.KindCommand || info.Local != true {
		t.Fatal("InfoOf works before Build and dereferences pointers")
	}
	if _, ok := m.InfoOf(reflect.TypeFor[string]()); ok {
		t.Fatal("InfoOf unknown")
	}
	if len(m.Order()) != 0 {
		t.Fatal("Order before Build is empty")
	}
	build(t, m)

	if got := m.Order(); !reflect.DeepEqual(got, []string{"A", "B", "N"}) {
		t.Fatalf("order = %v", got)
	}
	got := m.Order()
	got[0] = "mutated"
	if m.Order()[0] != "A" {
		t.Fatal("Order must return a copy")
	}
	if got := m.ChainFor(reflect.TypeFor[*bCmd]()); !reflect.DeepEqual(got, []string{"A", "B"}) {
		t.Fatalf("chain = %v", got)
	}
	if got := m.ChainFor(reflect.TypeFor[bEvent]()); !reflect.DeepEqual(got, []string{"N"}) {
		t.Fatalf("event chain = %v", got)
	}
	if got := m.ChainFor(reflect.TypeFor[string]()); got != nil {
		t.Fatalf("unknown chain = %v", got)
	}

	var names []string
	for _, r := range m.Requests() {
		names = append(names, r.Name)
	}
	if !reflect.DeepEqual(names, []string{"Dup", "bCmd", "bQuery", "bStream"}) {
		t.Fatalf("requests = %v", names)
	}
	names = nil
	for _, e := range m.Events() {
		names = append(names, e.Name)
	}
	if !reflect.DeepEqual(names, []string{"bEvent", "bPlainEvent"}) {
		t.Fatalf("events = %v", names)
	}
	info, ok := m.Lookup("Dup")
	if !ok || info.Local || info.RequestType != reflect.TypeFor[dupA]() || info.ResponseType != reflect.TypeFor[mediator.Void]() {
		t.Fatalf("Dup info = %+v", info)
	}
	if info, ok := m.Lookup("bStream"); !ok || info.Kind != mediator.KindStream || info.ResponseType != reflect.TypeFor[int]() {
		t.Fatalf("stream info = %+v", info)
	}
	if _, ok := m.Lookup("nope"); ok {
		t.Fatal("Lookup unknown")
	}
	v, ok := m.NewRequest("bQuery")
	if !ok {
		t.Fatal("NewRequest")
	}
	if _, isPtr := v.(*bQuery); !isPtr {
		t.Fatalf("NewRequest returned %T", v)
	}
	if _, ok := m.NewRequest("nope"); ok {
		t.Fatal("NewRequest unknown")
	}
	if !info.Implements(reflect.TypeFor[mediator.Named]()) || info.Implements(reflect.TypeFor[mediator.Timeouter]()) {
		t.Fatal("Implements")
	}

	regs := m.ConsumerRegistrations()
	var keys []string
	for _, r := range regs {
		keys = append(keys, r.Group+"/"+r.EventName+"/"+r.Topic)
	}
	want := []string{"alpha/bEvent/bEvent", "alpha/goodTopicEvent/orders.v1", "zeta/bEvent/bEvent"}
	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("registrations = %v", keys)
	}
	if got := m.Topics(); !reflect.DeepEqual(got, []string{"bEvent", "orders.v1"}) {
		t.Fatalf("topics = %v", got)
	}

	entries := m.Names()
	var flat []string
	for _, e := range entries {
		flat = append(flat, e.Kind.String()+":"+e.Name+":"+e.Group+":"+e.Topic)
	}
	wantNames := []string{
		"command:Dup::", "command:bCmd::", "query:bQuery::", "stream:bStream::",
		"notification:bEvent::bEvent", "notification:bPlainEvent::bPlainEvent",
		"consumer:bEvent:alpha:bEvent", "consumer:bEvent:zeta:bEvent", "consumer:goodTopicEvent:alpha:orders.v1",
	}
	if !reflect.DeepEqual(flat, wantNames) {
		t.Fatalf("names = %v\nwant %v", flat, wantNames)
	}
	if !entries[0].Pinned || entries[1].Pinned || entries[0].GoType != "mediator_test.dupA" {
		t.Fatalf("entry = %+v", entries[0])
	}
	if s := regs[0].Info.String(); s != "consumer bEvent[alpha]" {
		t.Fatalf("String = %q", s)
	}
}

func TestBuild_EmptyMediator(t *testing.T) {
	m := mediator.New()
	build(t, m)
	if len(m.Names()) != 0 || len(m.Order()) != 0 || len(m.Topics()) != 0 {
		t.Fatal("empty")
	}
	if err := m.Build(); !errors.Is(err, mediator.ErrAlreadyBuilt) {
		t.Fatal(err)
	}
}

// sortedLines splits a joined error into its sorted lines; Build reports
// problems in registry (map) order, which is not stable.
func sortedLines(err error) []string {
	lines := strings.Split(err.Error(), "\n")
	sort.Strings(lines)
	return lines
}

// TestBuild_DuplicateConsumersKeepRegistrationOrder pins that the accessors
// stay usable after a failed Build and list two consumers with the same group
// and event name in registration order: the sorts treat equal keys as equal
// instead of swapping them.
func TestBuild_DuplicateConsumersKeepRegistrationOrder(t *testing.T) {
	cases := []struct {
		name string
		reg  func(m *mediator.Mediator)
		want []string
	}{
		{"B then C", func(m *mediator.Mediator) {
			mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[evB]))
			mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[evC]))
		}, []string{"mediator_test.evB", "mediator_test.evC"}},
		{"C then B", func(m *mediator.Mediator) {
			mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[evC]))
			mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[evB]))
		}, []string{"mediator_test.evC", "mediator_test.evB"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := mediator.New()
			c.reg(m)
			wantContains(t, m.Build(), `two consumers in group "g" handle event name "SameEv"`)
			var regs []string
			for _, r := range m.ConsumerRegistrations() {
				if r.Group != "g" || r.EventName != "SameEv" {
					t.Fatalf("registration = %+v", r)
				}
				regs = append(regs, r.EventType.String())
			}
			if !reflect.DeepEqual(regs, c.want) {
				t.Fatalf("ConsumerRegistrations = %v, want %v", regs, c.want)
			}
			var names []string
			for _, e := range m.Names() {
				if e.Kind != mediator.KindConsumer || e.Name != "SameEv" || e.Group != "g" {
					t.Fatalf("entry = %+v", e)
				}
				names = append(names, e.GoType)
			}
			if !reflect.DeepEqual(names, c.want) {
				t.Fatalf("Names = %v, want %v", names, c.want)
			}
		})
	}
}
