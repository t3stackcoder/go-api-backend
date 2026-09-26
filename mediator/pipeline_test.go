package mediator_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type pCmd struct {
	mediator.Command[int]
	N int `json:"n"`
}

type pQuery struct{ mediator.Query[int] }
type pStream struct{ mediator.StreamQuery[int] }

type pEvent struct {
	mediator.Event
	K string `json:"k"`
}

func (e pEvent) StreamKey() string { return e.K }

// rec is a recording behavior with an optional failure mode.
type rec struct {
	name string
	log  *[]string
	mode string // "", "error", "panic", "nil", "wrong"
}

func (r rec) Name() string { return r.name }

func (r rec) Handle(ctx context.Context, req any, _ *mediator.RequestInfo, next mediator.Next) (any, error) {
	*r.log = append(*r.log, r.name+">")
	switch r.mode {
	case "error":
		return nil, errors.New("fail in " + r.name)
	case "panic":
		panic("panic in " + r.name)
	case "nil":
		return nil, nil
	case "wrong":
		return "wrong", nil
	}
	res, err := next(ctx, req)
	*r.log = append(*r.log, "<"+r.name)
	return res, err
}

// forward returns the names that entered the pipeline, in order.
func forward(log []string) []string {
	var out []string
	for _, e := range log {
		if strings.HasSuffix(e, ">") {
			out = append(out, strings.TrimSuffix(e, ">"))
		}
	}
	return out
}

func nesting(names []string, core string) string {
	var b strings.Builder
	for _, n := range names {
		b.WriteString(n + ">")
	}
	b.WriteString(core)
	for i := len(names) - 1; i >= 0; i-- {
		b.WriteString("<" + names[i])
	}
	return b.String()
}

var standardNames = []string{
	mediator.NameRecovery, mediator.NameTracing, mediator.NameLogging, mediator.NameMetrics,
	mediator.NameTimeout, mediator.NameAuthorization, mediator.NameRateLimit, mediator.NameValidation,
	mediator.NameCache, mediator.NameRetry, mediator.NameUnitOfWork, mediator.NameIdempotency, mediator.NameInbox,
}

func assertPanicErr(t *testing.T, err error, want string) {
	t.Helper()
	var pe *mediator.PanicError
	if mediator.CodeOf(err) != mediator.CodeInternal || !errors.As(err, &pe) {
		t.Fatalf("want recovered panic, got %v", err)
	}
	if s, _ := pe.Value.(string); !strings.Contains(s, want) {
		t.Fatalf("panic value %v, want %q", pe.Value, want)
	}
	if len(pe.Stack) == 0 || !strings.Contains(pe.Error(), want) {
		t.Fatalf("panic error %q lacks stack or value", pe.Error())
	}
}

func TestPipeline_RecorderAtEveryPosition(t *testing.T) {
	m := mediator.New()
	var log []string
	mustNil(t, mediator.HandleFunc(m, func(_ context.Context, c pCmd) (int, error) {
		log = append(log, "handler")
		return c.N, nil
	}))
	for _, n := range standardNames {
		mustNil(t, mediator.Use(m, rec{name: n, log: &log}))
	}
	want := []string{"before:" + mediator.NameRecovery}
	mustNil(t, mediator.Use(m, rec{name: want[0], log: &log}, mediator.Before(mediator.NameRecovery)))
	for _, n := range standardNames {
		want = append(want, n, "after:"+n)
		mustNil(t, mediator.Use(m, rec{name: "after:" + n, log: &log}, mediator.After(n)))
	}
	build(t, m)
	if got := m.ChainFor(reflect.TypeFor[pCmd]()); !reflect.DeepEqual(got, want) {
		t.Fatalf("chain =\n%v\nwant\n%v", got, want)
	}
	if got := m.Order(); !reflect.DeepEqual(got, want) {
		t.Fatalf("order = %v", got)
	}
	res, err := mediator.Send(context.Background(), m, pCmd{N: 9})
	if err != nil || res != 9 {
		t.Fatal(res, err)
	}
	if got := strings.Join(log, ""); got != nesting(want, "handler") {
		t.Fatalf("log = %s", got)
	}
}

func TestPipeline_ScopeFilters(t *testing.T) {
	m := mediator.New()
	var log []string
	mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { return 1, nil }))
	mustNil(t, mediator.HandleFunc(m, func(context.Context, pQuery) (int, error) { return 1, nil }))
	mustNil(t, mediator.HandleStreamFunc(m, func(context.Context, pStream) iter1 { return nil }))
	mustNil(t, mediator.OnFunc(m, noopEvent[pEvent]))
	mustNil(t, mediator.ConsumeFunc(m, "g", noopEvent[pEvent]))
	isCommand := func(i *mediator.RequestInfo) bool { return i.Kind == mediator.KindCommand }
	behaviors := []struct {
		name string
		opts []mediator.UseOption
	}{
		{"default", nil},
		{"commands", []mediator.UseOption{mediator.Commands()}},
		{"queries", []mediator.UseOption{mediator.Queries()}},
		{"streams", []mediator.UseOption{mediator.Streams()}},
		{"notifications", []mediator.UseOption{mediator.Notifications()}},
		{"consumers", []mediator.UseOption{mediator.Consumers()}},
		{"requests", []mediator.UseOption{mediator.Requests()}},
		{"everywhere", []mediator.UseOption{mediator.Everywhere()}},
		{"for-query", []mediator.UseOption{mediator.For(reflect.TypeFor[pQuery]())}},
		{"for-ptrcmd-event", []mediator.UseOption{mediator.For(reflect.TypeFor[*pCmd](), reflect.TypeFor[pEvent]()), mediator.Everywhere()}},
		{"where-command", []mediator.UseOption{mediator.Where(isCommand)}},
		{"where-durable", []mediator.UseOption{mediator.Everywhere(), mediator.Where(func(i *mediator.RequestInfo) bool { return i.Traits.Durable })}},
		{"commands-queries", []mediator.UseOption{mediator.Commands(), mediator.Queries()}},
		{"notifications-consumers", []mediator.UseOption{mediator.Notifications(), mediator.Consumers()}},
		{"never", []mediator.UseOption{mediator.For(reflect.TypeFor[pQuery]()), mediator.Where(func(*mediator.RequestInfo) bool { return false })}},
		{"group-g", []mediator.UseOption{mediator.Consumers(), mediator.Where(func(i *mediator.RequestInfo) bool { return i.Group == "g" })}},
		{"group-other", []mediator.UseOption{mediator.Consumers(), mediator.Where(func(i *mediator.RequestInfo) bool { return i.Group == "other" })}},
	}
	for _, b := range behaviors {
		mustNil(t, mediator.Use(m, rec{name: b.name, log: &log}, b.opts...))
	}
	build(t, m)
	want := map[reflect.Type][]string{
		reflect.TypeFor[pCmd]():    {"default", "commands", "requests", "everywhere", "for-ptrcmd-event", "where-command", "commands-queries"},
		reflect.TypeFor[pQuery]():  {"default", "queries", "requests", "everywhere", "for-query", "commands-queries"},
		reflect.TypeFor[pStream](): {"default", "streams", "requests", "everywhere"},
		reflect.TypeFor[pEvent]():  {"notifications", "everywhere", "for-ptrcmd-event", "where-durable", "notifications-consumers"},
	}
	for typ, w := range want {
		if got := m.ChainFor(typ); !reflect.DeepEqual(got, w) {
			t.Errorf("%s: chain = %v, want %v", typ, got, w)
		}
	}
	// The consumer chain is observed through Deliver.
	mustNil(t, m.Deliver(context.Background(), "g", mediator.Envelope{Type: "pEvent"}, []byte(`{"k":"a"}`)))
	wantConsumer := []string{"consumers", "everywhere", "for-ptrcmd-event", "where-durable", "notifications-consumers", "group-g"}
	if got := forward(log); !reflect.DeepEqual(got, wantConsumer) {
		t.Fatalf("consumer chain = %v, want %v", got, wantConsumer)
	}
	// The notification chain is observed through Publish.
	log = nil
	mustNil(t, mediator.Publish(mediator.WithUnitOfWork(context.Background(), &fakeUoW{}), m, pEvent{K: "a"}))
	if got := forward(log); !reflect.DeepEqual(got, want[reflect.TypeFor[pEvent]()]) {
		t.Fatalf("notification chain = %v", got)
	}
}

func TestPipeline_BeforeAfterResolution(t *testing.T) {
	type reg struct {
		name string
		opts []mediator.UseOption
	}
	cases := []struct {
		name string
		regs []reg
		want []string
	}{
		{"after chain", []reg{{"A", nil}, {"B", nil}, {"X", []mediator.UseOption{mediator.After("A")}}, {"Y", []mediator.UseOption{mediator.After("X")}}}, []string{"A", "X", "Y", "B"}},
		{"after siblings keep registration order", []reg{{"A", nil}, {"B", nil}, {"X", []mediator.UseOption{mediator.After("A")}}, {"Y", []mediator.UseOption{mediator.After("A")}}, {"Z", []mediator.UseOption{mediator.After("A")}}}, []string{"A", "X", "Y", "Z", "B"}},
		{"before chain", []reg{{"A", nil}, {"B", nil}, {"X", []mediator.UseOption{mediator.Before("B")}}, {"Y", []mediator.UseOption{mediator.Before("X")}}}, []string{"A", "Y", "X", "B"}},
		{"before siblings keep registration order", []reg{{"A", nil}, {"B", nil}, {"C", nil}, {"X", []mediator.UseOption{mediator.Before("C")}}, {"Y", []mediator.UseOption{mediator.Before("C")}}}, []string{"A", "B", "X", "Y", "C"}},
		{"explicit before wins over sibling order", []reg{{"A", nil}, {"X", []mediator.UseOption{mediator.After("A")}}, {"Y", []mediator.UseOption{mediator.After("A"), mediator.Before("X")}}}, []string{"A", "Y", "X"}},
		{"after and before bracket", []reg{{"A", nil}, {"B", nil}, {"C", nil}, {"X", []mediator.UseOption{mediator.After("A"), mediator.Before("C")}}}, []string{"A", "X", "B", "C"}},
		{"latest of several after anchors", []reg{{"A", nil}, {"B", nil}, {"C", nil}, {"X", []mediator.UseOption{mediator.After("A"), mediator.After("C")}}}, []string{"A", "B", "C", "X"}},
		{"earliest of several before anchors", []reg{{"A", nil}, {"B", nil}, {"C", nil}, {"X", []mediator.UseOption{mediator.Before("C"), mediator.Before("A")}}}, []string{"X", "A", "B", "C"}},
		{"forward reference after", []reg{{"X", []mediator.UseOption{mediator.After("B")}}, {"A", nil}, {"B", nil}}, []string{"A", "B", "X"}},
		{"forward reference before", []reg{{"X", []mediator.UseOption{mediator.Before("A")}}, {"A", nil}, {"B", nil}}, []string{"X", "A", "B"}},
		{"forward chain", []reg{{"Y", []mediator.UseOption{mediator.After("X")}}, {"X", []mediator.UseOption{mediator.After("A")}}, {"A", nil}}, []string{"A", "X", "Y"}},
		{"nested sibling after", []reg{{"A", nil}, {"X", []mediator.UseOption{mediator.After("A")}}, {"Y", []mediator.UseOption{mediator.After("X")}}, {"Z", []mediator.UseOption{mediator.After("A")}}}, []string{"A", "X", "Z", "Y"}},
		{"single", []reg{{"A", nil}}, []string{"A"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := mediator.New()
			var log []string
			mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { return 1, nil }))
			for _, r := range c.regs {
				mustNil(t, mediator.Use(m, rec{name: r.name, log: &log}, r.opts...))
			}
			build(t, m)
			if got := m.Order(); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("order = %v, want %v", got, c.want)
			}
			if _, err := mediator.Send(context.Background(), m, pCmd{}); err != nil {
				t.Fatal(err)
			}
			if got := forward(log); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("observed = %v, want %v", got, c.want)
			}
		})
	}
}

func TestPipeline_ShortCircuitSkipsInnerBehaviors(t *testing.T) {
	for _, mode := range []string{"error", "nil", "wrong", "panic"} {
		m := mediator.New()
		var log []string
		called := false
		mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { called = true; return 1, nil }))
		mustNil(t, mediator.Use(m, rec{name: "outer", log: &log}))
		mustNil(t, mediator.Use(m, rec{name: "short", log: &log, mode: mode}))
		mustNil(t, mediator.Use(m, rec{name: "inner", log: &log}))
		build(t, m)
		res, err := mediator.Send(context.Background(), m, pCmd{})
		if called {
			t.Fatalf("%s: handler ran", mode)
		}
		wantLog := "outer>short><outer"
		if mode == "panic" {
			wantLog = "outer>short>" // the panic unwinds through outer; safeCall catches it
		}
		if got := strings.Join(log, ""); got != wantLog {
			t.Fatalf("%s: log = %s", mode, got)
		}
		switch mode {
		case "error":
			if err == nil || !strings.Contains(err.Error(), "fail in short") {
				t.Fatal(err)
			}
		case "nil":
			if err != nil || res != 0 {
				t.Fatal(res, err)
			}
		case "wrong":
			if mediator.CodeOf(err) != mediator.CodeInternal || !strings.Contains(err.Error(), "handler returned string, want int") {
				t.Fatal(err)
			}
		case "panic":
			assertPanicErr(t, err, "panic in short")
			// The outer behavior does not see the return because the panic
			// unwinds through it; safeCall catches it at the top.
		}
	}
}

func TestPipeline_PanicNeverEscapes(t *testing.T) {
	tb := func(mode string) mediator.TypedBehaviorFunc[pCmd, int] {
		return func(ctx context.Context, c pCmd, next func(context.Context, pCmd) (int, error)) (int, error) {
			if mode == "panic" {
				panic("panic in typed")
			}
			return next(ctx, c)
		}
	}
	t.Run("handler", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { panic("panic in handler") }))
		mustNil(t, mediator.Use(m, rec{name: "outer", log: new([]string)}))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, pCmd{})
		assertPanicErr(t, err, "panic in handler")
	})
	t.Run("inner behavior", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { return 1, nil }))
		var log []string
		mustNil(t, mediator.Use(m, rec{name: "outer", log: &log}))
		mustNil(t, mediator.Use(m, rec{name: "inner", log: &log, mode: "panic"}))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, pCmd{})
		assertPanicErr(t, err, "panic in inner")
	})
	t.Run("typed behavior", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { return 1, nil }))
		mustNil(t, mediator.UseFor(m, tb("panic")))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, pCmd{})
		assertPanicErr(t, err, "panic in typed")
	})
	t.Run("positioned typed behavior", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { return 1, nil }))
		mustNil(t, mediator.Use(m, rec{name: "A", log: new([]string)}))
		mustNil(t, mediator.UseFor(m, tb("panic"), mediator.After("A")))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, pCmd{})
		assertPanicErr(t, err, "panic in typed")
	})
	t.Run("pre-processor", func(t *testing.T) {
		m := mediator.New()
		called := false
		mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { called = true; return 1, nil }))
		mustNil(t, mediator.Pre(m, mediator.PreProcessorFunc[pCmd, int](func(context.Context, pCmd) error { panic("panic in pre") })))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, pCmd{})
		assertPanicErr(t, err, "panic in pre")
		if called {
			t.Fatal("handler ran after a pre-processor panic")
		}
	})
	t.Run("post-processor", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { return 1, nil }))
		mustNil(t, mediator.Post(m, mediator.PostProcessorFunc[pCmd, int](func(context.Context, pCmd, int) error { panic("panic in post") })))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, pCmd{})
		assertPanicErr(t, err, "panic in post")
	})
	t.Run("error handler", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { return 0, errors.New("h") }))
		mustNil(t, mediator.OnError(m, mediator.ErrorHandlerFunc[pCmd, int](func(context.Context, pCmd, error) (int, bool, error) { panic("panic in onerror") })))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, pCmd{})
		assertPanicErr(t, err, "panic in onerror")
	})
	t.Run("notification handler under every strategy", func(t *testing.T) {
		for _, s := range []mediator.PublishStrategy{mediator.StopOnFirstError, mediator.ContinueOnError, mediator.Parallel} {
			m := mediator.New(mediator.WithPublishStrategy(s))
			ran := 0
			mustNil(t, mediator.OnFunc(m, func(context.Context, pEvent) error { panic("panic in on") }))
			mustNil(t, mediator.OnFunc(m, func(context.Context, pEvent) error { ran++; return nil }))
			build(t, m)
			err := mediator.Publish(context.Background(), m, pEvent{K: "k"})
			assertPanicErr(t, err, "panic in on")
			if want := map[mediator.PublishStrategy]int{mediator.StopOnFirstError: 0, mediator.ContinueOnError: 1, mediator.Parallel: 1}[s]; ran != want {
				t.Fatalf("%v: second handler ran %d times, want %d", s, ran, want)
			}
		}
	})
	t.Run("notification behavior", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.OnFunc(m, noopEvent[pEvent]))
		mustNil(t, mediator.Use(m, rec{name: "N", log: new([]string), mode: "panic"}, mediator.Notifications()))
		build(t, m)
		assertPanicErr(t, mediator.Publish(context.Background(), m, pEvent{K: "k"}), "panic in N")
	})
	t.Run("consumer handler and behavior", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.ConsumeFunc(m, "g", func(context.Context, pEvent) error { panic("panic in consumer") }))
		mustNil(t, mediator.ConsumeFunc(m, "h", noopEvent[pEvent]))
		mustNil(t, mediator.Use(m, rec{name: "C", log: new([]string), mode: "panic"}, mediator.Consumers(), mediator.Where(func(i *mediator.RequestInfo) bool { return i.Group == "h" })))
		build(t, m)
		assertPanicErr(t, m.Deliver(context.Background(), "g", mediator.Envelope{Type: "pEvent"}, []byte(`{}`)), "panic in consumer")
		assertPanicErr(t, m.Deliver(context.Background(), "h", mediator.Envelope{Type: "pEvent"}, []byte(`{}`)), "panic in C")
	})
}

type namedTyped struct {
	name string
	log  *[]string
}

func (n namedTyped) Name() string { return n.name }

func (n namedTyped) Handle(ctx context.Context, c pCmd, next func(context.Context, pCmd) (int, error)) (int, error) {
	*n.log = append(*n.log, n.name+">")
	r, err := next(ctx, c)
	*n.log = append(*n.log, "<"+n.name)
	return r, err
}

func typedRec[Q mediator.Request[int]](name string, log *[]string) mediator.TypedBehaviorFunc[Q, int] {
	return func(ctx context.Context, q Q, next func(context.Context, Q) (int, error)) (int, error) {
		*log = append(*log, name+">")
		r, err := next(ctx, q)
		*log = append(*log, "<"+name)
		return r, err
	}
}

func TestPipeline_TypedBehaviorsPositionedAndNamed(t *testing.T) {
	m := mediator.New()
	var log []string
	mustNil(t, mediator.HandleFunc(m, func(_ context.Context, c pCmd) (int, error) { log = append(log, "handler"); return c.N, nil }))
	mustNil(t, mediator.HandleFunc(m, func(context.Context, pQuery) (int, error) { log = append(log, "qhandler"); return 7, nil }))
	mustNil(t, mediator.Use(m, rec{name: "A", log: &log}))
	mustNil(t, mediator.Use(m, rec{name: "B", log: &log}))
	mustNil(t, mediator.UseFor(m, typedRec[pCmd]("t0", &log), mediator.After("A")))
	mustNil(t, mediator.UseFor(m, typedRec[pCmd]("t1", &log), mediator.Before("A")))
	mustNil(t, mediator.UseFor(m, namedTyped{name: "tenant", log: &log}, mediator.After("B")))
	mustNil(t, mediator.UseFor(m, typedRec[pCmd]("inner1", &log)))
	mustNil(t, mediator.UseFor(m, typedRec[pCmd]("inner2", &log)))
	mustNil(t, mediator.UseFor(m, typedRec[pQuery]("q0", &log), mediator.After("A")))
	// A named typed behavior that collides with an existing name is rejected.
	wantContains(t, mediator.UseFor(m, namedTyped{name: "A", log: &log}, mediator.After("B")), "already registered")
	build(t, m)

	if got := m.ChainFor(reflect.TypeFor[pCmd]()); !reflect.DeepEqual(got, []string{"typed:pCmd:1", "A", "typed:pCmd:0", "B", "tenant"}) {
		t.Fatalf("command chain = %v", got)
	}
	if got := m.ChainFor(reflect.TypeFor[pQuery]()); !reflect.DeepEqual(got, []string{"A", "typed:pQuery:0", "B"}) {
		t.Fatalf("query chain = %v", got)
	}
	if got := m.Order(); !reflect.DeepEqual(got, []string{"typed:pCmd:1", "A", "typed:pCmd:0", "typed:pQuery:0", "B", "tenant"}) {
		t.Fatalf("order = %v", got)
	}
	res, err := mediator.Send(context.Background(), m, pCmd{N: 3})
	if err != nil || res != 3 {
		t.Fatal(res, err)
	}
	if got := strings.Join(log, ""); got != nesting([]string{"t1", "A", "t0", "B", "tenant", "inner1", "inner2"}, "handler") {
		t.Fatalf("log = %s", got)
	}
	log = nil
	if _, err := mediator.Send(context.Background(), m, pQuery{}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(log, ""); got != nesting([]string{"A", "q0", "B"}, "qhandler") {
		t.Fatalf("query log = %s", got)
	}
}

func TestPipeline_ErrorHandlerChain(t *testing.T) {
	m := mediator.New()
	var log []string
	mustNil(t, mediator.HandleFunc(m, func(_ context.Context, c pCmd) (int, error) {
		if c.N < 0 {
			return 0, mediator.E(mediator.CodeNotFound, "missing")
		}
		return c.N, nil
	}))
	var seenByH2 mediator.Code
	mustNil(t, mediator.OnError(m, mediator.ErrorHandlerFunc[pCmd, int](func(_ context.Context, c pCmd, err error) (int, bool, error) {
		log = append(log, "h1")
		switch c.N {
		case -1:
			return 100, true, nil // handled, nil out
		case -2:
			return 0, false, mediator.E(mediator.CodeConflict, "translated") // unhandled, replacement
		}
		return 0, false, nil // unhandled, unchanged
	})))
	mustNil(t, mediator.OnError(m, mediator.ErrorHandlerFunc[pCmd, int](func(_ context.Context, c pCmd, err error) (int, bool, error) {
		log = append(log, "h2")
		seenByH2 = mediator.CodeOf(err)
		switch c.N {
		case -2:
			return 200, true, mediator.E(mediator.CodeForbidden, "final") // handled with translated out
		case -3:
			return 0, false, mediator.E(mediator.CodePrecondition, "replaced")
		}
		return 0, false, nil
	})))
	mustNil(t, mediator.OnError(m, mediator.ErrorHandlerFunc[pCmd, int](func(context.Context, pCmd, error) (int, bool, error) {
		log = append(log, "h3")
		return 0, false, nil
	})))
	mustNil(t, mediator.Use(m, rec{name: "outer", log: &log}, mediator.Where(func(i *mediator.RequestInfo) bool { return true })))
	build(t, m)

	cases := []struct {
		n       int
		wantRes int
		wantErr mediator.Code
		wantLog string
		h2Sees  mediator.Code
	}{
		{5, 5, "", "outer><outer", ""},
		{-1, 100, "", "outer>h1<outer", ""},
		{-2, 0, mediator.CodeForbidden, "outer>h1h2<outer", mediator.CodeConflict}, // handled with an error: Send returns the zero value
		{-3, 0, mediator.CodePrecondition, "outer>h1h2h3<outer", mediator.CodeNotFound},
		{-4, 0, mediator.CodeNotFound, "outer>h1h2h3<outer", mediator.CodeNotFound},
	}
	for _, c := range cases {
		log, seenByH2 = nil, ""
		res, err := mediator.Send(context.Background(), m, pCmd{N: c.n})
		if res != c.wantRes || mediator.CodeOf(err) != c.wantErr {
			t.Errorf("N=%d: got (%d, %v), want (%d, %s)", c.n, res, err, c.wantRes, c.wantErr)
		}
		if got := strings.Join(log, ""); got != c.wantLog {
			t.Errorf("N=%d: log = %s, want %s", c.n, got, c.wantLog)
		}
		if seenByH2 != c.h2Sees {
			t.Errorf("N=%d: h2 saw %s, want %s", c.n, seenByH2, c.h2Sees)
		}
	}
	// Errors from outer behaviors never reach error handlers.
	m2 := mediator.New()
	log = nil
	mustNil(t, mediator.HandleFunc(m2, func(context.Context, pCmd) (int, error) { return 1, nil }))
	mustNil(t, mediator.OnError(m2, mediator.ErrorHandlerFunc[pCmd, int](func(context.Context, pCmd, error) (int, bool, error) {
		log = append(log, "h")
		return 9, true, nil
	})))
	mustNil(t, mediator.Use(m2, rec{name: "failing", log: &log, mode: "error"}))
	build(t, m2)
	if _, err := mediator.Send(context.Background(), m2, pCmd{}); err == nil || strings.Join(log, "") != "failing>" {
		t.Fatalf("outer error reached handlers: err=%v log=%v", err, log)
	}
}

func TestPipeline_PreAndPostProcessors(t *testing.T) {
	m := mediator.New()
	var log []string
	mustNil(t, mediator.HandleFunc(m, func(_ context.Context, c pCmd) (int, error) {
		log = append(log, "handler")
		if c.N == 3 {
			return 0, errors.New("handler failed")
		}
		return c.N * 10, nil
	}))
	mustNil(t, mediator.Pre(m, mediator.PreProcessorFunc[pCmd, int](func(_ context.Context, c pCmd) error {
		log = append(log, "pre1")
		if c.N == 1 {
			return errors.New("pre1 failed")
		}
		return nil
	})))
	mustNil(t, mediator.Pre(m, mediator.PreProcessorFunc[pCmd, int](func(context.Context, pCmd) error {
		log = append(log, "pre2")
		return nil
	})))
	mustNil(t, mediator.Post(m, mediator.PostProcessorFunc[pCmd, int](func(_ context.Context, c pCmd, res int) error {
		log = append(log, "post1")
		if res != c.N*10 {
			return errors.New("post saw the wrong result")
		}
		if c.N == 2 {
			return errors.New("post1 failed")
		}
		return nil
	})))
	mustNil(t, mediator.Post(m, mediator.PostProcessorFunc[pCmd, int](func(context.Context, pCmd, int) error {
		log = append(log, "post2")
		return nil
	})))
	build(t, m)
	cases := []struct {
		n       int
		wantRes int
		wantErr string
		wantLog string
	}{
		{0, 0, "", "pre1,pre2,handler,post1,post2"},
		{4, 40, "", "pre1,pre2,handler,post1,post2"},
		{1, 0, "pre1 failed", "pre1"},
		{2, 0, "post1 failed", "pre1,pre2,handler,post1"},
		{3, 0, "handler failed", "pre1,pre2,handler"},
	}
	for _, c := range cases {
		log = nil
		res, err := mediator.Send(context.Background(), m, pCmd{N: c.n})
		if res != c.wantRes || (err == nil) != (c.wantErr == "") || (err != nil && err.Error() != c.wantErr) {
			t.Errorf("N=%d: got (%d, %v)", c.n, res, err)
		}
		if got := strings.Join(log, ","); got != c.wantLog {
			t.Errorf("N=%d: log = %s, want %s", c.n, got, c.wantLog)
		}
	}
}

func TestPipeline_BehaviorFuncAndUseErrors(t *testing.T) {
	m := mediator.New()
	bf := mediator.BehaviorFunc{N: "bf", F: func(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
		return next(ctx, req)
	}}
	if bf.Name() != "bf" {
		t.Fatal("Name")
	}
	wantContains(t, mediator.Use(m, mediator.BehaviorFunc{N: ""}), "empty name")
	mustNil(t, mediator.Use(m, bf))
	wantContains(t, mediator.Use(m, bf), "already registered")
	mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { return 5, nil }))
	build(t, m)
	res, err := bf.Handle(context.Background(), nil, nil, func(context.Context, any) (any, error) { return 1, nil })
	if err != nil || res != 1 {
		t.Fatal(res, err)
	}
	if res, err := mediator.Send(context.Background(), m, pCmd{}); err != nil || res != 5 {
		t.Fatal(res, err)
	}
}
