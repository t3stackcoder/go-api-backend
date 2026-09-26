package mediator_test

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type createOrder struct {
	mediator.Command[createOrderResult]
	CustomerID string `json:"customerId"`
}

type createOrderResult struct {
	OrderID string `json:"orderId"`
}

type getOrder struct {
	mediator.Query[orderView]
	OrderID string `json:"orderId"`
}

type orderView struct {
	OrderID string `json:"orderId"`
}

type ping struct {
	mediator.Command[mediator.Void]
}

type countTo struct {
	mediator.StreamQuery[int]
	N int `json:"n"`
}

type orderCreated struct {
	mediator.Event
	OrderID string `json:"orderId"`
}

type createOrderHandler struct{ calls int }

func (h *createOrderHandler) Handle(_ context.Context, c createOrder) (createOrderResult, error) {
	h.calls++
	return createOrderResult{OrderID: "o-" + c.CustomerID}, nil
}

type recorder struct {
	name  string
	log   *[]string
	short bool
	panic bool
}

func (r recorder) Name() string { return r.name }
func (r recorder) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	*r.log = append(*r.log, r.name+">")
	if r.panic {
		panic("boom in " + r.name)
	}
	if r.short {
		return nil, mediator.E(mediator.CodeForbidden, "short-circuit by "+r.name)
	}
	res, err := next(ctx, req)
	*r.log = append(*r.log, "<"+r.name)
	return res, err
}

func build(t *testing.T, m *mediator.Mediator) {
	t.Helper()
	if err := m.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}
}

func TestSend_InferenceAndTypedResponse(t *testing.T) {
	m := mediator.New()
	h := &createOrderHandler{}
	if err := mediator.Handle(m, h); err != nil {
		t.Fatal(err)
	}
	if err := mediator.HandleFunc(m, func(_ context.Context, q getOrder) (orderView, error) {
		return orderView{OrderID: q.OrderID}, nil
	}); err != nil {
		t.Fatal(err)
	}
	mediator.MustHandle(m, mediator.HandlerFunc[ping, mediator.Void](func(context.Context, ping) (mediator.Void, error) {
		return mediator.Void{}, nil
	}))
	build(t, m)

	res, err := mediator.Send(context.Background(), m, createOrder{CustomerID: "c1"})
	if err != nil || res.OrderID != "o-c1" {
		t.Fatalf("got %+v, %v", res, err)
	}
	// Pointer requests are dereferenced.
	res, err = mediator.Send(context.Background(), m, &createOrder{CustomerID: "c2"})
	if err != nil || res.OrderID != "o-c2" {
		t.Fatalf("pointer: got %+v, %v", res, err)
	}
	if h.calls != 2 {
		t.Fatalf("handler ran %d times", h.calls)
	}
	view, err := mediator.Send(context.Background(), m, getOrder{OrderID: "x"})
	if err != nil || view.OrderID != "x" {
		t.Fatalf("query: %+v %v", view, err)
	}
	if _, err := mediator.Send(context.Background(), m, ping{}); err != nil {
		t.Fatal(err)
	}
}

func TestSend_UnregisteredAndNotBuilt(t *testing.T) {
	m := mediator.New()
	if _, err := mediator.Send(context.Background(), m, ping{}); !errors.Is(err, mediator.ErrNotBuilt) {
		t.Fatalf("want ErrNotBuilt, got %v", err)
	}
	build(t, m)
	_, err := mediator.Send(context.Background(), m, ping{})
	if !errors.Is(err, mediator.ErrHandlerNotFound) || mediator.CodeOf(err) != mediator.CodeHandlerNotFound {
		t.Fatalf("want ErrHandlerNotFound, got %v", err)
	}
	if err := mediator.HandleFunc(m, func(context.Context, ping) (mediator.Void, error) { return mediator.Void{}, nil }); !errors.Is(err, mediator.ErrAlreadyBuilt) {
		t.Fatalf("want ErrAlreadyBuilt, got %v", err)
	}
}

func TestPipeline_OrderAndPlacement(t *testing.T) {
	m := mediator.New()
	var log []string
	mediator.MustHandle(m, mediator.HandlerFunc[ping, mediator.Void](func(context.Context, ping) (mediator.Void, error) {
		log = append(log, "handler")
		return mediator.Void{}, nil
	}))
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(mediator.Use(m, recorder{name: "A", log: &log}))
	must(mediator.Use(m, recorder{name: "B", log: &log}))
	must(mediator.Use(m, recorder{name: "C", log: &log}))
	must(mediator.Use(m, recorder{name: "afterA1", log: &log}, mediator.After("A")))
	must(mediator.Use(m, recorder{name: "afterA2", log: &log}, mediator.After("A")))
	must(mediator.Use(m, recorder{name: "beforeC", log: &log}, mediator.Before("C")))
	must(mediator.Use(m, recorder{name: "queriesOnly", log: &log}, mediator.Queries()))
	must(mediator.Use(m, recorder{name: "early", log: &log}, mediator.Before("A")))
	build(t, m)

	want := []string{"early", "A", "afterA1", "afterA2", "B", "beforeC", "C"}
	if got := m.ChainFor(reflect.TypeFor[ping]()); !reflect.DeepEqual(got, want) {
		t.Fatalf("chain = %v, want %v", got, want)
	}
	if _, err := mediator.Send(context.Background(), m, ping{}); err != nil {
		t.Fatal(err)
	}
	wantLog := "early>A>afterA1>afterA2>B>beforeC>C>handler<C<beforeC<B<afterA2<afterA1<A<early"
	if got := strings.Join(log, ""); got != wantLog {
		t.Fatalf("log = %s\nwant %s", got, wantLog)
	}
}

func TestPipeline_PositionErrors(t *testing.T) {
	m := mediator.New()
	var log []string
	_ = mediator.Use(m, recorder{name: "A", log: &log}, mediator.After("missing"))
	_ = mediator.Use(m, recorder{name: "B", log: &log}, mediator.After("C"))
	_ = mediator.Use(m, recorder{name: "C", log: &log}, mediator.After("B"))
	err := m.Build()
	if err == nil {
		t.Fatal("want build error")
	}
	if !strings.Contains(err.Error(), "unknown behavior") {
		t.Fatalf("want unknown anchor error, got %v", err)
	}

	m2 := mediator.New()
	_ = mediator.Use(m2, recorder{name: "B", log: &log}, mediator.After("C"))
	_ = mediator.Use(m2, recorder{name: "C", log: &log}, mediator.After("B"))
	if err := m2.Build(); err == nil || !strings.Contains(err.Error(), "cyclic") {
		t.Fatalf("want cycle error, got %v", err)
	}

	m3 := mediator.New()
	_ = mediator.Use(m3, recorder{name: "A", log: &log})
	_ = mediator.Use(m3, recorder{name: "B", log: &log})
	_ = mediator.Use(m3, recorder{name: "X", log: &log}, mediator.After("B"), mediator.Before("A"))
	if err := m3.Build(); err == nil || !strings.Contains(err.Error(), "cannot be both") {
		t.Fatalf("want contradiction error, got %v", err)
	}

	m4 := mediator.New()
	_ = mediator.Use(m4, recorder{name: "A", log: &log})
	if err := mediator.Use(m4, recorder{name: "A", log: &log}); err == nil {
		t.Fatal("want duplicate name error")
	}
}

func TestPipeline_ShortCircuitAndPanic(t *testing.T) {
	m := mediator.New()
	var log []string
	called := false
	mediator.MustHandle(m, mediator.HandlerFunc[ping, mediator.Void](func(context.Context, ping) (mediator.Void, error) {
		called = true
		return mediator.Void{}, nil
	}))
	_ = mediator.Use(m, recorder{name: "A", log: &log})
	_ = mediator.Use(m, recorder{name: "S", log: &log, short: true})
	_ = mediator.Use(m, recorder{name: "B", log: &log})
	build(t, m)
	_, err := mediator.Send(context.Background(), m, ping{})
	if mediator.CodeOf(err) != mediator.CodeForbidden || called {
		t.Fatalf("short-circuit failed: err=%v called=%v log=%v", err, called, log)
	}
	if strings.Join(log, "") != "A>S><A" {
		t.Fatalf("log = %v", log)
	}

	m2 := mediator.New()
	mediator.MustHandle(m2, mediator.HandlerFunc[ping, mediator.Void](func(context.Context, ping) (mediator.Void, error) {
		panic("handler exploded")
	}))
	build(t, m2)
	_, err = mediator.Send(context.Background(), m2, ping{})
	var pe *mediator.PanicError
	if mediator.CodeOf(err) != mediator.CodeInternal || !errors.As(err, &pe) || pe.Value != "handler exploded" {
		t.Fatalf("want recovered panic, got %v", err)
	}
}

func TestSend_NestedScope(t *testing.T) {
	m := mediator.New()
	var innerCorr, outerCorr string
	var depth int
	mediator.MustHandle(m, mediator.HandlerFunc[getOrder, orderView](func(ctx context.Context, q getOrder) (orderView, error) {
		innerCorr = mediator.CorrelationID(ctx)
		depth = mediator.Depth(ctx)
		if mediator.CausationID(ctx) == mediator.RequestID(ctx) {
			t.Error("causation must differ from request ID")
		}
		return orderView{OrderID: q.OrderID}, nil
	}))
	mediator.MustHandle(m, mediator.HandlerFunc[createOrder, createOrderResult](func(ctx context.Context, c createOrder) (createOrderResult, error) {
		outerCorr = mediator.CorrelationID(ctx)
		v, err := mediator.Send(ctx, m, getOrder{OrderID: c.CustomerID})
		return createOrderResult(v), err
	}))
	build(t, m)
	ctx := mediator.WithCorrelationID(context.Background(), "corr-1")
	if _, err := mediator.Send(ctx, m, createOrder{CustomerID: "z"}); err != nil {
		t.Fatal(err)
	}
	if outerCorr != "corr-1" || innerCorr != "corr-1" || depth != 2 {
		t.Fatalf("outer=%s inner=%s depth=%d", outerCorr, innerCorr, depth)
	}

	// Depth cap.
	m2 := mediator.New(mediator.WithMaxDepth(3))
	mediator.MustHandle(m2, mediator.HandlerFunc[ping, mediator.Void](func(ctx context.Context, p ping) (mediator.Void, error) {
		return mediator.Send(ctx, m2, ping{})
	}))
	build(t, m2)
	if _, err := mediator.Send(context.Background(), m2, ping{}); !errors.Is(err, mediator.ErrDepthExceeded) {
		t.Fatalf("want depth error, got %v", err)
	}
}

func TestPublish_Strategies(t *testing.T) {
	for _, tc := range []struct {
		strategy mediator.PublishStrategy
		wantRuns int
		wantErrs int
	}{
		{mediator.StopOnFirstError, 2, 1},
		{mediator.ContinueOnError, 3, 2},
		{mediator.Parallel, 3, 2},
	} {
		m := mediator.New(mediator.WithPublishStrategy(tc.strategy))
		runs := 0
		var mu = make(chan struct{}, 1)
		count := func() {
			mu <- struct{}{}
			runs++
			<-mu
		}
		_ = mediator.OnFunc(m, func(context.Context, orderCreated) error { count(); return nil })
		_ = mediator.OnFunc(m, func(context.Context, orderCreated) error { count(); return errors.New("h2") })
		_ = mediator.OnFunc(m, func(context.Context, orderCreated) error { count(); return errors.New("h3") })
		build(t, m)
		err := mediator.Publish(context.Background(), m, orderCreated{OrderID: "1"})
		if err == nil {
			t.Fatalf("%v: want error", tc.strategy)
		}
		if runs != tc.wantRuns {
			t.Fatalf("%v: runs = %d, want %d", tc.strategy, runs, tc.wantRuns)
		}
		n := 0
		for _, s := range []string{"h2", "h3"} {
			if strings.Contains(err.Error(), s) {
				n++
			}
		}
		if n != tc.wantErrs {
			t.Fatalf("%v: err = %v", tc.strategy, err)
		}
	}
}

func TestPublish_UnregisteredEventIsNoop(t *testing.T) {
	m := mediator.New()
	build(t, m)
	if err := mediator.Publish(context.Background(), m, orderCreated{}); err != nil {
		t.Fatal(err)
	}
}

func TestStream_Basic(t *testing.T) {
	m := mediator.New()
	var log []string
	_ = mediator.Use(m, recorder{name: "A", log: &log})
	if err := mediator.HandleStreamFunc(m, func(ctx context.Context, q countTo) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			for i := 1; i <= q.N; i++ {
				if !yield(i, nil) {
					return
				}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	build(t, m)
	var got []int
	for v, err := range mediator.Stream(context.Background(), m, countTo{N: 3}) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, v)
	}
	if !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Fatalf("got %v", got)
	}
	if strings.Join(log, "") != "A><A" {
		t.Fatalf("behaviors ran once at call time, log=%v", log)
	}
	// Early break stops the sequence.
	n := 0
	for range mediator.Stream(context.Background(), m, countTo{N: 100}) {
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 {
		t.Fatalf("break: n=%d", n)
	}
	// Canceled context terminates with a timeout error.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var last error
	for _, err := range mediator.Stream(ctx, m, countTo{N: 100}) {
		last = err
	}
	if mediator.CodeOf(last) != mediator.CodeTimeout {
		t.Fatalf("want timeout, got %v", last)
	}
}

func TestTyped_ProcessorsAndErrorHandlers(t *testing.T) {
	m := mediator.New()
	var log []string
	mediator.MustHandle(m, mediator.HandlerFunc[createOrder, createOrderResult](func(_ context.Context, c createOrder) (createOrderResult, error) {
		log = append(log, "handler")
		if c.CustomerID == "bad" {
			return createOrderResult{}, mediator.E(mediator.CodeNotFound, "nope")
		}
		return createOrderResult{OrderID: "ok"}, nil
	}))
	_ = mediator.Use(m, recorder{name: "U", log: &log})
	_ = mediator.UseFor(m, mediator.TypedBehaviorFunc[createOrder, createOrderResult](func(ctx context.Context, c createOrder, next func(context.Context, createOrder) (createOrderResult, error)) (createOrderResult, error) {
		log = append(log, "typed>")
		r, err := next(ctx, c)
		log = append(log, "<typed")
		return r, err
	}))
	_ = mediator.Pre(m, mediator.PreProcessorFunc[createOrder, createOrderResult](func(context.Context, createOrder) error {
		log = append(log, "pre")
		return nil
	}))
	_ = mediator.Post(m, mediator.PostProcessorFunc[createOrder, createOrderResult](func(context.Context, createOrder, createOrderResult) error {
		log = append(log, "post")
		return nil
	}))
	_ = mediator.OnError(m, mediator.ErrorHandlerFunc[createOrder, createOrderResult](func(_ context.Context, _ createOrder, err error) (createOrderResult, bool, error) {
		log = append(log, "onerror")
		if mediator.CodeOf(err) == mediator.CodeNotFound {
			return createOrderResult{OrderID: "recovered"}, true, nil
		}
		return createOrderResult{}, false, nil
	}))
	build(t, m)
	res, err := mediator.Send(context.Background(), m, createOrder{CustomerID: "good"})
	if err != nil || res.OrderID != "ok" {
		t.Fatal(res, err)
	}
	if got := strings.Join(log, ","); got != "U>,typed>,pre,handler,post,<typed,<U" {
		t.Fatalf("order: %s", got)
	}
	log = nil
	res, err = mediator.Send(context.Background(), m, createOrder{CustomerID: "bad"})
	if err != nil || res.OrderID != "recovered" {
		t.Fatal(res, err)
	}
	if got := strings.Join(log, ","); got != "U>,typed>,pre,handler,<typed,onerror,<U" {
		t.Fatalf("order: %s", got)
	}
}

func TestBuild_ReportsAllProblems(t *testing.T) {
	type badName struct {
		mediator.Command[mediator.Void]
	}
	type chanResp struct {
		mediator.Query[chan int]
	}
	m := mediator.New()
	_ = mediator.HandleFunc(m, func(context.Context, chanResp) (chan int, error) { return nil, nil })
	_ = mediator.Use(m, recorder{name: "A"}, mediator.After("nope"))
	err := m.Build()
	if err == nil {
		t.Fatal("want error")
	}
	for _, want := range []string{"round-trip", "unknown behavior"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in %v", want, err)
		}
	}
	_ = badName{}
}

func TestCanonicalJSON(t *testing.T) {
	a, err := mediator.CanonicalizeJSON([]byte(`{"b": [1.50, 2, {"z":null,"a":"x"}], "a": 1e2, "c": true}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != `{"a":100,"b":[1.5,2,{"a":"x","z":null}],"c":true}` {
		t.Fatalf("got %s", a)
	}
	b, err := mediator.CanonicalizeJSON([]byte(`{"c":true,"a":100.0,"b":[1.5,2,{"a":"x","z":null}]}`))
	if err != nil || string(a) != string(b) {
		t.Fatalf("permuted: %s %v", b, err)
	}
	if _, err := mediator.CanonicalizeJSON([]byte(`{"a":1,"a":2}`)); err == nil {
		t.Fatal("want duplicate error")
	}
	h1, _ := mediator.CanonicalHash(map[string]any{"x": 1, "y": []int{1, 2}})
	h2, _ := mediator.CanonicalHash(map[string]any{"y": []int{1, 2}, "x": 1.0})
	if h1 != h2 {
		t.Fatal("hash differs for equal values")
	}
}

func TestNames(t *testing.T) {
	m := mediator.New()
	mediator.MustHandle(m, mediator.HandlerFunc[ping, mediator.Void](func(context.Context, ping) (mediator.Void, error) { return mediator.Void{}, nil }))
	_ = mediator.OnFunc(m, func(context.Context, orderCreated) error { return nil })
	build(t, m)
	names := m.Names()
	if len(names) != 2 || names[0].Name != "ping" || names[1].Name != "orderCreated" || names[1].Topic != "orderCreated" {
		t.Fatalf("names = %+v", names)
	}
	info, ok := m.Lookup("ping")
	if !ok || info.Kind != mediator.KindCommand {
		t.Fatal("lookup failed")
	}
}

func TestPartition_Deterministic(t *testing.T) {
	first, again := mediator.Partition("k", 16), mediator.Partition("k", 16)
	if first != again {
		t.Fatal("unstable")
	}
	if mediator.Partition("k", 1) != 0 || mediator.Partition("k", 0) != 0 {
		t.Fatal("p<=1 must be 0")
	}
	counts := make([]int, 8)
	for i := 0; i < 8000; i++ {
		counts[mediator.Partition(strings.Repeat("x", i%50)+string(rune('a'+i%26))+string(rune(i)), 8)]++
	}
	for p, c := range counts {
		if c > 3000 {
			t.Fatalf("partition %d holds %d of 8000", p, c)
		}
	}
}

func BenchmarkSend_Core(b *testing.B) {
	m := mediator.New()
	mediator.MustHandle(m, mediator.HandlerFunc[createOrder, createOrderResult](func(_ context.Context, c createOrder) (createOrderResult, error) {
		return createOrderResult{OrderID: c.CustomerID}, nil
	}))
	if err := m.Build(); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	req := createOrder{CustomerID: "c"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mediator.Send(ctx, m, req); err != nil {
			b.Fatal(err)
		}
	}
}
