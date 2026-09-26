package mediator_test

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type iter1 = iter.Seq2[int, error]

type stQuery struct {
	mediator.StreamQuery[int]
	N       int `json:"n"`
	FailAt  int `json:"failAt"`
	PanicAt int `json:"panicAt"`
}

type stHandler struct {
	started int
	broke   bool
}

func (h *stHandler) Handle(_ context.Context, q stQuery) iter.Seq2[int, error] {
	if q.N < 0 {
		return nil
	}
	return func(yield func(int, error) bool) {
		h.started++
		for i := 1; i <= q.N; i++ {
			if i == q.FailAt {
				yield(0, mediator.E(mediator.CodeNotFound, "fail"))
				return
			}
			if i == q.PanicAt {
				panic("iterator boom")
			}
			if !yield(i, nil) {
				h.broke = true
				return
			}
		}
	}
}

// streamRec implements StreamBehavior and records what it observes.
type streamRec struct {
	name     string
	log      *[]string
	mode     string // "", "callErr", "callPanic", "iterPanic"
	items    int
	terminal error
}

func (s *streamRec) Name() string { return s.name }

func (s *streamRec) Handle(ctx context.Context, req any, _ *mediator.RequestInfo, next mediator.Next) (any, error) {
	*s.log = append(*s.log, s.name+">handle")
	return next(ctx, req)
}

func (s *streamRec) HandleStream(ctx context.Context, req any, _ *mediator.RequestInfo, next mediator.StreamNext) iter.Seq2[any, error] {
	*s.log = append(*s.log, s.name+">")
	switch s.mode {
	case "callErr":
		return func(yield func(any, error) bool) { yield(nil, mediator.E(mediator.CodeForbidden, "denied")) }
	case "callPanic":
		panic("stream boom")
	}
	inner := next(ctx, req)
	return func(yield func(any, error) bool) {
		n := 0
		for v, err := range inner {
			if err != nil {
				s.terminal = err
				yield(nil, err)
				return
			}
			n++
			if s.mode == "iterPanic" && n == 2 {
				panic("iter boom in " + s.name)
			}
			if !yield(v, nil) {
				break
			}
		}
		s.items = n
		*s.log = append(*s.log, "<"+s.name)
	}
}

func collect(seq iter.Seq2[int, error]) ([]int, error) {
	var out []int
	for v, err := range seq {
		if err != nil {
			return out, err
		}
		out = append(out, v)
	}
	return out, nil
}

func TestStream_HandlerShapes(t *testing.T) {
	m := mediator.New()
	h := &stHandler{}
	mustNil(t, mediator.HandleStream(m, h))
	build(t, m)
	ctx := context.Background()
	cases := []struct {
		name string
		q    stQuery
		want []int
		code mediator.Code
	}{
		{"nil sequence", stQuery{N: -1}, nil, ""},
		{"empty", stQuery{N: 0}, nil, ""},
		{"one", stQuery{N: 1}, []int{1}, ""},
		{"many", stQuery{N: 3}, []int{1, 2, 3}, ""},
		{"error terminates", stQuery{N: 5, FailAt: 3}, []int{1, 2}, mediator.CodeNotFound},
		{"error first", stQuery{N: 5, FailAt: 1}, nil, mediator.CodeNotFound},
		{"panic in body", stQuery{N: 5, PanicAt: 2}, []int{1}, mediator.CodeInternal},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := collect(mediator.Stream(ctx, m, c.q))
			if !reflect.DeepEqual(got, c.want) || mediator.CodeOf(err) != c.code {
				t.Fatalf("got %v %v", got, err)
			}
			if c.code == mediator.CodeInternal {
				assertPanicErr(t, err, "iterator boom")
			}
		})
	}
	t.Run("early break is observed by the handler", func(t *testing.T) {
		h.broke = false
		n := 0
		for range mediator.Stream(ctx, m, stQuery{N: 100}) {
			n++
			if n == 2 {
				break
			}
		}
		if n != 2 || !h.broke {
			t.Fatalf("n=%d broke=%v", n, h.broke)
		}
	})
	t.Run("pointer request", func(t *testing.T) {
		got, err := collect(mediator.Stream(ctx, m, &stQuery{N: 2}))
		if err != nil || !reflect.DeepEqual(got, []int{1, 2}) {
			t.Fatal(got, err)
		}
	})
}

func TestStream_Boundaries(t *testing.T) {
	m := mediator.New()
	h := &stHandler{}
	mustNil(t, mediator.HandleStream(m, h))
	mustNil(t, mediator.HandleFunc(m, func(ctx context.Context, c pCmd) (int, error) {
		_, err := collect(mediator.Stream(ctx, m, stQuery{N: 1}))
		return 0, err
	}))
	ctx := context.Background()
	single := func(seq iter.Seq2[any, error]) error {
		var errs []error
		n := 0
		for _, err := range seq {
			n++
			errs = append(errs, err)
		}
		if n != 1 || errs[0] == nil {
			t.Fatalf("want exactly one error element, got %d: %v", n, errs)
		}
		return errs[0]
	}
	if !errors.Is(single(m.StreamAny(ctx, stQuery{})), mediator.ErrNotBuilt) {
		t.Fatal("not built")
	}
	build(t, m)
	if mediator.CodeOf(single(m.StreamAny(ctx, nil))) != mediator.CodeBadRequest {
		t.Fatal("nil")
	}
	if mediator.CodeOf(single(m.StreamAny(ctx, (*stQuery)(nil)))) != mediator.CodeBadRequest {
		t.Fatal("nil pointer")
	}
	if err := single(m.StreamAny(ctx, bStream{})); !errors.Is(err, mediator.ErrHandlerNotFound) {
		t.Fatalf("unregistered: %v", err)
	}
	var last error
	for _, err := range mediator.Stream(ctx, m, (*stQuery)(nil)) {
		last = err
	}
	if mediator.CodeOf(last) != mediator.CodeBadRequest {
		t.Fatal("typed nil pointer")
	}

	// Expired deadline: the first item is produced, then the guard reports the
	// cancellation instead of delivering it.
	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	got, err := collect(mediator.Stream(expired, m, stQuery{N: 3}))
	if len(got) != 0 || mediator.CodeOf(err) != mediator.CodeTimeout || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired: %v %v", got, err)
	}
	// Canceled mid-way.
	cctx, ccancel := context.WithCancel(ctx)
	defer ccancel()
	got = nil
	for v, err := range mediator.Stream(cctx, m, stQuery{N: 10}) {
		if err != nil {
			last = err
			break
		}
		got = append(got, v)
		if v == 2 {
			ccancel()
		}
	}
	if !reflect.DeepEqual(got, []int{1, 2}) || !errors.Is(last, context.Canceled) {
		t.Fatalf("canceled: %v %v", got, last)
	}

	// Depth cap applies to Stream too.
	m2 := mediator.New(mediator.WithMaxDepth(1))
	mustNil(t, mediator.HandleStream(m2, h))
	mustNil(t, mediator.HandleFunc(m2, func(ctx context.Context, c pCmd) (int, error) {
		_, err := collect(mediator.Stream(ctx, m2, stQuery{N: 1}))
		return 0, err
	}))
	build(t, m2)
	if _, err := mediator.Send(ctx, m2, pCmd{}); !errors.Is(err, mediator.ErrDepthExceeded) {
		t.Fatalf("depth: %v", err)
	}
	// And scope values are visible inside the stream handler.
	m3 := mediator.New()
	var depth int
	var corr string
	mustNil(t, mediator.HandleStreamFunc(m3, func(ctx context.Context, q stQuery) iter.Seq2[int, error] {
		depth, corr = mediator.Depth(ctx), mediator.CorrelationID(ctx)
		return nil
	}))
	build(t, m3)
	if _, err := collect(mediator.Stream(mediator.WithCorrelationID(ctx, "c9"), m3, stQuery{})); err != nil || depth != 1 || corr != "c9" {
		t.Fatalf("scope: depth=%d corr=%s err=%v", depth, corr, err)
	}
}

// substitute is a plain behavior on a stream that replaces the sequence.
type substitute struct {
	seq iter.Seq2[any, error]
}

func (substitute) Name() string { return "substitute" }
func (s substitute) Handle(context.Context, any, *mediator.RequestInfo, mediator.Next) (any, error) {
	return s.seq, nil
}

func TestStream_ItemTypeMismatch(t *testing.T) {
	newM := func(seq iter.Seq2[any, error]) *mediator.Mediator {
		m := mediator.New()
		mustNil(t, mediator.HandleStream(m, &stHandler{}))
		mustNil(t, mediator.Use(m, substitute{seq: seq}))
		build(t, m)
		return m
	}
	t.Run("wrong item type", func(t *testing.T) {
		m := newM(func(yield func(any, error) bool) { yield(1, nil); yield("two", nil); yield(3, nil) })
		got, err := collect(mediator.Stream(context.Background(), m, stQuery{}))
		if !reflect.DeepEqual(got, []int{1}) || mediator.CodeOf(err) != mediator.CodeInternal || !strings.Contains(err.Error(), "stream yielded string, want int") {
			t.Fatal(got, err)
		}
	})
	t.Run("nil item is the zero value", func(t *testing.T) {
		m := newM(func(yield func(any, error) bool) { yield(nil, nil); yield(2, nil) })
		got, err := collect(mediator.Stream(context.Background(), m, stQuery{}))
		if err != nil || !reflect.DeepEqual(got, []int{0, 2}) {
			t.Fatal(got, err)
		}
	})
	t.Run("consumer stops early", func(t *testing.T) {
		yielded := 0
		m := newM(func(yield func(any, error) bool) {
			for i := 1; i <= 5; i++ {
				yielded++
				if !yield(i, nil) {
					return
				}
			}
		})
		for v := range mediator.Stream(context.Background(), m, stQuery{}) {
			if v == 2 {
				break
			}
		}
		if yielded != 2 {
			t.Fatalf("yielded %d", yielded)
		}
	})
}

func TestStream_Behaviors(t *testing.T) {
	ctx := context.Background()
	t.Run("stream behavior wraps items and a plain behavior only sees the call", func(t *testing.T) {
		m := mediator.New()
		var log []string
		h := &stHandler{}
		mustNil(t, mediator.HandleStream(m, h))
		mustNil(t, mediator.Use(m, rec{name: "P", log: &log}))
		s := &streamRec{name: "S", log: &log}
		mustNil(t, mediator.Use(m, s))
		build(t, m)
		seq := mediator.Stream(ctx, m, stQuery{N: 3})
		if got := strings.Join(log, ","); got != "P>,S>,<P" {
			t.Fatalf("call-time log = %s", got)
		}
		got, err := collect(seq)
		if err != nil || !reflect.DeepEqual(got, []int{1, 2, 3}) || s.items != 3 || s.terminal != nil {
			t.Fatalf("got %v %v items=%d terminal=%v", got, err, s.items, s.terminal)
		}
		if got := strings.Join(log, ","); got != "P>,S>,<P,<S" {
			t.Fatalf("log = %s", got)
		}
		if got := m.ChainFor(reflect.TypeFor[stQuery]()); !reflect.DeepEqual(got, []string{"P", "S"}) {
			t.Fatalf("chain = %v", got)
		}
	})
	t.Run("stream behavior observes the terminal error", func(t *testing.T) {
		m := mediator.New()
		var log []string
		mustNil(t, mediator.HandleStream(m, &stHandler{}))
		s := &streamRec{name: "S", log: &log}
		mustNil(t, mediator.Use(m, s))
		build(t, m)
		got, err := collect(mediator.Stream(ctx, m, stQuery{N: 5, FailAt: 3}))
		if !reflect.DeepEqual(got, []int{1, 2}) || mediator.CodeOf(err) != mediator.CodeNotFound || mediator.CodeOf(s.terminal) != mediator.CodeNotFound {
			t.Fatalf("got %v %v terminal=%v", got, err, s.terminal)
		}
	})
	t.Run("stream behavior erroring before next yields a single error", func(t *testing.T) {
		m := mediator.New()
		h := &stHandler{}
		mustNil(t, mediator.HandleStream(m, h))
		mustNil(t, mediator.Use(m, &streamRec{name: "S", log: new([]string), mode: "callErr"}))
		build(t, m)
		got, err := collect(mediator.Stream(ctx, m, stQuery{N: 3}))
		if len(got) != 0 || mediator.CodeOf(err) != mediator.CodeForbidden || h.started != 0 {
			t.Fatalf("got %v %v started=%d", got, err, h.started)
		}
	})
	t.Run("stream behavior panicking at call time", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleStream(m, &stHandler{}))
		mustNil(t, mediator.Use(m, &streamRec{name: "S", log: new([]string), mode: "callPanic"}))
		build(t, m)
		got, err := collect(mediator.Stream(ctx, m, stQuery{N: 3}))
		if len(got) != 0 {
			t.Fatal(got)
		}
		assertPanicErr(t, err, "stream boom")
	})
	t.Run("stream behavior panicking inside the sequence", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleStream(m, &stHandler{}))
		mustNil(t, mediator.Use(m, &streamRec{name: "S", log: new([]string), mode: "iterPanic"}))
		build(t, m)
		got, err := collect(mediator.Stream(ctx, m, stQuery{N: 3}))
		if !reflect.DeepEqual(got, []int{1}) {
			t.Fatal(got)
		}
		assertPanicErr(t, err, "iter boom in S")
	})
	t.Run("plain behavior failure modes", func(t *testing.T) {
		for _, c := range []struct {
			mode string
			code mediator.Code
			text string
		}{
			{"error", mediator.CodeInternal, "fail in P"},
			{"wrong", mediator.CodeInternal, "behavior P returned string instead of a stream"},
			{"nil", mediator.CodeInternal, "behavior P returned <nil> instead of a stream"},
			{"panic", mediator.CodeInternal, "panic in P"},
		} {
			m := mediator.New()
			h := &stHandler{}
			mustNil(t, mediator.HandleStream(m, h))
			mustNil(t, mediator.Use(m, rec{name: "P", log: new([]string), mode: c.mode}))
			build(t, m)
			got, err := collect(mediator.Stream(ctx, m, stQuery{N: 3}))
			if len(got) != 0 || mediator.CodeOf(err) != c.code || !strings.Contains(err.Error(), c.text) || h.started != 0 {
				t.Fatalf("%s: got %v %v started=%d", c.mode, got, err, h.started)
			}
		}
	})
	t.Run("plain behavior sees the iterator as the response", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleStream(m, &stHandler{}))
		var sawSeq bool
		mustNil(t, mediator.Use(m, mediator.BehaviorFunc{N: "inspect", F: func(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
			if info.Kind != mediator.KindStream || info.Name != "stQuery" {
				t.Errorf("info = %+v", info)
			}
			res, err := next(ctx, req)
			_, sawSeq = res.(iter.Seq2[any, error])
			return res, err
		}}))
		build(t, m)
		got, err := collect(mediator.Stream(ctx, m, stQuery{N: 2}))
		if err != nil || !reflect.DeepEqual(got, []int{1, 2}) || !sawSeq {
			t.Fatal(got, err, sawSeq)
		}
	})
	t.Run("stream behaviors are ordinary behaviors on commands", func(t *testing.T) {
		m := mediator.New()
		var log []string
		mustNil(t, mediator.HandleFunc(m, func(context.Context, pCmd) (int, error) { return 4, nil }))
		mustNil(t, mediator.Use(m, &streamRec{name: "S", log: &log}))
		build(t, m)
		res, err := mediator.Send(ctx, m, pCmd{})
		if err != nil || res != 4 || strings.Join(log, "") != "S>handle" {
			t.Fatal(res, err, log)
		}
	})
}
