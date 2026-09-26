package mediator_test

import (
	"context"
	"errors"
	"iter"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// fakeRemote records what the mediator hands to a RemoteDispatcher and
// returns whatever the test configured.
type fakeRemote struct {
	mu    sync.Mutex
	calls []remoteCall
	res   any
	err   error
}

type remoteCall struct {
	info *mediator.RequestInfo
	req  any
	ctx  context.Context
}

func (f *fakeRemote) Send(ctx context.Context, info *mediator.RequestInfo, req any) (any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, remoteCall{info: info, req: req, ctx: ctx})
	return f.res, f.err
}

type sQuery struct {
	mediator.Query[int]
	N int `json:"n"`
}

type sCmd struct {
	mediator.Command[string]
	S string `json:"s"`
}

type sAnyResp struct{ mediator.Command[any] }

type sStream struct{ mediator.StreamQuery[int] }

func TestSend_Boundaries(t *testing.T) {
	m := mediator.New()
	mustNil(t, mediator.HandleFunc(m, func(ctx context.Context, q sQuery) (int, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return q.N * 2, nil
	}))
	mustNil(t, mediator.HandleFunc(m, func(context.Context, sAnyResp) (any, error) { return nil, nil }))
	mustNil(t, mediator.HandleStreamFunc(m, func(context.Context, sStream) iter.Seq2[int, error] { return nil }))
	build(t, m)
	ctx := context.Background()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	expired, cancel2 := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel2()

	t.Run("value and pointer", func(t *testing.T) {
		for _, req := range []mediator.Request[int]{sQuery{N: 0}, sQuery{N: 1}, &sQuery{N: 21}} {
			got, err := mediator.Send(ctx, m, req)
			if err != nil {
				t.Fatal(err)
			}
			want := 0
			switch r := req.(type) {
			case sQuery:
				want = r.N * 2
			case *sQuery:
				want = r.N * 2
			}
			if got != want {
				t.Fatalf("got %d want %d", got, want)
			}
		}
	})
	t.Run("nil pointer", func(t *testing.T) {
		_, err := mediator.Send(ctx, m, (*sQuery)(nil))
		if mediator.CodeOf(err) != mediator.CodeBadRequest || !strings.Contains(err.Error(), "nil request") {
			t.Fatalf("got %v", err)
		}
		for _, in := range []any{nil, (*sQuery)(nil)} {
			if _, err := m.SendAny(ctx, in); mediator.CodeOf(err) != mediator.CodeBadRequest {
				t.Fatalf("SendAny(%v): %v", in, err)
			}
		}
	})
	t.Run("SendAny with pointer unboxes and dereferences", func(t *testing.T) {
		res, err := m.SendAny(ctx, &sQuery{N: 4})
		if err != nil || res.(int) != 8 {
			t.Fatalf("got %v %v", res, err)
		}
	})
	t.Run("canceled context reaches the handler", func(t *testing.T) {
		_, err := mediator.Send(canceled, m, sQuery{N: 1})
		if !errors.Is(err, context.Canceled) || mediator.CodeOf(err) != mediator.CodeTimeout {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("expired deadline reaches the handler", func(t *testing.T) {
		_, err := mediator.Send(expired, m, sQuery{N: 1})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("stream type through Send", func(t *testing.T) {
		_, err := m.SendAny(ctx, sStream{})
		if mediator.CodeOf(err) != mediator.CodeInternal || !strings.Contains(err.Error(), "use Stream") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("command type through Stream", func(t *testing.T) {
		var got error
		for _, err := range m.StreamAny(ctx, sQuery{}) {
			got = err
		}
		if mediator.CodeOf(got) != mediator.CodeInternal || !strings.Contains(got.Error(), "use Send") {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("nil interface response unboxes to zero", func(t *testing.T) {
		res, err := mediator.Send(ctx, m, sAnyResp{})
		if err != nil || res != nil {
			t.Fatalf("got %v %v", res, err)
		}
	})
	t.Run("unregistered", func(t *testing.T) {
		_, err := mediator.Send(ctx, m, sCmd{})
		if !errors.Is(err, mediator.ErrHandlerNotFound) || !strings.Contains(err.Error(), "sCmd") {
			t.Fatalf("got %v", err)
		}
	})
}

func TestSend_NotBuilt(t *testing.T) {
	m := mediator.New()
	if _, err := m.SendAny(context.Background(), sQuery{}); !errors.Is(err, mediator.ErrNotBuilt) {
		t.Fatal(err)
	}
}

func TestSend_WrongDynamicResponseType(t *testing.T) {
	t.Run("from a behavior", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleFunc(m, func(context.Context, sQuery) (int, error) { return 1, nil }))
		mustNil(t, mediator.Use(m, mediator.BehaviorFunc{N: "wrong", F: func(context.Context, any, *mediator.RequestInfo, mediator.Next) (any, error) {
			return "not an int", nil
		}}))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, sQuery{})
		if mediator.CodeOf(err) != mediator.CodeInternal || !strings.Contains(err.Error(), "handler returned string, want int") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("from a remote dispatcher", func(t *testing.T) {
		rd := &fakeRemote{res: 3.5}
		m := mediator.New(mediator.WithRemote(rd))
		mustNil(t, mediator.Declare[sQuery, int](m))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, sQuery{})
		if mediator.CodeOf(err) != mediator.CodeInternal || !strings.Contains(err.Error(), "float64") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("short-circuit with nil response yields the zero value", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.HandleFunc(m, func(context.Context, sQuery) (int, error) { return 1, nil }))
		mustNil(t, mediator.Use(m, mediator.BehaviorFunc{N: "nil", F: func(context.Context, any, *mediator.RequestInfo, mediator.Next) (any, error) {
			return nil, nil
		}}))
		build(t, m)
		got, err := mediator.Send(context.Background(), m, sQuery{N: 5})
		if err != nil || got != 0 {
			t.Fatalf("got %d %v", got, err)
		}
	})
}

func TestSend_Remote(t *testing.T) {
	t.Run("declared type is dispatched with info and the dereferenced request", func(t *testing.T) {
		rd := &fakeRemote{res: 42}
		m := mediator.New(mediator.WithRemote(rd))
		mustNil(t, mediator.Declare[sQuery, int](m))
		var log []string
		mustNil(t, mediator.Use(m, recorder{name: "A", log: &log}))
		build(t, m)
		got, err := mediator.Send(mediator.WithCorrelationID(context.Background(), "c1"), m, &sQuery{N: 7})
		if err != nil || got != 42 {
			t.Fatalf("got %d %v", got, err)
		}
		if len(rd.calls) != 1 {
			t.Fatalf("calls = %d", len(rd.calls))
		}
		c := rd.calls[0]
		if c.info.Name != "sQuery" || c.info.Local || c.info.Kind != mediator.KindQuery || c.info.ResponseType != reflect.TypeFor[int]() {
			t.Fatalf("info = %+v", c.info)
		}
		if q, ok := c.req.(sQuery); !ok || q.N != 7 {
			t.Fatalf("req = %#v", c.req)
		}
		// The remote call happens inside the Send scope, so the dispatcher can
		// forward correlation and request IDs.
		if mediator.CorrelationID(c.ctx) != "c1" || mediator.RequestID(c.ctx) == uuid.Nil || mediator.Depth(c.ctx) != 1 {
			t.Fatal("remote call must run inside the Send scope")
		}
		// Behaviors do not run for remote dispatch; the remote node runs its own.
		if len(log) != 0 {
			t.Fatalf("behaviors ran locally: %v", log)
		}
		if got := m.ChainFor(reflect.TypeFor[sQuery]()); !reflect.DeepEqual(got, []string{"A"}) {
			t.Fatalf("chain = %v", got)
		}
	})
	t.Run("remote errors propagate", func(t *testing.T) {
		rd := &fakeRemote{err: mediator.E(mediator.CodeUnavailable, "no node")}
		m := mediator.New(mediator.WithRemote(rd))
		mustNil(t, mediator.Declare[sQuery, int](m))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, sQuery{})
		if mediator.CodeOf(err) != mediator.CodeUnavailable {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("without WithRemote a declared type is not found", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.Declare[sQuery, int](m))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, sQuery{})
		if !errors.Is(err, mediator.ErrHandlerNotFound) || !strings.Contains(err.Error(), "remote dispatch is not enabled") {
			t.Fatalf("got %v", err)
		}
	})
}

func TestSend_Scope(t *testing.T) {
	m := mediator.New()
	type seen struct {
		corr  string
		req   uuid.UUID
		cause uuid.UUID
		depth int
	}
	var inner, outer seen
	mustNil(t, mediator.HandleFunc(m, func(ctx context.Context, q sQuery) (int, error) {
		inner = seen{corr: mediator.CorrelationID(ctx), req: mediator.RequestID(ctx), cause: mediator.CausationID(ctx), depth: mediator.Depth(ctx)}
		return q.N, nil
	}))
	mustNil(t, mediator.HandleFunc(m, func(ctx context.Context, c sCmd) (string, error) {
		outer = seen{corr: mediator.CorrelationID(ctx), req: mediator.RequestID(ctx), cause: mediator.CausationID(ctx), depth: mediator.Depth(ctx)}
		switch c.S {
		case "nested":
			_, err := mediator.Send(ctx, m, sQuery{N: 1})
			return "", err
		case "recorrelate":
			_, err := mediator.Send(mediator.WithCorrelationID(ctx, "corr-2"), m, sQuery{N: 1})
			return "", err
		case "recause":
			_, err := mediator.Send(mediator.WithCausation(ctx, "corr-3", uuid.MustParse("11111111-1111-7111-8111-111111111111")), m, sQuery{N: 1})
			return "", err
		}
		return c.S, nil
	}))
	build(t, m)
	ctx := context.Background()

	t.Run("outside a scope everything is zero", func(t *testing.T) {
		if mediator.CorrelationID(ctx) != "" || mediator.RequestID(ctx) != uuid.Nil || mediator.CausationID(ctx) != uuid.Nil || mediator.Depth(ctx) != 0 {
			t.Fatal("zero values expected")
		}
	})
	t.Run("top level generates a correlation ID equal to the request ID", func(t *testing.T) {
		sendOK(t, ctx, m, sCmd{S: "plain"})
		if outer.corr != outer.req.String() || outer.req.Version() != 7 || outer.cause != uuid.Nil || outer.depth != 1 {
			t.Fatalf("outer = %+v", outer)
		}
	})
	t.Run("nested send joins the correlation and is caused by the parent request", func(t *testing.T) {
		sendOK(t, mediator.WithCorrelationID(ctx, "corr-1"), m, sCmd{S: "nested"})
		if outer.corr != "corr-1" || inner.corr != "corr-1" || inner.cause != outer.req || inner.depth != 2 || inner.req == outer.req || inner.req == uuid.Nil {
			t.Fatalf("outer = %+v inner = %+v", outer, inner)
		}
	})
	t.Run("WithCorrelationID on an existing scope keeps the current request as cause", func(t *testing.T) {
		sendOK(t, mediator.WithCorrelationID(ctx, "corr-1"), m, sCmd{S: "recorrelate"})
		if inner.corr != "corr-2" || inner.cause != outer.req || inner.depth != 2 {
			t.Fatalf("outer = %+v inner = %+v", outer, inner)
		}
	})
	t.Run("WithCausation replaces the cause and the correlation", func(t *testing.T) {
		sendOK(t, ctx, m, sCmd{S: "recause"})
		want := uuid.MustParse("11111111-1111-7111-8111-111111111111")
		if inner.corr != "corr-3" || inner.cause != want || inner.depth != 2 {
			t.Fatalf("inner = %+v", inner)
		}
		// At the top level WithCausation starts the scope: depth stays 1.
		sendOK(t, mediator.WithCausation(ctx, "corr-4", want), m, sCmd{S: "plain"})
		if outer.corr != "corr-4" || outer.cause != want || outer.depth != 1 {
			t.Fatalf("outer = %+v", outer)
		}
	})
	t.Run("WithCorrelationID with an empty id is regenerated", func(t *testing.T) {
		sendOK(t, mediator.WithCorrelationID(ctx, ""), m, sCmd{S: "plain"})
		if outer.corr == "" || outer.corr != outer.req.String() {
			t.Fatalf("outer = %+v", outer)
		}
	})
}

func sendOK[R any](t *testing.T, ctx context.Context, m *mediator.Mediator, req mediator.Request[R]) {
	t.Helper()
	if _, err := mediator.Send(ctx, m, req); err != nil {
		t.Fatal(err)
	}
}

func TestSend_DepthCap(t *testing.T) {
	for _, maxDepth := range []int{1, 2, 5} {
		m := mediator.New(mediator.WithMaxDepth(maxDepth))
		calls := 0
		mustNil(t, mediator.HandleFunc(m, func(ctx context.Context, c sCmd) (string, error) {
			calls++
			if mediator.Depth(ctx) != calls {
				t.Errorf("depth %d at call %d", mediator.Depth(ctx), calls)
			}
			return mediator.Send(ctx, m, sCmd{})
		}))
		build(t, m)
		_, err := mediator.Send(context.Background(), m, sCmd{})
		if !errors.Is(err, mediator.ErrDepthExceeded) || calls != maxDepth {
			t.Fatalf("max %d: calls=%d err=%v", maxDepth, calls, err)
		}
	}
}
