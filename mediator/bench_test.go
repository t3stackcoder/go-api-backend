package mediator_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type benchCmd struct {
	mediator.Command[benchRes]
	ID string `json:"id"`
}

type benchRes struct {
	ID string `json:"id"`
}

type benchEvent struct {
	mediator.Event
	ID string `json:"id"`
}

type ctxKey int

func benchMediator(b testing.TB, nBehaviors int, deriveCtx bool) *mediator.Mediator {
	b.Helper()
	m := mediator.New()
	if err := mediator.HandleFunc(m, func(_ context.Context, c benchCmd) (benchRes, error) {
		return benchRes{ID: c.ID}, nil
	}); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < nBehaviors; i++ {
		key := ctxKey(i)
		f := func(ctx context.Context, req any, _ *mediator.RequestInfo, next mediator.Next) (any, error) {
			if deriveCtx {
				ctx = context.WithValue(ctx, key, i)
			}
			return next(ctx, req)
		}
		if err := mediator.Use(m, mediator.BehaviorFunc{N: fmt.Sprintf("b%d", i), F: f}); err != nil {
			b.Fatal(err)
		}
	}
	if err := m.Build(); err != nil {
		b.Fatal(err)
	}
	return m
}

// BenchmarkSend_WithNBehaviors measures the core cost of the default chain
// length: 12 behaviors that each add one context value.
func BenchmarkSend_WithNBehaviors(b *testing.B) {
	m := benchMediator(b, 12, true)
	ctx := context.Background()
	req := benchCmd{ID: "c"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mediator.Send(ctx, m, req); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublish_InProcess(b *testing.B) {
	m := mediator.New()
	for i := 0; i < 3; i++ {
		if err := mediator.OnFunc(m, func(context.Context, benchEvent) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
	if err := m.Build(); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	ev := benchEvent{ID: "e"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mediator.Publish(ctx, m, ev); err != nil {
			b.Fatal(err)
		}
	}
}

// TestSend_CoreAllocations guards the allocation budget of the bare Send path
// (spec 4.12, G17): box request, scope, context value, box response. A cold
// context costs nothing extra: the generated correlation ID is kept as a
// UUID in the scope and formatted only when CorrelationID reads it.
func TestSend_CoreAllocations(t *testing.T) {
	bare := benchMediator(t, 0, false)
	passthrough := benchMediator(t, 12, false)
	cold := context.Background()
	warm := mediator.WithCorrelationID(cold, "corr")
	req := benchCmd{ID: "c"}
	cases := []struct {
		name string
		m    *mediator.Mediator
		ctx  context.Context
		max  float64
	}{
		{"bare, ambient correlation", bare, warm, 4},
		{"bare, cold context", bare, cold, 4},
		{"12 pass-through behaviors, ambient correlation", passthrough, warm, 4},
		{"12 pass-through behaviors, cold context", passthrough, cold, 4},
	}
	for _, c := range cases {
		got := testing.AllocsPerRun(2000, func() {
			if _, err := mediator.Send(c.ctx, c.m, req); err != nil {
				t.Fatal(err)
			}
		})
		if got > c.max {
			t.Errorf("%s: %v allocs per Send, budget %v", c.name, got, c.max)
		}
	}
}
