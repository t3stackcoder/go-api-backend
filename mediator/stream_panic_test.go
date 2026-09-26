package mediator_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type panicStream struct {
	mediator.StreamQuery[int]
	Where string `json:"where"`
}

// TestStream_PanicOrigin distinguishes a panic in the handler's iterator
// (converted to a terminal error, G3) from a panic in the caller's loop body
// (which must propagate, because Go forbids resuming a range-over-func
// after its body panicked).
func TestStream_PanicOrigin(t *testing.T) {
	m := mediator.New()
	if err := mediator.HandleStreamFunc(m, func(ctx context.Context, q panicStream) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			if !yield(1, nil) {
				return
			}
			if q.Where == "handler" {
				panic("iterator exploded")
			}
			yield(2, nil)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}

	var got []int
	var last error
	for v, err := range mediator.Stream(context.Background(), m, panicStream{Where: "handler"}) {
		if err != nil {
			last = err
			continue
		}
		got = append(got, v)
	}
	var pe *mediator.PanicError
	if len(got) != 1 || !errors.As(last, &pe) || pe.Value != "iterator exploded" {
		t.Fatalf("handler panic: got=%v err=%v", got, last)
	}

	defer func() {
		v := recover()
		if v != "body exploded" {
			t.Fatalf("caller panic must propagate, recovered %v", v)
		}
	}()
	for range mediator.Stream(context.Background(), m, panicStream{Where: "caller"}) {
		panic("body exploded")
	}
	t.Fatal("unreachable")
}
