package mediator_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

func TestContext_Accessors(t *testing.T) {
	ctx := context.Background()

	t.Run("idempotency key", func(t *testing.T) {
		if k, ok := mediator.IdempotencyKeyFrom(ctx); ok || k != "" {
			t.Fatal("unset")
		}
		if k, ok := mediator.IdempotencyKeyFrom(mediator.WithIdempotencyKey(ctx, "")); ok || k != "" {
			t.Fatal("empty key counts as unset")
		}
		if k, ok := mediator.IdempotencyKeyFrom(mediator.WithIdempotencyKey(ctx, "abc")); !ok || k != "abc" {
			t.Fatal("set")
		}
		if k, ok := mediator.IdempotencyKeyFrom(mediator.WithIdempotencyKey(mediator.WithIdempotencyKey(ctx, "a"), "b")); !ok || k != "b" {
			t.Fatal("override")
		}
	})
	t.Run("envelope", func(t *testing.T) {
		if _, ok := mediator.EnvelopeFrom(ctx); ok {
			t.Fatal("unset")
		}
		env := mediator.Envelope{ID: mediator.NewID(time.Now()), Type: "x", Headers: map[string]string{"a": "b"}}
		got, ok := mediator.EnvelopeFrom(mediator.WithEnvelope(ctx, env))
		if !ok || got.ID != env.ID || got.Type != "x" || got.Headers["a"] != "b" {
			t.Fatalf("got %+v", got)
		}
		if _, ok := mediator.EnvelopeFrom(mediator.WithEnvelope(ctx, mediator.Envelope{})); !ok {
			t.Fatal("zero envelope is still present")
		}
	})
	t.Run("fencing token", func(t *testing.T) {
		if v, ok := mediator.FencingToken(ctx); ok || v != 0 {
			t.Fatal("unset")
		}
		if v, ok := mediator.FencingToken(mediator.WithFencingToken(ctx, 0)); !ok || v != 0 {
			t.Fatal("zero token is present")
		}
		if v, ok := mediator.FencingToken(mediator.WithFencingToken(ctx, 42)); !ok || v != 42 {
			t.Fatal("set")
		}
	})
	t.Run("no cache", func(t *testing.T) {
		if mediator.NoCache(ctx) || !mediator.NoCache(mediator.WithNoCache(ctx)) {
			t.Fatal("no cache")
		}
	})
	t.Run("unit of work", func(t *testing.T) {
		if _, ok := mediator.UnitOfWorkFrom(ctx); ok {
			t.Fatal("unset")
		}
		if _, ok := mediator.UnitOfWorkFrom(mediator.WithUnitOfWork(ctx, nil)); ok {
			t.Fatal("nil is unset")
		}
		u := &fakeUoW{readOnly: true}
		got, ok := mediator.UnitOfWorkFrom(mediator.WithUnitOfWork(ctx, u))
		if !ok || got != u || !got.ReadOnly() {
			t.Fatal("set")
		}
	})
	t.Run("consumer state", func(t *testing.T) {
		if _, ok := mediator.ConsumerStateFrom(ctx); ok {
			t.Fatal("unset")
		}
		if _, ok := mediator.ConsumerStateFrom(mediator.WithConsumerState(ctx, nil)); ok {
			t.Fatal("nil is unset")
		}
		st := &mediator.ConsumerState{Attempt: 2}
		got, ok := mediator.ConsumerStateFrom(mediator.WithConsumerState(ctx, st))
		if !ok || got != st {
			t.Fatal("set")
		}
		got.Duplicate = true
		if !st.Duplicate {
			t.Fatal("state is shared by pointer")
		}
	})
	t.Run("scope outside any call", func(t *testing.T) {
		if mediator.CorrelationID(ctx) != "" || mediator.RequestID(ctx) != uuid.Nil || mediator.CausationID(ctx) != uuid.Nil || mediator.Depth(ctx) != 0 {
			t.Fatal("zero")
		}
		c := mediator.WithCorrelationID(ctx, "x")
		if mediator.CorrelationID(c) != "x" || mediator.RequestID(c) != uuid.Nil || mediator.Depth(c) != 0 {
			t.Fatal("WithCorrelationID alone sets only the correlation")
		}
		id := uuid.MustParse("33333333-3333-7333-8333-333333333333")
		c = mediator.WithCausation(ctx, "y", id)
		if mediator.CorrelationID(c) != "y" || mediator.RequestID(c) != id || mediator.CausationID(c) != uuid.Nil || mediator.Depth(c) != 0 {
			t.Fatal("WithCausation sets the correlation and the current ID")
		}
		c2 := mediator.WithCorrelationID(c, "z")
		if mediator.CorrelationID(c2) != "z" || mediator.RequestID(c2) != id {
			t.Fatal("WithCorrelationID keeps the rest of an existing scope")
		}
	})
}

func TestNewID(t *testing.T) {
	now := time.Date(2026, 1, 2, 3, 4, 5, 678_000_000, time.UTC)
	a := mediator.NewID(now)
	b := mediator.NewID(now)
	if a.Version() != 7 || a.Variant() != uuid.RFC4122 {
		t.Fatalf("version %d variant %v", a.Version(), a.Variant())
	}
	if a == b {
		t.Fatal("random bits must differ")
	}
	if idMillis(a) != now.UnixMilli() || idMillis(b) != now.UnixMilli() {
		t.Fatalf("timestamp %d want %d", idMillis(a), now.UnixMilli())
	}
	later := mediator.NewID(now.Add(time.Millisecond))
	if bytes.Compare(a[:], later[:]) >= 0 {
		t.Fatal("ids must sort by time")
	}
	seen := map[uuid.UUID]bool{}
	for range 1000 {
		id := mediator.NewID(now)
		if seen[id] {
			t.Fatal("duplicate")
		}
		seen[id] = true
	}
	// Zero and far-future times encode without panicking.
	_ = mediator.NewID(time.Time{})
	_ = mediator.NewID(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
	if n := testing.AllocsPerRun(100, func() { _ = mediator.NewID(now) }); n != 0 {
		t.Fatalf("NewID allocates %v", n)
	}
}

func TestPartition(t *testing.T) {
	cases := []struct {
		key  string
		p    int
		want int
	}{
		{"", 0, 0}, {"", 1, 0}, {"k", -3, 0}, {"k", 1, 0},
	}
	for _, c := range cases {
		if got := mediator.Partition(c.key, c.p); got != c.want {
			t.Errorf("Partition(%q, %d) = %d", c.key, c.p, got)
		}
	}
	for _, p := range []int{2, 3, 16, 64, 1000} {
		for _, key := range []string{"", "a", "order-1", "order-2", "\x00\xff"} {
			got := mediator.Partition(key, p)
			if got < 0 || got >= p || got != mediator.Partition(key, p) {
				t.Fatalf("Partition(%q, %d) = %d", key, p, got)
			}
		}
	}
	// Known FNV-1a value: fnv1a64("a") = 0xaf63dc4c8601ec8c.
	if got := mediator.Partition("a", 1000); got != int(uint64(0xaf63dc4c8601ec8c)%1000) {
		t.Fatalf("fnv1a: %d", got)
	}
}
