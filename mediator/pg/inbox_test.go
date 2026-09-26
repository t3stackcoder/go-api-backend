package pg_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

func TestInbox_SkipsDuplicates(t *testing.T) {
	store := memstore.New(memstore.Config{})
	calls := 0
	var fail error
	m := build(t, func(m *mediator.Mediator) {
		must(t, mediator.ConsumeFunc(m, "proj", func(ctx context.Context, e thingCreated) error {
			calls++
			if _, ok := pg.StoreTxFrom(ctx); !ok {
				t.Error("handler must run inside the unit of work")
			}
			return fail
		}))
	}, uow(store), pg.Inbox())
	ctx := context.Background()
	env := mediator.Envelope{ID: mediator.NewID(time.Now()), Type: "thingCreated", Topic: "thingCreated", StreamKey: "o1"}
	payload := []byte(`{"id":"o1"}`)

	st := &mediator.ConsumerState{Attempt: 1}
	if err := m.Deliver(mediator.WithConsumerState(ctx, st), "proj", env, payload); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || st.Duplicate || store.Committed() != 1 {
		t.Fatalf("first delivery: calls=%d dup=%v committed=%d", calls, st.Duplicate, store.Committed())
	}
	if ids := store.Inbox()["proj"]; len(ids) != 1 || ids[0] != env.ID {
		t.Fatalf("inbox: %v", store.Inbox())
	}

	st = &mediator.ConsumerState{Attempt: 2}
	if err := m.Deliver(mediator.WithConsumerState(ctx, st), "proj", env, payload); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || !st.Duplicate || store.Committed() != 2 {
		t.Fatalf("redelivery: calls=%d dup=%v committed=%d", calls, st.Duplicate, store.Committed())
	}

	// A failing handler rolls back the inbox row so the next attempt runs again.
	fail = errors.New("boom")
	env2 := mediator.Envelope{ID: mediator.NewID(time.Now()), Type: "thingCreated", Topic: "thingCreated", StreamKey: "o2"}
	if err := m.Deliver(ctx, "proj", env2, []byte(`{"id":"o2"}`)); !errors.Is(err, fail) {
		t.Fatalf("want boom, got %v", err)
	}
	if len(store.Inbox()["proj"]) != 1 || store.RolledBack() != 1 {
		t.Fatal("failed delivery must not keep the inbox row")
	}
	fail = nil
	if err := m.Deliver(ctx, "proj", env2, []byte(`{"id":"o2"}`)); err != nil || calls != 3 {
		t.Fatalf("retry: err=%v calls=%d", err, calls)
	}
	// Without consumer state the duplicate is still skipped without error.
	if err := m.Deliver(ctx, "proj", env2, []byte(`{"id":"o2"}`)); err != nil || calls != 3 {
		t.Fatalf("duplicate without state: err=%v calls=%d", err, calls)
	}
}

func TestInbox_Errors(t *testing.T) {
	ctx := context.Background()
	b := pg.Inbox()
	if b.Name() != mediator.NameInbox {
		t.Fatal("name")
	}
	nextCalled := false
	next := func(ctx context.Context, req any) (any, error) { nextCalled = true; return nil, nil }
	consumer := &mediator.RequestInfo{Kind: mediator.KindConsumer, Group: "g"}

	if _, err := b.Handle(ctx, nil, &mediator.RequestInfo{Kind: mediator.KindCommand}, next); err != nil || !nextCalled {
		t.Fatalf("non-consumer kinds pass through: %v", err)
	}
	nextCalled = false
	if _, err := b.Handle(ctx, nil, consumer, next); mediator.CodeOf(err) != mediator.CodeInternal || nextCalled {
		t.Fatalf("no envelope: %v", err)
	}
	env := mediator.Envelope{ID: mediator.NewID(time.Now())}
	if _, err := b.Handle(mediator.WithEnvelope(ctx, env), nil, consumer, next); !errors.Is(err, pg.ErrNoUnitOfWork) || nextCalled {
		t.Fatalf("no unit of work: %v", err)
	}

	// A lock timeout on the inbox row is reported as transient.
	store := memstore.New(memstore.Config{})
	holder, _ := store.Begin(ctx, pg.TxOptions{})
	if _, err := holder.InboxInsert(ctx, "g", env.ID); err != nil {
		t.Fatal(err)
	}
	err := pg.WithTx(mediator.WithEnvelope(ctx, env), store, pg.TxOptions{LockTimeout: 30 * time.Millisecond}, func(ctx context.Context) error {
		_, err := b.Handle(ctx, nil, consumer, next)
		return err
	})
	if mediator.CodeOf(err) != mediator.CodeUnavailable || !mediator.IsTransient(err) || !pg.IsLockTimeout(err) {
		t.Fatalf("want transient lock timeout, got %v", err)
	}
	_ = holder.Rollback(ctx)
}
