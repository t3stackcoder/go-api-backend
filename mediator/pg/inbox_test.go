package pg_test

import (
	"context"
	"errors"
	"slices"
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

func TestInbox_FencesStaleLease(t *testing.T) {
	store := memstore.New(memstore.Config{})
	calls := 0
	m := build(t, func(m *mediator.Mediator) {
		must(t, mediator.ConsumeFunc(m, "proj", func(ctx context.Context, e thingCreated) error {
			calls++
			return nil
		}))
	}, uow(store), pg.Inbox())
	ctx := context.Background()
	newEnv := func(key string, partition int) mediator.Envelope {
		return mediator.Envelope{ID: mediator.NewID(time.Now()), Type: "thingCreated", Topic: "thingCreated", StreamKey: key, Partition: partition}
	}
	deliver := func(ctx context.Context, env mediator.Envelope) error {
		return m.Deliver(ctx, "proj", env, []byte(`{"id":"`+env.StreamKey+`"}`))
	}
	row := memstore.PartitionKey{Group: "proj", Topic: "thingCreated", Partition: 0}

	// The owner's token is recorded together with the effect.
	if err := deliver(mediator.WithFencingToken(ctx, 5), newEnv("o1", 0)); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || store.Committed() != 1 || store.PartitionEpochs()[row] != 5 {
		t.Fatalf("token 5: calls=%d committed=%d epochs=%v", calls, store.Committed(), store.PartitionEpochs())
	}
	// A newer owner moves the epoch up.
	if err := deliver(mediator.WithFencingToken(ctx, 7), newEnv("o2", 0)); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || store.PartitionEpochs()[row] != 7 {
		t.Fatalf("token 7: calls=%d epochs=%v", calls, store.PartitionEpochs())
	}
	// The stale owner is rejected before the handler runs: a definite
	// conflict, not transient, the transaction rolled back, no inbox row.
	env3 := newEnv("o3", 0)
	err := deliver(mediator.WithFencingToken(ctx, 6), env3)
	if !errors.Is(err, pg.ErrStaleLease) || mediator.CodeOf(err) != mediator.CodeConflict || mediator.IsTransient(err) {
		t.Fatalf("token 6 after 7: want a non-transient conflict wrapping ErrStaleLease, got %v", err)
	}
	if calls != 2 || store.RolledBack() != 1 || store.Committed() != 2 {
		t.Fatalf("stale delivery: calls=%d rolledBack=%d committed=%d", calls, store.RolledBack(), store.Committed())
	}
	if ids := store.Inbox()["proj"]; len(ids) != 2 || slices.Contains(ids, env3.ID) {
		t.Fatalf("stale delivery must not keep its inbox row: %v", ids)
	}
	if store.PartitionEpochs()[row] != 7 {
		t.Fatalf("epoch after a stale delivery: %v", store.PartitionEpochs())
	}
	// The same event under the current owner's token is applied.
	if err := deliver(mediator.WithFencingToken(ctx, 7), env3); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || len(store.Inbox()["proj"]) != 3 || store.PartitionEpochs()[row] != 7 {
		t.Fatalf("redelivery with token 7: calls=%d inbox=%v epochs=%v", calls, store.Inbox(), store.PartitionEpochs())
	}
	// Without a fencing token there is no fence: the epoch table is untouched.
	if err := deliver(ctx, newEnv("o4", 3)); err != nil {
		t.Fatal(err)
	}
	if epochs := store.PartitionEpochs(); calls != 4 || len(epochs) != 1 || epochs[row] != 7 {
		t.Fatalf("delivery without a token: calls=%d epochs=%v", calls, epochs)
	}

	// A lock timeout on the epoch row is reported as transient, like one on
	// the inbox row.
	b := pg.Inbox()
	store2 := memstore.New(memstore.Config{})
	holder, err := store2.Begin(ctx, pg.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := holder.FencePartition(ctx, "g", "t", 0, 1); err != nil {
		t.Fatal(err)
	}
	nextCalled := false
	next := func(ctx context.Context, req any) (any, error) { nextCalled = true; return nil, nil }
	consumer := &mediator.RequestInfo{Kind: mediator.KindConsumer, Group: "g"}
	fenced := mediator.WithFencingToken(mediator.WithEnvelope(ctx, mediator.Envelope{ID: mediator.NewID(time.Now()), Topic: "t"}), 2)
	err = pg.WithTx(fenced, store2, pg.TxOptions{LockTimeout: 30 * time.Millisecond}, func(ctx context.Context) error {
		_, err := b.Handle(ctx, nil, consumer, next)
		return err
	})
	if mediator.CodeOf(err) != mediator.CodeUnavailable || !mediator.IsTransient(err) || !pg.IsLockTimeout(err) || nextCalled {
		t.Fatalf("want a transient lock timeout on the epoch row, got %v (next called: %v)", err, nextCalled)
	}
	_ = holder.Rollback(ctx)
}
