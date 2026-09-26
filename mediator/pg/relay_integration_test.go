//go:build integration

package pg_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

// publish appends n durable events per key through the real write path.
func publish(t *testing.T, store pg.Store, topic string, keys []string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		err := pg.WithTx(context.Background(), store, pg.TxOptions{}, func(ctx context.Context) error {
			uow, _ := mediator.UnitOfWorkFrom(ctx)
			for _, k := range keys {
				env := &mediator.Envelope{ID: mediator.NewID(time.Now()), Type: "relayEvent", Topic: topic, StreamKey: k,
					OccurredAt: time.Now().UTC(), CorrelationID: "corr", CausationID: uuid.New().String(), SchemaVersion: 2,
					Headers: map[string]string{"h": k}}
				if err := uow.AppendOutbox(ctx, env, []byte(`{"id":"`+k+`"}`)); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func sinkTotal(sink *memstore.Streams, topic string, partitions int) int {
	n := 0
	for p := 0; p < partitions; p++ {
		n += len(sink.Entries(topic, p))
	}
	return n
}

func TestIntegration_Relay_EndToEnd(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, schema := newSchema(t, true)
	const partitions = 2
	topic := "evt_" + schema // advisory locks are global; keep topics unique per test
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: partitions})
	sink := memstore.NewStreams(nil)
	keys := []string{"a", "b", "c"}
	publish(t, store, topic, keys, 2) // 6 rows before the relay starts

	relay := pg.NewRelay(pool, sink, pg.RelayConfig{
		Topics: []string{topic}, Partitions: partitions, BatchSize: 4, PollInterval: 200 * time.Millisecond,
		KnownGroups: func() []string { return []string{"proj"} },
	})
	var markFailures atomic.Int32
	pg.SetRelayBeforeMark(relay, func(context.Context) error {
		if markFailures.Load() > 0 {
			markFailures.Add(-1)
			return errors.New("simulated crash after XADD, before mark")
		}
		return nil
	})
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()

	// The sink is appended before the rows are marked and committed, so a
	// "relayed" condition must include the database state.
	relayed := func(n int) func() bool {
		return func() bool {
			return sinkTotal(sink, topic, partitions) == n &&
				count(t, pool, `SELECT count(*) FROM mediator_outbox WHERE published_at IS NULL`) == 0
		}
	}
	eventually(t, 10*time.Second, "initial backlog relayed", relayed(6))
	eventually(t, 5*time.Second, "relay healthy", func() bool { return relay.Healthy() == nil })
	// Envelope fields and payload survive the round trip through the table;
	// within a key, seq ascends in stream order.
	lastSeq := map[string]int64{}
	for p := 0; p < partitions; p++ {
		for _, e := range sink.Entries(topic, p) {
			env := e.Entry.Envelope
			if env.Partition != p || env.Topic != topic || env.Type != "relayEvent" || env.SchemaVersion != 2 ||
				env.CorrelationID != "corr" || env.CausationID == "" || env.Headers["h"] != env.StreamKey || env.OccurredAt.IsZero() ||
				string(e.Entry.Payload) != `{"id": "`+env.StreamKey+`"}` || e.Entry.CreatedAt.IsZero() || e.Entry.ID == 0 {
				t.Fatalf("relayed entry lost fields: %+v", e.Entry)
			}
			if env.Seq <= lastSeq[env.StreamKey] {
				t.Fatalf("key %s: seq %d after %d", env.StreamKey, env.Seq, lastSeq[env.StreamKey])
			}
			lastSeq[env.StreamKey] = env.Seq
		}
	}
	if n := count(t, pool, `SELECT count(*) FROM mediator_relay_cursor WHERE topic = $1`, topic); n != partitions {
		t.Fatalf("%d cursor rows, want %d", n, partitions)
	}

	// New rows are picked up through LISTEN/NOTIFY well within the poll interval.
	start := time.Now()
	publish(t, store, topic, keys, 1)
	eventually(t, 5*time.Second, "notified rows relayed", relayed(9))
	t.Logf("wake-up latency %s", time.Since(start))

	// Crash simulation: the mark fails after the append, so the rows stay
	// unpublished, the sink holds duplicates, and the retry marks them.
	markFailures.Store(1)
	publish(t, store, topic, []string{"a"}, 1)
	eventually(t, 10*time.Second, "row relayed after the simulated crash", relayed(11))
	pa := mediator.Partition("a", partitions)
	dup := 0
	entries := sink.Entries(topic, pa)
	last := entries[len(entries)-1].Entry.Envelope.ID
	for _, e := range entries {
		if e.Entry.Envelope.ID == last {
			dup++
		}
	}
	if dup != 2 {
		t.Fatalf("the crashed batch must appear twice in the sink, found %d", dup)
	}
	if st := relay.Stats(); st.Published != 10 || st.Errors == 0 {
		t.Fatalf("stats after crash: %+v", st)
	}

	// Redis data loss: drop the stream; the next poll replays the published
	// rows after the (missing) tail and recreates the consumer groups.
	sink.Drop(topic, pa)
	eventually(t, 10*time.Second, "replay after data loss", func() bool {
		return relay.Stats().Replayed > 0 && len(sink.Groups(topic, pa)) == 1
	})
	rows := count(t, pool, `SELECT count(*) FROM mediator_outbox WHERE topic = $1 AND partition = $2`, topic, pa)
	eventually(t, 5*time.Second, "replayed rows in the sink", func() bool { return int64(len(sink.Entries(topic, pa))) == rows })
	if relay.Stats().Replayed != rows {
		t.Fatalf("replayed %d, want %d", relay.Stats().Replayed, rows)
	}
	eventually(t, 5*time.Second, "no second replay", func() bool {
		time.Sleep(300 * time.Millisecond)
		return relay.Stats().Replayed == rows
	})

	// Gauges are refreshed on the poll.
	eventually(t, 5*time.Second, "gauges", func() bool {
		for _, s := range relay.Stats().Slots {
			if !s.Owned || s.Unpublished != 0 {
				return false
			}
		}
		return true
	})

	// A second relay process shares the slots: it owns none while the first
	// holds them, then takes over when the first stops.
	relay2 := pg.NewRelay(pool, sink, pg.RelayConfig{Topics: []string{topic}, Partitions: partitions, PollInterval: 100 * time.Millisecond})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan error, 1)
	go func() { done2 <- relay2.Run(ctx2) }()
	eventually(t, 5*time.Second, "second relay listening", func() bool { return relay2.Healthy() == nil })
	time.Sleep(300 * time.Millisecond)
	for _, s := range relay2.Stats().Slots {
		if s.Owned {
			t.Fatal("second relay must not own a slot the first holds")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("relay run: %v", err)
	}
	if relay.Healthy() == nil {
		t.Fatal("stopped relay must be unhealthy")
	}
	eventually(t, 5*time.Second, "second relay takes over", func() bool {
		for _, s := range relay2.Stats().Slots {
			if !s.Owned {
				return false
			}
		}
		return true
	})
	publish(t, store, topic, []string{"b"}, 1)
	eventually(t, 5*time.Second, "second relay relays", func() bool { return relay2.Stats().Published == 1 })
	cancel2()
	if err := <-done2; err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_Relay_ConnectionLossReconnects(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool, schema := newSchema(t, true)
	topic := "evt_" + schema
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: 1})
	sink := memstore.NewStreams(nil)
	// The relay gets its own pool with a unique application_name so the
	// server-side kill below hits only its sessions.
	rpool, err := pg.NewPool(ctx, pgURL, pg.PoolConfig{SearchPath: schema, ApplicationName: "relay_" + schema, MaxConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer rpool.Close()
	relay := pg.NewRelay(rpool, sink, pg.RelayConfig{Topics: []string{topic}, Partitions: 1, PollInterval: 100 * time.Millisecond, MinBackoff: 50 * time.Millisecond})
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	eventually(t, 5*time.Second, "listening", func() bool { return relay.Healthy() == nil })
	// Kill the relay's sessions, including the dedicated LISTEN connection, from the server side.
	if _, err := root(t).Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = $1`, "relay_"+schema); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "session error counted", func() bool { return relay.Stats().Errors > 0 })
	eventually(t, 10*time.Second, "reconnected", func() bool { return relay.Healthy() == nil && relay.Stats().Slots[0].Owned })
	publish(t, store, topic, []string{"k"}, 1)
	eventually(t, 5*time.Second, "relays after reconnect", func() bool { return len(sink.Entries(topic, 0)) == 1 })
	cancel()
	<-done
}
