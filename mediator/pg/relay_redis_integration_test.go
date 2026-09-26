//go:build integration

package pg_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// TestIntegration_Relay_RedisFlushBetweenWakeups is G12 under load against a
// real Redis: the poll never runs (PollInterval is an hour), the relay is
// driven by NOTIFY wake-ups only, and Redis is flushed between two batches.
// The tail check before the next append must replay every published row and
// recreate the consumer group at ID 0 (7.7).
func TestIntegration_Relay_RedisFlushBetweenWakeups(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rc, err := tcredis.Run(ctx, "redis:8")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(rc) })
	conn, err := rc.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rcfg := redisx.Config{Addr: strings.TrimPrefix(conn, "redis://"), Prefix: "flush"}.WithDefaults()
	client, err := redisx.NewClient(ctx, rcfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sink := redisx.NewStreams(client, rcfg)

	pool, schema := newSchema(t, true)
	topic := "evt_" + schema
	store := pg.NewStore(pool, pg.StoreConfig{Partitions: 1})
	stream := rcfg.Keys().Stream(topic, 0)
	relay := pg.NewRelay(pool, sink, pg.RelayConfig{
		Topics: []string{topic}, Partitions: 1, BatchSize: 8, PollInterval: time.Hour,
		KnownGroups: func() []string { return []string{"proj"} },
	})
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	eventually(t, 5*time.Second, "listening", func() bool { return relay.Healthy() == nil })
	published := func() int64 {
		return count(t, pool, `SELECT count(*) FROM mediator_outbox WHERE published_at IS NOT NULL`)
	}
	streamLen := func() int64 {
		n, _ := client.XLen(ctx, stream).Result()
		return n
	}
	keys := []string{"a", "b", "c"}

	// Wake-up load: 30 transactions of 3 rows.
	for i := 0; i < 30; i++ {
		publish(t, store, topic, keys, 1)
	}
	eventually(t, 10*time.Second, "load relayed", func() bool { return published() == 90 && streamLen() == 90 })
	if st := relay.Stats(); st.Replayed != 0 || st.Errors != 0 {
		t.Fatalf("before the flush: %+v", st)
	}

	// Redis loses everything between two batches. Only wake-ups follow.
	if err := client.FlushAll(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if streamLen() != 0 {
		t.Fatal("flush")
	}
	for i := 0; i < 5; i++ {
		publish(t, store, topic, keys, 1)
	}
	eventually(t, 10*time.Second, "loss detected on the wake-up path and replayed", func() bool {
		return relay.Stats().Replayed == 90 && published() == 105 && streamLen() >= 105
	})
	// Every published row is back in its stream.
	inStream := map[string]bool{}
	entries, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if id, ok := e.Values[redisx.FieldID].(string); ok {
			inStream[id] = true
		}
	}
	rows, err := pool.Query(ctx, `SELECT event_id::text FROM mediator_outbox WHERE published_at IS NOT NULL`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	missing := 0
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		if !inStream[id] {
			missing++
		}
	}
	if missing != 0 {
		t.Fatalf("I2: %d published rows missing from the stream after the replay", missing)
	}
	// The consumer group was recreated at ID 0 so consumers replay the window.
	groups, err := client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, g := range groups {
		if g.Name == "proj" {
			found = true
			if g.LastDeliveredID != "0-0" {
				t.Fatalf("group recreated at %s, want 0-0", g.LastDeliveredID)
			}
		}
	}
	if !found {
		t.Fatalf("group not recreated: %+v", groups)
	}
	// Consistent afterwards: no second replay.
	publish(t, store, topic, keys, 1)
	eventually(t, 5*time.Second, "next batch relayed", func() bool { return published() == 108 && streamLen() >= 108 })
	if st := relay.Stats(); st.Replayed != 90 || st.Errors != 0 {
		t.Fatalf("after the replay: %+v", st)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
