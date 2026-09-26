//go:build integration

package redisx

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

func TestStreams_SinkAndTrimmer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	s := NewStreams(client, cfg)
	if s.Keys().Prefix != cfg.Prefix {
		t.Fatal("keys")
	}

	if id, err := s.Append(ctx, "orders", 1, nil); err != nil || id != "" {
		t.Fatalf("empty append: %q %v", id, err)
	}
	if _, _, ok, err := s.Tail(ctx, "orders", 1); err != nil || ok {
		t.Fatalf("tail of missing stream: %v %v", ok, err)
	}
	var entries []pg.OutboxEntry
	for i := 1; i <= 3; i++ {
		e := sampleEntry()
		e.ID = int64(i)
		e.Envelope.ID = mediator.NewID(time.Now())
		e.Envelope.Seq = int64(i)
		e.Envelope.Partition = 1
		entries = append(entries, e)
	}
	last, err := s.Append(ctx, "orders", 1, entries)
	if err != nil || last == "" {
		t.Fatalf("append: %q %v", last, err)
	}
	tailID, outboxID, ok, err := s.Tail(ctx, "orders", 1)
	if err != nil || !ok || tailID != last || outboxID != 3 {
		t.Fatalf("tail: %q %d %v %v", tailID, outboxID, ok, err)
	}
	msgs, err := client.XRange(ctx, cfg.Keys().Stream("orders", 1), "-", "+").Result()
	if err != nil || len(msgs) != 3 {
		t.Fatalf("xrange: %d %v", len(msgs), err)
	}
	for i, m := range msgs {
		env, payload, oid, err := DecodeEntry(m.Values)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(env, entries[i].Envelope) || string(payload) != string(entries[i].Payload) || oid != entries[i].ID {
			t.Fatalf("entry %d mismatch: %+v", i, env)
		}
	}
	if n, err := s.Length(ctx, "orders", 1); err != nil || n != 3 {
		t.Fatalf("length %d %v", n, err)
	}

	// Groups start at 0 so they replay the retained window; BUSYGROUP is ignored.
	for i := 0; i < 2; i++ {
		if err := s.EnsureGroups(ctx, "orders", 1, []string{"proj", "audit"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.EnsureGroups(ctx, "orders", 1, nil); err != nil {
		t.Fatal(err)
	}
	groups, err := client.XInfoGroups(ctx, cfg.Keys().Stream("orders", 1)).Result()
	if err != nil || len(groups) != 2 {
		t.Fatalf("groups %v %v", groups, err)
	}
	for _, g := range groups {
		if g.LastDeliveredID != "0-0" {
			t.Fatalf("group %s starts at %s, want 0-0", g.Name, g.LastDeliveredID)
		}
	}
	// MKSTREAM creates the stream for a group on a partition that has no entries yet.
	if err := s.EnsureGroups(ctx, "orders", 2, []string{"proj"}); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := s.Tail(ctx, "orders", 2); err != nil || ok {
		t.Fatalf("empty stream tail: %v %v", ok, err)
	}

	// A foreign entry at the tail still counts as an existing stream.
	if err := client.XAdd(ctx, redisArgs{Stream: cfg.Keys().Stream("orders", 1), Values: map[string]any{"foreign": "1"}}.args()).Err(); err != nil {
		t.Fatal(err)
	}
	if _, oid, ok, err := s.Tail(ctx, "orders", 1); err != nil || !ok || oid != 0 {
		t.Fatalf("foreign tail: %d %v %v", oid, ok, err)
	}

	// Trimming by time removes everything older than the horizon.
	if err := s.TrimBefore(ctx, "orders", 1, time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Length(ctx, "orders", 1); n != 4 {
		t.Fatalf("trim in the past removed entries: %d", n)
	}
	if err := s.TrimBefore(ctx, "orders", 1, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Length(ctx, "orders", 1); n != 0 {
		t.Fatalf("trim left %d entries", n)
	}

	closed := NewStreams(closedClient(t, cfg), cfg)
	if _, err := closed.Append(ctx, "orders", 1, entries); err == nil {
		t.Fatal("append on closed client")
	}
	if _, _, _, err := closed.Tail(ctx, "orders", 1); err == nil {
		t.Fatal("tail on closed client")
	}
	if err := closed.EnsureGroups(ctx, "orders", 1, []string{"g"}); err == nil {
		t.Fatal("ensure on closed client")
	}
	if err := closed.TrimBefore(ctx, "orders", 1, time.Now()); err == nil {
		t.Fatal("trim on closed client")
	}
}
