//go:build integration && faultinject

package redisx

import (
	"context"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// TestFault_ClaimForeignBefore fails the claim that runs before a fresh batch
// (fault point redis.xautoclaim): the batch stays pending, nothing is applied
// out of order, and the periodic claim pass restores progress once the point
// works again.
func TestFault_ClaimForeignBefore(t *testing.T) {
	defer testkit.Disarm()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, nil)
	n := startNode(t, m, client, cfg, &counterFencing{})
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })
	p := mediator.Partition("k", cfg.PartitionsPerTopic)
	stream := cfg.Keys().Stream("intEvent", p)
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 1}})
	eventually(t, 10*time.Second, "first entry applied", func() bool { return rec.count("k") == 1 })

	ghost := ghostRead(ctx, client, stream, "ghost")
	time.Sleep(2 * cfg.ReadBlock)
	armError("redis.xautoclaim", 0)
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 2}, {Key: "k", Seq: 3}})
	if msgs := <-ghost; len(msgs) != 1 {
		t.Fatalf("ghost read %v", msgs)
	}
	never(t, 600*time.Millisecond, "batch applied ahead of the foreign entry", func() bool { return rec.count("k") > 1 })
	testkit.Disarm()
	eventually(t, 15*time.Second, "claim pass restores progress", func() bool { return rec.count("k") == 3 })
	if got := rec.seqs("k"); len(got) != 3 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("order through the fault: %v", got)
	}
	if err := n.c.Healthy(); err != nil {
		t.Fatal(err)
	}
}
