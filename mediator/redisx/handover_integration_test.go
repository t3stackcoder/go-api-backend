//go:build integration

package redisx

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// keyOnPartition returns a stream key that hashes to partition want.
func keyOnPartition(t *testing.T, partitions, want int) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		k := fmt.Sprintf("h%d", i)
		if mediator.Partition(k, partitions) == want {
			return k
		}
	}
	t.Fatalf("no key on partition %d", want)
	return ""
}

// leaseHolder returns the node holding the partition lease, or "".
func leaseHolder(t *testing.T, client *redis.Client, cfg Config, partition int) string {
	t.Helper()
	leases, err := LeaseList(context.Background(), client, cfg, testGroup)
	if err != nil {
		t.Fatal(err)
	}
	for _, l := range leases {
		if l.Partition == partition {
			return l.Node
		}
	}
	return ""
}

// pendingAtRelease records, for every surplus release, how many entries the
// releasing consumer still had pending on the partition at that moment.
type pendingAtRelease struct {
	NopObserver
	client *redis.Client
	cfg    Config
	node   string
	mu     sync.Mutex
	counts map[int]int64
}

func (o *pendingAtRelease) LeaseEnded(_, topic string, partition int, reason string) {
	if reason != LeaseEndSurplus {
		return
	}
	p, err := o.client.XPending(context.Background(), o.cfg.Keys().Stream(topic, partition), testGroup).Result()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.counts == nil {
		o.counts = map[int]int64{}
	}
	if err != nil {
		o.counts[partition] = -1
		return
	}
	o.counts[partition] = p.Consumers[o.node]
}

// TestConsumers_SurplusReleaseDrainsBeforeHandover is the voluntary handover
// of spec 7.2 (G6): a node that releases a surplus partition first stops
// reading, finishes the entry in hand, processes what its outstanding read
// had delivered into its PEL, and only then releases the lease. Nothing is
// left for the next owner to wait ClaimMinIdle for, and per-key order holds
// across the handover.
func TestConsumers_SurplusReleaseDrainsBeforeHandover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n1")
	cfg.ReadBlock = 2 * time.Second    // a blocking read outstanding at release time is the bug's precondition
	cfg.ClaimMinIdle = 5 * time.Second // an entry left in a stale PEL would wait this long
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, nil)
	fence := &counterFencing{}
	const p = 3 // the surplus release hands the highest partitions over first
	key := keyOnPartition(t, cfg.PartitionsPerTopic, p)
	stream := cfg.Keys().Stream("intEvent", p)
	obs := &pendingAtRelease{client: client, cfg: cfg, node: "n1"}
	n1 := startNode(t, m, client, cfg, fence, WithObserver(obs))
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n1) == 4 })

	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: key, Seq: 1}, {Key: key, Seq: 2}, {Key: key, Seq: 3}})
	eventually(t, 10*time.Second, "first entries applied", func() bool { return rec.count(key) == 3 })

	// Entry 4 is in flight (its handler blocks). Entry 5 is delivered into
	// n1's PEL by an outstanding read the release must not run ahead of;
	// the test issues that read in n1's name.
	release := make(chan struct{})
	rec.mu.Lock()
	rec.block[key+"/4"] = release
	rec.mu.Unlock()
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: key, Seq: 4}})
	eventually(t, 10*time.Second, "entry 4 in flight", func() bool { return rec.attemptsOf(key+"/4") == 1 })
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: key, Seq: 5}})
	res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: testGroup, Consumer: "n1", Streams: []string{stream, ">"}, Count: 1, Block: -1}).Result()
	if err != nil || len(res) != 1 || len(res[0].Messages) != 1 {
		t.Fatalf("read as n1: %v %v", res, err)
	}

	// A second node joins: n1 must hand over p3 and p2. p2 is idle and
	// moves at once; p3 stays with n1 while its handler runs.
	cfg2 := cfg
	cfg2.NodeID = "n2"
	n2 := startNode(t, m, client, cfg2, fence)
	eventually(t, 10*time.Second, "idle surplus partition handed over", func() bool { return leaseHolder(t, client, cfg, 2) == "n2" })
	never(t, time.Second, "release while the handler runs", func() bool { return leaseHolder(t, client, cfg, p) != "n1" })
	if ownedTotal(n1) != 3 {
		t.Fatalf("n1 owns %d, want 3 while draining", ownedTotal(n1))
	}

	// The handler returns: n1 acknowledges 4, drains 5 from its own PEL,
	// releases; n2 acquires and continues with 6 without any claim delay.
	start := time.Now()
	close(release)
	eventually(t, 10*time.Second, "p3 handed over", func() bool { return leaseHolder(t, client, cfg, p) == "n2" })
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: key, Seq: 6}})
	eventually(t, cfg.ClaimMinIdle-time.Second, "entries applied without a claim wait", func() bool { return rec.count(key) == 6 })
	if got := rec.seqs(key); fmt.Sprint(got) != "[1 2 3 4 5 6]" {
		t.Fatalf("I6: applied sequence %v", got)
	}
	rec.mu.Lock()
	ds := append([]delivery(nil), rec.byKey[key]...)
	rec.mu.Unlock()
	if ds[3].node != "n1" || ds[4].node != "n1" || ds[5].node != "n2" {
		t.Fatalf("nodes: 4 by %s, 5 by %s, 6 by %s; want n1, n1, n2", ds[3].node, ds[4].node, ds[5].node)
	}
	if ds[5].epoch <= ds[4].epoch {
		t.Fatalf("G14: epoch %d after handover not above %d", ds[5].epoch, ds[4].epoch)
	}
	obs.mu.Lock()
	left, seen := obs.counts[p]
	obs.mu.Unlock()
	if !seen || left != 0 {
		t.Fatalf("n1 still had %d pending entries on p%d when it released (seen=%v)", left, p, seen)
	}
	t.Logf("handover took %s", time.Since(start))
	if err := n1.c.Healthy(); err != nil {
		t.Fatal(err)
	}
	if err := n2.c.Healthy(); err != nil {
		t.Fatal(err)
	}
}

// ghostRead issues a blocking XREADGROUP in the name of a stale consumer and
// delivers what it received. Redis serves the oldest blocked reader first,
// so after two ReadBlock periods the ghost is ahead of the node's own read.
func ghostRead(ctx context.Context, client *redis.Client, stream, consumer string) <-chan []redis.XMessage {
	got := make(chan []redis.XMessage, 1)
	go func() {
		res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: testGroup, Consumer: consumer, Streams: []string{stream, ">"}, Count: 1, Block: 10 * time.Second}).Result()
		if err != nil || len(res) != 1 {
			got <- nil
			return
		}
		got <- res[0].Messages
	}()
	return got
}

// TestConsumers_ClaimsForeignEntriesBelowBatch is the involuntary handover
// (spec 7.2, G6): a stale owner whose blocking read is still outstanding
// takes the next entry into its PEL; the live owner's fresh batch starts
// above it. The live owner claims and processes the lower entry first
// instead of reading past it and waiting for the ClaimMinIdle pass.
func TestConsumers_ClaimsForeignEntriesBelowBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	cfg.ClaimMinIdle = 5 * time.Second // the periodic claim pass must not be what restores the order
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, nil)
	n := startNode(t, m, client, cfg, &counterFencing{})
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })
	p := mediator.Partition("k", cfg.PartitionsPerTopic)
	stream := cfg.Keys().Stream("intEvent", p)
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 1}, {Key: "k", Seq: 2}, {Key: "k", Seq: 3}})
	eventually(t, 10*time.Second, "first entries applied", func() bool { return rec.count("k") == 3 })

	ghost := ghostRead(ctx, client, stream, "ghost")
	time.Sleep(2 * cfg.ReadBlock)
	start := time.Now()
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 4}, {Key: "k", Seq: 5}})
	msgs := <-ghost
	if len(msgs) != 1 {
		t.Fatalf("ghost read %v", msgs)
	}
	if env, _, _, err := DecodeEntry(msgs[0].Values); err != nil || env.Seq != 4 {
		t.Fatalf("ghost took %+v %v, want seq 4", env, err)
	}
	eventually(t, cfg.ClaimMinIdle-time.Second, "entries applied without a claim wait", func() bool { return rec.count("k") == 5 })
	t.Logf("claimed and applied in %s", time.Since(start))
	if got := rec.seqs("k"); fmt.Sprint(got) != "[1 2 3 4 5]" {
		t.Fatalf("I6: applied sequence %v", got)
	}
	rec.mu.Lock()
	claimed := rec.byKey["k"][3]
	rec.mu.Unlock()
	if claimed.attempt != 2 {
		t.Fatalf("claimed entry attempt %d, want 2 (delivery count after XCLAIM)", claimed.attempt)
	}
	pend, err := client.XPending(ctx, stream, testGroup).Result()
	if err != nil || pend.Count != 0 {
		t.Fatalf("pending after the claim: %+v %v", pend, err)
	}
	if err := n.c.Healthy(); err != nil {
		t.Fatal(err)
	}
}
