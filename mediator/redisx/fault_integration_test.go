//go:build integration && faultinject

package redisx

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// These tests arm testkit fault points, which are process-global, so they
// run sequentially (no t.Parallel) and therefore before every parallel test.

// armError arms one point to fail at the given hit (0 = every hit),
// replacing any previous schedule.
func armError(point string, hit int) {
	testkit.Arm(testkit.Schedule{Point: point, Kind: testkit.FaultError, Hit: hit})
}

// armOnce arms every point to fail at its first hit. Arm replaces the whole
// schedule, so points that must fail together are armed together.
func armOnce(points ...string) {
	s := make([]testkit.Schedule, len(points))
	for i, p := range points {
		s[i] = testkit.Schedule{Point: p, Kind: testkit.FaultError, Hit: 1}
	}
	testkit.Arm(s...)
}

func TestFault_StreamsCacheLimiterLeases(t *testing.T) {
	defer testkit.Disarm()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	s := NewStreams(client, cfg)
	entries := []pg.OutboxEntry{sampleEntry()}

	armError("redis.xadd", 0)
	if _, err := s.Append(ctx, "orders", 1, entries); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("xadd fault: %v", err)
	}
	if _, _, ok, _ := s.Tail(ctx, "orders", 1); ok {
		t.Fatal("entry written despite the fault")
	}
	// Ambiguous: the entry is written, the caller gets a connection error.
	testkit.Arm(testkit.Schedule{Point: "redis.xadd", Kind: testkit.FaultAmbiguous})
	if _, err := s.Append(ctx, "orders", 1, entries); !errors.Is(err, testkit.ErrInjectedAmbiguous) {
		t.Fatalf("xadd ambiguous: %v", err)
	}
	testkit.Disarm()
	if _, oid, ok, err := s.Tail(ctx, "orders", 1); err != nil || !ok || oid != 77 {
		t.Fatalf("ambiguous append not applied: %d %v %v", oid, ok, err)
	}
	armError("redis.stream.info", 0)
	if _, _, _, err := s.Tail(ctx, "orders", 1); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("tail fault: %v", err)
	}
	armError("redis.stream.replay", 0)
	if err := s.EnsureGroups(ctx, "orders", 1, []string{"g"}); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("ensure fault: %v", err)
	}
	testkit.Disarm()

	c := NewCache(client, cfg)
	armError("redis.cache.get", 0)
	if _, _, err := c.Get(ctx, "k"); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("cache get fault: %v", err)
	}
	armError("redis.cache.set", 0)
	if err := c.Set(ctx, "k", nil, []byte("1"), time.Second); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("cache set fault: %v", err)
	}
	armError("redis.tag.bump.pre", 0)
	if err := c.BumpTagsPre(ctx, []string{"a"}); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("bump pre fault: %v", err)
	}
	if err := c.BumpTagsPost(ctx, []string{"a"}); err != nil {
		t.Fatalf("bump post must not be affected by the pre point: %v", err)
	}
	armError("redis.tag.bump.post", 0)
	if err := c.BumpTagsPost(ctx, []string{"a"}); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("bump post fault: %v", err)
	}
	testkit.Disarm()

	l := NewLimiter(client, cfg)
	armError("redis.rl.check", 0)
	if _, err := l.Check(ctx, "n", "k", ratelimit.Policy{Rate: 1, Period: time.Second, Burst: 1}); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("limiter fault: %v", err)
	}
	testkit.Disarm()

	store := redisLeaseStore{client: client}
	key := cfg.Keys().Lease("g", "t", 0)
	armError("redis.lease.acquire", 0)
	if _, err := store.acquire(ctx, key, "n:1", time.Second); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("acquire fault: %v", err)
	}
	testkit.Disarm()
	if ok, err := store.acquire(ctx, key, "n:1", time.Minute); err != nil || !ok {
		t.Fatalf("acquire: %v %v", ok, err)
	}
	if ok, err := store.acquire(ctx, key, "m:2", time.Minute); err != nil || ok {
		t.Fatalf("second acquire must fail: %v %v", ok, err)
	}
	armError("redis.lease.renew", 0)
	if _, err := store.renew(ctx, key, "n:1", time.Minute); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("renew fault: %v", err)
	}
	testkit.Disarm()
	if ok, _ := store.renew(ctx, key, "m:2", time.Minute); ok {
		t.Fatal("renew with a foreign value succeeded")
	}
	if ok, err := store.renew(ctx, key, "n:1", time.Minute); err != nil || !ok {
		t.Fatalf("renew: %v %v", ok, err)
	}
	armError("redis.lease.release", 0)
	if _, err := store.release(ctx, key, "n:1"); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("release fault: %v", err)
	}
	testkit.Disarm()
	if ok, _ := store.release(ctx, key, "m:2"); ok {
		t.Fatal("release with a foreign value succeeded")
	}
	if ok, err := store.release(ctx, key, "n:1"); err != nil || !ok {
		t.Fatalf("release: %v %v", ok, err)
	}
	armError("redis.members.beat", 0)
	if _, err := store.beat(ctx, cfg.Keys().Members("g"), "n", time.Now(), time.Minute); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("beat fault: %v", err)
	}
	testkit.Disarm()
	live, err := store.beat(ctx, cfg.Keys().Members("g"), "n", time.Now(), time.Minute)
	if err != nil || len(live) != 1 || live[0] != "n" {
		t.Fatalf("beat: %v %v", live, err)
	}
	// A stale member is pruned by the next heartbeat of another node.
	if _, err := store.beat(ctx, cfg.Keys().Members("g"), "old", time.Now().Add(-2*time.Minute), time.Minute); err != nil {
		t.Fatal(err)
	}
	live, _ = store.beat(ctx, cfg.Keys().Members("g"), "n", time.Now(), time.Minute)
	if len(live) != 1 {
		t.Fatalf("stale member kept: %v", live)
	}

	// Ops requeue honours the xadd point.
	closedCfg := cfg
	armError("redis.xadd", 0)
	if err := DLQRequeue(ctx, client, closedCfg, "g", "0-1"); !errors.Is(err, ErrDLQEntryNotFound) {
		t.Fatalf("requeue of unknown id should fail before the fault: %v", err)
	}
	testkit.Disarm()
}

func TestFault_RemoteSendAndServer(t *testing.T) {
	defer testkit.Disarm()
	ctx := context.Background()
	cfg := testConfig("client")
	client := newTestClient(t, cfg)
	remote := NewRemote(client, cfg)
	info := &mediator.RequestInfo{Name: "remoteCmd"}
	if err := client.ZAdd(ctx, cfg.Keys().Handlers("remoteCmd"), redis.Z{Score: float64(time.Now().UnixMilli()), Member: "ghost"}).Err(); err != nil {
		t.Fatal(err)
	}
	armError("redis.rpc.xadd", 0)
	if _, err := remote.Send(ctx, info, remoteCmd{}); mediator.CodeOf(err) != mediator.CodeUnavailable {
		t.Fatalf("rpc xadd fault: %v", err)
	}
	testkit.Arm(testkit.Schedule{Point: "redis.rpc.xadd", Kind: testkit.FaultAmbiguous})
	_, err := remote.Send(ctx, info, remoteCmd{})
	if mediator.CodeOf(err) != mediator.CodeUnavailable || !mediator.IsAmbiguous(err) {
		t.Fatalf("rpc xadd ambiguous: %v", err)
	}
	testkit.Disarm()
	if n, _ := client.XLen(ctx, cfg.Keys().RPC("remoteCmd")).Result(); n != 1 {
		t.Fatalf("ambiguous send not written: %d", n)
	}

	// The reply reader survives a read fault.
	armError("redis.rpc.readreply", 1)
	rr := NewReplyReader(remote)
	startComponent(t, rr)
	time.Sleep(500 * time.Millisecond)
	if err := rr.Healthy(); err != nil {
		t.Fatal(err)
	}
	testkit.Disarm()

	// The server fails to start when the first heartbeat fails, and reports
	// later heartbeat faults without dying.
	srvM := serverMediator(t, &atomic.Int64{})
	armError("redis.handlers.beat", 0)
	if err := NewRemoteServer(srvM, client, cfg).Run(ctx); !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("server start with heartbeat fault: %v", err)
	}
	testkit.Disarm()
	// Reply faults: the keyed request is not acknowledged and is claimed later.
	srvCfg := cfg
	srvCfg.NodeID = "s"
	server := NewRemoteServer(srvM, client, srvCfg, withHeartbeat(100*time.Millisecond))
	testkit.Arm(
		testkit.Schedule{Point: "redis.rpc.reply", Kind: testkit.FaultError, Hit: 1},
		testkit.Schedule{Point: "redis.rpc.reply", Kind: testkit.FaultAmbiguous, Hit: 2},
		testkit.Schedule{Point: "redis.xreadgroup", Kind: testkit.FaultError, Hit: 1},
		testkit.Schedule{Point: "redis.xautoclaim", Kind: testkit.FaultError, Hit: 1},
		testkit.Schedule{Point: "redis.handlers.beat", Kind: testkit.FaultError, Hit: 2},
	)
	startComponent(t, server)
	clientM := clientMediator(t, remote)
	remote.InvalidateView("remoteCmd")
	kctx, cancel := context.WithTimeout(mediator.WithIdempotencyKey(ctx, "fault-key"), 15*time.Second)
	defer cancel()
	res, err := mediator.Send(kctx, clientM, remoteCmd{Name: "faulty"})
	if err != nil || res.Greeting != "hello faulty" {
		t.Fatalf("send through faults: %+v %v", res, err)
	}
	// The ambiguous reply was written but not acknowledged: the entry is
	// claimed, replayed, replied again (dropped), and acknowledged.
	eventually(t, 15*time.Second, "faulty request acknowledged", func() bool {
		p, err := client.XPending(ctx, cfg.Keys().RPC("remoteCmd"), RPCGroup).Result()
		return err == nil && p.Count == 0
	})
	if err := server.Healthy(); err != nil {
		t.Fatalf("server after a heartbeat fault: %v", err)
	}
	testkit.Disarm()
	// An ack fault on an unkeyed request skips execution; the request is
	// claimed and executed once later.
	armError("redis.xack", 1)
	res, err = mediator.Send(ctx, clientM, remoteCmd{Name: "unkeyed"})
	if err != nil || res.Greeting != "hello unkeyed" {
		t.Fatalf("send with ack fault: %+v %v", res, err)
	}
	testkit.Disarm()
}

func TestFault_ConsumerLoop(t *testing.T) {
	defer testkit.Disarm()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, nil)
	rec.mu.Lock()
	rec.failUntil["bad/1"] = 99
	rec.mu.Unlock()
	// One failure at each point: the loop backs off and continues, the
	// entry whose XACK failed is reclaimed and deduplicated, and the entry
	// whose dead-letter XADD failed is dead-lettered on the next pass.
	testkit.Arm(
		testkit.Schedule{Point: "redis.stream.replay", Kind: testkit.FaultError, Hit: 1},
		testkit.Schedule{Point: "redis.xautoclaim", Kind: testkit.FaultError, Hit: 1},
		testkit.Schedule{Point: "redis.xreadgroup", Kind: testkit.FaultError, Hit: 1},
		testkit.Schedule{Point: "redis.xack", Kind: testkit.FaultError, Hit: 1},
		testkit.Schedule{Point: "redis.xadd.dlq", Kind: testkit.FaultError, Hit: 1},
		testkit.Schedule{Point: "redis.xadd.dlq", Kind: testkit.FaultAmbiguous, Hit: 2},
		testkit.Schedule{Point: "redis.lease.renew", Kind: testkit.FaultError, Hit: 1},
		testkit.Schedule{Point: "redis.members.beat", Kind: testkit.FaultError, Hit: 1},
	)
	n := startNode(t, m, client, cfg, &counterFencing{})
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 1}, {Key: "k", Seq: 2}, {Key: "bad", Seq: 1}})
	eventually(t, 20*time.Second, "delivered through read faults", func() bool { return rec.count("k") == 2 })
	// The first dead-letter XADD fails, the second is ambiguous (written,
	// not acknowledged), the third succeeds: the DLQ holds the entry at
	// least once, always for the same original stream ID.
	var dlq []DLQEntry
	eventually(t, 20*time.Second, "dead letter after dlq faults", func() bool {
		dlq, _ = DLQList(ctx, client, cfg, testGroup)
		return len(dlq) >= 1
	})
	for _, e := range dlq {
		if e.StreamID != dlq[0].StreamID || e.Envelope.StreamKey != "bad" {
			t.Fatalf("unexpected dead letter %+v", e)
		}
	}
	// The entry whose XACK failed is redelivered and deduplicated.
	eventually(t, 20*time.Second, "redelivery deduplicated", func() bool {
		return n.c.Stats().Processed[OutcomeKey{testGroup, OutcomeDedup}] >= 1
	})
	eventually(t, 10*time.Second, "nothing pending", func() bool {
		lag, err := ConsumerLag(ctx, client, cfg, []string{testGroup}, []string{"intEvent"})
		if err != nil {
			return false
		}
		for _, l := range lag {
			if l.Pending != 0 {
				return false
			}
		}
		return true
	})
	if got := rec.seqs("k"); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("order through faults: %v", got)
	}
	if err := n.c.Healthy(); err != nil {
		t.Fatal(err)
	}
}
