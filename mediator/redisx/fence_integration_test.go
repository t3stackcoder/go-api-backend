//go:build integration

package redisx

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// staleFence stands in for the inbox behavior: it rejects one delivery of the
// armed (key, seq) with the CodeConflict-wrapped pg.ErrStaleLease the Postgres
// partition fence produces (spec 7.2, G14), and records the fencing token the
// rejected delivery carried.
type staleFence struct {
	dedupStub
	mu       sync.Mutex
	armKey   string
	armSeq   int64
	rejected []int64 // fencing tokens of the rejected deliveries
}

func (f *staleFence) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	e, ok := req.(intEvent)
	if !ok {
		if pe, isPtr := req.(*intEvent); isPtr {
			e, ok = *pe, true
		}
	}
	if ok {
		f.mu.Lock()
		armed := f.armKey == e.Key && f.armSeq == e.Seq
		if armed {
			f.armKey = ""
			token, _ := mediator.FencingToken(ctx)
			f.rejected = append(f.rejected, token)
		}
		f.mu.Unlock()
		if armed {
			return nil, mediator.Wrap(mediator.CodeConflict, "inbox: stale lease; a newer owner has taken the partition", pg.ErrStaleLease)
		}
	}
	return f.dedupStub.Handle(ctx, req, info, next)
}

func (f *staleFence) arm(key string, seq int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armKey, f.armSeq = key, seq
}

func (f *staleFence) tokens() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.rejected...)
}

// leaseEnds records every LeaseEnded reason per partition.
type leaseEnds struct {
	NopObserver
	mu      sync.Mutex
	reasons map[int][]string
}

func (o *leaseEnds) LeaseEnded(_, _ string, partition int, reason string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.reasons == nil {
		o.reasons = map[int][]string{}
	}
	o.reasons[partition] = append(o.reasons[partition], reason)
}

func (o *leaseEnds) of(partition int) []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.reasons[partition]...)
}

func epochOf(c *Consumers, partition int) int64 {
	for _, p := range c.Stats().Partitions {
		if p.Group == testGroup && p.Partition == partition {
			return p.Epoch
		}
	}
	return 0
}

// TestConsumers_StaleLeaseFenceEndsLeaseAndReacquires is the transport side
// of fencing at the effect (spec 7.2, G14; design notes 8.7): when the inbox
// reports pg.ErrStaleLease the worker ends its lease as lost at once instead
// of retrying or dead-lettering, leaves the entry pending, and stops. The
// node then re-acquires the partition under a higher epoch and applies the
// pending entries in order, without waiting for ClaimMinIdle.
func TestConsumers_StaleLeaseFenceEndsLeaseAndReacquires(t *testing.T) {
	t.Parallel()
	cfg := testConfig("n")
	cfg.ClaimMinIdle = 5 * time.Second // the periodic claim pass must not be what resumes the partition
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	rec := newRecorder()
	fence := &staleFence{dedupStub: dedupStub{seen: map[uuid.UUID]bool{}}}
	m := mediator.New(mediator.WithLogger(quietLogger()))
	if err := mediator.Use(m, fence, mediator.Consumers()); err != nil {
		t.Fatal(err)
	}
	if err := mediator.ConsumeFunc(m, testGroup, rec.handle); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	obs := &leaseEnds{}
	n := startNode(t, m, client, cfg, &counterFencing{}, WithObserver(obs))
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })
	p := mediator.Partition("k", cfg.PartitionsPerTopic)
	stream := cfg.Keys().Stream("intEvent", p)
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 1}, {Key: "k", Seq: 2}, {Key: "k", Seq: 3}})
	eventually(t, 10*time.Second, "first entries applied", func() bool { return rec.count("k") == 3 })
	before := epochOf(n.c, p)
	if before == 0 {
		t.Fatalf("no epoch for partition %d: %+v", p, n.c.Stats().Partitions)
	}

	fence.arm("k", 4)
	start := time.Now()
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 4}, {Key: "k", Seq: 5}})
	eventually(t, 10*time.Second, "lease ended as lost", func() bool {
		for _, r := range obs.of(p) {
			if r == LeaseEndLost {
				return true
			}
		}
		return false
	})
	if got := fence.tokens(); len(got) != 1 || got[0] != before {
		t.Fatalf("rejected tokens %v, want [%d]", got, before)
	}
	eventually(t, cfg.ClaimMinIdle-time.Second, "entries applied after the re-acquire", func() bool { return rec.count("k") == 5 })
	elapsed := time.Since(start)
	t.Logf("fenced, re-acquired, and applied in %s", elapsed)
	if elapsed >= cfg.LeaseTTL {
		// The stale lease is released, not merely ended: with the key still
		// holding this node's value the re-acquire would otherwise wait for
		// the TTL.
		t.Fatalf("re-acquire took %s, at least LeaseTTL %s: the stale lease key was not released", elapsed, cfg.LeaseTTL)
	}
	if got := rec.seqs("k"); fmt.Sprint(got) != "[1 2 3 4 5]" {
		t.Fatalf("I6: applied sequence %v", got)
	}
	if after := epochOf(n.c, p); after <= before {
		t.Fatalf("epoch after the re-acquire %d, want above %d", after, before)
	}
	st := n.c.Stats()
	if st.Processed[OutcomeKey{testGroup, OutcomeError}] != 0 || st.Processed[OutcomeKey{testGroup, OutcomeDLQ}] != 0 {
		t.Fatalf("a fenced delivery is neither an error nor dead-lettered: %v", st.Processed)
	}
	if st.Processed[OutcomeKey{testGroup, OutcomeOK}] != 5 {
		t.Fatalf("processed ok %d, want 5: %v", st.Processed[OutcomeKey{testGroup, OutcomeOK}], st.Processed)
	}
	if reasons := obs.of(p); len(reasons) != 1 || reasons[0] != LeaseEndLost {
		t.Fatalf("lease end reasons %v, want exactly one %q", reasons, LeaseEndLost)
	}
	pend, err := client.XPending(context.Background(), stream, testGroup).Result()
	if err != nil || pend.Count != 0 {
		t.Fatalf("pending after the re-acquire: %+v %v", pend, err)
	}
	if err := n.c.Healthy(); err != nil {
		t.Fatal(err)
	}
}
