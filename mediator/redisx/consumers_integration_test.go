//go:build integration

package redisx

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// redisArgs is a tiny adapter so tests can build XAddArgs without importing
// go-redis in every file.
type redisArgs struct {
	Stream string
	Values map[string]any
}

func (a redisArgs) args() *redis.XAddArgs { return &redis.XAddArgs{Stream: a.Stream, Values: a.Values} }

// intEvent is the durable event of the consumer tests.
type intEvent struct {
	mediator.Event
	Key  string `json:"key"`
	Seq  int64  `json:"seq"`
	Fail bool   `json:"fail"`
}

func (e intEvent) StreamKey() string { return e.Key }

const testGroup = "proj"

type delivery struct {
	key       string
	seq       int64
	node      string
	epoch     int64
	partition int
	attempt   int
}

// recorder is the projection: it records every successful delivery per key
// and per partition, in handler order.
type recorder struct {
	mu        sync.Mutex
	byKey     map[string][]delivery
	byPart    map[int][]delivery
	attempts  map[string]int
	failUntil map[string]int // id -> remaining failures
	transient bool
	block     map[string]chan struct{} // id -> released when closed
}

func newRecorder() *recorder {
	return &recorder{byKey: map[string][]delivery{}, byPart: map[int][]delivery{}, attempts: map[string]int{}, failUntil: map[string]int{}, block: map[string]chan struct{}{}}
}

func (r *recorder) handle(ctx context.Context, e intEvent) error {
	id := e.Key + "/" + strconv.FormatInt(e.Seq, 10)
	env, _ := mediator.EnvelopeFrom(ctx)
	token, _ := mediator.FencingToken(ctx)
	st, _ := mediator.ConsumerStateFrom(ctx)
	r.mu.Lock()
	r.attempts[id]++
	blocker := r.block[id]
	fail := e.Fail
	if n := r.failUntil[id]; n > 0 {
		r.failUntil[id] = n - 1
		fail = true
	}
	transient := r.transient
	r.mu.Unlock()
	if blocker != nil {
		select {
		case <-blocker:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fail {
		if transient {
			return mediator.E(mediator.CodeUnavailable, "flaky")
		}
		return mediator.E(mediator.CodeInternal, "poison")
	}
	d := delivery{key: e.Key, seq: e.Seq, node: ConsumerNodeFrom(ctx), epoch: token, partition: env.Partition}
	if st != nil {
		d.attempt = st.Attempt
	}
	r.mu.Lock()
	r.byKey[e.Key] = append(r.byKey[e.Key], d)
	r.byPart[env.Partition] = append(r.byPart[env.Partition], d)
	r.mu.Unlock()
	return nil
}

func (r *recorder) seqs(key string) []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int64, 0, len(r.byKey[key]))
	for _, d := range r.byKey[key] {
		out = append(out, d.seq)
	}
	return out
}

func (r *recorder) count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.byKey[key])
}

func (r *recorder) attemptsOf(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts[id]
}

// dedupStub mimics the inbox behavior with a map so the tests need no
// Postgres: a second delivery of an event ID skips the handler and reports
// Duplicate through ConsumerState.
type dedupStub struct {
	mu   sync.Mutex
	seen map[uuid.UUID]bool
	dups int
}

func (d *dedupStub) Name() string { return mediator.NameInbox }

func (d *dedupStub) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	env, _ := mediator.EnvelopeFrom(ctx)
	d.mu.Lock()
	if d.seen[env.ID] {
		d.dups++
		d.mu.Unlock()
		if st, ok := mediator.ConsumerStateFrom(ctx); ok {
			st.Duplicate = true
		}
		return nil, nil
	}
	d.mu.Unlock()
	res, err := next(ctx, req)
	if err == nil {
		d.mu.Lock()
		d.seen[env.ID] = true
		d.mu.Unlock()
	}
	return res, err
}

// buildConsumerFixture returns a built mediator with the dedup stub on the
// consumer path and one "proj" consumer of intEvent recording into rec.
func buildConsumerFixture(t *testing.T, opts []mediator.ConsumeOption) (*mediator.Mediator, *recorder, *dedupStub) {
	t.Helper()
	rec := newRecorder()
	stub := &dedupStub{seen: map[uuid.UUID]bool{}}
	logger := quietLogger()
	if os.Getenv("REDISX_TEST_LOG") != "" {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	m := mediator.New(mediator.WithLogger(logger))
	if err := mediator.Use(m, stub, mediator.Consumers()); err != nil {
		t.Fatal(err)
	}
	if err := mediator.ConsumeFunc(m, testGroup, rec.handle, opts...); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	return m, rec, stub
}

// appendEvents writes the events to their partition streams in order and
// returns the entries so tests can re-append duplicates.
func appendEvents(t *testing.T, s *Streams, partitions int, events []intEvent) []pg.OutboxEntry {
	t.Helper()
	var entries []pg.OutboxEntry
	byPart := map[int][]pg.OutboxEntry{}
	var order []int
	for i, e := range events {
		p := mediator.Partition(e.Key, partitions)
		payload, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		entry := pg.OutboxEntry{ID: int64(i + 1), Payload: payload, Envelope: mediator.Envelope{
			ID: mediator.NewID(time.Now()), Type: "intEvent", Topic: "intEvent", StreamKey: e.Key, Seq: e.Seq,
			Partition: p, OccurredAt: time.Now(), CorrelationID: "corr", SchemaVersion: 1,
		}}
		entries = append(entries, entry)
		if _, seen := byPart[p]; !seen {
			order = append(order, p)
		}
		byPart[p] = append(byPart[p], entry)
	}
	for _, p := range order {
		if _, err := s.Append(context.Background(), "intEvent", p, byPart[p]); err != nil {
			t.Fatal(err)
		}
	}
	return entries
}

func appendEntries(t *testing.T, s *Streams, entries []pg.OutboxEntry) {
	t.Helper()
	byPart := map[int][]pg.OutboxEntry{}
	for _, e := range entries {
		byPart[e.Envelope.Partition] = append(byPart[e.Envelope.Partition], e)
	}
	for p, es := range byPart {
		if _, err := s.Append(context.Background(), "intEvent", p, es); err != nil {
			t.Fatal(err)
		}
	}
}

// node is one running Consumers instance.
type node struct {
	c       *Consumers
	cancel  context.CancelFunc
	done    chan error
	stopped bool
}

func startNode(t *testing.T, m *mediator.Mediator, client *redis.Client, cfg Config, fence pg.FencingSource, opts ...ConsumersOption) *node {
	t.Helper()
	opts = append([]ConsumersOption{withBackoff(backoff{min: 20 * time.Millisecond, max: 200 * time.Millisecond})}, opts...)
	c := NewConsumers(m, client, cfg, fence, opts...)
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{c: c, cancel: cancel, done: make(chan error, 1)}
	go func() { n.done <- c.Run(ctx) }()
	t.Cleanup(func() { n.stop(t) })
	return n
}

func (n *node) stop(t *testing.T) {
	t.Helper()
	if n.stopped {
		return
	}
	n.stopped = true
	n.cancel()
	select {
	case err := <-n.done:
		if err != nil {
			t.Errorf("consumers run: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Errorf("consumers did not stop")
	}
}

func ownedTotal(nodes ...*node) int {
	total := 0
	for _, n := range nodes {
		for _, c := range n.c.Stats().Owned {
			total += c
		}
	}
	return total
}

func processedTotal(outcome string, nodes ...*node) int64 {
	var total int64
	for _, n := range nodes {
		total += n.c.Stats().Processed[OutcomeKey{testGroup, outcome}]
	}
	return total
}

func TestConsumers_ThreeNodes_OrderDedupHandoverFencing(t *testing.T) {
	t.Parallel()
	cfg := testConfig("n1")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, stub := buildConsumerFixture(t, nil)
	fence := &counterFencing{}
	cfg2, cfg3 := cfg, cfg
	cfg2.NodeID, cfg3.NodeID = "n2", "n3"
	n1 := startNode(t, m, client, cfg, fence)
	n2 := startNode(t, m, client, cfg2, fence)
	n3 := startNode(t, m, client, cfg3, fence)
	eventually(t, 10*time.Second, "all partitions leased", func() bool { return ownedTotal(n1, n2, n3) == 4 })

	const keys, perKey = 10, 20
	var events []intEvent
	for k := 0; k < keys; k++ {
		for s := 1; s <= perKey; s++ {
			events = append(events, intEvent{Key: fmt.Sprintf("k%d", k), Seq: int64(s)})
		}
	}
	entries := appendEvents(t, streams, cfg.PartitionsPerTopic, events)
	// Relay duplicates: the first quarter is appended again.
	appendEntries(t, streams, entries[:len(entries)/4])

	allDelivered := func(want int) func() bool {
		return func() bool {
			for k := 0; k < keys; k++ {
				if rec.count(fmt.Sprintf("k%d", k)) < want {
					return false
				}
			}
			return true
		}
	}
	eventually(t, 30*time.Second, "first batch delivered", allDelivered(perKey))
	checkOrder := func(upTo int64) {
		t.Helper()
		for k := 0; k < keys; k++ {
			got := rec.seqs(fmt.Sprintf("k%d", k))
			if int64(len(got)) != upTo {
				t.Fatalf("k%d: %d deliveries, want %d: %v", k, len(got), upTo, got)
			}
			for i, s := range got {
				if s != int64(i+1) {
					t.Fatalf("k%d: sequence %v is not 1..%d in order", k, got, upTo)
				}
			}
		}
	}
	checkOrder(perKey)
	eventually(t, 10*time.Second, "duplicates deduplicated", func() bool {
		return processedTotal(OutcomeDedup, n1, n2, n3) >= int64(len(entries)/4)
	})
	if got := processedTotal(OutcomeOK, n1, n2, n3); got != int64(len(events)) {
		t.Fatalf("ok outcomes %d, want %d", got, len(events))
	}
	stub.mu.Lock()
	dups := stub.dups
	stub.mu.Unlock()
	if dups < len(entries)/4 {
		t.Fatalf("stub saw %d duplicates", dups)
	}
	for _, n := range []*node{n1, n2, n3} {
		if err := n.c.Healthy(); err != nil {
			t.Fatalf("%s unhealthy: %v", n.c.NodeID(), err)
		}
	}

	// Handover: stop the node owning the most partitions (with P=4 and
	// three nodes, desired is 2 each, so one node may own nothing); the
	// others take its partitions.
	epochBefore := map[int]int64{}
	rec.mu.Lock()
	for p, ds := range rec.byPart {
		epochBefore[p] = ds[len(ds)-1].epoch
	}
	rec.mu.Unlock()
	nodes := []*node{n1, n2, n3}
	victim := nodes[0]
	for _, n := range nodes {
		if ownedTotal(n) > ownedTotal(victim) {
			victim = n
		}
	}
	if ownedTotal(victim) == 0 {
		t.Fatal("no node owns a partition")
	}
	var rest []*node
	for _, n := range nodes {
		if n != victim {
			rest = append(rest, n)
		}
	}
	scope := ScopeKey{testGroup, "intEvent"}
	handoversBefore := rest[0].c.Stats().Handovers[scope] + rest[1].c.Stats().Handovers[scope]
	stoppedNode := victim.c.NodeID()
	victim.stop(t)
	eventually(t, 10*time.Second, "partitions handed over", func() bool { return ownedTotal(rest...) == 4 })
	if after := rest[0].c.Stats().Handovers[scope] + rest[1].c.Stats().Handovers[scope]; after <= handoversBefore {
		t.Fatalf("handover counter did not move: %d -> %d", handoversBefore, after)
	}
	var more []intEvent
	for k := 0; k < keys; k++ {
		for s := perKey + 1; s <= perKey+10; s++ {
			more = append(more, intEvent{Key: fmt.Sprintf("k%d", k), Seq: int64(s)})
		}
	}
	appendEvents(t, streams, cfg.PartitionsPerTopic, more)
	eventually(t, 30*time.Second, "second batch delivered", allDelivered(perKey+10))
	checkOrder(perKey + 10)

	// Fencing tokens never decrease within a partition and strictly increase
	// when the owner changes (G14).
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for p, ds := range rec.byPart {
		for i := 1; i < len(ds); i++ {
			prev, cur := ds[i-1], ds[i]
			if cur.epoch < prev.epoch {
				t.Fatalf("partition %d: epoch decreased %d -> %d", p, prev.epoch, cur.epoch)
			}
			if cur.node != prev.node && cur.epoch <= prev.epoch {
				t.Fatalf("partition %d: owner changed %s -> %s without a higher epoch (%d -> %d)", p, prev.node, cur.node, prev.epoch, cur.epoch)
			}
		}
		if ds[len(ds)-1].node == stoppedNode {
			t.Fatalf("partition %d: last delivery still by the stopped node", p)
		}
		if ds[len(ds)-1].epoch < epochBefore[p] {
			t.Fatalf("partition %d: epoch after handover %d below %d", p, ds[len(ds)-1].epoch, epochBefore[p])
		}
	}
	if len(rec.byPart) != 4 {
		t.Fatalf("partitions with deliveries: %d", len(rec.byPart))
	}
}

func TestConsumers_DLQAndOps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, nil)
	fence := &counterFencing{}
	rec.mu.Lock()
	rec.failUntil["bad/1"] = 99
	rec.mu.Unlock()
	n := startNode(t, m, client, cfg, fence)
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })

	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "bad", Seq: 1}, {Key: "bad", Seq: 2}, {Key: "other", Seq: 1}})
	var dlq []DLQEntry
	eventually(t, 15*time.Second, "dead letter", func() bool {
		var err error
		dlq, err = DLQList(ctx, client, cfg, testGroup)
		return err == nil && len(dlq) == 1
	})
	e := dlq[0]
	p := mediator.Partition("bad", cfg.PartitionsPerTopic)
	if e.Group != testGroup || e.Attempts != 3 || !strings.Contains(e.Error, "poison") || e.StreamID == "" || e.Node != "n" ||
		e.Stream != cfg.Keys().Stream("intEvent", p) || e.Envelope.StreamKey != "bad" || e.Envelope.Seq != 1 || e.DecodeErr != nil || e.FailedAt.IsZero() {
		t.Fatalf("dlq entry %+v", e)
	}
	if got := rec.attemptsOf("bad/1"); got != 3 {
		t.Fatalf("attempts %d, want 3 (non-transient cap)", got)
	}
	// The partition moved on past the poison entry.
	eventually(t, 10*time.Second, "bad/2 delivered", func() bool { return rec.count("bad") == 1 })
	if got := rec.seqs("bad"); got[0] != 2 {
		t.Fatalf("bad seqs %v", got)
	}
	eventually(t, 10*time.Second, "dlq size refreshed", func() bool { return n.c.Stats().DLQSize[testGroup] == 1 })
	st := n.c.Stats()
	if st.Processed[OutcomeKey{testGroup, OutcomeDLQ}] != 1 || st.Processed[OutcomeKey{testGroup, OutcomeError}] != 3 || len(st.Partitions) != 4 {
		t.Fatalf("stats %+v", st)
	}

	// Ops: leases and lag.
	leases, err := LeaseList(ctx, client, cfg, testGroup)
	if err != nil || len(leases) != 4 {
		t.Fatalf("leases %v %v", leases, err)
	}
	for i, l := range leases {
		if l.Node != "n" || l.Epoch < 1 || l.TTL <= 0 || l.Topic != "intEvent" || l.Partition != i || l.Group != testGroup {
			t.Fatalf("lease %+v", l)
		}
	}
	if none, err := LeaseList(ctx, client, cfg, "nobody"); err != nil || len(none) != 0 {
		t.Fatalf("no leases: %v %v", none, err)
	}
	lag, err := ConsumerLag(ctx, client, cfg, []string{testGroup, "nogroup"}, []string{"intEvent"})
	if err != nil || len(lag) != 8 {
		t.Fatalf("lag %v %v", lag, err)
	}
	for _, l := range lag {
		if l.Group == "nogroup" && !l.Missing {
			t.Fatalf("missing group not reported: %+v", l)
		}
		if l.Group == testGroup && (l.Missing || l.Pending != 0) {
			t.Fatalf("lag %+v", l)
		}
	}

	// Requeue re-adds the entry; it succeeds now that the handler is fixed.
	rec.mu.Lock()
	rec.failUntil["bad/1"] = 0
	rec.mu.Unlock()
	if err := DLQRequeue(ctx, client, cfg, testGroup, e.ID); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "requeued entry delivered", func() bool { return rec.count("bad") == 2 })
	if rest, _ := DLQList(ctx, client, cfg, testGroup); len(rest) != 0 {
		t.Fatalf("dlq after requeue: %v", rest)
	}
	if err := DLQRequeue(ctx, client, cfg, testGroup, e.ID); !errors.Is(err, ErrDLQEntryNotFound) {
		t.Fatalf("requeue unknown: %v", err)
	}
	if err := DLQDrop(ctx, client, cfg, testGroup, e.ID); !errors.Is(err, ErrDLQEntryNotFound) {
		t.Fatalf("drop unknown: %v", err)
	}
	// Drop.
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "bad2", Seq: 1, Fail: true}})
	eventually(t, 15*time.Second, "second dead letter", func() bool {
		dlq, _ = DLQList(ctx, client, cfg, testGroup)
		return len(dlq) == 1
	})
	if err := DLQDrop(ctx, client, cfg, testGroup, dlq[0].ID); err != nil {
		t.Fatal(err)
	}
	if rest, _ := DLQList(ctx, client, cfg, testGroup); len(rest) != 0 {
		t.Fatal("dlq after drop")
	}

	// A forced release makes the node lose and re-acquire the partition
	// with a higher epoch.
	before := leases[0].Epoch
	handovers := n.c.Stats().Handovers[ScopeKey{testGroup, "intEvent"}]
	if err := LeaseRelease(ctx, client, cfg, testGroup, "intEvent", 0); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "partition re-acquired", func() bool {
		ls, _ := LeaseList(ctx, client, cfg, testGroup)
		return len(ls) == 4 && ls[0].Epoch > before && n.c.Stats().Handovers[ScopeKey{testGroup, "intEvent"}] > handovers
	})
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "other", Seq: 2}})
	eventually(t, 10*time.Second, "delivery after forced handover", func() bool { return rec.count("other") == 2 })

	// Closed-client ops fail cleanly.
	closed := closedClient(t, cfg)
	if _, err := DLQList(ctx, closed, cfg, testGroup); err == nil {
		t.Fatal("dlq list closed")
	}
	if err := DLQRequeue(ctx, closed, cfg, testGroup, "1-0"); err == nil {
		t.Fatal("requeue closed")
	}
	if err := DLQDrop(ctx, closed, cfg, testGroup, "1-0"); err == nil {
		t.Fatal("drop closed")
	}
	if _, err := LeaseList(ctx, closed, cfg, testGroup); err == nil {
		t.Fatal("lease list closed")
	}
	if err := LeaseRelease(ctx, closed, cfg, testGroup, "intEvent", 0); err == nil {
		t.Fatal("lease release closed")
	}
	if _, err := ConsumerLag(ctx, closed, cfg, []string{testGroup}, []string{"intEvent"}); err == nil {
		t.Fatal("lag closed")
	}
	if err := PartitionSkip(ctx, closed, cfg, testGroup, "intEvent", 0, "1-0"); err == nil {
		t.Fatal("skip closed")
	}
	store := redisLeaseStore{client: closed}
	if _, err := store.acquire(ctx, "k", "v", time.Second); err == nil {
		t.Fatal("acquire closed")
	}
	if _, err := store.renew(ctx, "k", "v", time.Second); err == nil {
		t.Fatal("renew closed")
	}
	if _, err := store.release(ctx, "k", "v"); err == nil {
		t.Fatal("release closed")
	}
	if _, err := store.beat(ctx, "m", "n", time.Now(), time.Second); err == nil {
		t.Fatal("beat closed")
	}
}

func TestConsumers_StopWhileHalted(t *testing.T) {
	t.Parallel()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, _, _ := buildConsumerFixture(t, []mediator.ConsumeOption{mediator.StrictOrder(true), mediator.MaxAttempts(1)})
	n := startNode(t, m, client, cfg, &counterFencing{})
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 1, Fail: true}})
	eventually(t, 15*time.Second, "partition halted", func() bool { return n.c.Stats().Halted == 1 })
	start := time.Now()
	n.stop(t)
	if time.Since(start) > 5*time.Second {
		t.Fatal("stopping a halted node took too long")
	}
	if st := n.c.Stats(); st.Halted != 0 || st.Running {
		t.Fatalf("stats after stop: %+v", st)
	}
}

func TestConsumers_StrictOrderHaltsAndResumes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, []mediator.ConsumeOption{mediator.StrictOrder(true), mediator.MaxAttempts(2)})
	n := startNode(t, m, client, cfg, &counterFencing{})
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })

	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 1, Fail: true}, {Key: "k", Seq: 2}})
	eventually(t, 15*time.Second, "partition halted", func() bool { return n.c.Stats().Halted == 1 })
	if err := n.c.Healthy(); err == nil || !strings.Contains(err.Error(), "halted") {
		t.Fatalf("healthy: %v", err)
	}
	if got := rec.attemptsOf("k/1"); got != 2 {
		t.Fatalf("attempts %d, want 2", got)
	}
	var halted PartitionStats
	for _, ps := range n.c.Stats().Partitions {
		if ps.Halted {
			halted = ps
		}
	}
	if halted.HaltedID == "" || halted.Partition != mediator.Partition("k", cfg.PartitionsPerTopic) {
		t.Fatalf("halted partition %+v", halted)
	}
	never(t, time.Second, "delivery past the poison entry", func() bool { return rec.count("k") > 0 })
	if dlq, _ := DLQList(ctx, client, cfg, testGroup); len(dlq) != 0 {
		t.Fatal("strict order must not dead-letter")
	}
	if err := PartitionSkip(ctx, client, cfg, testGroup, "intEvent", halted.Partition, "0-1"); err == nil {
		t.Fatal("skipping a non-pending entry must fail")
	}
	if err := PartitionSkip(ctx, client, cfg, testGroup, "intEvent", halted.Partition, halted.HaltedID); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "partition resumed", func() bool { return rec.count("k") == 1 && n.c.Stats().Halted == 0 })
	if err := n.c.Healthy(); err != nil {
		t.Fatal(err)
	}
	if got := rec.seqs("k"); got[0] != 2 {
		t.Fatalf("seqs %v", got)
	}
}

func TestConsumers_TransientRetryInPlace(t *testing.T) {
	t.Parallel()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, nil)
	rec.mu.Lock()
	rec.transient = true
	rec.failUntil["k/1"] = 2
	rec.mu.Unlock()
	n := startNode(t, m, client, cfg, &counterFencing{})
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "k", Seq: 1}, {Key: "k", Seq: 2}})
	eventually(t, 15*time.Second, "delivered after retries", func() bool { return rec.count("k") == 2 })
	rec.mu.Lock()
	first := rec.byKey["k"][0]
	rec.mu.Unlock()
	if first.seq != 1 || first.attempt != 3 || rec.attemptsOf("k/1") != 3 {
		t.Fatalf("first delivery %+v attempts %d", first, rec.attemptsOf("k/1"))
	}
	st := n.c.Stats()
	if st.Processed[OutcomeKey{testGroup, OutcomeError}] != 2 || st.Processed[OutcomeKey{testGroup, OutcomeOK}] != 2 {
		t.Fatalf("stats %+v", st.Processed)
	}
}

func TestConsumers_SkipUnknownTypeAndGarbage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, nil)
	n := startNode(t, m, client, cfg, &counterFencing{})
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })

	stream := cfg.Keys().Stream("intEvent", 0)
	foreign := sampleEntry()
	foreign.Envelope.Type = "someOtherEvent"
	foreign.Envelope.Topic = "intEvent"
	foreign.Envelope.Partition = 0
	if _, err := streams.Append(ctx, "intEvent", 0, []pg.OutboxEntry{foreign}); err != nil {
		t.Fatal(err)
	}
	if err := client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: map[string]any{"garbage": "yes"}}).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, "skip and dead letter", func() bool {
		st := n.c.Stats()
		return st.Processed[OutcomeKey{testGroup, OutcomeSkip}] == 1 && st.Processed[OutcomeKey{testGroup, OutcomeDLQ}] == 1
	})
	dlq, err := DLQList(ctx, client, cfg, testGroup)
	if err != nil || len(dlq) != 1 || dlq[0].DecodeErr == nil || !strings.Contains(dlq[0].Error, "malformed") || dlq[0].Fields["garbage"] != "yes" {
		t.Fatalf("dlq %+v %v", dlq, err)
	}
	eventually(t, 10*time.Second, "everything acknowledged", func() bool {
		p, err := client.XPending(ctx, stream, testGroup).Result()
		return err == nil && p.Count == 0
	})
	if rec.count("o-1") != 0 {
		t.Fatal("foreign event reached the handler")
	}
	// Requeue of a garbage entry without a recorded stream is refused.
	if err := DLQRequeue(ctx, client, cfg, testGroup, dlq[0].ID); err != nil {
		t.Fatalf("requeue with recorded stream: %v", err)
	}
	if err := client.XAdd(ctx, &redis.XAddArgs{Stream: cfg.Keys().DLQ(testGroup), Values: map[string]any{"garbage": "1", DLQFieldError: "x"}}).Err(); err != nil {
		t.Fatal(err)
	}
	dlq, _ = DLQList(ctx, client, cfg, testGroup)
	var last DLQEntry
	for _, e := range dlq {
		if e.Stream == "" {
			last = e
		}
	}
	if err := DLQRequeue(ctx, client, cfg, testGroup, last.ID); err == nil {
		t.Fatal("requeue without a stream must fail")
	}
}

func TestConsumers_LeaseLossCancelsHandler(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, nil)
	release := make(chan struct{})
	rec.mu.Lock()
	rec.block["slow/1"] = release
	rec.mu.Unlock()
	n := startNode(t, m, client, cfg, &counterFencing{})
	eventually(t, 10*time.Second, "partitions leased", func() bool { return ownedTotal(n) == 4 })
	p := mediator.Partition("slow", cfg.PartitionsPerTopic)
	leases, _ := LeaseList(ctx, client, cfg, testGroup)
	before := leases[p].Epoch

	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "slow", Seq: 1}})
	eventually(t, 10*time.Second, "handler running", func() bool { return rec.attemptsOf("slow/1") == 1 })
	// The lease is taken away while the handler runs: the handler's context
	// is canceled at the next renewal and the entry is redelivered later
	// under a higher epoch.
	if err := LeaseRelease(ctx, client, cfg, testGroup, "intEvent", p); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "handler canceled and retried", func() bool { return rec.attemptsOf("slow/1") >= 2 })
	close(release)
	eventually(t, 15*time.Second, "delivered under the new lease", func() bool { return rec.count("slow") == 1 })
	rec.mu.Lock()
	d := rec.byKey["slow"][0]
	rec.mu.Unlock()
	if d.epoch <= before {
		t.Fatalf("epoch %d not above %d", d.epoch, before)
	}
	st := n.c.Stats()
	if st.Processed[OutcomeKey{testGroup, OutcomeOK}] != 1 || st.Processed[OutcomeKey{testGroup, OutcomeError}] != 0 {
		t.Fatalf("stats %+v", st.Processed)
	}
}

func TestConsumers_ClaimsDeadConsumerInPagesAndRecreatesGroup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("n")
	client := newTestClient(t, cfg)
	streams := NewStreams(client, cfg)
	m, rec, _ := buildConsumerFixture(t, nil)

	// A previous owner read 40 entries and died without acknowledging.
	var events []intEvent
	for s := 1; s <= 40; s++ {
		events = append(events, intEvent{Key: "d", Seq: int64(s)})
	}
	p := mediator.Partition("d", cfg.PartitionsPerTopic)
	stream := cfg.Keys().Stream("intEvent", p)
	appendEvents(t, streams, cfg.PartitionsPerTopic, events)
	if err := streams.EnsureGroups(ctx, "intEvent", p, []string{testGroup}); err != nil {
		t.Fatal(err)
	}
	res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{Group: testGroup, Consumer: "dead", Streams: []string{stream, ">"}, Count: 40, Block: -1}).Result()
	if err != nil || len(res) != 1 || len(res[0].Messages) != 40 {
		t.Fatalf("dead consumer read: %v %v", res, err)
	}
	lag, err := ConsumerLag(ctx, client, cfg, []string{testGroup}, []string{"intEvent"})
	if err != nil {
		t.Fatal(err)
	}
	if l := lag[p]; l.Pending != 40 || l.OldestAge <= 0 || l.Consumers["dead"] != 40 || l.Missing {
		t.Fatalf("lag with pending entries: %+v", l)
	}
	start := time.Now()
	n := startNode(t, m, client, cfg, &counterFencing{})
	eventually(t, 30*time.Second, "dead consumer's entries claimed and delivered", func() bool { return rec.count("d") == 40 })
	if time.Since(start) < cfg.ClaimMinIdle {
		t.Fatal("entries were taken before ClaimMinIdle passed")
	}
	got := rec.seqs("d")
	for i, s := range got {
		if s != int64(i+1) {
			t.Fatalf("order after claim: %v", got)
		}
	}
	rec.mu.Lock()
	attempt := rec.byKey["d"][0].attempt
	rec.mu.Unlock()
	if attempt != 2 {
		t.Fatalf("claimed entry attempt %d, want 2 (delivery count from XPENDING)", attempt)
	}

	// The group disappears (Redis loss): the reader recreates it from 0 and
	// the retained window is replayed through the dedup stub.
	if err := client.XGroupDestroy(ctx, stream, testGroup).Err(); err != nil {
		t.Fatal(err)
	}
	appendEvents(t, streams, cfg.PartitionsPerTopic, []intEvent{{Key: "d", Seq: 41}})
	eventually(t, 30*time.Second, "delivery after group recreation", func() bool { return rec.count("d") == 41 })
	eventually(t, 10*time.Second, "replayed window deduplicated", func() bool {
		return n.c.Stats().Processed[OutcomeKey{testGroup, OutcomeDedup}] >= 40
	})
	if got := rec.seqs("d"); got[40] != 41 {
		t.Fatalf("seqs %v", got[35:])
	}
}
