package redisx

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// Delivery outcomes reported through Observer.Processed and Stats.Processed
// (metric mediator.consumer.processed, spec 9.1).
const (
	OutcomeOK    = "ok"    // handler committed
	OutcomeDedup = "dedup" // inbox reported a duplicate; handler skipped
	OutcomeError = "error" // one failed attempt
	OutcomeDLQ   = "dlq"   // dead-lettered after MaxAttempts
	OutcomeSkip  = "skip"  // no consumer for the event type in the group on this node
)

// nonTransientAttempts is the lower attempt cap for non-transient errors
// (spec 7.3): a handler bug does not deserve ten tries.
const nonTransientAttempts = 3

// defaultHandlerTimeout bounds one delivery when the consumer declares none.
const defaultHandlerTimeout = 30 * time.Second

// opTimeout bounds non-blocking Redis operations of the consumer loop.
const opTimeout = 10 * time.Second

// Observer receives the consumer events behind the metrics of spec 9.1. The
// otel package implements it; NopObserver is the default.
type Observer interface {
	// LeaseAcquired is called when this node takes a partition (mediator.lease.handovers).
	LeaseAcquired(group, topic string, partition int, epoch int64)
	// LeaseEnded is called when a lease is released, lost, or expired.
	LeaseEnded(group, topic string, partition int, reason string)
	// LeasesOwned reports the owned partition count per scope after each tick (mediator.lease.owned).
	LeasesOwned(group, topic string, owned int)
	// Processed counts one delivery outcome (mediator.consumer.processed).
	Processed(group, outcome string)
	// Halted reports a partition halted on a poison entry under StrictOrder
	// (mediator.consumer.halted).
	Halted(group, topic string, partition int, halted bool)
	// Pending reports the pending count and the age of the oldest pending
	// entry of an owned partition (mediator.consumer.pending, mediator.consumer.lag).
	Pending(group, topic string, partition int, pending int64, oldest time.Duration)
	// DLQSize reports the length of a group's dead-letter stream (mediator.dlq.size).
	DLQSize(group string, size int64)
}

// NopObserver ignores every event.
type NopObserver struct{}

// LeaseAcquired ignores the event.
func (NopObserver) LeaseAcquired(string, string, int, int64) {}

// LeaseEnded ignores the event.
func (NopObserver) LeaseEnded(string, string, int, string) {}

// LeasesOwned ignores the gauge.
func (NopObserver) LeasesOwned(string, string, int) {}

// Processed ignores the count.
func (NopObserver) Processed(string, string) {}

// Halted ignores the gauge.
func (NopObserver) Halted(string, string, int, bool) {}

// Pending ignores the gauges.
func (NopObserver) Pending(string, string, int, int64, time.Duration) {}

// DLQSize ignores the gauge.
func (NopObserver) DLQSize(string, int64) {}

// ConsumersOption configures NewConsumers.
type ConsumersOption func(*Consumers)

// WithClock sets the time source of the lease loop and the retry backoff.
// Tests pass a testkit clock or run inside a synctest bubble.
func WithClock(c testkit.Clock) ConsumersOption {
	return func(cs *Consumers) {
		if c != nil {
			cs.clock = c
		}
	}
}

// WithObserver installs the metrics hook.
func WithObserver(o Observer) ConsumersOption {
	return func(cs *Consumers) {
		if o != nil {
			cs.obs = o
		}
	}
}

// WithLogger overrides the logger (default: the mediator's).
func WithLogger(l *slog.Logger) ConsumersOption {
	return func(cs *Consumers) {
		if l != nil {
			cs.logger = l
		}
	}
}

// withLeaseStore replaces the Redis lease store (tests).
func withLeaseStore(s leaseStore) ConsumersOption {
	return func(cs *Consumers) { cs.store = s }
}

// withBackoff replaces the retry schedule (tests).
func withBackoff(b backoff) ConsumersOption {
	return func(cs *Consumers) { cs.backoff = b }
}

// ScopeKey identifies a (group, topic) pair in Stats.
type ScopeKey struct {
	Group, Topic string
}

// OutcomeKey identifies a (group, outcome) pair in Stats.
type OutcomeKey struct {
	Group, Outcome string
}

// PartitionStats describes one partition this node owns.
type PartitionStats struct {
	Group     string
	Topic     string
	Partition int
	Epoch     int64
	// Pending is the group's pending count on the partition (all consumers).
	Pending int64
	// OldestAge is the age of the oldest pending entry, from its stream ID.
	OldestAge time.Duration
	// Halted is set while the partition is parked on a poison entry.
	Halted bool
	// HaltedID is the stream ID of the poison entry while halted.
	HaltedID string
}

// Stats is a snapshot of the consumer state of this node.
type Stats struct {
	NodeID  string
	Running bool
	// Partitions lists the owned leases sorted by group, topic, partition.
	Partitions []PartitionStats
	// Owned counts owned leases per scope.
	Owned map[ScopeKey]int
	// Handovers counts lease acquisitions per scope since start; leases are
	// acquired only at rebalance and handover.
	Handovers map[ScopeKey]int64
	// Processed counts deliveries per outcome.
	Processed map[OutcomeKey]int64
	// DLQSize is the length of each group's dead-letter stream at the last refresh.
	DLQSize map[string]int64
	// Halted is the number of halted partitions.
	Halted int
}

type pendingStat struct {
	pending int64
	oldest  time.Duration
}

// Consumers is the mediator.Component that owns partition leases, reads the
// partition streams of every consumer group registered on this node, and
// runs the consumer pipeline for each entry (spec 7.2 and 7.3). One instance
// serves every group of the node; each (group, topic, partition) has its own
// lease and reader.
type Consumers struct {
	m       *mediator.Mediator
	client  *redis.Client
	cfg     Config
	keys    Keys
	fencing pg.FencingSource
	store   leaseStore
	clock   testkit.Clock
	logger  *slog.Logger
	obs     Observer
	node    string
	streams *Streams
	backoff backoff
	lm      *leaseManager

	// regs maps group -> event name -> registration.
	regs   map[string]map[string]mediator.ConsumerInfo
	scopes []leaseScope

	running  atomic.Bool
	stopping atomic.Bool
	idle     atomic.Bool // no registrations: nothing to lease
	base     context.Context
	readCtx  context.Context
	readStop context.CancelFunc
	wg       sync.WaitGroup

	mu        sync.Mutex
	halted    map[leaseKey]string
	processed map[OutcomeKey]int64
	pending   map[leaseKey]pendingStat
	dlq       map[string]int64
}

var _ mediator.Component = (*Consumers)(nil)

// NewConsumers builds the consumer component for every durable consumer
// registered on m. fencing issues the lease epochs (spec 7.2); pg.Store
// satisfies it. The mediator must be built.
func NewConsumers(m *mediator.Mediator, client *redis.Client, cfg Config, fencing pg.FencingSource, opts ...ConsumersOption) *Consumers {
	if cfg.NodeID == "" {
		cfg.NodeID = m.NodeID()
	}
	cfg = cfg.WithDefaults()
	c := &Consumers{
		m:         m,
		client:    client,
		cfg:       cfg,
		keys:      cfg.Keys(),
		fencing:   fencing,
		store:     redisLeaseStore{client: client},
		clock:     testkit.RealClock{},
		logger:    m.Logger(),
		obs:       NopObserver{},
		node:      cfg.NodeID,
		streams:   NewStreams(client, cfg),
		backoff:   defaultBackoff,
		regs:      map[string]map[string]mediator.ConsumerInfo{},
		halted:    map[leaseKey]string{},
		processed: map[OutcomeKey]int64{},
		pending:   map[leaseKey]pendingStat{},
		dlq:       map[string]int64{},
	}
	for _, o := range opts {
		o(c)
	}
	seen := map[leaseScope]bool{}
	for _, reg := range m.ConsumerRegistrations() {
		if c.regs[reg.Group] == nil {
			c.regs[reg.Group] = map[string]mediator.ConsumerInfo{}
		}
		c.regs[reg.Group][reg.EventName] = reg
		sc := leaseScope{reg.Group, reg.Topic}
		if !seen[sc] {
			seen[sc] = true
			c.scopes = append(c.scopes, sc)
		}
	}
	sort.Slice(c.scopes, func(i, j int) bool {
		if c.scopes[i].group != c.scopes[j].group {
			return c.scopes[i].group < c.scopes[j].group
		}
		return c.scopes[i].topic < c.scopes[j].topic
	})
	c.lm = newLeaseManager(c.store, c.fencing, c.clock, c.keys, c.node, cfg.LeaseTTL, cfg.LeaseRenew, cfg.PartitionsPerTopic, c.scopes, c.logger)
	c.lm.onAcquire = c.startWorker
	c.lm.onEnd = func(l *lease, reason string) {
		c.obs.LeaseEnded(l.key.group, l.key.topic, l.key.partition, reason)
	}
	return c
}

// NodeID returns the consumer name used in Redis.
func (c *Consumers) NodeID() string { return c.node }

// Scopes returns the (group, topic) pairs this node leases, sorted.
func (c *Consumers) Scopes() []ScopeKey {
	out := make([]ScopeKey, len(c.scopes))
	for i, sc := range c.scopes {
		out[i] = ScopeKey{sc.group, sc.topic}
	}
	return out
}

// Run checks the eviction policy, starts the lease loop, and serves owned
// partitions until ctx is done. Shutdown lets each worker finish and
// acknowledge its current entry, then releases every lease.
func (c *Consumers) Run(ctx context.Context) error {
	if !c.cfg.AssumeNoEviction {
		if err := CheckEviction(ctx, c.client); err != nil {
			return err
		}
	}
	c.base = context.WithoutCancel(ctx)
	c.lm.base = c.base
	if len(c.scopes) == 0 {
		c.idle.Store(true)
		c.running.Store(true)
		defer c.running.Store(false)
		c.logger.Info("consumers idle: no durable consumers registered", "node", c.node)
		<-ctx.Done()
		return nil
	}
	c.readCtx, c.readStop = context.WithCancel(c.base)
	loopCtx, stopLoop := context.WithCancel(c.base)
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		c.lm.run(loopCtx)
	}()
	statsDone := make(chan struct{})
	go func() {
		defer close(statsDone)
		c.statsLoop(loopCtx)
	}()
	c.running.Store(true)
	c.logger.Info("consumers started", "node", c.node, "scopes", len(c.scopes), "partitions", c.cfg.PartitionsPerTopic)

	<-ctx.Done()
	c.logger.Info("consumers stopping", "node", c.node)
	c.lm.stopAcquiring()
	c.stopping.Store(true)
	c.readStop()
	c.wg.Wait()
	relCtx, cancel := context.WithTimeout(c.base, opTimeout)
	c.lm.releaseAll(relCtx)
	cancel()
	stopLoop()
	<-loopDone
	<-statsDone
	c.running.Store(false)
	c.logger.Info("consumers stopped", "node", c.node)
	return nil
}

// Healthy returns nil while the lease loop runs and no partition is halted.
func (c *Consumers) Healthy() error {
	if !c.running.Load() {
		return errors.New("redisx: consumers not running")
	}
	if c.idle.Load() {
		return nil
	}
	if last := c.lm.lastTickTime(); !last.IsZero() && c.clock.Now().Sub(last) > 3*c.cfg.LeaseRenew {
		return fmt.Errorf("redisx: lease loop stalled for %s", c.clock.Now().Sub(last))
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.halted) > 0 {
		keys := make([]string, 0, len(c.halted))
		for k, id := range c.halted {
			keys = append(keys, k.String()+"@"+id)
		}
		sort.Strings(keys)
		return fmt.Errorf("redisx: %d partition(s) halted on poison entries: %v", len(keys), keys)
	}
	return nil
}

// Stats returns a snapshot of the consumer state.
func (c *Consumers) Stats() Stats {
	st := Stats{
		NodeID:    c.node,
		Running:   c.running.Load(),
		Owned:     map[ScopeKey]int{},
		Handovers: map[ScopeKey]int64{},
		Processed: map[OutcomeKey]int64{},
		DLQSize:   map[string]int64{},
	}
	for sc, n := range c.lm.ownedCount() {
		st.Owned[ScopeKey{sc.group, sc.topic}] = n
	}
	for sc, n := range c.lm.handoverCounts() {
		st.Handovers[ScopeKey{sc.group, sc.topic}] = n
	}
	c.mu.Lock()
	for k, v := range c.processed {
		st.Processed[k] = v
	}
	for g, n := range c.dlq {
		st.DLQSize[g] = n
	}
	for _, l := range c.lm.snapshot() {
		ps := PartitionStats{Group: l.key.group, Topic: l.key.topic, Partition: l.key.partition, Epoch: l.epoch}
		if p, ok := c.pending[l.key]; ok {
			ps.Pending, ps.OldestAge = p.pending, p.oldest
		}
		if id, ok := c.halted[l.key]; ok {
			ps.Halted, ps.HaltedID = true, id
		}
		st.Partitions = append(st.Partitions, ps)
	}
	st.Halted = len(c.halted)
	c.mu.Unlock()
	return st
}

func (c *Consumers) count(group, outcome string) {
	c.mu.Lock()
	c.processed[OutcomeKey{group, outcome}]++
	c.mu.Unlock()
	c.obs.Processed(group, outcome)
}

func (c *Consumers) setHalted(key leaseKey, id string, halted bool) {
	c.mu.Lock()
	if halted {
		c.halted[key] = id
	} else {
		delete(c.halted, key)
	}
	c.mu.Unlock()
	c.obs.Halted(key.group, key.topic, key.partition, halted)
}

// opCtx returns a context for a non-blocking Redis operation that must
// complete even during shutdown (acks, releases).
func (c *Consumers) opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(c.base, opTimeout)
}

// startWorker is the lease manager's onAcquire callback. It runs under the
// manager's lock so wg.Add cannot race with the shutdown wg.Wait.
func (c *Consumers) startWorker(l *lease) {
	c.obs.LeaseAcquired(l.key.group, l.key.topic, l.key.partition, l.epoch)
	if c.stopping.Load() {
		return
	}
	w := &partitionWorker{
		c: c, l: l, group: l.key.group, topic: l.key.topic, partition: l.key.partition,
		stream: c.keys.Stream(l.key.topic, l.key.partition),
	}
	c.wg.Add(1)
	go w.run()
}

// statsLoop refreshes pending counts and DLQ sizes every LeaseRenew and
// publishes the owned-lease gauges.
func (c *Consumers) statsLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.clock.After(c.cfg.LeaseRenew):
		}
		c.refreshStats(ctx)
	}
}

func (c *Consumers) refreshStats(ctx context.Context) {
	octx, cancel := context.WithTimeout(ctx, opTimeout)
	defer cancel()
	now := c.clock.Now()
	for _, l := range c.lm.snapshot() {
		p, err := c.client.XPending(octx, c.keys.Stream(l.key.topic, l.key.partition), l.key.group).Result()
		if err != nil {
			continue
		}
		var oldest time.Duration
		if p.Count > 0 {
			if ms, ok := streamIDMillis(p.Lower); ok {
				oldest = now.Sub(time.UnixMilli(ms))
				if oldest < 0 {
					oldest = 0
				}
			}
		}
		c.mu.Lock()
		c.pending[l.key] = pendingStat{pending: p.Count, oldest: oldest}
		c.mu.Unlock()
		c.obs.Pending(l.key.group, l.key.topic, l.key.partition, p.Count, oldest)
	}
	for sc, n := range c.lm.ownedCount() {
		c.obs.LeasesOwned(sc.group, sc.topic, n)
	}
	for _, g := range c.lm.groups() {
		n, err := c.client.XLen(octx, c.keys.DLQ(g)).Result()
		if err != nil {
			continue
		}
		c.mu.Lock()
		c.dlq[g] = n
		c.mu.Unlock()
		c.obs.DLQSize(g, n)
	}
}

type consumerNodeKey struct{}

func withConsumerNode(ctx context.Context, node string) context.Context {
	return context.WithValue(ctx, consumerNodeKey{}, node)
}

// ConsumerNodeFrom returns the node ID of the Consumers instance delivering
// the current event, or "" outside a delivery.
func ConsumerNodeFrom(ctx context.Context) string {
	n, _ := ctx.Value(consumerNodeKey{}).(string)
	return n
}

// readPhase is the position in the loop of spec 7.2.
type readPhase int

const (
	phaseClaimed readPhase = iota // XAUTOCLAIM until empty
	phasePending                  // XREADGROUP from 0 until empty
	phaseNew                      // XREADGROUP > with BLOCK
)

type handleResult int

const (
	resContinue handleResult = iota // move to the next entry
	resStop                         // stop the worker (lease lost or shutdown)
	resHalt                         // StrictOrder poison entry: park
)

// partitionWorker reads one owned partition for one group.
type partitionWorker struct {
	c         *Consumers
	l         *lease
	group     string
	topic     string
	partition int
	stream    string
	lastClaim time.Time
}

func (w *partitionWorker) key() leaseKey { return w.l.key }

func (w *partitionWorker) run() {
	defer w.c.wg.Done()
	c := w.c
	log := c.logger.With("group", w.group, "stream", w.stream, "node", c.node, "epoch", w.l.epoch)
	if !w.ensureGroup(log) {
		return
	}
	phase := phaseClaimed
	cursor := "0-0"
	for {
		if w.stopNow() {
			return
		}
		var msgs []redis.XMessage
		var err error
		needCounts := true
		switch phase {
		case phaseClaimed:
			msgs, cursor, err = w.autoclaim(cursor)
			if err == nil && len(msgs) == 0 {
				if cursor != "0-0" {
					continue
				}
				w.lastClaim = c.clock.Now()
				foreign, ferr := w.foreignPending()
				if ferr == nil && foreign > 0 {
					// Entries delivered to another consumer and not yet idle
					// for ClaimMinIdle: wait for them rather than read past
					// them, which would reorder the key (spec 7.2).
					log.Debug("waiting for foreign pending entries", "count", foreign)
					if !w.wait(c.cfg.ReadBlock) {
						return
					}
					continue
				}
				phase = phasePending
				continue
			}
		case phasePending:
			msgs, err = w.read("0", -1)
			if err == nil && len(msgs) == 0 {
				phase = phaseNew
				continue
			}
		case phaseNew:
			if c.clock.Now().Sub(w.lastClaim) >= c.cfg.ClaimMinIdle {
				// Periodic claim pass: picks up entries a stale owner read
				// after the handover and entries whose XACK failed.
				phase, cursor = phaseClaimed, "0-0"
				continue
			}
			needCounts = false
			msgs, err = w.read(">", c.cfg.ReadBlock)
		}
		if err != nil {
			if w.stopNow() {
				return
			}
			if isNoGroup(err) {
				if !w.ensureGroup(log) {
					return
				}
				continue
			}
			log.Warn("stream read failed", "phase", int(phase), "error", err)
			if !w.wait(c.backoff.jittered(1)) {
				return
			}
			continue
		}
		if len(msgs) == 0 {
			continue
		}
		if !w.process(log, msgs, needCounts) {
			return
		}
	}
}

// stopNow reports whether the worker must exit: shutdown or lease end. A
// surplus lease is released by the manager itself while the worker is idle,
// or by finish right after the in-flight entry, so the worker only ever
// observes the canceled lease context.
func (w *partitionWorker) stopNow() bool {
	return w.c.stopping.Load() || w.l.ctx.Err() != nil
}

// wait sleeps d unless shutdown or lease loss interrupts; it returns false
// when interrupted.
func (w *partitionWorker) wait(d time.Duration) bool {
	select {
	case <-w.c.clock.After(d):
		return true
	case <-w.c.readCtx.Done():
		return false
	case <-w.l.ctx.Done():
		return false
	}
}

func (w *partitionWorker) ensureGroup(log *slog.Logger) bool {
	for {
		ctx, cancel := w.c.opCtx()
		err := w.c.streams.EnsureGroups(ctx, w.topic, w.partition, []string{w.group})
		cancel()
		if err == nil {
			return true
		}
		log.Warn("consumer group creation failed", "error", err)
		if !w.wait(w.c.backoff.jittered(2)) {
			return false
		}
	}
}

func (w *partitionWorker) autoclaim(cursor string) ([]redis.XMessage, string, error) {
	ctx, cancel := context.WithTimeout(w.c.readCtx, opTimeout)
	defer cancel()
	if err := testkit.Fault(ctx, "redis.xautoclaim"); err != nil {
		return nil, cursor, err
	}
	msgs, next, err := w.c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: w.stream, Group: w.group, Consumer: w.c.node,
		MinIdle: w.c.cfg.ClaimMinIdle, Start: cursor, Count: int64(w.c.cfg.ReadBatch),
	}).Result()
	if err != nil {
		return nil, cursor, err
	}
	if next == "" {
		next = "0-0"
	}
	return msgs, next, nil
}

// read is XREADGROUP from id; block < 0 means no BLOCK clause.
func (w *partitionWorker) read(id string, block time.Duration) ([]redis.XMessage, error) {
	timeout := opTimeout
	if block > 0 {
		timeout = block + opTimeout
	}
	ctx, cancel := context.WithTimeout(w.c.readCtx, timeout)
	defer cancel()
	if err := testkit.Fault(ctx, "redis.xreadgroup"); err != nil {
		return nil, err
	}
	res, err := w.c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: w.group, Consumer: w.c.node, Streams: []string{w.stream, id},
		Count: int64(w.c.cfg.ReadBatch), Block: block,
	}).Result()
	if err = nonNil(err); err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}
	return res[0].Messages, nil
}

// foreignPending counts pending entries of the group held by other consumers.
func (w *partitionWorker) foreignPending() (int64, error) {
	ctx, cancel := w.c.opCtx()
	defer cancel()
	p, err := w.c.client.XPending(ctx, w.stream, w.group).Result()
	if err != nil {
		return 0, err
	}
	var n int64
	for consumer, count := range p.Consumers {
		if consumer != w.c.node {
			n += count
		}
	}
	return n, nil
}

// deliveryCounts returns the XPENDING delivery counter of each entry.
func (w *partitionWorker) deliveryCounts(msgs []redis.XMessage) map[string]int64 {
	out := map[string]int64{}
	if len(msgs) == 0 {
		return out
	}
	ctx, cancel := w.c.opCtx()
	defer cancel()
	res, err := w.c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: w.stream, Group: w.group, Start: msgs[0].ID, End: msgs[len(msgs)-1].ID,
		Count: int64(len(msgs)), Consumer: w.c.node,
	}).Result()
	if err != nil {
		return out
	}
	for _, p := range res {
		out[p.ID] = p.RetryCount
	}
	return out
}

func (w *partitionWorker) ack(id string) error {
	ctx, cancel := w.c.opCtx()
	defer cancel()
	if err := testkit.Fault(ctx, "redis.xack"); err != nil {
		return err
	}
	return w.c.client.XAck(ctx, w.stream, w.group, id).Err()
}

// process delivers a batch in order. It returns false when the worker must exit.
func (w *partitionWorker) process(log *slog.Logger, msgs []redis.XMessage, needCounts bool) bool {
	c := w.c
	var counts map[string]int64
	if needCounts {
		counts = w.deliveryCounts(msgs)
	}
	for i := 0; i < len(msgs); {
		msg := msgs[i]
		if c.stopping.Load() || w.l.ctx.Err() != nil {
			return false
		}
		if !w.l.begin(c.clock.Now(), c.cfg.LeaseTTL) {
			if w.l.ctx.Err() != nil || w.l.isReleasing() {
				// Lease ended or about to be released: the remaining entries
				// stay pending for the next owner.
				return false
			}
			// Stale: renewals have been failing. Wait for the next tick,
			// which either renews or ends the lease.
			log.Warn("lease not renewed within ttl; pausing partition")
			if !w.wait(c.cfg.LeaseRenew) {
				return false
			}
			continue
		}
		res := w.handle(log, msg, counts[msg.ID])
		ctx, cancel := c.opCtx()
		c.lm.finish(ctx, w.l)
		cancel()
		switch res {
		case resStop:
			return false
		case resHalt:
			if !w.park(log, msg.ID) {
				return false
			}
			// The operator acknowledged the poison entry: continue.
		case resContinue:
			// Move to the next entry.
		}
		i++
	}
	return true
}

// handle delivers one entry with in-place retries (spec 7.3).
func (w *partitionWorker) handle(log *slog.Logger, msg redis.XMessage, delivered int64) handleResult {
	c := w.c
	env, payload, _, decErr := DecodeEntry(msg.Values)
	base := int(delivered)
	if base < 1 {
		base = 1
	}
	attempt := base
	var reg mediator.ConsumerInfo
	known := false
	if decErr == nil {
		reg, known = c.regs[w.group][env.Type]
		if !known {
			log.Debug("no consumer for event type in group; acknowledging", "id", msg.ID, "event", env.Type)
			if err := w.ack(msg.ID); err != nil {
				log.Warn("xack failed; entry will be redelivered", "id", msg.ID, "error", err)
				return resContinue
			}
			c.count(w.group, OutcomeSkip)
			return resContinue
		}
	}
	maxAttempts := c.cfg.MaxAttempts
	if known && reg.MaxAttempts > 0 {
		maxAttempts = reg.MaxAttempts
	}
	timeout := defaultHandlerTimeout
	if known && reg.HandlerTimeout > 0 {
		timeout = reg.HandlerTimeout
	}
	for {
		var err error
		if decErr != nil {
			err = mediator.Wrap(mediator.CodeBadRequest, "decode stream entry", decErr)
		} else {
			state := &mediator.ConsumerState{Attempt: attempt}
			ctx := mediator.WithFencingToken(w.l.ctx, w.l.epoch)
			ctx = mediator.WithConsumerState(ctx, state)
			ctx = withConsumerNode(ctx, c.node)
			ctx, cancel := context.WithTimeout(ctx, timeout)
			err = c.m.Deliver(ctx, w.group, env, payload)
			cancel()
			if err == nil {
				if aerr := w.ack(msg.ID); aerr != nil {
					log.Warn("xack failed; entry will be redelivered and deduplicated", "id", msg.ID, "error", aerr)
					return resContinue
				}
				outcome := OutcomeOK
				if state.Duplicate {
					outcome = OutcomeDedup
				}
				c.count(w.group, outcome)
				log.Debug("entry processed", "id", msg.ID, "event", env.ID, "key", env.StreamKey, "seq", env.Seq, "outcome", outcome, "attempt", attempt)
				return resContinue
			}
		}
		if w.l.ctx.Err() != nil {
			log.Info("delivery canceled by lease loss; entry stays pending", "id", msg.ID)
			return resStop
		}
		if c.stopping.Load() {
			return resStop
		}
		c.count(w.group, OutcomeError)
		limit := maxAttempts
		transient := mediator.IsTransient(err)
		if !transient && limit > nonTransientAttempts {
			limit = nonTransientAttempts
		}
		log.Warn("consumer delivery failed", "id", msg.ID, "event", env.ID, "key", env.StreamKey, "seq", env.Seq,
			"attempt", attempt, "limit", limit, "transient", transient, "error", err)
		if attempt >= limit {
			if known && reg.StrictOrder {
				log.Error("partition halted on poison entry (StrictOrder)", "id", msg.ID, "event", env.ID, "attempts", attempt, "error", err)
				return resHalt
			}
			if derr := w.deadLetter(msg, err, attempt); derr != nil {
				log.Warn("dead-letter failed; entry stays pending", "id", msg.ID, "error", derr)
				return resContinue
			}
			c.count(w.group, OutcomeDLQ)
			log.Error("entry dead-lettered", "id", msg.ID, "event", env.ID, "key", env.StreamKey, "seq", env.Seq, "attempts", attempt, "error", err)
			return resContinue
		}
		if !w.wait(c.backoff.jittered(attempt - base + 1)) {
			return resStop
		}
		attempt++
	}
}

// deadLetter copies the entry to the group's DLQ stream and acknowledges it.
// Fault points redis.xadd.dlq and redis.xack.
func (w *partitionWorker) deadLetter(msg redis.XMessage, cause error, attempts int) error {
	ctx, cancel := w.c.opCtx()
	defer cancel()
	if err := testkit.Fault(ctx, "redis.xadd.dlq"); err != nil {
		return err
	}
	fields := dlqFields(msg.Values, w.stream, msg.ID, w.group, w.c.node, cause, attempts, w.c.clock.Now())
	if err := w.c.client.XAdd(ctx, &redis.XAddArgs{Stream: w.c.keys.DLQ(w.group), Values: fields}).Err(); err != nil {
		return err
	}
	if err := testkit.FaultAfter(ctx, "redis.xadd.dlq"); err != nil {
		return err
	}
	return w.ack(msg.ID)
}

// park holds the partition while the poison entry is pending. It returns
// true when an operator acknowledged the entry (PartitionSkip) and the
// worker may continue, false on shutdown or lease loss.
func (w *partitionWorker) park(log *slog.Logger, id string) bool {
	w.c.setHalted(w.key(), id, true)
	defer w.c.setHalted(w.key(), id, false)
	for {
		if !w.wait(w.c.cfg.LeaseRenew) {
			return false
		}
		ctx, cancel := w.c.opCtx()
		pend, err := w.c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: w.stream, Group: w.group, Start: id, End: id, Count: 1,
		}).Result()
		cancel()
		if err == nil && len(pend) == 0 {
			log.Info("halted entry acknowledged by operator; resuming partition", "id", id)
			return true
		}
	}
}

// Dead-letter fields added to the copied entry. Everything else is the
// original entry verbatim so DLQRequeue can re-add it.
const (
	DLQFieldError    = "dlq_error"
	DLQFieldAttempts = "dlq_attempts"
	DLQFieldStreamID = "dlq_stream_id"
	DLQFieldStream   = "dlq_stream"
	DLQFieldGroup    = "dlq_group"
	DLQFieldNode     = "dlq_node"
	DLQFieldAt       = "dlq_at"
)

// dlqFields builds the dead-letter record: the original fields plus the last
// error, attempt count, original stream and ID, group, node, and time.
func dlqFields(orig map[string]any, stream, id, group, node string, cause error, attempts int, now time.Time) map[string]any {
	out := make(map[string]any, len(orig)+7)
	for k, v := range orig {
		out[k] = v
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	if len(msg) > 4096 {
		msg = msg[:4096]
	}
	out[DLQFieldError] = msg
	out[DLQFieldAttempts] = strconv.Itoa(attempts)
	out[DLQFieldStreamID] = id
	out[DLQFieldStream] = stream
	out[DLQFieldGroup] = group
	out[DLQFieldNode] = node
	out[DLQFieldAt] = now.UTC().Format(time.RFC3339Nano)
	return out
}
