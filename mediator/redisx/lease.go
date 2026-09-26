package redisx

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// leaseStore is the small interface the lease manager needs from Redis. The
// Redis implementation is redisLeaseStore; tests use an in-memory fake so the
// state machine runs deterministically under testing/synctest.
type leaseStore interface {
	// acquire is SET key value NX PX ttl. Fault point redis.lease.acquire.
	acquire(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// renew extends the TTL only while the key still holds value.
	// Fault point redis.lease.renew.
	renew(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	// release deletes the key only while it still holds value.
	// Fault point redis.lease.release.
	release(ctx context.Context, key, value string) (bool, error)
	// beat records the node in the membership zset with score now, prunes
	// members older than ttl, and returns the live members.
	// Fault point redis.members.beat.
	beat(ctx context.Context, membersKey, node string, now time.Time, ttl time.Duration) ([]string, error)
}

// Lua scripts of the lease protocol (spec 7.2). Both compare the stored
// value with the caller's "<node>:<epoch>" so a stale owner can neither
// extend nor delete a lease that moved on.
var (
	leaseRenewScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0`)
	leaseReleaseScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`)
)

// redisLeaseStore is the production leaseStore.
type redisLeaseStore struct {
	client *redis.Client
}

func (s redisLeaseStore) acquire(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if err := testkit.Fault(ctx, "redis.lease.acquire"); err != nil {
		return false, err
	}
	ok, err := s.client.SetNX(ctx, key, value, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redisx: lease acquire %s: %w", key, err)
	}
	return ok, nil
}

func (s redisLeaseStore) renew(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if err := testkit.Fault(ctx, "redis.lease.renew"); err != nil {
		return false, err
	}
	n, err := leaseRenewScript.Run(ctx, s.client, []string{key}, value, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("redisx: lease renew %s: %w", key, err)
	}
	return n == 1, nil
}

func (s redisLeaseStore) release(ctx context.Context, key, value string) (bool, error) {
	if err := testkit.Fault(ctx, "redis.lease.release"); err != nil {
		return false, err
	}
	n, err := leaseReleaseScript.Run(ctx, s.client, []string{key}, value).Int64()
	if err != nil {
		return false, fmt.Errorf("redisx: lease release %s: %w", key, err)
	}
	return n == 1, nil
}

func (s redisLeaseStore) beat(ctx context.Context, membersKey, node string, now time.Time, ttl time.Duration) ([]string, error) {
	if err := testkit.Fault(ctx, "redis.members.beat"); err != nil {
		return nil, err
	}
	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, membersKey, redis.Z{Score: float64(now.UnixMilli()), Member: node})
	pipe.ZRemRangeByScore(ctx, membersKey, "-inf", "("+strconv.FormatInt(now.Add(-ttl).UnixMilli(), 10))
	live := pipe.ZRange(ctx, membersKey, 0, -1)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("redisx: members beat %s: %w", membersKey, err)
	}
	return live.Val(), nil
}

// leaseScope is one (group, topic) pair whose partitions are leased.
type leaseScope struct {
	group, topic string
}

// leaseKey identifies one partition lease.
type leaseKey struct {
	group, topic string
	partition    int
}

func (k leaseKey) scope() leaseScope { return leaseScope{k.group, k.topic} }

func (k leaseKey) String() string {
	return fmt.Sprintf("%s/%s/p%d", k.group, k.topic, k.partition)
}

// lease is one owned partition lease. ctx is canceled when the lease is lost
// or released, which cancels the in-flight handler of that partition.
type lease struct {
	key    leaseKey
	epoch  int64
	value  string
	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	busy      bool      // a message is in flight
	releasing bool      // surplus: release after the current message
	ended     bool      // lost or released
	lastRenew time.Time // last successful acquire or renew
}

// begin marks a message in flight. It returns false when the lease is
// releasing, ended, or stale (last renewal older than ttl), in which case the
// caller must not process the entry.
func (l *lease) begin(now time.Time, ttl time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.releasing || l.ended || now.Sub(l.lastRenew) >= ttl {
		return false
	}
	l.busy = true
	return true
}

// end clears the in-flight mark and reports whether a deferred release is due.
func (l *lease) end() (releaseNow bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.busy = false
	return l.releasing && !l.ended
}

// markRelease flags the lease as surplus and reports whether it can be
// released right away (no message in flight).
func (l *lease) markRelease() (releaseNow bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.releasing = true
	return !l.busy && !l.ended
}

// fresh reports whether the lease is live and was renewed within ttl.
func (l *lease) fresh(now time.Time, ttl time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.ended && now.Sub(l.lastRenew) < ttl
}

func (l *lease) isReleasing() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.releasing
}

// Lease end reasons reported to the Observer.
const (
	LeaseEndSurplus  = "surplus"  // released to rebalance
	LeaseEndShutdown = "shutdown" // released at shutdown
	LeaseEndLost     = "lost"     // renewal found another owner
	LeaseEndExpired  = "expired"  // renewal failed until the TTL passed
)

// leaseManager implements spec 7.2: membership heartbeat, desired partition
// count ceil(P / liveMembers), acquisition with a fencing epoch, renewal,
// graceful surplus release, and loss detection. It is driven by tick, which
// run calls every renew interval.
type leaseManager struct {
	store      leaseStore
	fencing    pg.FencingSource
	clock      testkit.Clock
	keys       Keys
	node       string
	ttl        time.Duration
	renew      time.Duration
	partitions int
	scopes     []leaseScope
	logger     *slog.Logger
	base       context.Context // parent of lease contexts

	onAcquire func(*lease)
	onEnd     func(*lease, string)

	mu        sync.Mutex
	owned     map[leaseKey]*lease
	live      map[string]int // group -> live members
	handovers map[leaseScope]int64
	lastTick  time.Time
	stopped   bool // no more acquisitions
}

func newLeaseManager(store leaseStore, fencing pg.FencingSource, clock testkit.Clock, keys Keys, node string, ttl, renew time.Duration, partitions int, scopes []leaseScope, logger *slog.Logger) *leaseManager {
	lm := &leaseManager{
		store: store, fencing: fencing, clock: clock, keys: keys, node: node,
		ttl: ttl, renew: renew, partitions: partitions, scopes: scopes, logger: logger,
		base:      context.Background(),
		owned:     map[leaseKey]*lease{},
		live:      map[string]int{},
		handovers: map[leaseScope]int64{},
	}
	for _, sc := range scopes {
		lm.live[sc.group] = 1
	}
	return lm
}

// run ticks every renew interval until ctx is done. It does not release
// anything on exit; the owner calls releaseAll explicitly so that leases
// survive until the workers have drained.
func (lm *leaseManager) run(ctx context.Context) {
	for {
		lm.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-lm.clock.After(lm.renew):
		}
	}
}

// tick performs one round: heartbeat, renew, rebalance.
func (lm *leaseManager) tick(ctx context.Context) {
	now := lm.clock.Now()
	lm.beatAll(ctx, now)
	lm.renewAll(ctx, now)
	lm.rebalance(ctx)
	lm.mu.Lock()
	lm.lastTick = now
	lm.mu.Unlock()
}

func (lm *leaseManager) groups() []string {
	seen := map[string]bool{}
	var out []string
	for _, sc := range lm.scopes {
		if !seen[sc.group] {
			seen[sc.group] = true
			out = append(out, sc.group)
		}
	}
	return out
}

func (lm *leaseManager) beatAll(ctx context.Context, now time.Time) {
	for _, g := range lm.groups() {
		live, err := lm.store.beat(ctx, lm.keys.Members(g), lm.node, now, lm.ttl)
		if err != nil {
			lm.logger.Warn("membership heartbeat failed", "group", g, "node", lm.node, "error", err)
			continue
		}
		n := len(live)
		if n < 1 {
			n = 1
		}
		lm.mu.Lock()
		lm.live[g] = n
		lm.mu.Unlock()
	}
}

func (lm *leaseManager) renewAll(ctx context.Context, now time.Time) {
	for _, l := range lm.snapshot() {
		ok, err := lm.store.renew(ctx, lm.keys.Lease(l.key.group, l.key.topic, l.key.partition), l.value, lm.ttl)
		switch {
		case err != nil:
			l.mu.Lock()
			expired := now.Sub(l.lastRenew) >= lm.ttl
			l.mu.Unlock()
			if expired {
				lm.logger.Warn("lease renewal failed past ttl; dropping partition", "lease", l.key.String(), "epoch", l.epoch, "error", err)
				lm.end(l, LeaseEndExpired)
			} else {
				lm.logger.Warn("lease renewal failed; retrying at next tick", "lease", l.key.String(), "epoch", l.epoch, "error", err)
			}
		case !ok:
			lm.logger.Info("lease lost to another owner", "lease", l.key.String(), "epoch", l.epoch, "node", lm.node)
			lm.end(l, LeaseEndLost)
		default:
			l.mu.Lock()
			l.lastRenew = now
			l.mu.Unlock()
		}
	}
}

// rebalance releases surplus leases and acquires missing ones per scope.
func (lm *leaseManager) rebalance(ctx context.Context) {
	lm.mu.Lock()
	stopped := lm.stopped
	lm.mu.Unlock()
	if stopped {
		return
	}
	for _, sc := range lm.scopes {
		lm.mu.Lock()
		live := lm.live[sc.group]
		var mine []*lease
		ownedAny := map[int]bool{}
		for k, l := range lm.owned {
			if k.scope() != sc {
				continue
			}
			ownedAny[k.partition] = true
			if !l.isReleasing() {
				mine = append(mine, l)
			}
		}
		lm.mu.Unlock()
		desired := ceilDiv(lm.partitions, live)
		switch {
		case len(mine) > desired:
			sort.Slice(mine, func(i, j int) bool { return mine[i].key.partition > mine[j].key.partition })
			for _, l := range mine[:len(mine)-desired] {
				lm.logger.Info("releasing surplus lease", "lease", l.key.String(), "epoch", l.epoch, "desired", desired, "live", live)
				if l.markRelease() {
					lm.release(ctx, l, LeaseEndSurplus)
				}
			}
		case len(mine) < desired:
			var candidates []int
			for p := 0; p < lm.partitions; p++ {
				if !ownedAny[p] {
					candidates = append(candidates, p)
				}
			}
			rand.Shuffle(len(candidates), func(i, j int) { candidates[i], candidates[j] = candidates[j], candidates[i] }) //nolint:gosec // G404: spreads nodes over free partitions, not security-sensitive
			have := len(mine)
			for _, p := range candidates {
				if have >= desired || ctx.Err() != nil {
					break
				}
				if lm.acquire(ctx, leaseKey{sc.group, sc.topic, p}) {
					have++
				}
			}
		}
	}
}

// acquire takes a fencing epoch and tries SET NX. A failed SET burns a
// number, which is harmless (spec 7.2).
func (lm *leaseManager) acquire(ctx context.Context, key leaseKey) bool {
	epoch, err := lm.fencing.NextFencingToken(ctx)
	if err != nil {
		lm.logger.Warn("fencing token unavailable; not acquiring", "lease", key.String(), "error", err)
		return false
	}
	value := LeaseValue(lm.node, epoch)
	ok, err := lm.store.acquire(ctx, lm.keys.Lease(key.group, key.topic, key.partition), value, lm.ttl)
	if err != nil {
		lm.logger.Warn("lease acquire failed", "lease", key.String(), "error", err)
		return false
	}
	if !ok {
		return false
	}
	lctx, cancel := context.WithCancel(lm.base)
	l := &lease{key: key, epoch: epoch, value: value, ctx: lctx, cancel: cancel, lastRenew: lm.clock.Now()}
	lm.mu.Lock()
	if lm.stopped {
		lm.mu.Unlock()
		cancel()
		_, _ = lm.store.release(ctx, lm.keys.Lease(key.group, key.topic, key.partition), value)
		return false
	}
	lm.owned[key] = l
	lm.handovers[key.scope()]++
	onAcquire := lm.onAcquire
	lm.mu.Unlock()
	lm.logger.Info("lease acquired", "lease", key.String(), "epoch", epoch, "node", lm.node)
	if onAcquire != nil {
		onAcquire(l)
	}
	return true
}

// release performs the compare-and-delete and ends the lease.
func (lm *leaseManager) release(ctx context.Context, l *lease, reason string) {
	l.mu.Lock()
	already := l.ended
	l.mu.Unlock()
	if already {
		return
	}
	if _, err := lm.store.release(ctx, lm.keys.Lease(l.key.group, l.key.topic, l.key.partition), l.value); err != nil {
		lm.logger.Warn("lease release failed; it will expire", "lease", l.key.String(), "epoch", l.epoch, "error", err)
	} else {
		lm.logger.Info("lease released", "lease", l.key.String(), "epoch", l.epoch, "reason", reason)
	}
	lm.end(l, reason)
}

// end cancels the lease context, forgets the lease, and notifies the owner.
func (lm *leaseManager) end(l *lease, reason string) {
	l.mu.Lock()
	if l.ended {
		l.mu.Unlock()
		return
	}
	l.ended = true
	l.mu.Unlock()
	l.cancel()
	lm.mu.Lock()
	if cur, ok := lm.owned[l.key]; ok && cur == l {
		delete(lm.owned, l.key)
	}
	onEnd := lm.onEnd
	lm.mu.Unlock()
	if onEnd != nil {
		onEnd(l, reason)
	}
}

// finish is called by a worker after each message: it clears the busy mark
// and performs a deferred surplus release when one is due.
func (lm *leaseManager) finish(ctx context.Context, l *lease) {
	if l.end() {
		lm.release(ctx, l, LeaseEndSurplus)
	}
}

// stopAcquiring prevents further acquisitions; renewals continue so that
// draining workers keep their leases.
func (lm *leaseManager) stopAcquiring() {
	lm.mu.Lock()
	lm.stopped = true
	lm.mu.Unlock()
}

// releaseAll releases every owned lease (shutdown).
func (lm *leaseManager) releaseAll(ctx context.Context) {
	for _, l := range lm.snapshot() {
		lm.release(ctx, l, LeaseEndShutdown)
	}
}

// snapshot returns the owned leases sorted by key.
func (lm *leaseManager) snapshot() []*lease {
	lm.mu.Lock()
	out := make([]*lease, 0, len(lm.owned))
	for _, l := range lm.owned {
		out = append(out, l)
	}
	lm.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].key.String() < out[j].key.String() })
	return out
}

// ownedCount returns the number of owned leases per scope.
func (lm *leaseManager) ownedCount() map[leaseScope]int {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	out := map[leaseScope]int{}
	for k := range lm.owned {
		out[k.scope()]++
	}
	return out
}

// handoverCounts returns a copy of the acquisition counters per scope.
func (lm *leaseManager) handoverCounts() map[leaseScope]int64 {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	out := make(map[leaseScope]int64, len(lm.handovers))
	for k, v := range lm.handovers {
		out[k] = v
	}
	return out
}

func (lm *leaseManager) lastTickTime() time.Time {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.lastTick
}
