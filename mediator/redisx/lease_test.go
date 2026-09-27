package redisx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// fakeLeaseStore is an in-memory leaseStore with TTL expiry driven by a
// clock, so the lease state machine runs under synctest without Redis.
type fakeLeaseStore struct {
	clock testkit.Clock

	mu      sync.Mutex
	leases  map[string]fakeLease
	members map[string]map[string]time.Time
	calls   map[string]int

	acquireErr, renewErr, releaseErr, beatErr error

	// afterAcquire runs after a successful SET NX, before acquire returns:
	// tests use it to land a stop between the key write and the manager's
	// stopped check.
	afterAcquire func()
}

type fakeLease struct {
	value   string
	expires time.Time
}

func newFakeLeaseStore(clock testkit.Clock) *fakeLeaseStore {
	return &fakeLeaseStore{clock: clock, leases: map[string]fakeLease{}, members: map[string]map[string]time.Time{}, calls: map[string]int{}}
}

func (s *fakeLeaseStore) expire() {
	now := s.clock.Now()
	for k, l := range s.leases {
		if !l.expires.After(now) {
			delete(s.leases, k)
		}
	}
}

func (s *fakeLeaseStore) acquire(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	s.calls["acquire"]++
	if s.acquireErr != nil {
		s.mu.Unlock()
		return false, s.acquireErr
	}
	s.expire()
	if _, held := s.leases[key]; held {
		s.mu.Unlock()
		return false, nil
	}
	s.leases[key] = fakeLease{value: value, expires: s.clock.Now().Add(ttl)}
	hook := s.afterAcquire
	s.mu.Unlock()
	if hook != nil {
		hook()
	}
	return true, nil
}

func (s *fakeLeaseStore) renew(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls["renew"]++
	if s.renewErr != nil {
		return false, s.renewErr
	}
	s.expire()
	l, held := s.leases[key]
	if !held || l.value != value {
		return false, nil
	}
	l.expires = s.clock.Now().Add(ttl)
	s.leases[key] = l
	return true, nil
}

func (s *fakeLeaseStore) release(ctx context.Context, key, value string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls["release"]++
	if s.releaseErr != nil {
		return false, s.releaseErr
	}
	s.expire()
	l, held := s.leases[key]
	if !held || l.value != value {
		return false, nil
	}
	delete(s.leases, key)
	return true, nil
}

func (s *fakeLeaseStore) beat(_ context.Context, membersKey, node string, now time.Time, ttl time.Duration) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls["beat"]++
	if s.beatErr != nil {
		return nil, s.beatErr
	}
	m := s.members[membersKey]
	if m == nil {
		m = map[string]time.Time{}
		s.members[membersKey] = m
	}
	m[node] = now
	var live []string
	for n, at := range m {
		if now.Sub(at) >= ttl {
			delete(m, n)
			continue
		}
		live = append(live, n)
	}
	sort.Strings(live)
	return live, nil
}

// holder returns the node and epoch holding key, or "" when nobody does.
func (s *fakeLeaseStore) holder(key string) (string, int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire()
	l, ok := s.leases[key]
	if !ok {
		return "", 0
	}
	node, epoch, _ := ParseLeaseValue(l.value)
	return node, epoch
}

func (s *fakeLeaseStore) held() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expire()
	return len(s.leases)
}

// count returns how many times op has been called, under the lock: the
// manager goroutine increments the counters while a test reads them.
func (s *fakeLeaseStore) count(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[op]
}

// forget drops every lease key, as FLUSHALL or a restore from an older
// snapshot does; the owners learn at their next renewal.
func (s *fakeLeaseStore) forget() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases = map[string]fakeLease{}
}

// counterFencing is a pg.FencingSource backed by an atomic counter.
type counterFencing struct {
	n   atomic.Int64
	err error
}

func (f *counterFencing) NextFencingToken(context.Context) (int64, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.n.Add(1), nil
}

const (
	testTTL   = 15 * time.Second
	testRenew = 5 * time.Second
)

var testScopes = []leaseScope{{"g", "orders"}}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestManager(store leaseStore, fence *counterFencing, node string, partitions int) *leaseManager {
	return newLeaseManager(store, fence, testkit.RealClock{}, Keys{Prefix: "t"}, node, testTTL, testRenew, partitions, testScopes, quietLogger())
}

func epochsOf(lm *leaseManager) map[int]int64 {
	out := map[int]int64{}
	for _, l := range lm.snapshot() {
		out[l.key.partition] = l.epoch
	}
	return out
}

func TestLeaseManager_SingleNodeOwnsAll(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeLeaseStore(testkit.RealClock{})
		fence := &counterFencing{}
		a := newTestManager(store, fence, "a", 4)
		var acquired []int
		a.onAcquire = func(l *lease) { acquired = append(acquired, l.key.partition) }
		a.tick(context.Background())
		if n := len(a.snapshot()); n != 4 {
			t.Fatalf("owned %d, want 4", n)
		}
		if len(acquired) != 4 || store.held() != 4 {
			t.Fatalf("acquired %v held %d", acquired, store.held())
		}
		if h := a.handoverCounts()[testScopes[0]]; h != 4 {
			t.Fatalf("handovers %d", h)
		}
		if c := a.ownedCount()[testScopes[0]]; c != 4 {
			t.Fatalf("ownedCount %d", c)
		}
		// The snapshot is sorted by key, ascending.
		for i, l := range a.snapshot() {
			if l.key.partition != i {
				t.Fatalf("snapshot[%d] = %s, want p%d", i, l.key, i)
			}
		}
		seen := map[int64]bool{}
		for p, e := range epochsOf(a) {
			if seen[e] || e < 1 || e > 4 {
				t.Fatalf("partition %d epoch %d", p, e)
			}
			seen[e] = true
			if node, epoch := store.holder(Keys{Prefix: "t"}.Lease("g", "orders", p)); node != "a" || epoch != e {
				t.Fatalf("store holder p%d = %s:%d", p, node, epoch)
			}
		}
		// A second tick renews and changes nothing.
		time.Sleep(testRenew)
		a.tick(context.Background())
		if len(a.snapshot()) != 4 || a.handoverCounts()[testScopes[0]] != 4 || a.lastTickTime().IsZero() {
			t.Fatal("second tick changed ownership")
		}
	})
}

// TestLeaseManager_AcquiresExactlyDesired: with a live peer that owns
// nothing yet, every partition is free but the node takes only its desired
// share, ceil(P / live), and not one more.
func TestLeaseManager_AcquiresExactlyDesired(t *testing.T) {
	ctx := context.Background()
	store := newFakeLeaseStore(testkit.RealClock{})
	a := newTestManager(store, &counterFencing{}, "a", 4)
	if _, err := store.beat(ctx, a.keys.Members("g"), "b", time.Now(), testTTL); err != nil {
		t.Fatal(err)
	}
	a.tick(ctx) // live=2, desired=2, four free partitions
	if n := len(a.snapshot()); n != 2 {
		t.Fatalf("owned %d, want exactly the desired 2", n)
	}
	if held := store.held(); held != 2 {
		t.Fatalf("held %d, want 2", held)
	}
	if h := a.handoverCounts()[testScopes[0]]; h != 2 {
		t.Fatalf("handovers %d, want 2", h)
	}
	// A second tick with the same membership changes nothing.
	a.tick(ctx)
	if n := len(a.snapshot()); n != 2 {
		t.Fatalf("owned %d after a second tick, want 2", n)
	}
	a.stopAcquiring()
	a.releaseAll(ctx)
}

// logCapture is a slog.Handler that records every message it receives, so
// tests can assert which log line a branch produced.
type logCapture struct {
	mu   sync.Mutex
	msgs []string
}

func (h *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.msgs = append(h.msgs, r.Level.String()+" "+r.Message)
	h.mu.Unlock()
	return nil
}

func (h *logCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logCapture) WithGroup(string) slog.Handler      { return h }

// count returns how many recorded lines start with prefix ("LEVEL message").
func (h *logCapture) count(prefix string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, m := range h.msgs {
		if strings.HasPrefix(m, prefix) {
			n++
		}
	}
	return n
}

// TestLeaseManager_ReleaseLogsOutcome pins the log line of each release
// outcome: a successful compare-and-delete is reported as released with its
// reason, a store error as a failure that will expire.
func TestLeaseManager_ReleaseLogsOutcome(t *testing.T) {
	ctx := context.Background()
	store := newFakeLeaseStore(testkit.RealClock{})
	logs := &logCapture{}
	a := newTestManager(store, &counterFencing{}, "a", 2)
	a.logger = slog.New(logs)
	a.tick(ctx)
	leases := a.snapshot()
	if len(leases) != 2 {
		t.Fatalf("owned %d", len(leases))
	}
	a.release(ctx, leases[0], LeaseEndSurplus)
	if logs.count("INFO lease released") != 1 || logs.count("WARN lease release failed") != 0 {
		t.Fatalf("after a successful release: %v", logs.msgs)
	}
	store.releaseErr = errors.New("redis down")
	a.release(ctx, leases[1], LeaseEndShutdown)
	if logs.count("INFO lease released") != 1 || logs.count("WARN lease release failed") != 1 {
		t.Fatalf("after a failed release: %v", logs.msgs)
	}
	if len(a.snapshot()) != 0 || leases[1].ctx.Err() == nil {
		t.Fatal("a failed release must still end the lease")
	}
}

func TestLeaseManager_RebalanceOnJoin_TokensIncrease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newFakeLeaseStore(testkit.RealClock{})
		fence := &counterFencing{}
		a := newTestManager(store, fence, "a", 4)
		b := newTestManager(store, fence, "b", 4)
		var ends []string
		a.onEnd = func(l *lease, reason string) { ends = append(ends, reason) }
		a.tick(ctx)
		before := epochsOf(a)
		b.tick(ctx) // joins: live=2, desired=2, nothing free yet
		if len(b.snapshot()) != 0 {
			t.Fatal("b acquired before a released")
		}
		time.Sleep(testRenew)
		a.tick(ctx) // sees live=2, releases 2 surplus
		if n := len(a.snapshot()); n != 2 {
			t.Fatalf("a owns %d after rebalance, want 2", n)
		}
		if len(ends) != 2 || ends[0] != LeaseEndSurplus {
			t.Fatalf("end reasons %v", ends)
		}
		b.tick(ctx)
		if n := len(b.snapshot()); n != 2 {
			t.Fatalf("b owns %d, want 2", n)
		}
		after := epochsOf(b)
		for p, e := range after {
			if _, still := epochsOf(a)[p]; still {
				t.Fatalf("partition %d owned by both", p)
			}
			if e <= before[p] {
				t.Fatalf("G14: partition %d epoch %d not above previous %d", p, e, before[p])
			}
		}
		if store.held() != 4 {
			t.Fatalf("held %d", store.held())
		}
	})
}

func TestLeaseManager_StaleOwnerStopsAtNextRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newFakeLeaseStore(testkit.RealClock{})
		fence := &counterFencing{}
		a := newTestManager(store, fence, "a", 4)
		b := newTestManager(store, fence, "b", 4)
		var lost []string
		a.onEnd = func(l *lease, reason string) { lost = append(lost, reason) }
		a.tick(ctx)
		aLeases := a.snapshot()
		before := epochsOf(a)
		// a pauses: no ticks for longer than the TTL.
		time.Sleep(testTTL + time.Second)
		for _, l := range aLeases {
			if l.fresh(time.Now(), testTTL) || l.begin(time.Now(), testTTL) {
				t.Fatal("stale lease reported fresh or accepted a message")
			}
			if l.ctx.Err() != nil {
				t.Fatal("lease context canceled before the owner noticed")
			}
		}
		b.tick(ctx) // a's membership expired: live=1, desired=4
		if n := len(b.snapshot()); n != 4 {
			t.Fatalf("b owns %d, want 4", n)
		}
		for p, e := range epochsOf(b) {
			if e <= before[p] {
				t.Fatalf("G14: partition %d epoch %d not above %d", p, e, before[p])
			}
		}
		// a wakes up: every renewal fails, every lease context is canceled.
		a.tick(ctx)
		if len(a.snapshot()) != 0 {
			t.Fatal("stale owner kept leases")
		}
		for _, l := range aLeases {
			if l.ctx.Err() == nil {
				t.Fatal("lost lease context not canceled")
			}
		}
		if len(lost) != 4 || lost[0] != LeaseEndLost {
			t.Fatalf("end reasons %v", lost)
		}
		// a is back: live=2 again, desired=2; b releases, a re-acquires with higher epochs.
		time.Sleep(testRenew)
		b.tick(ctx)
		a.tick(ctx)
		if len(a.snapshot()) != 2 || len(b.snapshot()) != 2 {
			t.Fatalf("after rejoin a=%d b=%d", len(a.snapshot()), len(b.snapshot()))
		}
		bEpochs := epochsOf(b)
		for p, e := range epochsOf(a) {
			if e <= before[p] || e <= 4 {
				t.Fatalf("partition %d epoch %d did not increase", p, e)
			}
			if _, dup := bEpochs[p]; dup {
				t.Fatalf("partition %d owned twice", p)
			}
		}
	})
}

func TestLeaseManager_RenewErrorPastTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newFakeLeaseStore(testkit.RealClock{})
		a := newTestManager(store, &counterFencing{}, "a", 2)
		var reasons []string
		a.onEnd = func(l *lease, reason string) { reasons = append(reasons, reason) }
		a.tick(ctx)
		leases := a.snapshot()
		store.renewErr = errors.New("redis down")
		store.acquireErr = store.renewErr
		time.Sleep(testRenew)
		a.tick(ctx)
		time.Sleep(testRenew)
		a.tick(ctx)
		if len(a.snapshot()) != 2 || leases[0].ctx.Err() != nil {
			t.Fatal("lease dropped before the ttl passed")
		}
		time.Sleep(testRenew)
		a.tick(ctx) // 15 s since the last successful renewal
		if len(a.snapshot()) != 0 || leases[0].ctx.Err() == nil || leases[1].ctx.Err() == nil {
			t.Fatal("expired lease kept")
		}
		if len(reasons) != 2 || reasons[0] != LeaseEndExpired {
			t.Fatalf("reasons %v", reasons)
		}
		// Redis comes back: leases are re-acquired.
		store.renewErr, store.acquireErr = nil, nil
		time.Sleep(testRenew)
		a.tick(ctx)
		if len(a.snapshot()) != 2 {
			t.Fatal("not re-acquired")
		}
	})
}

func TestLeaseManager_BusyLeaseReleasedAfterMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newFakeLeaseStore(testkit.RealClock{})
		fence := &counterFencing{}
		a := newTestManager(store, fence, "a", 4)
		b := newTestManager(store, fence, "b", 4)
		a.tick(ctx)
		var busy *lease
		for _, l := range a.snapshot() {
			if l.key.partition == 3 { // surplus release picks the highest partitions first
				busy = l
			}
		}
		if !busy.begin(time.Now(), testTTL) {
			t.Fatal("begin")
		}
		b.tick(ctx)
		time.Sleep(testRenew)
		a.tick(ctx)
		if !busy.isReleasing() || busy.ctx.Err() != nil {
			t.Fatal("busy lease should be marked releasing but still live")
		}
		if node, _ := store.holder(Keys{Prefix: "t"}.Lease("g", "orders", 3)); node != "a" {
			t.Fatalf("busy lease released early; holder %q", node)
		}
		if n := store.held(); n != 3 {
			t.Fatalf("held %d, want 3 (one idle surplus released)", n)
		}
		// Still renewed while busy.
		time.Sleep(testRenew)
		a.tick(ctx)
		if !busy.fresh(time.Now(), testTTL) {
			t.Fatal("releasing lease not renewed")
		}
		// The message finishes: the deferred release happens now.
		a.finish(ctx, busy)
		if busy.ctx.Err() == nil || store.held() != 2 {
			t.Fatalf("deferred release: ctx=%v held=%d", busy.ctx.Err(), store.held())
		}
		if busy.begin(time.Now(), testTTL) || busy.end() {
			t.Fatal("ended lease accepted work")
		}
		b.tick(ctx)
		if len(b.snapshot()) != 2 || len(a.snapshot()) != 2 {
			t.Fatalf("a=%d b=%d", len(a.snapshot()), len(b.snapshot()))
		}
	})
}

func TestLeaseManager_ErrorsAndStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newFakeLeaseStore(testkit.RealClock{})
		fence := &counterFencing{err: errors.New("pg down")}
		a := newTestManager(store, fence, "a", 3)
		a.tick(ctx)
		if len(a.snapshot()) != 0 {
			t.Fatal("acquired without a fencing token")
		}
		fence.err = nil
		store.acquireErr = errors.New("redis down")
		a.tick(ctx)
		if len(a.snapshot()) != 0 {
			t.Fatal("acquired despite store error")
		}
		store.acquireErr = nil
		store.beatErr = errors.New("redis down")
		a.tick(ctx) // heartbeat failure keeps the last known membership (1)
		if len(a.snapshot()) != 3 {
			t.Fatalf("owned %d", len(a.snapshot()))
		}
		store.beatErr = nil
		store.releaseErr = errors.New("redis down")
		a.stopAcquiring()
		a.releaseAll(ctx)
		if len(a.snapshot()) != 0 {
			t.Fatal("releaseAll kept leases after a store error")
		}
		store.releaseErr = nil
		time.Sleep(testRenew)
		a.tick(ctx)
		if len(a.snapshot()) != 0 {
			t.Fatal("acquired after stopAcquiring")
		}
		// A lease can only end once.
		l := &lease{key: leaseKey{"g", "orders", 0}, ctx: ctx, cancel: func() {}}
		a.end(l, LeaseEndLost)
		a.end(l, LeaseEndLost)
		a.release(ctx, l, LeaseEndSurplus)
		if !l.ended {
			t.Fatal("ended")
		}
	})
}

func TestLeaseManager_RunLoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeLeaseStore(testkit.RealClock{})
		a := newTestManager(store, &counterFencing{}, "a", 2)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			a.run(ctx)
		}()
		synctest.Wait()
		if len(a.snapshot()) != 2 {
			t.Fatal("run did not tick immediately")
		}
		renews := store.count("renew")
		time.Sleep(testRenew)
		synctest.Wait()
		if store.count("renew") != renews+2 {
			t.Fatalf("renew calls %d", store.count("renew"))
		}
		cancel()
		<-done
		if store.held() != 2 {
			t.Fatal("run must not release on exit")
		}
	})
}

func TestLease_BeginEndMarks(t *testing.T) {
	now := time.Now()
	l := &lease{lastRenew: now, ctx: context.Background(), cancel: func() {}}
	// A lease is fresh strictly inside the ttl and stale at exactly the ttl,
	// the same instant begin refuses work.
	if !l.fresh(now, testTTL) || !l.fresh(now.Add(testTTL-time.Nanosecond), testTTL) {
		t.Fatal("lease inside the ttl reported stale")
	}
	if l.fresh(now.Add(testTTL), testTTL) || l.begin(now.Add(testTTL), testTTL) {
		t.Fatal("lease at exactly the ttl reported fresh")
	}
	if !l.begin(now, testTTL) || l.end() {
		t.Fatal("plain begin/end")
	}
	if !l.begin(now, testTTL) {
		t.Fatal("begin")
	}
	if l.markRelease() {
		t.Fatal("busy lease released immediately")
	}
	if !l.end() {
		t.Fatal("end must request the deferred release")
	}
	if l.begin(now, testTTL) {
		t.Fatal("releasing lease accepted work")
	}
	l2 := &lease{lastRenew: now}
	if !l2.markRelease() {
		t.Fatal("idle lease should release immediately")
	}
	if (leaseKey{"g", "t", 3}).String() != "g/t/p3" {
		t.Fatal("key string")
	}
}

// TestLeaseManager_WorkerOwnsSurplusRelease covers the release protocol of
// spec 7.2 with a worker attached: the manager only marks the surplus lease
// and interrupts the worker's read; the lease stays held and renewed until
// the worker has finished what it holds and releases it itself, so a new
// owner can never start while the old one still has a read outstanding.
func TestLeaseManager_WorkerOwnsSurplusRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newFakeLeaseStore(testkit.RealClock{})
		fence := &counterFencing{}
		a := newTestManager(store, fence, "a", 4)
		b := newTestManager(store, fence, "b", 4)
		var mu sync.Mutex
		var events []string
		record := func(s string) {
			mu.Lock()
			events = append(events, s)
			mu.Unlock()
		}
		// The entry in hand outlives two renew intervals and ends between ticks.
		const handler = 2*testRenew + time.Second
		a.onAcquire = func(l *lease) {
			l.attachWorker()
			go func() {
				// The worker: blocked in XREADGROUP until the read is
				// interrupted, then it finishes the entry in hand, which
				// takes longer than a renew interval, then releases.
				<-l.workCtx.Done()
				if l.ctx.Err() != nil {
					return
				}
				record("interrupted p" + strconv.Itoa(l.key.partition))
				if !l.begin(time.Now(), testTTL) {
					t.Errorf("p%d: worker refused work while releasing", l.key.partition)
				}
				time.Sleep(handler)
				a.finish(ctx, l)
				holder, _ := store.holder(Keys{Prefix: "t"}.Lease("g", "orders", l.key.partition))
				if l.ctx.Err() != nil || holder != "a" {
					t.Errorf("p%d: released before the worker was done (holder %q)", l.key.partition, holder)
				}
				record("release p" + strconv.Itoa(l.key.partition))
				a.release(ctx, l, LeaseEndSurplus)
			}()
		}
		t.Cleanup(func() {
			// End the remaining leases so every worker goroutine leaves the
			// bubble.
			a.stopAcquiring()
			a.releaseAll(ctx)
			synctest.Wait()
		})
		a.tick(ctx)
		b.tick(ctx) // joins: live=2, desired=2, nothing free yet
		time.Sleep(testRenew)
		a.tick(ctx) // marks p3 and p2 surplus
		synctest.Wait()
		if store.held() != 4 || len(a.snapshot()) != 4 {
			t.Fatalf("manager released a lease with a worker attached: held=%d owned=%d", store.held(), len(a.snapshot()))
		}
		for _, l := range a.snapshot() {
			surplus := l.key.partition >= 2
			if l.isReleasing() != surplus || (l.workCtx.Err() != nil) != surplus || l.ctx.Err() != nil {
				t.Fatalf("p%d: releasing=%v work=%v ctx=%v", l.key.partition, l.isReleasing(), l.workCtx.Err(), l.ctx.Err())
			}
		}
		// The releasing leases keep being renewed while the workers drain,
		// and the joining node cannot take them.
		time.Sleep(testRenew)
		a.tick(ctx)
		b.tick(ctx)
		for _, l := range a.snapshot() {
			if !l.fresh(time.Now(), testTTL) {
				t.Fatalf("p%d not renewed while releasing", l.key.partition)
			}
		}
		if len(b.snapshot()) != 0 || store.held() != 4 {
			t.Fatalf("b acquired before the workers released: b=%d held=%d", len(b.snapshot()), store.held())
		}
		// The workers finish and release; only then does b acquire, with
		// higher epochs. Both nodes keep ticking meanwhile.
		time.Sleep(testRenew)
		a.tick(ctx)
		b.tick(ctx)
		if len(b.snapshot()) != 0 || store.held() != 4 {
			t.Fatalf("b acquired before the workers released: b=%d held=%d", len(b.snapshot()), store.held())
		}
		time.Sleep(handler - 2*testRenew)
		synctest.Wait()
		if store.held() != 2 || len(a.snapshot()) != 2 {
			t.Fatalf("after the workers released: held=%d owned=%d", store.held(), len(a.snapshot()))
		}
		mu.Lock()
		got := append([]string(nil), events...)
		mu.Unlock()
		sort.Strings(got)
		want := []string{"interrupted p2", "interrupted p3", "release p2", "release p3"}
		if len(got) != len(want) {
			t.Fatalf("events %v", got)
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("events %v, want %v", got, want)
			}
		}
		before := epochsOf(a)
		time.Sleep(testRenew)
		a.tick(ctx)
		b.tick(ctx)
		if len(b.snapshot()) != 2 || len(a.snapshot()) != 2 {
			t.Fatalf("a owns %d, b owns %d, want 2 each", len(a.snapshot()), len(b.snapshot()))
		}
		for p, e := range epochsOf(b) {
			if _, still := before[p]; still || e <= 4 {
				t.Fatalf("partition %d epoch %d", p, e)
			}
		}
	})
}

// TestLeaseManager_LostLeaseWaitsForWorkerExit covers the same-node
// re-acquire rule (design notes 8.7). When Redis forgets the lease keys, the
// renewals report the leases lost and their contexts are canceled, but a
// worker's blocking XREADGROUP stays outstanding until Redis replies or
// BLOCK expires. The manager keeps those partitions out of rebalance until
// each worker has exited, so it never starts a second reader under the same
// consumer name beside the old one; a lost lease without a worker is
// re-acquired in the same tick as before.
func TestLeaseManager_LostLeaseWaitsForWorkerExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		store := newFakeLeaseStore(testkit.RealClock{})
		fence := &counterFencing{}
		a := newTestManager(store, fence, "a", 4)
		var ends []string
		a.onEnd = func(l *lease, reason string) { ends = append(ends, reason) }
		// The outstanding read of a lost lease returns after two more ticks.
		const blocked = 2*testRenew + time.Second
		stop := make(chan struct{}) // shutdown: the workers exit before Run releases
		a.onAcquire = func(l *lease) {
			if l.key.partition == 0 {
				return // no worker attached: the manager owns this lease
			}
			l.attachWorker()
			go func() {
				select {
				case <-l.ctx.Done():
					time.Sleep(blocked) // the blocking read is still out
				case <-stop:
				}
				a.workerDone(l)
			}()
		}
		a.tick(ctx)
		before := epochsOf(a)
		if len(before) != 4 || store.held() != 4 {
			t.Fatalf("owned %d held %d", len(before), store.held())
		}
		old := a.snapshot()

		// Redis forgets the keys. The next tick loses every lease; only p0,
		// which has no worker, is re-acquired in the same tick.
		store.forget()
		time.Sleep(testRenew)
		a.tick(ctx)
		if len(ends) != 4 || ends[0] != LeaseEndLost || ends[3] != LeaseEndLost {
			t.Fatalf("end reasons %v", ends)
		}
		for _, l := range old {
			if l.ctx.Err() == nil {
				t.Fatalf("p%d: lost lease context not canceled", l.key.partition)
			}
		}
		if got := epochsOf(a); len(got) != 1 || got[0] <= before[0] || store.held() != 1 {
			t.Fatalf("same tick: owned %v held %d, want only p0 re-acquired above %d", got, store.held(), before[0])
		}
		if n := a.endingCount(); n != 3 {
			t.Fatalf("ending %d, want 3", n)
		}
		// Still blocked at the next two ticks: still not re-acquired.
		time.Sleep(testRenew)
		a.tick(ctx)
		if len(a.snapshot()) != 1 || store.held() != 1 || a.endingCount() != 3 {
			t.Fatalf("while blocked: owned %d held %d ending %d", len(a.snapshot()), store.held(), a.endingCount())
		}
		time.Sleep(testRenew)
		a.tick(ctx)
		if len(a.snapshot()) != 1 || store.held() != 1 {
			t.Fatalf("while blocked: owned %d held %d", len(a.snapshot()), store.held())
		}
		// The reads return and the workers exit: ending empties, and the
		// next tick re-acquires with higher epochs.
		time.Sleep(time.Second)
		synctest.Wait()
		if n := a.endingCount(); n != 0 {
			t.Fatalf("ending %d after the workers exited", n)
		}
		if len(a.snapshot()) != 1 {
			t.Fatal("re-acquired without a tick")
		}
		time.Sleep(testRenew)
		a.tick(ctx)
		after := epochsOf(a)
		if len(after) != 4 || store.held() != 4 {
			t.Fatalf("after the workers exited: owned %v held %d", after, store.held())
		}
		for p, e := range after {
			if e <= before[p] {
				t.Fatalf("G14: partition %d epoch %d not above %d", p, e, before[p])
			}
		}
		if h := a.handoverCounts()[testScopes[0]]; h != 8 {
			t.Fatalf("handovers %d, want 8", h)
		}
		// Shutdown: the workers are joined before releaseAll, so nothing is
		// parked in ending and every lease is released.
		close(stop)
		synctest.Wait()
		a.stopAcquiring()
		a.releaseAll(ctx)
		if len(a.snapshot()) != 0 || store.held() != 0 || a.endingCount() != 0 {
			t.Fatalf("shutdown: owned %d held %d ending %d", len(a.snapshot()), store.held(), a.endingCount())
		}
		if len(ends) != 8 || ends[7] != LeaseEndShutdown {
			t.Fatalf("end reasons %v", ends)
		}
	})
}

// TestLeaseManager_EndingTracksWorkerExit pins the bookkeeping behind that
// rule: end parks a lease only while a worker is attached and has not
// exited, workerDone unparks exactly that lease, and the worker's order of
// deferred calls (exit's surplus release, then workerDone) leaves nothing
// behind while rebalance skips the partition in between.
func TestLeaseManager_EndingTracksWorkerExit(t *testing.T) {
	ctx := context.Background()
	store := newFakeLeaseStore(testkit.RealClock{})
	fence := &counterFencing{}
	fence.n.Store(5) // the hand-built leases below use epochs 1 to 5
	a := newTestManager(store, fence, "a", 2)
	key := leaseKey{"g", "orders", 1}
	now := time.Now()

	// No worker: ended and forgotten at once.
	bare := newLease(ctx, key, 1, "a:1", now)
	a.owned[key] = bare
	a.end(bare, LeaseEndLost)
	if a.endingCount() != 0 || len(a.snapshot()) != 0 {
		t.Fatal("a lease without a worker must not be parked")
	}

	// Worker attached, not exited: parked until its own workerDone.
	l := newLease(ctx, key, 2, "a:2", now)
	l.attachWorker()
	a.owned[key] = l
	a.end(l, LeaseEndLost)
	if a.endingCount() != 1 || len(a.snapshot()) != 0 {
		t.Fatalf("ending %d owned %d", a.endingCount(), len(a.snapshot()))
	}
	other := newLease(ctx, key, 3, "a:3", now)
	other.attachWorker()
	a.workerDone(other)
	if a.endingCount() != 1 {
		t.Fatal("workerDone of a different lease unparked the key")
	}
	a.workerDone(l)
	if a.endingCount() != 0 {
		t.Fatal("workerDone did not unpark the lease")
	}
	a.end(l, LeaseEndLost)
	if a.endingCount() != 0 {
		t.Fatal("an ended lease was parked again")
	}

	// Worker already exited (shutdown joins the workers before releaseAll):
	// not parked.
	l2 := newLease(ctx, key, 4, "a:4", now)
	l2.attachWorker()
	a.owned[key] = l2
	a.workerDone(l2)
	a.end(l2, LeaseEndShutdown)
	if a.endingCount() != 0 {
		t.Fatal("a lease whose worker exited must not be parked")
	}

	// The worker's exit order: the surplus release parks the lease, a
	// rebalance meanwhile leaves the partition alone, then workerDone frees
	// it and the next tick may take it again.
	l3 := newLease(ctx, key, 5, "a:5", now)
	l3.attachWorker()
	if ok, err := store.acquire(ctx, a.keys.Lease("g", "orders", 1), "a:5", testTTL); !ok || err != nil {
		t.Fatalf("seed store: %v %v", ok, err)
	}
	a.owned[key] = l3
	if l3.markRelease() {
		t.Fatal("manager released a lease with a worker")
	}
	a.release(ctx, l3, LeaseEndSurplus) // what exit does
	if a.endingCount() != 1 || store.held() != 0 || l3.ctx.Err() == nil {
		t.Fatalf("after the worker's release: ending %d held %d ctx %v", a.endingCount(), store.held(), l3.ctx.Err())
	}
	a.tick(ctx) // live=1, desired=2: p0 is free, p1 is ending
	if got := epochsOf(a); len(got) != 1 || got[0] == 0 {
		t.Fatalf("rebalance while ending: owned %v, want p0 only", got)
	}
	a.workerDone(l3)
	if a.endingCount() != 0 {
		t.Fatal("workerDone after the release did not unpark the lease")
	}
	a.tick(ctx)
	if got := epochsOf(a); len(got) != 2 || got[1] <= 5 {
		t.Fatalf("after workerDone: owned %v, want p1 re-acquired above 5", got)
	}
	a.stopAcquiring()
	a.releaseAll(ctx)
}

func TestLease_WorkerAttachedSemantics(t *testing.T) {
	now := time.Now()
	l := newLease(context.Background(), leaseKey{"g", "orders", 1}, 7, "a:7", now)
	defer l.cancel()
	l.attachWorker()
	if l.markRelease() {
		t.Fatal("a lease with a worker must not be released by the manager")
	}
	if l.workCtx.Err() == nil || l.ctx.Err() != nil {
		t.Fatal("markRelease must interrupt the work context and keep the lease live")
	}
	if !l.begin(now, testTTL) {
		t.Fatal("the worker keeps processing the entries it holds while releasing")
	}
	if l.end() {
		t.Fatal("end must not request a manager release with a worker attached")
	}
	if l.begin(now.Add(testTTL), testTTL) {
		t.Fatal("stale lease accepted work")
	}
	// Ending the lease cancels the work context with it, and markRelease on
	// a lease built without contexts (older tests) is a no-op cancel.
	l.cancel()
	if l.workCtx.Err() == nil {
		t.Fatal("work context must descend from the lease context")
	}
	bare := &lease{lastRenew: now}
	if !bare.markRelease() {
		t.Fatal("idle lease without a worker releases immediately")
	}
}

// TestLeaseManager_AcquireAfterStopReleasesUnderDetachedContext: an
// acquisition whose SET NX lands after stopAcquiring gives the key back even
// though the tick's context is canceled by then (Run cancels the loop right
// after releaseAll), so no key outlives the node until its TTL.
func TestLeaseManager_AcquireAfterStopReleasesUnderDetachedContext(t *testing.T) {
	store := newFakeLeaseStore(testkit.RealClock{})
	fence := &counterFencing{}
	logs := &logCapture{}
	lm := newTestManager(store, fence, "a", 1)
	lm.logger = slog.New(logs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store.afterAcquire = func() {
		lm.stopAcquiring()
		cancel()
	}
	key := leaseKey{"g", "orders", 0}
	if lm.acquire(ctx, key) {
		t.Fatal("acquired after stop")
	}
	if held := store.held(); held != 0 {
		t.Fatalf("key leaked after a stop landed mid-acquisition: held=%d", held)
	}
	if n := len(lm.snapshot()); n != 0 {
		t.Fatalf("owned %d, want 0", n)
	}
	const notReleased = "WARN lease acquired after stop could not be released"
	if logs.count(notReleased) != 0 {
		t.Fatalf("release succeeded but was reported as failed: %v", logs.msgs)
	}
	// A canceled tick context before the write acquires nothing and leaves
	// nothing behind either.
	store.afterAcquire = nil
	if lm.acquire(ctx, key) || store.held() != 0 {
		t.Fatalf("acquire under a canceled context: held=%d", store.held())
	}
	// When the give-back itself fails the key stays until its TTL, and the
	// warning says so.
	store.afterAcquire = lm.stopAcquiring
	store.releaseErr = errors.New("redis down")
	if lm.acquire(context.Background(), key) {
		t.Fatal("acquired after stop")
	}
	if held := store.held(); held != 1 {
		t.Fatalf("held %d after a failed give-back, want the key left to expire", held)
	}
	if logs.count(notReleased) != 1 || len(lm.snapshot()) != 0 {
		t.Fatalf("failed give-back not reported: %v", logs.msgs)
	}
}
