package redisx

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sort"
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

func (s *fakeLeaseStore) acquire(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls["acquire"]++
	if s.acquireErr != nil {
		return false, s.acquireErr
	}
	s.expire()
	if _, held := s.leases[key]; held {
		return false, nil
	}
	s.leases[key] = fakeLease{value: value, expires: s.clock.Now().Add(ttl)}
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

func (s *fakeLeaseStore) release(_ context.Context, key, value string) (bool, error) {
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
		renews := store.calls["renew"]
		time.Sleep(testRenew)
		synctest.Wait()
		if store.calls["renew"] != renews+2 {
			t.Fatalf("renew calls %d", store.calls["renew"])
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
