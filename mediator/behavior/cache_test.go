package behavior_test

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior/cachemodel"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// The redisx cache satisfies the behavior's backend interface and computes
// the same logical key.
var _ behavior.CacheBackend = (*redisx.Cache)(nil)

func TestCacheKey_MatchesRedisx(t *testing.T) {
	q := cachedQuery{ID: "k", TTLSeconds: 60}
	a, err := behavior.CacheKey("cachedQuery", q)
	if err != nil {
		t.Fatal(err)
	}
	b, err := redisx.CacheKey("cachedQuery", q)
	if err != nil {
		t.Fatal(err)
	}
	if a != b || len(a) != len("cachedQuery:")+64 {
		t.Fatalf("%s != %s", a, b)
	}
	if _, err := behavior.CacheKey("x", badKeyQuery{Extra: make(chan int)}); err == nil {
		t.Fatal("want key error")
	}
}

// badKeyQuery cannot be encoded when Extra holds a channel.
type badKeyQuery struct {
	mediator.Query[queryResult]
	Extra any `json:"extra"`
}

func (badKeyQuery) CacheTags() []string { return []string{"things"} }

// anyResult may hold an unencodable value.
type anyResult struct {
	V any `json:"v"`
}

type anyQuery struct {
	mediator.Query[anyResult]
	ID string `json:"id"`
}

func (anyQuery) CacheTags() []string { return []string{"things"} }

func cacheHarness(t *testing.T, opts ...harnessOption) *harness {
	t.Helper()
	opts = append(opts, withPreBuild(func(m *mediator.Mediator) error {
		if err := mediator.HandleFunc(m, func(context.Context, badKeyQuery) (queryResult, error) { return queryResult{ID: "bad"}, nil }); err != nil {
			return err
		}
		return mediator.HandleFunc(m, func(context.Context, anyQuery) (anyResult, error) { return anyResult{V: make(chan int)}, nil })
	}))
	h := newHarness(t, opts...)
	h.backend.Now = h.clock.Now
	return h
}

func cacheResult(t *testing.T, ctx context.Context) httpapi.CacheResult {
	t.Helper()
	r, ok := httpapi.CacheResultFrom(ctx)
	if !ok {
		t.Fatal("no cache result reported")
	}
	return r
}

func TestCache_HitMissBypass(t *testing.T) {
	h := cacheHarness(t)
	var calls atomic.Int32
	h.hooks.cached = func(_ context.Context, q cachedQuery) (queryResult, error) {
		calls.Add(1)
		return queryResult{ID: q.ID, Value: int64(calls.Load())}, nil
	}
	send := func(ctx context.Context, q cachedQuery) (queryResult, httpapi.CacheResult) {
		t.Helper()
		ctx = httpapi.WithCacheResultHolder(ctx)
		res, err := mediator.Send(ctx, h.m, q)
		if err != nil {
			t.Fatal(err)
		}
		return res, cacheResult(t, ctx)
	}
	ctx := context.Background()
	if res, r := send(ctx, cachedQuery{ID: "a"}); r != httpapi.CacheMiss || res.Value != 1 {
		t.Fatalf("first: %v %+v", r, res)
	}
	if h.store.Begun() != 1 {
		t.Fatalf("a miss runs the unit of work once: %d", h.store.Begun())
	}
	if res, r := send(ctx, cachedQuery{ID: "a"}); r != httpapi.CacheHit || res.Value != 1 {
		t.Fatalf("second: %v %+v", r, res)
	}
	if h.store.Begun() != 1 || calls.Load() != 1 {
		t.Fatal("a hit must skip the unit of work and the handler")
	}
	if res, r := send(mediator.WithNoCache(ctx), cachedQuery{ID: "a"}); r != httpapi.CacheBypass || res.Value != 2 {
		t.Fatalf("bypass: %v %+v", r, res)
	}
	if res, r := send(ctx, cachedQuery{ID: "a"}); r != httpapi.CacheHit || res.Value != 2 {
		t.Fatalf("bypass must still store: %v %+v", r, res)
	}
	if _, r := send(ctx, cachedQuery{ID: "b"}); r != httpapi.CacheMiss {
		t.Fatalf("other key: %v", r)
	}
	rm := h.tel.collect(t)
	for result, n := range map[string]int64{behavior.CacheHit: 2, behavior.CacheMiss: 2, behavior.CacheBypass: 1} {
		if p := point(t, rm, "mediator.cache.requests", map[string]string{"name": "cachedQuery", "result": result}); p.Int != n {
			t.Errorf("%s = %d, want %d", result, p.Int, n)
		}
	}
	if hasPoint(rm, "mediator.cache.requests", map[string]string{"result": behavior.CacheError}) {
		t.Error("no error expected")
	}
}

func TestCache_TTL(t *testing.T) {
	h := cacheHarness(t, withConfig(func(cfg *behavior.Config) { cfg.CacheTTL = 2 * time.Minute }))
	ctx := context.Background()
	calls := 0
	h.hooks.cached = func(_ context.Context, q cachedQuery) (queryResult, error) {
		calls++
		return queryResult{ID: q.ID}, nil
	}
	send := func(q cachedQuery) {
		t.Helper()
		if _, err := mediator.Send(ctx, h.m, q); err != nil {
			t.Fatal(err)
		}
	}
	send(cachedQuery{ID: "trait", TTLSeconds: 60})
	send(cachedQuery{ID: "config"})
	h.clock.Advance(90 * time.Second)
	send(cachedQuery{ID: "trait", TTLSeconds: 60})
	send(cachedQuery{ID: "config"})
	if calls != 3 {
		t.Fatalf("trait TTL expired, config TTL not: calls %d", calls)
	}
	h.clock.Advance(time.Minute)
	send(cachedQuery{ID: "config"})
	if calls != 4 {
		t.Fatalf("config TTL expired: calls %d", calls)
	}
	if got := behavior.NewCache(behavior.Config{}); got.Name() != behavior.Cache {
		t.Fatal(got.Name())
	}
}

func TestCache_OnlySuccessIsStored(t *testing.T) {
	h := cacheHarness(t)
	h.hooks.cached = func(context.Context, cachedQuery) (queryResult, error) {
		return queryResult{}, mediator.E(mediator.CodeNotFound, "x")
	}
	_, err := mediator.Send(context.Background(), h.m, cachedQuery{ID: "a"})
	codeIs(t, err, mediator.CodeNotFound)
	if h.backend.Ops(cachemodel.OpSet) != 0 {
		t.Fatal("a failed query must not be stored")
	}
}

func TestCache_Degradation(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		fail   string
		setOps int
	}{
		{"get", cachemodel.OpGet, 1},
		{"snapshot", cachemodel.OpSnapshot, 0},
		{"set", cachemodel.OpSet, 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := cacheHarness(t)
			h.backend.Fail = func(op string) error {
				if op == c.fail {
					return errors.New("redis down")
				}
				return nil
			}
			hctx := httpapi.WithCacheResultHolder(ctx)
			res, err := mediator.Send(hctx, h.m, cachedQuery{ID: "a"})
			if err != nil || res.Value != 1 {
				t.Fatalf("degraded call must succeed: %+v %v", res, err)
			}
			if r := cacheResult(t, hctx); r != httpapi.CacheMiss {
				t.Fatalf("result %v", r)
			}
			if h.backend.Ops(cachemodel.OpSet) != c.setOps {
				t.Fatalf("set ops %d, want %d", h.backend.Ops(cachemodel.OpSet), c.setOps)
			}
			if _, err := mediator.Send(ctx, h.m, cachedQuery{ID: "a"}); err != nil {
				t.Fatal(err)
			}
			rm := h.tel.collect(t)
			if p := point(t, rm, "mediator.cache.requests", map[string]string{"name": "cachedQuery", "result": behavior.CacheError}); p.Int != 2 {
				t.Fatalf("error count %d", p.Int)
			}
			warns := h.logs.find("cache degraded")
			if len(warns) != 1 || warns[0].Level != slog.LevelWarn || warns[0].Attrs["op"] != c.fail {
				t.Fatalf("want one rate-limited warning, got %v", warns)
			}
			h.clock.Advance(time.Minute)
			if _, err := mediator.Send(ctx, h.m, cachedQuery{ID: "a"}); err != nil {
				t.Fatal(err)
			}
			if len(h.logs.find("cache degraded")) != 2 {
				t.Fatal("the warning must return after the window")
			}
		})
	}
}

func TestCache_DecodeKeyAndEncodeErrors(t *testing.T) {
	h := cacheHarness(t)
	ctx := context.Background()

	// Undecodable entry: treated as a miss and replaced.
	key, _ := behavior.CacheKey("cachedQuery", cachedQuery{ID: "a"})
	snap, _ := h.backend.SnapshotTags(ctx, []string{"things"})
	must(t, h.backend.Set(ctx, key, snap, []byte("{not json"), 0))
	hctx := httpapi.WithCacheResultHolder(ctx)
	if res, err := mediator.Send(hctx, h.m, cachedQuery{ID: "a"}); err != nil || res.Value != 1 || cacheResult(t, hctx) != httpapi.CacheMiss {
		t.Fatalf("decode error: %+v %v", res, err)
	}
	if body, _ := h.backend.Entry(key); string(body) == "{not json" {
		t.Fatal("the bad entry must be replaced")
	}
	if warns := h.logs.find("cache degraded"); len(warns) != 1 || warns[0].Attrs["op"] != "decode" {
		t.Fatalf("warnings %v", warns)
	}

	// Unencodable request: no key, bypass.
	h.logs.reset()
	hctx = httpapi.WithCacheResultHolder(ctx)
	if res, err := mediator.Send(hctx, h.m, badKeyQuery{Extra: make(chan int)}); err != nil || res.ID != "bad" || cacheResult(t, hctx) != httpapi.CacheBypass {
		t.Fatalf("key error: %+v %v", res, err)
	}
	if warns := h.logs.find("cache degraded"); len(warns) != 1 || warns[0].Attrs["op"] != "key" {
		t.Fatalf("warnings %v", warns)
	}

	// Unencodable response: returned, not stored.
	h.logs.reset()
	if res, err := mediator.Send(ctx, h.m, anyQuery{ID: "x"}); err != nil || res.V == nil {
		t.Fatalf("encode error: %+v %v", res, err)
	}
	if warns := h.logs.find("cache degraded"); len(warns) != 1 || warns[0].Attrs["op"] != "encode" {
		t.Fatalf("warnings %v", warns)
	}
	rm := h.tel.collect(t)
	if p := point(t, rm, "mediator.cache.requests", map[string]string{"result": behavior.CacheError, "name": "cachedQuery"}); p.Int != 1 {
		t.Fatalf("errors %d", p.Int)
	}
}

func TestCache_Singleflight(t *testing.T) {
	h := cacheHarness(t)
	ctx := context.Background()
	const waiters = 4
	var calls atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{}, waiters)
	h.hooks.cached = func(_ context.Context, q cachedQuery) (queryResult, error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return queryResult{ID: q.ID, Value: 7}, nil
	}
	var wg sync.WaitGroup
	results := make([]queryResult, waiters)
	errs := make([]error, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i], errs[i] = mediator.Send(ctx, h.m, cachedQuery{ID: "hot"})
		}()
	}
	<-started
	// Give the other callers time to reach the singleflight wait.
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	for i := range results {
		if errs[i] != nil || results[i].Value != 7 {
			t.Fatalf("waiter %d: %+v %v", i, results[i], errs[i])
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times for one key", calls.Load())
	}

	// A waiter whose context ends returns while the load continues.
	release = make(chan struct{})
	h.backend.Reset()
	wctx, cancel := context.WithCancel(ctx)
	var first, second error
	wg.Add(2)
	go func() { defer wg.Done(); _, first = mediator.Send(ctx, h.m, cachedQuery{ID: "hot"}) }()
	<-started
	go func() { defer wg.Done(); _, second = mediator.Send(wctx, h.m, cachedQuery{ID: "hot"}) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	if first != nil {
		t.Fatalf("loader: %v", first)
	}
	codeIs(t, second, mediator.CodeTimeout)
}

func TestCache_PanicInFill(t *testing.T) {
	h := cacheHarness(t)
	ctx := context.Background()
	const waiters = 3
	release := make(chan struct{})
	started := make(chan struct{}, waiters)
	h.hooks.cached = func(context.Context, cachedQuery) (queryResult, error) {
		started <- struct{}{}
		<-release
		panic("query exploded")
	}
	var wg sync.WaitGroup
	errs := make([]error, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = mediator.Send(ctx, h.m, cachedQuery{ID: "boom"})
		}()
	}
	<-started
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	for i, err := range errs {
		codeIs(t, err, mediator.CodeInternal)
		if !isPanicError(err) {
			t.Fatalf("waiter %d: %v", i, err)
		}
	}
	if recs := h.logs.find("panic recovered"); len(recs) != 1 {
		t.Fatalf("panic must be logged once with the stack: %v", recs)
	}
	rm := h.tel.collect(t)
	if p := point(t, rm, "mediator.request.duration", map[string]string{"name": "cachedQuery", "outcome": behavior.OutcomePanic}); p.Count != waiters {
		t.Fatalf("panic outcomes %d", p.Count)
	}
}

func TestCache_Direct(t *testing.T) {
	b := behavior.NewCache(behavior.Config{})
	if err := b.(mediator.Preparer).Prepare(nil); err == nil {
		t.Fatal("Prepare without a backend must fail")
	}
	res, err := b.Handle(context.Background(), plainQuery{}, &mediator.RequestInfo{}, func(context.Context, any) (any, error) { return 1, nil })
	if err != nil || res != 1 {
		t.Fatal(res, err)
	}
}
