//go:build faultsweep && faultinject

package faultsweep

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/behavior/cachemodel"
	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// SweepCache (spec 11.4, G9): the scheduler of behavior/cachemodel drives
// readers and writers step by step against a real redisx.Cache. The clean
// cell first runs every interleaving of one reader and one writer
// (preflight); every cell then runs the representative interleavings below
// with a fault at each cache step, followed by three limiter checks so the
// rate limiter's point is a step too. Expect: no cached read returned a
// value older than a write that had returned (outside degraded windows),
// after the run a hit is the latest value, and after recovery (the
// behavior's bump retry) every entry is invalidated.

// interleaving is an order of actor steps; readers are 0..readers-1, the
// writer is actor readers.
type interleaving struct {
	name    string
	readers int
	order   []int
}

var cacheInterleavings = []interleaving{
	{"reader-fills-then-writer", 2, []int{0, 0, 0, 0, 2, 2, 2, 2, 2, 1}},
	{"writer-then-reader", 1, []int{1, 1, 1, 1, 1, 0, 0, 0, 0}},
	{"snapshot-before-prebump", 2, []int{0, 0, 2, 2, 0, 2, 2, 2, 0, 1}},
	{"snapshot-after-prebump-read-before-commit", 2, []int{0, 2, 2, 0, 0, 2, 0, 2, 2, 1}},
	{"read-after-commit-set-before-postbump", 2, []int{0, 2, 2, 0, 2, 0, 0, 2, 2, 1}},
	{"reader-inside-writer", 2, []int{2, 0, 0, 0, 2, 2, 2, 2, 0, 1}},
}

var limiterPolicy = ratelimit.Policy{Rate: 1, Period: time.Minute, Burst: 2}

type cacheState struct {
	cache    *redisx.Cache
	limiter  *redisx.Limiter
	degraded map[int]bool
	rlErrors int
}

func cacheKey(i int) string    { return "q:" + strconv.Itoa(i) }
func cacheTags(i int) []string { return []string{"tag" + strconv.Itoa(i)} }

// pickOrder returns a pick function following order, then the first
// runnable actor.
func pickOrder(order []int) func([]int) int {
	i := 0
	return func(runnable []int) int {
		for i < len(order) {
			a := order[i]
			i++
			for _, r := range runnable {
				if r == a {
					return a
				}
			}
		}
		return runnable[0]
	}
}

// runInterleaving executes one interleaving on the backend and returns the
// scheduler for inspection.
func runInterleaving(ctx context.Context, backend cachemodel.Backend, key string, tags []string, readers int, order []int) (*cachemodel.Scheduler, error) {
	s := cachemodel.New(ctx, backend, cachemodel.Config{Readers: readers, Writers: 1, Key: key, Tags: tags, TTL: time.Minute})
	return s, s.Run(pickOrder(order))
}

var scenarioCache = &scenario{
	name:         "SweepCache",
	patterns:     []string{"redis.cache.*", "redis.tag.*", "redis.rl.check"},
	noInvariants: true,
	variants:     []string{"oom"},
	newState:     func() any { return &cacheState{degraded: map[int]bool{}} },
	preflight: func(ctx context.Context, c *cellRun) error {
		// Every interleaving of one reader (4 steps) and one writer (5 steps).
		cfg := c.env.redisConfig("pre")
		client, err := redisx.NewClient(ctx, cfg)
		if err != nil {
			return err
		}
		defer client.Close()
		cache := redisx.NewCache(client, cfg)
		n := 0
		var gen func(prefix []int, r, w int) error
		gen = func(prefix []int, r, w int) error {
			if r == 0 && w == 0 {
				sctx, cancel := context.WithTimeout(ctx, sendTimeout)
				defer cancel()
				s, err := runInterleaving(sctx, cache, "x:"+strconv.Itoa(n), []string{"xt" + strconv.Itoa(n)}, 1, prefix)
				n++
				if err != nil {
					return fmt.Errorf("interleaving %v: %w", prefix, err)
				}
				if d := s.Degraded(); len(d) > 0 {
					return fmt.Errorf("interleaving %v degraded without a fault: %v", prefix, d)
				}
				return nil
			}
			if r > 0 {
				if err := gen(append(append([]int{}, prefix...), 0), r-1, w); err != nil {
					return err
				}
			}
			if w > 0 {
				if err := gen(append(append([]int{}, prefix...), 1), r, w-1); err != nil {
					return err
				}
			}
			return nil
		}
		if err := gen(nil, 4, 5); err != nil {
			return err
		}
		c.t.Logf("SweepCache preflight: %d interleavings of one reader and one writer held G9 against Redis", n)
		return nil
	},
	build: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*cacheState)
		n, err := c.newNode(ctx, nodeOpts{id: c.nodeID("n1"), noStore: true})
		if err != nil {
			return err
		}
		st.cache = redisx.NewCache(n.client, n.cfg)
		st.limiter = redisx.NewLimiter(n.client, n.cfg)
		return nil
	},
	run: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*cacheState)
		for i, il := range cacheInterleavings {
			sctx, cancel := c.reqCtx(ctx)
			s, err := runInterleaving(sctx, st.cache, cacheKey(i), cacheTags(i), il.readers, il.order)
			cancel()
			if err != nil {
				c.t.Errorf("I7 violated in %s: %v", il.name, err)
			}
			if d := s.Degraded(); len(d) > 0 {
				st.degraded[i] = true
				c.note("%s degraded: %v", il.name, d)
			}
		}
		// After every writer returned, a hit must be the latest value (1).
		for i := range cacheInterleavings {
			if st.degraded[i] {
				continue
			}
			gctx, cancel := c.reqCtx(ctx)
			body, ok, err := st.cache.Get(gctx, cacheKey(i))
			cancel()
			if err != nil {
				c.note("post-run get %d: %v", i, err)
				st.degraded[i] = true
				continue
			}
			if ok && string(body) != "1" {
				c.t.Errorf("G9: %s: cache serves %q after the write returned", cacheInterleavings[i].name, body)
			}
		}
		var decisions []string
		for i := 0; i < 3; i++ {
			lctx, cancel := c.reqCtx(ctx)
			d, err := st.limiter.Check(lctx, "rl", "k", limiterPolicy)
			cancel()
			if err != nil {
				st.rlErrors++
				decisions = append(decisions, "err")
				continue
			}
			decisions = append(decisions, fmt.Sprintf("allowed=%v remaining=%d", d.Allowed, d.Remaining))
			if want := i < limiterPolicy.Burst; st.rlErrors == 0 && d.Allowed != want {
				c.t.Errorf("limiter check %d: allowed=%v, want %v (%v)", i+1, d.Allowed, want, decisions)
			}
		}
		c.note("limiter: %v", decisions)
		return nil
	},
	recover: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*cacheState)
		// The behavior retries a failed bump for up to the TTL; the sweep's
		// recovery is that retry for every tag of the run.
		var tags []string
		for i := range cacheInterleavings {
			tags = append(tags, cacheTags(i)...)
		}
		bctx, cancel := context.WithTimeout(ctx, sendTimeout)
		defer cancel()
		return st.cache.BumpTagsPost(bctx, tags)
	},
	verify: func(ctx context.Context, c *cellRun, v *verifier) {
		cache := redisx.NewCache(v.client, v.cfg)
		for i, il := range cacheInterleavings {
			if _, ok, err := cache.Get(ctx, cacheKey(i)); err != nil || ok {
				v.t.Errorf("%s: entry still served after every tag was bumped (ok=%v err=%v)", il.name, ok, err)
			}
		}
		// A fresh key per verify: a crash cell verifies in the recovery
		// child and again in the parent.
		d, err := redisx.NewLimiter(v.client, v.cfg).Check(ctx, "rl", "fresh-"+randomHex(4), limiterPolicy)
		if err != nil || !d.Allowed || d.Remaining != limiterPolicy.Burst-1 {
			v.t.Errorf("limiter after recovery: %+v %v", d, err)
		}
	},
}

func TestSweepCache(t *testing.T) { runSweep(t, scenarioCache) }
