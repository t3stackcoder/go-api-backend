//go:build faultsweep && faultinject

package faultsweep

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/test/faultsweep/plan"
)

// variant is one resource-exhaustion condition of spec 11.4. kinds lists
// the fault kinds swept (at the first hit of every point) under the
// condition; a variant without kinds runs the scenario once under it.
type variant struct {
	name     string
	maxConns int32
	params   map[string]string
	kinds    []string
	delay    time.Duration
	// maxBody makes the HTTP path use MaxBodyBytes = request size - 1
	// during the run phase and the exact size during recovery.
	maxBody bool
	// beforeRun and afterRun bracket the run phase (Redis maxmemory).
	beforeRun func(ctx context.Context, c *cellRun) error
	afterRun  func(ctx context.Context, c *cellRun) error
}

var variants = map[string]*variant{
	"pool1":  {name: "pool1", maxConns: 1, kinds: []string{"error", "timeout"}},
	"stmt50": {name: "stmt50", params: map[string]string{"statement_timeout": "50ms"}, kinds: []string{"delay"}, delay: 100 * time.Millisecond},
	"body":   {name: "body", maxBody: true},
	"oom":    {name: "oom", beforeRun: oomLimit, afterRun: oomLift},
}

// TestSweepVariants runs every scenario under its resource-exhaustion
// variants: a pool of size one with error and timeout faults, a 50 ms
// statement timeout with injected delays, MaxBodyBytes one byte short of
// the request, and Redis maxmemory below the working set until recovery.
func TestSweepVariants(t *testing.T) {
	for _, sc := range allScenarios {
		if !opt.scenarioSelected(sc.name) {
			continue
		}
		for _, vn := range sc.variants {
			v := variants[vn]
			if v == nil {
				t.Fatalf("%s: unknown variant %q", sc.name, vn)
			}
			t.Run(sc.name+"/"+vn, func(t *testing.T) {
				if !opt.variantSelected(vn) {
					t.Skip("variant not selected")
				}
				env := newScenarioEnv(t, sc.name+strings.ToUpper(vn[:1])+vn[1:])
				start := time.Now()
				if v.beforeRun != nil {
					// A clean cell first, so the variant can measure it.
					if _, ok := executeCell(t, env, sc, plan.Run{}, nil); !ok {
						t.Fatalf("%s: the clean run before %s failed", sc.name, vn)
					}
				}
				hits, ok := executeCell(t, env, sc, plan.Run{}, v)
				if !ok {
					t.Fatalf("%s under %s: the clean run failed", sc.name, vn)
				}
				var kinds []string
				for _, k := range v.kinds {
					if opt.kindSelected(k) {
						kinds = append(kinds, k)
					}
				}
				patterns := sc.patterns
				if len(opt.points) > 0 {
					patterns = intersectPatterns(hits, sc.patterns, opt.points)
				}
				runs := plan.Enumerate(hits, patterns, kinds, 1)
				for _, r := range runs {
					r := r
					t.Run(r.String(), func(t *testing.T) { executeCell(t, env, sc, r, v) })
				}
				t.Logf("%s/%s: %d cells in %s", sc.name, vn, len(runs)+1, time.Since(start).Round(time.Second))
			})
		}
	}
	saveCoverage(t)
}

// oomLimit sets Redis maxmemory just above the working set: the keys of
// the previous (clean) cell of the same scenario are still present and
// their MEMORY USAGE is the working set W; the limit is the current
// used_memory plus W/2, so the run phase's own keys exceed it midway and
// writes fail with OOM until oomLift restores the limit. It applies only to
// the test Redis.
func oomLimit(ctx context.Context, c *cellRun) error {
	client, err := redisx.NewClient(ctx, redisx.Config{Addr: redisAddr})
	if err != nil {
		return err
	}
	defer client.Close()
	used, err := usedMemory(ctx, client)
	if err != nil {
		return err
	}
	working, err := prefixMemory(ctx, client, c.env.prev)
	if err != nil {
		return err
	}
	if working < 4096 {
		working = 4096
	}
	limit := used + working/2
	before, _ := oomErrors(ctx, client)
	c.note("oom: used_memory=%d working_set=%d maxmemory=%d oom_errors_before=%d", used, working, limit, before)
	return client.ConfigSet(ctx, "maxmemory", strconv.FormatInt(limit, 10)).Err()
}

// oomLift restores maxmemory and reports how many OOM errors surfaced.
func oomLift(ctx context.Context, c *cellRun) error {
	client, err := redisx.NewClient(ctx, redisx.Config{Addr: redisAddr})
	if err != nil {
		return err
	}
	defer client.Close()
	after, _ := oomErrors(ctx, client)
	c.note("oom: oom_errors_after=%d", after)
	c.t.Logf("oom variant: %d OOM error replies counted by Redis since start", after)
	return client.ConfigSet(ctx, "maxmemory", "0").Err()
}

func usedMemory(ctx context.Context, client *redis.Client) (int64, error) {
	info, err := client.Info(ctx, "memory").Result()
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "used_memory:"); ok {
			return strconv.ParseInt(v, 10, 64)
		}
	}
	return 0, fmt.Errorf("used_memory not in INFO memory")
}

// prefixMemory sums MEMORY USAGE over the keys under prefix.
func prefixMemory(ctx context.Context, client *redis.Client, prefix string) (int64, error) {
	if prefix == "" {
		return 0, nil
	}
	var total int64
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+":*", 1000).Result()
		if err != nil {
			return 0, err
		}
		for _, k := range keys {
			n, err := client.MemoryUsage(ctx, k).Result()
			if err == nil {
				total += n
			}
		}
		if next == 0 {
			return total, nil
		}
		cursor = next
	}
}

// oomErrors reads errorstat_OOM from INFO errorstats.
func oomErrors(ctx context.Context, client *redis.Client) (int64, error) {
	info, err := client.Info(ctx, "errorstats").Result()
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(info, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(line), "errorstat_OOM:count="); ok {
			return strconv.ParseInt(v, 10, 64)
		}
	}
	return 0, nil
}
