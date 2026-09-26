// Package plan holds the parts of the fault-sweep tier that need neither
// containers nor the faultinject build tag: the enumeration of
// (point, hit, kind) runs from a recorded hit map, the completeness
// comparison of spec 11.8, the marshaling of a crash run into the child
// process environment, and a memstore-based dry run of the atomicity and
// idempotency scenarios that runs on every push.
//
// test/faultsweep imports it; its own tests run with plain go test.
package plan

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Run is one cell of the sweep matrix: fault Kind armed at the Hit-th
// (1-based) reach of Point.
type Run struct {
	Point string
	Hit   int
	Kind  string
}

// String renders the cell as "point#hit/kind", which is also the subtest name.
func (r Run) String() string { return r.Point + "#" + strconv.Itoa(r.Hit) + "/" + r.Kind }

// Enumerate builds the runs of one scenario from the hit map its clean run
// recorded: for every point (sorted) that matches one of the patterns (nil
// or empty means every point), for every hit index 1..min(n, maxHits)
// (maxHits <= 0 means all), for every kind in the order given. Each cell
// appears exactly once.
func Enumerate(hits map[string]int, patterns []string, kinds []string, maxHits int) []Run {
	points := make([]string, 0, len(hits))
	for p, n := range hits {
		if n > 0 && MatchAny(patterns, p) {
			points = append(points, p)
		}
	}
	sort.Strings(points)
	var out []Run
	for _, p := range points {
		n := hits[p]
		if maxHits > 0 && n > maxHits {
			n = maxHits
		}
		for h := 1; h <= n; h++ {
			for _, k := range kinds {
				out = append(out, Run{Point: p, Hit: h, Kind: k})
			}
		}
	}
	return out
}

// MatchPoint reports whether point matches pattern: an exact name, or a
// prefix followed by "*" (for example "redis.lease.*" or "pg.*").
func MatchPoint(pattern, point string) bool {
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(point, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == point
}

// MatchAny reports whether point matches any pattern; no patterns match all.
func MatchAny(patterns []string, point string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if MatchPoint(strings.TrimSpace(p), point) {
			return true
		}
	}
	return false
}

// SplitList splits a comma-separated flag value, dropping empties.
func SplitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ParseCatalogue parses faultpoints.txt: whitespace-separated point names,
// with "#" starting a comment that runs to the end of the line.
func ParseCatalogue(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		out = append(out, strings.Fields(line)...)
	}
	sort.Strings(out)
	return out
}

// Coverage records which (point, kind) cells the sweep exercised: a cell
// counts when the point was reached at the armed hit index while that kind
// was armed (for crash, when the child exited with 137 at that point). It
// is safe for concurrent use.
type Coverage struct {
	mu sync.Mutex
	m  map[string]map[string]bool
}

// NewCoverage returns an empty record.
func NewCoverage() *Coverage { return &Coverage{m: map[string]map[string]bool{}} }

// Mark records that kind was exercised at point.
func (c *Coverage) Mark(point, kind string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m[point] == nil {
		c.m[point] = map[string]bool{}
	}
	c.m[point][kind] = true
}

// Has reports whether the cell was exercised.
func (c *Coverage) Has(point, kind string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[point][kind]
}

// Kinds returns the kinds exercised at point, sorted.
func (c *Coverage) Kinds(point string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.m[point]))
	for k := range c.m[point] {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Points returns every point with at least one exercised kind, sorted.
func (c *Coverage) Points() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.m))
	for p := range c.m {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// Merge adds every cell of o.
func (c *Coverage) Merge(o *Coverage) {
	o.mu.Lock()
	cells := make([][2]string, 0)
	for p, ks := range o.m {
		for k := range ks {
			cells = append(cells, [2]string{p, k})
		}
	}
	o.mu.Unlock()
	for _, cell := range cells {
		c.Mark(cell[0], cell[1])
	}
}

// Gap is one hole in the completeness gate: a catalogued point that some
// kinds never exercised.
type Gap struct {
	Point string
	// Missing lists the kinds never exercised at the point, in the order of
	// the kinds passed to Completeness. Empty means the point was never
	// reached by any kind.
	Missing []string
}

func (g Gap) String() string {
	if len(g.Missing) == 0 {
		return g.Point + ": never reached"
	}
	return g.Point + ": missing " + strings.Join(g.Missing, ",")
}

// Completeness is the gate of spec 11.8: every catalogued point that is not
// in allowed (point -> reason) must have been exercised by every kind. It
// returns the gaps sorted by point, and the allowed points that were in fact
// fully covered, so a stale allowance can be reported and removed. See
// CompletenessWith for per-kind allowances.
func Completeness(catalogue []string, cov *Coverage, kinds []string, allowed map[string]string) (gaps []Gap, staleAllowed []string) {
	conv := make(map[string]Allowance, len(allowed))
	for p, reason := range allowed {
		conv[p] = Allow(reason)
	}
	return CompletenessWith(catalogue, cov, kinds, conv)
}

// Environment variable names of a child process run.
const (
	EnvChild     = "FAULTSWEEP_CHILD"
	EnvRecover   = "FAULTSWEEP_RECOVER"
	EnvScenario  = "FAULTSWEEP_SCENARIO"
	EnvPoint     = "FAULTSWEEP_POINT"
	EnvHit       = "FAULTSWEEP_HIT"
	EnvKind      = "FAULTSWEEP_KIND"
	EnvPGURL     = "FAULTSWEEP_PG_URL"
	EnvSchema    = "FAULTSWEEP_SCHEMA"
	EnvRedisAddr = "FAULTSWEEP_REDIS_ADDR"
	EnvPrefix    = "FAULTSWEEP_PREFIX"
	EnvObserved  = "FAULTSWEEP_OBSERVED"
	EnvVariant   = "FAULTSWEEP_VARIANT"
)

// ChildArgs is everything a crash-sweep child process needs: which scenario
// to run, the fault to arm (or Recover to run recovery instead), and the
// database, schema, Redis address, and key prefix the parent prepared.
// ObservedFile is where the child writes the fault points it reached.
type ChildArgs struct {
	Scenario     string
	Point        string
	Hit          int
	Kind         string
	Recover      bool
	PGURL        string
	Schema       string
	RedisAddr    string
	Prefix       string
	ObservedFile string
	// Variant names a resource-exhaustion variant, "" for the plain run.
	Variant string
}

// Environ renders the arguments as KEY=VALUE pairs for exec.Cmd.Env. The
// child marker is always set.
func (a ChildArgs) Environ() []string {
	recover := "0"
	if a.Recover {
		recover = "1"
	}
	return []string{
		EnvChild + "=1",
		EnvRecover + "=" + recover,
		EnvScenario + "=" + a.Scenario,
		EnvPoint + "=" + a.Point,
		EnvHit + "=" + strconv.Itoa(a.Hit),
		EnvKind + "=" + a.Kind,
		EnvPGURL + "=" + a.PGURL,
		EnvSchema + "=" + a.Schema,
		EnvRedisAddr + "=" + a.RedisAddr,
		EnvPrefix + "=" + a.Prefix,
		EnvObserved + "=" + a.ObservedFile,
		EnvVariant + "=" + a.Variant,
	}
}

// ErrNotChild is returned by ChildArgsFromEnv when the child marker is absent.
var ErrNotChild = errors.New("plan: not a fault-sweep child process")

// ChildArgsFromEnv parses the arguments of Environ from an environment
// lookup (os.LookupEnv). It returns ErrNotChild when the child marker is
// not set and a descriptive error when a required value is missing or
// malformed.
func ChildArgsFromEnv(lookup func(string) (string, bool)) (ChildArgs, error) {
	if v, ok := lookup(EnvChild); !ok || v != "1" {
		return ChildArgs{}, ErrNotChild
	}
	get := func(name string) string {
		v, _ := lookup(name)
		return v
	}
	a := ChildArgs{
		Scenario: get(EnvScenario), Point: get(EnvPoint), Kind: get(EnvKind),
		PGURL: get(EnvPGURL), Schema: get(EnvSchema), RedisAddr: get(EnvRedisAddr),
		Prefix: get(EnvPrefix), ObservedFile: get(EnvObserved), Variant: get(EnvVariant),
		Recover: get(EnvRecover) == "1",
	}
	var missing []string
	for name, v := range map[string]string{EnvScenario: a.Scenario, EnvPGURL: a.PGURL, EnvSchema: a.Schema, EnvRedisAddr: a.RedisAddr, EnvPrefix: a.Prefix} {
		if v == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return ChildArgs{}, fmt.Errorf("plan: child environment lacks %s", strings.Join(missing, ", "))
	}
	if !a.Recover {
		if a.Point == "" {
			return ChildArgs{}, fmt.Errorf("plan: child environment lacks %s", EnvPoint)
		}
		hit, err := strconv.Atoi(get(EnvHit))
		if err != nil || hit < 1 {
			return ChildArgs{}, fmt.Errorf("plan: %s must be a positive integer, got %q", EnvHit, get(EnvHit))
		}
		a.Hit = hit
	}
	return a, nil
}
