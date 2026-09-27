package plan

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestEnumerate_EveryCellOnce(t *testing.T) {
	hits := map[string]int{"pg.tx.commit": 2, "redis.xadd": 1, "pg.tx.begin": 3, "never": 0}
	kinds := []string{"error", "ambiguous", "crash"}
	runs := Enumerate(hits, nil, kinds, 0)
	want := 2*3 + 1*3 + 3*3
	if len(runs) != want {
		t.Fatalf("got %d runs, want %d: %v", len(runs), want, runs)
	}
	seen := map[Run]bool{}
	for _, r := range runs {
		if seen[r] {
			t.Fatalf("cell %s enumerated twice", r)
		}
		seen[r] = true
		if r.Hit < 1 || r.Hit > hits[r.Point] {
			t.Fatalf("cell %s outside the recorded hits", r)
		}
	}
	// Sorted by point, then hit, then kind order.
	if runs[0] != (Run{"pg.tx.begin", 1, "error"}) || runs[1] != (Run{"pg.tx.begin", 1, "ambiguous"}) || runs[3] != (Run{"pg.tx.begin", 2, "error"}) {
		t.Fatalf("order: %v", runs[:4])
	}
	if runs[len(runs)-1] != (Run{"redis.xadd", 1, "crash"}) {
		t.Fatalf("last: %v", runs[len(runs)-1])
	}
	if s := runs[0].String(); s != "pg.tx.begin#1/error" {
		t.Fatalf("String = %q", s)
	}

	// maxHits caps the hit index; patterns narrow the points.
	if got := Enumerate(hits, nil, kinds, 1); len(got) != 3*3 {
		t.Fatalf("maxHits=1: %d", len(got))
	}
	if got := Enumerate(hits, []string{"pg.tx.*"}, []string{"error"}, 0); len(got) != 5 {
		t.Fatalf("pattern: %v", got)
	}
	if got := Enumerate(hits, []string{"redis.xadd", " pg.tx.begin "}, []string{"error"}, 0); len(got) != 4 {
		t.Fatalf("list: %v", got)
	}
	if got := Enumerate(nil, nil, kinds, 0); len(got) != 0 {
		t.Fatalf("empty hits: %v", got)
	}
}

func TestMatchAndSplit(t *testing.T) {
	if !MatchPoint("pg.*", "pg.tx.commit") || MatchPoint("pg.*", "redis.xadd") || !MatchPoint("redis.xadd", "redis.xadd") || MatchPoint("redis.xadd", "redis.xadd.dlq") {
		t.Fatal("MatchPoint")
	}
	if !MatchAny(nil, "x") || !MatchAny([]string{}, "x") || MatchAny([]string{"a"}, "x") || !MatchAny([]string{"a", "x"}, "x") {
		t.Fatal("MatchAny")
	}
	if got := SplitList(" a, ,b ,"); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("SplitList = %v", got)
	}
	if got := SplitList(""); got != nil {
		t.Fatalf("SplitList empty = %v", got)
	}
}

func TestParseCatalogue_GoldenFile(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "..", "..", "mediator", "testkit", "faultpoints.txt"))
	if err != nil {
		t.Fatal(err)
	}
	points := ParseCatalogue(string(src))
	if len(points) != 40 {
		t.Fatalf("catalogue has %d points: %v", len(points), points)
	}
	for _, p := range points {
		if strings.ContainsAny(p, "# \t") {
			t.Fatalf("bad token %q", p)
		}
	}
	if points[0] != "http.decode" || points[len(points)-1] != "redis.xreadgroup" {
		t.Fatalf("not sorted: %v", points)
	}
	if got := ParseCatalogue("a b # c\n# d\ne"); !reflect.DeepEqual(got, []string{"a", "b", "e"}) {
		t.Fatalf("comments: %v", got)
	}
}

func TestCoverageAndCompleteness(t *testing.T) {
	kinds := []string{"error", "crash"}
	cov := NewCoverage()
	cov.Mark("a", "error")
	cov.Mark("a", "crash")
	cov.Mark("b", "error")
	other := NewCoverage()
	other.Mark("d", "error")
	other.Mark("d", "crash")
	cov.Merge(other)
	if !cov.Has("d", "crash") || cov.Has("c", "error") || !reflect.DeepEqual(cov.Points(), []string{"a", "b", "d"}) || !reflect.DeepEqual(cov.Kinds("a"), []string{"crash", "error"}) {
		t.Fatal("coverage")
	}
	gaps, stale := Completeness([]string{"a", "b", "c", "d", "e"}, cov, kinds, map[string]string{"c": "unreachable", "d": "was unreachable"})
	if len(gaps) != 2 || gaps[0].Point != "b" || !reflect.DeepEqual(gaps[0].Missing, []string{"crash"}) || gaps[1].Point != "e" || gaps[1].Missing != nil {
		t.Fatalf("gaps = %v", gaps)
	}
	if gaps[0].String() != "b: missing crash" || gaps[1].String() != "e: never reached" {
		t.Fatalf("strings: %v %v", gaps[0], gaps[1])
	}
	if !reflect.DeepEqual(stale, []string{"d"}) {
		t.Fatalf("stale = %v", stale)
	}
	gaps, _ = Completeness([]string{"a"}, cov, kinds, nil)
	if len(gaps) != 0 {
		t.Fatalf("complete catalogue reports %v", gaps)
	}
}

func TestChildArgs_RoundTrip(t *testing.T) {
	a := ChildArgs{
		Scenario: "SweepCommandAtomicity", Point: "pg.tx.commit", Hit: 2, Kind: "crash",
		PGURL: "postgres://u:p@h:5/d?sslmode=disable", Schema: "fs_abc", RedisAddr: "h:6379",
		Prefix: "fs1", ObservedFile: `C:\tmp\obs.txt`, Variant: "pool1",
	}
	env := map[string]string{}
	for _, kv := range a.Environ() {
		k, v, ok := strings.Cut(kv, "=")
		if !ok {
			t.Fatalf("bad pair %q", kv)
		}
		env[k] = v
	}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	got, err := ChildArgsFromEnv(lookup)
	if err != nil || got != a {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	a.Recover = true
	a.Point, a.Hit, a.Kind = "", 0, ""
	env = map[string]string{}
	for _, kv := range a.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	if got, err := ChildArgsFromEnv(lookup); err != nil || got != a {
		t.Fatalf("recover round trip: %+v %v", got, err)
	}

	// Not a child.
	if _, err := ChildArgsFromEnv(func(string) (string, bool) { return "", false }); err != ErrNotChild {
		t.Fatalf("not child: %v", err)
	}
	// Missing values and a bad hit are reported by name.
	env = map[string]string{EnvChild: "1", EnvScenario: "s"}
	if _, err := ChildArgsFromEnv(lookup); err == nil || !strings.Contains(err.Error(), EnvPGURL) || !strings.Contains(err.Error(), EnvPrefix) {
		t.Fatalf("missing: %v", err)
	}
	env = map[string]string{EnvChild: "1", EnvScenario: "s", EnvPGURL: "u", EnvSchema: "s", EnvRedisAddr: "r", EnvPrefix: "p", EnvPoint: "x", EnvHit: "zero"}
	if _, err := ChildArgsFromEnv(lookup); err == nil || !strings.Contains(err.Error(), EnvHit) {
		t.Fatalf("bad hit: %v", err)
	}
	delete(env, EnvPoint)
	env[EnvHit] = "1"
	if _, err := ChildArgsFromEnv(lookup); err == nil || !strings.Contains(err.Error(), EnvPoint) {
		t.Fatalf("missing point: %v", err)
	}
}
