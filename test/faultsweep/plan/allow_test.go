package plan

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCompletenessWith_PerKindAllowances(t *testing.T) {
	kinds := []string{"error", "ambiguous", "crash"}
	cov := NewCoverage()
	cov.Mark("a", "error")
	cov.Mark("a", "ambiguous")
	cov.Mark("a", "crash")
	cov.Mark("b", "error")
	cov.Mark("b", "crash")
	cov.Mark("c", "error")
	cov.Mark("d", "error")
	cov.Mark("d", "ambiguous")
	allowed := map[string]Allowance{
		"b": AllowKinds("no FaultAfter hook", "ambiguous"),
		"c": AllowKinds("no FaultAfter hook", "ambiguous"),
		"d": AllowKinds("was unreachable", "ambiguous"),
		"e": Allow("exercised elsewhere"),
		"f": {Kinds: []string{"error"}},
	}
	gaps, stale := CompletenessWith([]string{"a", "b", "c", "d", "e", "f", "g"}, cov, kinds, allowed)
	want := []Gap{
		{Point: "c", Missing: []string{"crash"}},
		{Point: "d", Missing: []string{"crash"}},
		{Point: "f", Missing: []string{"allowance without a reason"}},
		{Point: "g"},
	}
	if !reflect.DeepEqual(gaps, want) {
		t.Fatalf("gaps = %v, want %v", gaps, want)
	}
	// d's excused kind was covered anyway: the allowance is stale. b's was
	// not, so it stays. e excuses everything and nothing was covered.
	if !reflect.DeepEqual(stale, []string{"d"}) {
		t.Fatalf("stale = %v", stale)
	}
	// Completeness delegates: every-kind allowances behave as before.
	gaps, stale = Completeness([]string{"a", "e"}, cov, kinds, map[string]string{"e": "elsewhere"})
	if len(gaps) != 0 || len(stale) != 0 {
		t.Fatalf("delegation: %v %v", gaps, stale)
	}
	if !Allow("r").excuses("anything") || AllowKinds("r", "x").excuses("y") || !AllowKinds("r", "x").excuses("x") {
		t.Fatal("excuses")
	}
}

func TestAfterPoints_ScansNonTestSources(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		t.Helper()
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("pg/tx.go", `package pg
func f(ctx context.Context) error {
	if err := testkit.Fault(ctx, "pg.tx.commit"); err != nil { return err }
	return testkit.FaultAfter(ctx, "pg.tx.commit")
}
func g(ctx context.Context) error {
	if err := testkit.Fault(ctx, "pg.tx.begin"); err != nil { return err }
	return nil
}`)
	write("redisx/streams.go", `package redisx
func a(ctx context.Context) error { return testkit.FaultAfter( ctx,  "redis.xadd") }`)
	write("redisx/streams_test.go", `package redisx
func b(ctx context.Context) error { return testkit.FaultAfter(ctx, "redis.test.only") }`)
	write("testdata/x.go", `package x
func c(ctx context.Context) error { return testkit.FaultAfter(ctx, "ignored.testdata") }`)
	got, err := AfterPoints(root)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"pg.tx.commit", "redis.xadd"}) {
		t.Fatalf("after points = %v", got)
	}
	if _, err := AfterPoints(filepath.Join(root, "missing")); err == nil {
		t.Fatal("missing root must fail")
	}
}

func TestCoverage_SaveLoadMerges(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sub", "coverage.json")
	if _, err := LoadCoverage(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file: %v", err)
	}
	first := NewCoverage()
	first.Mark("a", "error")
	first.Mark("b", "crash")
	if err := first.Save(path); err != nil {
		t.Fatal(err)
	}
	second := NewCoverage()
	second.Mark("a", "timeout")
	if err := second.Save(path); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCoverage(path)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string][]string{"a": {"error", "timeout"}, "b": {"crash"}}
	if !reflect.DeepEqual(got.Cells(), want) {
		t.Fatalf("cells = %v", got.Cells())
	}
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCoverage(path); err == nil {
		t.Fatal("corrupt file must fail")
	}
	if err := first.Save(path); err == nil {
		t.Fatal("save over a corrupt file must fail rather than overwrite")
	}
}
