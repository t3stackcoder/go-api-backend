//go:build faultsweep && faultinject

package faultsweep

import (
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/test/faultsweep/plan"
)

// exclusions are the justified allowances of the completeness gate. Every
// entry must carry a reason; an entry whose point turns out fully covered
// fails the gate as stale. Allowances for the ambiguous kind at points
// without a testkit.FaultAfter call site are generated from the sources,
// not listed here, so adding a FaultAfter call automatically demands the
// ambiguous cell.
var exclusions = map[string]plan.Allowance{
	"pg.migrate.apply": plan.Allow("migrations run before any schedule is armed; TestIntegration_Migrations_UpDownUp in mediator/pg applies, reverts, and re-applies every migration"),
	"http.sse.write":   plan.Allow("no sweep scenario streams over SSE; the faultinject tests of mediator/httpapi cover the point"),
}

// TestFaultSweepCompleteness is the gate of spec 11.8: every point of
// mediator/testkit/faultpoints.txt is exercised by every kind in at least
// one scenario, except the justified exclusions above. It runs last in the
// process (file order) and merges SWEEP_COVERAGE_FILE when set, so a run
// split over several invocations can still be gated.
func TestFaultSweepCompleteness(t *testing.T) {
	ran := 0
	sweepsRun.Range(func(_, _ any) bool { ran++; return true })
	// A hit cap (SWEEP_QUICK) still exercises every point with every kind
	// at its first hit; only scenario, point, and kind filters make a run
	// partial.
	filtered := len(opt.scenarios) > 0 || len(opt.points) > 0 || len(opt.kinds) > 0
	cov := plan.NewCoverage()
	cov.Merge(coverage)
	if opt.coverageFile != "" {
		if prev, err := plan.LoadCoverage(opt.coverageFile); err == nil {
			cov.Merge(prev)
		} else if !os.IsNotExist(err) && !strings.Contains(err.Error(), "cannot find") {
			t.Fatalf("load coverage: %v", err)
		}
	} else if filtered {
		t.Skip("SWEEP_* filters narrowed this run; completeness is meaningful for the full matrix or with SWEEP_COVERAGE_FILE")
	} else if ran == 0 {
		t.Fatal("no scenario sweep ran in this process and SWEEP_COVERAGE_FILE is unset; the gate cannot be evaluated")
	}

	raw, err := os.ReadFile(filepath.Join(sourceRoot(), "mediator", "testkit", "faultpoints.txt"))
	if err != nil {
		t.Fatalf("read catalogue: %v", err)
	}
	catalogue := plan.ParseCatalogue(string(raw))
	after := afterPoints(t)
	allowed := make(map[string]plan.Allowance, len(exclusions))
	for p, a := range exclusions {
		if !slices.Contains(catalogue, p) {
			t.Errorf("exclusion for %s names a point that is not in the catalogue", p)
		}
		allowed[p] = a
	}
	for _, p := range catalogue {
		if _, ok := allowed[p]; ok || slices.Contains(after, p) {
			continue
		}
		allowed[p] = plan.AllowKinds("no testkit.FaultAfter call site: the ambiguous kind cannot act here", string(testkit.FaultAmbiguous))
	}
	gaps, stale := plan.CompletenessWith(catalogue, cov, gateKinds, allowed)
	for _, g := range gaps {
		t.Errorf("incomplete: %s", g)
	}
	for _, p := range stale {
		t.Errorf("stale exclusion: %s is fully covered; remove its allowance", p)
	}

	// Every point must have been observed at least once, in this process
	// or by a recovery child, unless excluded outright.
	observed := map[string]bool{}
	for _, p := range testkit.Observed() {
		observed[p] = true
	}
	observedByChildren.Range(func(k, _ any) bool { observed[k.(string)] = true; return true })
	for _, p := range cov.Points() {
		observed[p] = true
	}
	for _, p := range catalogue {
		if a, ok := exclusions[p]; ok && a.Kinds == nil {
			continue
		}
		if !observed[p] {
			t.Errorf("never observed: %s", p)
		}
	}

	// Summary: points covered per kind.
	var lines []string
	for _, k := range gateKinds {
		n := 0
		for _, p := range catalogue {
			if cov.Has(p, k) {
				n++
			}
		}
		lines = append(lines, k+"="+strconv.Itoa(n)+"/"+strconv.Itoa(len(catalogue)))
	}
	sort.Strings(lines)
	t.Logf("coverage per kind: %s; excluded outright: %d; ambiguous excused at %d points without FaultAfter",
		strings.Join(lines, " "), len(exclusions), len(catalogue)-len(after)-len(exclusions))
}
