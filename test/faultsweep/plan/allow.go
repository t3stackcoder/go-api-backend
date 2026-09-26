package plan

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// Allowance excuses a catalogued point from the completeness gate, for the
// listed Kinds or, when Kinds is nil, for every kind. Reason is reported
// with the allowance and must not be empty.
type Allowance struct {
	Kinds  []string
	Reason string
}

// Allow excuses every kind at a point.
func Allow(reason string) Allowance { return Allowance{Reason: reason} }

// AllowKinds excuses only the listed kinds at a point; the remaining kinds
// must still be exercised.
func AllowKinds(reason string, kinds ...string) Allowance {
	return Allowance{Kinds: append([]string(nil), kinds...), Reason: reason}
}

func (a Allowance) excuses(kind string) bool {
	if a.Kinds == nil {
		return true
	}
	for _, k := range a.Kinds {
		if k == kind {
			return true
		}
	}
	return false
}

// CompletenessWith is Completeness with per-kind allowances. Every
// catalogued point must be exercised by every kind its allowance (if any)
// does not excuse. An allowance is stale, and reported in staleAllowed,
// when every kind it excuses was exercised anyway. An allowance without a
// reason is reported as a gap of its own so that exclusions stay justified.
func CompletenessWith(catalogue []string, cov *Coverage, kinds []string, allowed map[string]Allowance) (gaps []Gap, staleAllowed []string) {
	for _, p := range catalogue {
		a, excused := allowed[p]
		if excused && strings.TrimSpace(a.Reason) == "" {
			gaps = append(gaps, Gap{Point: p, Missing: []string{"allowance without a reason"}})
			continue
		}
		var missing []string
		excusedCovered := true
		for _, k := range kinds {
			has := cov.Has(p, k)
			if excused && a.excuses(k) {
				if !has {
					excusedCovered = false
				}
				continue
			}
			if !has {
				missing = append(missing, k)
			}
		}
		if excused && excusedCovered {
			staleAllowed = append(staleAllowed, p)
		}
		if len(missing) == len(kinds) {
			gaps = append(gaps, Gap{Point: p})
		} else if len(missing) > 0 {
			gaps = append(gaps, Gap{Point: p, Missing: missing})
		}
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i].Point < gaps[j].Point })
	sort.Strings(staleAllowed)
	return gaps, staleAllowed
}

var faultAfterRe = regexp.MustCompile(`testkit\.FaultAfter\(\s*[^,]+,\s*"([a-z0-9_.]+)"`)

// AfterPoints scans the non-test Go sources under root for calls of
// testkit.FaultAfter and returns the point names, sorted and unique. The
// ambiguous and crash kinds act only at those points: everywhere else
// testkit.Fault records them and nothing consumes the record, so the sweep
// skips those cells and the completeness gate excuses them by construction.
func AfterPoints(root string) ([]string, error) {
	r, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("plan: scan %s: %w", root, err)
	}
	defer r.Close()
	fsys := r.FS()
	seen := map[string]bool{}
	err = fs.WalkDir(fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "testdata", "vendor":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := fs.ReadFile(fsys, path)
		if err != nil {
			return err
		}
		for _, m := range faultAfterRe.FindAllSubmatch(src, -1) {
			seen[string(m[1])] = true
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("plan: scan %s: %w", root, err)
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out, nil
}

// Cells returns the exercised (point -> sorted kinds) map.
func (c *Coverage) Cells() map[string][]string {
	out := map[string][]string{}
	for _, p := range c.Points() {
		out[p] = c.Kinds(p)
	}
	return out
}

// Save merges the coverage into the JSON file at path (created when
// missing), so that runs split across processes or invocations accumulate
// one record for the completeness gate.
func (c *Coverage) Save(path string) error {
	merged := NewCoverage()
	if prev, err := LoadCoverage(path); err == nil {
		merged.Merge(prev)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	merged.Merge(c)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("plan: save coverage: %w", err)
	}
	b, err := json.Marshal(merged.Cells(), json.Deterministic(true))
	if err != nil {
		return fmt.Errorf("plan: save coverage: %w", err)
	}
	if err := os.WriteFile(path, b, 0o600); err != nil {
		return fmt.Errorf("plan: save coverage: %w", err)
	}
	return nil
}

// LoadCoverage reads a file written by Save. A missing file returns an
// error wrapping os.ErrNotExist.
func LoadCoverage(path string) (*Coverage, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("plan: load coverage: %w", err)
	}
	var cells map[string][]string
	if err := json.Unmarshal(b, &cells); err != nil {
		return nil, fmt.Errorf("plan: load coverage %s: %w", path, err)
	}
	cov := NewCoverage()
	for p, kinds := range cells {
		for _, k := range kinds {
			cov.Mark(p, k)
		}
	}
	return cov, nil
}
