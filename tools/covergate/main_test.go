package main

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var spaces = regexp.MustCompile(` +`)

// contains compares with runs of spaces collapsed, so the assertions do not
// depend on tabwriter column widths.
func contains(out, want string) bool {
	return strings.Contains(spaces.ReplaceAllString(out, " "), spaces.ReplaceAllString(want, " "))
}

// fixture writes a module with synthetic sources under a temp root and
// returns the root. Line numbers in the profile below refer to these files.
func fixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	write(t, filepath.Join(root, "go.mod"), "module example.com/m\n\ngo 1.27\n")
	// mediator/a.go: line 7 carries a marker above the block starting at 8;
	// line 11 carries a marker on the block's own line.
	write(t, filepath.Join(root, "mediator", "a.go"), strings.Join([]string{
		"package mediator",    // 1
		"",                    // 2
		"func A(x int) int {", // 3
		"	if x > 0 {",         // 4
		"		return 1",          // 5
		"	}",                  // 6
		"	// covergate:ignore defensive branch, unreachable in tests", // 7
		"	if x < -100 {", // 8
		"		return -1",    // 9
		"	}",             // 10
		"	return 0 // covergate:ignore trailing marker", // 11
		"}",                                   // 12
		"",                                    // 13
		"// covergate:ignore matches nothing", // 14
		"var unused = 1",                      // 15
	}, "\n"))
	write(t, filepath.Join(root, "mediator", "pg", "b.go"), "package pg\n\nfunc B() {}\n\nfunc C() {}\n")
	write(t, filepath.Join(root, "mediator", "otel", "c.go"), "package otel\n")
	return root
}

const goodProfile = `mode: atomic
example.com/m/mediator/a.go:3.19,4.11 1 1
example.com/m/mediator/a.go:4.11,6.3 1 2
example.com/m/mediator/a.go:8.15,10.3 2 0
example.com/m/mediator/a.go:11.2,11.10 1 0
example.com/m/mediator/pg/b.go:3.11,3.13 1 0
example.com/m/mediator/pg/b.go:3.11,3.13 1 1
example.com/m/mediator/pg/b.go:5.11,5.13 1 1
example.com/m/mediator/otel/c.go:1.1,1.2 0 0
`

func write(t *testing.T, p, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func runGate(t *testing.T, root, profile string, extra ...string) (code int, stdout, stderr string) {
	t.Helper()
	pf := filepath.Join(root, "profile.out")
	write(t, pf, profile)
	var out, errb bytes.Buffer
	args := append([]string{"-profile", pf, "-root", root}, extra...)
	code = run(args, &out, &errb)
	return code, out.String(), errb.String()
}

func TestGatePassesWithIgnores(t *testing.T) {
	root := fixture(t)
	summary := filepath.Join(root, "summary.md")
	code, out, errs := runGate(t, root, goodProfile, "-summary", summary)
	if code != 0 {
		t.Fatalf("exit %d\nstdout:\n%s\nstderr:\n%s", code, out, errs)
	}
	for _, want := range []string{
		"mediator      2      2        3        100.0%    100.0%     ok",
		"mediator/pg   2      2        0        100.0%    95.0%      ok",
		"mediator/otel  0      0        0        -         -          skipped (no statements)",
		"ignored lines (2):",
		"mediator/a.go:7  2 stmt(s)  defensive branch, unreachable in tests",
		"mediator/a.go:11  1 stmt(s)  trailing marker",
		"warnings (1):",
		"mediator/a.go:14: covergate:ignore matches no coverage block (matches nothing)",
		"covergate: all 3 package(s) meet their thresholds",
	} {
		if !contains(out, want) {
			t.Errorf("stdout lacks %q:\n%s", want, out)
		}
	}
	md, err := os.ReadFile(summary)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"## Coverage gate",
		"| `mediator` | 2 | 2 | 3 | 100.0% | 100.0% | ok |",
		"| `mediator/otel` | 0 | 0 | 0 | - | - | skipped |",
		"| `mediator/a.go:7` | 2 | defensive branch, unreachable in tests |",
		"### Warnings",
		"**Result:** all 3 package(s) meet their thresholds.",
	} {
		if !strings.Contains(string(md), want) {
			t.Errorf("summary lacks %q:\n%s", want, md)
		}
	}
}

func TestGateFailsBelowThreshold(t *testing.T) {
	root := fixture(t)
	profile := strings.Replace(goodProfile, "pg/b.go:5.11,5.13 1 1", "pg/b.go:5.11,5.13 1 0", 1)
	code, out, _ := runGate(t, root, profile)
	if code != 1 {
		t.Fatalf("exit %d, want 1\n%s", code, out)
	}
	if !contains(out, "mediator/pg   2      1        0        50.0%     95.0%      FAIL") {
		t.Errorf("stdout:\n%s", out)
	}
	if !contains(out, "covergate: 1 of 3 package(s) below threshold") {
		t.Errorf("stdout:\n%s", out)
	}

	// An override lowers the bar for that package only.
	code, out, _ = runGate(t, root, profile, "-thresholds", "mediator/pg=50")
	if code != 0 {
		t.Errorf("with override: exit %d\n%s", code, out)
	}
	// A lower default does not touch the core packages.
	coreMiss := strings.Replace(goodProfile, "a.go:4.11,6.3 1 2", "a.go:4.11,6.3 1 0", 1)
	code, out, _ = runGate(t, root, coreMiss, "-default", "10")
	if code != 1 || !contains(out, "mediator      2      1        3        50.0%     100.0%     FAIL") {
		t.Errorf("core below 100 with default 10: exit %d\n%s", code, out)
	}
	// Markdown marks the failure.
	summary := filepath.Join(root, "s.md")
	if code, _, _ := runGate(t, root, profile, "-summary", summary); code != 1 {
		t.Fatal("expected failure")
	}
	md, _ := os.ReadFile(summary)
	if !strings.Contains(string(md), "| **FAIL** |") || !strings.Contains(string(md), "**Result:** 1 of 3 package(s) below threshold.") {
		t.Errorf("summary:\n%s", md)
	}
}

func TestMarkerWithoutReasonFails(t *testing.T) {
	root := fixture(t)
	write(t, filepath.Join(root, "mediator", "pg", "b.go"), "package pg\n\nfunc B() {} // covergate:ignore\n\nfunc C() {} //covergate:ignore   \n")
	code, _, errs := runGate(t, root, goodProfile)
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if !strings.Contains(errs, "b.go:3: covergate:ignore needs a reason") || !strings.Contains(errs, "b.go:5: covergate:ignore needs a reason") {
		t.Errorf("stderr:\n%s", errs)
	}
}

func TestOutsideModuleFilesAreCountedWithoutMarkers(t *testing.T) {
	root := fixture(t)
	profile := goodProfile + "other.org/dep/x.go:1.1,2.2 4 4\n"
	code, out, _ := runGate(t, root, profile)
	if code != 0 {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if !contains(out, "other.org/dep  4      4        0        100.0%    95.0%      ok") {
		t.Errorf("stdout:\n%s", out)
	}
	if !contains(out, "other.org/dep/x.go: outside module example.com/m, ignore markers not applied") {
		t.Errorf("stdout:\n%s", out)
	}
	// With an explicit -module that matches nothing every file is outside
	// the module: full import paths are reported and no marker applies, so
	// the ignored blocks count and mediator drops to 2 of 5 statements.
	code, out, _ = runGate(t, root, goodProfile, "-module", "example.com/elsewhere")
	if code != 1 || !contains(out, "example.com/m/mediator 5 2 0 40.0% 95.0% FAIL") {
		t.Errorf("explicit module: exit %d\n%s", code, out)
	}
}

func TestMissingSourceFileFails(t *testing.T) {
	root := fixture(t)
	profile := goodProfile + "example.com/m/mediator/missing.go:1.1,2.2 1 1\n"
	code, _, errs := runGate(t, root, profile)
	if code != 2 || !strings.Contains(errs, "missing.go") {
		t.Errorf("exit %d stderr %q", code, errs)
	}
}

func TestUsageAndProfileErrors(t *testing.T) {
	root := fixture(t)
	var out, errs bytes.Buffer
	if code := run([]string{"-bogus"}, &out, &errs); code != 2 {
		t.Errorf("bad flag: exit %d", code)
	}
	if code := run([]string{"-profile", filepath.Join(root, "nope.out"), "-root", root}, &out, &errs); code != 2 {
		t.Errorf("missing profile: exit %d", code)
	}
	if code := run([]string{"-profile", "x", "-root", filepath.Join(root, "nomod")}, &out, &errs); code != 2 {
		t.Errorf("missing go.mod: exit %d", code)
	}
	write(t, filepath.Join(root, "nomodule", "go.mod"), "go 1.27\n")
	if code := run([]string{"-profile", "x", "-root", filepath.Join(root, "nomodule")}, &out, &errs); code != 2 {
		t.Errorf("go.mod without module: exit %d", code)
	}
	for _, bad := range []string{"mediator", "mediator=abc", "=5", "mediator=101", "a=1,b"} {
		if code, _, errs := runGate(t, root, goodProfile, "-thresholds", bad); code != 2 {
			t.Errorf("-thresholds %q: exit %d, stderr %q", bad, code, errs)
		}
	}
	if code, _, _ := runGate(t, root, goodProfile, "-thresholds", " mediator/pg = 90 , ,"); code != 0 {
		t.Errorf("spaced thresholds rejected: exit %d", code)
	}
	if code, _, errs := runGate(t, root, "mode: atomic\ngarbage line\n"); code != 2 || !strings.Contains(errs, "profile line 2") {
		t.Errorf("bad profile line: exit %d stderr %q", code, errs)
	}
	if code, _, errs := runGate(t, root, "example.com/m/mediator/a.go:3.19,4.11 1 1\n"); code != 2 || !strings.Contains(errs, "no mode line") {
		t.Errorf("no mode: exit %d stderr %q", code, errs)
	}
	if code, _, errs := runGate(t, root, goodProfile, "-summary", filepath.Join(root, "nodir", "s.md")); code != 2 || !strings.Contains(errs, "write summary") {
		t.Errorf("unwritable summary: exit %d stderr %q", code, errs)
	}
}

func TestParseProfileMergesDuplicates(t *testing.T) {
	_, blocks, err := parseProfile(strings.NewReader("mode: set\na.go:1.1,2.2 3 0\na.go:1.1,2.2 3 1\na.go:3.1,4.2 1 0\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(blocks) != 2 || blocks[0].count != 1 || blocks[0].stmts != 3 || blocks[1].count != 0 {
		t.Errorf("blocks = %+v", blocks)
	}
	if blocks[0].startLine != 1 || blocks[0].startCol != 1 || blocks[0].endLine != 2 || blocks[0].endCol != 2 {
		t.Errorf("range = %+v", blocks[0])
	}
}

func TestPkgResult(t *testing.T) {
	p := pkgResult{stmts: 1000, covered: 999, threshold: 100}
	if p.ok() {
		t.Error("99.9% must not pass a 100% threshold")
	}
	p.threshold = 99.9
	if !p.ok() {
		t.Error("99.9% must pass a 99.9% threshold")
	}
	if got := (pkgResult{}).percent(); got != 0 {
		t.Errorf("empty percent = %v", got)
	}
}

func TestScanMarkersMissingFile(t *testing.T) {
	if _, err := scanMarkers(filepath.Join(t.TempDir(), "missing.go")); err == nil {
		t.Error("expected error")
	}
}
