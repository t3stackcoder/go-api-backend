// Command covergate enforces per-package statement coverage thresholds on a
// Go cover profile (spec 11.2).
//
//	go run ./tools/covergate -profile coverage/unit.out [flags]
//
// Rules:
//
//   - mediator, mediator/behavior and mediator/validate must reach 100 percent;
//     every other package must reach the -default threshold (95 percent).
//     -thresholds pkg=pct,... overrides both for named packages.
//   - Packages with zero statements in the profile are skipped.
//   - A coverage block is excluded when its first line, or the line
//     immediately above it, carries a `// covergate:ignore <reason>` marker.
//     The reason is mandatory; a marker without one fails the run. Excluded
//     lines are listed with their reasons, and markers that match no block
//     are reported as warnings.
//   - Duplicate blocks (one per test binary that linked the package under
//     -coverpkg) are merged by summing their counts.
//
// Exit codes: 0 every package meets its threshold, 1 at least one does not,
// 2 the profile, flags, sources, or markers are invalid.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
)

// coreThresholds are the packages spec 11.2 holds to full coverage.
var coreThresholds = map[string]float64{
	"mediator":          100,
	"mediator/behavior": 100,
	"mediator/validate": 100,
}

// block is one range of a cover profile.
type block struct {
	file                                 string // import-path-qualified file name as written in the profile
	startLine, startCol, endLine, endCol int
	stmts, count                         int
}

// marker is one covergate:ignore comment in a source file.
type marker struct {
	line   int
	reason string
	used   bool
}

// ignoredLine is one marker that excluded coverage blocks.
type ignoredLine struct {
	file   string // module-relative path
	line   int
	stmts  int
	reason string
}

// pkgResult is the outcome for one package.
type pkgResult struct {
	pkg       string
	stmts     int
	covered   int
	ignored   int
	threshold float64
}

// percent is the statement coverage after exclusions.
func (p pkgResult) percent() float64 {
	if p.stmts == 0 {
		return 0
	}
	return float64(p.covered) * 100 / float64(p.stmts)
}

// skipped reports whether the package has no statements to measure.
func (p pkgResult) skipped() bool { return p.stmts == 0 }

// ok reports whether the package meets its threshold.
func (p pkgResult) ok() bool {
	return p.skipped() || p.percent()+1e-9 >= p.threshold
}

// report is the full outcome of a run.
type report struct {
	profile  string
	mode     string
	pkgs     []pkgResult
	ignored  []ignoredLine
	warnings []string
}

// failed counts the packages below threshold.
func (r report) failed() int {
	n := 0
	for _, p := range r.pkgs {
		if !p.ok() {
			n++
		}
	}
	return n
}

// config holds the parsed flags.
type config struct {
	profile          string
	summary          string
	root             string
	module           string
	defaultThreshold float64
	thresholds       map[string]float64
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the gate and returns the exit code.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("covergate", flag.ContinueOnError)
	fs.SetOutput(stderr)
	cfg := config{}
	fs.StringVar(&cfg.profile, "profile", "coverage/unit.out", "cover profile written by go test -coverprofile")
	thresholds := fs.String("thresholds", "", "overrides as pkg=pct,... with pkg relative to the module (mediator/pg=90)")
	fs.Float64Var(&cfg.defaultThreshold, "default", 95, "threshold for packages without a rule")
	fs.StringVar(&cfg.summary, "summary", "", "write a Markdown summary to this file")
	fs.StringVar(&cfg.root, "root", ".", "module root used to resolve source files")
	fs.StringVar(&cfg.module, "module", "", "module path; read from go.mod under -root when empty")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	var err error
	if cfg.thresholds, err = parseThresholds(*thresholds); err != nil {
		fmt.Fprintf(stderr, "covergate: -thresholds: %v\n", err)
		return 2
	}
	if cfg.module == "" {
		if cfg.module, err = modulePath(filepath.Join(cfg.root, "go.mod")); err != nil {
			fmt.Fprintf(stderr, "covergate: %v\n", err)
			return 2
		}
	}
	rep, err := gate(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "covergate: %v\n", err)
		return 2
	}
	printReport(stdout, rep)
	if cfg.summary != "" {
		if err := os.WriteFile(cfg.summary, []byte(markdown(rep)), 0o644); err != nil {
			fmt.Fprintf(stderr, "covergate: write summary: %v\n", err)
			return 2
		}
		fmt.Fprintf(stdout, "covergate: summary written to %s\n", cfg.summary)
	}
	if rep.failed() > 0 {
		return 1
	}
	return 0
}

// parseThresholds parses "pkg=pct,pkg=pct".
func parseThresholds(s string) (map[string]float64, error) {
	out := map[string]float64{}
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		pkg, pct, ok := strings.Cut(item, "=")
		if !ok || strings.TrimSpace(pkg) == "" {
			return nil, fmt.Errorf("%q is not pkg=pct", item)
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(pct), 64)
		if err != nil || v < 0 || v > 100 {
			return nil, fmt.Errorf("%q: percentage must be a number between 0 and 100", item)
		}
		out[strings.TrimSpace(pkg)] = v
	}
	return out, nil
}

// modulePath reads the module directive of a go.mod file.
func modulePath(gomod string) (string, error) {
	f, err := os.Open(gomod)
	if err != nil {
		return "", err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if rest, ok := strings.CutPrefix(line, "module "); ok {
			return strings.Trim(strings.TrimSpace(rest), `"`), nil
		}
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("%s: no module directive", gomod)
}

var blockRE = regexp.MustCompile(`^(.+):(\d+)\.(\d+),(\d+)\.(\d+) (\d+) (\d+)$`)

// parseProfile reads a cover profile and merges duplicate blocks by summing
// their counts.
func parseProfile(r io.Reader) (mode string, blocks []block, err error) {
	index := map[string]int{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if rest, ok := strings.CutPrefix(line, "mode:"); ok {
			mode = strings.TrimSpace(rest)
			continue
		}
		m := blockRE.FindStringSubmatch(line)
		if m == nil {
			return "", nil, fmt.Errorf("profile line %d: cannot parse %q", lineNo, line)
		}
		key := m[1] + ":" + m[2] + "." + m[3] + "," + m[4] + "." + m[5]
		count := atoi(m[7])
		if i, ok := index[key]; ok {
			blocks[i].count += count
			continue
		}
		index[key] = len(blocks)
		blocks = append(blocks, block{
			file:      m[1],
			startLine: atoi(m[2]), startCol: atoi(m[3]),
			endLine: atoi(m[4]), endCol: atoi(m[5]),
			stmts: atoi(m[6]), count: count,
		})
	}
	if err := sc.Err(); err != nil {
		return "", nil, err
	}
	if mode == "" {
		return "", nil, errors.New("profile has no mode line")
	}
	return mode, blocks, nil
}

// atoi converts a string the regexp already validated as digits.
func atoi(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

var markerRE = regexp.MustCompile(`//\s*covergate:ignore\b(.*)$`)

// scanMarkers reads every covergate:ignore marker of a source file, keyed
// by line number. A marker without a reason is an error.
func scanMarkers(path string) (map[int]*marker, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	markers := map[int]*marker{}
	var errs []error
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	line := 0
	for sc.Scan() {
		line++
		m := markerRE.FindStringSubmatch(sc.Text())
		if m == nil {
			continue
		}
		reason := strings.TrimSpace(m[1])
		if reason == "" {
			errs = append(errs, fmt.Errorf("%s:%d: covergate:ignore needs a reason", path, line))
			continue
		}
		markers[line] = &marker{line: line, reason: reason}
	}
	if err := sc.Err(); err != nil {
		errs = append(errs, err)
	}
	return markers, errors.Join(errs...)
}

// gate computes the report for cfg.
func gate(cfg config) (report, error) {
	f, err := os.Open(cfg.profile)
	if err != nil {
		return report{}, err
	}
	defer f.Close()
	mode, blocks, err := parseProfile(f)
	if err != nil {
		return report{}, fmt.Errorf("%s: %w", cfg.profile, err)
	}
	rep := report{profile: cfg.profile, mode: mode}

	pkgs := map[string]*pkgResult{}
	markersByFile := map[string]map[int]*marker{}
	ignoredByKey := map[string]*ignoredLine{}
	unresolved := map[string]bool{}
	var errs []error

	for _, b := range blocks {
		rel, inModule := strings.CutPrefix(b.file, cfg.module+"/")
		pkg := path.Dir(b.file)
		if inModule {
			pkg = path.Dir(rel)
		}
		pr := pkgs[pkg]
		if pr == nil {
			pr = &pkgResult{pkg: pkg, threshold: thresholdFor(cfg, pkg)}
			pkgs[pkg] = pr
		}
		if !inModule {
			if !unresolved[b.file] {
				unresolved[b.file] = true
				rep.warnings = append(rep.warnings, fmt.Sprintf("%s: outside module %s, ignore markers not applied", b.file, cfg.module))
			}
		} else {
			markers, ok := markersByFile[rel]
			if !ok {
				markers, err = scanMarkers(filepath.Join(cfg.root, filepath.FromSlash(rel)))
				if err != nil {
					errs = append(errs, err)
				}
				markersByFile[rel] = markers
			}
			if m := matchMarker(markers, b.startLine); m != nil {
				m.used = true
				key := fmt.Sprintf("%s:%d", rel, m.line)
				il := ignoredByKey[key]
				if il == nil {
					il = &ignoredLine{file: rel, line: m.line, reason: m.reason}
					ignoredByKey[key] = il
				}
				il.stmts += b.stmts
				pr.ignored += b.stmts
				continue
			}
		}
		pr.stmts += b.stmts
		if b.count > 0 {
			pr.covered += b.stmts
		}
	}
	if err := errors.Join(errs...); err != nil {
		return report{}, err
	}

	for _, p := range pkgs {
		rep.pkgs = append(rep.pkgs, *p)
	}
	sort.Slice(rep.pkgs, func(i, j int) bool { return rep.pkgs[i].pkg < rep.pkgs[j].pkg })

	rep.ignored = rep.ignored[:0]
	for _, il := range ignoredByKey {
		rep.ignored = append(rep.ignored, *il)
	}
	sort.Slice(rep.ignored, func(i, j int) bool {
		if rep.ignored[i].file != rep.ignored[j].file {
			return rep.ignored[i].file < rep.ignored[j].file
		}
		return rep.ignored[i].line < rep.ignored[j].line
	})

	files := make([]string, 0, len(markersByFile))
	for f := range markersByFile {
		files = append(files, f)
	}
	sort.Strings(files)
	for _, f := range files {
		lines := make([]int, 0, len(markersByFile[f]))
		for l := range markersByFile[f] {
			lines = append(lines, l)
		}
		sort.Ints(lines)
		for _, l := range lines {
			if m := markersByFile[f][l]; !m.used {
				rep.warnings = append(rep.warnings, fmt.Sprintf("%s:%d: covergate:ignore matches no coverage block (%s)", f, l, m.reason))
			}
		}
	}
	return rep, nil
}

// matchMarker returns the marker on a block's first line or the line above.
func matchMarker(markers map[int]*marker, startLine int) *marker {
	if m := markers[startLine]; m != nil {
		return m
	}
	return markers[startLine-1]
}

// thresholdFor resolves the threshold of a package: explicit override, then
// the core packages, then the default.
func thresholdFor(cfg config, pkg string) float64 {
	if v, ok := cfg.thresholds[pkg]; ok {
		return v
	}
	if v, ok := coreThresholds[pkg]; ok {
		return v
	}
	return cfg.defaultThreshold
}

// printReport writes the human-readable table.
func printReport(w io.Writer, rep report) {
	fmt.Fprintf(w, "covergate: %s (mode %s)\n\n", rep.profile, rep.mode)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "PACKAGE\tSTMTS\tCOVERED\tIGNORED\tCOVERAGE\tTHRESHOLD\tSTATUS")
	for _, p := range rep.pkgs {
		if p.skipped() {
			fmt.Fprintf(tw, "%s\t0\t0\t%d\t-\t-\tskipped (no statements)\n", p.pkg, p.ignored)
			continue
		}
		status := "ok"
		if !p.ok() {
			status = "FAIL"
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%.1f%%\t%.1f%%\t%s\n", p.pkg, p.stmts, p.covered, p.ignored, p.percent(), p.threshold, status)
	}
	_ = tw.Flush()
	if len(rep.ignored) > 0 {
		fmt.Fprintf(w, "\nignored lines (%d):\n", len(rep.ignored))
		for _, il := range rep.ignored {
			fmt.Fprintf(w, "  %s:%d  %d stmt(s)  %s\n", il.file, il.line, il.stmts, il.reason)
		}
	}
	if len(rep.warnings) > 0 {
		fmt.Fprintf(w, "\nwarnings (%d):\n", len(rep.warnings))
		for _, s := range rep.warnings {
			fmt.Fprintf(w, "  %s\n", s)
		}
	}
	if n := rep.failed(); n > 0 {
		fmt.Fprintf(w, "\ncovergate: %d of %d package(s) below threshold\n", n, len(rep.pkgs))
	} else {
		fmt.Fprintf(w, "\ncovergate: all %d package(s) meet their thresholds\n", len(rep.pkgs))
	}
}

// markdown renders the summary for CI.
func markdown(rep report) string {
	var b strings.Builder
	fmt.Fprintf(&b, "## Coverage gate\n\nProfile `%s` (mode %s).\n\n", rep.profile, rep.mode)
	b.WriteString("| Package | Statements | Covered | Ignored | Coverage | Threshold | Status |\n")
	b.WriteString("|---|---:|---:|---:|---:|---:|---|\n")
	for _, p := range rep.pkgs {
		if p.skipped() {
			fmt.Fprintf(&b, "| `%s` | 0 | 0 | %d | - | - | skipped |\n", p.pkg, p.ignored)
			continue
		}
		status := "ok"
		if !p.ok() {
			status = "**FAIL**"
		}
		fmt.Fprintf(&b, "| `%s` | %d | %d | %d | %.1f%% | %.1f%% | %s |\n", p.pkg, p.stmts, p.covered, p.ignored, p.percent(), p.threshold, status)
	}
	b.WriteString("\n### Ignored lines\n\n")
	if len(rep.ignored) == 0 {
		b.WriteString("None.\n")
	} else {
		b.WriteString("| Location | Statements | Reason |\n|---|---:|---|\n")
		for _, il := range rep.ignored {
			fmt.Fprintf(&b, "| `%s:%d` | %d | %s |\n", il.file, il.line, il.stmts, il.reason)
		}
	}
	if len(rep.warnings) > 0 {
		b.WriteString("\n### Warnings\n\n")
		for _, s := range rep.warnings {
			fmt.Fprintf(&b, "- %s\n", s)
		}
	}
	if n := rep.failed(); n > 0 {
		fmt.Fprintf(&b, "\n**Result:** %d of %d package(s) below threshold.\n", n, len(rep.pkgs))
	} else {
		fmt.Fprintf(&b, "\n**Result:** all %d package(s) meet their thresholds.\n", len(rep.pkgs))
	}
	return b.String()
}
