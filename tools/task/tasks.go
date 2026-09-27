package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Paths used by several tasks, relative to the module root.
const (
	composeFile   = "deploy/docker-compose.yml"
	openAPIFile   = "api/openapi.json"
	coverDir      = "coverage"
	coverProfile  = "coverage/unit.out"
	coverSummary  = "coverage/summary.md"
	benchFile     = "coverage/bench.txt"
	benchBaseline = "coverage/bench-baseline.txt"
)

// Install commands for the optional tools.
const (
	installStaticcheck = "go install honnef.co/go/tools/cmd/staticcheck@latest"
	installGovulncheck = "go install golang.org/x/vuln/cmd/govulncheck@latest"
	installGolangci    = "go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest"
	installGremlins    = "go install github.com/go-gremlins/gremlins/cmd/gremlins@latest"
	installBenchstat   = "go install golang.org/x/perf/cmd/benchstat@latest"
)

// chaosWorkloads is the nightly matrix of spec 11.6.
var chaosWorkloads = []string{
	"register", "register-cached", "bank", "bank-idempotent",
	"idempotent-append", "events", "cache-staleness", "remote-send",
}

// compose runs docker compose against deploy/docker-compose.yml.
func compose(a *App, args ...string) error {
	return a.run("docker", append([]string{"compose", "-f", composeFile}, args...)...)
}

func taskUp(a *App, _ []string) error   { return compose(a, "up", "-d", "--wait") }
func taskDown(a *App, _ []string) error { return compose(a, "down", "-v") }

func taskChaosUp(a *App, _ []string) error {
	return compose(a, "--profile", "chaos", "up", "-d", "--wait", "--build")
}

func taskChaosDown(a *App, _ []string) error {
	return compose(a, "--profile", "chaos", "down", "-v")
}

// taskOrdersUp and taskOrdersDown run the compose stack with the example
// service of spec 13 (profile `orders`, Dockerfile target `orders`).
func taskOrdersUp(a *App, _ []string) error {
	return compose(a, "--profile", "orders", "up", "-d", "--wait", "--build")
}

func taskOrdersDown(a *App, _ []string) error {
	return compose(a, "--profile", "orders", "down", "-v")
}

// taskFmt lists unformatted files with gofmt -l and fails when there are any.
func taskFmt(a *App, _ []string) error {
	c := a.cmd("gofmt", "-l", ".")
	a.step(c)
	out, err := a.Runner.Output(c)
	if err != nil {
		return fmt.Errorf("gofmt: %w", err)
	}
	files := nonEmptyLines(string(out))
	if len(files) > 0 {
		return fmt.Errorf("gofmt: %d file(s) need formatting (fix with: gofmt -w %s):\n  %s",
			len(files), strings.Join(files, " "), strings.Join(files, "\n  "))
	}
	a.printf("gofmt: all files formatted\n")
	return nil
}

// taskLint runs go vet and, when installed, staticcheck, govulncheck, and
// golangci-lint. Missing tools, and tools built with a Go older than the
// module's go directive, are reported as skipped unless TASK_REQUIRE_TOOLS
// is set.
func taskLint(a *App, _ []string) error {
	if err := a.goRun("vet", "./..."); err != nil {
		return err
	}
	if err := a.optionalAnalyzer("staticcheck", installStaticcheck, func() error {
		return a.run("staticcheck", "./...")
	}); err != nil {
		return err
	}
	if err := a.optionalAnalyzer("govulncheck", installGovulncheck, func() error {
		return a.run("govulncheck", "./...")
	}); err != nil {
		return err
	}
	return a.optionalAnalyzer("golangci-lint", installGolangci, func() error {
		return a.run("golangci-lint", "run")
	})
}

// taskTest runs tiers 0 to 2: go test with shuffle and count=2, adding -race
// only when cgo is enabled and the configured C compiler is on PATH.
func taskTest(a *App, _ []string) error {
	race, why := a.raceSupported()
	args := []string{"test"}
	mode := "race detector off"
	if race {
		args = append(args, "-race")
		mode = "race detector on"
	}
	args = append(args, "-shuffle=on", "-count=2", "./...")
	a.printf("test mode: %s (%s)\n", mode, why)
	return a.goRun(args...)
}

// raceSupported reports whether go test -race can work here: CGO_ENABLED=1
// and the compiler named by `go env CC` resolves on PATH.
func (a *App) raceSupported() (bool, string) {
	out, err := a.Runner.Output(a.cmd("go", "env", "CGO_ENABLED", "CC"))
	if err != nil {
		return false, "go env failed: " + err.Error()
	}
	lines := nonEmptyLines(string(out))
	if len(lines) < 2 {
		return false, "go env returned unexpected output"
	}
	cgo, cc := lines[0], lines[1]
	if cgo != "1" {
		return false, "CGO_ENABLED=" + cgo
	}
	if _, err := a.Runner.LookPath(cc); err != nil {
		return false, fmt.Sprintf("CGO_ENABLED=1 but C compiler %q is not on PATH", cc)
	}
	return true, "CGO_ENABLED=1, CC=" + cc
}

// taskFuzz discovers every Fuzz function in the default build and runs each
// one for -fuzztime. All targets run even when one fails, then the failures
// are reported together.
func taskFuzz(a *App, args []string) error {
	fs := flag.NewFlagSet("fuzz", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	fuzztime := fs.String("fuzztime", "30s", "time to fuzz each target (go test -fuzztime)")
	if err := fs.Parse(args); err != nil {
		return usageError{err}
	}
	targets, err := discoverFuzzTargets(a.Root)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		a.printf("fuzz: no Fuzz targets found\n")
		return nil
	}
	a.printf("fuzz: %d target(s), %s each\n", len(targets), *fuzztime)
	var failed []string
	for _, t := range targets {
		err := a.goRun("test", "-run", "^$", "-fuzz", "^"+t.Name+"$", "-fuzztime", *fuzztime, t.Pkg)
		if err != nil {
			failed = append(failed, t.Pkg+":"+t.Name)
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("%d fuzz target(s) failed: %s", len(failed), strings.Join(failed, ", "))
	}
	return nil
}

func taskTestIntegration(a *App, _ []string) error {
	return a.goRun("test", "-tags", "integration", "-count=1", "./...")
}

func taskTestSweep(a *App, _ []string) error {
	return a.goRun("test", "-tags", "faultsweep,faultinject", "-count=1", "-timeout", "60m", "./test/faultsweep/...")
}

// taskChaos runs one chaos run. Flags override the WORKLOAD, SEED, and
// DURATION environment variables the Makefile passes.
func taskChaos(a *App, args []string) error {
	fs := flag.NewFlagSet("chaos", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	workload := fs.String("workload", a.envOr("WORKLOAD", "register"), "workload name (spec 11.6)")
	seed := fs.String("seed", a.envOr("SEED", "1"), "generator seed")
	duration := fs.String("duration", a.envOr("DURATION", "5m"), "mayhem duration")
	if err := fs.Parse(args); err != nil {
		return usageError{err}
	}
	if _, err := time.ParseDuration(*duration); err != nil {
		return usageError{fmt.Errorf("invalid -duration %q: %w", *duration, err)}
	}
	return a.runChaos(*workload, *seed, *duration)
}

// runChaos runs TestChaos in ./test/chaos with the workload flags passed
// through to the test binary. The go test timeout is the mayhem duration
// plus thirty minutes for warm-up, healing, quiescence, and the checkers.
func (a *App) runChaos(workload, seed, duration string) error {
	d, err := time.ParseDuration(duration)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", duration, err)
	}
	timeout := d + 30*time.Minute
	c := a.cmd("go", "test", "-tags", "chaos", "-count=1", "-run", "TestChaos",
		"-timeout", timeout.String(), "./test/chaos/",
		"-workload="+workload, "-seed="+seed, "-duration="+duration)
	c.Env = []string{"CHAOS_WORKLOAD=" + workload, "CHAOS_SEED=" + seed, "CHAOS_DURATION=" + duration}
	return a.runCmd(c)
}

// taskChaosMatrix runs every workload for every seed and reports the whole
// matrix before failing, so one bad cell does not hide the others.
func taskChaosMatrix(a *App, _ []string) error {
	duration := a.envOr("CHAOS_DURATION", "5m")
	if _, err := time.ParseDuration(duration); err != nil {
		return usageError{fmt.Errorf("invalid CHAOS_DURATION %q: %w", duration, err)}
	}
	workloads := splitList(a.envOr("CHAOS_WORKLOADS", strings.Join(chaosWorkloads, ",")))
	seeds := splitList(a.envOr("CHAOS_SEEDS", "1,2,3"))
	type cell struct {
		workload, seed string
		err            error
	}
	var cells []cell
	for _, w := range workloads {
		for _, s := range seeds {
			a.printf("--- chaos workload=%s seed=%s duration=%s\n", w, s, duration)
			cells = append(cells, cell{w, s, a.runChaos(w, s, duration)})
		}
	}
	a.printf("\nchaos matrix (%s each)\n", duration)
	failed := 0
	for _, c := range cells {
		status := "ok"
		if c.err != nil {
			status = "FAIL: " + c.err.Error()
			failed++
		}
		a.printf("  %-18s seed %-3s %s\n", c.workload, c.seed, status)
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d chaos runs failed", failed, len(cells))
	}
	return nil
}

// taskOpenAPI regenerates the committed OpenAPI document.
func taskOpenAPI(a *App, _ []string) error {
	if err := a.mkdir("api"); err != nil {
		return err
	}
	return a.goRun("run", "./cmd/mediatorctl", "openapi", "export", "--out", openAPIFile)
}

// taskOpenAPICheck exports to a temporary file and diffs it against the
// committed document; any difference fails.
func taskOpenAPICheck(a *App, _ []string) error {
	dir := a.TempDir
	if dir == "" {
		dir = os.TempDir()
	}
	tmp := filepath.Join(dir, fmt.Sprintf("openapi-check-%d.json", os.Getpid()))
	defer os.Remove(tmp)
	if err := a.goRun("run", "./cmd/mediatorctl", "openapi", "export", "--out", tmp); err != nil {
		return err
	}
	if err := a.run("git", "diff", "--no-index", "--exit-code", openAPIFile, tmp); err != nil {
		return fmt.Errorf("%s is out of date: run `go run ./tools/task openapi` and commit the result", openAPIFile)
	}
	a.printf("openapi: %s is up to date\n", openAPIFile)
	return nil
}

// coverThresholds are the packages the gate holds below the 95 percent
// default (design-notes 8.10 and 8.11):
//   - mediator/pg/storetest=80: a conformance suite whose failure branches
//     run only when a store does not conform.
//
// An explicit -thresholds argument replaces them.
const coverThresholds = "mediator/pg/storetest=80"

// taskCover writes the atomic coverage profile of ./mediator/... and runs
// the covergate; extra arguments are passed to covergate (for example
// -thresholds mediator/pg=90, which replaces coverThresholds). The profile
// includes the integration and fault-injection tests (Docker), because the
// pg and redisx drivers reach their thresholds only through the tests that
// talk to Postgres and Redis.
func taskCover(a *App, args []string) error {
	if err := a.mkdir(coverDir); err != nil {
		return err
	}
	if err := a.goRun("test", "-tags", "integration,faultinject", "-covermode=atomic", "-coverpkg=./mediator/...",
		"-coverprofile="+coverProfile, "./mediator/..."); err != nil {
		return err
	}
	gate := []string{"run", "./tools/covergate", "-profile", coverProfile, "-summary", coverSummary}
	if !slices.ContainsFunc(args, func(s string) bool { return s == "-thresholds" || strings.HasPrefix(s, "-thresholds=") }) {
		gate = append(gate, "-thresholds", coverThresholds)
	}
	return a.goRun(append(gate, args...)...)
}

// taskMutate runs gremlins on the core packages. gremlins takes a directory,
// not a package pattern, so ./mediator covers everything below it. The test
// cache is cleared first: gremlins sizes every mutant's test timeout from the
// wall time of its coverage run, and a run served from the cache in a couple
// of seconds leaves the real test runs no time, so most mutants time out and
// the efficacy gate passes on the few that ran (design-notes 8.13).
func taskMutate(a *App, _ []string) error {
	return a.optional("gremlins", installGremlins, func() error {
		if err := a.goRun("clean", "-testcache"); err != nil {
			return err
		}
		return a.run("gremlins", "unleash", "./mediator", "--threshold-efficacy", "80")
	})
}

// taskBench runs the benchmarks into coverage/bench.txt and, when benchstat
// and a baseline are present, fails on a median sec/op regression above
// -threshold percent (default 10).
func taskBench(a *App, args []string) error {
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.SetOutput(a.Stderr)
	threshold := fs.Float64("threshold", 10, "fail when a sec/op median regresses by more than this percent")
	count := fs.Int("count", 6, "benchmark repetitions (go test -count)")
	if err := fs.Parse(args); err != nil {
		return usageError{err}
	}
	if err := a.mkdir(coverDir); err != nil {
		return err
	}
	f, err := os.Create(a.abs(benchFile))
	if err != nil {
		return err
	}
	c := a.cmd("go", "test", "-run", "xxx", "-bench", ".", "-benchmem", "-count", strconv.Itoa(*count), "./mediator/...")
	c.Stdout = io.MultiWriter(a.Stdout, f)
	err = a.runCmd(c)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	a.printf("bench: results written to %s\n", benchFile)
	if _, err := os.Stat(a.abs(benchBaseline)); err != nil {
		a.printf("bench: no baseline at %s; run `go run ./tools/task bench-baseline` to record one\n", benchBaseline)
		return nil
	}
	return a.optional("benchstat", installBenchstat, func() error { return a.benchGate(*threshold) })
}

// benchGate prints the human benchstat comparison, then parses the CSV form
// of the same comparison (see parseBenchstatCSV) and fails when any sec/op
// median regressed by more than threshold percent with p below alpha.
func (a *App) benchGate(threshold float64) error {
	if err := a.run("benchstat", "-alpha", "0.05", benchBaseline, benchFile); err != nil {
		return err
	}
	c := a.cmd("benchstat", "-alpha", "0.05", "-format", "csv", benchBaseline, benchFile)
	out, err := a.Runner.Output(c)
	if err != nil {
		return fmt.Errorf("benchstat: %w", err)
	}
	deltas, err := parseBenchstatCSV(string(out))
	if err != nil {
		return err
	}
	bad := regressions(deltas, "sec/op", threshold)
	if len(bad) == 0 {
		a.printf("bench: no sec/op regression above %.0f%% (%d benchmark(s) compared)\n", threshold, countMetric(deltas, "sec/op"))
		return nil
	}
	var b strings.Builder
	for _, r := range bad {
		fmt.Fprintf(&b, "\n  %s %s %+.2f%% (%s)", r.Name, r.Metric, r.Percent, r.P)
	}
	return fmt.Errorf("bench: %d benchmark(s) regressed more than %.0f%% on the median:%s", len(bad), threshold, b.String())
}

// taskBenchBaseline stores the last bench results as the baseline.
func taskBenchBaseline(a *App, _ []string) error {
	if _, err := os.Stat(a.abs(benchFile)); err != nil {
		return fmt.Errorf("%s not found: run `go run ./tools/task bench` first", benchFile)
	}
	if err := copyFile(a.abs(benchFile), a.abs(benchBaseline)); err != nil {
		return err
	}
	a.printf("bench: baseline written to %s\n", benchBaseline)
	return nil
}

// taskCI runs the push-time gates in order.
func taskCI(a *App, _ []string) error {
	for _, name := range []string{"fmt", "lint", "test", "cover"} {
		t, _ := lookup(name)
		a.printf("\n### %s\n", name)
		if err := t.fn(a, nil); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

// taskHelp prints the task list.
func taskHelp(a *App, _ []string) error {
	a.printf("usage: go run ./tools/task <task> [args]\n\ntasks:\n")
	for _, t := range tasks {
		a.printf("  %-17s %s\n", t.name, t.summary)
	}
	a.printf("\nenvironment: TASK_REQUIRE_TOOLS=1 fails instead of skipping missing optional tools.\n")
	return nil
}

// nonEmptyLines splits s into trimmed, non-empty lines.
func nonEmptyLines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if l = strings.TrimSpace(l); l != "" {
			out = append(out, l)
		}
	}
	return out
}

// splitList splits a comma-separated list, trimming and dropping empties.
func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
