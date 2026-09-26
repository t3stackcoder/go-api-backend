package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// App holds the dependencies of every task. Tests construct it with a fake
// Runner, an in-memory stdout, and a temporary module root.
type App struct {
	// Runner executes external commands.
	Runner Runner
	// Stdout and Stderr receive the tool's own messages.
	Stdout io.Writer
	Stderr io.Writer
	// Root is the module root; every command runs with it as working directory.
	Root string
	// TempDir is where scratch files are written (openapi-check).
	TempDir string
	// Getenv reads environment variables; nil means os.Getenv.
	Getenv func(string) string
}

// taskFunc implements one task. args are the arguments after the task name.
type taskFunc func(a *App, args []string) error

// task is one named entry of the registry.
type task struct {
	name    string
	summary string
	fn      taskFunc
}

// tasks is the registry, in the order shown by the usage text. It is filled
// in init because help and ci refer back to it.
var tasks []task

func init() {
	tasks = []task{
		{"up", "start Postgres and Redis (docker compose, no profiles)", taskUp},
		{"down", "stop the compose stack and delete its volumes", taskDown},
		{"chaos-up", "start the compose stack with the chaos profile (Toxiproxy and nodes)", taskChaosUp},
		{"chaos-down", "stop the chaos profile stack and delete its volumes", taskChaosDown},
		{"orders-up", "start Postgres, Redis, and the example service (compose profile orders)", taskOrdersUp},
		{"orders-down", "stop the orders profile stack and delete its volumes", taskOrdersDown},
		{"fmt", "gofmt -l; fails when any file is not formatted", taskFmt},
		{"lint", "go vet plus staticcheck, govulncheck, golangci-lint when installed", taskLint},
		{"test", "tiers 0 to 2: go test -shuffle=on -count=2 ./... (-race when cgo works)", taskTest},
		{"fuzz", "run every Fuzz target for -fuzztime (default 30s)", taskFuzz},
		{"test-integration", "tier 4: go test -tags integration ./...", taskTestIntegration},
		{"test-sweep", "tier 3: go test -tags faultsweep,faultinject ./test/faultsweep/...", taskTestSweep},
		{"chaos", "one chaos run: -workload -seed -duration (env WORKLOAD, SEED, DURATION)", taskChaos},
		{"chaos-matrix", "every workload x seeds 1,2,3 (env CHAOS_DURATION, CHAOS_WORKLOADS, CHAOS_SEEDS)", taskChaosMatrix},
		{"openapi", "regenerate api/openapi.json with mediatorctl", taskOpenAPI},
		{"openapi-check", "fail when api/openapi.json differs from a fresh export", taskOpenAPICheck},
		{"cover", "coverage profile of ./mediator/... and the covergate thresholds", taskCover},
		{"mutate", "gremlins mutation testing on ./mediator (skipped when not installed)", taskMutate},
		{"bench", "benchmarks to coverage/bench.txt, benchstat gate against the baseline", taskBench},
		{"bench-baseline", "copy coverage/bench.txt to coverage/bench-baseline.txt", taskBenchBaseline},
		{"ci", "fmt, lint, test, cover in sequence", taskCI},
		{"help", "print this list", taskHelp},
	}
}

// lookup returns the task registered under name.
func lookup(name string) (task, bool) {
	for _, t := range tasks {
		if t.name == name {
			return t, true
		}
	}
	return task{}, false
}

// Run dispatches args (the task name followed by its arguments) and returns
// the process exit code: 0 on success, 1 when the task failed, 2 on usage
// errors.
func (a *App) Run(args []string) int {
	if len(args) == 0 {
		_ = taskHelp(a, nil)
		return 2
	}
	t, ok := lookup(args[0])
	if !ok {
		a.errorf("unknown task %q\n\n", args[0])
		_ = taskHelp(a, nil)
		return 2
	}
	if err := t.fn(a, args[1:]); err != nil {
		var ue usageError
		if errors.As(err, &ue) {
			a.errorf("%s: %v\n", t.name, err)
			return 2
		}
		a.errorf("task %s failed: %v\n", t.name, err)
		return 1
	}
	return 0
}

// usageError marks an argument problem; Run maps it to exit code 2.
type usageError struct{ err error }

func (u usageError) Error() string { return u.err.Error() }
func (u usageError) Unwrap() error { return u.err }

func (a *App) printf(format string, args ...any) {
	fmt.Fprintf(a.Stdout, format, args...)
}

func (a *App) errorf(format string, args ...any) {
	fmt.Fprintf(a.Stderr, format, args...)
}

// step announces a command before it runs so logs show exactly what ran.
func (a *App) step(c Cmd) {
	a.printf("==> %s\n", c.String())
}

// getenv reads an environment variable through the injected Getenv.
func (a *App) getenv(key string) string {
	if a.Getenv != nil {
		return a.Getenv(key)
	}
	return os.Getenv(key)
}

// envOr returns the environment variable key, or def when it is empty.
func (a *App) envOr(key, def string) string {
	if v := a.getenv(key); v != "" {
		return v
	}
	return def
}

// cmd builds a Cmd rooted at the module root.
func (a *App) cmd(name string, args ...string) Cmd {
	return Cmd{Name: name, Args: args, Dir: a.Root}
}

// run announces and runs a command.
func (a *App) run(name string, args ...string) error {
	return a.runCmd(a.cmd(name, args...))
}

// runCmd announces and runs a prepared command.
func (a *App) runCmd(c Cmd) error {
	a.step(c)
	if err := a.Runner.Run(c); err != nil {
		return fmt.Errorf("%s: %w", c.Name, err)
	}
	return nil
}

// goRun runs the go command with args.
func (a *App) goRun(args ...string) error {
	return a.run("go", args...)
}

// requireTools reports whether missing optional tools must fail the task.
// CI sets TASK_REQUIRE_TOOLS=1 after installing them.
func (a *App) requireTools() bool {
	return a.getenv("TASK_REQUIRE_TOOLS") != ""
}

// optional runs fn when tool is on PATH. When it is not, it prints a clear
// "skipped" line (or fails when TASK_REQUIRE_TOOLS is set).
func (a *App) optional(tool, install string, fn func() error) error {
	if _, err := a.Runner.LookPath(tool); err != nil {
		return a.skip(tool, "not installed", install)
	}
	return fn()
}

// optionalAnalyzer is optional for tools that type-check the module
// (staticcheck, govulncheck, golangci-lint). Such a tool refuses a module
// whose go directive is newer than the Go it was built with, so a binary
// built with an older Go is reported as skipped with the reinstall command
// instead of failing with a confusing load error.
func (a *App) optionalAnalyzer(tool, install string, fn func() error) error {
	path, err := a.Runner.LookPath(tool)
	if err != nil {
		return a.skip(tool, "not installed", install)
	}
	if why := a.builtWithOlderGo(path); why != "" {
		return a.skip(tool, why, install)
	}
	return fn()
}

// builtWithOlderGo reports, as a reason string, when the Go binary at path
// was built with a Go older than the go directive of go.mod. It uses
// `go version -m`, whose first line is "<path>: go1.X.Y". It returns ""
// when the binary is recent enough or its version cannot be determined.
func (a *App) builtWithOlderGo(path string) string {
	out, err := a.Runner.Output(a.cmd("go", "version", "-m", path))
	if err != nil {
		return ""
	}
	first, _, _ := strings.Cut(string(out), "\n")
	i := strings.LastIndex(first, ": ")
	if i < 0 {
		return ""
	}
	bmaj, bmin, ok := parseGoVersion(first[i+2:])
	mmaj, mmin, merr := a.moduleGoVersion()
	if !ok || merr != nil {
		return ""
	}
	if bmaj < mmaj || (bmaj == mmaj && bmin < mmin) {
		return fmt.Sprintf("was built with go%d.%d but go.mod targets go %d.%d; reinstall it", bmaj, bmin, mmaj, mmin)
	}
	return ""
}

// skip reports an optional tool as skipped, or fails when tools are required.
func (a *App) skip(tool, why, install string) error {
	if a.requireTools() {
		return fmt.Errorf("%s: %s and TASK_REQUIRE_TOOLS is set; install with: %s", tool, why, install)
	}
	a.printf("skipped: %s %s (install with: %s)\n", tool, why, install)
	return nil
}

// abs joins rel to the module root.
func (a *App) abs(rel string) string {
	return filepath.Join(a.Root, filepath.FromSlash(rel))
}

// mkdir creates a directory under the module root.
func (a *App) mkdir(rel string) error {
	return os.MkdirAll(a.abs(rel), 0o750)
}

// moduleGoVersion reads the "go X.Y" directive from go.mod.
func (a *App) moduleGoVersion() (major, minor int, err error) {
	f, err := os.Open(a.abs("go.mod"))
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if rest, ok := strings.CutPrefix(line, "go "); ok {
			if maj, min, ok := parseGoVersion(strings.TrimSpace(rest)); ok {
				return maj, min, nil
			}
		}
	}
	if err := sc.Err(); err != nil {
		return 0, 0, err
	}
	return 0, 0, errors.New("go.mod: no go directive found")
}

var goVersionRE = regexp.MustCompile(`(?:^|go)(\d+)\.(\d+)`)

// parseGoVersion extracts major and minor from strings such as "1.27",
// "go1.26.5", or "built with go1.26.5 from ...".
func parseGoVersion(s string) (major, minor int, ok bool) {
	m := goVersionRE.FindStringSubmatch(s)
	if m == nil {
		return 0, 0, false
	}
	major, err1 := strconv.Atoi(m[1])
	minor, err2 := strconv.Atoi(m[2])
	if err1 != nil || err2 != nil {
		return 0, 0, false
	}
	return major, minor, true
}

// findRoot walks up from dir until it finds go.mod. It returns dir when no
// module root exists above it.
func findRoot(dir string) string {
	d := dir
	for {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			return dir
		}
		d = parent
	}
}

// copyFile copies src to dst, creating or truncating dst.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}
