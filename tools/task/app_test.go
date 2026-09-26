package main

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeRunner records every command and answers from canned tables keyed by
// Cmd.String().
type fakeRunner struct {
	cmds    []Cmd
	outputs map[string]string // Output results
	fails   map[string]error  // Run and Output failures
	tools   map[string]bool   // LookPath answers
	// stdoutText is written to Cmd.Stdout when a Run command sets it.
	stdoutText string
}

func (f *fakeRunner) Run(c Cmd) error {
	f.cmds = append(f.cmds, c)
	if c.Stdout != nil && f.stdoutText != "" {
		_, _ = io.WriteString(c.Stdout, f.stdoutText)
	}
	return f.fails[c.String()]
}

func (f *fakeRunner) Output(c Cmd) ([]byte, error) {
	f.cmds = append(f.cmds, c)
	if err := f.fails[c.String()]; err != nil {
		return nil, err
	}
	return []byte(f.outputs[c.String()]), nil
}

func (f *fakeRunner) LookPath(name string) (string, error) {
	if f.tools[name] {
		return "/usr/bin/" + name, nil
	}
	return "", errors.New("not found")
}

// strings renders the recorded commands.
func (f *fakeRunner) strings() []string {
	out := make([]string, len(f.cmds))
	for i, c := range f.cmds {
		out[i] = c.String()
	}
	return out
}

type harness struct {
	app    *App
	runner *fakeRunner
	stdout *bytes.Buffer
	stderr *bytes.Buffer
	env    map[string]string
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/m\n\ngo 1.27\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := &harness{
		runner: &fakeRunner{outputs: map[string]string{}, fails: map[string]error{}, tools: map[string]bool{}},
		stdout: &bytes.Buffer{},
		stderr: &bytes.Buffer{},
		env:    map[string]string{},
	}
	h.app = &App{
		Runner:  h.runner,
		Stdout:  h.stdout,
		Stderr:  h.stderr,
		Root:    root,
		TempDir: t.TempDir(),
		Getenv:  func(k string) string { return h.env[k] },
	}
	return h
}

func (h *harness) run(t *testing.T, args ...string) int {
	t.Helper()
	return h.app.Run(args)
}

func assertCmds(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("commands:\n got %q\nwant %q", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("command %d:\n got %q\nwant %q", i, got[i], want[i])
		}
	}
}

func assertRoot(t *testing.T, h *harness) {
	t.Helper()
	for _, c := range h.runner.cmds {
		if c.Dir != h.app.Root {
			t.Errorf("%s: Dir = %q, want module root %q", c, c.Dir, h.app.Root)
		}
	}
}

func TestRunUsage(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t); code != 2 {
		t.Errorf("no args: exit %d, want 2", code)
	}
	if !strings.Contains(h.stdout.String(), "usage: go run ./tools/task") {
		t.Errorf("usage not printed: %s", h.stdout)
	}
	h = newHarness(t)
	if code := h.run(t, "bogus"); code != 2 {
		t.Errorf("unknown task: exit %d, want 2", code)
	}
	if !strings.Contains(h.stderr.String(), `unknown task "bogus"`) {
		t.Errorf("stderr = %q", h.stderr)
	}
	h = newHarness(t)
	if code := h.run(t, "help"); code != 0 {
		t.Errorf("help: exit %d, want 0", code)
	}
	for _, task := range tasks {
		if !strings.Contains(h.stdout.String(), task.name) {
			t.Errorf("help does not list %s", task.name)
		}
	}
}

func TestComposeTasks(t *testing.T) {
	cases := map[string]string{
		"up":         "docker compose -f deploy/docker-compose.yml up -d --wait",
		"down":       "docker compose -f deploy/docker-compose.yml down -v",
		"chaos-up":   "docker compose -f deploy/docker-compose.yml --profile chaos up -d --wait --build",
		"chaos-down": "docker compose -f deploy/docker-compose.yml --profile chaos down -v",
	}
	for task, want := range cases {
		h := newHarness(t)
		if code := h.run(t, task); code != 0 {
			t.Errorf("%s: exit %d, stderr %s", task, code, h.stderr)
		}
		assertCmds(t, h.runner.strings(), []string{want})
		assertRoot(t, h)
	}
	h := newHarness(t)
	h.runner.fails["docker compose -f deploy/docker-compose.yml up -d --wait"] = errors.New("exit status 1")
	if code := h.run(t, "up"); code != 1 {
		t.Errorf("failing up: exit %d, want 1", code)
	}
	if !strings.Contains(h.stderr.String(), "task up failed: docker: exit status 1") {
		t.Errorf("stderr = %q", h.stderr)
	}
}

func TestFmt(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t, "fmt"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{"gofmt -l ."})
	if !strings.Contains(h.stdout.String(), "all files formatted") {
		t.Errorf("stdout = %q", h.stdout)
	}

	h = newHarness(t)
	h.runner.outputs["gofmt -l ."] = "a.go\n\nsub/b.go\n"
	if code := h.run(t, "fmt"); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(h.stderr.String(), "2 file(s) need formatting (fix with: gofmt -w a.go sub/b.go)") {
		t.Errorf("stderr = %q", h.stderr)
	}

	h = newHarness(t)
	h.runner.fails["gofmt -l ."] = errors.New("boom")
	if code := h.run(t, "fmt"); code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
}

func TestLintSkipsMissingTools(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t, "lint"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{"go vet ./..."})
	for _, tool := range []string{"staticcheck", "govulncheck", "golangci-lint"} {
		if !strings.Contains(h.stdout.String(), "skipped: "+tool+" not installed") {
			t.Errorf("no skip line for %s in %q", tool, h.stdout)
		}
	}
}

func TestLintRunsInstalledTools(t *testing.T) {
	h := newHarness(t)
	for _, tool := range []string{"staticcheck", "govulncheck", "golangci-lint"} {
		h.runner.tools[tool] = true
		h.runner.outputs["go version -m /usr/bin/"+tool] = "/usr/bin/" + tool + ": go1.27.1\n\tpath\tx\n"
	}
	if code := h.run(t, "lint"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{
		"go vet ./...",
		"go version -m /usr/bin/staticcheck",
		"staticcheck ./...",
		"go version -m /usr/bin/govulncheck",
		"govulncheck ./...",
		"go version -m /usr/bin/golangci-lint",
		"golangci-lint run",
	})
}

func TestLintSkipsToolsBuiltWithOlderGo(t *testing.T) {
	h := newHarness(t)
	h.runner.tools["govulncheck"] = true
	h.runner.outputs["go version -m /usr/bin/govulncheck"] = "/usr/bin/govulncheck: go1.26.5\n\tpath\tgolang.org/x/vuln/cmd/govulncheck\n"
	if code := h.run(t, "lint"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{"go vet ./...", "go version -m /usr/bin/govulncheck"})
	if !strings.Contains(h.stdout.String(), "skipped: govulncheck was built with go1.26 but go.mod targets go 1.27; reinstall it (install with: "+installGovulncheck+")") {
		t.Errorf("stdout = %q", h.stdout)
	}

	// With TASK_REQUIRE_TOOLS the same situation fails.
	h = newHarness(t)
	h.env["TASK_REQUIRE_TOOLS"] = "1"
	for _, tool := range []string{"staticcheck", "govulncheck", "golangci-lint"} {
		h.runner.tools[tool] = true
	}
	h.runner.outputs["go version -m /usr/bin/golangci-lint"] = `C:\Users\x\go\bin\golangci-lint.exe: go1.26.5` + "\n"
	if code := h.run(t, "lint"); code != 1 {
		t.Errorf("exit %d, want 1: %s", code, h.stderr)
	}
	if !strings.Contains(h.stderr.String(), "golangci-lint: was built with go1.26 but go.mod targets go 1.27") {
		t.Errorf("stderr = %q", h.stderr)
	}

	// Output without a Go version, a failing go version -m, or an
	// unreadable go.mod do not block the run.
	for name, setup := range map[string]func(h *harness){
		"no version": func(h *harness) { h.runner.outputs["go version -m /usr/bin/golangci-lint"] = "not a go binary\n" },
		"no colon":   func(h *harness) { h.runner.outputs["go version -m /usr/bin/golangci-lint"] = "go1.26.5\n" },
		"failure":    func(h *harness) { h.runner.fails["go version -m /usr/bin/golangci-lint"] = errors.New("boom") },
		"no go.mod": func(h *harness) {
			h.runner.outputs["go version -m /usr/bin/golangci-lint"] = "/usr/bin/golangci-lint: go1.26.5\n"
			_ = os.Remove(filepath.Join(h.app.Root, "go.mod"))
		},
	} {
		h := newHarness(t)
		h.runner.tools["golangci-lint"] = true
		setup(h)
		if code := h.run(t, "lint"); code != 0 {
			t.Errorf("%s: exit %d: %s", name, code, h.stderr)
		}
		if got := h.runner.strings(); got[len(got)-1] != "golangci-lint run" {
			t.Errorf("%s: last command %q, want golangci-lint run", name, got[len(got)-1])
		}
	}
}

func TestLintRequireTools(t *testing.T) {
	h := newHarness(t)
	h.env["TASK_REQUIRE_TOOLS"] = "1"
	if code := h.run(t, "lint"); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(h.stderr.String(), "staticcheck: not installed and TASK_REQUIRE_TOOLS is set; install with: "+installStaticcheck) {
		t.Errorf("stderr = %q", h.stderr)
	}
}

func TestLintVetFailure(t *testing.T) {
	h := newHarness(t)
	h.runner.fails["go vet ./..."] = errors.New("exit status 2")
	if code := h.run(t, "lint"); code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
}

func TestTestRaceDetection(t *testing.T) {
	cases := []struct {
		name    string
		env     string
		envErr  error
		cc      bool
		want    string
		mode    string
		wantCmd string
	}{
		{name: "cgo and compiler", env: "1\ngcc\n", cc: true, mode: "race detector on (CGO_ENABLED=1, CC=gcc)",
			wantCmd: "go test -race -shuffle=on -count=2 ./..."},
		{name: "cgo off", env: "0\ngcc\n", cc: true, mode: "race detector off (CGO_ENABLED=0)",
			wantCmd: "go test -shuffle=on -count=2 ./..."},
		{name: "no compiler", env: "1\ngcc\n", cc: false, mode: `race detector off (CGO_ENABLED=1 but C compiler "gcc" is not on PATH)`,
			wantCmd: "go test -shuffle=on -count=2 ./..."},
		{name: "go env fails", envErr: errors.New("nope"), mode: "race detector off (go env failed: nope)",
			wantCmd: "go test -shuffle=on -count=2 ./..."},
		{name: "short output", env: "1\n", mode: "race detector off (go env returned unexpected output)",
			wantCmd: "go test -shuffle=on -count=2 ./..."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			h.runner.outputs["go env CGO_ENABLED CC"] = tc.env
			if tc.envErr != nil {
				h.runner.fails["go env CGO_ENABLED CC"] = tc.envErr
			}
			h.runner.tools["gcc"] = tc.cc
			if code := h.run(t, "test"); code != 0 {
				t.Fatalf("exit %d: %s", code, h.stderr)
			}
			assertCmds(t, h.runner.strings(), []string{"go env CGO_ENABLED CC", tc.wantCmd})
			if !strings.Contains(h.stdout.String(), "test mode: "+tc.mode) {
				t.Errorf("stdout = %q, want mode %q", h.stdout, tc.mode)
			}
		})
	}
}

func TestTierTasks(t *testing.T) {
	cases := map[string]string{
		"test-integration": "go test -tags integration -count=1 ./...",
		"test-sweep":       "go test -tags faultsweep,faultinject -count=1 ./test/faultsweep/...",
		"openapi":          "go run ./cmd/mediatorctl openapi export --out api/openapi.json",
		"mutate":           "gremlins unleash ./mediator --threshold-efficacy 80",
	}
	for task, want := range cases {
		h := newHarness(t)
		h.runner.tools["gremlins"] = true
		if code := h.run(t, task); code != 0 {
			t.Errorf("%s: exit %d: %s", task, code, h.stderr)
		}
		assertCmds(t, h.runner.strings(), []string{want})
	}
}

func TestMutateNotInstalled(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t, "mutate"); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if len(h.runner.cmds) != 0 {
		t.Errorf("ran %q, want nothing", h.runner.strings())
	}
	if !strings.Contains(h.stdout.String(), installGremlins) {
		t.Errorf("install instructions missing: %q", h.stdout)
	}
}

func TestFuzzTask(t *testing.T) {
	h := newHarness(t)
	writeFile(t, filepath.Join(h.app.Root, "mediator", "validate", "tag_test.go"),
		"package validate\n\nimport \"testing\"\n\nfunc FuzzTagGrammar(f *testing.F) {}\nfunc FuzzOther(f *testing.F) {}\n")
	writeFile(t, filepath.Join(h.app.Root, "mediator", "httpapi", "decode_test.go"),
		"package httpapi\n\nimport \"testing\"\n\nfunc FuzzRequestDecode(f *testing.F) {}\n")
	if code := h.run(t, "fuzz"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{
		"go test -run ^$ -fuzz ^FuzzRequestDecode$ -fuzztime 30s ./mediator/httpapi",
		"go test -run ^$ -fuzz ^FuzzOther$ -fuzztime 30s ./mediator/validate",
		"go test -run ^$ -fuzz ^FuzzTagGrammar$ -fuzztime 30s ./mediator/validate",
	})

	// -fuzztime is passed through and a failing target does not stop the others.
	h.runner.cmds = nil
	h.runner.fails["go test -run ^$ -fuzz ^FuzzOther$ -fuzztime 2m ./mediator/validate"] = errors.New("exit status 1")
	if code := h.run(t, "fuzz", "-fuzztime", "2m"); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if len(h.runner.cmds) != 3 {
		t.Errorf("ran %d commands after a failure, want 3", len(h.runner.cmds))
	}
	if !strings.Contains(h.stderr.String(), "1 fuzz target(s) failed: ./mediator/validate:FuzzOther") {
		t.Errorf("stderr = %q", h.stderr)
	}

	if code := h.run(t, "fuzz", "-bogus"); code != 2 {
		t.Errorf("bad flag: exit %d, want 2", code)
	}
}

func TestFuzzTaskNoTargets(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t, "fuzz"); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if len(h.runner.cmds) != 0 || !strings.Contains(h.stdout.String(), "no Fuzz targets") {
		t.Errorf("cmds %q stdout %q", h.runner.strings(), h.stdout)
	}
	// An unparsable test file is an error.
	writeFile(t, filepath.Join(h.app.Root, "x_test.go"), "package x\nfunc (")
	if code := h.run(t, "fuzz"); code != 1 {
		t.Errorf("parse error: exit %d, want 1", code)
	}
}

func TestChaos(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t, "chaos"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{
		"go test -tags chaos -count=1 -run TestChaos -timeout 35m0s ./test/chaos/ -workload=register -seed=1 -duration=5m",
	})
	wantEnv := []string{"CHAOS_WORKLOAD=register", "CHAOS_SEED=1", "CHAOS_DURATION=5m"}
	if got := h.runner.cmds[0].Env; strings.Join(got, " ") != strings.Join(wantEnv, " ") {
		t.Errorf("env = %q, want %q", got, wantEnv)
	}

	// Environment variables from the Makefile are honored, flags win.
	h = newHarness(t)
	h.env["WORKLOAD"] = "bank"
	h.env["SEED"] = "7"
	h.env["DURATION"] = "1m"
	if code := h.run(t, "chaos", "-seed", "9"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{
		"go test -tags chaos -count=1 -run TestChaos -timeout 31m0s ./test/chaos/ -workload=bank -seed=9 -duration=1m",
	})

	h = newHarness(t)
	if code := h.run(t, "chaos", "-duration", "soon"); code != 2 {
		t.Errorf("bad duration: exit %d, want 2", code)
	}
	if code := h.run(t, "chaos", "-nope"); code != 2 {
		t.Errorf("bad flag: exit %d, want 2", code)
	}
	h = newHarness(t)
	h.runner.fails["go test -tags chaos -count=1 -run TestChaos -timeout 35m0s ./test/chaos/ -workload=register -seed=1 -duration=5m"] = errors.New("exit status 1")
	if code := h.run(t, "chaos"); code != 1 {
		t.Errorf("failing run: exit %d, want 1", code)
	}
}

func TestChaosMatrix(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t, "chaos-matrix"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	if got := len(h.runner.cmds); got != len(chaosWorkloads)*3 {
		t.Fatalf("%d runs, want %d", got, len(chaosWorkloads)*3)
	}
	first := h.runner.cmds[0].String()
	if first != "go test -tags chaos -count=1 -run TestChaos -timeout 35m0s ./test/chaos/ -workload=register -seed=1 -duration=5m" {
		t.Errorf("first = %q", first)
	}
	last := h.runner.cmds[len(h.runner.cmds)-1].String()
	if !strings.HasSuffix(last, "-workload=remote-send -seed=3 -duration=5m") {
		t.Errorf("last = %q", last)
	}

	h = newHarness(t)
	h.env["CHAOS_DURATION"] = "20m"
	h.env["CHAOS_WORKLOADS"] = "bank, events"
	h.env["CHAOS_SEEDS"] = "1,2"
	h.runner.fails["go test -tags chaos -count=1 -run TestChaos -timeout 50m0s ./test/chaos/ -workload=events -seed=2 -duration=20m"] = errors.New("exit status 1")
	if code := h.run(t, "chaos-matrix"); code != 1 {
		t.Fatalf("exit %d, want 1: %s", code, h.stderr)
	}
	if len(h.runner.cmds) != 4 {
		t.Errorf("%d runs, want 4 (matrix continues after a failure)", len(h.runner.cmds))
	}
	if !strings.Contains(h.stderr.String(), "1 of 4 chaos runs failed") {
		t.Errorf("stderr = %q", h.stderr)
	}
	if !strings.Contains(h.stdout.String(), "events             seed 2   FAIL") {
		t.Errorf("summary missing failure: %q", h.stdout)
	}

	h = newHarness(t)
	h.env["CHAOS_DURATION"] = "forever"
	if code := h.run(t, "chaos-matrix"); code != 2 {
		t.Errorf("bad duration: exit %d, want 2", code)
	}
}

func TestOpenAPICheck(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t, "openapi-check"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	got := h.runner.strings()
	if len(got) != 2 {
		t.Fatalf("commands = %q", got)
	}
	tmp := filepath.Join(h.app.TempDir, "openapi-check-")
	if !strings.HasPrefix(got[0], "go run ./cmd/mediatorctl openapi export --out ") || !strings.Contains(got[0], tmp) {
		t.Errorf("export = %q", got[0])
	}
	if !strings.HasPrefix(got[1], "git diff --no-index --exit-code api/openapi.json ") || !strings.Contains(got[1], tmp) {
		t.Errorf("diff = %q", got[1])
	}

	h = newHarness(t)
	h.app.TempDir = ""
	h.runner.fails = nil
	h.runner.tools = nil
	failing := &failEveryGit{fakeRunner: h.runner}
	h.app.Runner = failing
	if code := h.run(t, "openapi-check"); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(h.stderr.String(), "api/openapi.json is out of date") {
		t.Errorf("stderr = %q", h.stderr)
	}
}

// failEveryGit fails any git command so the drift branch can be tested
// without knowing the temporary file name.
type failEveryGit struct{ *fakeRunner }

func (f *failEveryGit) Run(c Cmd) error {
	f.cmds = append(f.cmds, c)
	if c.Name == "git" {
		return errors.New("exit status 1")
	}
	return nil
}

func TestCover(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t, "cover", "-thresholds", "mediator/pg=90"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{
		"go test -covermode=atomic -coverpkg=./mediator/... -coverprofile=coverage/unit.out ./mediator/...",
		"go run ./tools/covergate -profile coverage/unit.out -summary coverage/summary.md -thresholds mediator/pg=90",
	})
	if _, err := os.Stat(filepath.Join(h.app.Root, "coverage")); err != nil {
		t.Errorf("coverage dir not created: %v", err)
	}
	h = newHarness(t)
	h.runner.fails["go test -covermode=atomic -coverpkg=./mediator/... -coverprofile=coverage/unit.out ./mediator/..."] = errors.New("exit status 1")
	if code := h.run(t, "cover"); code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
}

const benchOutput = "goos: linux\nBenchmarkSend-8 1000 1000 ns/op 128 B/op 4 allocs/op\nPASS\n"

func TestBenchWithoutBaseline(t *testing.T) {
	h := newHarness(t)
	h.runner.stdoutText = benchOutput
	if code := h.run(t, "bench"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{"go test -run xxx -bench . -benchmem -count 6 ./mediator/..."})
	data, err := os.ReadFile(filepath.Join(h.app.Root, "coverage", "bench.txt"))
	if err != nil || string(data) != benchOutput {
		t.Errorf("bench.txt = %q, %v", data, err)
	}
	if !strings.Contains(h.stdout.String(), "no baseline") {
		t.Errorf("stdout = %q", h.stdout)
	}
	if !strings.Contains(h.stdout.String(), "BenchmarkSend-8") {
		t.Errorf("bench output not echoed: %q", h.stdout)
	}
	if code := h.run(t, "bench", "-count", "x"); code != 2 {
		t.Errorf("bad flag: exit %d, want 2", code)
	}
}

func TestBenchGate(t *testing.T) {
	csvOK := ",old,,new,,,\n,sec/op,CI,sec/op,CI,vs base,P\nSend-8,1e-06,1%,1.05e-06,1%,+5.00%,p=0.002 n=6\nPublish-8,2e-06,1%,2e-06,1%,~,p=1.000 n=6\ngeomean,1,,1,,+2.00%,\n"
	csvBad := ",old,,new,,,\n,sec/op,CI,sec/op,CI,vs base,P\nSend-8,1e-06,1%,1.3e-06,1%,+29.97%,p=0.002 n=6\n,old,,new,,,\n,B/op,CI,B/op,CI,vs base,P\nSend-8,128,0%,256,0%,+100.00%,p=0.002 n=6\n"
	const gateCmd = "benchstat -alpha 0.05 -format csv coverage/bench-baseline.txt coverage/bench.txt"

	h := newHarness(t)
	h.runner.stdoutText = benchOutput
	h.runner.tools["benchstat"] = true
	writeFile(t, filepath.Join(h.app.Root, "coverage", "bench-baseline.txt"), benchOutput)
	h.runner.outputs[gateCmd] = csvOK
	if code := h.run(t, "bench", "-count", "3"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{
		"go test -run xxx -bench . -benchmem -count 3 ./mediator/...",
		"benchstat -alpha 0.05 coverage/bench-baseline.txt coverage/bench.txt",
		gateCmd,
	})
	if !strings.Contains(h.stdout.String(), "no sec/op regression above 10% (2 benchmark(s) compared)") {
		t.Errorf("stdout = %q", h.stdout)
	}

	// A regression above the threshold fails; B/op is reported but not gated.
	h.runner.cmds = nil
	h.runner.outputs[gateCmd] = csvBad
	if code := h.run(t, "bench"); code != 1 {
		t.Fatalf("exit %d, want 1: %s", code, h.stderr)
	}
	if !strings.Contains(h.stderr.String(), "1 benchmark(s) regressed more than 10% on the median:\n  Send-8 sec/op +29.97% (p=0.002 n=6)") {
		t.Errorf("stderr = %q", h.stderr)
	}
	// A looser threshold passes the same data.
	if code := h.run(t, "bench", "-threshold", "50"); code != 0 {
		t.Errorf("threshold 50: exit %d: %s", code, h.stderr)
	}
	// Unparsable CSV and a failing benchstat are errors.
	h.runner.outputs[gateCmd] = ",sec/op,CI,sec/op,CI,vs base,P\nSend-8,1,1%,1,1%,+x%,p\n"
	if code := h.run(t, "bench"); code != 1 {
		t.Errorf("bad csv: exit %d, want 1", code)
	}
	h.runner.fails[gateCmd] = errors.New("exit status 1")
	if code := h.run(t, "bench"); code != 1 {
		t.Errorf("benchstat csv failure: exit %d, want 1", code)
	}
	delete(h.runner.fails, gateCmd)
	h.runner.fails["benchstat -alpha 0.05 coverage/bench-baseline.txt coverage/bench.txt"] = errors.New("exit status 1")
	if code := h.run(t, "bench"); code != 1 {
		t.Errorf("benchstat text failure: exit %d, want 1", code)
	}

	// Without benchstat the gate is skipped.
	h = newHarness(t)
	h.runner.stdoutText = benchOutput
	writeFile(t, filepath.Join(h.app.Root, "coverage", "bench-baseline.txt"), benchOutput)
	if code := h.run(t, "bench"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	if !strings.Contains(h.stdout.String(), "skipped: benchstat not installed") {
		t.Errorf("stdout = %q", h.stdout)
	}

	// A failing benchmark run fails the task.
	h = newHarness(t)
	h.runner.fails["go test -run xxx -bench . -benchmem -count 6 ./mediator/..."] = errors.New("exit status 1")
	if code := h.run(t, "bench"); code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
}

func TestBenchBaseline(t *testing.T) {
	h := newHarness(t)
	if code := h.run(t, "bench-baseline"); code != 1 {
		t.Errorf("missing bench.txt: exit %d, want 1", code)
	}
	writeFile(t, filepath.Join(h.app.Root, "coverage", "bench.txt"), benchOutput)
	if code := h.run(t, "bench-baseline"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	data, err := os.ReadFile(filepath.Join(h.app.Root, "coverage", "bench-baseline.txt"))
	if err != nil || string(data) != benchOutput {
		t.Errorf("baseline = %q, %v", data, err)
	}
}

func TestCI(t *testing.T) {
	h := newHarness(t)
	h.runner.outputs["go env CGO_ENABLED CC"] = "0\ngcc\n"
	if code := h.run(t, "ci"); code != 0 {
		t.Fatalf("exit %d: %s", code, h.stderr)
	}
	assertCmds(t, h.runner.strings(), []string{
		"gofmt -l .",
		"go vet ./...",
		"go env CGO_ENABLED CC",
		"go test -shuffle=on -count=2 ./...",
		"go test -covermode=atomic -coverpkg=./mediator/... -coverprofile=coverage/unit.out ./mediator/...",
		"go run ./tools/covergate -profile coverage/unit.out -summary coverage/summary.md",
	})

	h = newHarness(t)
	h.runner.outputs["gofmt -l ."] = "bad.go\n"
	if code := h.run(t, "ci"); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if !strings.Contains(h.stderr.String(), "task ci failed: fmt: gofmt") {
		t.Errorf("stderr = %q", h.stderr)
	}
	if len(h.runner.cmds) != 1 {
		t.Errorf("ci continued after fmt failed: %q", h.runner.strings())
	}
}

func TestCmdString(t *testing.T) {
	c := Cmd{Name: "go", Args: []string{"test", "-tags", "a b", `x"y`}}
	if got, want := c.String(), `go test -tags "a b" "x\"y"`; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestParseGoVersion(t *testing.T) {
	cases := []struct {
		in         string
		maj, min   int
		ok         bool
		wantString string
	}{
		{"1.27", 1, 27, true, ""},
		{"go1.26.5 from abc", 1, 26, true, ""},
		{"golangci-lint has version 2.12.2 built with go1.26.5", 1, 26, true, ""},
		{"dev", 0, 0, false, ""},
		{"", 0, 0, false, ""},
	}
	for _, tc := range cases {
		maj, min, ok := parseGoVersion(tc.in)
		if maj != tc.maj || min != tc.min || ok != tc.ok {
			t.Errorf("parseGoVersion(%q) = %d,%d,%v want %d,%d,%v", tc.in, maj, min, ok, tc.maj, tc.min, tc.ok)
		}
	}
}

func TestModuleGoVersion(t *testing.T) {
	h := newHarness(t)
	maj, min, err := h.app.moduleGoVersion()
	if err != nil || maj != 1 || min != 27 {
		t.Errorf("moduleGoVersion = %d,%d,%v", maj, min, err)
	}
	writeFile(t, filepath.Join(h.app.Root, "go.mod"), "module x\n")
	if _, _, err := h.app.moduleGoVersion(); err == nil {
		t.Error("expected error without go directive")
	}
	h.app.Root = filepath.Join(h.app.Root, "missing")
	if _, _, err := h.app.moduleGoVersion(); err == nil {
		t.Error("expected error without go.mod")
	}
}

func TestFindRoot(t *testing.T) {
	root := t.TempDir()
	writeFile(t, filepath.Join(root, "go.mod"), "module x\n")
	nested := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	if got := findRoot(nested); got != root {
		t.Errorf("findRoot(%q) = %q, want %q", nested, got, root)
	}
	orphan := t.TempDir()
	if got := findRoot(orphan); got != orphan {
		// The temp dir may sit under a module on some machines; only assert
		// the fallback when nothing above it has a go.mod.
		if _, err := os.Stat(filepath.Join(got, "go.mod")); err != nil {
			t.Errorf("findRoot(%q) = %q", orphan, got)
		}
	}
}

func TestCopyFileErrors(t *testing.T) {
	dir := t.TempDir()
	if err := copyFile(filepath.Join(dir, "missing"), filepath.Join(dir, "out")); err == nil {
		t.Error("expected error for missing source")
	}
	writeFile(t, filepath.Join(dir, "src"), "x")
	if err := copyFile(filepath.Join(dir, "src"), filepath.Join(dir, "nodir", "out")); err == nil {
		t.Error("expected error for missing destination directory")
	}
}

func TestHelpers(t *testing.T) {
	if got := nonEmptyLines(" a \n\n b\n"); strings.Join(got, ",") != "a,b" {
		t.Errorf("nonEmptyLines = %q", got)
	}
	if got := splitList(" a, ,b,"); strings.Join(got, "|") != "a|b" {
		t.Errorf("splitList = %q", got)
	}
	h := newHarness(t)
	h.app.Getenv = nil
	t.Setenv("TASK_TEST_ENV_PROBE", "yes")
	if got := h.app.envOr("TASK_TEST_ENV_PROBE", "no"); got != "yes" {
		t.Errorf("envOr through os.Getenv = %q", got)
	}
	if got := h.app.envOr("TASK_TEST_ENV_MISSING", "def"); got != "def" {
		t.Errorf("envOr default = %q", got)
	}
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}
