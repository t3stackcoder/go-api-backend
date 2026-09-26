package main

import (
	"bytes"
	"errors"
	"go/ast"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// failProgram fails every command whose program is name, so error branches
// can be reached without knowing generated arguments such as temp files.
type failProgram struct {
	*fakeRunner
	name string
}

func (f *failProgram) Run(c Cmd) error {
	f.cmds = append(f.cmds, c)
	if c.Name == f.name {
		return errors.New("exit status 1")
	}
	return nil
}

func TestExecRunner(t *testing.T) {
	var stdout, stderr bytes.Buffer
	r := execRunner{stdout: &stdout, stderr: &stderr}
	out, err := r.Output(Cmd{Name: "go", Args: []string{"env", "GOOS"}})
	if err != nil || strings.TrimSpace(string(out)) != runtime.GOOS {
		t.Fatalf("Output = %q, %v", out, err)
	}
	var captured bytes.Buffer
	if err := r.Run(Cmd{Name: "go", Args: []string{"env", "GOARCH"}, Stdout: &captured, Env: []string{"TASK_PROBE=1"}}); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(captured.String()) != runtime.GOARCH {
		t.Errorf("captured = %q", captured.String())
	}
	if err := r.Run(Cmd{Name: "go", Args: []string{"env", "GOARCH"}}); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(stdout.String()) != runtime.GOARCH {
		t.Errorf("stdout = %q", stdout.String())
	}
	if _, err := r.LookPath("go"); err != nil {
		t.Errorf("LookPath(go): %v", err)
	}
	if _, err := r.LookPath("definitely-not-a-program-xyz"); err == nil {
		t.Error("LookPath of a missing program succeeded")
	}
	if err := r.Run(Cmd{Name: "definitely-not-a-program-xyz"}); err == nil {
		t.Error("Run of a missing program succeeded")
	}
}

func TestUsageErrorUnwrap(t *testing.T) {
	inner := errors.New("inner")
	if !errors.Is(usageError{inner}, inner) {
		t.Error("usageError does not unwrap")
	}
}

func TestIsTestingF(t *testing.T) {
	cases := map[string]ast.Expr{
		"value":       &ast.SelectorExpr{X: ast.NewIdent("testing"), Sel: ast.NewIdent("F")},
		"bare":        &ast.StarExpr{X: ast.NewIdent("F")},
		"other pkg":   &ast.StarExpr{X: &ast.SelectorExpr{X: ast.NewIdent("other"), Sel: ast.NewIdent("F")}},
		"testing.T":   &ast.StarExpr{X: &ast.SelectorExpr{X: ast.NewIdent("testing"), Sel: ast.NewIdent("T")}},
		"call expr X": &ast.StarExpr{X: &ast.SelectorExpr{X: &ast.CallExpr{}, Sel: ast.NewIdent("F")}},
	}
	for name, e := range cases {
		if isTestingF(e) {
			t.Errorf("%s: isTestingF = true", name)
		}
	}
	ok := &ast.StarExpr{X: &ast.SelectorExpr{X: ast.NewIdent("testing"), Sel: ast.NewIdent("F")}}
	if !isTestingF(ok) {
		t.Error("*testing.F not recognized")
	}
}

func TestParseGoVersionOverflow(t *testing.T) {
	if _, _, ok := parseGoVersion("go99999999999999999999.1"); ok {
		t.Error("overflowing major accepted")
	}
}

func TestModuleGoVersionUnparsable(t *testing.T) {
	h := newHarness(t)
	writeFile(t, filepath.Join(h.app.Root, "go.mod"), "module x\n\ngo abc\n")
	if _, _, err := h.app.moduleGoVersion(); err == nil {
		t.Error("expected error for unparsable go directive")
	}
}

func TestLintToolFailures(t *testing.T) {
	for _, tool := range []string{"staticcheck", "govulncheck"} {
		h := newHarness(t)
		h.runner.tools[tool] = true
		h.runner.fails[tool+" ./..."] = errors.New("exit status 1")
		if code := h.run(t, "lint"); code != 1 {
			t.Errorf("%s failure: exit %d, want 1", tool, code)
		}
	}
}

func TestRunChaosBadDuration(t *testing.T) {
	h := newHarness(t)
	if err := h.app.runChaos("register", "1", "bad"); err == nil {
		t.Error("expected error")
	}
}

func TestOpenAPIExportFailure(t *testing.T) {
	h := newHarness(t)
	h.app.Runner = &failProgram{fakeRunner: h.runner, name: "go"}
	if code := h.run(t, "openapi-check"); code != 1 {
		t.Errorf("exit %d, want 1", code)
	}
	if len(h.runner.cmds) != 1 {
		t.Errorf("diff ran after a failed export: %q", h.runner.strings())
	}
}

func TestDirectoryCreationFailures(t *testing.T) {
	// A file where a directory must be created makes MkdirAll fail.
	h := newHarness(t)
	writeFile(t, filepath.Join(h.app.Root, "coverage"), "not a dir")
	for _, task := range []string{"cover", "bench"} {
		if code := h.run(t, task); code != 1 {
			t.Errorf("%s with coverage as a file: exit %d, want 1", task, code)
		}
	}
	if len(h.runner.cmds) != 0 {
		t.Errorf("commands ran despite mkdir failure: %q", h.runner.strings())
	}
	writeFile(t, filepath.Join(h.app.Root, "api"), "not a dir")
	if code := h.run(t, "openapi"); code != 1 {
		t.Errorf("openapi with api as a file: exit %d, want 1", code)
	}

	// bench.txt as a directory makes os.Create fail.
	h = newHarness(t)
	if err := os.MkdirAll(filepath.Join(h.app.Root, "coverage", "bench.txt"), 0o755); err != nil {
		t.Fatal(err)
	}
	if code := h.run(t, "bench"); code != 1 {
		t.Errorf("bench with bench.txt as a directory: exit %d, want 1", code)
	}

	// bench-baseline.txt as a directory makes the copy fail.
	h = newHarness(t)
	writeFile(t, filepath.Join(h.app.Root, "coverage", "bench.txt"), benchOutput)
	if err := os.MkdirAll(filepath.Join(h.app.Root, "coverage", "bench-baseline.txt"), 0o755); err != nil {
		t.Fatal(err)
	}
	if code := h.run(t, "bench-baseline"); code != 1 {
		t.Errorf("bench-baseline onto a directory: exit %d, want 1", code)
	}
}
