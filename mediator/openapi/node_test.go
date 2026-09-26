package openapi_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// TestTypeScriptClient proves the document is consumable by the frontend
// job of spec 8.6: openapi-typescript generates a client from the golden and
// tsc type-checks it in strict mode. It needs Node and network access for
// npx, so it runs only with OPENAPI_TS=1.
func TestTypeScriptClient(t *testing.T) {
	if os.Getenv("OPENAPI_TS") != "1" {
		t.Skip("set OPENAPI_TS=1 to run the openapi-typescript and tsc check")
	}
	dir := t.TempDir()
	spec := filepath.Join(dir, "openapi.json")
	if err := os.WriteFile(spec, golden(t), 0o644); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "api.d.ts")
	npx(t, dir, "--yes", "openapi-typescript@7", spec, "-o", out)
	if _, err := os.Stat(out); err != nil {
		t.Fatalf("no client generated: %v", err)
	}
	npx(t, dir, "--yes", "-p", "typescript@5", "tsc", "--noEmit", "--strict", out)
}

// npx runs npx with args in dir and fails the test on a non-zero exit.
func npx(t *testing.T, dir string, args ...string) {
	t.Helper()
	name, argv := "npx", args
	if runtime.GOOS == "windows" {
		name, argv = "cmd", append([]string{"/c", "npx"}, args...)
	}
	cmd := exec.Command(name, argv...)
	cmd.Dir = dir
	outb, err := cmd.CombinedOutput()
	t.Logf("npx %v:\n%s", args, outb)
	if err != nil {
		t.Fatalf("npx %v: %v", args, err)
	}
}
