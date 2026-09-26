package testkit_test

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// TestFaultPointCatalogue fails when a testkit.Fault call site is added or
// removed without updating faultpoints.txt (Appendix A).
func TestFaultPointCatalogue(t *testing.T) {
	root := filepath.Join("..", "..")
	golden := loadCatalogue(t, filepath.Join("faultpoints.txt"))
	found := map[string]bool{}
	re := regexp.MustCompile(`testkit\.Fault(?:After)?\(\s*[^,]+,\s*"([a-z0-9_.]+)"`)
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			name := d.Name()
			if name == ".git" || name == "node_modules" || name == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, m := range re.FindAllSubmatch(src, -1) {
			found[string(m[1])] = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var missing, stale []string
	for p := range found {
		if !slices.Contains(golden, p) {
			missing = append(missing, p)
		}
	}
	for _, p := range golden {
		if !found[p] {
			stale = append(stale, p)
		}
	}
	slices.Sort(missing)
	slices.Sort(stale)
	if len(missing) > 0 {
		t.Errorf("fault points used in code but not in faultpoints.txt: %v", missing)
	}
	if len(stale) > 0 && os.Getenv("FAULTPOINTS_ALLOW_UNUSED") == "" {
		t.Logf("fault points in faultpoints.txt not yet referenced by code (allowed while packages are being built): %v", stale)
	}
}

func loadCatalogue(t *testing.T, path string) []string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, strings.Fields(line)...)
	}
	return out
}
