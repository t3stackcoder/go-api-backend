//go:build chaos

package chaos

import (
	"context"
	"strings"
	"testing"
)

// TestChaos runs one chaos run against the compose `chaos` profile with the
// workload, seed, and duration of the flags (spec 11.6). The verdict is the
// conjunction of every checker; the evidence is the run directory.
func TestChaos(t *testing.T) {
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	r, err := newRun(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	t.Logf("run directory: %s", r.sum.RunDir)
	if err := r.execute(context.Background()); err != nil {
		t.Fatalf("chaos run aborted: %v (run directory %s)", err, r.sum.RunDir)
	}
	t.Logf("ops %v; nemeses %d %v; checkers %s", r.sum.Ops, r.sum.NemesisTotal, r.sum.Nemeses, describeCheckers(r.sum))
	if !r.sum.Pass {
		t.Fatalf("chaos run failed (run directory %s):\n%s", r.sum.RunDir, strings.Join(r.sum.Failures, "\n"))
	}
}

// TestReplay re-runs the offline checkers on the run directory named by
// -replay-dir or CHAOS_REPLAY_DIR. It needs neither Docker nor the stores.
func TestReplay(t *testing.T) {
	dir := *flagReplayDir
	if dir == "" {
		t.Skip("set -replay-dir or CHAOS_REPLAY_DIR to a run directory")
	}
	s, err := replay(dir, *flagCheckTO)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("replayed %s seed %d: ops %v; nemeses %d; checkers %s", s.Workload, s.Seed, s.Ops, s.NemesisTotal, describeCheckers(*s))
	if !s.Pass {
		t.Fatalf("replay of %s failed:\n%s", dir, strings.Join(s.Failures, "\n"))
	}
}

// describeCheckers renders "name=ok name=FAIL(n)" for a log line.
func describeCheckers(s summary) string {
	var parts []string
	for _, name := range sortedKeys(s.Checkers) {
		c := s.Checkers[name]
		if c.OK {
			parts = append(parts, name+"=ok")
		} else {
			parts = append(parts, name+"=FAIL("+itoa(len(c.Violations))+")")
		}
	}
	return strings.Join(parts, " ")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
