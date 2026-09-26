//go:build chaos

package chaos

import (
	"encoding/json/v2"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
)

// replay re-runs the offline checkers on a recorded run directory: the
// workload checker over history.jsonl with the degraded windows of
// nemesis.jsonl, fencing monotonicity and the log scan over the saved node
// logs, and the shutdown check over the nemesis log. It needs no Docker
// and no stores. The result is written to summary-replay.json.
func replay(dir string, checkTimeout time.Duration) (*summary, error) {
	dir = resolveRunDir(dir)
	name, seed := "", int64(0)
	if raw, err := os.ReadFile(filepath.Join(dir, "summary.json")); err == nil {
		var prev summary
		if err := json.Unmarshal(raw, &prev); err == nil {
			name, seed = prev.Workload, prev.Seed
		}
	}
	if name == "" {
		if raw, err := os.ReadFile(filepath.Join(dir, "config.json")); err == nil {
			var cfg config
			if err := json.Unmarshal(raw, &cfg); err == nil {
				name, seed = cfg.Workload, cfg.Seed
			}
		}
	}
	if name == "" {
		base := filepath.Base(dir)
		if i := strings.Index(base, "-seed"); i > 0 {
			name = base[:i]
		}
	}
	wl, err := newWorkload(name)
	if err != nil {
		return nil, fmt.Errorf("replay %s: %w", dir, err)
	}
	f, err := os.Open(filepath.Join(dir, "history.jsonl"))
	if err != nil {
		return nil, err
	}
	ops, err := history.ReadJSONL(f)
	_ = f.Close()
	if err != nil {
		return nil, err
	}
	events, err := readNemesisLog(filepath.Join(dir, "nemesis.jsonl"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	logs := map[string][]string{}
	matches, _ := filepath.Glob(filepath.Join(dir, "node*.log"))
	for _, m := range matches {
		raw, err := os.ReadFile(m)
		if err != nil {
			continue
		}
		logs[strings.TrimSuffix(filepath.Base(m), ".log")] = splitLines(string(raw))
	}
	checks := offlineChecks(wl, ops, events, logs, filepath.Join(dir, "porcupine-replay.html"), checkTimeout)
	counts, total := nemesisCounts(events)
	s := &summary{
		Workload: name, Seed: seed, RunDir: dir, Nodes: sortedKeys(logs),
		Ops: opCounts(ops), Nemeses: counts, NemesisTotal: total,
		Checkers: checks, Timings: map[string]int64{}, Failures: []string{},
		Started: time.Now().Format(time.RFC3339),
	}
	s.Finished = time.Now().Format(time.RFC3339)
	s.Failures = s.failures()
	s.Pass = len(s.Failures) == 0
	if b, err := json.Marshal(s, json.Deterministic(true)); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "summary-replay.json"), b, 0o600)
	}
	return s, nil
}

// opCounts counts ops by type.
func opCounts(ops []history.Op) map[string]int {
	out := map[string]int{"invoke": 0, "ok": 0, "fail": 0, "info": 0}
	for _, op := range ops {
		out[op.Type]++
	}
	return out
}

// resolveRunDir accepts a run directory relative to the working directory
// (the package directory under go test), relative to the module root, or
// as a bare run name under runs/.
func resolveRunDir(dir string) string {
	if info, err := os.Stat(dir); err == nil && info.IsDir() {
		return dir
	}
	for _, cand := range []string{filepath.Join("..", "..", dir), filepath.Join("runs", filepath.Base(dir))} {
		if info, err := os.Stat(cand); err == nil && info.IsDir() {
			return cand
		}
	}
	return dir
}
