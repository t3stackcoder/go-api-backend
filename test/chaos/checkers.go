//go:build chaos

package chaos

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
)

// checkResult is the outcome of one checker in summary.json.
type checkResult struct {
	OK         bool                `json:"ok"`
	Violations []history.Violation `json:"violations"`
	ElapsedMS  int64               `json:"elapsed_ms"`
	Note       string              `json:"note,omitempty"`
	Error      string              `json:"error,omitempty"`
}

// newCheck builds a checkResult; a check error is a failure too.
func newCheck(started time.Time, vs []history.Violation, err error) checkResult {
	if vs == nil {
		vs = []history.Violation{}
	}
	c := checkResult{OK: len(vs) == 0 && err == nil, Violations: vs, ElapsedMS: time.Since(started).Milliseconds()}
	if err != nil {
		c.Error = err.Error()
	}
	return c
}

// l2Summary reports the resolution of retired info operations (L2).
type l2Summary struct {
	Resent       int              `json:"resent"`
	ResolvedOK   int              `json:"resolved_ok"`
	ResolvedFail int              `json:"resolved_fail"`
	Unresolved   []map[string]any `json:"unresolved"`
}

// summary is summary.json.
type summary struct {
	Workload      string                 `json:"workload"`
	Seed          int64                  `json:"seed"`
	Duration      string                 `json:"duration"`
	Started       string                 `json:"started"`
	Finished      string                 `json:"finished"`
	RunDir        string                 `json:"run_dir"`
	Nodes         []string               `json:"nodes"`
	Ops           map[string]int         `json:"ops"`
	Nemeses       map[string]int         `json:"nemeses"`
	NemesisTotal  int                    `json:"nemesis_total"`
	Checkers      map[string]checkResult `json:"checkers"`
	Timings       map[string]int64       `json:"timings_ms"`
	L2            *l2Summary             `json:"l2,omitempty"`
	Goroutines    map[string]int         `json:"goroutines,omitempty"`
	RelayReplayed int64                  `json:"relay_replayed"`
	// RelayReplayedLog counts the relay's "stream data loss detected" log
	// records across the node logs; unlike the live counter it survives
	// node restarts.
	RelayReplayedLog int                 `json:"relay_replayed_log"`
	Soak             []history.Violation `json:"soak_violations,omitempty"`
	Pass             bool                `json:"pass"`
	Failures         []string            `json:"failures"`
}

// failures lists every failed checker and harness failure, one line each.
func (s *summary) failures() []string {
	out := append([]string(nil), s.Failures...)
	for _, name := range sortedKeys(s.Checkers) {
		c := s.Checkers[name]
		if c.OK {
			continue
		}
		msg := fmt.Sprintf("%s: %d violation(s)", name, len(c.Violations))
		if c.Error != "" {
			msg += "; error: " + c.Error
		}
		for i, v := range c.Violations {
			if i >= 5 {
				msg += fmt.Sprintf("\n    ... %d more", len(c.Violations)-5)
				break
			}
			msg += "\n    " + v.String()
		}
		out = append(out, msg)
	}
	if len(s.Soak) > 0 {
		out = append(out, fmt.Sprintf("soak: %d violation(s) during mayhem", len(s.Soak)))
	}
	return out
}

// splitLines splits container logs into lines without carriage returns.
func splitLines(raw string) []string {
	lines := strings.Split(raw, "\n")
	for i, l := range lines {
		lines[i] = strings.TrimRight(l, "\r")
	}
	return lines
}

// offlineChecks are the checks that need only the run directory: the
// workload checker, fencing monotonicity from the node logs, the log scan
// per node, and the shutdown check from the nemesis log. They run at the
// end of a run and again in TestReplay.
func offlineChecks(wl scenario, ops []history.Op, events []nemesisEvent, logs map[string][]string, htmlPath string, timeout time.Duration) map[string]checkResult {
	out := map[string]checkResult{}
	runEnd := int64(0)
	for _, op := range ops {
		if op.Time > runEnd {
			runEnd = op.Time
		}
	}
	windows := windowsFrom(events, runEnd)

	started := time.Now()
	out["workload"] = newCheck(started, wl.check(ops, windows, htmlPath, timeout), nil)

	started = time.Now()
	var all []string
	for _, name := range sortedKeys(logs) {
		all = append(all, logs[name]...)
	}
	out["fencing_logs"] = newCheck(started, history.FencingFromLogs(all), nil)

	started = time.Now()
	var scan []history.Violation
	for _, name := range sortedKeys(logs) {
		for _, v := range history.LogScan(logs[name]) {
			if v.Details == nil {
				v.Details = map[string]any{}
			}
			v.Details["node"] = name
			scan = append(scan, v)
		}
	}
	out["logscan"] = newCheck(started, scan, nil)

	started = time.Now()
	out["shutdown"] = newCheck(started, shutdownCheck(events), nil)
	return out
}

// groupByID splits violations by their ID so summary.json shows one entry
// per invariant; wanted lists the IDs that must appear even when clean.
func groupByID(vs []history.Violation, wanted []string, started time.Time, err error) map[string]checkResult {
	byID := map[string][]history.Violation{}
	for _, v := range vs {
		byID[v.ID] = append(byID[v.ID], v)
	}
	ids := append([]string(nil), wanted...)
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := map[string]checkResult{}
	for _, id := range ids {
		if _, done := out[id]; done {
			continue
		}
		out[id] = newCheck(started, byID[id], nil)
	}
	if err != nil {
		out["invariants_error"] = newCheck(started, nil, err)
	}
	return out
}
