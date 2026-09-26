package history

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// LogScanOptions tunes LogScan.
type LogScanOptions struct {
	// GoroutineSlack is how many goroutines the final goroutines=<n> marker
	// may exceed the first one by before growth is reported. Default 100.
	GoroutineSlack int
	// Patterns are the substrings that fail the scan. Default: "panic",
	// "invariant violated", "conn busy", "DATA RACE".
	Patterns []string
}

// DefaultLogPatterns are the substrings LogScan rejects (spec 11.6).
var DefaultLogPatterns = []string{"panic", "invariant violated", "conn busy", "DATA RACE"}

var goroutineMarker = regexp.MustCompile(`\bgoroutines=(\d+)\b`)

// LogScan scans node log lines for the forbidden patterns of spec 11.6 (any
// panic, "invariant violated", pgx "conn busy", the race detector's
// "DATA RACE") and for goroutine growth: the last goroutines=<n> marker may
// not exceed the first by more than the slack. Every match is a violation
// with the line number and text.
func LogScan(lines []string) []Violation { return LogScanWith(lines, LogScanOptions{}) }

// LogScanWith is LogScan with options.
func LogScanWith(lines []string, opts LogScanOptions) []Violation {
	if opts.GoroutineSlack <= 0 {
		opts.GoroutineSlack = 100
	}
	patterns := opts.Patterns
	if len(patterns) == 0 {
		patterns = DefaultLogPatterns
	}
	var out []Violation
	first, last := -1, -1
	firstLine, lastLine := 0, 0
	for i, line := range lines {
		for _, p := range patterns {
			if strings.Contains(line, p) {
				out = append(out, Violation{ID: IDLogScan, Msg: "forbidden pattern in log", Details: map[string]any{"pattern": p, "line": i + 1, "text": strings.TrimSpace(line)}})
				break
			}
		}
		if m := goroutineMarker.FindStringSubmatch(line); m != nil {
			n, err := strconv.Atoi(m[1])
			if err != nil {
				continue
			}
			if first < 0 {
				first, firstLine = n, i+1
			}
			last, lastLine = n, i+1
		}
	}
	if first >= 0 && last > first+opts.GoroutineSlack {
		out = append(out, Violation{ID: IDLogScan, Msg: "goroutine count grew above baseline", Details: map[string]any{"baseline": first, "baseline_line": firstLine, "final": last, "final_line": lastLine, "slack": opts.GoroutineSlack}})
	}
	return out
}

// FencingRecord is one fencing observation parsed from a node log line:
// "fencing=<n> partition=<p> group=<g> node=<id>" with an optional
// "topic=<t>" and an optional "time=<RFC3339>" used for ordering.
type FencingRecord struct {
	Group     string
	Topic     string
	Partition int
	Node      string
	Token     int64
	Time      time.Time
	Line      int
}

var (
	fencingRe   = regexp.MustCompile(`\bfencing=(\d+)\b`)
	partitionRe = regexp.MustCompile(`\bpartition=(\d+)\b`)
	groupRe     = regexp.MustCompile(`\bgroup=("[^"]*"|\S+)`)
	nodeRe      = regexp.MustCompile(`\bnode=("[^"]*"|\S+)`)
	topicRe     = regexp.MustCompile(`\btopic=("[^"]*"|\S+)`)
	timeRe      = regexp.MustCompile(`\b(?:time|ts)=(\S+)`)
)

func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		if u, err := strconv.Unquote(s); err == nil {
			return u
		}
		return s[1 : len(s)-1]
	}
	return s
}

// ParseFencing extracts the fencing records from log lines, in line order.
// Lines without all of fencing, partition, group, and node are skipped.
func ParseFencing(lines []string) []FencingRecord {
	var out []FencingRecord
	for i, line := range lines {
		f := fencingRe.FindStringSubmatch(line)
		p := partitionRe.FindStringSubmatch(line)
		g := groupRe.FindStringSubmatch(line)
		n := nodeRe.FindStringSubmatch(line)
		if f == nil || p == nil || g == nil || n == nil {
			continue
		}
		token, err1 := strconv.ParseInt(f[1], 10, 64)
		part, err2 := strconv.Atoi(p[1])
		if err1 != nil || err2 != nil {
			continue
		}
		rec := FencingRecord{Group: unquote(g[1]), Partition: part, Node: unquote(n[1]), Token: token, Line: i + 1}
		if t := topicRe.FindStringSubmatch(line); t != nil {
			rec.Topic = unquote(t[1])
		}
		if t := timeRe.FindStringSubmatch(line); t != nil {
			if ts, err := time.Parse(time.RFC3339Nano, unquote(t[1])); err == nil {
				rec.Time = ts
			}
		}
		out = append(out, rec)
	}
	return out
}

// CheckFencing applies the monotonicity rule of G14 to records ordered by
// time (when every record carries one) or by line: per (group, topic,
// partition), a token must never decrease, and a record from a different
// node than the previous record must carry a token strictly greater than
// every token seen before on that partition.
func CheckFencing(records []FencingRecord) []Violation {
	recs := make([]FencingRecord, len(records))
	copy(recs, records)
	timed := len(recs) > 0
	for _, r := range recs {
		if r.Time.IsZero() {
			timed = false
			break
		}
	}
	if timed {
		sort.SliceStable(recs, func(i, j int) bool { return recs[i].Time.Before(recs[j].Time) })
	}
	type key struct {
		group, topic string
		partition    int
	}
	type state struct {
		node string
		last int64
		max  int64
		line int
	}
	seen := map[key]*state{}
	var out []Violation
	for _, r := range recs {
		k := key{r.Group, r.Topic, r.Partition}
		s := seen[k]
		if s == nil {
			seen[k] = &state{node: r.Node, last: r.Token, max: r.Token, line: r.Line}
			continue
		}
		details := map[string]any{"group": r.Group, "topic": r.Topic, "partition": r.Partition, "node": r.Node, "token": r.Token, "previous_node": s.node, "previous_token": s.last, "max_token": s.max, "line": r.Line, "previous_line": s.line}
		switch {
		case r.Node != s.node && r.Token <= s.max:
			out = append(out, Violation{ID: IDFencing, Msg: "fencing token of a new owner is not greater than every earlier token", Details: details})
		case r.Token < s.last:
			out = append(out, Violation{ID: IDFencing, Msg: "fencing token decreased", Details: details})
		}
		s.node, s.last, s.line = r.Node, r.Token, r.Line
		if r.Token > s.max {
			s.max = r.Token
		}
	}
	return out
}

// FencingFromLogs parses the fencing records of node logs and checks G14
// (spec 11.6, "Fencing monotonicity from node logs").
func FencingFromLogs(lines []string) []Violation { return CheckFencing(ParseFencing(lines)) }
