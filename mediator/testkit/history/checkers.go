package history

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// Checker violation IDs.
const (
	IDAppend       = "Append"
	IDStaleness    = "Staleness"
	IDConservation = "Conservation"
	IDLogScan      = "LogScan"
	IDFencing      = "Fencing"
)

// AppendListChecker checks the idempotent-append workload (spec 11.6). Per
// key, the observed list is the value of the last ok read-list op. Every ok
// append value must appear exactly once, every fail append value must be
// absent, every info append value may appear at most once, and the observed
// order must be a valid interleaving of the per-process sequences of ok
// appends (a process appends sequentially, so its values appear in
// invocation order; info values are unconstrained because their process was
// retired). Keys with appends but no final read are reported.
func AppendListChecker(ops []Op) []Violation {
	type appendOp struct {
		process int
		value   int64
		typ     string
		invoke  int64
	}
	byKey := map[string][]appendOp{}
	finals := map[string][]int64{}
	pending := map[int]Op{}
	sorted := make([]Op, len(ops))
	copy(sorted, ops)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Time < sorted[j].Time })
	var out []Violation
	for _, op := range sorted {
		switch {
		case op.Type == TypeInvoke:
			pending[op.Process] = op
		case op.IsReturn():
			inv, ok := pending[op.Process]
			if !ok {
				continue
			}
			delete(pending, op.Process)
			switch op.F {
			case FAppend:
				v, ok := Int(inv.Value)
				if !ok {
					out = append(out, Violation{ID: IDAppend, Msg: "append value is not an integer", Details: map[string]any{"key": op.Key, "process": op.Process, "value": inv.Value}})
					continue
				}
				byKey[op.Key] = append(byKey[op.Key], appendOp{process: op.Process, value: v, typ: op.Type, invoke: inv.Time})
			case FReadList:
				if op.Type != TypeOK {
					continue
				}
				list, ok := Ints(op.Value)
				if !ok {
					out = append(out, Violation{ID: IDAppend, Msg: "read-list value is not a list of integers", Details: map[string]any{"key": op.Key, "value": op.Value}})
					continue
				}
				finals[op.Key] = list
			}
		}
	}
	// Invokes never completed are info.
	for _, inv := range pending {
		if inv.F == FAppend {
			if v, ok := Int(inv.Value); ok {
				byKey[inv.Key] = append(byKey[inv.Key], appendOp{process: inv.Process, value: v, typ: TypeInfo, invoke: inv.Time})
			}
		}
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		appends := byKey[key]
		final, ok := finals[key]
		if !ok {
			out = append(out, Violation{ID: IDAppend, Msg: "no final read-list for key", Details: map[string]any{"key": key}})
			continue
		}
		count := map[int64]int{}
		position := map[int64][]int{}
		for i, v := range final {
			count[v]++
			position[v] = append(position[v], i)
		}
		sort.Slice(appends, func(i, j int) bool { return appends[i].invoke < appends[j].invoke })
		expected := map[int64]bool{}
		perProcess := map[int][]appendOp{}
		for _, a := range appends {
			expected[a.value] = true
			switch a.typ {
			case TypeOK:
				if count[a.value] != 1 {
					out = append(out, Violation{ID: IDAppend, Msg: "ok append value must appear exactly once", Details: map[string]any{"key": key, "value": a.value, "count": count[a.value], "process": a.process}})
				}
				perProcess[a.process] = append(perProcess[a.process], a)
			case TypeFail:
				if count[a.value] != 0 {
					out = append(out, Violation{ID: IDAppend, Msg: "failed append value must be absent", Details: map[string]any{"key": key, "value": a.value, "count": count[a.value], "process": a.process}})
				}
			case TypeInfo:
				if count[a.value] > 1 {
					out = append(out, Violation{ID: IDAppend, Msg: "info append value may appear at most once", Details: map[string]any{"key": key, "value": a.value, "count": count[a.value], "process": a.process}})
				}
			}
		}
		for i, v := range final {
			if !expected[v] {
				out = append(out, Violation{ID: IDAppend, Msg: "list holds a value no append produced", Details: map[string]any{"key": key, "value": v, "index": i}})
			}
		}
		procs := make([]int, 0, len(perProcess))
		for p := range perProcess {
			procs = append(procs, p)
		}
		sort.Ints(procs)
		for _, p := range procs {
			seq := perProcess[p]
			last := -1
			for _, a := range seq {
				pos := position[a.value]
				if len(pos) != 1 {
					continue // already reported
				}
				if pos[0] < last {
					out = append(out, Violation{ID: IDAppend, Msg: "list order is not an interleaving of per-process order", Details: map[string]any{"key": key, "process": p, "value": a.value, "index": pos[0], "previous_index": last}})
				}
				if pos[0] > last {
					last = pos[0]
				}
			}
		}
	}
	return out
}

// Stale describes one stale read found by CheckStaleness.
type Stale struct {
	Read Read
	// Write is the latest definite write that returned before the read
	// began and whose effect the read missed.
	Write Write
}

// CheckStaleness is the core of I7 and of BoundedStalenessChecker. A read
// invoked after a definite write returned must not return a value older than
// that write: the returned value must be the value of some write (to the same
// key) that returned no earlier than the latest write completed before the
// read began, or of an in-flight or info write. A read that returns nil (not
// found) after any definite write is stale. A stale read is excused when a
// degraded window covering one of its tags overlaps
// [read.Invoke - ttl, read.Return]: the entry may have been stored while
// Redis was unreachable for the bumps and served until its TTL (spec 7.4,
// G9). Reads and writes to different keys never interact.
func CheckStaleness(reads []Read, writes []Write, degraded []Window, ttl time.Duration) []Stale {
	byKey := map[string][]Write{}
	for _, w := range writes {
		byKey[w.Key] = append(byKey[w.Key], w)
	}
	var out []Stale
	for _, r := range reads {
		ws := byKey[r.Key]
		// Latest definite write that completed before the read began.
		var latest *Write
		for i := range ws {
			w := &ws[i]
			if !w.Definite || w.Return > r.Invoke {
				continue
			}
			if latest == nil || w.Return > latest.Return {
				latest = w
			}
		}
		if latest == nil {
			continue
		}
		fresh := false
		if r.Value != nil {
			for _, w := range ws {
				if !Equal(w.Value, r.Value) {
					continue
				}
				bound := w.Return
				if !w.Definite {
					bound = math.MaxInt64
				}
				if bound >= latest.Return {
					fresh = true
					break
				}
			}
		}
		if fresh {
			continue
		}
		from := r.Invoke - int64(ttl)
		excused := false
		for _, w := range degraded {
			if w.Overlaps(from, r.Return, r.Tags) {
				excused = true
				break
			}
		}
		if !excused {
			out = append(out, Stale{Read: r, Write: *latest})
		}
	}
	return out
}

// TagsOf returns the cache tags recorded in an op's Extra["tags"], or the
// key itself when none were recorded.
func TagsOf(op Op) []string {
	if op.Extra != nil {
		if tags := Strings(op.Extra["tags"]); len(tags) > 0 {
			return tags
		}
	}
	return []string{op.Key}
}

// ReadsAndWrites converts a register history into the reads (ok get ops)
// and writes (ok and info set ops) of the staleness check. Ops with other
// functions are ignored.
func ReadsAndWrites(ops []Op) ([]Read, []Write) {
	sorted := make([]Op, len(ops))
	copy(sorted, ops)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Time < sorted[j].Time })
	pending := map[int]Op{}
	var reads []Read
	var writes []Write
	for _, op := range sorted {
		switch {
		case op.Type == TypeInvoke:
			pending[op.Process] = op
		case op.IsReturn():
			inv, ok := pending[op.Process]
			if !ok {
				continue
			}
			delete(pending, op.Process)
			switch {
			case op.F == FGet && op.Type == TypeOK:
				reads = append(reads, Read{Process: op.Process, Key: op.Key, Tags: TagsOf(inv), Invoke: inv.Time, Return: op.Time, Value: Normalize(op.Value)})
			case op.F == FSet && op.Type == TypeOK:
				writes = append(writes, Write{Process: op.Process, Key: op.Key, Tags: TagsOf(inv), Invoke: inv.Time, Return: op.Time, Value: Normalize(inv.Value), Definite: true})
			case op.F == FSet && op.Type == TypeInfo:
				writes = append(writes, Write{Process: op.Process, Key: op.Key, Tags: TagsOf(inv), Invoke: inv.Time, Return: math.MaxInt64, Value: Normalize(inv.Value)})
			}
		}
	}
	for _, inv := range pending {
		if inv.F == FSet {
			writes = append(writes, Write{Process: inv.Process, Key: inv.Key, Tags: TagsOf(inv), Invoke: inv.Time, Return: math.MaxInt64, Value: Normalize(inv.Value)})
		}
	}
	return reads, writes
}

// BoundedStalenessChecker checks the register-cached and cache-staleness
// workloads: no ok get may return a value older than the last set that
// returned before the get was invoked, unless a degraded window for its tags
// overlaps the TTL before it (spec 11.6, G9). Windows come from the nemesis
// log; ttl is the cache TTL of the query.
func BoundedStalenessChecker(ops []Op, windows []Window, ttl time.Duration) []Violation {
	reads, writes := ReadsAndWrites(ops)
	var out []Violation
	for _, s := range CheckStaleness(reads, writes, windows, ttl) {
		out = append(out, StaleViolation(s))
	}
	return out
}

// StaleViolation renders a stale read as a violation.
func StaleViolation(s Stale) Violation {
	return Violation{ID: IDStaleness, Msg: "cached read returned a value older than a completed write", Details: map[string]any{
		"key": s.Read.Key, "process": s.Read.Process, "read_invoke": s.Read.Invoke, "read_return": s.Read.Return, "read_value": s.Read.Value,
		"write_process": s.Write.Process, "write_return": s.Write.Return, "write_value": s.Write.Value,
	}}
}

// Conservation checks every ok read-all of the bank workload: the balances
// must sum to total (spec 11.6, G11).
func Conservation(ops []Op, total int64) []Violation {
	var out []Violation
	for _, op := range ops {
		if op.F != FReadAll || op.Type != TypeOK {
			continue
		}
		m, ok := Normalize(op.Value).(map[string]any)
		if !ok {
			out = append(out, Violation{ID: IDConservation, Msg: "read-all value is not an object", Details: map[string]any{"process": op.Process, "value": op.Value}})
			continue
		}
		if bal, ok := m["balances"]; ok {
			m, _ = bal.(map[string]any)
		}
		var sum int64
		for a, v := range m {
			n, ok := Int(v)
			if !ok {
				out = append(out, Violation{ID: IDConservation, Msg: "balance is not an integer", Details: map[string]any{"process": op.Process, "account": a, "value": v}})
				continue
			}
			sum += n
		}
		if sum != total {
			out = append(out, Violation{ID: IDConservation, Msg: fmt.Sprintf("balances sum to %d, want %d", sum, total), Details: map[string]any{"process": op.Process, "time": op.Time, "sum": sum, "total": total}})
		}
	}
	return out
}
