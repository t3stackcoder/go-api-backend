// Package history records and checks operation histories in the Jepsen style
// (spec 11.6, Appendix C). A Recorder writes one Op at invoke time and one at
// return time for every operation a chaos or sweep client performs; the ops
// are stored as JSON Lines, converted to Porcupine operations for
// linearizability checking, or fed to the non-linearizability checkers in
// this package (append lists, bounded staleness, log scans, fencing).
//
// The package is a leaf: it imports nothing of the framework so that every
// tier can use it, including the checkers that run offline over a recorded
// history.
package history

import (
	"bufio"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Op types (Appendix C).
const (
	// TypeInvoke starts an operation.
	TypeInvoke = "invoke"
	// TypeOK completes an operation that definitely happened.
	TypeOK = "ok"
	// TypeFail completes an operation that definitely did not happen.
	TypeFail = "fail"
	// TypeInfo completes an operation with an unknown outcome, typically a
	// timeout. It retires the process.
	TypeInfo = "info"
)

// Function names of the workloads in spec 11.6. Checkers select ops by F.
const (
	FSet      = "set"       // SetValue{key, val}
	FGet      = "get"       // GetValue / GetValueCached{key}
	FTransfer = "transfer"  // Transfer{from, to, amt}
	FReadAll  = "read-all"  // ReadAll
	FAppend   = "append"    // Append{key, val}
	FReadList = "read-list" // ReadList{key}
	FBump     = "bump"      // Bump{key}
)

// Op is one history record: an invocation or a completion of an operation
// by a process (Appendix C). Time is nanoseconds from the run start on the
// controller clock.
type Op struct {
	Process int            `json:"process"`
	Client  string         `json:"client,omitempty"`
	Type    string         `json:"type"`
	F       string         `json:"f"`
	Key     string         `json:"key,omitempty"`
	Value   any            `json:"value,omitempty"`
	Error   string         `json:"error,omitempty"`
	Time    int64          `json:"time"`
	Extra   map[string]any `json:"extra,omitempty"`
}

// IsReturn reports whether the op completes an operation (ok, fail, or info).
func (o Op) IsReturn() bool { return o.Type == TypeOK || o.Type == TypeFail || o.Type == TypeInfo }

// Violation is one failed check. ID names the invariant or checker (I1..I7,
// L1, Fencing, LogScan, Append, Staleness, Conservation), Msg is a one-line
// description, and Details carries the evidence.
type Violation struct {
	ID      string         `json:"id"`
	Msg     string         `json:"msg"`
	Details map[string]any `json:"details,omitempty"`
}

func (v Violation) String() string {
	if len(v.Details) == 0 {
		return v.ID + ": " + v.Msg
	}
	keys := make([]string, 0, len(v.Details))
	for k := range v.Details {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(v.ID)
	b.WriteString(": ")
	b.WriteString(v.Msg)
	for _, k := range keys {
		fmt.Fprintf(&b, " %s=%v", k, v.Details[k])
	}
	return b.String()
}

// Report collects the violations of one or more checks.
type Report struct {
	Violations []Violation
}

// OK reports whether the report holds no violation.
func (r Report) OK() bool { return len(r.Violations) == 0 }

// Add appends violations to the report.
func (r *Report) Add(vs ...Violation) { r.Violations = append(r.Violations, vs...) }

// String lists every violation, one per line, or "ok".
func (r Report) String() string {
	if r.OK() {
		return "ok"
	}
	lines := make([]string, len(r.Violations))
	for i, v := range r.Violations {
		lines[i] = v.String()
	}
	return strings.Join(lines, "\n")
}

// Recorder records the history of a run. It is safe for concurrent use; every
// op takes its time from a monotonic clock started at NewRecorder and ops are
// appended in time order.
//
// Following Jepsen, a process has at most one operation outstanding and a
// process whose operation ended in info is retired: its operation may
// complete at any later time, so a fresh process number takes over. Invoke
// panics on a retired process or on a process with an outstanding op, because
// either would make the history unsound; both are harness bugs.
type Recorder struct {
	mu       sync.Mutex
	start    time.Time
	ops      []Op
	nextProc int
	active   map[int]bool
	retired  map[int]bool
}

// NewRecorder starts the clock and returns an empty recorder.
func NewRecorder() *Recorder {
	return &Recorder{start: time.Now(), active: map[int]bool{}, retired: map[int]bool{}}
}

// Now returns the nanoseconds elapsed since the recorder was created, from
// the monotonic clock.
func (r *Recorder) Now() int64 { return int64(time.Since(r.start)) }

// NextProcess hands out a fresh, never used process number.
func (r *Recorder) NextProcess() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.nextProcessLocked()
}

func (r *Recorder) nextProcessLocked() int {
	p := r.nextProc
	r.nextProc++
	return p
}

// Retired reports whether the process ended an operation with info.
func (r *Recorder) Retired(process int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retired[process]
}

// Pending is an invoked operation waiting for its completion.
type Pending struct {
	r     *Recorder
	index int // position of the invoke op
	op    Op
	done  bool
}

// Invoke records the invocation of f on key with value by process on behalf
// of client and returns the pending operation to complete. A process number
// never handed out by NextProcess is accepted and reserved.
func (r *Recorder) Invoke(process int, client, f, key string, value any) *Pending {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.retired[process] {
		panic(fmt.Sprintf("history: process %d is retired", process))
	}
	if r.active[process] {
		panic(fmt.Sprintf("history: process %d already has an operation outstanding", process))
	}
	if process >= r.nextProc {
		r.nextProc = process + 1
	}
	r.active[process] = true
	op := Op{Process: process, Client: client, Type: TypeInvoke, F: f, Key: key, Value: value, Time: r.Now()}
	r.ops = append(r.ops, op)
	return &Pending{r: r, index: len(r.ops) - 1, op: op}
}

// WithExtra attaches a metadata field to the invoke op and to the completion
// op, for example the cache tags of a read ("tags") that the staleness
// checker uses.
func (p *Pending) WithExtra(key string, value any) *Pending {
	p.r.mu.Lock()
	defer p.r.mu.Unlock()
	if p.op.Extra == nil {
		p.op.Extra = map[string]any{}
	}
	p.op.Extra[key] = value
	p.r.ops[p.index].Extra = cloneExtra(p.op.Extra)
	return p
}

// Op returns the recorded invoke op.
func (p *Pending) Op() Op { return p.op }

func (p *Pending) complete(typ string, value any, err error) Op {
	p.r.mu.Lock()
	defer p.r.mu.Unlock()
	if p.done {
		panic(fmt.Sprintf("history: process %d completed the same operation twice", p.op.Process))
	}
	p.done = true
	delete(p.r.active, p.op.Process)
	op := p.op
	op.Type = typ
	op.Value = value
	op.Extra = cloneExtra(p.op.Extra)
	if err != nil {
		op.Error = err.Error()
	}
	op.Time = p.r.Now()
	p.r.ops = append(p.r.ops, op)
	return op
}

// OK completes the operation with its result value.
func (p *Pending) OK(value any) Op { return p.complete(TypeOK, value, nil) }

// Fail completes the operation as definitely not applied. The invoke value is
// kept so checkers know which value must be absent.
func (p *Pending) Fail(err error) Op { return p.complete(TypeFail, p.op.Value, err) }

// Info completes the operation with an unknown outcome, retires the process,
// and returns the fresh process number that takes over for the client.
func (p *Pending) Info(err error) (op Op, fresh int) {
	op = p.complete(TypeInfo, p.op.Value, err)
	p.r.mu.Lock()
	defer p.r.mu.Unlock()
	p.r.retired[p.op.Process] = true
	return op, p.r.nextProcessLocked()
}

// Ops returns a copy of the history so far, in time order.
func (r *Recorder) Ops() []Op {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Op, len(r.ops))
	copy(out, r.ops)
	return out
}

// Len returns the number of recorded ops.
func (r *Recorder) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ops)
}

func cloneExtra(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// WriteJSONL writes the ops as JSON Lines (Appendix C), one object per line
// with deterministic member order.
func WriteJSONL(w io.Writer, ops []Op) error {
	bw := bufio.NewWriter(w)
	for i, op := range ops {
		b, err := json.Marshal(op, json.Deterministic(true))
		if err != nil {
			return fmt.Errorf("history: encode op %d: %w", i, err)
		}
		if _, err := bw.Write(b); err != nil {
			return err
		}
		if err := bw.WriteByte('\n'); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// ReadJSONL reads a JSON Lines history. Blank lines are skipped. Numbers in
// Value decode as float64; the models normalize them (see Normalize).
func ReadJSONL(r io.Reader) ([]Op, error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var ops []Op
	line := 0
	for sc.Scan() {
		line++
		raw := strings.TrimSpace(sc.Text())
		if raw == "" {
			continue
		}
		var op Op
		if err := json.Unmarshal([]byte(raw), &op); err != nil {
			return ops, fmt.Errorf("history: line %d: %w", line, err)
		}
		if op.Type != TypeInvoke && !op.IsReturn() {
			return ops, fmt.Errorf("history: line %d: unknown op type %q", line, op.Type)
		}
		ops = append(ops, op)
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return ops, fmt.Errorf("history: line %d too long: %w", line+1, err)
		}
		return ops, err
	}
	return ops, nil
}

// Window is a period during which a fault affected the cache tags listed
// (empty Tags means every tag). Kind names the fault, for reports. Times are
// nanoseconds from the run start, like Op.Time.
type Window struct {
	Start int64    `json:"start"`
	End   int64    `json:"end"`
	Tags  []string `json:"tags,omitempty"`
	Kind  string   `json:"kind,omitempty"`
}

// Overlaps reports whether the window intersects the closed interval
// [from, to] and covers at least one of the tags (or every tag when the
// window has none).
func (w Window) Overlaps(from, to int64, tags []string) bool {
	if w.End < from || w.Start > to {
		return false
	}
	if len(w.Tags) == 0 {
		return true
	}
	for _, t := range tags {
		for _, wt := range w.Tags {
			if t == wt {
				return true
			}
		}
	}
	return false
}

// Read is a completed cached read for the staleness check (I7).
type Read struct {
	Process int      `json:"process"`
	Key     string   `json:"key"`
	Tags    []string `json:"tags,omitempty"`
	Invoke  int64    `json:"invoke"`
	Return  int64    `json:"return"`
	// Value is the value returned; nil means not found.
	Value any `json:"value,omitempty"`
}

// Write is a write for the staleness check (I7). Definite is true for an ok
// write; an info write (unknown outcome) has Definite false and its value
// may appear at any later time.
type Write struct {
	Process  int      `json:"process"`
	Key      string   `json:"key"`
	Tags     []string `json:"tags,omitempty"`
	Invoke   int64    `json:"invoke"`
	Return   int64    `json:"return"`
	Value    any      `json:"value,omitempty"`
	Definite bool     `json:"definite"`
}
