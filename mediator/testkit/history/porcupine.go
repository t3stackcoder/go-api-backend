package history

import (
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
)

// Input is the Porcupine operation input built from an invoke op.
type Input struct {
	F     string
	Key   string
	Value any
}

// Output is the Porcupine operation output built from a completion op. Type
// is ok, fail, or info; Value is the result of an ok op (or the invoke value
// of a fail or info op) and Error the recorded error text.
type Output struct {
	Type  string
	Value any
	Error string
}

// DropFails is a drop function for ToPorcupine that removes every fail op:
// a failed operation had no effect, so it does not constrain the model.
func DropFails(op Op) bool { return op.Type == TypeFail }

// DropFailsExcept returns a drop function that removes fail ops unless their
// error text is one of the given values. The bank model keeps the
// insufficient-funds failure ("precondition_failed", mediator.CodePrecondition)
// because it is a definite, model-checkable outcome.
func DropFailsExcept(errors ...string) func(Op) bool {
	keep := map[string]bool{}
	for _, e := range errors {
		keep[e] = true
	}
	return func(op Op) bool { return op.Type == TypeFail && !keep[op.Error] }
}

// InsufficientFunds is the error text of a transfer that failed because the
// source account had too little money: mediator.CodePrecondition as recorded
// by the workload clients.
const InsufficientFunds = "precondition_failed"

// ToPorcupine pairs invoke and completion ops by process and converts them to
// Porcupine operations. An info op, and an invoke with no completion, gets a
// return time of math.MaxInt64 so the checker may linearize it at any later
// point (Appendix C). A completion op for which drop returns true is left
// out together with its invoke; a nil drop keeps everything. ClientId is the
// process number.
func ToPorcupine(ops []Op, drop func(Op) bool) []porcupine.Operation {
	sorted := make([]Op, len(ops))
	copy(sorted, ops)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Time < sorted[j].Time })
	pending := map[int]Op{}
	var out []porcupine.Operation
	for _, op := range sorted {
		switch {
		case op.Type == TypeInvoke:
			pending[op.Process] = op
		case op.IsReturn():
			inv, ok := pending[op.Process]
			if !ok {
				continue // completion without an invoke: unsound record, skipped
			}
			delete(pending, op.Process)
			if drop != nil && drop(op) {
				continue
			}
			ret := op.Time
			if op.Type == TypeInfo {
				ret = math.MaxInt64
			}
			out = append(out, porcupine.Operation{
				ClientId: op.Process,
				Input:    Input{F: inv.F, Key: inv.Key, Value: Normalize(inv.Value)},
				Call:     inv.Time,
				Output:   Output{Type: op.Type, Value: Normalize(op.Value), Error: op.Error},
				Return:   ret,
			})
		}
	}
	// Invokes that never completed are open at the end of the run.
	rest := make([]Op, 0, len(pending))
	for _, inv := range pending {
		rest = append(rest, inv)
	}
	sort.Slice(rest, func(i, j int) bool { return rest[i].Time < rest[j].Time })
	for _, inv := range rest {
		out = append(out, porcupine.Operation{
			ClientId: inv.Process,
			Input:    Input{F: inv.F, Key: inv.Key, Value: Normalize(inv.Value)},
			Call:     inv.Time,
			Output:   Output{Type: TypeInfo, Value: Normalize(inv.Value)},
			Return:   math.MaxInt64,
		})
	}
	return out
}

// partitionByKey groups operations by Input.Key so each key is checked
// independently (spec 11.6, register workload).
func partitionByKey(history []porcupine.Operation) [][]porcupine.Operation {
	byKey := map[string][]porcupine.Operation{}
	var keys []string
	for _, op := range history {
		k := op.Input.(Input).Key
		if _, ok := byKey[k]; !ok {
			keys = append(keys, k)
		}
		byKey[k] = append(byKey[k], op)
	}
	sort.Strings(keys)
	out := make([][]porcupine.Operation, 0, len(keys))
	for _, k := range keys {
		out = append(out, byKey[k])
	}
	return out
}

func describeOp(input, output any) string {
	in := input.(Input)
	out := output.(Output)
	var b strings.Builder
	b.WriteString(in.F)
	b.WriteString("(")
	b.WriteString(in.Key)
	if in.Value != nil {
		b.WriteString(", ")
		b.WriteString(Describe(in.Value))
	}
	b.WriteString(") -> ")
	switch out.Type {
	case TypeOK:
		b.WriteString(Describe(out.Value))
	case TypeInfo:
		b.WriteString("?")
	default:
		b.WriteString("fail")
		if out.Error != "" {
			b.WriteString(":" + out.Error)
		}
	}
	return b.String()
}

// RegisterModel is the per-key linearizable register of the register and
// remote-send workloads: set(v) writes, get reads the last written value or
// nil before any write. Histories are partitioned by key. An info set may
// have taken effect; an info get accepts any state.
func RegisterModel() porcupine.Model {
	return porcupine.Model{
		Partition: partitionByKey,
		Init:      func() any { return nil },
		Step: func(state, input, output any) (bool, any) {
			in := input.(Input)
			out := output.(Output)
			switch in.F {
			case FSet:
				if out.Type == TypeFail {
					return true, state
				}
				return true, in.Value
			case FGet:
				if out.Type != TypeOK {
					return true, state
				}
				return Equal(state, out.Value), state
			}
			return false, state
		},
		Equal:             Equal,
		DescribeOperation: describeOp,
		DescribeState:     func(state any) string { return Describe(state) },
	}
}

// MaxBankAccounts bounds the accounts of BankModel; the state is a fixed
// array so that it is comparable.
const MaxBankAccounts = 8

type bankState struct {
	n   int
	bal [MaxBankAccounts]int64
}

// TransferValue is the value recorded with a transfer op.
type TransferValue struct {
	From string `json:"from"`
	To   string `json:"to"`
	Amt  int64  `json:"amt"`
}

// AsTransfer decodes a transfer value recorded as TransferValue or read back
// from JSON as a map.
func AsTransfer(v any) (TransferValue, bool) {
	if t, ok := v.(TransferValue); ok {
		return t, true
	}
	m, ok := Normalize(v).(map[string]any)
	if !ok {
		return TransferValue{}, false
	}
	from, _ := m["from"].(string)
	to, _ := m["to"].(string)
	amt, ok := Int(m["amt"])
	if from == "" || to == "" || !ok {
		return TransferValue{}, false
	}
	return TransferValue{From: from, To: to, Amt: amt}, true
}

// BankModel is the bank of the bank workloads: accounts start at initial,
// transfer(from, to, amt) moves money when the source balance suffices and
// fails with InsufficientFunds otherwise, and read-all returns every balance.
// An info transfer may or may not have applied, which makes the model
// nondeterministic. Keep the insufficient-funds fails when converting
// (DropFailsExcept(InsufficientFunds)); drop other fails.
func BankModel(accounts []string, initial int64) porcupine.Model {
	if len(accounts) > MaxBankAccounts {
		panic("history: BankModel supports at most 8 accounts")
	}
	index := map[string]int{}
	for i, a := range accounts {
		index[a] = i
	}
	init := bankState{n: len(accounts)}
	for i := range accounts {
		init.bal[i] = initial
	}
	describe := func(s bankState) string {
		parts := make([]string, s.n)
		for i := 0; i < s.n; i++ {
			parts[i] = fmt.Sprintf("%s:%d", accounts[i], s.bal[i])
		}
		return "{" + strings.Join(parts, " ") + "}"
	}
	nm := porcupine.NondeterministicModel{
		Init: func() []any { return []any{init} },
		Step: func(state, input, output any) []any {
			s := state.(bankState)
			in := input.(Input)
			out := output.(Output)
			switch in.F {
			case FTransfer:
				t, ok := AsTransfer(in.Value)
				if !ok {
					return nil
				}
				fi, okf := index[t.From]
				ti, okt := index[t.To]
				if !okf || !okt || t.Amt <= 0 {
					return nil
				}
				enough := s.bal[fi] >= t.Amt
				applied := s
				applied.bal[fi] -= t.Amt
				applied.bal[ti] += t.Amt
				switch out.Type {
				case TypeOK:
					if !enough {
						return nil
					}
					return []any{applied}
				case TypeFail:
					if enough {
						return nil
					}
					return []any{s}
				default: // info: applied or not
					if enough {
						return []any{s, applied}
					}
					return []any{s}
				}
			case FReadAll:
				if out.Type != TypeOK {
					return []any{s}
				}
				m, ok := Normalize(out.Value).(map[string]any)
				if !ok {
					return nil
				}
				if bal, ok := m["balances"]; ok {
					m, _ = bal.(map[string]any)
				}
				if len(m) != s.n {
					return nil
				}
				for a, i := range index {
					got, ok := Int(m[a])
					if !ok || got != s.bal[i] {
						return nil
					}
				}
				return []any{s}
			}
			return nil
		},
		Equal:             func(a, b any) bool { return a.(bankState) == b.(bankState) },
		DescribeOperation: describeOp,
		DescribeState:     func(state any) string { return describe(state.(bankState)) },
	}
	return nm.ToModel()
}

// Result is the outcome of Check.
type Result struct {
	// Check is Ok, Illegal, or Unknown (timed out).
	Check porcupine.CheckResult
	// Linearizable is true only for Ok.
	Linearizable bool
	// Info supports visualization of the (partial) linearization.
	Info porcupine.LinearizationInfo
	// HTML is the path of the visualization written for a failure, if any.
	HTML string
}

// Check runs the Porcupine checker on ops with the model. A timeout of zero
// is unlimited. When the history is not linearizable (or the check timed
// out) and htmlPath is not empty, the visualization is written there and
// its path is returned in Result.HTML; a write failure is returned as the
// error, the check result is still valid.
func Check(model porcupine.Model, ops []porcupine.Operation, timeout time.Duration, htmlPath string) (Result, error) {
	res, info := porcupine.CheckOperationsVerbose(model, ops, timeout)
	r := Result{Check: res, Linearizable: res == porcupine.Ok, Info: info}
	if r.Linearizable || htmlPath == "" {
		return r, nil
	}
	if err := VisualizeHTML(model, info, htmlPath); err != nil {
		return r, err
	}
	r.HTML = htmlPath
	return r, nil
}

// VisualizeHTML writes Porcupine's HTML visualization of a check to path.
func VisualizeHTML(model porcupine.Model, info porcupine.LinearizationInfo, path string) error {
	f, err := os.Create(path) //nolint:gosec // path comes from the harness
	if err != nil {
		return fmt.Errorf("history: visualization: %w", err)
	}
	if err := porcupine.Visualize(model, info, f); err != nil {
		_ = f.Close()
		return fmt.Errorf("history: visualization: %w", err)
	}
	return f.Close()
}
