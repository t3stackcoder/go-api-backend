package history

import (
	"bytes"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func TestRecorder_RetiresProcessOnInfo(t *testing.T) {
	r := NewRecorder()
	p := r.NextProcess()
	if p != 0 || r.NextProcess() != 1 {
		t.Fatalf("process numbers must start at 0 and increase")
	}
	pend := r.Invoke(p, "node1", FSet, "k", 41)
	op, fresh := pend.Info(errors.New("timeout"))
	if op.Type != TypeInfo || op.Error != "timeout" || op.Value != 41 {
		t.Fatalf("info op: %+v", op)
	}
	if fresh != 2 || !r.Retired(p) || r.Retired(fresh) {
		t.Fatalf("fresh=%d retired(p)=%v", fresh, r.Retired(p))
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("invoke on a retired process must panic")
			}
		}()
		r.Invoke(p, "node1", FGet, "k", nil)
	}()
	ops := r.Ops()
	if len(ops) != 2 || ops[0].Type != TypeInvoke || ops[1].Type != TypeInfo || ops[1].Time < ops[0].Time {
		t.Fatalf("ops: %+v", ops)
	}
	if r.Len() != 2 {
		t.Fatal("Len")
	}
	// The copy does not alias the recorder.
	ops[0].Key = "changed"
	if r.Ops()[0].Key != "k" {
		t.Fatal("Ops must return a copy")
	}
}

func TestRecorder_OKFailAndExtra(t *testing.T) {
	r := NewRecorder()
	p := r.Invoke(7, "c", FGet, "k", nil).WithExtra("tags", []string{"t1"})
	if p.Op().Extra["tags"] == nil {
		t.Fatal("extra on invoke")
	}
	ok := p.OK(int64(3))
	if ok.Type != TypeOK || ok.Value != int64(3) || ok.Extra["tags"] == nil || ok.Process != 7 {
		t.Fatalf("ok op: %+v", ok)
	}
	// A process number handed out by the caller is reserved.
	if n := r.NextProcess(); n != 8 {
		t.Fatalf("NextProcess after Invoke(7) = %d", n)
	}
	f := r.Invoke(7, "c", FAppend, "k", 5).Fail(errors.New("nope"))
	if f.Type != TypeFail || f.Value != 5 || f.Error != "nope" {
		t.Fatalf("fail op: %+v", f)
	}
	// Fail without an error keeps Error empty.
	if op := r.Invoke(7, "c", FAppend, "k", 6).Fail(nil); op.Error != "" {
		t.Fatal("nil error")
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("double completion must panic")
			}
		}()
		p.OK(nil)
	}()
	pend := r.Invoke(9, "c", FGet, "k", nil)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("two outstanding ops on one process must panic")
			}
		}()
		r.Invoke(9, "c", FGet, "k", nil)
	}()
	pend.OK(nil)
	if r.Now() < 0 {
		t.Fatal("clock")
	}
}

func TestJSONL_RoundTrip(t *testing.T) {
	r := NewRecorder()
	p := r.NextProcess()
	r.Invoke(p, "node2", FSet, "k3", 41).OK(nil)
	r.Invoke(p, "node2", FGet, "k3", nil).WithExtra("tags", []string{"wl:register:k3"}).OK(41)
	r.Invoke(p, "node1", FTransfer, "", TransferValue{From: "a", To: "b", Amt: 5}).Info(errors.New("timeout"))
	ops := r.Ops()
	var buf bytes.Buffer
	if err := WriteJSONL(&buf, ops); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"type":"invoke"`) || strings.Count(buf.String(), "\n") != len(ops) {
		t.Fatalf("jsonl:\n%s", buf.String())
	}
	back, err := ReadJSONL(strings.NewReader(buf.String() + "\n\n"))
	if err != nil {
		t.Fatal(err)
	}
	if len(back) != len(ops) {
		t.Fatalf("read %d ops, want %d", len(back), len(ops))
	}
	for i := range ops {
		a, b := ops[i], back[i]
		if a.Process != b.Process || a.Client != b.Client || a.Type != b.Type || a.F != b.F || a.Key != b.Key || a.Error != b.Error || a.Time != b.Time {
			t.Fatalf("op %d: %+v != %+v", i, a, b)
		}
		if !Equal(a.Value, b.Value) {
			t.Fatalf("op %d value: %v != %v", i, a.Value, b.Value)
		}
	}
	if tags := Strings(back[2].Extra["tags"]); len(tags) != 1 || tags[0] != "wl:register:k3" {
		t.Fatalf("extra: %v", back[2].Extra)
	}
	if tr, ok := AsTransfer(back[4].Value); !ok || tr != (TransferValue{From: "a", To: "b", Amt: 5}) {
		t.Fatalf("transfer: %v", back[4].Value)
	}
	// Appendix C example lines parse.
	example := `{"process": 7, "client": "node2", "type": "invoke", "f": "set", "key": "k3", "value": 41, "time": 1203004000}
{"process": 8, "client": "node1", "type": "info",   "f": "get", "key": "k3", "error": "timeout", "time": 1240000000}`
	ex, err := ReadJSONL(strings.NewReader(example))
	if err != nil || len(ex) != 2 || ex[1].Error != "timeout" || ex[0].Time != 1203004000 {
		t.Fatalf("appendix C: %v %+v", err, ex)
	}
}

func TestJSONL_Errors(t *testing.T) {
	if _, err := ReadJSONL(strings.NewReader("{not json")); err == nil {
		t.Fatal("bad json")
	}
	if _, err := ReadJSONL(strings.NewReader(`{"process":1,"type":"bogus","f":"x","time":1}`)); err == nil {
		t.Fatal("bad type")
	}
	long := strings.Repeat("x", 17*1024*1024)
	if _, err := ReadJSONL(strings.NewReader(`{"process":1,"type":"ok","f":"` + long + `","time":1}`)); err == nil {
		t.Fatal("too long")
	}
	if err := WriteJSONL(failWriter{}, []Op{{Type: TypeOK}}); err == nil {
		t.Fatal("write error")
	}
	if err := WriteJSONL(&bytes.Buffer{}, []Op{{Type: TypeOK, Value: make(chan int)}}); err == nil {
		t.Fatal("encode error")
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestValues(t *testing.T) {
	type named struct {
		A int    `json:"a"`
		B string `json:"-"`
		C bool
		d int
	}
	cases := []struct {
		in   any
		want any
	}{
		{nil, nil}, {"s", "s"}, {true, true}, {int8(1), int64(1)}, {int16(1), int64(1)}, {int32(1), int64(1)},
		{uint(2), int64(2)}, {uint8(2), int64(2)}, {uint16(2), int64(2)}, {uint32(2), int64(2)}, {uint64(2), int64(2)},
		{float32(3), int64(3)}, {2.5, 2.5}, {[]int{1}, []any{int64(1)}}, {[]any{1.0}, []any{int64(1)}},
		{map[string]any{"x": 1}, map[string]any{"x": int64(1)}}, {map[int]int{1: 2}, map[string]any{"1": int64(2)}},
		{[]int64(nil), []any{}}, {[2]int{1, 2}, []any{int64(1), int64(2)}},
		{named{A: 1, B: "b", C: true, d: 4}, map[string]any{"a": int64(1), "C": true}},
		{&named{A: 2}, map[string]any{"a": int64(2), "C": false}}, {(*named)(nil), nil},
		{complex(1, 2), "(1+2i)"},
	}
	for _, c := range cases {
		if !Equal(Normalize(c.in), c.want) {
			t.Errorf("Normalize(%#v) = %#v, want %#v", c.in, Normalize(c.in), c.want)
		}
	}
	if n, ok := Int(4.0); !ok || n != 4 {
		t.Fatal("Int float")
	}
	if n, ok := Int("12"); !ok || n != 12 {
		t.Fatal("Int string")
	}
	if _, ok := Int(2.5); ok {
		t.Fatal("Int fraction")
	}
	if _, ok := Int("x"); ok {
		t.Fatal("Int bad string")
	}
	if _, ok := Int(nil); ok {
		t.Fatal("Int nil")
	}
	if l, ok := Ints(nil); !ok || len(l) != 0 {
		t.Fatal("Ints nil")
	}
	if l, ok := Ints([]float64{1, 2}); !ok || l[1] != 2 {
		t.Fatal("Ints floats")
	}
	if _, ok := Ints("no"); ok {
		t.Fatal("Ints string")
	}
	if _, ok := Ints([]any{"x"}); ok {
		t.Fatal("Ints element")
	}
	if s := Strings("one"); len(s) != 1 || s[0] != "one" {
		t.Fatal("Strings string")
	}
	if s := Strings([]any{"a", 1}); len(s) != 2 || s[1] != "1" {
		t.Fatal("Strings list")
	}
	if Strings(nil) != nil || Strings(5) != nil {
		t.Fatal("Strings nil")
	}
	if Describe(nil) != "nil" || Describe("s") != `"s"` || Describe(int64(3)) != "3" {
		t.Fatal("Describe scalars")
	}
	if got := Describe(map[string]any{"b": 1, "a": []int{1, 2}}); got != "{a:[1 2] b:1}" {
		t.Fatalf("Describe map = %s", got)
	}
}

func TestToPorcupine(t *testing.T) {
	ops := []Op{
		{Process: 0, Type: TypeInvoke, F: FSet, Key: "k", Value: 1, Time: 10},
		{Process: 1, Type: TypeInvoke, F: FGet, Key: "k", Time: 12},
		{Process: 0, Type: TypeOK, F: FSet, Key: "k", Value: 1, Time: 20},
		{Process: 1, Type: TypeInfo, F: FGet, Key: "k", Time: 30},
		{Process: 2, Type: TypeInvoke, F: FSet, Key: "k", Value: 2, Time: 31},
		{Process: 2, Type: TypeFail, F: FSet, Key: "k", Value: 2, Error: "unavailable", Time: 32},
		{Process: 3, Type: TypeOK, F: FSet, Key: "k", Time: 33}, // no invoke: skipped
		{Process: 4, Type: TypeInvoke, F: FGet, Key: "k", Time: 40},
	}
	all := ToPorcupine(ops, nil)
	if len(all) != 4 {
		t.Fatalf("got %d operations, want 4", len(all))
	}
	if out, ok := all[1].Output.(Output); !ok || all[1].Return != math.MaxInt64 || out.Type != TypeInfo {
		t.Fatalf("info op: %+v", all[1])
	}
	if all[3].Return != math.MaxInt64 || all[3].Call != 40 {
		t.Fatalf("open invoke: %+v", all[3])
	}
	if in, ok := all[0].Input.(Input); !ok || in.Value != int64(1) || all[0].ClientId != 0 || all[0].Call != 10 || all[0].Return != 20 {
		t.Fatalf("set op: %+v", all[0])
	}
	dropped := ToPorcupine(ops, DropFails)
	if len(dropped) != 3 {
		t.Fatalf("DropFails left %d", len(dropped))
	}
	keep := ToPorcupine(ops, DropFailsExcept("unavailable"))
	if len(keep) != 4 {
		t.Fatalf("DropFailsExcept left %d", len(keep))
	}
	if d := describeOp(all[2].Input, all[2].Output); d != "set(k, 2) -> fail:unavailable" {
		t.Fatalf("describe fail: %s", d)
	}
	if d := describeOp(all[1].Input, all[1].Output); d != "get(k) -> ?" {
		t.Fatalf("describe info: %s", d)
	}
}

func TestRegisterModel(t *testing.T) {
	model := RegisterModel()
	good := []Op{
		{Process: 0, Type: TypeInvoke, F: FGet, Key: "a", Time: 1},
		{Process: 0, Type: TypeOK, F: FGet, Key: "a", Value: nil, Time: 2},
		{Process: 0, Type: TypeInvoke, F: FSet, Key: "a", Value: 1, Time: 10},
		{Process: 1, Type: TypeInvoke, F: FGet, Key: "a", Time: 11},
		{Process: 1, Type: TypeOK, F: FGet, Key: "a", Value: 1.0, Time: 15}, // concurrent with the set
		{Process: 0, Type: TypeOK, F: FSet, Key: "a", Value: 1, Time: 20},
		{Process: 2, Type: TypeInvoke, F: FSet, Key: "b", Value: 9, Time: 21},
		{Process: 2, Type: TypeInfo, F: FSet, Key: "b", Value: 9, Time: 25},
		{Process: 3, Type: TypeInvoke, F: FGet, Key: "b", Time: 30},
		{Process: 3, Type: TypeOK, F: FGet, Key: "b", Value: 9, Time: 31}, // the info set took effect
		{Process: 4, Type: TypeInvoke, F: FGet, Key: "a", Time: 40},
		{Process: 4, Type: TypeInfo, F: FGet, Key: "a", Time: 41},
		{Process: 5, Type: TypeInvoke, F: FSet, Key: "a", Value: 7, Time: 42},
		{Process: 5, Type: TypeFail, F: FSet, Key: "a", Value: 7, Time: 43},
	}
	res, err := Check(model, ToPorcupine(good, nil), 0, "")
	if err != nil || !res.Linearizable {
		t.Fatalf("good history: %v %v\n%s", res.Check, err, describeAll(good))
	}
	// Classic anomaly: a read that starts after a write returned sees the old value.
	bad := []Op{
		{Process: 0, Type: TypeInvoke, F: FSet, Key: "a", Value: 1, Time: 1},
		{Process: 0, Type: TypeOK, F: FSet, Key: "a", Value: 1, Time: 2},
		{Process: 1, Type: TypeInvoke, F: FGet, Key: "a", Time: 3},
		{Process: 1, Type: TypeOK, F: FGet, Key: "a", Value: nil, Time: 4},
	}
	html := t.TempDir() + "/porcupine.html"
	res, err = Check(model, ToPorcupine(bad, DropFails), 0, html)
	if err != nil || res.Linearizable || res.HTML != html {
		t.Fatalf("bad history: %v %v html=%q", res.Check, err, res.HTML)
	}
	// Stale read of a value from before the latest write (different key must not interfere).
	stale := []Op{
		{Process: 0, Type: TypeInvoke, F: FSet, Key: "a", Value: 1, Time: 1},
		{Process: 0, Type: TypeOK, F: FSet, Key: "a", Value: 1, Time: 2},
		{Process: 0, Type: TypeInvoke, F: FSet, Key: "a", Value: 2, Time: 3},
		{Process: 0, Type: TypeOK, F: FSet, Key: "a", Value: 2, Time: 4},
		{Process: 1, Type: TypeInvoke, F: FSet, Key: "b", Value: 1, Time: 4},
		{Process: 1, Type: TypeOK, F: FSet, Key: "b", Value: 1, Time: 5},
		{Process: 2, Type: TypeInvoke, F: FGet, Key: "a", Time: 6},
		{Process: 2, Type: TypeOK, F: FGet, Key: "a", Value: 1, Time: 7},
	}
	if res, _ := Check(model, ToPorcupine(stale, nil), 0, ""); res.Linearizable {
		t.Fatal("stale read must not be linearizable")
	}
	// An unknown function is rejected by the model.
	odd := []Op{{Process: 0, Type: TypeInvoke, F: "weird", Key: "a", Time: 1}, {Process: 0, Type: TypeOK, F: "weird", Key: "a", Time: 2}}
	if res, _ := Check(model, ToPorcupine(odd, nil), 0, ""); res.Linearizable {
		t.Fatal("unknown op must not be accepted")
	}
	if model.DescribeState(int64(3)) != "3" {
		t.Fatal("describe state")
	}
	// A visualization failure is reported but does not change the result.
	res, err = Check(model, ToPorcupine(bad, DropFails), 0, t.TempDir()+"/missing/dir/x.html")
	if err == nil || res.Linearizable {
		t.Fatal("bad html path must error")
	}
}

func describeAll(ops []Op) string {
	var b strings.Builder
	for _, op := range ToPorcupine(ops, nil) {
		b.WriteString(describeOp(op.Input, op.Output))
		b.WriteString("\n")
	}
	return b.String()
}

func TestBankModel(t *testing.T) {
	accounts := []string{"a", "b", "c", "d"}
	model := BankModel(accounts, 100)
	balances := func(a, b, c, d int64) map[string]any {
		return map[string]any{"balances": map[string]int64{"a": a, "b": b, "c": c, "d": d}, "total": a + b + c + d}
	}
	good := []Op{
		{Process: 0, Type: TypeInvoke, F: FTransfer, Value: TransferValue{From: "a", To: "b", Amt: 30}, Time: 1},
		{Process: 0, Type: TypeOK, F: FTransfer, Value: TransferValue{From: "a", To: "b", Amt: 30}, Time: 2},
		{Process: 1, Type: TypeInvoke, F: FTransfer, Value: TransferValue{From: "a", To: "c", Amt: 80}, Time: 3},
		{Process: 1, Type: TypeFail, F: FTransfer, Value: TransferValue{From: "a", To: "c", Amt: 80}, Error: InsufficientFunds, Time: 4},
		{Process: 2, Type: TypeInvoke, F: FTransfer, Value: TransferValue{From: "c", To: "d", Amt: 10}, Time: 5},
		{Process: 2, Type: TypeInfo, F: FTransfer, Value: TransferValue{From: "c", To: "d", Amt: 10}, Error: "timeout", Time: 6},
		{Process: 3, Type: TypeInvoke, F: FReadAll, Time: 7},
		{Process: 3, Type: TypeOK, F: FReadAll, Value: balances(70, 130, 90, 110), Time: 8}, // info transfer applied
		{Process: 4, Type: TypeInvoke, F: FTransfer, Value: TransferValue{From: "b", To: "a", Amt: 5}, Time: 9},
		{Process: 4, Type: TypeFail, F: FTransfer, Value: TransferValue{From: "b", To: "a", Amt: 5}, Error: "unavailable", Time: 10},
		{Process: 5, Type: TypeInvoke, F: FReadAll, Time: 11},
		{Process: 5, Type: TypeInfo, F: FReadAll, Time: 12},
	}
	res, err := Check(model, ToPorcupine(good, DropFailsExcept(InsufficientFunds)), 0, "")
	if err != nil || !res.Linearizable {
		t.Fatalf("good bank history: %v %v", res.Check, err)
	}
	if vs := Conservation(good, 400); len(vs) != 0 {
		t.Fatalf("conservation: %v", vs)
	}
	// Money created out of nothing.
	bad := []Op{
		{Process: 0, Type: TypeInvoke, F: FReadAll, Time: 1},
		{Process: 0, Type: TypeOK, F: FReadAll, Value: balances(100, 100, 100, 110), Time: 2},
	}
	if res, _ := Check(model, ToPorcupine(bad, nil), 0, ""); res.Linearizable {
		t.Fatal("bad read-all must not be linearizable")
	}
	if vs := Conservation(bad, 400); len(vs) != 1 || vs[0].ID != IDConservation {
		t.Fatalf("conservation: %v", vs)
	}
	// An ok transfer without funds, and a fail with funds, are both illegal.
	broke := []Op{
		{Process: 0, Type: TypeInvoke, F: FTransfer, Value: TransferValue{From: "a", To: "b", Amt: 500}, Time: 1},
		{Process: 0, Type: TypeOK, F: FTransfer, Value: TransferValue{From: "a", To: "b", Amt: 500}, Time: 2},
	}
	if res, _ := Check(model, ToPorcupine(broke, nil), 0, ""); res.Linearizable {
		t.Fatal("overdraft must not be linearizable")
	}
	rich := []Op{
		{Process: 0, Type: TypeInvoke, F: FTransfer, Value: TransferValue{From: "a", To: "b", Amt: 5}, Time: 1},
		{Process: 0, Type: TypeFail, F: FTransfer, Value: TransferValue{From: "a", To: "b", Amt: 5}, Error: InsufficientFunds, Time: 2},
	}
	if res, _ := Check(model, ToPorcupine(rich, nil), 0, ""); res.Linearizable {
		t.Fatal("spurious insufficient funds must not be linearizable")
	}
	// Malformed values are rejected rather than accepted.
	for _, ops := range [][]Op{
		{{Process: 0, Type: TypeInvoke, F: FTransfer, Value: "junk", Time: 1}, {Process: 0, Type: TypeOK, F: FTransfer, Value: "junk", Time: 2}},
		{{Process: 0, Type: TypeInvoke, F: FTransfer, Value: TransferValue{From: "zz", To: "b", Amt: 5}, Time: 1}, {Process: 0, Type: TypeOK, F: FTransfer, Time: 2}},
		{{Process: 0, Type: TypeInvoke, F: FReadAll, Time: 1}, {Process: 0, Type: TypeOK, F: FReadAll, Value: "junk", Time: 2}},
		{{Process: 0, Type: TypeInvoke, F: FReadAll, Time: 1}, {Process: 0, Type: TypeOK, F: FReadAll, Value: map[string]any{"a": 1}, Time: 2}},
		{{Process: 0, Type: TypeInvoke, F: FReadAll, Time: 1}, {Process: 0, Type: TypeOK, F: FReadAll, Value: map[string]any{"a": "x", "b": 1, "c": 1, "d": 1}, Time: 2}},
		{{Process: 0, Type: TypeInvoke, F: FGet, Key: "a", Time: 1}, {Process: 0, Type: TypeOK, F: FGet, Key: "a", Time: 2}},
	} {
		if res, _ := Check(model, ToPorcupine(ops, nil), 0, ""); res.Linearizable {
			t.Fatalf("malformed history accepted: %+v", ops)
		}
	}
	// Info transfer without funds cannot have applied.
	poor := []Op{
		{Process: 0, Type: TypeInvoke, F: FTransfer, Value: TransferValue{From: "a", To: "b", Amt: 500}, Time: 1},
		{Process: 0, Type: TypeInfo, F: FTransfer, Value: TransferValue{From: "a", To: "b", Amt: 500}, Time: 2},
		{Process: 1, Type: TypeInvoke, F: FReadAll, Time: 3},
		{Process: 1, Type: TypeOK, F: FReadAll, Value: balances(100, 100, 100, 100), Time: 4},
	}
	if res, _ := Check(model, ToPorcupine(poor, nil), 0, ""); !res.Linearizable {
		t.Fatal("unapplied poor info transfer must be linearizable")
	}
	if got := model.DescribeState([]any{bankState{n: 2, bal: [MaxBankAccounts]int64{1, 2}}}); got != "{{a:1 b:2}}" {
		t.Fatalf("describe state: %s", got)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("too many accounts must panic")
			}
		}()
		BankModel(make([]string, MaxBankAccounts+1), 1)
	}()
	if vs := Conservation([]Op{{Type: TypeOK, F: FReadAll, Value: "junk"}, {Type: TypeOK, F: FReadAll, Value: map[string]any{"a": "x"}}}, 0); len(vs) != 2 {
		t.Fatalf("conservation malformed: %v", vs)
	}
	if _, ok := AsTransfer(map[string]any{"from": "a"}); ok {
		t.Fatal("AsTransfer incomplete")
	}
}

func TestAppendListChecker(t *testing.T) {
	seq := func(process int, key string, vals ...int) []Op {
		var ops []Op
		for i, v := range vals {
			ops = append(ops,
				Op{Process: process, Type: TypeInvoke, F: FAppend, Key: key, Value: v, Time: int64(100*process + 2*i)},
				Op{Process: process, Type: TypeOK, F: FAppend, Key: key, Value: v, Time: int64(100*process + 2*i + 1)})
		}
		return ops
	}
	final := func(key string, list []int64, at int64) []Op {
		return []Op{
			{Process: 99, Type: TypeInvoke, F: FReadList, Key: key, Time: at},
			{Process: 99, Type: TypeOK, F: FReadList, Key: key, Value: list, Time: at + 1},
		}
	}
	var good []Op
	good = append(good, seq(0, "k", 1, 2, 3)...)
	good = append(good, seq(1, "k", 10, 20)...)
	good = append(good,
		Op{Process: 2, Type: TypeInvoke, F: FAppend, Key: "k", Value: 7, Time: 300},
		Op{Process: 2, Type: TypeFail, F: FAppend, Key: "k", Value: 7, Time: 301},
		Op{Process: 3, Type: TypeInvoke, F: FAppend, Key: "k", Value: 8, Time: 302},
		Op{Process: 3, Type: TypeInfo, F: FAppend, Key: "k", Value: 8, Time: 303},
		Op{Process: 4, Type: TypeInvoke, F: FAppend, Key: "k", Value: 9, Time: 304}, // never returned
		Op{Process: 5, Type: TypeInvoke, F: FReadList, Key: "k", Time: 305},
		Op{Process: 5, Type: TypeInfo, F: FReadList, Key: "k", Time: 306},
	)
	good = append(good, final("k", []int64{1, 10, 8, 2, 20, 3}, 400)...)
	if vs := AppendListChecker(good); len(vs) != 0 {
		t.Fatalf("good: %v", vs)
	}
	cases := map[string]struct {
		list []int64
		msg  string
	}{
		"duplicate ok":    {[]int64{1, 1, 2, 3, 10, 20}, "exactly once"},
		"missing ok":      {[]int64{1, 2, 10, 20}, "exactly once"},
		"failed present":  {[]int64{1, 2, 3, 10, 20, 7}, "must be absent"},
		"info twice":      {[]int64{1, 2, 3, 10, 20, 8, 8}, "at most once"},
		"unknown value":   {[]int64{1, 2, 3, 10, 20, 42}, "no append produced"},
		"reordered":       {[]int64{2, 1, 3, 10, 20}, "interleaving"},
		"reordered other": {[]int64{1, 2, 3, 20, 10}, "interleaving"},
	}
	for name, c := range cases {
		var ops []Op
		ops = append(ops, good[:len(good)-2]...)
		ops = append(ops, final("k", c.list, 400)...)
		vs := AppendListChecker(ops)
		if len(vs) == 0 {
			t.Fatalf("%s: no violation", name)
		}
		found := false
		for _, v := range vs {
			if v.ID == IDAppend && strings.Contains(v.Msg, c.msg) {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: want %q, got %v", name, c.msg, vs)
		}
	}
	// No final read, non-integer values.
	var none []Op
	none = append(none, seq(0, "k", 1)...)
	if vs := AppendListChecker(none); len(vs) != 1 || !strings.Contains(vs[0].Msg, "no final read") {
		t.Fatalf("no final: %v", vs)
	}
	junk := []Op{
		{Process: 0, Type: TypeInvoke, F: FAppend, Key: "k", Value: "x", Time: 1},
		{Process: 0, Type: TypeOK, F: FAppend, Key: "k", Value: "x", Time: 2},
		{Process: 1, Type: TypeInvoke, F: FReadList, Key: "k", Time: 3},
		{Process: 1, Type: TypeOK, F: FReadList, Key: "k", Value: "x", Time: 4},
		{Process: 2, Type: TypeOK, F: FAppend, Key: "k", Value: 1, Time: 5}, // no invoke
	}
	if vs := AppendListChecker(junk); len(vs) != 2 {
		t.Fatalf("junk: %v", vs)
	}
}

func TestBoundedStaleness(t *testing.T) {
	ops := []Op{
		{Process: 0, Type: TypeInvoke, F: FSet, Key: "k", Value: 1, Time: 10},
		{Process: 0, Type: TypeOK, F: FSet, Key: "k", Value: 1, Time: 20},
		{Process: 0, Type: TypeInvoke, F: FSet, Key: "k", Value: 2, Time: 30},
		{Process: 0, Type: TypeOK, F: FSet, Key: "k", Value: 2, Time: 40},
		{Process: 1, Type: TypeInvoke, F: FGet, Key: "k", Time: 50},
		{Process: 1, Type: TypeOK, F: FGet, Key: "k", Value: 1, Time: 60}, // stale
		{Process: 2, Type: TypeInvoke, F: FGet, Key: "k", Time: 35},
		{Process: 2, Type: TypeOK, F: FGet, Key: "k", Value: 1, Time: 45}, // concurrent with the second set: fine
		{Process: 3, Type: TypeInvoke, F: FSet, Key: "k", Value: 3, Time: 70},
		{Process: 3, Type: TypeInfo, F: FSet, Key: "k", Value: 3, Time: 75},
		{Process: 4, Type: TypeInvoke, F: FGet, Key: "k", Time: 80},
		{Process: 4, Type: TypeOK, F: FGet, Key: "k", Value: 3, Time: 85}, // info write visible: fine
		{Process: 5, Type: TypeInvoke, F: FGet, Key: "k", Time: 90},
		{Process: 5, Type: TypeOK, F: FGet, Key: "k", Value: nil, Time: 95}, // not found after writes: stale
		{Process: 6, Type: TypeInvoke, F: FGet, Key: "other", Time: 96},
		{Process: 6, Type: TypeOK, F: FGet, Key: "other", Value: nil, Time: 97}, // other key, no writes: fine
		{Process: 7, Type: TypeInvoke, F: FSet, Key: "k", Value: 4, Time: 98},   // never returned
	}
	vs := BoundedStalenessChecker(ops, nil, 0)
	if len(vs) != 2 || vs[0].ID != IDStaleness || vs[0].Details["process"] != 1 || vs[1].Details["process"] != 5 {
		t.Fatalf("no windows: %v", vs)
	}
	// A degraded window for the key's tag within the TTL excuses the first read only.
	windows := []Window{{Start: 42, End: 44, Tags: []string{"k"}, Kind: "partition-redis"}}
	vs = BoundedStalenessChecker(ops, windows, 10)
	if len(vs) != 1 || vs[0].Details["process"] != 5 {
		t.Fatalf("with window: %v", vs)
	}
	// A window for another tag does not excuse anything; a tagless window excuses everything it overlaps.
	if vs := BoundedStalenessChecker(ops, []Window{{Start: 42, End: 44, Tags: []string{"z"}}}, 10); len(vs) != 2 {
		t.Fatalf("other tag: %v", vs)
	}
	if vs := BoundedStalenessChecker(ops, []Window{{Start: 0, End: 100}}, 0); len(vs) != 0 {
		t.Fatalf("tagless: %v", vs)
	}
	// Reads carry their tags from Extra.
	tagged := []Op{
		{Process: 0, Type: TypeInvoke, F: FSet, Key: "k", Value: 1, Time: 1},
		{Process: 0, Type: TypeOK, F: FSet, Key: "k", Value: 1, Time: 2},
		{Process: 1, Type: TypeInvoke, F: FGet, Key: "k", Time: 3, Extra: map[string]any{"tags": []string{"wl:register:k"}}},
		{Process: 1, Type: TypeOK, F: FGet, Key: "k", Value: nil, Time: 4},
	}
	if vs := BoundedStalenessChecker(tagged, []Window{{Start: 3, End: 3, Tags: []string{"wl:register:k"}}}, 0); len(vs) != 0 {
		t.Fatalf("tagged window: %v", vs)
	}
	reads, writes := ReadsAndWrites(ops)
	if len(reads) != 5 || len(writes) != 4 || writes[2].Definite || writes[2].Return != math.MaxInt64 || writes[3].Return != math.MaxInt64 {
		t.Fatalf("reads=%d writes=%+v", len(reads), writes)
	}
	if (Window{Start: 5, End: 6}).Overlaps(7, 8, nil) || !(Window{Start: 5, End: 7}).Overlaps(7, 8, nil) {
		t.Fatal("Overlaps")
	}
}

func TestLogScan(t *testing.T) {
	lines := []string{
		"time=1 level=INFO msg=started goroutines=40",
		"time=2 level=ERROR msg=\"panic: runtime error: index out of range\"",
		"time=3 level=WARN msg=\"invariant violated: seq gap\"",
		"time=4 level=ERROR msg=\"conn busy\"",
		"WARNING: DATA RACE",
		"time=5 level=INFO msg=quiet goroutines=41",
		"time=6 level=INFO msg=end goroutines=200",
	}
	vs := LogScan(lines)
	if len(vs) != 5 {
		t.Fatalf("got %d violations: %v", len(vs), vs)
	}
	for i, p := range []string{"panic", "invariant violated", "conn busy", "DATA RACE"} {
		if vs[i].Details["pattern"] != p || vs[i].Details["line"] != i+2 {
			t.Fatalf("violation %d: %v", i, vs[i])
		}
	}
	if !strings.Contains(vs[4].Msg, "goroutine") || vs[4].Details["baseline"] != 40 || vs[4].Details["final"] != 200 {
		t.Fatalf("growth: %v", vs[4])
	}
	if vs := LogScanWith(lines[5:7], LogScanOptions{GoroutineSlack: 500}); len(vs) != 0 {
		t.Fatalf("slack: %v", vs)
	}
	if vs := LogScanWith([]string{"all fine goroutines=10", "goroutines=12"}, LogScanOptions{Patterns: []string{"fine"}}); len(vs) != 1 {
		t.Fatalf("custom pattern: %v", vs)
	}
	if vs := LogScan([]string{"goroutines=abc", "nothing"}); len(vs) != 0 {
		t.Fatalf("clean: %v", vs)
	}
}

func TestFencingFromLogs(t *testing.T) {
	good := []string{
		"time=2026-09-26T10:00:00Z level=DEBUG msg=applied fencing=1 partition=0 group=read-model node=n1 topic=wl.bumped",
		"time=2026-09-26T10:00:01Z level=DEBUG msg=applied fencing=1 partition=0 group=read-model node=n1 topic=wl.bumped",
		"time=2026-09-26T10:00:02Z level=DEBUG msg=applied fencing=3 partition=0 group=\"read-model\" node=\"n2\" topic=wl.bumped",
		"time=2026-09-26T10:00:03Z level=DEBUG msg=applied fencing=2 partition=1 group=read-model node=n2 topic=wl.bumped",
		"time=2026-09-26T10:00:04Z level=DEBUG msg=applied fencing=4 partition=0 group=audit node=n1",
		"time=2026-09-26T10:00:05Z level=INFO msg=unrelated partition=0",
	}
	if vs := FencingFromLogs(good); len(vs) != 0 {
		t.Fatalf("good: %v", vs)
	}
	recs := ParseFencing(good)
	if len(recs) != 5 || recs[2].Node != "n2" || recs[2].Group != "read-model" || recs[2].Topic != "wl.bumped" || recs[4].Topic != "" || recs[0].Time.IsZero() {
		t.Fatalf("records: %+v", recs)
	}
	// Lines out of time order are sorted by time before checking.
	shuffled := []string{good[2], good[0], good[1]}
	if vs := FencingFromLogs(shuffled); len(vs) != 0 {
		t.Fatalf("sorted by time: %v", vs)
	}
	bad := []string{
		"fencing=5 partition=0 group=g node=n1",
		"fencing=5 partition=0 group=g node=n2", // new owner not greater
		"fencing=4 partition=0 group=g node=n2", // decreased
		"fencing=9 partition=0 group=g node=n1",
		"fencing=7 partition=0 group=g node=n1", // decreased on the same node
	}
	vs := FencingFromLogs(bad)
	if len(vs) != 3 || vs[0].ID != IDFencing || !strings.Contains(vs[0].Msg, "new owner") || !strings.Contains(vs[1].Msg, "decreased") || vs[2].Details["line"] != 5 {
		t.Fatalf("bad: %v", vs)
	}
	// Without a time on every line, line order is used.
	mixed := []string{"time=2026-09-26T10:00:05Z fencing=2 partition=0 group=g node=n1", "fencing=1 partition=0 group=g node=n1"}
	if vs := FencingFromLogs(mixed); len(vs) != 1 {
		t.Fatalf("mixed: %v", vs)
	}
	if recs := ParseFencing([]string{"fencing=99999999999999999999 partition=0 group=g node=n"}); len(recs) != 0 {
		t.Fatal("overflow must be skipped")
	}
	if unquote(`"a\"b"`) != `a"b` || unquote(`"bad`) != `"bad` || unquote(`"\q"`) != `\q` {
		t.Fatal("unquote")
	}
}

func TestViolationAndReport(t *testing.T) {
	v := Violation{ID: "I1", Msg: "broken", Details: map[string]any{"b": 2, "a": 1}}
	if v.String() != "I1: broken a=1 b=2" {
		t.Fatalf("String = %q", v.String())
	}
	if (Violation{ID: "X", Msg: "m"}).String() != "X: m" {
		t.Fatal("String without details")
	}
	var r Report
	if !r.OK() || r.String() != "ok" {
		t.Fatal("empty report")
	}
	r.Add(v, Violation{ID: "L1", Msg: "slow"})
	if r.OK() || r.String() != "I1: broken a=1 b=2\nL1: slow" {
		t.Fatalf("report: %q", r.String())
	}
}

func TestCheckTimeout(t *testing.T) {
	// A timeout of one nanosecond on a non-trivial history yields Unknown or
	// a definite answer; either way the call returns promptly.
	var ops []Op
	for p := 0; p < 6; p++ {
		ops = append(ops,
			Op{Process: p, Type: TypeInvoke, F: FSet, Key: "k", Value: p, Time: 1},
			Op{Process: p, Type: TypeOK, F: FSet, Key: "k", Value: p, Time: 100},
			Op{Process: p + 10, Type: TypeInvoke, F: FGet, Key: "k", Time: 1},
			Op{Process: p + 10, Type: TypeOK, F: FGet, Key: "k", Value: (p + 1) % 6, Time: 100})
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := Check(RegisterModel(), ToPorcupine(ops, nil), time.Nanosecond, ""); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("check did not return")
	}
}
