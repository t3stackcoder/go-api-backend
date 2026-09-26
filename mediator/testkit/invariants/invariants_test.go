package invariants

import (
	"errors"
	"math"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

func TestEnvDefaults(t *testing.T) {
	var e Env
	if e.bound() != DefaultBound || e.poll() != 250*time.Millisecond || e.partitions() != 16 {
		t.Fatalf("defaults: %v %v %d", e.bound(), e.poll(), e.partitions())
	}
	if g := e.groups(); len(g) != 2 || g[0] != workload.GroupReadModel {
		t.Fatalf("groups: %v", g)
	}
	if tp := e.topics(); len(tp) != 1 || tp[0] != workload.TopicBumped {
		t.Fatalf("topics: %v", tp)
	}
	if e.keys().Stream("t", 1) != "mediator:{t:p1}" {
		t.Fatal("keys")
	}
	e = Env{Bound: time.Second, Poll: time.Millisecond, Partitions: 4, Groups: []string{}, Topics: []string{"x"}, Cfg: redisx.Config{Prefix: "p", PartitionsPerTopic: 8}}
	if e.bound() != time.Second || e.poll() != time.Millisecond || e.partitions() != 4 || len(e.groups()) != 0 || e.topics()[0] != "x" || e.keys().Prefix != "p" {
		t.Fatal("explicit values")
	}
	if e.partitionOf("k") != mediator.Partition("k", 4) {
		t.Fatal("partitionOf")
	}
	if (Env{Cfg: redisx.Config{PartitionsPerTopic: 8}}).partitions() != 8 {
		t.Fatal("partitions from config")
	}
	if isMissing(nil) || !isMissing(errors.New("NOGROUP No such key")) || !isMissing(errors.New("ERR no such key")) || isMissing(errors.New("other")) {
		t.Fatal("isMissing")
	}
}

func TestExpectFor(t *testing.T) {
	cases := []struct {
		req  any
		want Expect
	}{
		{workload.SetValue{Key: "k", CmdID: "c", Keyed: true}, Expect{Name: workload.NameSetValue, IdemKey: "c", Register: []string{"k"}}},
		{&workload.SetValue{Key: "k", CmdID: "c"}, Expect{Name: workload.NameSetValue, Register: []string{"k"}}},
		{workload.Transfer{CmdID: "c", Keyed: true}, Expect{Name: workload.NameTransfer, IdemKey: "c"}},
		{&workload.Transfer{CmdID: "c"}, Expect{Name: workload.NameTransfer}},
		{workload.Append{Key: "k", CmdID: "c"}, Expect{Name: workload.NameAppend, IdemKey: "c", Appends: true}},
		{&workload.Append{Key: "k", CmdID: "c"}, Expect{Name: workload.NameAppend, IdemKey: "c", Appends: true}},
		{workload.Bump{Key: "k", CmdID: "c"}, Expect{Name: workload.NameBump, IdemKey: "c", Events: 1}},
		{&workload.Bump{Key: "k"}, Expect{Name: workload.NameBump, Events: 1}},
		{workload.AtomicScenario{CmdID: "c", Key1: "a", Key2: "b", CmdKey: "k"}, Expect{Name: workload.NameAtomicScenario, IdemKey: "k", Register: []string{"a"}, Events: 2, Side: true}},
		{&workload.AtomicScenario{CmdID: "c", Key1: "a", Key2: "b"}, Expect{Name: workload.NameAtomicScenario, Register: []string{"a"}, Events: 2, Side: true}},
		{workload.Touch{Key: "k", CmdID: "c"}, Expect{Name: workload.NameTouch}},
		{&workload.Touch{Key: "k", CmdID: "c"}, Expect{Name: workload.NameTouch}},
	}
	for _, c := range cases {
		got, ok := ExpectFor(c.req)
		if !ok || got.Name != c.want.Name || got.IdemKey != c.want.IdemKey || got.Events != c.want.Events || got.Side != c.want.Side || got.Appends != c.want.Appends || len(got.Register) != len(c.want.Register) {
			t.Fatalf("ExpectFor(%T) = %+v, want %+v", c.req, got, c.want)
		}
		if !got.explicit() {
			t.Fatal("explicit")
		}
	}
	if _, ok := ExpectFor(workload.GetValue{}); ok {
		t.Fatal("query is not a command")
	}
	if e := expectForName(workload.NameAppend, "c"); e.IdemKey != "c" || !e.Appends {
		t.Fatalf("append defaults: %+v", e)
	}
	if e := expectForName(workload.NameBump, "c"); e.Events != 1 || e.IdemKey != "" {
		t.Fatalf("bump defaults: %+v", e)
	}
	if e := expectForName(workload.NameAtomicScenario, "c"); e.Events != 2 || !e.Side {
		t.Fatalf("atomic defaults: %+v", e)
	}
	if e := expectForName(workload.NameSetValue, "c"); e.Name != workload.NameSetValue || e.Events != 0 || (Expect{}).explicit() {
		t.Fatalf("set defaults: %+v", e)
	}
}

func TestSameResponses(t *testing.T) {
	if vs := SameResponses(map[string][][]byte{"a": {[]byte(`{"n":1}`), []byte(`{"n":1}`)}, "b": {[]byte("x")}, "c": nil}); len(vs) != 0 {
		t.Fatalf("same: %v", vs)
	}
	vs := SameResponses(map[string][][]byte{"z": {[]byte("1"), []byte("1"), []byte("2")}, "a": {[]byte("1"), []byte("2")}})
	if len(vs) != 2 || vs[0].ID != IDI4 || vs[0].Details["key"] != "a" || vs[1].Details["index"] != 2 {
		t.Fatalf("differ: %v", vs)
	}
}

func TestI7(t *testing.T) {
	writes := []history.Write{
		{Key: "k", Value: 1, Invoke: 10, Return: 20, Definite: true},
		{Key: "k", Value: 2, Invoke: 30, Return: 40, Definite: true},
		{Key: "k", Value: 3, Invoke: 70, Return: math.MaxInt64},
	}
	reads := []history.Read{
		{Process: 1, Key: "k", Tags: []string{"wl:register:k"}, Invoke: 50, Return: 60, Value: 1}, // stale
		{Process: 2, Key: "k", Invoke: 35, Return: 45, Value: 1},                                  // concurrent with write 2
		{Process: 3, Key: "k", Invoke: 80, Return: 85, Value: 3},                                  // info write visible
		{Process: 4, Key: "k", Invoke: 90, Return: 95, Value: nil},                                // not found: stale
		{Process: 5, Key: "z", Invoke: 90, Return: 95, Value: nil},                                // other key
	}
	vs := I7(reads, writes, nil)
	if len(vs) != 2 || vs[0].ID != IDI7 || vs[0].Details["process"] != 1 || vs[1].Details["process"] != 4 {
		t.Fatalf("no windows: %v", vs)
	}
	// Windows are taken as already extended by the TTL.
	windows := []history.Window{{Start: 45, End: 55, Tags: []string{"wl:register:k"}, Kind: "partition-redis"}}
	if vs := I7(reads, writes, windows); len(vs) != 1 || vs[0].Details["process"] != 4 {
		t.Fatalf("with window: %v", vs)
	}
	if vs := I7(reads, writes, []history.Window{{Start: 0, End: 100}}); len(vs) != 0 {
		t.Fatalf("tagless window: %v", vs)
	}
	if vs := I7(nil, writes, nil); len(vs) != 0 {
		t.Fatal("no reads")
	}
}

func TestReportAlias(t *testing.T) {
	var r Report
	r.Add(Violation{ID: IDL1, Msg: "slow"})
	if r.OK() || r.String() != "L1: slow" {
		t.Fatalf("report: %q", r.String())
	}
	if IDFencing != history.IDFencing {
		t.Fatal("fencing id")
	}
	if vs := capSeqs(make([]int64, 30)); len(vs) != maxDetails {
		t.Fatal("capSeqs")
	}
	if ks := keysOf(map[string]any{"b": 1, "a": 2}); len(ks) != 2 || ks[0] != "a" {
		t.Fatal("keysOf")
	}
	ids := map[uuid.UUID]bool{}
	for i := 0; i < maxDetails+5; i++ {
		ids[uuid.New()] = true
	}
	if got := sortedIDs(ids); len(got) != maxDetails || !sort.StringsAreSorted(got) {
		t.Fatalf("sortedIDs: %d", len(got))
	}
}
