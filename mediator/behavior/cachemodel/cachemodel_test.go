package cachemodel

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMemory_Protocol(t *testing.T) {
	ctx := context.Background()
	m := NewMemory()
	now := time.Unix(1000, 0)
	m.Now = func() time.Time { return now }

	if _, ok, err := m.Get(ctx, "k"); ok || err != nil {
		t.Fatal("empty backend")
	}
	snap, err := m.SnapshotTags(ctx, []string{"a", "b"})
	if err != nil || snap["a"] != 0 || snap["b"] != 0 {
		t.Fatal(snap, err)
	}
	must(t, m.Set(ctx, "k", snap, []byte("v1"), time.Minute))
	body, ok, err := m.Get(ctx, "k")
	if err != nil || !ok || string(body) != "v1" {
		t.Fatal(string(body), ok, err)
	}
	body[0] = 'X' // the returned body is a copy
	if b, _ := m.Entry("k"); string(b) != "v1" {
		t.Fatal("entry aliased")
	}

	// Expiry.
	now = now.Add(time.Minute)
	if _, ok, _ := m.Get(ctx, "k"); ok {
		t.Fatal("expired entry served")
	}
	if _, ok := m.Entry("k"); ok {
		t.Fatal("expired entry kept")
	}

	// No TTL means forever.
	must(t, m.Set(ctx, "k", snap, []byte("v2"), 0))
	now = now.Add(24 * time.Hour)
	if _, ok, _ := m.Get(ctx, "k"); !ok {
		t.Fatal("forever entry dropped")
	}

	// A bump invalidates on read.
	must(t, m.BumpTagsPre(ctx, []string{"a"}))
	if m.Version("a") != 1 || m.Version("b") != 0 {
		t.Fatal("versions")
	}
	if _, ok, _ := m.Get(ctx, "k"); ok {
		t.Fatal("stale entry served")
	}
	if _, ok := m.Entry("k"); ok {
		t.Fatal("stale entry not unlinked")
	}

	// A set with an old snapshot is rejected.
	must(t, m.Set(ctx, "k", snap, []byte("v3"), 0))
	if _, ok := m.Entry("k"); ok {
		t.Fatal("stale set stored")
	}
	snap, _ = m.SnapshotTags(ctx, []string{"a", "b"})
	must(t, m.Set(ctx, "k", snap, []byte("v3"), 0))
	must(t, m.BumpTagsPost(ctx, []string{"b"}))
	if _, ok, _ := m.Get(ctx, "k"); ok {
		t.Fatal("entry must depend on every tag")
	}

	if m.Ops(OpGet) != 6 || m.Ops(OpSet) != 4 || m.Ops(OpBumpPre) != 1 || m.Ops(OpBumpPost) != 1 || m.Ops(OpSnapshot) != 2 {
		t.Fatalf("ops get=%d set=%d", m.Ops(OpGet), m.Ops(OpSet))
	}
	m.Reset()
	if m.Version("a") != 0 || m.Ops(OpGet) != 0 {
		t.Fatal("reset")
	}
}

func TestMemory_FailuresAndContext(t *testing.T) {
	m := NewMemory()
	boom := errors.New("boom")
	var failing string
	m.Fail = func(op string) error {
		if op == failing {
			return boom
		}
		return nil
	}
	ctx := context.Background()
	calls := map[string]func() error{
		OpGet:      func() error { _, _, err := m.Get(ctx, "k"); return err },
		OpSnapshot: func() error { _, err := m.SnapshotTags(ctx, []string{"a"}); return err },
		OpSet:      func() error { return m.Set(ctx, "k", nil, nil, 0) },
		OpBumpPre:  func() error { return m.BumpTagsPre(ctx, []string{"a"}) },
		OpBumpPost: func() error { return m.BumpTagsPost(ctx, []string{"a"}) },
	}
	for op := range calls {
		failing = op
		for other, c := range calls {
			err := c()
			if (other == op) != errors.Is(err, boom) {
				t.Errorf("failing %s: %s returned %v", op, other, err)
			}
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := m.Get(canceled, "k"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestScheduler_Model(t *testing.T) {
	ctx := context.Background()
	cfg := Config{Readers: 1, Writers: 1, Key: "k", Tags: []string{"t"}, TTL: time.Minute}
	s := New(ctx, NewMemory(), cfg)
	if r := s.Runnable(); len(r) != 2 || r[0] != 0 || r[1] != 1 {
		t.Fatal(r)
	}
	// Reader first (miss, fills), then the writer, then a fresh reader hits
	// a fresh value.
	must(t, s.Run(func(r []int) int { return r[0] }))
	if len(s.Runnable()) != 0 || s.Violation() != nil || len(s.Degraded()) != 0 {
		t.Fatal("run state")
	}
	h := s.History()
	if len(h) != len(readerSteps)+len(writerSteps) || h[0].String() != "0:get(miss)" || h[3].String() != "0:set(0)" || h[4].String() != "1:begin" {
		t.Fatalf("history %v", h)
	}
	if s.Step(0) != nil || s.Step(99) != nil || s.Step(-1) != nil {
		t.Fatal("stepping a finished or unknown actor is a no-op")
	}

	// The reader now hits and the value must be at least the returned write.
	backend := NewMemory()
	s = New(ctx, backend, cfg)
	must(t, s.Run(func(r []int) int { return r[len(r)-1] })) // writer first, then reader misses and fills with 1
	s2 := New(ctx, backend, Config{Readers: 1, Key: "k", Tags: []string{"t"}})
	must(t, s2.Run(func(r []int) int { return r[0] }))
	if h := s2.History(); len(h) != 1 || h[0].String() != "0:get(hit 1)" {
		t.Fatalf("history %v", h)
	}

	// Skipped bumps show in the history; a violation is reported once.
	s = New(ctx, NewMemory(), Config{Readers: 2, Writers: 1, Key: "k", Tags: []string{"t"}, SkipPreBump: true, SkipPostBump: true})
	order := []int{0, 0, 0, 2, 2, 2, 2, 2, 0, 1}
	i := 0
	err := s.Run(func([]int) int { i++; return order[i-1] })
	var v *Violation
	if !errors.As(err, &v) || v.Reader != 1 || v.Got != 0 || v.Bound != 1 || s.Violation() != v {
		t.Fatalf("want violation, got %v", err)
	}
	if !strings.Contains(err.Error(), "reader 1 served 0 from cache after write 1 returned") || !strings.Contains(err.Error(), "2:bumppre(skipped)") {
		t.Fatalf("message %v", err)
	}
	if len(s.Runnable()) != 0 {
		t.Fatal("a served reader is finished")
	}
}

func TestScheduler_Degraded(t *testing.T) {
	ctx := context.Background()
	cfg := Config{Readers: 1, Writers: 1, Key: "k", Tags: []string{"t"}}
	steps := map[string]string{OpGet: StepGet, OpSnapshot: StepSnapshot, OpSet: StepSet, OpBumpPre: StepBumpPre, OpBumpPost: StepBumpPost}
	for _, op := range []string{OpGet, OpSnapshot, OpSet, OpBumpPre, OpBumpPost} {
		backend := NewMemory()
		backend.Fail = func(o string) error {
			if o == op {
				return errors.New("down")
			}
			return nil
		}
		s := New(ctx, backend, cfg)
		must(t, s.Run(func(r []int) int { return r[0] }))
		if d := s.Degraded(); len(d) != 1 || !strings.Contains(d[0].Error(), steps[op]) {
			t.Fatalf("%s: degraded %v", op, d)
		}
		if op == OpSnapshot {
			found := false
			for _, st := range s.History() {
				found = found || st.String() == "0:set(skipped)"
			}
			if !found {
				t.Fatalf("set must be skipped without a snapshot: %v", s.History())
			}
		}
	}

	// An undecodable body degrades instead of violating; a degraded run
	// never reports a violation.
	backend := NewMemory()
	must(t, backend.Set(ctx, "k", map[string]int64{"t": 0}, []byte("junk"), 0))
	s := New(ctx, backend, Config{Readers: 1, Key: "k", Tags: []string{"t"}})
	must(t, s.Run(func(r []int) int { return r[0] }))
	if d := s.Degraded(); len(d) != 1 || !strings.Contains(d[0].Error(), "undecodable") {
		t.Fatalf("degraded %v", d)
	}
	backend = NewMemory()
	backend.Fail = func(o string) error {
		if o == OpBumpPre {
			return errors.New("down")
		}
		return nil
	}
	s = New(ctx, backend, Config{Readers: 2, Writers: 1, Key: "k", Tags: []string{"t"}, SkipPostBump: true})
	order := []int{0, 0, 0, 2, 2, 2, 2, 2, 0, 1}
	i := 0
	if err := s.Run(func([]int) int { i++; return order[i-1] }); err != nil || s.Violation() != nil {
		t.Fatalf("degraded runs are not checked: %v", err)
	}
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
