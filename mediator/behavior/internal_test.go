package behavior

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

func TestInfoCache(t *testing.T) {
	builds := 0
	c := infoCache[int]{build: func(i *mediator.RequestInfo) *int { builds++; n := len(i.Name); return &n }}
	a, b := &mediator.RequestInfo{Name: "aa"}, &mediator.RequestInfo{Name: "bbb"}
	first, again := *c.get(a), *c.get(a)
	if first != 2 || again != 2 || builds != 1 {
		t.Fatalf("builds %d", builds)
	}
	c.prepare([]*mediator.RequestInfo{a, b})
	if *c.get(b) != 3 || builds != 2 {
		t.Fatalf("builds %d", builds)
	}
	// The slow path returns the existing value when another goroutine won.
	if *c.slow(a) != 2 || builds != 2 {
		t.Fatalf("builds %d", builds)
	}
	var wg sync.WaitGroup
	d := &mediator.RequestInfo{Name: "dddd"}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if *c.get(d) != 4 {
				t.Error("bad value")
			}
		}()
	}
	wg.Wait()
	if builds != 3 {
		t.Fatalf("builds %d", builds)
	}
}

func TestLogLimiter(t *testing.T) {
	clock := testkit.NewFakeClock(time.Unix(0, 0))
	l := newLogLimiter(clock, time.Minute)
	if !l.allow("k") || l.allow("k") {
		t.Fatal("one per window")
	}
	clock.Advance(59 * time.Second)
	if l.allow("k") {
		t.Fatal("still inside the window")
	}
	clock.Advance(time.Second)
	if !l.allow("k") {
		t.Fatal("window elapsed")
	}
	// The map is bounded: expired keys are swept when it is full.
	for i := 0; i < logLimiterMaxKeys; i++ {
		l.allow(string(rune('a'+i%26)) + string(rune(i)))
	}
	clock.Advance(time.Minute)
	l.allow("fresh")
	if len(l.last) > 2 {
		t.Fatalf("expired keys not swept: %d", len(l.last))
	}
}

func TestOutcomeNames(t *testing.T) {
	if outcomeOK.String() != OutcomeOK || outcomePanic.String() != OutcomePanic || outcomeTimeout.String() != OutcomeTimeout || outcomeError.String() != OutcomeError {
		t.Fatal("names")
	}
	if outcomeOf(nil) != outcomeOK || outcomeOf(context.DeadlineExceeded) != outcomeTimeout || outcomeOf(errors.New("x")) != outcomeError {
		t.Fatal("classification")
	}
	n := 0
	for _, err := range errSeq(errors.New("one")) {
		n++
		if err == nil {
			t.Fatal("want error")
		}
	}
	if n != 1 {
		t.Fatal(n)
	}
}

func TestConfigHelpers(t *testing.T) {
	var cfg Config
	if cfg.logger() == nil || cfg.tracer() == nil || cfg.meter() == nil || cfg.clock().Now().IsZero() {
		t.Fatal("defaults")
	}
	if cfg.timeout() != DefaultTimeout || cfg.cacheTTL() != DefaultCacheTTL || cfg.validator() == nil {
		t.Fatal("defaults")
	}
	v := validate.New()
	if (Config{Validator: v}).validator() != v {
		t.Fatal("validator kept")
	}
	// after: the fake clock's After is used when present, else time.After.
	fake := testkit.NewFakeClock(time.Unix(0, 0))
	ch := (Config{Clock: fake}).after()(time.Hour)
	select {
	case <-ch:
		t.Fatal("fake timer fired without Advance")
	default:
	}
	fake.Advance(time.Hour)
	<-ch
	select {
	case <-(Config{Clock: realClock{}}).after()(0):
	case <-time.After(time.Second):
		t.Fatal("time.After not used")
	}
	// The instrument set is created once per Standard call.
	shared := cfg.shared()
	if shared.inst == nil || shared.shared().inst != shared.inst {
		t.Fatal("instruments not shared")
	}
	inst, err := shared.instruments()
	if err != nil || inst != shared.inst {
		t.Fatal("instruments not reused")
	}
	if _, err := cfg.instruments(); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutErrorAndPhase(t *testing.T) {
	err := timeoutError(nil, context.DeadlineExceeded)
	if mediator.CodeOf(err) != mediator.CodeTimeout || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if phase(true) != "post-commit" || phase(false) != "pre-commit" {
		t.Fatal("phase")
	}
	tm := &timeout{def: time.Second}
	if d := tm.durationFor(nil, &mediator.RequestInfo{Kind: mediator.KindConsumer}); d != time.Second {
		t.Fatalf("no registrations: %s", d)
	}
}

// ---------------------------------------------------------------- redaction edge cases

type roInner struct {
	S   string
	I   int
	U   uint
	F   float64
	B   bool
	C   complex128
	Ch  chan int
	Sec string `log:"redact"`
	T   time.Time
}

type roOuter struct {
	roInner
	Own uintptr
}

type marshalsSelf struct{ v string }

func (m *marshalsSelf) MarshalText() ([]byte, error) { return []byte("<" + m.v + ">"), nil }

type node struct {
	V    int   `json:"v"`
	Next *node `json:"next"`
}

type secretString string

type edgeCases struct {
	mediator.Command[mediator.Void]
	secretString
	time.Duration
	Opt     string         `json:",omitempty"`
	Self    *node          `json:"self"`
	Text    marshalsSelf   `json:"text"`
	PText   *marshalsSelf  `json:"ptext"`
	IntKeys map[int]string `json:"intKeys"`
	NilMap  map[string]int `json:"nilMap"`
	NilPtr  *node          `json:"nilPtr"`
	NilAny  any            `json:"nilAny"`
	NilSl   []int          `json:"nilSl"`
	Arr     [2]string      `json:"arr"`
	Embed   *node          `json:"-"`
	*node
	skipped int `log:"redact"` //lint:ignore U1000 unexported: the redactor must skip it, asserted below
}

func TestRedactor_EdgeCases(t *testing.T) {
	r := &redactor{}
	// Exported fields of an unexported embedded struct are flattened and
	// readable (reflect clears the embedded read-only flag on them).
	out := r.Redact(roOuter{roInner: roInner{S: "s", I: -1, U: 2, F: 1.5, B: true, C: 1i, Sec: "x", T: time.Unix(1, 0)}, Own: 7}).(map[string]any)
	want := map[string]any{"S": "s", "I": -1, "U": uint(2), "F": 1.5, "B": true, "C": 1i, "Ch": (chan int)(nil), "Sec": Redacted, "Own": uintptr(7), "T": time.Unix(1, 0)}
	for k, v := range want {
		if !reflect.DeepEqual(out[k], v) {
			t.Errorf("%s = %#v, want %#v", k, out[k], v)
		}
	}
	// Read-only values (an unexported non-embedded field) fall back to the
	// kind-specific accessors; an opaque struct that cannot be handed out is
	// expanded.
	ro := reflect.ValueOf(struct {
		in roInner
	}{in: roInner{S: "s", I: -1, U: 2, F: 1.5, B: true, C: 1i, T: time.Unix(1, 0)}}).Field(0)
	if ro.CanInterface() {
		t.Fatal("test needs a read-only value")
	}
	roWant := map[string]any{"S": "s", "I": int64(-1), "U": uint64(2), "F": 1.5, "B": true, "C": 1i, "Ch": nil}
	for name, v := range roWant {
		if got := scalar(ro.FieldByName(name)); !reflect.DeepEqual(got, v) {
			t.Errorf("read-only %s = %#v, want %#v", name, got, v)
		}
	}
	if _, ok := r.value(ro.FieldByName("T"), 0).(map[string]any); !ok {
		t.Errorf("a read-only time.Time is expanded, got %#v", r.value(ro.FieldByName("T"), 0))
	}

	// Self-referential chain deeper than the limit, marshalers, map keys,
	// nils, arrays, flattening through a nil embedded pointer.
	head := &node{}
	cur := head
	for i := 0; i < maxRedactDepth+5; i++ {
		cur.Next = &node{V: i}
		cur = cur.Next
	}
	e := edgeCases{secretString: "hidden", Duration: time.Second, Opt: "o", Self: head, Text: marshalsSelf{"t"}, PText: &marshalsSelf{"p"}, IntKeys: map[int]string{1: "one"}, Arr: [2]string{"a", "b"}}
	out = r.Redact(e).(map[string]any)
	if out["Opt"] != "o" || out["Duration"] != time.Second {
		t.Errorf("tag without a name keeps the Go name: %v", out)
	}
	if _, ok := out["secretString"]; ok {
		t.Error("an unexported embedded non-struct is not logged")
	}
	depth := 0
	for n := out["self"]; ; depth++ {
		m, ok := n.(map[string]any)
		if !ok {
			if n != "[depth exceeded]" {
				t.Fatalf("chain ended with %#v at depth %d", n, depth)
			}
			break
		}
		n = m["next"]
	}
	if _, ok := out["text"].(marshalsSelf); !ok {
		t.Errorf("text %#v", out["text"])
	}
	if _, ok := out["ptext"].(marshalsSelf); !ok {
		t.Errorf("ptext %#v", out["ptext"])
	}
	if out["intKeys"].(map[string]any)["1"] != "one" || out["nilMap"] != nil || out["nilPtr"] != nil || out["nilAny"] != nil || out["nilSl"] != nil {
		t.Errorf("nils/keys %v", out)
	}
	if arr := out["arr"].([]any); len(arr) != 2 || arr[1] != "b" {
		t.Errorf("arr %v", out["arr"])
	}
	for _, absent := range []string{"Embed", "skipped", "v", "next", "Command"} {
		if _, ok := out[absent]; ok {
			t.Errorf("%s must not be logged: %v", absent, out)
		}
	}
	// A non-nil embedded pointer is flattened.
	e.node = &node{V: 9}
	out = r.Redact(e).(map[string]any)
	if out["v"] != 9 {
		t.Errorf("flattened v = %#v", out["v"])
	}
	if r.Redact(nil) != nil || r.Redact("s") != "s" {
		t.Error("leaf values")
	}
	if got := scalar(reflect.ValueOf(struct{ f func() }{}).Field(0)); got != nil {
		t.Errorf("func leaf %#v", got)
	}
	if mapKey(reflect.ValueOf(struct{ k int }{3}).Field(0)) != "<int Value>" {
		t.Error("read-only map key falls back to String()")
	}
}
