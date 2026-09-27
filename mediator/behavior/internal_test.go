package behavior

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior/cachemodel"
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
	// The sweep runs once the map holds exactly maxKeys entries: a full map
	// of expired keys is emptied by the next new key; one short of full is
	// not swept and grows by one.
	for _, tc := range []struct {
		name string
		keys int
		want int
	}{
		{"full", logLimiterMaxKeys, 1},
		{"one short of full", logLimiterMaxKeys - 1, logLimiterMaxKeys},
	} {
		l = newLogLimiter(clock, time.Minute)
		for i := 0; i < tc.keys; i++ {
			l.allow(strconv.Itoa(i))
		}
		clock.Advance(time.Minute)
		l.allow("fresh")
		if len(l.last) != tc.want {
			t.Errorf("%s: %d keys after a fresh one, want %d", tc.name, len(l.last), tc.want)
		}
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
	// An error that already carries CodeTimeout is returned as is, not
	// wrapped a second time.
	own := mediator.Wrap(mediator.CodeTimeout, "own", context.DeadlineExceeded)
	if got := timeoutError(own, context.DeadlineExceeded); got != error(own) {
		t.Fatalf("a timeout error must be returned unchanged, got %v", got)
	}
	if phase(true) != "post-commit" || phase(false) != "pre-commit" {
		t.Fatal("phase")
	}
	tm := &timeout{def: time.Second}
	if d := tm.durationFor(nil, &mediator.RequestInfo{Kind: mediator.KindConsumer}); d != time.Second {
		t.Fatalf("no registrations: %s", d)
	}
}

// tick is a durable event for the consumer registrations below.
type tick struct {
	mediator.Event
	ID string `json:"id"`
}

func (e tick) StreamKey() string { return e.ID }

// TestTimeout_OnBuildRecordsExplicitTimeoutsOnly: OnBuild records the
// HandlerTimeout of the consumers that set one; a registration without one
// (zero) gets no entry and durationFor falls back to the default for it.
func TestTimeout_OnBuildRecordsExplicitTimeoutsOnly(t *testing.T) {
	m := mediator.New()
	noop := func(context.Context, tick) error { return nil }
	if err := mediator.ConsumeFunc(m, "explicit", noop, mediator.HandlerTimeout(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := mediator.ConsumeFunc(m, "implicit", noop); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	tm := &timeout{def: time.Second}
	if err := tm.OnBuild(m); err != nil {
		t.Fatal(err)
	}
	recorded := *tm.consumer.Load()
	if len(recorded) != 1 {
		t.Fatalf("recorded %v, want the explicit consumer only", recorded)
	}
	for _, c := range m.ConsumerRegistrations() {
		want := map[string]time.Duration{"explicit": 5 * time.Second, "implicit": time.Second}[c.Group]
		if d, ok := recorded[c.Info]; ok != (c.Group == "explicit") || (ok && d != want) {
			t.Errorf("%s: recorded %v %s", c.Group, ok, d)
		}
		if got := tm.durationFor(tick{}, c.Info); got != want {
			t.Errorf("%s: durationFor = %s, want %s", c.Group, got, want)
		}
	}
}

// TestCache_PrepareWarmsCachedQueries: Prepare precomputes the metric
// attributes of every cached query (kind query with CacheTags) so Handle
// never takes the slow path for one; other infos are left to the fallback.
func TestCache_PrepareWarmsCachedQueries(t *testing.T) {
	c := NewCache(Config{Cache: cachemodel.NewMemory()}).(*cache)
	cached := &mediator.RequestInfo{Name: "cached", Kind: mediator.KindQuery, Traits: mediator.Traits{CacheTags: true}}
	plain := &mediator.RequestInfo{Name: "plain", Kind: mediator.KindQuery}
	tagged := &mediator.RequestInfo{Name: "tagged", Kind: mediator.KindCommand, Traits: mediator.Traits{CacheTags: true}}
	if err := c.Prepare([]*mediator.RequestInfo{cached, plain, tagged}); err != nil {
		t.Fatal(err)
	}
	warmed := c.attrs.m.Load()
	if warmed == nil {
		t.Fatal("Prepare must warm the attribute cache")
	}
	for info, want := range map[*mediator.RequestInfo]bool{cached: true, plain: false, tagged: false} {
		if _, ok := (*warmed)[info]; ok != want {
			t.Errorf("%s warmed at Prepare: %v, want %v", info.Name, ok, want)
		}
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
	// Each node costs two levels (pointer, struct) and the head sits at
	// depth 2, so exactly maxRedactDepth/2 nodes are expanded before the
	// next pointer, at depth maxRedactDepth+1, is cut.
	if depth != maxRedactDepth/2 {
		t.Errorf("%d nodes expanded, want %d", depth, maxRedactDepth/2)
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

// ladder is a chain whose every rung flattens an embedded struct holding a
// pointer, so the depth limit is exercised on the flatten path (fill) as
// well as on the pointer and struct paths (value).
type rungLeaf struct {
	V int `json:"v"`
}

type rungFlat struct {
	Leaf *rungLeaf `json:"leaf"`
}

type ladder struct {
	rungFlat
	Next *ladder `json:"next"`
}

// TestRedactor_DepthLimit pins where the limit cuts. Rung k's struct sits
// at depth 2k (the next pointer at 2k-1); its flattened leaf is reached at
// 2k+1, the leaf pointer at 2k+2 and the leaf struct at 2k+3. So with the
// limit at 32: rung 14's leaf fits, rung 15's leaf struct (33) is cut, rung
// 16 (struct at 32) is expanded but neither flattened (32 is not below the
// limit) nor followed (its next pointer is at 33).
func TestRedactor_DepthLimit(t *testing.T) {
	r := &redactor{}
	var head *ladder
	for k := maxRedactDepth; k >= 0; k-- {
		head = &ladder{rungFlat: rungFlat{Leaf: &rungLeaf{V: k}}, Next: head}
	}
	rungs := []map[string]any{r.Redact(*head).(map[string]any)}
	for {
		next, ok := rungs[len(rungs)-1]["next"].(map[string]any)
		if !ok {
			break
		}
		rungs = append(rungs, next)
	}
	const last = maxRedactDepth / 2
	if len(rungs) != last+1 {
		t.Fatalf("%d rungs expanded, want %d", len(rungs), last+1)
	}
	if rungs[last]["next"] != "[depth exceeded]" {
		t.Errorf("rung %d next = %#v", last, rungs[last]["next"])
	}
	if leaf, _ := rungs[last-2]["leaf"].(map[string]any); leaf == nil || leaf["v"] != last-2 {
		t.Errorf("rung %d leaf = %#v, want its value", last-2, rungs[last-2]["leaf"])
	}
	if rungs[last-1]["leaf"] != "[depth exceeded]" {
		t.Errorf("rung %d leaf = %#v, want it cut", last-1, rungs[last-1]["leaf"])
	}
	if v, ok := rungs[last]["leaf"]; ok {
		t.Errorf("rung %d was flattened at the limit: leaf = %#v", last, v)
	}
}

// nestMap and nestList nest without an interface in between, so each level
// costs exactly one depth.
type (
	nestMap  map[string]nestMap
	nestList []nestList
)

// TestRedactor_DepthThroughMapsAndLists: map values and list elements count
// one level each; levels 0 to maxRedactDepth are expanded and the next one
// is cut, well before the leaf of a deeper nesting.
func TestRedactor_DepthThroughMapsAndLists(t *testing.T) {
	r := &redactor{}
	const nesting = maxRedactDepth + 8
	m, l := nestMap{}, nestList{}
	for i := 0; i < nesting; i++ {
		m, l = nestMap{"k": m}, nestList{l}
	}
	const want = maxRedactDepth + 1

	depth := 0
	for n := r.Redact(m); ; depth++ {
		mm, ok := n.(map[string]any)
		if !ok {
			if n != "[depth exceeded]" {
				t.Fatalf("map chain ended with %#v at depth %d", n, depth)
			}
			break
		}
		n = mm["k"]
	}
	if depth != want {
		t.Errorf("%d map levels expanded, want %d", depth, want)
	}

	depth = 0
	for n := r.Redact(l); ; depth++ {
		ll, ok := n.([]any)
		if !ok {
			if n != "[depth exceeded]" {
				t.Fatalf("list chain ended with %#v at depth %d", n, depth)
			}
			break
		}
		if len(ll) == 0 {
			t.Fatalf("list chain reached its leaf at depth %d without being cut", depth)
		}
		n = ll[0]
	}
	if depth != want {
		t.Errorf("%d list levels expanded, want %d", depth, want)
	}
}
