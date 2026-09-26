package mediator

// Internal tests reach statements the exported API cannot: non-struct types
// that satisfy the unexported marker interfaces, registry states no public
// registration function produces, and encoder failures.

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type intReq int

func (intReq) mediatorRequest(Void) {}
func (intReq) mediatorKind() Kind   { return KindCommand }

type intEv int

func (intEv) mediatorEvent() {}

type intDurable int

func (intDurable) mediatorEvent()    {}
func (intDurable) StreamKey() string { return "k" }

type internalStream struct{ StreamQuery[int] }
type internalPlainEvent struct{ Event }

func TestInternal_NonStructTypes(t *testing.T) {
	m := New()
	err := Handle(m, HandlerFunc[intReq, Void](func(context.Context, intReq) (Void, error) { return Void{}, nil }))
	if err == nil || !strings.Contains(err.Error(), "must be a struct") || strings.Contains(err.Error(), "pointer") {
		t.Fatalf("Handle: %v", err)
	}
	err = On(m, NotificationHandlerFunc[intEv](func(context.Context, intEv) error { return nil }))
	if err == nil || !strings.Contains(err.Error(), "must be a struct") || strings.Contains(err.Error(), "pointer") {
		t.Fatalf("On: %v", err)
	}
	err = Consume(m, "g", NotificationHandlerFunc[intDurable](func(context.Context, intDurable) error { return nil }))
	if err == nil || !strings.Contains(err.Error(), "must be a struct") {
		t.Fatalf("Consume: %v", err)
	}
}

func TestInternal_StreamWithoutHandler(t *testing.T) {
	m := New()
	if err := m.registerRequest(reflect.TypeFor[internalStream](), reflect.TypeFor[int](), KindStream, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	var got error
	for _, err := range m.StreamAny(context.Background(), internalStream{}) {
		got = err
	}
	if !errors.Is(got, ErrHandlerNotFound) || !strings.Contains(got.Error(), "no local stream handler") {
		t.Fatalf("got %v", got)
	}
}

func TestInternal_ConsumerMustBeDurable(t *testing.T) {
	m := New()
	et := reflect.TypeFor[internalPlainEvent]()
	m.consumers = append(m.consumers, &consumerReg{
		info:    &RequestInfo{Kind: KindConsumer, RequestType: et, Traits: traitsOf(et), Group: "g", Local: true},
		group:   "g",
		handler: func(context.Context, any) error { return nil },
	})
	err := m.Build()
	if err == nil || !strings.Contains(err.Error(), "must implement Durable") {
		t.Fatalf("got %v", err)
	}
}

func TestInternal_Helpers(t *testing.T) {
	if recovered(nil) != nil {
		t.Fatal("recovered(nil)")
	}
	if err := recovered("x"); CodeOf(err) != CodeInternal {
		t.Fatal(err)
	}
	if roundTrips(nil) != nil {
		t.Fatal("roundTrips(nil)")
	}
	if joinNames([]string{"a", "b"}) != "a > b" || joinNames(nil) != "" {
		t.Fatal("joinNames")
	}
	if kindOf[intReq, Void]() != KindCommand {
		t.Fatal("kindOf")
	}
}

// TestInternal_MarkerMethods calls the marker methods directly: they exist
// only so that the marker interfaces cannot be satisfied outside embedding,
// and nothing invokes them at run time.
func TestInternal_MarkerMethods(t *testing.T) {
	(Command[int]{}).mediatorRequest(0)
	(Query[string]{}).mediatorRequest("")
	(StreamQuery[int]{}).mediatorStream(0)
	(Event{}).mediatorEvent()
	if (Command[int]{}).mediatorKind() != KindCommand || (Query[int]{}).mediatorKind() != KindQuery || (StreamQuery[int]{}).mediatorKind() != KindStream {
		t.Fatal("marker kinds")
	}
	var _ Request[int] = Command[int]{}
	var _ Request[int] = Query[int]{}
	var _ StreamRequest[int] = StreamQuery[int]{}
	var _ Notification = Event{}
}

// limitWriter fails once more than limit bytes have been written.
type limitWriter struct {
	limit, n int
}

func (w *limitWriter) Write(p []byte) (int, error) {
	if w.n+len(p) > w.limit {
		return 0, errors.New("write limit")
	}
	w.n += len(p)
	return len(p), nil
}

// TestInternal_WriteNodeEncoderErrors drives writeNode against encoders whose
// writer fails at every possible byte offset, over documents of many shapes,
// so each error return is exercised and none of them panics.
func TestInternal_WriteNodeEncoderErrors(t *testing.T) {
	// A padding string slides every later token across the encoder's flush
	// thresholds one byte at a time, so the write error surfaces at each kind
	// of token for some pad length.
	var docs []string
	for pad := 0; pad < 120; pad++ {
		p := strings.Repeat("p", pad)
		docs = append(docs,
			`{"a":"`+p+`","o":{"k":1.5},"q":[true,false,null],"z":"s"}`,
			`["`+p+`",{"k":[1]},[{"n":null}],"s"]`,
		)
	}
	docs = append(docs, "{}", "[]", "1", `"s"`, "true", "false", "null")
	for _, doc := range docs {
		node, err := readNode(jsontext.NewDecoder(bytes.NewReader([]byte(doc))))
		if err != nil {
			t.Fatalf("%s: %v", doc, err)
		}
		full, err := CanonicalizeJSON([]byte(doc))
		if err != nil {
			t.Fatal(err)
		}
		sawErr := false
		for limit := 0; limit <= len(full); limit++ {
			w := &limitWriter{limit: limit}
			enc := jsontext.NewEncoder(w)
			if err := writeNode(enc, node); err != nil {
				sawErr = true
			}
			if w.n > limit {
				t.Fatalf("%s: wrote %d bytes past limit %d", doc, w.n, limit)
			}
		}
		if !sawErr && len(full) > 1 {
			t.Fatalf("%s: no write error surfaced for any limit", doc)
		}
	}
}

// TestInternal_WriteNodeTokenErrors reaches the token-level failures of
// writeNode that a writer error cannot produce: the encoder never flushes
// right after an object name, so a key can only fail on invalid UTF-8, and an
// opening delimiter fails once the encoder's nesting limit is exceeded.
func TestInternal_WriteNodeTokenErrors(t *testing.T) {
	nest := func(kind byte, depth int) *jsonNode {
		root := &jsonNode{kind: kind}
		cur := root
		for i := 0; i < depth; i++ {
			child := &jsonNode{kind: kind}
			if kind == '{' {
				cur.obj = []jsonMember{{key: "a", val: child}}
			} else {
				cur.arr = []*jsonNode{child}
			}
			cur = child
		}
		return root
	}
	for _, c := range []struct {
		name string
		node *jsonNode
	}{
		{"object nesting limit", nest('{', 20_000)},
		{"array nesting limit", nest('[', 20_000)},
		{"invalid utf-8 key", &jsonNode{kind: '{', obj: []jsonMember{{key: "\xff", val: &jsonNode{kind: 'n'}}}}},
		{"invalid utf-8 key after a valid member", &jsonNode{kind: '{', obj: []jsonMember{{key: "a", val: &jsonNode{kind: 't'}}, {key: "\xff", val: &jsonNode{kind: 'f'}}}}},
		{"invalid utf-8 string in an array", &jsonNode{kind: '[', arr: []*jsonNode{{kind: '"', str: "\xff"}}}},
	} {
		var buf bytes.Buffer
		if err := writeNode(jsontext.NewEncoder(&buf), c.node); err == nil {
			t.Errorf("%s: want error", c.name)
		}
	}
	// Small valid documents of every kind still encode.
	for _, n := range []*jsonNode{{kind: '{'}, {kind: '['}, {kind: '"', str: "s"}, {kind: '0', num: "1"}, {kind: 't'}, {kind: 'f'}, {kind: 'n'}, nest('{', 50), nest('[', 50)} {
		var buf bytes.Buffer
		if err := writeNode(jsontext.NewEncoder(&buf), n); err != nil {
			t.Errorf("%c: %v", n.kind, err)
		}
	}
}

func TestInternal_CanonicalNumber(t *testing.T) {
	cases := map[string]string{
		"0": "0", "-0": "0", "7": "7", "-7": "-7", "7.0": "7", "7e0": "7", "-7.5": "-7.5",
		"1e6": "1000000", "1000000.0": "1000000", "1e20": "100000000000000000000", "1e21": "1e21",
		"1e-5": "1e-05", "0.5": "0.5", "1e400": "1e400", "-0.0": "0", "1E2": "100", "2.5E-3": "0.0025",
		"123456789012345678901234567890": "123456789012345678901234567890",
		// Integral floats above 2^53 use the shortest digits, as json.Marshal does.
		"3.602879701896397e16": "36028797018963970", "36028797018963968.0": "36028797018963970",
		"1e15": "1000000000000000", "9007199254740993.0": "9007199254740992",
	}
	for in, want := range cases {
		if got := canonicalNumber(in); got != want {
			t.Errorf("canonicalNumber(%q) = %q, want %q", in, got, want)
		}
	}
}
