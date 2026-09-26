package behavior_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
)

func TestLogging_Records(t *testing.T) {
	h := newHarness(t)
	ctx := mediator.WithCorrelationID(context.Background(), "corr-9")
	h.hooks.plain = func(context.Context, plainCmd) (cmdResult, error) {
		h.clock.Advance(15 * time.Millisecond)
		return cmdResult{}, nil
	}
	if _, err := mediator.Send(ctx, h.m, plainCmd{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	starts, ends := h.logs.find("request.start"), h.logs.find("request.end")
	if len(starts) != 1 || len(ends) != 1 {
		t.Fatalf("records: %v", h.logs.all())
	}
	s, e := starts[0], ends[0]
	for _, r := range []logRecord{s, e} {
		if r.Level != slog.LevelInfo || r.Attrs["name"] != "plainCmd" || r.Attrs["kind"] != "command" || r.Attrs["correlation_id"] != "corr-9" {
			t.Fatalf("bad record %+v", r)
		}
		if id, _ := r.Attrs["request_id"].(string); len(id) != 36 {
			t.Fatalf("request_id %v", r.Attrs["request_id"])
		}
	}
	if _, ok := s.Attrs["request"]; ok {
		t.Fatal("payload logged without LogPayloads")
	}
	if e.Attrs["duration_ms"] != 15.0 || e.Attrs["outcome"] != behavior.OutcomeOK || e.Attrs["error_code"] != "" {
		t.Fatalf("end record %+v", e)
	}
	if _, ok := e.Attrs["items"]; ok {
		t.Fatal("items logged for a non-stream")
	}
}

func TestLogging_Outcomes(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		panics  bool
		outcome string
		code    string
	}{
		{"error", mediator.E(mediator.CodeConflict, "c"), false, behavior.OutcomeError, "conflict"},
		{"timeout", mediator.Wrap(mediator.CodeTimeout, "t", context.DeadlineExceeded), false, behavior.OutcomeTimeout, "timeout"},
		{"deadline", context.DeadlineExceeded, false, behavior.OutcomeTimeout, "timeout"},
		{"panic", nil, true, behavior.OutcomePanic, "internal"},
		{"converted panic", mediator.Wrap(mediator.CodeInternal, "p", &mediator.PanicError{Value: "v"}), false, behavior.OutcomePanic, "internal"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := newHarness(t)
			h.hooks.plain = func(context.Context, plainCmd) (cmdResult, error) {
				if c.panics {
					panic("p")
				}
				return cmdResult{}, c.err
			}
			_, _ = mediator.Send(context.Background(), h.m, plainCmd{ID: "a"})
			ends := h.logs.find("request.end")
			if len(ends) != 1 {
				t.Fatalf("records: %v", h.logs.all())
			}
			e := ends[0]
			if e.Attrs["outcome"] != c.outcome || e.Attrs["error_code"] != c.code {
				t.Fatalf("end record %+v", e)
			}
			if !c.panics {
				if msg, _ := e.Attrs["error"].(string); msg == "" {
					t.Fatalf("error text missing: %+v", e)
				}
			}
		})
	}
}

func TestLogging_ConsumerAndNotification(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	if err := h.deliver(ctx, "c1", mediator.Envelope{CorrelationID: "corr-c"}); err != nil {
		t.Fatal(err)
	}
	ends := h.logs.find("request.end")
	if len(ends) != 1 || ends[0].Attrs["group"] != consumerGroup || ends[0].Attrs["kind"] != "consumer" || ends[0].Attrs["correlation_id"] != "corr-c" {
		t.Fatalf("consumer record %v", ends)
	}
	h.logs.reset()
	if err := mediator.Publish(ctx, h.m, thingEvent{ID: "e"}); err != nil {
		t.Fatal(err)
	}
	ends = h.logs.find("request.end")
	if len(ends) != 1 || ends[0].Attrs["kind"] != "notification" {
		t.Fatalf("notification record %v", ends)
	}
	if _, ok := ends[0].Attrs["group"]; ok {
		t.Fatal("group logged outside the consumer path")
	}
}

func TestLogging_Stream(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	for range mediator.Stream(ctx, h.m, numStream{N: 4}) {
	}
	ends := h.logs.find("request.end")
	if len(ends) != 1 || ends[0].Attrs["items"] != int64(4) || ends[0].Attrs["outcome"] != behavior.OutcomeOK || ends[0].Attrs["kind"] != "stream" {
		t.Fatalf("stream record %v", ends)
	}

	h.logs.reset()
	for v := range mediator.Stream(ctx, h.m, numStream{N: 4}) {
		if v == 0 {
			break
		}
	}
	if ends = h.logs.find("request.end"); len(ends) != 1 || ends[0].Attrs["items"] != int64(1) {
		t.Fatalf("early stop record %v", ends)
	}

	h.logs.reset()
	h.hooks.stream = func(context.Context, numStream) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			_ = yield(1, nil) && yield(0, errors.New("cut"))
		}
	}
	for range mediator.Stream(ctx, h.m, numStream{N: 1}) {
	}
	if ends = h.logs.find("request.end"); len(ends) != 1 || ends[0].Attrs["items"] != int64(1) || ends[0].Attrs["outcome"] != behavior.OutcomeError {
		t.Fatalf("error record %v", ends)
	}

	h.logs.reset()
	h.hooks.stream = func(context.Context, numStream) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			yield(1, nil)
			panic("it")
		}
	}
	for range mediator.Stream(ctx, h.m, numStream{N: 1}) {
	}
	if ends = h.logs.find("request.end"); len(ends) != 1 || ends[0].Attrs["items"] != int64(1) || ends[0].Attrs["outcome"] != behavior.OutcomePanic {
		t.Fatalf("panic record %v", ends)
	}
}

// TestLogging_Disabled: a logger below info level costs nothing and the
// calls still run, on requests and streams.
func TestLogging_Disabled(t *testing.T) {
	sink := newLogSink(slog.LevelError)
	h := newHarness(t, withConfig(func(cfg *behavior.Config) { cfg.Logger = slog.New(sink) }))
	if _, err := mediator.Send(context.Background(), h.m, plainCmd{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	n := 0
	for range mediator.Stream(context.Background(), h.m, numStream{N: 2}) {
		n++
	}
	if n != 2 || len(sink.all()) != 0 {
		t.Fatalf("n=%d records=%v", n, sink.all())
	}
}

// ---------------------------------------------------------------- redaction sink test

// vault is a request whose every secret carries the sentinel prefix, spread
// over nested structs, slices, maps, pointers, an embedded struct, and an
// interface field.
type vault struct {
	mediator.Command[mediator.Void]
	creds
	Name     string            `json:"name"`
	Password string            `json:"password" log:"redact"`
	Token    string            `json:"token" log:"-"`
	Card     *card             `json:"card"`
	Cards    []card            `json:"cards"`
	ByName   map[string]card   `json:"byName"`
	Labels   map[string]string `json:"labels" log:"redact"`
	Extra    any               `json:"extra"`
	Nums     [2]int            `json:"nums"`
	Raw      []byte            `json:"raw"`
	When     time.Time         `json:"when"`
	Ignored  string            `json:"-"`
	hidden   string
}

type creds struct {
	APIKey string `json:"apiKey" log:"redact"`
	User   string `json:"user"`
}

type card struct {
	Number string `json:"number" log:"redact"`
	Holder string `json:"holder"`
	Inner  *card  `json:"inner,omitempty"`
}

const secret = "SECRET-"

func sampleVault(seed string) vault {
	sec := func(s string) string { return secret + seed + "-" + s }
	return vault{
		creds:    creds{APIKey: sec("api"), User: "u"},
		Name:     "n",
		Password: sec("pw"),
		Token:    sec("tok"),
		Card:     &card{Number: sec("c1"), Holder: "h1", Inner: &card{Number: sec("c1i"), Holder: "h1i"}},
		Cards:    []card{{Number: sec("c2"), Holder: "h2"}, {Number: sec("c3"), Holder: "h3"}},
		ByName:   map[string]card{"x": {Number: sec("c4"), Holder: "h4"}},
		Labels:   map[string]string{"k": sec("lbl")},
		Extra:    card{Number: sec("c5"), Holder: "h5"},
		Nums:     [2]int{1, 2},
		Raw:      []byte("raw"),
		When:     time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		Ignored:  "ignored",
		hidden:   sec("hidden"),
	}
}

// leaks walks a logged value and returns every string containing the
// sentinel.
func leaks(v any) []string {
	var out []string
	var walk func(any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			if strings.Contains(x, secret) {
				out = append(out, x)
			}
		case []byte:
			walk(string(x))
		case map[string]any:
			for _, e := range x {
				walk(e)
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		default:
			walk(fmt.Sprint(x))
		}
	}
	walk(v)
	return out
}

// TestLogging_RedactionSink proves that no value of a field tagged
// log:"-" or log:"redact" reaches the slog handler, at any depth, and that
// the untagged fields do.
func TestLogging_RedactionSink(t *testing.T) {
	sink := newLogSink(slog.LevelDebug)
	m := mediator.New()
	must(t, mediator.HandleFunc(m, func(context.Context, vault) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(t, behavior.UseStandard(m, behavior.Config{Logger: slog.New(sink), LogPayloads: true}))
	must(t, m.Build())

	rapid.Check(t, func(rt *rapid.T) {
		seed := rapid.StringMatching(`[a-z0-9]{1,8}`).Draw(rt, "seed")
		sink.reset()
		if _, err := mediator.Send(context.Background(), m, sampleVault(seed)); err != nil {
			rt.Fatal(err)
		}
		starts := sink.find("request.start")
		if len(starts) != 1 {
			rt.Fatalf("records %v", sink.all())
		}
		payload := starts[0].Attrs["request"]
		if leaked := leaks(payload); len(leaked) > 0 {
			rt.Fatalf("secrets reached the sink: %v", leaked)
		}
		for _, r := range sink.all() {
			for k, v := range r.Attrs {
				if l := leaks(v); len(l) > 0 {
					rt.Fatalf("record %q attr %s leaked %v", r.Msg, k, l)
				}
			}
		}
		got, ok := payload.(map[string]any)
		if !ok {
			rt.Fatalf("payload %T", payload)
		}
		if got["name"] != "n" || got["user"] != "u" || got["password"] != behavior.Redacted || got["token"] != behavior.Redacted || got["apiKey"] != behavior.Redacted {
			rt.Fatalf("payload %v", got)
		}
		if _, ok := got["Ignored"]; ok {
			rt.Fatal("json:\"-\" field logged")
		}
		if _, ok := got["hidden"]; ok {
			rt.Fatal("unexported field logged")
		}
		if _, ok := got["Command"]; ok {
			rt.Fatal("marker field logged")
		}
	})
}
