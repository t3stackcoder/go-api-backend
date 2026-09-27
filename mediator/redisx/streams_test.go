package redisx

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

func sampleEntry() pg.OutboxEntry {
	return pg.OutboxEntry{
		ID: 77,
		Envelope: mediator.Envelope{
			ID:            uuid.MustParse("0192f1a0-1234-7abc-8def-0123456789ab"),
			Type:          "OrderCreated",
			Topic:         "orders",
			StreamKey:     "o-1",
			Seq:           3,
			Partition:     5,
			OccurredAt:    time.Date(2026, 9, 26, 12, 0, 0, 123456789, time.UTC),
			CorrelationID: "corr-1",
			CausationID:   "cause-1",
			TraceParent:   "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
			SchemaVersion: 2,
			Headers:       map[string]string{"b": "2", "a": "1"},
		},
		Payload: []byte(`{"orderId":"o-1","total":12345678901234567}`),
	}
}

func TestEntry_RoundTrip(t *testing.T) {
	e := sampleEntry()
	fields := EncodeEntry(e)
	want := []string{FieldID, FieldType, FieldTopic, FieldKey, FieldSeq, FieldPartition, FieldAt, FieldCorr, FieldCause, FieldTrace, FieldSchema, FieldHeaders, FieldPayload, FieldOutboxID}
	for _, f := range want {
		if _, ok := fields[f]; !ok {
			t.Errorf("field %s missing", f)
		}
	}
	if len(fields) != len(want) {
		t.Errorf("fields = %d, want %d", len(fields), len(want))
	}
	if fields[FieldHeaders] != `{"a":"1","b":"2"}` {
		t.Errorf("headers = %v", fields[FieldHeaders])
	}
	env, payload, outboxID, err := DecodeEntry(fields)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(env, e.Envelope) {
		t.Fatalf("envelope mismatch:\n got %+v\nwant %+v", env, e.Envelope)
	}
	if string(payload) != string(e.Payload) || outboxID != 77 {
		t.Fatalf("payload %s outbox %d", payload, outboxID)
	}
}

func TestEntry_Defaults(t *testing.T) {
	fields := map[string]any{
		FieldID: uuid.NewString(), FieldType: "E", FieldKey: "k", FieldSeq: "1", FieldHeaders: "{}",
	}
	env, payload, outboxID, err := DecodeEntry(fields)
	if err != nil {
		t.Fatal(err)
	}
	if env.Topic != "E" || env.SchemaVersion != 1 || env.Headers != nil || payload != nil || outboxID != 0 || env.Partition != 0 {
		t.Fatalf("defaults: %+v payload=%v outbox=%d", env, payload, outboxID)
	}
	// Non-string values are stringified, nil is missing.
	fields2 := map[string]any{FieldID: uuid.NewString(), FieldType: "E", FieldKey: "k", FieldSeq: 5, FieldPartition: 2, FieldOutboxID: nil, FieldPayload: []byte("{}")}
	env, payload, _, err = DecodeEntry(fields2)
	if err != nil || env.Seq != 5 || env.Partition != 2 || string(payload) != "{}" {
		t.Fatalf("stringified: %+v %s %v", env, payload, err)
	}
	// Empty headers object decodes to nil, a header map to the map.
	fields2[FieldHeaders] = `{"x":"y"}`
	env, _, _, err = DecodeEntry(fields2)
	if err != nil || env.Headers["x"] != "y" {
		t.Fatalf("headers: %+v %v", env.Headers, err)
	}
	// Empty outbox id and schema strings are tolerated.
	fields2[FieldOutboxID], fields2[FieldSchema], fields2[FieldAt] = "", "", ""
	if _, _, _, err = DecodeEntry(fields2); err != nil {
		t.Fatal(err)
	}
	// An empty object that is not the literal "{}" decodes to no headers
	// either, so the envelope compares equal to one encoded without any.
	fields2[FieldHeaders] = "{ }"
	env, _, _, err = DecodeEntry(fields2)
	if err != nil || env.Headers != nil {
		t.Fatalf("headers %q: %#v %v", fields2[FieldHeaders], env.Headers, err)
	}
}

func TestEntry_Garbage(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{FieldID: uuid.NewString(), FieldType: "E", FieldKey: "k", FieldSeq: "1"}
	}
	cases := map[string]func(map[string]any){
		"nil fields":       nil,
		"missing id":       func(m map[string]any) { delete(m, FieldID) },
		"bad uuid":         func(m map[string]any) { m[FieldID] = "nope" },
		"missing type":     func(m map[string]any) { m[FieldType] = "" },
		"missing key":      func(m map[string]any) { delete(m, FieldKey) },
		"missing seq":      func(m map[string]any) { delete(m, FieldSeq) },
		"bad seq":          func(m map[string]any) { m[FieldSeq] = "x" },
		"bad partition":    func(m map[string]any) { m[FieldPartition] = "-1" },
		"bad partition2":   func(m map[string]any) { m[FieldPartition] = "p" },
		"bad at":           func(m map[string]any) { m[FieldAt] = "yesterday" },
		"bad schema":       func(m map[string]any) { m[FieldSchema] = "v2" },
		"bad headers":      func(m map[string]any) { m[FieldHeaders] = "[1]" },
		"bad headers json": func(m map[string]any) { m[FieldHeaders] = "{" },
		"bad outbox":       func(m map[string]any) { m[FieldOutboxID] = "1.5" },
	}
	for name, mutate := range cases {
		var m map[string]any
		if mutate != nil {
			m = base()
			mutate(m)
		}
		_, _, _, err := DecodeEntry(m)
		if !errors.Is(err, ErrBadEntry) {
			t.Errorf("%s: err = %v, want ErrBadEntry", name, err)
		}
	}
}

func TestDLQFields_Shape(t *testing.T) {
	orig := EncodeEntry(sampleEntry())
	now := time.Date(2026, 9, 26, 13, 0, 0, 0, time.UTC)
	cause := errors.New(strings.Repeat("x", 5000))
	f := dlqFields(orig, "mediator:{orders:p5}", "1-0", "proj", "node-a", cause, 10, now)
	for k, v := range orig {
		if f[k] != v {
			t.Errorf("original field %s changed: %v", k, f[k])
		}
	}
	if f[DLQFieldAttempts] != "10" || f[DLQFieldStreamID] != "1-0" || f[DLQFieldStream] != "mediator:{orders:p5}" ||
		f[DLQFieldGroup] != "proj" || f[DLQFieldNode] != "node-a" || f[DLQFieldAt] != "2026-09-26T13:00:00Z" {
		t.Fatalf("dlq fields: %v", f)
	}
	if len(f[DLQFieldError].(string)) != 4096 {
		t.Fatalf("error not truncated: %d", len(f[DLQFieldError].(string)))
	}
	if len(f) != len(orig)+7 {
		t.Fatalf("len = %d", len(f))
	}
	f2 := dlqFields(orig, "s", "1-0", "g", "n", nil, 1, now)
	if f2[DLQFieldError] != "" {
		t.Fatalf("nil cause: %v", f2[DLQFieldError])
	}

	// parseDLQ inverts it.
	e := parseDLQ(redis.XMessage{ID: "9-0", Values: f}, "proj")
	if e.ID != "9-0" || e.Group != "proj" || e.Attempts != 10 || e.StreamID != "1-0" || e.Node != "node-a" || !e.FailedAt.Equal(now) || e.DecodeErr != nil {
		t.Fatalf("parsed: %+v", e)
	}
	if e.Envelope.ID != sampleEntry().Envelope.ID || e.OutboxID != 77 || len(e.Fields) != len(orig) {
		t.Fatalf("parsed envelope: %+v fields=%d", e.Envelope, len(e.Fields))
	}
	if _, has := e.Fields[DLQFieldError]; has {
		t.Fatal("dlq field leaked into original fields")
	}
	bad := parseDLQ(redis.XMessage{ID: "9-1", Values: map[string]any{DLQFieldError: "e", FieldID: "x"}}, "g")
	if bad.DecodeErr == nil {
		t.Fatal("expected decode error")
	}
	// The recorded group wins over the group the entry was listed under; an
	// empty recorded group falls back to it.
	f[DLQFieldGroup] = "other"
	if e := parseDLQ(redis.XMessage{ID: "9-2", Values: f}, "proj"); e.Group != "other" {
		t.Fatalf("recorded group ignored: %q", e.Group)
	}
	f[DLQFieldGroup] = ""
	if e := parseDLQ(redis.XMessage{ID: "9-3", Values: f}, "proj"); e.Group != "proj" {
		t.Fatalf("empty recorded group: %q", e.Group)
	}
}
