package redisx

import (
	"reflect"
	"testing"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// FuzzEnvelopeDecode proves DecodeEntry never panics on garbage fields and
// that whatever it accepts round-trips through EncodeEntry.
func FuzzEnvelopeDecode(f *testing.F) {
	f.Add(uuid.NewString(), "OrderCreated", "orders", "o-1", "3", "5", "2026-09-26T12:00:00.123456789Z",
		"corr", "cause", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "2", `{"a":"1"}`, `{"x":1}`, "77")
	f.Add("", "", "", "", "", "", "", "", "", "", "", "", "", "")
	f.Add("not-a-uuid", "E", "t", "k", "1", "0", "", "", "", "", "", "{}", "", "")
	f.Add(uuid.NewString(), "E", "", "k", "-9223372036854775808", "2147483648", "1970-01-01T00:00:00Z", "", "", "", "1", "[1]", "", "9999999999999999999")
	f.Add(uuid.NewString(), "E", "t", "k", "1", "0", "", "", "", "", "", `{"h":"\u0000"}`, "\xff\xfe", "-1")
	f.Fuzz(func(t *testing.T, id, typ, topic, key, seq, partition, at, corr, cause, trace, schema, headers, payload, outbox string) {
		fields := map[string]any{
			FieldID: id, FieldType: typ, FieldTopic: topic, FieldKey: key, FieldSeq: seq, FieldPartition: partition,
			FieldAt: at, FieldCorr: corr, FieldCause: cause, FieldTrace: trace, FieldSchema: schema,
			FieldHeaders: headers, FieldPayload: payload, FieldOutboxID: outbox,
		}
		env, body, outboxID, err := DecodeEntry(fields)
		if err != nil {
			return
		}
		again, body2, outbox2, err := DecodeEntry(EncodeEntry(pg.OutboxEntry{ID: outboxID, Envelope: env, Payload: body}))
		if err != nil {
			t.Fatalf("re-decode: %v", err)
		}
		if !reflect.DeepEqual(again, env) || string(body2) != string(body) || outbox2 != outboxID {
			t.Fatalf("round trip changed the entry:\n got %+v\nwant %+v", again, env)
		}
	})
}
