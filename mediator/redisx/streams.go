package redisx

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// Stream entry field names (design notes 3.3). The relay writes them with
// XADD and the consumers rebuild the envelope from them.
const (
	FieldID        = "id"        // event UUID
	FieldType      = "type"      // event name
	FieldTopic     = "topic"     // stream topic
	FieldKey       = "key"       // stream key
	FieldSeq       = "seq"       // dense per (topic, key)
	FieldPartition = "partition" // partition number
	FieldAt        = "at"        // RFC3339Nano
	FieldCorr      = "corr"      // correlation ID
	FieldCause     = "cause"     // causation ID
	FieldTrace     = "trace"     // W3C traceparent
	FieldSchema    = "schema"    // schema version
	FieldHeaders   = "headers"   // JSON object
	FieldPayload   = "payload"   // event JSON
	FieldOutboxID  = "outbox_id" // outbox row id
)

// Streams is the Redis side of the relay and the janitor: it implements
// pg.StreamSink and pg.StreamTrimmer over the partition streams of 7.1.
type Streams struct {
	client *redis.Client
	keys   Keys
}

var (
	_ pg.StreamSink    = (*Streams)(nil)
	_ pg.StreamTrimmer = (*Streams)(nil)
)

// NewStreams returns a sink and trimmer over client with the prefix of cfg.
func NewStreams(client *redis.Client, cfg Config) *Streams {
	cfg = cfg.WithDefaults()
	return &Streams{client: client, keys: cfg.Keys()}
}

// Keys returns the key layout the sink writes to.
func (s *Streams) Keys() Keys { return s.keys }

// EncodeEntry returns the XADD field values for one outbox row.
func EncodeEntry(e pg.OutboxEntry) map[string]any {
	env := e.Envelope
	headers := "{}"
	if len(env.Headers) > 0 {
		if b, err := json.Marshal(env.Headers, json.Deterministic(true)); err == nil {
			headers = string(b)
		}
	}
	return map[string]any{
		FieldID:        env.ID.String(),
		FieldType:      env.Type,
		FieldTopic:     env.Topic,
		FieldKey:       env.StreamKey,
		FieldSeq:       strconv.FormatInt(env.Seq, 10),
		FieldPartition: strconv.Itoa(env.Partition),
		FieldAt:        env.OccurredAt.UTC().Format(time.RFC3339Nano),
		FieldCorr:      env.CorrelationID,
		FieldCause:     env.CausationID,
		FieldTrace:     env.TraceParent,
		FieldSchema:    strconv.Itoa(env.SchemaVersion),
		FieldHeaders:   headers,
		FieldPayload:   string(e.Payload),
		FieldOutboxID:  strconv.FormatInt(e.ID, 10),
	}
}

// ErrBadEntry is wrapped by DecodeEntry for entries that cannot be turned
// into an envelope. Such entries are poison: the consumer dead-letters them.
var ErrBadEntry = errors.New("redisx: malformed stream entry")

// DecodeEntry rebuilds the envelope, payload, and outbox ID from the fields of
// a stream entry. It never panics on garbage: every field is checked and a
// descriptive error wrapping ErrBadEntry is returned instead. The id, type,
// key, and seq fields are required; the others default to their zero values.
func DecodeEntry(fields map[string]any) (env mediator.Envelope, payload []byte, outboxID int64, err error) {
	if fields == nil {
		return env, nil, 0, fmt.Errorf("%w: no fields", ErrBadEntry)
	}
	str := func(name string) (string, bool) {
		v, ok := fields[name]
		if !ok || v == nil {
			return "", false
		}
		switch x := v.(type) {
		case string:
			return x, true
		case []byte:
			return string(x), true
		default:
			return fmt.Sprint(x), true
		}
	}
	idStr, ok := str(FieldID)
	if !ok || idStr == "" {
		return env, nil, 0, fmt.Errorf("%w: missing %s", ErrBadEntry, FieldID)
	}
	id, perr := uuid.Parse(idStr)
	if perr != nil {
		return env, nil, 0, fmt.Errorf("%w: %s: %v", ErrBadEntry, FieldID, perr)
	}
	env.ID = id
	if env.Type, ok = str(FieldType); !ok || env.Type == "" {
		return env, nil, 0, fmt.Errorf("%w: missing %s", ErrBadEntry, FieldType)
	}
	if env.StreamKey, ok = str(FieldKey); !ok || env.StreamKey == "" {
		return env, nil, 0, fmt.Errorf("%w: missing %s", ErrBadEntry, FieldKey)
	}
	seqStr, ok := str(FieldSeq)
	if !ok {
		return env, nil, 0, fmt.Errorf("%w: missing %s", ErrBadEntry, FieldSeq)
	}
	if env.Seq, perr = strconv.ParseInt(seqStr, 10, 64); perr != nil {
		return env, nil, 0, fmt.Errorf("%w: %s: %v", ErrBadEntry, FieldSeq, perr)
	}
	env.Topic, _ = str(FieldTopic)
	if env.Topic == "" {
		env.Topic = env.Type
	}
	if p, ok := str(FieldPartition); ok && p != "" {
		n, perr := strconv.Atoi(p)
		if perr != nil || n < 0 {
			return env, nil, 0, fmt.Errorf("%w: %s: %q", ErrBadEntry, FieldPartition, p)
		}
		env.Partition = n
	}
	if at, ok := str(FieldAt); ok && at != "" {
		t, perr := time.Parse(time.RFC3339Nano, at)
		if perr != nil {
			return env, nil, 0, fmt.Errorf("%w: %s: %v", ErrBadEntry, FieldAt, perr)
		}
		// The field is written in UTC (EncodeEntry) and is read back in UTC:
		// time.Parse keeps the offset it saw as a fixed or local Location, so
		// a "+00:00" from another producer would otherwise decode to the same
		// instant with a different Location and not round-trip. An offset
		// that carries the instant outside years 0000 to 9999 in UTC cannot
		// be written back in RFC 3339 and is malformed like any other bad field.
		t = t.UTC()
		if y := t.Year(); y < 0 || y > 9999 {
			return env, nil, 0, fmt.Errorf("%w: %s: year %d is outside RFC 3339", ErrBadEntry, FieldAt, y)
		}
		env.OccurredAt = t
	}
	env.CorrelationID, _ = str(FieldCorr)
	env.CausationID, _ = str(FieldCause)
	env.TraceParent, _ = str(FieldTrace)
	env.SchemaVersion = 1
	if sv, ok := str(FieldSchema); ok && sv != "" {
		n, perr := strconv.Atoi(sv)
		if perr != nil {
			return env, nil, 0, fmt.Errorf("%w: %s: %q", ErrBadEntry, FieldSchema, sv)
		}
		env.SchemaVersion = n
	}
	if h, ok := str(FieldHeaders); ok && h != "" && h != "{}" {
		var headers map[string]string
		if perr := json.Unmarshal([]byte(h), &headers); perr != nil {
			return env, nil, 0, fmt.Errorf("%w: %s: %v", ErrBadEntry, FieldHeaders, perr)
		}
		if len(headers) > 0 {
			env.Headers = headers
		}
	}
	if p, ok := str(FieldPayload); ok {
		payload = []byte(p)
	}
	if o, ok := str(FieldOutboxID); ok && o != "" {
		if outboxID, perr = strconv.ParseInt(o, 10, 64); perr != nil {
			return env, nil, 0, fmt.Errorf("%w: %s: %q", ErrBadEntry, FieldOutboxID, o)
		}
	}
	return env, payload, outboxID, nil
}

// Append adds the entries to the partition stream in order with one
// pipeline and returns the stream ID of the last entry. A failure in the
// middle of the pipeline leaves the earlier entries in the stream; the relay
// treats the batch as failed and re-adds it, and the inbox absorbs the
// duplicates (spec 6.4). Fault point redis.xadd.
func (s *Streams) Append(ctx context.Context, topic string, partition int, entries []pg.OutboxEntry) (string, error) {
	if len(entries) == 0 {
		return "", nil
	}
	if err := testkit.Fault(ctx, "redis.xadd"); err != nil {
		return "", err
	}
	key := s.keys.Stream(topic, partition)
	pipe := s.client.Pipeline()
	cmds := make([]*redis.StringCmd, len(entries))
	for i, e := range entries {
		cmds[i] = pipe.XAdd(ctx, &redis.XAddArgs{Stream: key, Values: EncodeEntry(e)})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("redisx: xadd %s: %w", key, err)
	}
	if err := testkit.FaultAfter(ctx, "redis.xadd"); err != nil {
		return "", err
	}
	return cmds[len(cmds)-1].Val(), nil
}

// Tail returns the stream ID and outbox ID of the last entry of the partition
// stream; ok is false when the stream is missing or empty. Fault point
// redis.stream.info.
func (s *Streams) Tail(ctx context.Context, topic string, partition int) (lastID string, lastOutboxID int64, ok bool, err error) {
	if err := testkit.Fault(ctx, "redis.stream.info"); err != nil {
		return "", 0, false, err
	}
	key := s.keys.Stream(topic, partition)
	msgs, err := s.client.XRevRangeN(ctx, key, "+", "-", 1).Result()
	if err != nil {
		return "", 0, false, fmt.Errorf("redisx: xrevrange %s: %w", key, err)
	}
	if len(msgs) == 0 {
		return "", 0, false, nil
	}
	_, _, outboxID, derr := DecodeEntry(msgs[0].Values)
	if derr != nil {
		// A foreign entry at the tail still proves the stream exists; the
		// relay compares stream IDs first and falls back to replaying from
		// outbox ID 0.
		return msgs[0].ID, 0, true, nil
	}
	return msgs[0].ID, outboxID, true, nil
}

// EnsureGroups creates the consumer groups on the partition stream with
// `XGROUP CREATE <stream> <group> 0 MKSTREAM`, ignoring BUSYGROUP. Starting
// at 0 rather than $ means a group that vanished with a Redis data loss
// replays the retained window, and the inbox drops what it already applied
// (spec 7.7). Fault point redis.stream.replay.
func (s *Streams) EnsureGroups(ctx context.Context, topic string, partition int, groups []string) error {
	if len(groups) == 0 {
		return nil
	}
	if err := testkit.Fault(ctx, "redis.stream.replay"); err != nil {
		return err
	}
	key := s.keys.Stream(topic, partition)
	for _, g := range groups {
		err := s.client.XGroupCreateMkStream(ctx, key, g, "0").Err()
		if err != nil && !isBusyGroup(err) {
			return fmt.Errorf("redisx: xgroup create %s %s: %w", key, g, err)
		}
	}
	return nil
}

// TrimBefore removes entries older than minTime with `XTRIM MINID ~`.
func (s *Streams) TrimBefore(ctx context.Context, topic string, partition int, minTime time.Time) error {
	key := s.keys.Stream(topic, partition)
	minID := strconv.FormatInt(minTime.UnixMilli(), 10) + "-0"
	if err := s.client.XTrimMinIDApprox(ctx, key, minID, 0).Err(); err != nil {
		return fmt.Errorf("redisx: xtrim %s: %w", key, err)
	}
	return nil
}

// Length returns XLEN of the partition stream (0 when missing).
func (s *Streams) Length(ctx context.Context, topic string, partition int) (int64, error) {
	return s.client.XLen(ctx, s.keys.Stream(topic, partition)).Result()
}
