package mediator

import (
	"hash/fnv"
	"time"

	"github.com/google/uuid"
)

// Envelope is the metadata persisted with every durable event and carried
// through Redis to consumers.
type Envelope struct {
	ID            uuid.UUID         `json:"id"`         // UUIDv7, unique per event
	Type          string            `json:"type"`       // stable event name
	Topic         string            `json:"topic"`      // defaults to Type; override with Topic() string
	StreamKey     string            `json:"streamKey"`  // ordering and partition key
	Seq           int64             `json:"seq"`        // dense per (topic, stream key)
	Partition     int               `json:"partition"`  // fnv1a64(streamKey) mod P
	OccurredAt    time.Time         `json:"occurredAt"` //
	CorrelationID string            `json:"correlationId"`
	CausationID   string            `json:"causationId"` // the request that produced this event
	TraceParent   string            `json:"traceParent"` // W3C trace context
	SchemaVersion int               `json:"schemaVersion"`
	Headers       map[string]string `json:"headers,omitempty"`
}

// Partition returns fnv1a64(key) mod p. Partition assignment depends only on
// the key and the partition count, so every node agrees.
func Partition(key string, p int) int {
	if p <= 1 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % uint64(p))
}
