package mediator

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
)

// Clock is the time source the mediator uses for envelopes and request IDs.
// testkit.Clock satisfies it.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// NewID returns a UUIDv7. It uses math/rand/v2 for the random bits, which is
// ChaCha8 seeded from the OS, so it is fast and allocation-free.
func NewID(now time.Time) uuid.UUID {
	var id uuid.UUID
	ms := uint64(now.UnixMilli())
	id[0] = byte(ms >> 40)
	id[1] = byte(ms >> 32)
	id[2] = byte(ms >> 24)
	id[3] = byte(ms >> 16)
	id[4] = byte(ms >> 8)
	id[5] = byte(ms)
	r1 := rand.Uint64()
	r2 := rand.Uint64()
	id[6] = 0x70 | byte(r1>>56)&0x0f
	id[7] = byte(r1 >> 48)
	id[8] = 0x80 | byte(r1>>40)&0x3f
	id[9] = byte(r1 >> 32)
	id[10] = byte(r1 >> 24)
	id[11] = byte(r1 >> 16)
	id[12] = byte(r2 >> 56)
	id[13] = byte(r2 >> 48)
	id[14] = byte(r2 >> 40)
	id[15] = byte(r2 >> 32)
	return id
}

// scope bundles the per-call identifiers so one context derivation per Send
// carries all of them.
type scope struct {
	correlation string
	current     uuid.UUID // the request or event being processed
	parent      uuid.UUID // what caused it
	depth       int
}

type (
	scopeKey         struct{}
	idemKeyKey       struct{}
	envelopeKey      struct{}
	fencingKey       struct{}
	noCacheKey       struct{}
	uowKey           struct{}
	consumerStateKey struct{}
)

func scopeFrom(ctx context.Context) *scope {
	s, _ := ctx.Value(scopeKey{}).(*scope)
	return s
}

// CorrelationID returns the correlation ID of the current call, or "" outside
// any call.
func CorrelationID(ctx context.Context) string {
	if s := scopeFrom(ctx); s != nil {
		return s.correlation
	}
	return ""
}

// WithCorrelationID returns a context that starts a call scope with the given
// correlation ID. The HTTP adapter, remote server, and consumers use it.
func WithCorrelationID(ctx context.Context, id string) context.Context {
	var next scope
	if s := scopeFrom(ctx); s != nil {
		next = *s
	}
	next.correlation = id
	return context.WithValue(ctx, scopeKey{}, &next)
}

// WithCausation returns a context whose next Send records cause as the
// causation ID, and whose CorrelationID is corr. Consumers call it with the
// envelope's ID so commands sent from a handler are attributed to the event.
func WithCausation(ctx context.Context, corr string, cause uuid.UUID) context.Context {
	var next scope
	if s := scopeFrom(ctx); s != nil {
		next = *s
	}
	next.correlation = corr
	next.current = cause
	return context.WithValue(ctx, scopeKey{}, &next)
}

// CausationID returns the ID of the request or event that caused the current
// one, or the zero UUID at the top level.
func CausationID(ctx context.Context) uuid.UUID {
	if s := scopeFrom(ctx); s != nil {
		return s.parent
	}
	return uuid.Nil
}

// RequestID returns the unique ID of the current Send (or, inside a consumer,
// of the current event).
func RequestID(ctx context.Context) uuid.UUID {
	if s := scopeFrom(ctx); s != nil {
		return s.current
	}
	return uuid.Nil
}

// Depth returns the Send nesting depth: 1 inside the outermost handler.
func Depth(ctx context.Context) int {
	if s := scopeFrom(ctx); s != nil {
		return s.depth
	}
	return 0
}

// WithIdempotencyKey attaches an idempotency key to the context. The HTTP
// adapter sets it from the Idempotency-Key header.
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, idemKeyKey{}, key)
}

// IdempotencyKeyFrom returns the key from the context, if any.
func IdempotencyKeyFrom(ctx context.Context) (string, bool) {
	k, ok := ctx.Value(idemKeyKey{}).(string)
	return k, ok && k != ""
}

// WithEnvelope attaches the envelope of the event being consumed.
func WithEnvelope(ctx context.Context, env Envelope) context.Context {
	return context.WithValue(ctx, envelopeKey{}, &env)
}

// EnvelopeFrom returns the envelope of the current event inside a consumer handler.
func EnvelopeFrom(ctx context.Context) (Envelope, bool) {
	e, ok := ctx.Value(envelopeKey{}).(*Envelope)
	if !ok {
		return Envelope{}, false
	}
	return *e, true
}

// WithFencingToken attaches the lease epoch of the partition being consumed.
func WithFencingToken(ctx context.Context, token int64) context.Context {
	return context.WithValue(ctx, fencingKey{}, token)
}

// FencingToken returns the lease epoch for the current partition, or 0 and
// false outside a consumer.
func FencingToken(ctx context.Context) (int64, bool) {
	t, ok := ctx.Value(fencingKey{}).(int64)
	return t, ok
}

// WithNoCache marks the call to bypass cache reads. Writes still happen.
func WithNoCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, noCacheKey{}, true)
}

// NoCache reports whether cache reads are bypassed for this call.
func NoCache(ctx context.Context) bool {
	v, _ := ctx.Value(noCacheKey{}).(bool)
	return v
}

// UnitOfWork is the ambient transaction the unit of work behavior puts in the
// context. Publish uses it to append durable events. pg implements it.
type UnitOfWork interface {
	// ReadOnly reports whether the transaction is read-only.
	ReadOnly() bool
	// AppendOutbox inserts one outbox row in the transaction. The
	// implementation assigns env.Seq and env.Partition.
	AppendOutbox(ctx context.Context, env *Envelope, payload []byte) error
}

// WithUnitOfWork attaches the ambient unit of work.
func WithUnitOfWork(ctx context.Context, u UnitOfWork) context.Context {
	return context.WithValue(ctx, uowKey{}, u)
}

// UnitOfWorkFrom returns the ambient unit of work, if any.
func UnitOfWorkFrom(ctx context.Context) (UnitOfWork, bool) {
	u, ok := ctx.Value(uowKey{}).(UnitOfWork)
	return u, ok && u != nil
}

// ConsumerState is shared between the consumer loop and the behaviors of one
// delivery. The inbox behavior sets Duplicate when the event was already
// processed by the group, in which case the handler did not run.
type ConsumerState struct {
	Duplicate bool
	// Attempt is the delivery count reported by the transport, 1 for the first.
	Attempt int
}

// WithConsumerState attaches the state of the current delivery.
func WithConsumerState(ctx context.Context, s *ConsumerState) context.Context {
	return context.WithValue(ctx, consumerStateKey{}, s)
}

// ConsumerStateFrom returns the state of the current delivery, if any.
func ConsumerStateFrom(ctx context.Context) (*ConsumerState, bool) {
	s, ok := ctx.Value(consumerStateKey{}).(*ConsumerState)
	return s, ok && s != nil
}
