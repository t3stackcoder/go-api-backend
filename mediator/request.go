package mediator

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
	"github.com/t3stackcoder/go-api-backend/mediator/retry"
)

// Kind classifies a message flowing through a pipeline.
type Kind uint8

const (
	KindCommand Kind = iota + 1
	KindQuery
	KindStream
	// KindNotification is the in-process publish path of an event.
	KindNotification
	// KindConsumer is the durable consumer path of an event in one group.
	KindConsumer
)

func (k Kind) String() string {
	switch k {
	case KindCommand:
		return "command"
	case KindQuery:
		return "query"
	case KindStream:
		return "stream"
	case KindNotification:
		return "notification"
	case KindConsumer:
		return "consumer"
	default:
		return "unknown"
	}
}

// IsRequest reports whether the kind is a command, query, or stream.
func (k Kind) IsRequest() bool { return k == KindCommand || k == KindQuery || k == KindStream }

// Void is the response type of commands that return nothing.
type Void struct{}

// Command marks a request that mutates state and returns R.
type Command[R any] struct{}

func (Command[R]) mediatorRequest(R)  {}
func (Command[R]) mediatorKind() Kind { return KindCommand }

// Query marks a request that reads state and returns R.
type Query[R any] struct{}

func (Query[R]) mediatorRequest(R)  {}
func (Query[R]) mediatorKind() Kind { return KindQuery }

// Request is satisfied only by types that embed Command[R] or Query[R].
type Request[R any] interface {
	mediatorRequest(R)
	mediatorKind() Kind
}

// StreamQuery marks a request whose result is a sequence of T.
type StreamQuery[T any] struct{}

func (StreamQuery[T]) mediatorStream(T)   {}
func (StreamQuery[T]) mediatorKind() Kind { return KindStream }

// StreamRequest is satisfied only by types that embed StreamQuery[T].
type StreamRequest[T any] interface {
	mediatorStream(T)
	mediatorKind() Kind
}

// Event marks a notification. Embed it in event structs.
type Event struct{}

func (Event) mediatorEvent() {}

// Notification is satisfied only by types that embed Event.
type Notification interface{ mediatorEvent() }

// Durable notifications are written to the outbox and delivered to consumers
// on any node. StreamKey is the ordering and partition key.
type Durable interface {
	Notification
	StreamKey() string
}

// Trait interfaces. A request or event type opts into a feature by
// implementing one. Presence is detected at Build and recorded in Traits;
// the method is invoked on the value at run time.
type (
	// Named pins the persisted name of a request or event type.
	Named interface{ Name() string }
	// Timeouter overrides the default deadline.
	Timeouter interface{ Timeout() time.Duration }
	// Requirer declares an authorization requirement.
	Requirer interface{ Requires() authz.Requirement }
	// RateLimited declares a rate limit policy.
	RateLimited interface{ RateLimit() ratelimit.Policy }
	// CacheTagger enables caching of a query and names its tags.
	CacheTagger interface{ CacheTags() []string }
	// CacheTTLer overrides the cache TTL of a query.
	CacheTTLer interface{ CacheTTL() time.Duration }
	// Invalidator names the tags a command bumps.
	Invalidator interface{ Invalidates() []string }
	// Retrier enables retry around the unit of work of a command.
	Retrier interface{ RetryPolicy() retry.Policy }
	// NoUnitOfWorker opts a request out of the transaction.
	NoUnitOfWorker interface{ NoUnitOfWork() }
	// IdempotencyKeyer supplies the idempotency key from the request body.
	IdempotencyKeyer interface{ IdempotencyKey() string }
	// Validator runs cross-field rules after tag rules.
	Validator interface {
		Validate(ctx context.Context) error
	}
	// Topicer overrides the stream topic of a durable event.
	Topicer interface{ Topic() string }
	// SchemaVersioner records a schema version in the envelope.
	SchemaVersioner interface{ SchemaVersion() int }
)

// Traits records which trait interfaces a registered type implements. Traits
// whose types live in packages that import mediator (TxOptions, Route,
// Describe) are detected by those packages with RequestInfo.Implements.
type Traits struct {
	Named          bool
	Timeout        bool
	Requires       bool
	RateLimit      bool
	CacheTags      bool
	CacheTTL       bool
	Invalidates    bool
	RetryPolicy    bool
	NoUnitOfWork   bool
	IdempotencyKey bool
	Validate       bool
	Topic          bool
	SchemaVersion  bool
	// Durable is set for events that implement Durable.
	Durable bool
}

var (
	namedT          = reflect.TypeFor[Named]()
	timeouterT      = reflect.TypeFor[Timeouter]()
	requirerT       = reflect.TypeFor[Requirer]()
	rateLimitedT    = reflect.TypeFor[RateLimited]()
	cacheTaggerT    = reflect.TypeFor[CacheTagger]()
	cacheTTLerT     = reflect.TypeFor[CacheTTLer]()
	invalidatorT    = reflect.TypeFor[Invalidator]()
	retrierT        = reflect.TypeFor[Retrier]()
	noUnitOfWorkerT = reflect.TypeFor[NoUnitOfWorker]()
	idemKeyerT      = reflect.TypeFor[IdempotencyKeyer]()
	validatorT      = reflect.TypeFor[Validator]()
	topicerT        = reflect.TypeFor[Topicer]()
	schemaVerT      = reflect.TypeFor[SchemaVersioner]()
	durableT        = reflect.TypeFor[Durable]()
)

func traitsOf(t reflect.Type) Traits {
	return Traits{
		Named:          t.Implements(namedT),
		Timeout:        t.Implements(timeouterT),
		Requires:       t.Implements(requirerT),
		RateLimit:      t.Implements(rateLimitedT),
		CacheTags:      t.Implements(cacheTaggerT),
		CacheTTL:       t.Implements(cacheTTLerT),
		Invalidates:    t.Implements(invalidatorT),
		RetryPolicy:    t.Implements(retrierT),
		NoUnitOfWork:   t.Implements(noUnitOfWorkerT),
		IdempotencyKey: t.Implements(idemKeyerT),
		Validate:       t.Implements(validatorT),
		Topic:          t.Implements(topicerT),
		SchemaVersion:  t.Implements(schemaVerT),
		Durable:        t.Implements(durableT),
	}
}

var markerPkgPath = reflect.TypeFor[Void]().PkgPath()

// IsMarker reports whether t is one of the embedded marker types (Command,
// Query, StreamQuery, Event). Validation and schema generation skip such fields.
func IsMarker(t reflect.Type) bool {
	if t.Kind() != reflect.Struct || t.PkgPath() != markerPkgPath {
		return false
	}
	n := t.Name()
	return n == "Event" || strings.HasPrefix(n, "Command[") || strings.HasPrefix(n, "Query[") || strings.HasPrefix(n, "StreamQuery[")
}

// countMarkers counts marker fields embedded at any depth of t.
func countMarkers(t reflect.Type, seen map[reflect.Type]bool) int {
	if t.Kind() != reflect.Struct || seen[t] {
		return 0
	}
	seen[t] = true
	n := 0
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if !f.Anonymous {
			continue
		}
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		if IsMarker(ft) {
			n++
			continue
		}
		n += countMarkers(ft, seen)
	}
	return n
}
