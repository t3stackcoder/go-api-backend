package mediator_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
	"github.com/t3stackcoder/go-api-backend/mediator/retry"
)

func TestKind(t *testing.T) {
	cases := []struct {
		k         mediator.Kind
		s         string
		isRequest bool
	}{
		{mediator.KindCommand, "command", true},
		{mediator.KindQuery, "query", true},
		{mediator.KindStream, "stream", true},
		{mediator.KindNotification, "notification", false},
		{mediator.KindConsumer, "consumer", false},
		{mediator.Kind(0), "unknown", false},
		{mediator.Kind(99), "unknown", false},
	}
	for _, c := range cases {
		if c.k.String() != c.s || c.k.IsRequest() != c.isRequest {
			t.Errorf("%d: %s %v", c.k, c.k.String(), c.k.IsRequest())
		}
	}
}

type otherEvent struct{}

func TestIsMarker(t *testing.T) {
	cases := []struct {
		t    reflect.Type
		want bool
	}{
		{reflect.TypeFor[mediator.Command[int]](), true},
		{reflect.TypeFor[mediator.Command[mediator.Void]](), true},
		{reflect.TypeFor[mediator.Query[string]](), true},
		{reflect.TypeFor[mediator.StreamQuery[[]byte]](), true},
		{reflect.TypeFor[mediator.Event](), true},
		{reflect.TypeFor[mediator.Void](), false},
		{reflect.TypeFor[mediator.Traits](), false},
		{reflect.TypeFor[mediator.Envelope](), false},
		{reflect.TypeFor[*mediator.Event](), false},
		{reflect.TypeFor[otherEvent](), false},
		{reflect.TypeFor[bCmd](), false},
		{reflect.TypeFor[int](), false},
		{reflect.TypeFor[string](), false},
	}
	for _, c := range cases {
		if got := mediator.IsMarker(c.t); got != c.want {
			t.Errorf("IsMarker(%s) = %v", c.t, got)
		}
	}
}

// allTraits implements every request trait the core detects.
type allTraits struct {
	mediator.Command[mediator.Void]
}

func (allTraits) Name() string                   { return "AllTraits" }
func (allTraits) Timeout() time.Duration         { return time.Second }
func (allTraits) Requires() authz.Requirement    { return authz.Authenticated() }
func (allTraits) RateLimit() ratelimit.Policy    { return ratelimit.Policy{Rate: 1, Period: time.Second} }
func (allTraits) CacheTags() []string            { return nil }
func (allTraits) CacheTTL() time.Duration        { return 0 }
func (allTraits) Invalidates() []string          { return nil }
func (allTraits) RetryPolicy() retry.Policy      { return retry.Policy{} }
func (allTraits) NoUnitOfWork()                  {}
func (allTraits) IdempotencyKey() string         { return "" }
func (allTraits) Validate(context.Context) error { return nil }
func (allTraits) Topic() string                  { return "t" }
func (allTraits) SchemaVersion() int             { return 1 }
func (allTraits) StreamKey() string              { return "k" }

func TestTraitsDetection(t *testing.T) {
	m := mediator.New()
	mustNil(t, mediator.Handle(m, voidHandler[allTraits]()))
	mustNil(t, mediator.Handle(m, voidHandler[bCmd]()))
	info, ok := m.InfoOf(reflect.TypeFor[allTraits]())
	if !ok {
		t.Fatal("InfoOf")
	}
	want := mediator.Traits{Named: true, Timeout: true, Requires: true, RateLimit: true, CacheTags: true, CacheTTL: true,
		Invalidates: true, RetryPolicy: true, NoUnitOfWork: true, IdempotencyKey: true, Validate: true, Topic: true, SchemaVersion: true}
	// StreamKey alone is not Durable: the type is not a Notification.
	if info.Traits != want {
		t.Fatalf("traits = %+v", info.Traits)
	}
	if info, _ := m.InfoOf(reflect.TypeFor[bCmd]()); info.Traits != (mediator.Traits{}) {
		t.Fatalf("traits = %+v", info.Traits)
	}
	if info, _ := m.InfoOf(reflect.TypeFor[allTraits]()); !info.Implements(reflect.TypeFor[mediator.Requirer]()) {
		t.Fatal("Implements")
	}
	if info.String() != "command " {
		t.Fatalf("name is derived at Build; String() = %q", info.String())
	}
}
