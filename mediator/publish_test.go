package mediator_test

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// fakeUoW records AppendOutbox calls.
type fakeUoW struct {
	readOnly bool
	err      error
	envs     []mediator.Envelope
	payloads []string
}

func (u *fakeUoW) ReadOnly() bool { return u.readOnly }

func (u *fakeUoW) AppendOutbox(_ context.Context, env *mediator.Envelope, payload []byte) error {
	if u.err != nil {
		return u.err
	}
	env.Seq = int64(len(u.envs) + 1) // the implementation assigns Seq
	u.envs = append(u.envs, *env)
	u.payloads = append(u.payloads, string(payload))
	return nil
}

type puEvent struct {
	mediator.Event
	K string `json:"k"`
	V int    `json:"v"`
}

func (e puEvent) StreamKey() string { return e.K }

type puTopicEvent struct {
	mediator.Event
	K string `json:"k"`
}

func (e puTopicEvent) StreamKey() string { return e.K }
func (puTopicEvent) Topic() string       { return "orders" }
func (puTopicEvent) SchemaVersion() int  { return 3 }

type puPlain struct {
	mediator.Event
	V int `json:"v"`
}

type puUnregistered struct {
	mediator.Event
	K string `json:"k"`
}

func (e puUnregistered) StreamKey() string { return e.K }

type puUnregisteredTopic struct {
	mediator.Event
	K string `json:"k"`
}

func (e puUnregisteredTopic) StreamKey() string { return e.K }
func (puUnregisteredTopic) Topic() string       { return "aaa" }

type puChan struct {
	mediator.Event
	C chan int
}

func (puChan) StreamKey() string { return "k" }

type puBadName struct{ mediator.Event }

func (puBadName) StreamKey() string { return "k" }
func (puBadName) Name() string      { return "bad name" }

var fixedNow = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

func idMillis(id uuid.UUID) int64 {
	var b [8]byte
	copy(b[2:], id[:6])
	return int64(binary.BigEndian.Uint64(b[:]))
}

func TestPublish_Boundaries(t *testing.T) {
	m := mediator.New()
	var got []puPlain
	var log []string
	mustNil(t, mediator.OnFunc(m, func(_ context.Context, e puPlain) error { got = append(got, e); return nil }))
	mustNil(t, mediator.RegisterEvent[puEvent](m))
	mustNil(t, mediator.Use(m, rec{name: "N", log: &log}, mediator.Notifications()))
	ctx := context.Background()
	if err := mediator.Publish(ctx, m, puPlain{}); !errors.Is(err, mediator.ErrNotBuilt) {
		t.Fatal(err)
	}
	build(t, m)
	if err := mediator.Publish(ctx, m, nil); mediator.CodeOf(err) != mediator.CodeBadRequest {
		t.Fatal(err)
	}
	if err := mediator.Publish(ctx, m, (*puPlain)(nil)); mediator.CodeOf(err) != mediator.CodeBadRequest {
		t.Fatal(err)
	}
	mustNil(t, mediator.Publish(ctx, m, puPlain{V: 1}))
	mustNil(t, mediator.Publish(ctx, m, &puPlain{V: 2}))
	if !reflect.DeepEqual(got, []puPlain{{V: 1}, {V: 2}}) {
		t.Fatalf("handlers saw %+v", got)
	}
	if strings.Join(log, "") != "N><N"+"N><N" {
		t.Fatalf("log = %v", log)
	}
	// Unregistered non-durable event: nothing to do.
	mustNil(t, mediator.Publish(ctx, m, bPlainEvent{}))
	// A registered event with no handlers runs the chain (and, being durable,
	// needs a unit of work).
	log = nil
	if err := mediator.Publish(ctx, m, puEvent{K: "k"}); !errors.Is(err, mediator.ErrNoUnitOfWork) {
		t.Fatal(err)
	}
	if strings.Join(log, "") != "N><N" {
		t.Fatalf("log = %v", log)
	}
	ev, ok := m.Events()[0], len(m.Events()) == 2
	if !ok || ev.Name != "puEvent" || ev.Handlers != 0 || !ev.Traits.Durable {
		t.Fatalf("events = %+v", m.Events())
	}
	// Headers on a non-durable event are ignored.
	mustNil(t, mediator.Publish(ctx, m, puPlain{}, mediator.Headers(map[string]string{"a": "b"})))
}

func TestPublish_DurableEnvelope(t *testing.T) {
	clock := testkit.NewFakeClock(fixedNow)
	m := mediator.New(mediator.WithClock(clock))
	mustNil(t, mediator.OnFunc(m, noopEvent[puEvent]))
	mustNil(t, mediator.OnFunc(m, noopEvent[puTopicEvent]))
	var requestID uuid.UUID
	mustNil(t, mediator.HandleFunc(m, func(ctx context.Context, c pCmd) (int, error) {
		requestID = mediator.RequestID(ctx)
		return 0, mediator.Publish(ctx, m, puEvent{K: "k1", V: c.N}, mediator.Headers(map[string]string{"a": "1"}), mediator.Headers(map[string]string{"b": "2", "a": "3"}))
	}))
	build(t, m)
	uow := &fakeUoW{}
	ctx := mediator.WithUnitOfWork(mediator.WithCorrelationID(context.Background(), "corr"), uow)

	// Inside a Send: causation is the request ID.
	if _, err := mediator.Send(ctx, m, pCmd{N: 2}); err != nil {
		t.Fatal(err)
	}
	// Outside any Send: no causation, no correlation.
	clock.Advance(time.Second)
	mustNil(t, mediator.Publish(mediator.WithUnitOfWork(context.Background(), uow), m, &puTopicEvent{K: "k2"}))

	if len(uow.envs) != 2 {
		t.Fatalf("appended %d", len(uow.envs))
	}
	e1, e2 := uow.envs[0], uow.envs[1]
	if e1.ID.Version() != 7 || e1.ID.Variant() != uuid.RFC4122 || idMillis(e1.ID) != fixedNow.UnixMilli() {
		t.Fatalf("id = %v (v%d) ms=%d want %d", e1.ID, e1.ID.Version(), idMillis(e1.ID), fixedNow.UnixMilli())
	}
	if e1.Type != "puEvent" || e1.Topic != "puEvent" || e1.StreamKey != "k1" || !e1.OccurredAt.Equal(fixedNow) || e1.Seq != 1 {
		t.Fatalf("e1 = %+v", e1)
	}
	if e1.CorrelationID != "corr" || e1.CausationID != requestID.String() || requestID == uuid.Nil {
		t.Fatalf("e1 ids = %+v want cause %s", e1, requestID)
	}
	if e1.SchemaVersion != 1 || !reflect.DeepEqual(e1.Headers, map[string]string{"a": "3", "b": "2"}) || e1.TraceParent != "" {
		t.Fatalf("e1 = %+v", e1)
	}
	if uow.payloads[0] != `{"k":"k1","v":2}` {
		t.Fatalf("payload = %s", uow.payloads[0])
	}
	if e2.Type != "puTopicEvent" || e2.Topic != "orders" || e2.SchemaVersion != 3 || e2.Headers != nil || e2.CausationID != "" || e2.CorrelationID != "" || !e2.OccurredAt.Equal(fixedNow.Add(time.Second)) || e2.Seq != 2 {
		t.Fatalf("e2 = %+v", e2)
	}
	if idMillis(e2.ID) != fixedNow.Add(time.Second).UnixMilli() || e2.ID == e1.ID {
		t.Fatal("e2 id")
	}
}

func TestPublish_DurableErrors(t *testing.T) {
	m := mediator.New()
	mustNil(t, mediator.OnFunc(m, noopEvent[puEvent]))
	build(t, m)
	bg := context.Background()
	cases := []struct {
		name string
		ctx  context.Context
		ev   mediator.Notification
		want error
		text string
	}{
		{"no unit of work", bg, puEvent{K: "k"}, mediator.ErrNoUnitOfWork, ""},
		{"nil unit of work", mediator.WithUnitOfWork(bg, nil), puEvent{K: "k"}, mediator.ErrNoUnitOfWork, ""},
		{"read-only", mediator.WithUnitOfWork(bg, &fakeUoW{readOnly: true}), puEvent{K: "k"}, mediator.ErrDurablePublishInQuery, ""},
		{"empty stream key", mediator.WithUnitOfWork(bg, &fakeUoW{}), puEvent{}, nil, "empty stream key"},
		{"unregistered empty key", mediator.WithUnitOfWork(bg, &fakeUoW{}), puUnregistered{}, nil, "empty stream key"},
		{"unregistered bad name", mediator.WithUnitOfWork(bg, &fakeUoW{}), puBadName{}, nil, `name "bad name"`},
		{"encode failure", mediator.WithUnitOfWork(bg, &fakeUoW{}), puChan{}, nil, "encode event"},
		{"append failure", mediator.WithUnitOfWork(bg, &fakeUoW{err: errors.New("db down")}), puEvent{K: "k"}, nil, "db down"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := mediator.Publish(c.ctx, m, c.ev)
			if c.want != nil && !errors.Is(err, c.want) {
				t.Fatalf("got %v, want %v", err, c.want)
			}
			if c.text != "" {
				wantContains(t, err, c.text)
			}
		})
	}
}

func TestPublish_UnregisteredDurableStillAppends(t *testing.T) {
	m := mediator.New()
	var log []string
	mustNil(t, mediator.Use(m, rec{name: "N", log: &log}, mediator.Notifications()))
	build(t, m)
	uow := &fakeUoW{}
	ctx := mediator.WithUnitOfWork(context.Background(), uow)
	mustNil(t, mediator.Publish(ctx, m, puUnregistered{K: "u"}, mediator.Headers(map[string]string{"h": "v"})))
	mustNil(t, mediator.Publish(ctx, m, &puUnregisteredTopic{K: "t"}))
	if len(uow.envs) != 2 || len(log) != 0 {
		t.Fatalf("envs=%d log=%v", len(uow.envs), log)
	}
	if e := uow.envs[0]; e.Type != "puUnregistered" || e.Topic != "puUnregistered" || e.StreamKey != "u" || !reflect.DeepEqual(e.Headers, map[string]string{"h": "v"}) {
		t.Fatalf("e = %+v", e)
	}
	if e := uow.envs[1]; e.Type != "puUnregisteredTopic" || e.Topic != "aaa" || e.Headers != nil {
		t.Fatalf("e = %+v", e)
	}
}

func TestPublish_StrategyOverrideAndSuccess(t *testing.T) {
	for _, s := range []mediator.PublishStrategy{mediator.StopOnFirstError, mediator.ContinueOnError, mediator.Parallel} {
		m := mediator.New(mediator.WithPublishStrategy(s))
		runs := make(chan int, 3)
		for i := range 3 {
			mustNil(t, mediator.OnFunc(m, func(context.Context, puPlain) error { runs <- i; return nil }))
		}
		mustNil(t, mediator.OnFunc(m, noopEvent[puEvent]))
		build(t, m)
		mustNil(t, mediator.Publish(context.Background(), m, puPlain{}))
		if len(runs) != 3 {
			t.Fatalf("%v: %d runs", s, len(runs))
		}
		// A durable event whose handlers all succeed is still appended.
		uow := &fakeUoW{}
		mustNil(t, mediator.Publish(mediator.WithUnitOfWork(context.Background(), uow), m, puEvent{K: "k"}))
		if len(uow.envs) != 1 {
			t.Fatalf("%v: appended %d rows after a successful fan-out, want 1", s, len(uow.envs))
		}
	}
	// Per-call override: the mediator default stops at the first error, the
	// call continues.
	m := mediator.New()
	runs := 0
	mustNil(t, mediator.OnFunc(m, func(context.Context, puPlain) error { runs++; return errors.New("h1") }))
	mustNil(t, mediator.OnFunc(m, func(context.Context, puPlain) error { runs++; return nil }))
	build(t, m)
	err := mediator.Publish(context.Background(), m, puPlain{}, mediator.Strategy(mediator.ContinueOnError))
	if err == nil || runs != 2 {
		t.Fatalf("override: runs=%d err=%v", runs, err)
	}
	runs = 0
	err = mediator.Publish(context.Background(), m, puPlain{}, mediator.Strategy(mediator.StopOnFirstError))
	if err == nil || runs != 1 {
		t.Fatalf("stop: runs=%d err=%v", runs, err)
	}
	for _, c := range []struct {
		s    mediator.PublishStrategy
		want string
	}{
		{mediator.StopOnFirstError, "stop_on_first_error"}, {mediator.ContinueOnError, "continue_on_error"},
		{mediator.Parallel, "parallel"}, {mediator.PublishStrategy(9), "unknown"},
	} {
		if c.s.String() != c.want {
			t.Errorf("%d.String() = %s", c.s, c.s.String())
		}
	}
}

func TestPublishAll_OrdersDurableAppends(t *testing.T) {
	m := mediator.New()
	var handled []string
	mustNil(t, mediator.OnFunc(m, func(_ context.Context, e puPlain) error { handled = append(handled, "plain"); return nil }))
	mustNil(t, mediator.OnFunc(m, func(_ context.Context, e puEvent) error { handled = append(handled, "ev:"+e.K); return nil }))
	mustNil(t, mediator.OnFunc(m, noopEvent[puTopicEvent]))
	build(t, m)
	uow := &fakeUoW{}
	ctx := mediator.WithUnitOfWork(context.Background(), uow)
	events := []mediator.Notification{
		puTopicEvent{K: "z"},         // topic orders
		bPlainEvent{},                // non-durable, unregistered
		puEvent{K: "9"},              // topic puEvent
		&puUnregisteredTopic{K: "1"}, // topic aaa
		puPlain{V: 1},                // non-durable
		puEvent{K: "1", V: 1},        // topic puEvent
		puUnregistered{K: "x"},       // topic puUnregistered (derived name)
		puTopicEvent{K: "a"},         // topic orders
		puEvent{K: "1", V: 2},        // same topic and key as V 1: stays after it
	}
	mustNil(t, mediator.PublishAll(ctx, m, events))
	var order []string
	for _, e := range uow.envs {
		order = append(order, e.Topic+"/"+e.StreamKey)
	}
	want := []string{"aaa/1", "orders/a", "orders/z", "puEvent/1", "puEvent/1", "puEvent/9", "puUnregistered/x"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("append order = %v, want %v", order, want)
	}
	// Two events with the same topic and stream key keep their input order,
	// so their sequence numbers reflect it.
	if uow.payloads[3] != `{"k":"1","v":1}` || uow.payloads[4] != `{"k":"1","v":2}` {
		t.Fatalf("same-key payloads = %v", uow.payloads[3:5])
	}
	// Non-durable events run first, in their original relative order.
	if !reflect.DeepEqual(handled, []string{"plain", "ev:1", "ev:1", "ev:9"}) {
		t.Fatalf("handled = %v", handled)
	}
	// Empty and nil lists are no-ops; an error stops the batch.
	mustNil(t, mediator.PublishAll(ctx, m, nil))
	mustNil(t, mediator.PublishAll(ctx, m, []mediator.Notification{}))
	uow2 := &fakeUoW{}
	err := mediator.PublishAll(mediator.WithUnitOfWork(context.Background(), uow2), m, []mediator.Notification{puEvent{K: "b"}, puEvent{}, puEvent{K: "a"}})
	wantContains(t, err, "empty stream key")
	if len(uow2.envs) != 0 {
		t.Fatalf("appended before the failing event: %+v", uow2.envs)
	}
	err = mediator.PublishAll(context.Background(), m, []mediator.Notification{puPlain{}, puEvent{K: "a"}})
	if !errors.Is(err, mediator.ErrNoUnitOfWork) {
		t.Fatal(err)
	}
}

func TestDeliver(t *testing.T) {
	m := mediator.New()
	var log []string
	type seen struct {
		env    mediator.Envelope
		envOK  bool
		reqID  uuid.UUID
		cause  uuid.UUID
		corr   string
		depth  int
		state  *mediator.ConsumerState
		nested uuid.UUID
		event  puEvent
	}
	var s seen
	mustNil(t, mediator.HandleFunc(m, func(ctx context.Context, c pCmd) (int, error) {
		s.nested = mediator.CausationID(ctx)
		return mediator.Depth(ctx), nil
	}))
	mustNil(t, mediator.ConsumeFunc(m, "g", func(ctx context.Context, e puEvent) error {
		s.env, s.envOK = mediator.EnvelopeFrom(ctx)
		s.reqID, s.cause, s.corr, s.depth = mediator.RequestID(ctx), mediator.CausationID(ctx), mediator.CorrelationID(ctx), mediator.Depth(ctx)
		s.state, _ = mediator.ConsumerStateFrom(ctx)
		s.event = e
		if e.V == 99 {
			return errors.New("consumer failed")
		}
		d, err := mediator.Send(ctx, m, pCmd{})
		if d != 2 {
			t.Errorf("nested depth = %d", d)
		}
		return err
	}))
	mustNil(t, mediator.Use(m, rec{name: "req", log: &log}))
	mustNil(t, mediator.Use(m, rec{name: "ntf", log: &log}, mediator.Notifications()))
	mustNil(t, mediator.Use(m, rec{name: "csm", log: &log}, mediator.Consumers()))
	mustNil(t, mediator.Use(m, rec{name: "all", log: &log}, mediator.Everywhere()))
	ctx := context.Background()
	if err := m.Deliver(ctx, "g", mediator.Envelope{Type: "puEvent"}, nil); !errors.Is(err, mediator.ErrNotBuilt) {
		t.Fatal(err)
	}
	build(t, m)

	cause := uuid.MustParse("22222222-2222-7222-8222-222222222222")
	env := mediator.Envelope{ID: mediator.NewID(fixedNow), Type: "puEvent", Topic: "puEvent", StreamKey: "a", Seq: 4, CorrelationID: "corr-x", CausationID: cause.String()}
	mustNil(t, m.Deliver(ctx, "g", env, []byte(`{"k":"a","v":1}`)))
	if !s.envOK || !reflect.DeepEqual(s.env, env) || s.reqID != env.ID || s.cause != cause || s.corr != "corr-x" || s.depth != 1 || s.event.K != "a" || s.event.V != 1 {
		t.Fatalf("seen = %+v", s)
	}
	if s.state == nil || s.state.Attempt != 1 || s.state.Duplicate {
		t.Fatalf("state = %+v", s.state)
	}
	if s.nested != env.ID {
		t.Fatalf("nested send caused by %s, want %s", s.nested, env.ID)
	}
	// Only Consumers-scoped behaviors run, then the nested Send's own chain.
	if got := forward(log); !reflect.DeepEqual(got, []string{"csm", "all", "req", "all"}) {
		t.Fatalf("chain = %v", got)
	}

	// Consumer state supplied by the transport is kept.
	st := &mediator.ConsumerState{Attempt: 3}
	mustNil(t, m.Deliver(mediator.WithConsumerState(ctx, st), "g", env, []byte(`{"k":"a"}`)))
	if s.state != st {
		t.Fatal("consumer state replaced")
	}
	// An unparsable causation ID is the zero UUID.
	env2 := env
	env2.CausationID = "not-a-uuid"
	mustNil(t, m.Deliver(ctx, "g", env2, []byte(`{"k":"a"}`)))
	if s.cause != uuid.Nil {
		t.Fatal("cause")
	}
	env2.CausationID = ""
	mustNil(t, m.Deliver(ctx, "g", env2, []byte(`{"k":"a"}`)))
	if s.cause != uuid.Nil {
		t.Fatal("cause")
	}

	// Errors.
	if err := m.Deliver(ctx, "other", env, []byte(`{}`)); !errors.Is(err, mediator.ErrHandlerNotFound) {
		t.Fatalf("unknown group: %v", err)
	}
	if err := m.Deliver(ctx, "g", mediator.Envelope{Type: "nope"}, []byte(`{}`)); !errors.Is(err, mediator.ErrHandlerNotFound) {
		t.Fatalf("unknown type: %v", err)
	}
	for _, payload := range [][]byte{nil, []byte(``), []byte(`{`), []byte(`[]`), []byte(`{"k":1}`)} {
		if err := m.Deliver(ctx, "g", env, payload); mediator.CodeOf(err) != mediator.CodeBadRequest || !strings.Contains(err.Error(), "decode event puEvent") {
			t.Fatalf("payload %q: %v", payload, err)
		}
	}
	if err := m.Deliver(ctx, "g", env, []byte(`{"k":"a","v":99}`)); err == nil || err.Error() != "consumer failed" {
		t.Fatalf("handler error: %v", err)
	}
}

// TestPublish_NestedCallDoesNotInheritOptions pins that a PublishOption
// configures one call: a Publish made from a handler of an outer call that
// passed options, itself passing none, runs with the mediator defaults and no
// headers, on every path that reads the options (fan-out strategy, fan-out
// headers, and the unregistered-durable append).
func TestPublish_NestedCallDoesNotInheritOptions(t *testing.T) {
	t.Run("strategy", func(t *testing.T) {
		m := mediator.New() // default: StopOnFirstError
		innerRuns := 0
		var innerOpts []mediator.PublishOption
		mustNil(t, mediator.OnFunc(m, func(context.Context, bPlainEvent) error { innerRuns++; return errors.New("inner h1") }))
		mustNil(t, mediator.OnFunc(m, func(context.Context, bPlainEvent) error { innerRuns++; return nil }))
		mustNil(t, mediator.OnFunc(m, func(ctx context.Context, _ puPlain) error {
			return mediator.Publish(ctx, m, bPlainEvent{}, innerOpts...)
		}))
		build(t, m)
		// The outer call continues on error; the inner call passes no options
		// and must use the mediator default, which stops at the first error.
		err := mediator.Publish(context.Background(), m, puPlain{}, mediator.Strategy(mediator.ContinueOnError))
		if err == nil || !strings.Contains(err.Error(), "inner h1") {
			t.Fatalf("outer err = %v, want the inner handler's failure", err)
		}
		if innerRuns != 1 {
			t.Fatalf("inner handlers run = %d, want 1: the nested call inherited the outer strategy", innerRuns)
		}
		// The inner call's own option still applies.
		innerRuns = 0
		innerOpts = []mediator.PublishOption{mediator.Strategy(mediator.ContinueOnError)}
		err = mediator.Publish(context.Background(), m, puPlain{}, mediator.Strategy(mediator.ContinueOnError))
		if err == nil || innerRuns != 2 {
			t.Fatalf("inner override: runs=%d err=%v, want 2 runs and the error", innerRuns, err)
		}
	})

	t.Run("headers", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.OnFunc(m, func(ctx context.Context, _ puEvent) error {
			return mediator.Publish(ctx, m, puTopicEvent{K: "inner"})
		}))
		mustNil(t, mediator.OnFunc(m, noopEvent[puTopicEvent]))
		build(t, m)
		uow := &fakeUoW{}
		ctx := mediator.WithUnitOfWork(context.Background(), uow)
		mustNil(t, mediator.Publish(ctx, m, puEvent{K: "outer"}, mediator.Headers(map[string]string{"outer": "1"})))
		if len(uow.envs) != 2 {
			t.Fatalf("appended %d rows, want 2", len(uow.envs))
		}
		byType := map[string]mediator.Envelope{}
		for _, e := range uow.envs {
			byType[e.Type] = e
		}
		if outer := byType["puEvent"]; !reflect.DeepEqual(outer.Headers, map[string]string{"outer": "1"}) {
			t.Fatalf("outer envelope headers = %v", outer.Headers)
		}
		if inner := byType["puTopicEvent"]; len(inner.Headers) != 0 {
			t.Fatalf("nested envelope inherited headers %v", inner.Headers)
		}
	})

	t.Run("unregistered durable", func(t *testing.T) {
		m := mediator.New()
		mustNil(t, mediator.OnFunc(m, func(ctx context.Context, _ puPlain) error {
			return mediator.Publish(ctx, m, puUnregistered{K: "u"})
		}))
		build(t, m)
		uow := &fakeUoW{}
		ctx := mediator.WithUnitOfWork(context.Background(), uow)
		mustNil(t, mediator.Publish(ctx, m, puPlain{}, mediator.Headers(map[string]string{"outer": "1"})))
		if len(uow.envs) != 1 || uow.envs[0].Type != "puUnregistered" {
			t.Fatalf("envs = %+v", uow.envs)
		}
		if h := uow.envs[0].Headers; len(h) != 0 {
			t.Fatalf("nested unregistered envelope inherited headers %v", h)
		}
	})
}
