package redisx

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

type unitEvent struct {
	mediator.Event
	Key string `json:"key"`
}

func (e unitEvent) StreamKey() string { return e.Key }

type sharedEvent struct {
	mediator.Event
	Key string `json:"key"`
}

func (e sharedEvent) StreamKey() string { return e.Key }
func (sharedEvent) Topic() string       { return "shared" }

func TestConfig_WithDefaults(t *testing.T) {
	c := Config{}.WithDefaults()
	if c.Addr != "localhost:6379" || c.Prefix != "mediator" || c.PartitionsPerTopic != 16 || c.LeaseTTL != 15*time.Second ||
		c.LeaseRenew != 5*time.Second || c.ClaimMinIdle != 30*time.Second || c.ReadBlock != time.Second || c.ReadBatch != 64 ||
		c.MaxAttempts != 10 || c.CacheDefaultTTL != 5*time.Minute || c.ReplyStreamMaxLen != 10000 || c.AssumeNoEviction {
		t.Fatalf("defaults: %+v", c)
	}
	if c.NodeID == "" || !strings.Contains(c.NodeID, "-") {
		t.Fatalf("node id %q", c.NodeID)
	}
	first, again := DefaultNodeID(), DefaultNodeID()
	if first == again {
		t.Fatal("node ids should be unique")
	}
	custom := Config{Prefix: "x", NodeID: "n", PartitionsPerTopic: 4}.WithDefaults()
	if custom.Prefix != "x" || custom.NodeID != "n" || custom.PartitionsPerTopic != 4 || custom.Keys().Prefix != "x" {
		t.Fatalf("custom: %+v", custom)
	}
}

func TestIsRedisTransient(t *testing.T) {
	transient := []error{
		redis.ErrClosed, io.EOF, io.ErrUnexpectedEOF,
		&net.OpError{Op: "dial", Err: errors.New("connection refused")},
		errors.New("LOADING Redis is loading the dataset in memory"),
		errors.New("READONLY You can't write against a read only replica."),
		errors.New("CLUSTERDOWN The cluster is down"),
		errors.New("TRYAGAIN x"), errors.New("MASTERDOWN x"), errors.New("BUSY x"),
		errors.New("dial tcp: connection refused"), errors.New("read: connection reset by peer"),
		errors.New("write: broken pipe"), errors.New("i/o timeout"), errors.New("redis: connection pool timeout"),
		errors.New("use of closed network connection"), errors.New("redis: client is closed"),
	}
	for _, err := range transient {
		if !IsRedisTransient(err) {
			t.Errorf("IsRedisTransient(%v) = false", err)
		}
		if !mediator.IsTransient(err) {
			t.Errorf("mediator.IsTransient(%v) = false; classifier not registered", err)
		}
	}
	for _, err := range []error{nil, redis.Nil, errors.New("ERR wrong number of arguments"), errors.New("NOGROUP x"), errors.New("BUSYGROUP x")} {
		if IsRedisTransient(err) {
			t.Errorf("IsRedisTransient(%v) = true", err)
		}
	}
	if !isBusyGroup(errors.New("BUSYGROUP Consumer Group name already exists")) || isBusyGroup(nil) {
		t.Error("isBusyGroup")
	}
	if !isNoGroup(errors.New("NOGROUP No such key")) || isNoGroup(errors.New("x")) {
		t.Error("isNoGroup")
	}
	if nonNil(redis.Nil) != nil || nonNil(io.EOF) == nil {
		t.Error("nonNil")
	}
}

func buildConsumerMediator(t *testing.T) *mediator.Mediator {
	t.Helper()
	m := mediator.New(mediator.WithNodeID("med-node"), mediator.WithLogger(quietLogger()))
	noop := func(context.Context, unitEvent) error { return nil }
	noop2 := func(context.Context, sharedEvent) error { return nil }
	if err := mediator.ConsumeFunc(m, "g1", noop); err != nil {
		t.Fatal(err)
	}
	if err := mediator.ConsumeFunc(m, "g1", noop2, mediator.StrictOrder(true), mediator.MaxAttempts(2)); err != nil {
		t.Fatal(err)
	}
	if err := mediator.ConsumeFunc(m, "g2", noop2); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestNewConsumers_Scopes(t *testing.T) {
	m := buildConsumerMediator(t)
	obs := &recordingObserver{}
	c := NewConsumers(m, nil, Config{PartitionsPerTopic: 4}, &counterFencing{},
		WithObserver(obs), WithClock(testkit.RealClock{}), WithLogger(quietLogger()), withBackoff(defaultBackoff))
	want := []ScopeKey{{"g1", "shared"}, {"g1", "unitEvent"}, {"g2", "shared"}}
	got := c.Scopes()
	if len(got) != len(want) {
		t.Fatalf("scopes %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("scopes %v, want %v", got, want)
		}
	}
	if c.NodeID() != "med-node" {
		t.Fatalf("node id %q should come from the mediator", c.NodeID())
	}
	if reg, ok := c.regs["g1"]["sharedEvent"]; !ok || !reg.StrictOrder || reg.MaxAttempts != 2 {
		t.Fatalf("registration %+v %v", reg, ok)
	}
	if err := c.Healthy(); err == nil {
		t.Fatal("not running should be unhealthy")
	}
	st := c.Stats()
	if st.Running || st.NodeID != "med-node" || len(st.Partitions) != 0 || st.Halted != 0 {
		t.Fatalf("stats %+v", st)
	}
	c.count("g1", OutcomeOK)
	c.setHalted(leaseKey{"g1", "shared", 1}, "1-0", true)
	if err := c.Healthy(); err == nil {
		t.Fatal("expected unhealthy")
	}
	st = c.Stats()
	if st.Processed[OutcomeKey{"g1", OutcomeOK}] != 1 || st.Halted != 1 || obs.processed != 1 || obs.halted != 1 {
		t.Fatalf("stats %+v obs %+v", st, obs)
	}
	c.setHalted(leaseKey{"g1", "shared", 1}, "1-0", false)
	if c.Stats().Halted != 0 {
		t.Fatal("unhalt")
	}
	// A running instance whose lease loop stopped ticking is unhealthy.
	c.running.Store(true)
	c.lm.mu.Lock()
	c.lm.lastTick = time.Now().Add(-time.Hour)
	c.lm.mu.Unlock()
	if err := c.Healthy(); err == nil || !strings.Contains(err.Error(), "stalled") {
		t.Fatalf("stalled loop: %v", err)
	}
	c.lm.mu.Lock()
	c.lm.lastTick = time.Now()
	c.lm.mu.Unlock()
	if err := c.Healthy(); err != nil {
		t.Fatalf("fresh tick: %v", err)
	}
	c.running.Store(false)
	if ConsumerNodeFrom(withConsumerNode(context.Background(), "n1")) != "n1" || ConsumerNodeFrom(context.Background()) != "" {
		t.Fatal("ConsumerNodeFrom")
	}
	fake := newFakeLeaseStore(testkit.RealClock{})
	explicit := NewConsumers(m, nil, Config{NodeID: "explicit"}, &counterFencing{}, WithObserver(nil), WithClock(nil), WithLogger(nil), withLeaseStore(fake))
	if explicit.NodeID() != "explicit" || explicit.lm.store != leaseStore(fake) {
		t.Fatal("explicit node id / lease store")
	}
}

func TestConsumers_IdleRun(t *testing.T) {
	m := mediator.New(mediator.WithLogger(quietLogger()))
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	c := NewConsumers(m, nil, Config{AssumeNoEviction: true}, &counterFencing{})
	if len(c.Scopes()) != 0 {
		t.Fatal("scopes")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for c.Healthy() != nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.Healthy(); err != nil {
		t.Fatalf("idle consumers unhealthy: %v", err)
	}
	if !c.Stats().Running {
		t.Fatal("running")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if c.Healthy() == nil {
		t.Fatal("stopped consumers should be unhealthy")
	}
}

// recordingObserver counts Observer calls.
type recordingObserver struct {
	acquired, ended, owned, processed, halted, pending, dlq int
}

func (o *recordingObserver) LeaseAcquired(string, string, int, int64)          { o.acquired++ }
func (o *recordingObserver) LeaseEnded(string, string, int, string)            { o.ended++ }
func (o *recordingObserver) LeasesOwned(string, string, int)                   { o.owned++ }
func (o *recordingObserver) Processed(string, string)                          { o.processed++ }
func (o *recordingObserver) Halted(string, string, int, bool)                  { o.halted++ }
func (o *recordingObserver) Pending(string, string, int, int64, time.Duration) { o.pending++ }
func (o *recordingObserver) DLQSize(string, int64)                             { o.dlq++ }

func TestNopObserver(t *testing.T) {
	var o Observer = NopObserver{}
	o.LeaseAcquired("g", "t", 0, 1)
	o.LeaseEnded("g", "t", 0, LeaseEndLost)
	o.LeasesOwned("g", "t", 1)
	o.Processed("g", OutcomeOK)
	o.Halted("g", "t", 0, true)
	o.Pending("g", "t", 0, 1, time.Second)
	o.DLQSize("g", 0)
}

func TestNextStreamID(t *testing.T) {
	cases := []struct {
		in, want string
		ok       bool
	}{
		{"5-3", "5-4", true},
		{"1700000000000-0", "1700000000000-1", true},
		{"5-18446744073709551615", "6-0", true},
		{"18446744073709551615-18446744073709551615", "", false},
		{"nodash", "", false},
		{"5-x", "", false},
		{"x-1", "", false},
	}
	for _, c := range cases {
		got, ok := nextStreamID(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("nextStreamID(%q) = %q %v, want %q %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestCompareStreamIDs(t *testing.T) {
	cases := []struct {
		a, b string
		want int
		ok   bool
	}{
		{"5-3", "5-3", 0, true},
		{"5-3", "5-4", -1, true},
		{"5-4", "5-3", 1, true},
		{"5-9", "6-0", -1, true},
		{"10-0", "9-99", 1, true}, // numeric, not lexical
		{"1700000000000-0", "1700000000001-0", -1, true},
		{"18446744073709551615-18446744073709551615", "18446744073709551615-18446744073709551614", 1, true},
		{"5", "5-0", 0, true}, // a missing sequence reads as 0
		{"5", "5-1", -1, true},
		{"", "5-0", 0, false},
		{"5-0", "", 0, false},
		{"x-1", "5-0", 0, false},
		{"5-x", "5-0", 0, false},
		{"5-0", "5-", 0, false},
		{"-5", "5-0", 0, false},
	}
	for _, c := range cases {
		got, ok := compareStreamIDs(c.a, c.b)
		if got != c.want || ok != c.ok {
			t.Errorf("compareStreamIDs(%q, %q) = %d %v, want %d %v", c.a, c.b, got, ok, c.want, c.ok)
		}
	}
}
