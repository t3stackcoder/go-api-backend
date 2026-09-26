//go:build integration

package redisx

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

type remoteCmd struct {
	mediator.Command[remoteRes]
	Name    string `json:"name"`
	Fail    bool   `json:"fail"`
	SleepMS int    `json:"sleepMs"`
}

type remoteRes struct {
	Greeting string `json:"greeting"`
	Subject  string `json:"subject"`
	Tenant   string `json:"tenant"`
	Corr     string `json:"corr"`
	Idem     string `json:"idem"`
	Big      int64  `json:"big"`
}

// idemStub mimics the idempotency behavior with a map: the first execution
// per key stores the response, later ones replay it without the handler.
type idemStub struct {
	mu   sync.Mutex
	resp map[string]any
}

func (s *idemStub) Name() string { return mediator.NameIdempotency }

func (s *idemStub) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	key, ok := mediator.IdempotencyKeyFrom(ctx)
	if !ok {
		return next(ctx, req)
	}
	s.mu.Lock()
	if r, seen := s.resp[key]; seen {
		s.mu.Unlock()
		return r, nil
	}
	s.mu.Unlock()
	res, err := next(ctx, req)
	if err == nil {
		s.mu.Lock()
		s.resp[key] = res
		s.mu.Unlock()
	}
	return res, err
}

// serverMediator has the handler; executions counts handler runs.
func serverMediator(t *testing.T, executions *atomic.Int64) *mediator.Mediator {
	t.Helper()
	m := mediator.New(mediator.WithNodeID("server"), mediator.WithLogger(quietLogger()))
	if err := mediator.Use(m, &idemStub{resp: map[string]any{}}, mediator.Commands()); err != nil {
		t.Fatal(err)
	}
	err := mediator.HandleFunc(m, func(ctx context.Context, c remoteCmd) (remoteRes, error) {
		executions.Add(1)
		if c.SleepMS > 0 {
			select {
			case <-time.After(time.Duration(c.SleepMS) * time.Millisecond):
			case <-ctx.Done():
				return remoteRes{}, ctx.Err()
			}
		}
		if c.Fail {
			return remoteRes{}, mediator.E(mediator.CodeNotFound, "no such thing").WithDetail("name", c.Name)
		}
		if c.Name == "" {
			return remoteRes{}, (&mediator.ValidationError{}).Add("/name", "required", "must not be empty")
		}
		if c.Name == "plain-error" {
			return remoteRes{}, errors.New("secret internal detail")
		}
		p := authz.PrincipalFrom(ctx)
		idem, _ := mediator.IdempotencyKeyFrom(ctx)
		return remoteRes{Greeting: "hello " + c.Name, Subject: p.Subject, Tenant: p.Tenant, Corr: mediator.CorrelationID(ctx), Idem: idem, Big: 9007199254740993}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	return m
}

// clientMediator declares the type and dispatches through r.
func clientMediator(t *testing.T, r *Remote) *mediator.Mediator {
	t.Helper()
	m := mediator.New(mediator.WithRemote(r), mediator.WithLogger(quietLogger()))
	if err := mediator.Declare[remoteCmd, remoteRes](m); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	return m
}

type component struct {
	cancel  context.CancelFunc
	done    chan error
	stopped bool
}

func startComponent(t *testing.T, c mediator.Component) *component {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	comp := &component{cancel: cancel, done: make(chan error, 1)}
	go func() { comp.done <- c.Run(ctx) }()
	eventually(t, 10*time.Second, "component healthy", func() bool { return c.Healthy() == nil })
	t.Cleanup(func() { comp.stop(t) })
	return comp
}

func (c *component) stop(t *testing.T) {
	t.Helper()
	if c.stopped {
		return
	}
	c.stopped = true
	c.cancel()
	select {
	case err := <-c.done:
		if err != nil {
			t.Errorf("component: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Error("component did not stop")
	}
}

func TestRemote_RoundTrip(t *testing.T) {
	t.Parallel()
	cfg := testConfig("client")
	client := newTestClient(t, cfg)
	var executions atomic.Int64
	serverM := serverMediator(t, &executions)
	scfg := cfg
	scfg.NodeID = ""
	server := NewRemoteServer(serverM, client, scfg)
	if server.NodeID() != "server" || len(server.Served()) != 1 || server.Served()[0] != "remoteCmd" {
		t.Fatalf("server %s serves %v", server.NodeID(), server.Served())
	}
	if err := server.Healthy(); err == nil {
		t.Fatal("not running should be unhealthy")
	}
	startComponent(t, server)
	remote := NewRemote(client, cfg, WithRemoteLogger(quietLogger()), WithRemoteClock(testkit.RealClock{}))
	reader := NewReplyReader(remote)
	if err := reader.Healthy(); err == nil {
		t.Fatal("reader not running should be unhealthy")
	}
	startComponent(t, reader)
	clientM := clientMediator(t, remote)

	ctx := authz.WithPrincipal(context.Background(), authz.Principal{Subject: "alice", Tenant: "acme", Roles: []string{"admin"}, Claims: map[string]any{"n": 1}})
	ctx = mediator.WithCorrelationID(ctx, "corr-123")
	ctx = mediator.WithIdempotencyKey(ctx, "key-1")
	res, err := mediator.Send(ctx, clientM, remoteCmd{Name: "bob"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Greeting != "hello bob" || res.Subject != "alice" || res.Tenant != "acme" || res.Corr != "corr-123" || res.Idem != "key-1" || res.Big != 9007199254740993 {
		t.Fatalf("response %+v", res)
	}
	// Same key: replayed by the idempotency stub, handler not re-run.
	res2, err := mediator.Send(ctx, clientM, remoteCmd{Name: "bob"})
	if err != nil || res2 != res || executions.Load() != 1 {
		t.Fatalf("replay: %+v %v executions=%d", res2, err, executions.Load())
	}
	// Anonymous, no key, no correlation: the server assigns one.
	res, err = mediator.Send(context.Background(), clientM, remoteCmd{Name: "eve"})
	if err != nil || res.Subject != "" || res.Idem != "" || res.Corr == "" {
		t.Fatalf("anonymous: %+v %v", res, err)
	}
	// Errors come back as *mediator.Error with code, message, and details.
	_, err = mediator.Send(context.Background(), clientM, remoteCmd{Name: "x", Fail: true})
	var me *mediator.Error
	if !errors.As(err, &me) || me.Code != mediator.CodeNotFound || me.Message != "no such thing" || me.Details["name"] != "x" {
		t.Fatalf("error reply: %v", err)
	}
	if remote.Waiting() != 0 {
		t.Fatal("waiters leaked")
	}
	// Validation errors carry their fields; unknown errors hide their text.
	_, err = mediator.Send(context.Background(), clientM, remoteCmd{})
	if !errors.As(err, &me) || me.Code != mediator.CodeValidation || me.Details["fields"] == nil {
		t.Fatalf("validation reply: %v", err)
	}
	_, err = mediator.Send(context.Background(), clientM, remoteCmd{Name: "plain-error"})
	if !errors.As(err, &me) || me.Code != mediator.CodeInternal || strings.Contains(me.Message, "secret") {
		t.Fatalf("internal reply: %v", err)
	}
	// Hand-crafted requests: unknown name, garbage body, garbage principal,
	// an expired deadline, and malformed entries are answered or
	// acknowledged, never stuck.
	manual := func(call string, fields map[string]any) reply {
		t.Helper()
		ch := remote.w.add(call)
		defer remote.w.remove(call)
		values := map[string]any{RPCFieldCall: call, RPCFieldNode: "client", RPCFieldName: "remoteCmd", RPCFieldBody: "{}"}
		for k, v := range fields {
			values[k] = v
		}
		if err := client.XAdd(context.Background(), &redis.XAddArgs{Stream: cfg.Keys().RPC("remoteCmd"), Values: values}).Err(); err != nil {
			t.Fatal(err)
		}
		select {
		case rep := <-ch:
			return rep
		case <-time.After(10 * time.Second):
			t.Fatalf("no reply for %s", call)
			return reply{}
		}
	}
	if _, err := decodeReply(manual("m-unknown", map[string]any{RPCFieldName: "nope"}), nil); !errors.Is(err, mediator.ErrHandlerNotFound) {
		t.Fatalf("unknown name: %v", err)
	}
	if _, err := decodeReply(manual("m-body", map[string]any{RPCFieldBody: "{"}), nil); mediator.CodeOf(err) != mediator.CodeBadRequest {
		t.Fatalf("garbage body: %v", err)
	}
	if _, err := decodeReply(manual("m-principal", map[string]any{RPCFieldPrincipal: "{"}), nil); mediator.CodeOf(err) != mediator.CodeBadRequest {
		t.Fatalf("garbage principal: %v", err)
	}
	if _, err := decodeReply(manual("m-deadline", map[string]any{RPCFieldDeadline: "1", RPCFieldName: ""}), reflect.TypeFor[remoteRes]()); mediator.CodeOf(err) != mediator.CodeTimeout {
		t.Fatalf("expired deadline: %v", err)
	}
	if err := client.XAdd(context.Background(), &redis.XAddArgs{Stream: cfg.Keys().RPC("remoteCmd"), Values: map[string]any{RPCFieldBody: "{}"}}).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "malformed request acknowledged", func() bool {
		p, err := client.XPending(context.Background(), cfg.Keys().RPC("remoteCmd"), RPCGroup).Result()
		return err == nil && p.Count == 0
	})
	// Concurrent calls are matched to their own replies.
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := strings.Repeat("z", i+1)
			r, err := mediator.Send(context.Background(), clientM, remoteCmd{Name: name})
			if err != nil {
				errs <- err
				return
			}
			if r.Greeting != "hello "+name {
				errs <- errors.New("mismatched reply " + r.Greeting)
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// A reply nobody waits for is dropped without harm.
	if err := client.XAdd(context.Background(), &redis.XAddArgs{Stream: cfg.Keys().Reply("client"), Values: map[string]any{ReplyFieldCall: "ghost", ReplyFieldStatus: "ok", ReplyFieldBody: "{}"}}).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := mediator.Send(context.Background(), clientM, remoteCmd{Name: "after-ghost"}); err != nil {
		t.Fatal(err)
	}
	// A stale heartbeat makes the server unhealthy until the next beat.
	server.lastBeat.Store(time.Now().Add(-time.Hour).UnixMilli())
	if err := server.Healthy(); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale heartbeat: %v", err)
	}
	server.lastBeat.Store(time.Now().UnixMilli())
	// The request group vanishes (Redis loss): the server recreates it and
	// keeps serving.
	if err := client.XGroupDestroy(context.Background(), cfg.Keys().RPC("remoteCmd"), RPCGroup).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "request group recreated", func() bool {
		groups, err := client.XInfoGroups(context.Background(), cfg.Keys().RPC("remoteCmd")).Result()
		return err == nil && len(groups) == 1
	})
	if _, err := mediator.Send(context.Background(), clientM, remoteCmd{Name: "after-nogroup"}); err != nil {
		t.Fatal(err)
	}
	// The reply stream is trimmed after a burst of replies.
	pipe := client.Pipeline()
	for i := 0; i < 1100; i++ {
		pipe.XAdd(context.Background(), &redis.XAddArgs{Stream: cfg.Keys().Reply("client"), Values: map[string]any{ReplyFieldCall: "junk", ReplyFieldStatus: "ok", ReplyFieldBody: "{}"}})
	}
	if _, err := pipe.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "reply stream trimmed", func() bool {
		n, err := client.XLen(context.Background(), cfg.Keys().Reply("client")).Result()
		return err == nil && n < 1100
	})
	if _, err := mediator.Send(context.Background(), clientM, remoteCmd{Name: "after-trim"}); err != nil {
		t.Fatal(err)
	}
}

func TestRemote_NoHandlerAndTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := testConfig("client")
	client := newTestClient(t, cfg)
	remote := NewRemote(client, cfg)
	startComponent(t, NewReplyReader(remote))
	clientM := clientMediator(t, remote)

	start := time.Now()
	_, err := mediator.Send(ctx, clientM, remoteCmd{Name: "nobody"})
	if !errors.Is(err, mediator.ErrHandlerNotFound) || time.Since(start) > 2*time.Second {
		t.Fatalf("no server: %v after %s", err, time.Since(start))
	}
	// A stale heartbeat does not count.
	stale := float64(time.Now().Add(-2 * HandlerStaleAfter).UnixMilli())
	if err := client.ZAdd(ctx, cfg.Keys().Handlers("remoteCmd"), redis.Z{Score: stale, Member: "dead"}).Err(); err != nil {
		t.Fatal(err)
	}
	remote.InvalidateView("remoteCmd")
	if _, err := mediator.Send(ctx, clientM, remoteCmd{Name: "nobody"}); !errors.Is(err, mediator.ErrHandlerNotFound) {
		t.Fatalf("stale heartbeat: %v", err)
	}
	// A fresh heartbeat of a node that never reads: the call times out at the deadline.
	if err := client.ZAdd(ctx, cfg.Keys().Handlers("remoteCmd"), redis.Z{Score: float64(time.Now().UnixMilli()), Member: "ghost"}).Err(); err != nil {
		t.Fatal(err)
	}
	remote.InvalidateView("remoteCmd")
	tctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	start = time.Now()
	_, err = mediator.Send(tctx, clientM, remoteCmd{Name: "ghost"})
	if mediator.CodeOf(err) != mediator.CodeTimeout || time.Since(start) < 400*time.Millisecond {
		t.Fatalf("timeout: %v after %s", err, time.Since(start))
	}
	if remote.Waiting() != 0 {
		t.Fatal("waiter leaked")
	}
	// The cached view answers without a lookup for a while.
	tctx2, cancel2 := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel2()
	if _, err := mediator.Send(tctx2, clientM, remoteCmd{Name: "ghost"}); mediator.CodeOf(err) != mediator.CodeTimeout {
		t.Fatalf("cached view: %v", err)
	}
	// Redis failures surface as unavailable.
	closed := NewRemote(closedClient(t, cfg), cfg)
	info, _ := clientM.Lookup("remoteCmd")
	if _, err := closed.Send(ctx, info, remoteCmd{}); mediator.CodeOf(err) != mediator.CodeUnavailable {
		t.Fatalf("closed lookup: %v", err)
	}
	closed.mu.Lock()
	closed.view["remoteCmd"] = handlersView{at: time.Now(), nodes: []string{"x"}}
	closed.mu.Unlock()
	if _, err := closed.Send(ctx, info, remoteCmd{}); mediator.CodeOf(err) != mediator.CodeUnavailable {
		t.Fatalf("closed xadd: %v", err)
	}
	if _, err := closed.Send(ctx, info, make(chan int)); mediator.CodeOf(err) != mediator.CodeInternal {
		t.Fatalf("unencodable: %v", err)
	}
	// A server with nothing to serve idles.
	empty := mediator.New(mediator.WithLogger(quietLogger()))
	if err := empty.Build(); err != nil {
		t.Fatal(err)
	}
	idle := NewRemoteServer(empty, client, cfg)
	startComponent(t, idle)
	if err := idle.Healthy(); err != nil {
		t.Fatal(err)
	}
	// A server on a closed client fails to start.
	broken := NewRemoteServer(serverMediator(t, &atomic.Int64{}), closedClient(t, cfg), cfg)
	if err := broken.Run(ctx); err == nil {
		t.Fatal("server on closed client started")
	}
	// A reader on a closed client keeps retrying until canceled.
	rctx, rcancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer rcancel()
	if err := NewReplyReader(closed).Run(rctx); err != nil {
		t.Fatal(err)
	}
}

func TestRemote_KeyedRequestSurvivesServerCrash(t *testing.T) {
	t.Parallel()
	cfg := testConfig("client")
	client := newTestClient(t, cfg)
	var executions atomic.Int64
	serverM := serverMediator(t, &executions)

	// server1 "crashes" once at each stage: after reading (before
	// executing), after executing (before replying), and after replying
	// (before acknowledging).
	var crashed atomic.Bool
	var stageMu sync.Mutex
	nextStage := "read"
	s1cfg := cfg
	s1cfg.NodeID = "s1"
	s1 := NewRemoteServer(serverM, client, s1cfg, withServerHook(func(stage, call string) bool {
		stageMu.Lock()
		defer stageMu.Unlock()
		if stage == nextStage && !crashed.Load() {
			crashed.Store(true)
			switch nextStage {
			case "read":
				nextStage = "executed"
			case "executed":
				nextStage = "replied"
			default:
				nextStage = ""
			}
			return false
		}
		return true
	}), WithServerClock(testkit.RealClock{}), WithServerLogger(quietLogger()))
	s1comp := startComponent(t, s1)
	s2cfg := cfg
	s2cfg.NodeID = "s2"
	s2 := NewRemoteServer(serverM, client, s2cfg)
	startComponent(t, s2)

	remote := NewRemote(client, cfg)
	startComponent(t, NewReplyReader(remote))
	clientM := clientMediator(t, remote)

	ctx := mediator.WithIdempotencyKey(context.Background(), "crash-key")
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	// Two servers read the same stream; whichever gets the entry first
	// might be s2, so send until s1 has crashed at every stage. Each
	// abandoned keyed entry is claimed by s2 after ClaimMinIdle and
	// re-executed; the idempotency stub replays the stored response, so the
	// handler runs exactly once for the key and every reply is identical.
	var first remoteRes
	for stage := 0; stage < 3; stage++ {
		crashed.Store(false)
		for i := 0; ; i++ {
			res, err := mediator.Send(ctx, clientM, remoteCmd{Name: "durable"})
			if err != nil {
				t.Fatal(err)
			}
			if res.Greeting != "hello durable" || res.Idem != "crash-key" {
				t.Fatalf("response %+v", res)
			}
			if stage == 0 && i == 0 {
				first = res
			} else if res != first {
				t.Fatalf("reply changed: %+v vs %+v", res, first)
			}
			if crashed.Load() {
				break
			}
			if i > 50 {
				t.Fatalf("s1 never took a request at stage %d", stage)
			}
		}
		eventually(t, 10*time.Second, "abandoned entry claimed and acknowledged", func() bool {
			p, err := client.XPending(context.Background(), cfg.Keys().RPC("remoteCmd"), RPCGroup).Result()
			return err == nil && p.Count == 0
		})
	}
	if executions.Load() != 1 {
		t.Fatalf("handler executed %d times for one key", executions.Load())
	}
	s1comp.stop(t)

	// Without a key the crashed request is never re-executed: the caller
	// times out and the handler ran once.
	crashed.Store(false)
	s3cfg := cfg
	s3cfg.NodeID = "s3"
	s3 := NewRemoteServer(serverM, client, s3cfg, withServerHook(func(stage, call string) bool {
		return stage != "executed" || !crashed.CompareAndSwap(false, true)
	}))
	startComponent(t, s3)
	before := executions.Load()
	timeouts := 0
	for i := 0; i < 50 && !crashed.Load(); i++ {
		tctx, tcancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := mediator.Send(tctx, clientM, remoteCmd{Name: "once"})
		tcancel()
		if err != nil {
			if mediator.CodeOf(err) != mediator.CodeTimeout {
				t.Fatalf("unexpected error %v", err)
			}
			timeouts++
		}
	}
	if !crashed.Load() || timeouts != 1 {
		t.Fatalf("crashed=%v timeouts=%d", crashed.Load(), timeouts)
	}
	executedBefore := executions.Load()
	time.Sleep(3 * cfg.ClaimMinIdle)
	if executions.Load() != executedBefore || executedBefore-before < 1 {
		t.Fatalf("unkeyed request re-executed: %d -> %d", executedBefore, executions.Load())
	}
	p, err := client.XPending(context.Background(), cfg.Keys().RPC("remoteCmd"), RPCGroup).Result()
	if err != nil || p.Count != 0 {
		t.Fatalf("pending after unkeyed crash: %+v %v", p, err)
	}
}
