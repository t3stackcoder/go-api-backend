package redisx

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// Remote dispatch timing (spec 7.6).
const (
	// HandlerHeartbeat is how often a server refreshes its entry in
	// <prefix>:handlers:<request>.
	HandlerHeartbeat = 5 * time.Second
	// HandlerStaleAfter is the age after which a heartbeat no longer counts.
	HandlerStaleAfter = 3 * HandlerHeartbeat
	// HandlersViewTTL is how long a client caches the set of serving nodes.
	HandlersViewTTL = 5 * time.Second
	// DefaultRemoteTimeout bounds a remote Send whose context has no deadline.
	DefaultRemoteTimeout = 30 * time.Second
)

// Request stream fields.
const (
	RPCFieldCall      = "call"      // unique per Send; the reply carries it back
	RPCFieldCorr      = "corr"      // caller's correlation ID
	RPCFieldNode      = "node"      // caller node; replies go to <prefix>:reply:<node>
	RPCFieldName      = "name"      // request name
	RPCFieldBody      = "body"      // request JSON
	RPCFieldPrincipal = "principal" // authz.Principal JSON, "" when anonymous
	RPCFieldIdem      = "idem"      // idempotency key, "" when none
	RPCFieldTrace     = "trace"     // W3C traceparent
	RPCFieldDeadline  = "deadline"  // caller deadline, unix ms
)

// Reply stream fields.
const (
	ReplyFieldCall   = "call"
	ReplyFieldCorr   = "corr"
	ReplyFieldStatus = "status" // "ok" or "err"
	ReplyFieldBody   = "body"   // response JSON, or the error document
	ReplyStatusOK    = "ok"
	ReplyStatusErr   = "err"
)

// RemoteErrorBody is the JSON document of an error reply; the client rebuilds
// a *mediator.Error from it.
type RemoteErrorBody struct {
	Code    mediator.Code  `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// reply is one decoded reply entry.
type reply struct {
	status string
	body   []byte
}

// waiters maps call IDs to the channel of the goroutine waiting for the reply.
type waiters struct {
	mu sync.Mutex
	m  map[string]chan reply
}

func newWaiters() *waiters { return &waiters{m: map[string]chan reply{}} }

func (w *waiters) add(call string) chan reply {
	ch := make(chan reply, 1)
	w.mu.Lock()
	w.m[call] = ch
	w.mu.Unlock()
	return ch
}

func (w *waiters) remove(call string) {
	w.mu.Lock()
	delete(w.m, call)
	w.mu.Unlock()
}

// deliver hands the reply to the waiter and reports whether one existed.
func (w *waiters) deliver(call string, r reply) bool {
	w.mu.Lock()
	ch, ok := w.m[call]
	if ok {
		delete(w.m, call)
	}
	w.mu.Unlock()
	if !ok {
		return false
	}
	ch <- r
	return true
}

func (w *waiters) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.m)
}

// RemoteOption configures NewRemote.
type RemoteOption func(*Remote)

// WithRemoteLogger overrides the logger.
func WithRemoteLogger(l *slog.Logger) RemoteOption {
	return func(r *Remote) {
		if l != nil {
			r.logger = l
		}
	}
}

// WithRemoteClock sets the time source used for heartbeat freshness.
func WithRemoteClock(c testkit.Clock) RemoteOption {
	return func(r *Remote) {
		if c != nil {
			r.clock = c
		}
	}
}

// Remote is the client side of remote dispatch (spec 7.6). It implements
// mediator.RemoteDispatcher: pass it to mediator.WithRemote and run a
// ReplyReader for the same instance. It takes no mediator because the
// mediator is constructed with it.
type Remote struct {
	client *redis.Client
	cfg    Config
	keys   Keys
	node   string
	clock  testkit.Clock
	logger *slog.Logger
	w      *waiters

	mu   sync.Mutex
	view map[string]handlersView
}

type handlersView struct {
	at    time.Time
	nodes []string
}

var _ mediator.RemoteDispatcher = (*Remote)(nil)

// NewRemote returns the remote dispatcher of this node. Replies arrive on
// <prefix>:reply:<cfg.NodeID>.
func NewRemote(client *redis.Client, cfg Config, opts ...RemoteOption) *Remote {
	cfg = cfg.WithDefaults()
	r := &Remote{
		client: client, cfg: cfg, keys: cfg.Keys(), node: cfg.NodeID,
		clock: testkit.RealClock{}, logger: slog.Default().WithGroup("mediator"),
		w: newWaiters(), view: map[string]handlersView{},
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// NodeID returns the node whose reply stream this client reads.
func (r *Remote) NodeID() string { return r.node }

// Waiting returns the number of calls awaiting a reply.
func (r *Remote) Waiting() int { return r.w.count() }

// freshHandlers returns the nodes with a fresh heartbeat for the request,
// from a view cached for HandlersViewTTL.
func (r *Remote) freshHandlers(ctx context.Context, name string) ([]string, error) {
	now := r.clock.Now()
	r.mu.Lock()
	v, ok := r.view[name]
	r.mu.Unlock()
	if ok && now.Sub(v.at) < HandlersViewTTL {
		return v.nodes, nil
	}
	min := strconv.FormatInt(now.Add(-HandlerStaleAfter).UnixMilli(), 10)
	nodes, err := r.client.ZRangeByScore(ctx, r.keys.Handlers(name), &redis.ZRangeBy{Min: min, Max: "+inf"}).Result()
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.view[name] = handlersView{at: now, nodes: nodes}
	r.mu.Unlock()
	return nodes, nil
}

// InvalidateView drops the cached handler view of a request (tests, ops).
func (r *Remote) InvalidateView(name string) {
	r.mu.Lock()
	delete(r.view, name)
	r.mu.Unlock()
}

// Send dispatches req to a node serving info.Name and waits for the reply
// until the context deadline (DefaultRemoteTimeout when none). It returns
// ErrHandlerNotFound when no node has a fresh heartbeat, CodeUnavailable
// when Redis fails, and CodeTimeout when the deadline passes.
func (r *Remote) Send(ctx context.Context, info *mediator.RequestInfo, req any) (any, error) {
	if info == nil {
		return nil, mediator.E(mediator.CodeInternal, "remote: nil request info")
	}
	nodes, err := r.freshHandlers(ctx, info.Name)
	if err != nil {
		return nil, mediator.Wrap(mediator.CodeUnavailable, "remote: handler lookup failed", err)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("%w: no node serves %s", mediator.ErrHandlerNotFound, info.Name)
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, mediator.Wrap(mediator.CodeInternal, "remote: encode request", err)
	}
	if _, has := ctx.Deadline(); !has {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultRemoteTimeout)
		defer cancel()
	}
	deadline, _ := ctx.Deadline()
	call := mediator.NewID(r.clock.Now()).String()
	fields := map[string]any{
		RPCFieldCall:     call,
		RPCFieldCorr:     mediator.CorrelationID(ctx),
		RPCFieldNode:     r.node,
		RPCFieldName:     info.Name,
		RPCFieldBody:     string(body),
		RPCFieldDeadline: strconv.FormatInt(deadline.UnixMilli(), 10),
	}
	if p := authz.PrincipalFrom(ctx); !p.IsAnonymous() {
		pj, err := json.Marshal(p)
		if err != nil {
			return nil, mediator.Wrap(mediator.CodeInternal, "remote: encode principal", err)
		}
		fields[RPCFieldPrincipal] = string(pj)
	} else {
		fields[RPCFieldPrincipal] = ""
	}
	idem, _ := mediator.IdempotencyKeyFrom(ctx)
	fields[RPCFieldIdem] = idem
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	fields[RPCFieldTrace] = carrier.Get("traceparent")

	ch := r.w.add(call)
	defer r.w.remove(call)
	if err := testkit.Fault(ctx, "redis.rpc.xadd"); err != nil {
		return nil, mediator.Wrap(mediator.CodeUnavailable, "remote: send request", err)
	}
	if err := r.client.XAdd(ctx, &redis.XAddArgs{Stream: r.keys.RPC(info.Name), Values: fields}).Err(); err != nil {
		return nil, mediator.Wrap(mediator.CodeUnavailable, "remote: send request", err)
	}
	if err := testkit.FaultAfter(ctx, "redis.rpc.xadd"); err != nil {
		// The request was added; the outcome is unknown to the caller.
		return nil, mediator.MarkAmbiguous(mediator.Wrap(mediator.CodeUnavailable, "remote: send request", err))
	}
	r.logger.Debug("remote request sent", "name", info.Name, "call", call, "nodes", len(nodes), "correlation_id", fields[RPCFieldCorr])
	select {
	case rep := <-ch:
		return decodeReply(rep, info.ResponseType)
	case <-ctx.Done():
		return nil, mediator.Wrap(mediator.CodeTimeout, "remote: no reply before the deadline", ctx.Err())
	}
}

// decodeReply turns a reply into the typed response or a *mediator.Error.
func decodeReply(rep reply, responseType reflect.Type) (any, error) {
	switch rep.status {
	case ReplyStatusOK:
		if responseType == nil {
			return nil, nil
		}
		v := reflect.New(responseType)
		if len(rep.body) > 0 {
			if err := json.Unmarshal(rep.body, v.Interface()); err != nil {
				return nil, mediator.Wrap(mediator.CodeInternal, "remote: decode response", err)
			}
		}
		return v.Elem().Interface(), nil
	case ReplyStatusErr:
		var body RemoteErrorBody
		if err := json.Unmarshal(rep.body, &body); err != nil || body.Code == "" {
			return nil, mediator.Wrap(mediator.CodeInternal, "remote: undecodable error reply", err)
		}
		return nil, &mediator.Error{Code: body.Code, Message: body.Message, Details: body.Details}
	default:
		return nil, mediator.E(mediator.CodeInternal, fmt.Sprintf("remote: unknown reply status %q", rep.status))
	}
}

// replyFromValues decodes a reply stream entry. ok is false when the entry
// carries no call ID.
func replyFromValues(values map[string]any) (call string, rep reply, ok bool) {
	str := func(name string) string {
		switch v := values[name].(type) {
		case string:
			return v
		case []byte:
			return string(v)
		default:
			return ""
		}
	}
	call = str(ReplyFieldCall)
	if call == "" {
		return "", reply{}, false
	}
	return call, reply{status: str(ReplyFieldStatus), body: []byte(str(ReplyFieldBody))}, true
}

// ReplyReader is the mediator.Component that reads <prefix>:reply:<nodeID>
// and hands each reply to the Send waiting for it. Replies nobody waits for
// are dropped. The stream is trimmed to ReplyStreamMaxLen.
type ReplyReader struct {
	r       *Remote
	running atomic.Bool
}

var _ mediator.Component = (*ReplyReader)(nil)

// NewReplyReader returns the reader for the replies of r.
func NewReplyReader(r *Remote) *ReplyReader { return &ReplyReader{r: r} }

// Run reads replies until ctx is done. Fault point redis.rpc.readreply.
func (rr *ReplyReader) Run(ctx context.Context) error {
	r := rr.r
	key := r.keys.Reply(r.node)
	last := "0-0"
	if tail, err := r.client.XRevRangeN(ctx, key, "+", "-", 1).Result(); err == nil && len(tail) > 0 {
		last = tail[0].ID
	} else if err != nil && ctx.Err() == nil {
		r.logger.Warn("reply stream tail lookup failed; reading from the beginning", "stream", key, "error", err)
	}
	rr.running.Store(true)
	defer rr.running.Store(false)
	r.logger.Info("reply reader started", "stream", key)
	var sinceTrim int64
	lastTrim := r.clock.Now()
	for ctx.Err() == nil {
		if err := testkit.Fault(ctx, "redis.rpc.readreply"); err != nil {
			r.logger.Warn("reply read failed", "error", err)
			if !sleepCtx(ctx, r.clock, defaultBackoff.jittered(1)) {
				break
			}
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, r.cfg.ReadBlock+opTimeout)
		res, err := r.client.XRead(rctx, &redis.XReadArgs{Streams: []string{key, last}, Count: 256, Block: r.cfg.ReadBlock}).Result()
		cancel()
		if err = nonNil(err); err != nil {
			if ctx.Err() != nil {
				break
			}
			r.logger.Warn("reply read failed", "error", err)
			if !sleepCtx(ctx, r.clock, defaultBackoff.jittered(1)) {
				break
			}
			continue
		}
		for _, s := range res {
			for _, msg := range s.Messages {
				last = msg.ID
				sinceTrim++
				call, rep, ok := replyFromValues(msg.Values)
				if !ok {
					continue
				}
				if !r.w.deliver(call, rep) {
					r.logger.Debug("dropping reply nobody waits for", "call", call)
				}
			}
		}
		if sinceTrim >= 1000 || r.clock.Now().Sub(lastTrim) >= time.Minute {
			tctx, tcancel := context.WithTimeout(ctx, opTimeout)
			_ = r.client.XTrimMaxLenApprox(tctx, key, r.cfg.ReplyStreamMaxLen, 0).Err()
			tcancel()
			sinceTrim, lastTrim = 0, r.clock.Now()
		}
	}
	r.logger.Info("reply reader stopped", "stream", key)
	return nil
}

// Healthy returns nil while the reader loop runs.
func (rr *ReplyReader) Healthy() error {
	if !rr.running.Load() {
		return errors.New("redisx: reply reader not running")
	}
	return nil
}

// sleepCtx sleeps d or until ctx is done; it returns false when interrupted.
func sleepCtx(ctx context.Context, clock testkit.Clock, d time.Duration) bool {
	select {
	case <-clock.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}
