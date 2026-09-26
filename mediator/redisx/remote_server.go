package redisx

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
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

// RPCGroup is the consumer group every server uses on request streams.
const RPCGroup = "handlers"

// serverConcurrency bounds the requests one server executes at a time.
const serverConcurrency = 64

// RemoteServerOption configures NewRemoteServer.
type RemoteServerOption func(*RemoteServer)

// WithServerLogger overrides the logger (default: the mediator's).
func WithServerLogger(l *slog.Logger) RemoteServerOption {
	return func(s *RemoteServer) {
		if l != nil {
			s.logger = l
		}
	}
}

// WithServerClock sets the time source for heartbeats and claims.
func WithServerClock(c testkit.Clock) RemoteServerOption {
	return func(s *RemoteServer) {
		if c != nil {
			s.clock = c
		}
	}
}

// withHeartbeat overrides the heartbeat interval (tests).
//
//lint:ignore U1000 used by the integration && faultinject tests only
func withHeartbeat(d time.Duration) RemoteServerOption {
	return func(s *RemoteServer) {
		if d > 0 {
			s.heartbeat = d
		}
	}
}

// withServerHook installs a test hook called with the stage ("read",
// "executed", "replied") and call ID of every request; returning false
// abandons the request at that stage, which simulates a crash.
//
//lint:ignore U1000 used by the integration tests only
func withServerHook(h func(stage, call string) bool) RemoteServerOption {
	return func(s *RemoteServer) { s.hook = h }
}

// RemoteServer is the mediator.Component that serves remote dispatch for
// every command handled locally (spec 7.6): it advertises itself in
// <prefix>:handlers:<request>, reads <prefix>:rpc:<request> with group
// "handlers", runs the full local pipeline with the caller's context
// restored, and replies on the caller's reply stream.
type RemoteServer struct {
	m      *mediator.Mediator
	client *redis.Client
	cfg    Config
	keys   Keys
	node   string
	clock  testkit.Clock
	logger *slog.Logger
	names  []string
	hook   func(stage, call string) bool
	// heartbeat is HandlerHeartbeat unless a test shortens it.
	heartbeat time.Duration

	running  atomic.Bool
	lastBeat atomic.Int64
	inflight sync.WaitGroup
	sem      chan struct{}
}

var _ mediator.Component = (*RemoteServer)(nil)

// NewRemoteServer builds the server for the local commands of m. The
// mediator must be built.
func NewRemoteServer(m *mediator.Mediator, client *redis.Client, cfg Config, opts ...RemoteServerOption) *RemoteServer {
	if cfg.NodeID == "" {
		cfg.NodeID = m.NodeID()
	}
	cfg = cfg.WithDefaults()
	s := &RemoteServer{
		m: m, client: client, cfg: cfg, keys: cfg.Keys(), node: cfg.NodeID,
		clock: testkit.RealClock{}, logger: m.Logger(), sem: make(chan struct{}, serverConcurrency),
		heartbeat: HandlerHeartbeat,
	}
	for _, o := range opts {
		o(s)
	}
	for _, info := range m.Requests() {
		if info.Local && info.Kind == mediator.KindCommand {
			s.names = append(s.names, info.Name)
		}
	}
	return s
}

// Served returns the request names this server advertises.
func (s *RemoteServer) Served() []string { return append([]string(nil), s.names...) }

// NodeID returns the consumer name used on the request streams.
func (s *RemoteServer) NodeID() string { return s.node }

// Run serves until ctx is done, then withdraws the heartbeats and waits for
// in-flight requests.
func (s *RemoteServer) Run(ctx context.Context) error {
	if len(s.names) == 0 {
		s.running.Store(true)
		defer s.running.Store(false)
		s.logger.Info("remote server idle: no local commands", "node", s.node)
		<-ctx.Done()
		return nil
	}
	base := context.WithoutCancel(ctx)
	if err := s.ensureGroups(ctx); err != nil {
		return err
	}
	if err := s.beat(ctx); err != nil {
		return fmt.Errorf("redisx: remote server heartbeat: %w", err)
	}
	s.running.Store(true)
	defer s.running.Store(false)
	s.logger.Info("remote server started", "node", s.node, "requests", s.names)

	var loops sync.WaitGroup
	loops.Add(2)
	go func() {
		defer loops.Done()
		s.beatLoop(ctx)
	}()
	go func() {
		defer loops.Done()
		s.claimLoop(ctx, base)
	}()
	s.readLoop(ctx, base)
	loops.Wait()
	wctx, cancel := context.WithTimeout(base, opTimeout)
	s.withdraw(wctx)
	cancel()
	s.inflight.Wait()
	s.logger.Info("remote server stopped", "node", s.node)
	return nil
}

// Healthy returns nil while the loops run and the heartbeat is fresh.
func (s *RemoteServer) Healthy() error {
	if !s.running.Load() {
		return errors.New("redisx: remote server not running")
	}
	if len(s.names) == 0 {
		return nil
	}
	if age := s.clock.Now().Sub(time.UnixMilli(s.lastBeat.Load())); age > HandlerStaleAfter {
		return fmt.Errorf("redisx: remote server heartbeat stale for %s", age)
	}
	return nil
}

func (s *RemoteServer) ensureGroups(ctx context.Context) error {
	for _, name := range s.names {
		// Requests sent before any server ever advertised the name are not
		// served: start at $ on first creation.
		err := s.client.XGroupCreateMkStream(ctx, s.keys.RPC(name), RPCGroup, "$").Err()
		if err != nil && !isBusyGroup(err) {
			return fmt.Errorf("redisx: xgroup create %s: %w", s.keys.RPC(name), err)
		}
	}
	return nil
}

// beat refreshes this node in every handlers zset and prunes stale nodes.
// Fault point redis.handlers.beat.
func (s *RemoteServer) beat(ctx context.Context) error {
	if err := testkit.Fault(ctx, "redis.handlers.beat"); err != nil {
		return err
	}
	now := s.clock.Now()
	pipe := s.client.Pipeline()
	stale := "(" + strconv.FormatInt(now.Add(-HandlerStaleAfter).UnixMilli(), 10)
	for _, name := range s.names {
		key := s.keys.Handlers(name)
		pipe.ZAdd(ctx, key, redis.Z{Score: float64(now.UnixMilli()), Member: s.node})
		pipe.ZRemRangeByScore(ctx, key, "-inf", stale)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	s.lastBeat.Store(now.UnixMilli())
	return nil
}

func (s *RemoteServer) beatLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.clock.After(s.heartbeat):
		}
		bctx, cancel := context.WithTimeout(ctx, opTimeout)
		err := s.beat(bctx)
		cancel()
		if err != nil && ctx.Err() == nil {
			s.logger.Warn("remote server heartbeat failed", "node", s.node, "error", err)
		}
	}
}

// withdraw removes this node from every handlers zset so clients stop
// routing to it immediately.
func (s *RemoteServer) withdraw(ctx context.Context) {
	pipe := s.client.Pipeline()
	for _, name := range s.names {
		pipe.ZRem(ctx, s.keys.Handlers(name), s.node)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		s.logger.Warn("remote server heartbeat withdrawal failed", "node", s.node, "error", err)
	}
}

// readLoop reads every request stream with one XREADGROUP and dispatches
// entries to bounded workers.
func (s *RemoteServer) readLoop(ctx context.Context, base context.Context) {
	streams := make([]string, 0, 2*len(s.names))
	for _, name := range s.names {
		streams = append(streams, s.keys.RPC(name))
	}
	for range s.names {
		streams = append(streams, ">")
	}
	for ctx.Err() == nil {
		if err := testkit.Fault(ctx, "redis.xreadgroup"); err != nil {
			s.logger.Warn("remote request read failed", "error", err)
			if !sleepCtx(ctx, s.clock, defaultBackoff.jittered(1)) {
				return
			}
			continue
		}
		rctx, cancel := context.WithTimeout(ctx, s.cfg.ReadBlock+opTimeout)
		res, err := s.client.XReadGroup(rctx, &redis.XReadGroupArgs{
			Group: RPCGroup, Consumer: s.node, Streams: streams,
			Count: int64(s.cfg.ReadBatch), Block: s.cfg.ReadBlock,
		}).Result()
		cancel()
		if err = nonNil(err); err != nil {
			if ctx.Err() != nil {
				return
			}
			if isNoGroup(err) {
				_ = s.ensureGroups(ctx)
			}
			s.logger.Warn("remote request read failed", "error", err)
			if !sleepCtx(ctx, s.clock, defaultBackoff.jittered(1)) {
				return
			}
			continue
		}
		for _, st := range res {
			for _, msg := range st.Messages {
				s.dispatch(ctx, base, st.Stream, msg)
			}
		}
	}
}

// claimLoop takes over keyed requests a crashed server left pending for
// longer than ClaimMinIdle (spec 7.6, second row of the table).
func (s *RemoteServer) claimLoop(ctx context.Context, base context.Context) {
	interval := s.cfg.ClaimMinIdle / 2
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.clock.After(interval):
		}
		for _, name := range s.names {
			stream := s.keys.RPC(name)
			cursor := "0-0"
			for ctx.Err() == nil {
				cctx, cancel := context.WithTimeout(ctx, opTimeout)
				err := testkit.Fault(cctx, "redis.xautoclaim")
				var msgs []redis.XMessage
				var next string
				if err == nil {
					msgs, next, err = s.client.XAutoClaim(cctx, &redis.XAutoClaimArgs{
						Stream: stream, Group: RPCGroup, Consumer: s.node,
						MinIdle: s.cfg.ClaimMinIdle, Start: cursor, Count: int64(s.cfg.ReadBatch),
					}).Result()
				}
				cancel()
				if err != nil {
					if ctx.Err() == nil {
						s.logger.Warn("remote request claim failed", "stream", stream, "error", err)
					}
					break
				}
				for _, msg := range msgs {
					s.logger.Info("claimed stale remote request", "stream", stream, "id", msg.ID)
					s.dispatch(ctx, base, stream, msg)
				}
				if next == "" || next == "0-0" {
					break
				}
				cursor = next
			}
		}
	}
}

// dispatch runs the request in a worker goroutine bounded by the semaphore.
func (s *RemoteServer) dispatch(ctx context.Context, base context.Context, stream string, msg redis.XMessage) {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return
	}
	s.inflight.Add(1)
	go func() {
		defer s.inflight.Done()
		defer func() { <-s.sem }()
		s.handle(base, stream, msg)
	}()
}

// remoteRequest is a decoded request entry.
type remoteRequest struct {
	call, corr, caller, name, body, principal, idem, trace string
	deadline                                               time.Time
}

func decodeRequest(values map[string]any) remoteRequest {
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
	r := remoteRequest{
		call: str(RPCFieldCall), corr: str(RPCFieldCorr), caller: str(RPCFieldNode), name: str(RPCFieldName),
		body: str(RPCFieldBody), principal: str(RPCFieldPrincipal), idem: str(RPCFieldIdem), trace: str(RPCFieldTrace),
	}
	if ms, err := strconv.ParseInt(str(RPCFieldDeadline), 10, 64); err == nil && ms > 0 {
		r.deadline = time.UnixMilli(ms)
	}
	return r
}

// handle executes one request with the acknowledgement timing of the 7.6
// table: no idempotency key, XACK before executing (at-most-once); key, XACK
// after replying (at-least-once attempt, idempotency makes the effect single).
func (s *RemoteServer) handle(base context.Context, stream string, msg redis.XMessage) {
	req := decodeRequest(msg.Values)
	if req.name == "" {
		req.name, _ = s.keys.ParseRPC(stream)
	}
	log := s.logger.With("name", req.name, "call", req.call, "caller", req.caller, "correlation_id", req.corr, "id", msg.ID)
	if s.hook != nil && !s.hook("read", req.call) {
		log.Warn("test hook abandoned request after read")
		return
	}
	if req.call == "" || req.caller == "" {
		log.Warn("malformed remote request; acknowledging")
		s.ack(base, stream, msg.ID, log)
		return
	}
	keyed := req.idem != ""
	if !keyed {
		if !s.ack(base, stream, msg.ID, log) {
			// Unknown whether the ack happened: do not execute; a later claim
			// executes it once.
			return
		}
	}
	res, err := s.execute(base, req, log)
	if s.hook != nil && !s.hook("executed", req.call) {
		log.Warn("test hook abandoned request after execution")
		return
	}
	if !s.reply(base, req, res, err, log) {
		return
	}
	if keyed {
		s.ack(base, stream, msg.ID, log)
	}
}

// execute restores the caller's context and runs the local pipeline.
func (s *RemoteServer) execute(base context.Context, req remoteRequest, log *slog.Logger) (any, error) {
	ptr, ok := s.m.NewRequest(req.name)
	if !ok {
		return nil, fmt.Errorf("%w: %s is not registered on this node", mediator.ErrHandlerNotFound, req.name)
	}
	if err := json.Unmarshal([]byte(req.body), ptr); err != nil {
		return nil, mediator.Wrap(mediator.CodeBadRequest, "remote: decode request body", err)
	}
	ctx := base
	if !req.deadline.IsZero() {
		if !req.deadline.After(s.clock.Now()) {
			// The caller has given up: do not spend the execution. A keyed
			// caller retries with the same key and gets the stored response.
			return nil, mediator.Wrap(mediator.CodeTimeout, "remote: caller deadline passed before execution", context.DeadlineExceeded)
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, req.deadline)
		defer cancel()
	}
	corr := req.corr
	if corr == "" {
		corr = req.call
	}
	ctx = mediator.WithCorrelationID(ctx, corr)
	if req.principal != "" {
		var p authz.Principal
		if err := json.Unmarshal([]byte(req.principal), &p); err != nil {
			return nil, mediator.Wrap(mediator.CodeBadRequest, "remote: decode principal", err)
		}
		ctx = authz.WithPrincipal(ctx, p)
	}
	if req.idem != "" {
		ctx = mediator.WithIdempotencyKey(ctx, req.idem)
	}
	if req.trace != "" {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": req.trace})
	}
	log.Debug("executing remote request")
	return s.m.SendAny(ctx, ptr)
}

// reply writes the response to the caller's reply stream. Fault point
// redis.rpc.reply. It returns false when the reply was not written.
func (s *RemoteServer) reply(base context.Context, req remoteRequest, res any, execErr error, log *slog.Logger) bool {
	fields := map[string]any{ReplyFieldCall: req.call, ReplyFieldCorr: req.corr}
	if execErr != nil {
		body := RemoteErrorBody{Code: mediator.CodeOf(execErr)}
		var me *mediator.Error
		if errors.As(execErr, &me) {
			body.Message, body.Details = me.Message, me.Details
		}
		var ve *mediator.ValidationError
		if errors.As(execErr, &ve) {
			body.Message = ve.Error()
			body.Details = map[string]any{"fields": ve.Fields}
		}
		if body.Message == "" {
			body.Message = body.Code.Title()
		}
		b, err := json.Marshal(body)
		if err != nil {
			b = []byte(`{"code":"internal","message":"Internal error"}`)
		}
		fields[ReplyFieldStatus] = ReplyStatusErr
		fields[ReplyFieldBody] = string(b)
		log.Info("remote request failed", "code", body.Code, "error", execErr)
	} else {
		b, err := json.Marshal(res)
		if err != nil {
			return s.reply(base, req, nil, mediator.Wrap(mediator.CodeInternal, "remote: encode response", err), log)
		}
		fields[ReplyFieldStatus] = ReplyStatusOK
		fields[ReplyFieldBody] = string(b)
	}
	ctx, cancel := context.WithTimeout(base, opTimeout)
	defer cancel()
	if err := testkit.Fault(ctx, "redis.rpc.reply"); err != nil {
		log.Warn("remote reply failed", "error", err)
		return false
	}
	err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.keys.Reply(req.caller), Values: fields, MaxLen: s.cfg.ReplyStreamMaxLen, Approx: true,
	}).Err()
	if err != nil {
		log.Warn("remote reply failed", "error", err)
		return false
	}
	if err := testkit.FaultAfter(ctx, "redis.rpc.reply"); err != nil {
		log.Warn("remote reply outcome unknown", "error", err)
		return false
	}
	if s.hook != nil && !s.hook("replied", req.call) {
		return false
	}
	return true
}

// ack acknowledges a request entry. Fault point redis.xack.
func (s *RemoteServer) ack(base context.Context, stream, id string, log *slog.Logger) bool {
	ctx, cancel := context.WithTimeout(base, opTimeout)
	defer cancel()
	if err := testkit.Fault(ctx, "redis.xack"); err != nil {
		log.Warn("remote request ack failed", "error", err)
		return false
	}
	if err := s.client.XAck(ctx, stream, RPCGroup, id).Err(); err != nil {
		log.Warn("remote request ack failed", "error", err)
		return false
	}
	return true
}
