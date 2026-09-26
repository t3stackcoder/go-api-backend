//go:build faultsweep && faultinject

package faultsweep

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// SweepRemote (spec 11.4, 7.6, G10): a caller mediator and a handler
// mediator in one process, faults at every step of the request and reply
// paths, with and without an idempotency key. Expect the table of 7.6:
// unkeyed executes at most once (the caller may time out with the outcome
// unknown); keyed executes exactly once and every attempt returns the same
// response, with a crashed attempt claimed and replayed after ClaimMinIdle.

const remoteKeyed = "rk-1"

var (
	remoteAppend = workload.Append{Key: "r", Val: 1, CmdID: remoteKeyed}
	remoteBump   = workload.Bump{Key: "ru"}
	remoteFail   = workload.Transfer{From: "a", To: "b", Amt: 10_000, CmdID: "rf-1"}
	remoteNames  = []string{workload.NameAppend, workload.NameBump, workload.NameTransfer}
)

type remoteState struct {
	server    *redisx.RemoteServer
	remote    *redisx.Remote
	caller    *mediator.Mediator
	client    *node
	unkeyedOK bool
}

// remoteSend invalidates the cached handler view and sends once.
func remoteSend(ctx context.Context, c *cellRun, st *remoteState, key string, req any) (any, error) {
	for _, n := range remoteNames {
		st.remote.InvalidateView(n)
	}
	if key != "" {
		return c.sendKeyed(ctx, st.caller, key, req)
	}
	return c.send(ctx, st.caller, req)
}

func serverHealthy(st *remoteState) func(context.Context) (bool, error) {
	return func(context.Context) (bool, error) { return st.server.Healthy() == nil, nil }
}

func rpcQuiet(st *remoteState) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		n, err := rpcPending(ctx, st.client.client, st.client.cfg, remoteNames)
		if err != nil {
			return false, nil
		}
		return n == 0 && st.remote.Waiting() == 0, nil
	}
}

var scenarioRemote = &scenario{
	name:     "SweepRemote",
	patterns: []string{"redis.rpc.*", "redis.handlers.beat", "redis.xreadgroup", "redis.xautoclaim", "redis.xack"},
	groups:   []string{},
	variants: []string{"pool1", "oom"},
	newState: func() any { return &remoteState{} },
	build: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*remoteState)
		srv, err := c.newNode(ctx, nodeOpts{id: c.nodeID("srv"), workload: true})
		if err != nil {
			return err
		}
		st.server = redisx.NewRemoteServer(srv.m, srv.client, srv.cfg, redisx.WithServerLogger(c.logger))
		srv.add("remote_server", st.server)
		cli, err := c.newNode(ctx, nodeOpts{id: c.nodeID("cli"), noStore: true})
		if err != nil {
			return err
		}
		st.client = cli
		st.remote = redisx.NewRemote(cli.client, cli.cfg, redisx.WithRemoteLogger(c.logger))
		m := mediator.New(mediator.WithNodeID(cli.id), mediator.WithLogger(c.logger), mediator.WithRemote(st.remote))
		if err := mediator.Declare[workload.Append, workload.AppendResult](m); err != nil {
			return err
		}
		if err := mediator.Declare[workload.Bump, workload.BumpResult](m); err != nil {
			return err
		}
		if err := mediator.Declare[workload.Transfer, mediator.Void](m); err != nil {
			return err
		}
		if err := m.Build(); err != nil {
			return err
		}
		st.caller = m
		cli.add("reply_reader", redisx.NewReplyReader(st.remote))
		return nil
	},
	run: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*remoteState)
		if err := waitFor(ctx, sendTimeout, serverHealthy(st)); err != nil {
			c.note("server not healthy before the requests: %v", st.server.Healthy())
		}
		res, err := remoteSend(ctx, c, st, remoteKeyed, remoteAppend)
		c.note("keyed: %v", err)
		if err == nil {
			c.record(remoteKeyed, res)
		}
		_, err = remoteSend(ctx, c, st, "", remoteBump)
		st.unkeyedOK = err == nil
		c.note("unkeyed: %v", err)
		_, err = remoteSend(ctx, c, st, "", remoteFail)
		if err == nil {
			c.t.Errorf("remote transfer of 10000 from an account with 100 succeeded")
		} else if mediator.CodeOf(err) != mediator.CodePrecondition {
			c.note("failing: %v", err)
		}
		return waitFor(ctx, runWait, rpcQuiet(st))
	},
	recover: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*remoteState)
		if err := waitFor(ctx, 10*time.Second, serverHealthy(st)); err != nil {
			return fmt.Errorf("server did not become healthy: %v", st.server.Healthy())
		}
		var res any
		var err error
		for i := 0; i < 10; i++ {
			if res, err = remoteSend(ctx, c, st, remoteKeyed, remoteAppend); err == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err != nil {
			return fmt.Errorf("keyed retry: %w", err)
		}
		c.record(remoteKeyed, res)
		for i := 0; i < 10; i++ {
			_, err = remoteSend(ctx, c, st, "", remoteFail)
			if mediator.CodeOf(err) == mediator.CodePrecondition {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if mediator.CodeOf(err) != mediator.CodePrecondition {
			return fmt.Errorf("failing command after recovery: %v", err)
		}
		return waitFor(ctx, recoverWait, rpcQuiet(st))
	},
	verify: func(ctx context.Context, c *cellRun, v *verifier) {
		st := c.st.(*remoteState)
		v.expectCount(ctx, 1, "keyed executions", `SELECT coalesce(sum(n), 0) FROM wl_executions WHERE scope = $1 AND key = $2`, workload.NameAppend, remoteKeyed)
		v.expectCount(ctx, 1, "keyed appends", `SELECT count(*) FROM wl_appends WHERE cmd_id = $1`, remoteKeyed)
		bumps := v.count(ctx, `SELECT coalesce(max(n), 0) FROM wl_bumps WHERE key = 'ru'`)
		switch {
		case bumps > 1:
			v.t.Errorf("G10: unkeyed command executed %d times", bumps)
		case st.unkeyedOK && bumps != 1:
			v.t.Errorf("G10: unkeyed command returned success but executed %d times", bumps)
		}
		v.expectCount(ctx, workload.BankTotal(), "bank total", `SELECT coalesce(sum(balance), 0) FROM wl_bank`)
		if n, err := rpcPending(ctx, v.client, v.cfg, remoteNames); err != nil || n != 0 {
			v.t.Errorf("%d remote requests still pending after recovery (%v)", n, err)
		}
	},
	expect: func(*cellRun) map[string]invariants.Expect {
		exp := map[string]invariants.Expect{}
		exp[remoteKeyed], _ = invariants.ExpectFor(remoteAppend)
		exp[remoteFail.CmdID] = invariants.Expect{Name: workload.NameTransfer}
		return exp
	},
}

func TestSweepRemote(t *testing.T) { runSweep(t, scenarioRemote) }
