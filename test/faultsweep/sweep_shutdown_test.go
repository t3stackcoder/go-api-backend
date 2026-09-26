//go:build faultsweep && faultinject

package faultsweep

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// SweepShutdown (spec 11.4, G16): a mediator.Runtime with the relay, two
// consumer groups, the janitor, a remote server, and a reply reader is
// shut down at every step of every loop (the runtime context is canceled
// while the step is delayed, which models SIGTERM on a platform that
// cannot send it). Expect: Run returns nil within the drain timeout, every
// acknowledged entry was committed first, every lease is released, and the
// recovery drains to the same end state as a clean run.

const shutdownKeyed = "sk-1"

var (
	shutdownGroups = workload.DefaultGroups
	shutdownBumps  = append(bumpCommands("k1", "sb1", 3), bumpCommands("k2", "sb2", 3)...)
	shutdownAppend = workload.Append{Key: "s", Val: 1, CmdID: shutdownKeyed}
)

// runtimeComp runs a mediator.Runtime as a component whose context the
// shutdown kind can cancel independently of the node.
type runtimeComp struct {
	rt *mediator.Runtime

	mu       sync.Mutex
	cancel   context.CancelFunc
	started  time.Time
	returned time.Time
	err      error
	done     bool
}

func (r *runtimeComp) Run(ctx context.Context) error {
	rctx, cancel := context.WithCancel(ctx)
	r.mu.Lock()
	r.cancel = cancel
	r.started = time.Now()
	r.mu.Unlock()
	err := r.rt.Run(rctx)
	cancel()
	r.mu.Lock()
	r.err, r.returned, r.done = err, time.Now(), true
	r.mu.Unlock()
	return err
}

func (r *runtimeComp) Healthy() error { return r.rt.Healthy() }

// shutdown cancels the runtime context (SIGTERM).
func (r *runtimeComp) shutdown() {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *runtimeComp) result() (done bool, took time.Duration, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.done, r.returned.Sub(r.started), r.err
}

type shutdownState struct {
	rc     *runtimeComp
	remote *redisx.Remote
	caller *mediator.Mediator
	runRC  *runtimeComp // the runtime of the run phase, kept for verify
}

var scenarioShutdown = &scenario{
	name:   "SweepShutdown",
	kinds:  []string{kindShutdown},
	groups: shutdownGroups,
	build: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*shutdownState)
		n, err := c.newNode(ctx, nodeOpts{id: c.nodeID("n1"), workload: true, deps: workload.Deps{Groups: shutdownGroups, StrictProjection: true}})
		if err != nil {
			return err
		}
		streams := redisx.NewStreams(n.client, n.cfg)
		st.remote = redisx.NewRemote(n.client, n.cfg, redisx.WithRemoteLogger(c.logger))
		m := mediator.New(mediator.WithNodeID(n.id), mediator.WithLogger(c.logger), mediator.WithRemote(st.remote))
		if err := mediator.Declare[workload.Append, workload.AppendResult](m); err != nil {
			return err
		}
		if err := m.Build(); err != nil {
			return err
		}
		st.caller = m
		rt := &mediator.Runtime{
			RemoteServer: redisx.NewRemoteServer(n.m, n.client, n.cfg, redisx.WithServerLogger(c.logger)),
			Consumers:    redisx.NewConsumers(n.m, n.client, n.cfg, n.store, redisx.WithLogger(c.logger)),
			Relay:        newRelay(c, n, shutdownGroups),
			ReplyReader:  redisx.NewReplyReader(st.remote),
			Janitor: pg.NewJanitor(n.aux, pg.JanitorConfig{Interval: 500 * time.Millisecond, Trimmer: streams,
				Topics: []string{workload.TopicBumped}, Partitions: partitions, Logger: c.logger}),
			DrainTimeout: 15 * time.Second,
			Logger:       c.logger,
		}
		st.rc = &runtimeComp{rt: rt}
		if c.phase == phaseRun {
			st.runRC = st.rc
		}
		n.add("runtime", st.rc)
		c.mu.Lock()
		c.runtimeCancel = st.rc.shutdown
		c.mu.Unlock()
		return nil
	},
	run: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*shutdownState)
		n := c.nodes[0]
		for _, b := range shutdownBumps {
			if _, err := c.retrySend(ctx, n.m, "", b, 3); err != nil {
				c.note("%s: %v", b.CmdID, err)
			}
		}
		st.remote.InvalidateView(workload.NameAppend)
		if res, err := c.sendKeyed(ctx, st.caller, shutdownKeyed, shutdownAppend); err != nil {
			c.note("remote append: %v", err)
		} else {
			c.record(shutdownKeyed, res)
		}
		quiet := drained(n.pool, n.client, n.cfg, shutdownGroups)
		runtimeDone := func(context.Context) (bool, error) {
			done, _, _ := st.rc.result()
			return done, nil
		}
		err := waitFor(ctx, runWait, func(ctx context.Context) (bool, error) {
			if done, _ := runtimeDone(ctx); done {
				return true, nil
			}
			return quiet(ctx)
		})
		if done, _ := runtimeDone(ctx); !done && c.run.Kind == kindShutdown {
			// The system is quiet but the armed step has not fired yet: give
			// a late loop step (a poll, a heartbeat) a moment to trigger it.
			// A step that only runs at shutdown (lease release) fires during
			// stopAll, where the cancellation is a no-op.
			_ = waitFor(ctx, 2*time.Second, runtimeDone)
		}
		if done, took, rerr := st.rc.result(); done {
			c.note("runtime returned after %s: %v", took.Round(time.Millisecond), rerr)
			if rerr != nil {
				c.t.Errorf("G16: runtime did not exit clean: %v", rerr)
			}
			v, verr := c.env.verifier(ctx, c.t)
			if verr != nil {
				return verr
			}
			checkAckedImpliesInbox(ctx, v, shutdownGroups)
			checkLeasesReleased(ctx, v, shutdownGroups)
			v.close()
		} else if c.run.Kind == kindShutdown {
			c.note("armed step never triggered the shutdown")
		}
		return err
	},
	recover: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*shutdownState)
		n := c.nodes[0]
		for _, b := range shutdownBumps {
			if _, err := c.retrySend(ctx, n.m, "", b, 8); err != nil {
				return fmt.Errorf("%s: %w", b.CmdID, err)
			}
		}
		var res any
		var err error
		for i := 0; i < 10; i++ {
			st.remote.InvalidateView(workload.NameAppend)
			if res, err = c.sendKeyed(ctx, st.caller, shutdownKeyed, shutdownAppend); err == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err != nil {
			return fmt.Errorf("remote append: %w", err)
		}
		c.record(shutdownKeyed, res)
		return c.drainedAndSettled(ctx, n, shutdownGroups)
	},
	verify: func(ctx context.Context, c *cellRun, v *verifier) {
		st := c.st.(*shutdownState)
		if st.runRC != nil {
			if done, took, err := st.runRC.result(); !done {
				v.t.Errorf("G16: the run-phase runtime never returned")
			} else if err != nil && !errors.Is(err, context.Canceled) {
				v.t.Errorf("G16: run-phase runtime exit: %v (after %s)", err, took)
			}
		}
		if done, _, err := st.rc.result(); !done || err != nil {
			v.t.Errorf("G16: recovery runtime exit: done=%v err=%v", done, err)
		}
		checkAckedImpliesInbox(ctx, v, shutdownGroups)
		checkLeasesReleased(ctx, v, shutdownGroups)
		checkProjection(ctx, v, "k1", 3)
		checkProjection(ctx, v, "k2", 3)
		v.expectCount(ctx, 1, "remote append executions", `SELECT coalesce(sum(n), 0) FROM wl_executions WHERE scope = $1 AND key = $2`, workload.NameAppend, shutdownKeyed)
	},
	expect: func(*cellRun) map[string]invariants.Expect {
		exp := map[string]invariants.Expect{}
		expectBumps(exp, shutdownBumps...)
		exp[shutdownKeyed], _ = invariants.ExpectFor(shutdownAppend)
		return exp
	},
	newState: func() any { return &shutdownState{} },
}

func TestSweepShutdown(t *testing.T) { runSweep(t, scenarioShutdown) }
