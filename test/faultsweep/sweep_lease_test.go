//go:build faultsweep && faultinject

package faultsweep

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// SweepLease (spec 11.4, G14): two consumer nodes contend for one
// partition while six events on two keys flow through; delays longer than
// the lease TTL and pauses at every lease step force expiry and handover.
// Expect: every apply carries a fencing token that never decreases per
// partition and is strictly greater after a change of owner, the
// projection is exactly once and in order, and every lease is released.

var (
	leaseGroups = []string{workload.GroupReadModel}
	leaseBumps  = append(bumpCommands("k1", "lb1", 3), bumpCommands("k2", "lb2", 3)...)
)

var scenarioLease = &scenario{
	name:     "SweepLease",
	patterns: []string{"redis.lease.*", "redis.members.beat", "pg.lease.epoch", "redis.xautoclaim"},
	delay:    leaseTTL + 500*time.Millisecond,
	groups:   leaseGroups,
	variants: []string{"pool1", "oom"},
	build: func(ctx context.Context, c *cellRun) error {
		deps := workload.Deps{Groups: leaseGroups, StrictProjection: true}
		n1, err := c.newNode(ctx, nodeOpts{id: c.nodeID("n1"), workload: true, deps: deps})
		if err != nil {
			return err
		}
		n1.add("relay", newRelay(c, n1, leaseGroups))
		n1.add("consumers", redisx.NewConsumers(n1.m, n1.client, n1.cfg, n1.store, redisx.WithLogger(c.logger)))
		n2, err := c.newNode(ctx, nodeOpts{id: c.nodeID("n2"), workload: true, deps: deps})
		if err != nil {
			return err
		}
		n2.add("consumers", redisx.NewConsumers(n2.m, n2.client, n2.cfg, n2.store, redisx.WithLogger(c.logger)))
		return nil
	},
	run: func(ctx context.Context, c *cellRun) error {
		n := c.nodes[0]
		for _, b := range leaseBumps {
			if _, err := c.retrySend(ctx, n.m, "", b, 3); err != nil {
				c.note("%s: %v", b.CmdID, err)
			}
		}
		return waitFor(ctx, runWait, drained(n.pool, n.client, n.cfg, leaseGroups))
	},
	recover: func(ctx context.Context, c *cellRun) error {
		n := c.nodes[0]
		for _, b := range leaseBumps {
			if _, err := c.retrySend(ctx, n.m, "", b, 8); err != nil {
				return fmt.Errorf("%s: %w", b.CmdID, err)
			}
		}
		return c.drainedAndSettled(ctx, n, leaseGroups)
	},
	verify: func(ctx context.Context, c *cellRun, v *verifier) {
		checkProjection(ctx, v, "k1", 3)
		checkProjection(ctx, v, "k2", 3)
		checkLeasesReleased(ctx, v, leaseGroups)
		v.expectCount(ctx, 6, "applies", `SELECT count(*) FROM wl_applied WHERE grp = $1`, workload.GroupReadModel)
	},
	expect: func(*cellRun) map[string]invariants.Expect {
		exp := map[string]invariants.Expect{}
		expectBumps(exp, leaseBumps...)
		return exp
	},
}

func TestSweepLease(t *testing.T) { runSweep(t, scenarioLease) }
