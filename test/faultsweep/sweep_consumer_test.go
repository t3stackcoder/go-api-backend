//go:build faultsweep && faultinject

package faultsweep

import (
	"context"
	"fmt"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// SweepConsumer (spec 11.4, G7): a consumer processing three events for one
// key with a strict projection, plus one poison event that the poison group
// dead-letters so the dead-letter path is a step too. Expect: the
// projection applied exactly once per event in order, inbox rows present,
// every entry acknowledged after recovery, the poison event dead-lettered
// once (duplicate dead letters of one entry permitted).

const poisonKey = "poison"

var (
	consumerGroups = []string{workload.GroupReadModel, workload.GroupPoison}
	consumerBumps  = append(bumpCommands("k1", "cb", 3), workload.Bump{Key: poisonKey, CmdID: "cp-1"})
)

var scenarioConsumer = &scenario{
	name:     "SweepConsumer",
	patterns: []string{"redis.xreadgroup", "redis.xautoclaim", "redis.xack", "redis.xadd.dlq", "redis.stream.replay", "pg.inbox.insert", "pg.tx.*"},
	groups:   consumerGroups,
	skipDLQ:  true,
	variants: []string{"pool1", "stmt50", "oom"},
	build: func(ctx context.Context, c *cellRun) error {
		n, err := c.newNode(ctx, nodeOpts{id: c.nodeID("n1"), workload: true,
			deps: workload.Deps{Groups: consumerGroups, PoisonKey: poisonKey, StrictProjection: true}})
		if err != nil {
			return err
		}
		n.add("relay", newRelay(c, n, consumerGroups))
		n.add("consumers", redisx.NewConsumers(n.m, n.client, n.cfg, n.store, redisx.WithLogger(c.logger)))
		return nil
	},
	run: func(ctx context.Context, c *cellRun) error {
		n := c.nodes[0]
		for _, b := range consumerBumps {
			if _, err := c.retrySend(ctx, n.m, "", b, 3); err != nil {
				c.note("%s: %v", b.CmdID, err)
			}
		}
		return waitFor(ctx, runWait, drained(n.pool, n.client, n.cfg, consumerGroups))
	},
	recover: func(ctx context.Context, c *cellRun) error {
		n := c.nodes[0]
		for _, b := range consumerBumps {
			if _, err := c.retrySend(ctx, n.m, "", b, 8); err != nil {
				return fmt.Errorf("%s: %w", b.CmdID, err)
			}
		}
		return c.drainedAndSettled(ctx, n, consumerGroups)
	},
	verify: func(ctx context.Context, c *cellRun, v *verifier) {
		checkProjection(ctx, v, "k1", 3)
		checkProjection(ctx, v, poisonKey, 1)
		checkDLQ(ctx, v, workload.GroupPoison, poisonKey, true)
		checkDLQ(ctx, v, workload.GroupReadModel, "", false)
		v.expectCount(ctx, 3, "read_model applies of k1", `SELECT count(*) FROM wl_applied WHERE grp = $1 AND key = 'k1'`, workload.GroupReadModel)
		v.expectCount(ctx, 3, "poison group applies of k1", `SELECT count(*) FROM wl_applied WHERE grp = $1 AND key = 'k1'`, workload.GroupPoison)
		v.expectCount(ctx, 4, "read_model inbox rows", `SELECT count(*) FROM mediator_inbox WHERE consumer_group = $1`, workload.GroupReadModel)
	},
	expect: func(*cellRun) map[string]invariants.Expect {
		exp := map[string]invariants.Expect{}
		expectBumps(exp, consumerBumps...)
		return exp
	},
}

func TestSweepConsumer(t *testing.T) { runSweep(t, scenarioConsumer) }
