//go:build faultsweep && faultinject

package faultsweep

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// SweepIdempotency (spec 11.4, G8): two clients with the same key, the
// second starting at every step of the first (the second client waits until
// the armed point is reached, then sends). Both retry until they hold a
// response. Expect: one execution, byte-identical responses, no leaked
// reservation.

var idemCmd = workload.Append{Key: "L", Val: 1, CmdID: "idem-1"}

type idemState struct {
	aRes, bRes any
	aErr, bErr error
}

var scenarioIdempotency = &scenario{
	name:     "SweepIdempotency",
	patterns: []string{"pg.idem.*", "pg.tx.*"},
	groups:   []string{},
	variants: []string{"pool1", "stmt50"},
	newState: func() any { return &idemState{} },
	build: func(ctx context.Context, c *cellRun) error {
		_, err := c.newNode(ctx, nodeOpts{id: c.nodeID("n1"), workload: true})
		return err
	},
	run: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*idemState)
		n := c.nodes[0]
		aDone := make(chan struct{})
		go func() {
			defer close(aDone)
			st.aRes, st.aErr = c.send(ctx, n.m, idemCmd)
		}()
		// Client B starts when A reaches the armed step, or when A is done.
	wait:
		for {
			select {
			case <-aDone:
				break wait
			default:
			}
			if c.run.Kind != "" && testkit.Hits()[c.run.Point] >= c.run.Hit {
				break wait
			}
			time.Sleep(time.Millisecond)
		}
		st.bRes, st.bErr = c.send(ctx, n.m, idemCmd)
		<-aDone
		c.note("first attempts: a=%v b=%v", st.aErr, st.bErr)
		for i := 0; i < 8 && (st.aErr != nil || st.bErr != nil) && ctx.Err() == nil; i++ {
			time.Sleep(150 * time.Millisecond)
			if st.aErr != nil {
				st.aRes, st.aErr = c.send(ctx, n.m, idemCmd)
			}
			if st.bErr != nil {
				st.bRes, st.bErr = c.send(ctx, n.m, idemCmd)
			}
		}
		if st.aErr == nil {
			c.record(idemCmd.CmdID, st.aRes)
		}
		if st.bErr == nil {
			c.record(idemCmd.CmdID, st.bRes)
		}
		return nil
	},
	recover: func(ctx context.Context, c *cellRun) error {
		n := c.nodes[0]
		a, err := c.retrySend(ctx, n.m, "", idemCmd, 8)
		if err != nil {
			return fmt.Errorf("client A: %w", err)
		}
		b, err := c.retrySend(ctx, n.m, "", idemCmd, 8)
		if err != nil {
			return fmt.Errorf("client B: %w", err)
		}
		c.record(idemCmd.CmdID, a)
		c.record(idemCmd.CmdID, b)
		ja, _ := json.Marshal(a)
		jb, _ := json.Marshal(b)
		if !bytes.Equal(ja, jb) {
			return fmt.Errorf("responses differ after recovery: %s vs %s", ja, jb)
		}
		return nil
	},
	verify: func(ctx context.Context, c *cellRun, v *verifier) {
		v.expectCount(ctx, 1, "executions", `SELECT coalesce(sum(n), 0) FROM wl_executions WHERE scope = $1 AND key = $2`, workload.NameAppend, idemCmd.CmdID)
		v.expectCount(ctx, 1, "appended rows", `SELECT count(*) FROM wl_appends WHERE cmd_id = $1`, idemCmd.CmdID)
		v.expectCount(ctx, 1, "completed idempotency rows", `SELECT count(*) FROM mediator_idempotency WHERE scope = $1 AND key = $2 AND response IS NOT NULL`, workload.NameAppend, idemCmd.CmdID)
	},
	expect: func(*cellRun) map[string]invariants.Expect {
		e, _ := invariants.ExpectFor(idemCmd)
		return map[string]invariants.Expect{idemCmd.CmdID: e}
	},
}

func TestSweepIdempotency(t *testing.T) { runSweep(t, scenarioIdempotency) }
