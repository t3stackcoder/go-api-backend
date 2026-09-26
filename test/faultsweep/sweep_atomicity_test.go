//go:build faultsweep && faultinject

package faultsweep

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// SweepCommandAtomicity (spec 11.4, G4): a keyed command writes a register
// row, publishes two durable events on two keys, and has an in-process
// handler writing a second row; a second one arrives over HTTP; a third
// command fails in its handler so the rollback path is a step too. Expect:
// everything committed or nothing, the idempotency table agrees, the
// handler ran at most once per Send, and after the client's retry every
// command committed exactly once.

var (
	atomCmd1 = workload.AtomicScenario{CmdID: "atom-1", Key1: "a", Key2: "b", Val: 7, CmdKey: "atom-1"}
	atomCmd2 = workload.AtomicScenario{CmdID: "atom-2", Key1: "c", Key2: "d", Val: 8, CmdKey: "atom-2"}
	atomFail = workload.Transfer{From: "a", To: "b", Amt: 10_000, CmdID: "atom-fail"}
)

func atomBody() []byte {
	b, err := json.Marshal(atomCmd2)
	if err != nil {
		panic(err)
	}
	return b
}

type atomState struct {
	invocations atomic.Int64
	url         string
	limit       int64
}

// countingPre counts handler invocations of AtomicScenario (G1).
type countingPre struct{ n *atomic.Int64 }

func (p countingPre) Process(context.Context, workload.AtomicScenario) error {
	p.n.Add(1)
	return nil
}

var scenarioAtomicity = &scenario{
	name:     "SweepCommandAtomicity",
	patterns: []string{"pg.tx.*", "pg.outbox.*", "pg.idem.*", "http.decode", "http.encode"},
	groups:   []string{},
	variants: []string{"pool1", "stmt50", "body"},
	newState: func() any { return &atomState{} },
	build: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*atomState)
		n, err := c.newNode(ctx, nodeOpts{id: c.nodeID("n1"), workload: true, register: func(m *mediator.Mediator) error {
			return mediator.Pre[workload.AtomicScenario, mediator.Void](m, countingPre{&st.invocations})
		}})
		if err != nil {
			return err
		}
		st.limit = 1 << 20
		if c.variant != nil && c.variant.maxBody {
			st.limit = int64(len(atomBody()))
			if c.phase == phaseRun {
				st.limit--
			}
		}
		srv, err := httpapi.New(n.m, httpapi.Config{MaxBodyBytes: st.limit, Logger: c.logger})
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		hs := httptest.NewServer(srv.Handler())
		st.url = hs.URL
		n.onClose(hs.Close)
		return nil
	},
	run: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*atomState)
		n := c.nodes[0]
		before := st.invocations.Load()
		_, err := c.send(ctx, n.m, atomCmd1)
		if got := st.invocations.Load() - before; got > 1 {
			c.t.Errorf("G1: one Send of atom-1 invoked the handler %d times", got)
		}
		c.note("atom-1: %v", err)
		status, herr := c.post(ctx, st.url, workload.NameAtomicScenario, atomBody())
		c.note("atom-2 over HTTP: status=%d err=%v", status, herr)
		if c.variant != nil && c.variant.maxBody {
			if status != http.StatusRequestEntityTooLarge {
				c.t.Errorf("MaxBodyBytes=%d: atom-2 got status %d (%v), want 413", st.limit, status, herr)
			}
			var rows int64
			if qerr := n.pool.QueryRow(ctx, `SELECT count(*) FROM wl_cmd_log WHERE cmd_id = 'atom-2'`).Scan(&rows); qerr != nil || rows != 0 {
				c.t.Errorf("a rejected body left %d wl_cmd_log rows (%v)", rows, qerr)
			}
		}
		_, ferr := c.send(ctx, n.m, atomFail)
		switch {
		case ferr == nil:
			c.t.Errorf("transfer of 10000 from an account with 100 succeeded")
		case mediator.CodeOf(ferr) != mediator.CodePrecondition:
			c.note("atom-fail: %v", ferr)
		}
		return nil
	},
	recover: func(ctx context.Context, c *cellRun) error {
		st := c.st.(*atomState)
		n := c.nodes[0]
		res, err := c.retrySend(ctx, n.m, "", atomCmd1, 8)
		if err != nil {
			return fmt.Errorf("atom-1 retry: %w", err)
		}
		c.record("atom-1", res)
		var status int
		for i := 0; i < 8; i++ {
			status, err = c.post(ctx, st.url, workload.NameAtomicScenario, atomBody())
			if err == nil && status == http.StatusNoContent {
				break
			}
			time.Sleep(150 * time.Millisecond)
		}
		if err != nil || status != http.StatusNoContent {
			return fmt.Errorf("atom-2 over HTTP: status=%d err=%v", status, err)
		}
		if _, err := c.send(ctx, n.m, atomFail); mediator.CodeOf(err) != mediator.CodePrecondition {
			return fmt.Errorf("atom-fail after recovery: %v", err)
		}
		return nil
	},
	verify: func(ctx context.Context, c *cellRun, v *verifier) {
		for _, id := range []string{"atom-1", "atom-2"} {
			v.expectCount(ctx, 1, id+" executions", `SELECT coalesce(sum(n), 0) FROM wl_executions WHERE scope = $1 AND key = $2`, workload.NameAtomicScenario, id)
			v.expectCount(ctx, 1, id+" side row", `SELECT count(*) FROM wl_side WHERE cmd_id = $1`, id)
			v.expectCount(ctx, 2, id+" outbox rows", `SELECT count(*) FROM mediator_outbox WHERE headers->'h'->>$1 = $2`, workload.HeaderCmd, id)
		}
		v.expectCount(ctx, 0, "atom-fail marker", `SELECT count(*) FROM wl_cmd_log WHERE cmd_id = 'atom-fail'`)
		v.expectCount(ctx, workload.BankTotal(), "bank total", `SELECT coalesce(sum(balance), 0) FROM wl_bank`)
		v.expectCount(ctx, 7, "register a", `SELECT val FROM wl_register WHERE key = 'a'`)
		v.expectCount(ctx, 8, "register c", `SELECT val FROM wl_register WHERE key = 'c'`)
	},
	expect: func(*cellRun) map[string]invariants.Expect {
		exp := map[string]invariants.Expect{}
		exp["atom-1"], _ = invariants.ExpectFor(atomCmd1)
		exp["atom-2"], _ = invariants.ExpectFor(atomCmd2)
		exp["atom-fail"] = invariants.Expect{Name: workload.NameTransfer}
		return exp
	},
}

// post sends a JSON body to the RPC route of a command under a request
// context registered for the cancel kind.
func (c *cellRun) post(ctx context.Context, base, name string, body []byte) (int, error) {
	rctx, cancel := c.reqCtx(ctx)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, base+"/rpc/"+name, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode, nil
}

func TestSweepCommandAtomicity(t *testing.T) { runSweep(t, scenarioAtomicity) }
