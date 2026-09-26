//go:build chaos

package chaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
)

// op is one operation a logical client performs: how it is recorded in the
// history (F, Key, Value, Extra), how it is sent (Req), and the client's
// policy for it.
type op struct {
	F     string
	Key   string
	Value any
	Extra map[string]any
	Req   request

	// CmdID is the command ID; Keyed reports that it is the idempotency key
	// of the command, so a retired info op can be resolved by re-sending it
	// (L2) and a retry is safe.
	CmdID string
	Keyed bool
	// Expect and HasExpect feed I1.
	Expect    invariants.Expect
	HasExpect bool
	// RetryUntilDefinite retries an info result until a definite one arrives
	// (bank-idempotent). RetryInfoMax bounds the extra attempts after an
	// info result otherwise (idempotent-append). RandomResend re-sends an ok
	// command once with probability 0.2 and records both responses for the
	// byte-identical check (idempotent-append, G8).
	RetryUntilDefinite bool
	RetryInfoMax       int
	RandomResend       bool
	// OKValue decodes the recorded value of an ok result; nil records Value.
	OKValue func(res result, pend *history.Pending) any
}

// pendingInfo is a retired info operation of a keyed command, to be
// re-sent after healing (L2).
type pendingInfo struct {
	Op      op
	Process int
	Err     string
}

// client is one logical Jepsen client: a process with at most one
// operation outstanding, bound to one node, with its own generator.
type client struct {
	name    string
	node    *node
	r       *run
	process int
	rng     *rand.Rand
}

// newClients creates Clients logical clients per node.
func (r *run) newClients() {
	for i, n := range r.ctl.nodes {
		for j := 0; j < r.cfg.Clients; j++ {
			c := &client{
				name: fmt.Sprintf("%s/c%d", n.ID, j), node: n, r: r,
				process: r.rec.NextProcess(),
				rng:     rand.New(rand.NewPCG(uint64(r.cfg.Seed), uint64(1000+i*100+j))), //nolint:gosec // seeded generator: reproducibility (spec 11.6)
			}
			r.clients = append(r.clients, c)
		}
	}
}

// runClients starts every client. A client stops starting new operations
// when stop is closed and abandons a retry loop when ctx ends.
func (r *run) runClients(ctx context.Context, stop <-chan struct{}) *sync.WaitGroup {
	var wg sync.WaitGroup
	for _, c := range r.clients {
		wg.Add(1)
		go func(c *client) {
			defer wg.Done()
			c.loop(ctx, stop)
		}(c)
	}
	return &wg
}

func (c *client) loop(ctx context.Context, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		default:
		}
		c.perform(ctx, c.r.wl.next(c))
		d := time.Duration(float64(time.Second) / c.r.cfg.Rate * (0.5 + c.rng.Float64()))
		select {
		case <-stop:
			return
		case <-ctx.Done():
			return
		case <-time.After(d):
		}
	}
}

// otherNode picks a node other than the client's own.
func (c *client) otherNode() *node {
	nodes := c.r.ctl.nodes
	if len(nodes) == 1 {
		return nodes[0]
	}
	i := c.rng.IntN(len(nodes) - 1)
	if i >= c.node.Index {
		i++
	}
	return nodes[i]
}

func (c *client) anyNode() *node {
	nodes := c.r.ctl.nodes
	return nodes[c.rng.IntN(len(nodes))]
}

// perform records the invocation, sends the request with the op's retry
// policy, and records the completion.
func (c *client) perform(ctx context.Context, o op) {
	pend := c.r.rec.Invoke(c.process, c.name, o.F, o.Key, o.Value)
	for k, v := range o.Extra {
		pend.WithExtra(k, v)
	}
	if o.HasExpect && o.CmdID != "" {
		c.r.addExpect(o.CmdID, o.Expect)
	}
	res := c.node.call(ctx, o.Req)
	attempts := 1
	// G16: a dial failure means no node saw the request; retry once elsewhere.
	if res.Outcome == outcomeFail && res.Dial && len(c.r.ctl.nodes) > 1 {
		res = c.otherNode().call(ctx, o.Req)
		attempts++
	}
	if o.Keyed && (o.RetryUntilDefinite || o.RetryInfoMax > 0) {
		backoff := 250 * time.Millisecond
		extra := 0
		for res.Outcome == outcomeInfo && ctx.Err() == nil {
			if !o.RetryUntilDefinite && extra >= o.RetryInfoMax {
				break
			}
			select {
			case <-ctx.Done():
			case <-time.After(backoff + time.Duration(c.rng.Int64N(int64(backoff)))):
			}
			if ctx.Err() != nil {
				break
			}
			res = c.anyNode().call(ctx, o.Req)
			attempts++
			extra++
			if backoff *= 2; backoff > 3*time.Second {
				backoff = 3 * time.Second
			}
		}
	}
	if o.RandomResend && o.Keyed && res.Outcome == outcomeOK && c.rng.Float64() < 0.2 {
		again := c.anyNode().call(ctx, o.Req)
		attempts++
		pend.WithExtra("resend", again.Outcome.String())
		if again.Outcome == outcomeOK {
			c.r.addResponse(o.CmdID, res, again)
		}
	}
	if attempts > 1 {
		pend.WithExtra("attempts", attempts)
	}
	if res.Cache != "" {
		pend.WithExtra("cache", res.Cache)
	}
	c.r.count(res.Outcome)
	switch res.Outcome {
	case outcomeOK:
		v := o.Value
		if o.OKValue != nil {
			v = o.OKValue(res, pend)
		}
		pend.OK(v)
	case outcomeFail:
		pend.Fail(errors.New(res.Err))
	default:
		_, fresh := pend.Info(errors.New(res.Err))
		c.process = fresh
		if o.Keyed {
			c.r.addInfo(pendingInfo{Op: o, Process: pend.Op().Process, Err: res.Err})
		}
	}
}

// finalOp performs one operation of the final-reads phase on a ready node
// with a fresh process, trying every node before giving up.
func (r *run) finalOp(ctx context.Context, o op) {
	process := r.rec.NextProcess()
	pend := r.rec.Invoke(process, "final", o.F, o.Key, o.Value)
	for k, v := range o.Extra {
		pend.WithExtra(k, v)
	}
	var res result
	for attempt := 0; attempt < 3*len(r.ctl.nodes); attempt++ {
		n := r.ctl.nodes[attempt%len(r.ctl.nodes)]
		res = n.call(ctx, o.Req)
		if res.Outcome == outcomeOK {
			break
		}
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
		if ctx.Err() != nil {
			break
		}
	}
	r.count(res.Outcome)
	switch res.Outcome {
	case outcomeOK:
		v := o.Value
		if o.OKValue != nil {
			v = o.OKValue(res, pend)
		}
		pend.OK(v)
	case outcomeFail:
		pend.Fail(errors.New(res.Err))
	default:
		pend.Info(errors.New(res.Err))
	}
}

// maxExpects caps the commands fed to I1 so a soak run's check stays bounded.
const maxExpects = 5000

func (r *run) addExpect(id string, e invariants.Expect) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.expects) >= maxExpects {
		// Keep a bounded, recent sample: drop one arbitrary entry.
		for k := range r.expects {
			delete(r.expects, k)
			break
		}
	}
	r.expects[id] = e
}

func (r *run) addResponse(id string, results ...result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, res := range results {
		r.responses[id] = append(r.responses[id], append([]byte(fmt.Sprintf("%d ", res.Status)), res.Body...))
	}
}

func (r *run) addInfo(p pendingInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.infos = append(r.infos, p)
}

func (r *run) count(o outcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[o.String()]++
}

func (r *run) takeInfos() []pendingInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.infos
	r.infos = nil
	return out
}
