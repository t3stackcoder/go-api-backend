//go:build chaos

package chaos

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// scenario is one row of the workloads table of spec 11.6: how clients
// generate operations, what the final reads are, and which checker decides.
type scenario interface {
	name() string
	// remote reports the remote-send topology: every node dispatches
	// commands remotely and exactly one node serves them.
	remote() bool
	next(c *client) op
	finalReads(ctx context.Context, r *run) error
	// check is the offline workload checker over the recorded history; it
	// also runs in TestReplay.
	check(ops []history.Op, windows []history.Window, htmlPath string, timeout time.Duration) []history.Violation
	// checkDB runs the workload's database checks that need the live stores.
	checkDB(ctx context.Context, r *run, ops []history.Op) ([]history.Violation, error)
}

// workloadNames lists the eight workloads.
var workloadNames = []string{"register", "register-cached", "bank", "bank-idempotent", "idempotent-append", "events", "remote-send", "cache-staleness"}

func newWorkload(name string) (scenario, error) {
	switch name {
	case "register":
		return &registerWL{wlName: name, keys: 5, readRatio: 0.5, checker: "register"}, nil
	case "register-cached":
		return &registerWL{wlName: name, cached: true, keys: 5, readRatio: 0.5, checker: "staleness"}, nil
	case "bank":
		return &bankWL{}, nil
	case "bank-idempotent":
		return &bankWL{idempotent: true}, nil
	case "idempotent-append":
		return &appendWL{keys: 3}, nil
	case "events":
		return &eventsWL{keys: 8}, nil
	case "remote-send":
		return &registerWL{wlName: name, keyed: true, remoteMode: true, keys: 5, readRatio: 0.5, checker: "register"}, nil
	case "cache-staleness":
		return &registerWL{wlName: name, cached: true, keys: 2, readRatio: 0.7, checker: "i7"}, nil
	}
	return nil, fmt.Errorf("chaos: unknown workload %q (one of %v)", name, workloadNames)
}

// opsWithF selects the ops of the given functions.
func opsWithF(ops []history.Op, fs ...string) []history.Op {
	want := map[string]bool{}
	for _, f := range fs {
		want[f] = true
	}
	out := make([]history.Op, 0, len(ops))
	for _, op := range ops {
		if want[op.F] {
			out = append(out, op)
		}
	}
	return out
}

// porcupineCheck runs Porcupine and renders its verdict as violations.
func porcupineCheck(model porcupine.Model, pops []porcupine.Operation, timeout time.Duration, htmlPath string) []history.Violation {
	res, err := history.Check(model, pops, timeout, htmlPath)
	var out []history.Violation
	if err != nil {
		out = append(out, history.Violation{ID: "Porcupine", Msg: "visualization: " + err.Error()})
	}
	switch res.Check {
	case porcupine.Ok:
	case porcupine.Illegal:
		out = append(out, history.Violation{ID: "Porcupine", Msg: "history is not linearizable", Details: map[string]any{"ops": len(pops), "html": res.HTML}})
	default:
		out = append(out, history.Violation{ID: "Porcupine", Msg: "linearizability check did not finish within the budget", Details: map[string]any{"ops": len(pops), "timeout": timeout.String()}})
	}
	return out
}

// extendWindows returns the windows with their end extended by ttl, the
// form invariants.I7 expects.
func extendWindows(windows []history.Window, ttl time.Duration) []history.Window {
	out := make([]history.Window, len(windows))
	for i, w := range windows {
		w.End += int64(ttl)
		out[i] = w
	}
	return out
}

func decodeValue(res result, _ *history.Pending) any {
	var v workload.ValueView
	if err := json.Unmarshal(res.Body, &v); err != nil {
		return string(res.Body)
	}
	if !v.Found {
		return nil
	}
	return v.Val
}

// registerWL is register, register-cached, remote-send, and
// cache-staleness: SetValue and GetValue (or GetValueCached) on a small key
// space.
type registerWL struct {
	wlName     string
	cached     bool // GetValueCached with invalidating writes
	keyed      bool // SetValue carries its idempotency key
	remoteMode bool
	keys       int
	readRatio  float64
	checker    string // register, staleness, i7
}

func (w *registerWL) name() string { return w.wlName }
func (w *registerWL) remote() bool { return w.remoteMode }

func (w *registerWL) keyName(i int) string { return fmt.Sprintf("k%d", i) }

func (w *registerWL) next(c *client) op {
	key := w.keyName(c.rng.IntN(w.keys))
	if c.rng.Float64() < w.readRatio {
		return w.readOp(key, w.cached)
	}
	n := c.r.next()
	cmd := c.r.cmdID("set", n)
	body := map[string]any{"key": key, "val": n, "cmdId": cmd, "keyed": w.keyed, "invalidate": w.cached}
	exp, _ := invariants.ExpectFor(workload.SetValue{Key: key, Val: n, CmdID: cmd, Keyed: w.keyed, Invalidate: w.cached})
	o := op{
		F: history.FSet, Key: key, Value: n, Extra: map[string]any{"cmd": cmd},
		Req:   request{Method: http.MethodPost, Path: "/rpc/" + workload.NameSetValue, Body: body},
		CmdID: cmd, Keyed: w.keyed, Expect: exp, HasExpect: true,
	}
	if w.cached {
		o.Extra["tags"] = []string{workload.RegisterTag(key)}
	}
	return o
}

func (w *registerWL) readOp(key string, cached bool) op {
	name := workload.NameGetValue
	if cached {
		name = workload.NameGetValueCached
	}
	o := op{F: history.FGet, Key: key, Req: request{Method: http.MethodGet, Path: "/rpc/" + name, Query: url.Values{"key": {key}}}, OKValue: decodeValue}
	if cached {
		o.Extra = map[string]any{"tags": []string{workload.RegisterTag(key)}}
	}
	return o
}

// finalReads reads every key without the cache.
func (w *registerWL) finalReads(ctx context.Context, r *run) error {
	for i := 0; i < w.keys; i++ {
		r.finalOp(ctx, w.readOp(w.keyName(i), false))
	}
	return nil
}

func (w *registerWL) check(ops []history.Op, windows []history.Window, htmlPath string, timeout time.Duration) []history.Violation {
	switch w.checker {
	case "staleness":
		return history.BoundedStalenessChecker(ops, windows, workload.CacheTTL)
	case "i7":
		reads, writes := history.ReadsAndWrites(ops)
		return invariants.I7(reads, writes, extendWindows(windows, workload.CacheTTL))
	default:
		pops := history.ToPorcupine(opsWithF(ops, history.FSet, history.FGet), history.DropFails)
		return porcupineCheck(history.RegisterModel(), pops, timeout, htmlPath)
	}
}

// checkDB verifies, for remote-send, that every ok keyed SetValue executed
// exactly once (G10): wl_executions.n == 1 for its command ID.
func (w *registerWL) checkDB(ctx context.Context, r *run, ops []history.Op) ([]history.Violation, error) {
	if !w.keyed {
		return nil, nil
	}
	rows, err := r.ctl.pool.Query(ctx, `SELECT key, n FROM wl_executions WHERE scope = $1`, workload.NameSetValue)
	if err != nil {
		return nil, fmt.Errorf("chaos: executions: %w", err)
	}
	defer rows.Close()
	execs := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		execs[k] = n
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []history.Violation
	for _, op := range ops {
		if op.Type != history.TypeOK || op.F != history.FSet {
			continue
		}
		cmd, _ := op.Extra["cmd"].(string)
		if cmd == "" {
			continue
		}
		if n := execs[cmd]; n != 1 {
			out = append(out, history.Violation{ID: "Executions", Msg: "ok keyed command did not execute exactly once", Details: map[string]any{"cmd": cmd, "executions": n, "process": op.Process}})
			if len(out) >= 20 {
				break
			}
		}
	}
	return out, nil
}

// bankWL is bank and bank-idempotent: Transfer between four accounts and
// ReadAll, checked by the Porcupine bank model and conservation.
type bankWL struct {
	idempotent bool
}

func (w *bankWL) name() string {
	if w.idempotent {
		return "bank-idempotent"
	}
	return "bank"
}
func (w *bankWL) remote() bool { return false }

func (w *bankWL) next(c *client) op {
	if c.rng.Float64() < 0.4 {
		return w.readOp()
	}
	accounts := workload.Accounts
	from := accounts[c.rng.IntN(len(accounts))]
	to := accounts[c.rng.IntN(len(accounts)-1)]
	if to == from {
		to = accounts[len(accounts)-1]
	}
	amt := int64(1 + c.rng.IntN(30))
	n := c.r.next()
	cmd := c.r.cmdID("transfer", n)
	body := map[string]any{"from": from, "to": to, "amt": amt, "cmdId": cmd, "keyed": w.idempotent}
	exp, _ := invariants.ExpectFor(workload.Transfer{From: from, To: to, Amt: amt, CmdID: cmd, Keyed: w.idempotent})
	return op{
		F: history.FTransfer, Key: from + ">" + to, Value: history.TransferValue{From: from, To: to, Amt: amt},
		Extra: map[string]any{"cmd": cmd},
		Req:   request{Method: http.MethodPost, Path: "/rpc/" + workload.NameTransfer, Body: body},
		CmdID: cmd, Keyed: w.idempotent, Expect: exp, HasExpect: true, RetryUntilDefinite: w.idempotent,
	}
}

func decodeBank(res result, _ *history.Pending) any {
	var v workload.BankView
	if err := json.Unmarshal(res.Body, &v); err != nil {
		return string(res.Body)
	}
	return v
}

func (w *bankWL) readOp() op {
	return op{F: history.FReadAll, Key: "bank", Req: request{Method: http.MethodGet, Path: "/rpc/" + workload.NameReadAll}, OKValue: decodeBank}
}

func (w *bankWL) finalReads(ctx context.Context, r *run) error {
	for range r.ctl.nodes {
		r.finalOp(ctx, w.readOp())
	}
	return nil
}

func (w *bankWL) check(ops []history.Op, _ []history.Window, htmlPath string, timeout time.Duration) []history.Violation {
	var out []history.Violation
	pops := history.ToPorcupine(opsWithF(ops, history.FTransfer, history.FReadAll), history.DropFailsExcept(history.InsufficientFunds))
	out = append(out, porcupineCheck(history.BankModel(workload.Accounts, workload.InitialBalance), pops, timeout, htmlPath)...)
	out = append(out, history.Conservation(ops, workload.BankTotal())...)
	if w.idempotent {
		infos := 0
		for _, op := range ops {
			if op.F == history.FTransfer && op.Type == history.TypeInfo {
				infos++
			}
		}
		if infos > 0 {
			out = append(out, history.Violation{ID: "BankIdempotent", Msg: "info transfers remain although clients retry until definite", Details: map[string]any{"info": infos}})
		}
	}
	return out
}

func (w *bankWL) checkDB(context.Context, *run, []history.Op) ([]history.Violation, error) {
	return nil, nil
}

// appendWL is idempotent-append: Append with random client retries and a
// final ReadList per key, checked by AppendListChecker.
type appendWL struct {
	keys int
}

func (w *appendWL) name() string { return "idempotent-append" }
func (w *appendWL) remote() bool { return false }

func (w *appendWL) keyName(i int) string { return fmt.Sprintf("l%d", i) }

func (w *appendWL) next(c *client) op {
	key := w.keyName(c.rng.IntN(w.keys))
	n := c.r.next()
	cmd := c.r.cmdID("append", n)
	body := map[string]any{"key": key, "val": n, "cmdId": cmd}
	exp, _ := invariants.ExpectFor(workload.Append{Key: key, Val: n, CmdID: cmd})
	return op{
		F: history.FAppend, Key: key, Value: n, Extra: map[string]any{"cmd": cmd},
		Req:   request{Method: http.MethodPost, Path: "/rpc/" + workload.NameAppend, Body: body},
		CmdID: cmd, Keyed: true, Expect: exp, HasExpect: true, RetryInfoMax: 3, RandomResend: true,
		OKValue: func(res result, pend *history.Pending) any {
			var v workload.AppendResult
			if err := json.Unmarshal(res.Body, &v); err == nil {
				pend.WithExtra("applied", v.Applied)
			}
			return n
		},
	}
}

func decodeList(res result, _ *history.Pending) any {
	var v []int64
	if err := json.Unmarshal(res.Body, &v); err != nil {
		return string(res.Body)
	}
	return v
}

func (w *appendWL) finalReads(ctx context.Context, r *run) error {
	for i := 0; i < w.keys; i++ {
		key := w.keyName(i)
		r.finalOp(ctx, op{F: history.FReadList, Key: key, Req: request{Method: http.MethodGet, Path: "/rpc/" + workload.NameReadList, Query: url.Values{"key": {key}}}, OKValue: decodeList})
	}
	return nil
}

func (w *appendWL) check(ops []history.Op, _ []history.Window, _ string, _ time.Duration) []history.Violation {
	return history.AppendListChecker(ops)
}

func (w *appendWL) checkDB(context.Context, *run, []history.Op) ([]history.Violation, error) {
	return nil, nil
}

// eventsWL is events: keyed Bump commands publishing Bumped; the projection
// and audit consumers are checked by I3, I6, and DLQEmpty after quiescence.
type eventsWL struct {
	keys int
}

func (w *eventsWL) name() string { return "events" }
func (w *eventsWL) remote() bool { return false }

func (w *eventsWL) next(c *client) op {
	key := fmt.Sprintf("e%d", c.rng.IntN(w.keys))
	n := c.r.next()
	cmd := c.r.cmdID("bump", n)
	body := map[string]any{"key": key, "cmdId": cmd}
	exp, _ := invariants.ExpectFor(workload.Bump{Key: key, CmdID: cmd})
	return op{
		F: history.FBump, Key: key, Extra: map[string]any{"cmd": cmd},
		Req:   request{Method: http.MethodPost, Path: "/rpc/" + workload.NameBump, Body: body},
		CmdID: cmd, Keyed: true, Expect: exp, HasExpect: true,
		OKValue: func(res result, _ *history.Pending) any {
			var v workload.BumpResult
			if err := json.Unmarshal(res.Body, &v); err != nil {
				return string(res.Body)
			}
			return v.N
		},
	}
}

func (w *eventsWL) finalReads(context.Context, *run) error { return nil }

// check verifies that the counter values returned by ok bumps are distinct
// per key: the counter is transactional, so two commands can never observe
// the same n.
func (w *eventsWL) check(ops []history.Op, _ []history.Window, _ string, _ time.Duration) []history.Violation {
	seen := map[string]map[int64]int{}
	var out []history.Violation
	for _, op := range ops {
		if op.F != history.FBump || op.Type != history.TypeOK {
			continue
		}
		n, ok := history.Int(op.Value)
		if !ok {
			continue
		}
		if seen[op.Key] == nil {
			seen[op.Key] = map[int64]int{}
		}
		if prev, dup := seen[op.Key][n]; dup {
			out = append(out, history.Violation{ID: "Bump", Msg: "two ok bumps returned the same counter value", Details: map[string]any{"key": op.Key, "n": n, "process": op.Process, "previous_process": prev}})
		}
		seen[op.Key][n] = op.Process
	}
	return out
}

// checkDB is the G12 part of the events workload: after a redis-flush or
// redis-restore-old nemesis the relay must have detected the loss and
// replayed the published outbox rows, so its replay counter (live, or the
// count of its log records when a node restarted) is non-zero.
func (w *eventsWL) checkDB(ctx context.Context, r *run, _ []history.Op) ([]history.Violation, error) {
	var loss []string
	for _, ev := range r.nem.all() {
		if ev.Type != "start" || (ev.Name != "redis-flush" && ev.Name != "redis-restore-old") {
			continue
		}
		if noop, _ := ev.Params["noop"].(bool); noop {
			continue
		}
		loss = append(loss, fmt.Sprintf("%s#%d@%.1fs", ev.Name, ev.ID, float64(ev.Time)/1e9))
	}
	if len(loss) == 0 {
		return nil, nil
	}
	var published int64
	if err := r.ctl.pool.QueryRow(ctx, `SELECT count(*) FROM mediator_outbox WHERE published_at IS NOT NULL`).Scan(&published); err != nil {
		return nil, fmt.Errorf("chaos: outbox count: %w", err)
	}
	if published == 0 || r.sum.RelayReplayed > 0 || r.sum.RelayReplayedLog > 0 {
		return nil, nil
	}
	return []history.Violation{{ID: "G12", Msg: "relay replay counter is zero although Redis lost its data (spec 7.7, G12)", Details: map[string]any{
		"loss_nemeses": loss, "published_rows": published, "relay_replayed": r.sum.RelayReplayed, "relay_replayed_log": r.sum.RelayReplayedLog,
	}}}, nil
}

// sortedKeys renders map keys for reports.
func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
