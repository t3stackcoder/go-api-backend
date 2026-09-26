//go:build chaos

package chaos

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// run is one chaos run: its configuration, run directory, controller,
// workload, recorder, nemesis log, and the shared state the clients feed
// the checkers with.
type run struct {
	cfg     config
	log     *slog.Logger
	logFile *os.File
	dir     string
	token   string
	ctl     *controller
	wl      scenario
	rec     *history.Recorder
	nem     *nemesisLog
	nemRNG  *rand.Rand
	started time.Time
	seq     atomic.Int64

	mu        sync.Mutex
	expects   map[string]invariants.Expect
	responses map[string][][]byte
	infos     []pendingInfo
	counts    map[string]int

	clients []*client
	sum     summary
}

// newRun creates the run directory and the run.
func newRun(cfg config) (*run, error) {
	wl, err := newWorkload(cfg.Workload)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	dir := filepath.Join(cfg.RunParent, fmt.Sprintf("%s-seed%d-%s", cfg.Workload, cfg.Seed, started.Format("20060102-150405")))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "seed"), []byte(fmt.Sprintf("%d\n", cfg.Seed)), 0o600); err != nil {
		return nil, err
	}
	cfgJSON, err := cfg.marshal()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(dir, "config.json"), cfgJSON, 0o600); err != nil {
		return nil, err
	}
	logFile, err := os.Create(filepath.Join(dir, "controller.log"))
	if err != nil {
		return nil, err
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		abs = dir
	}
	logger := slog.New(slog.NewTextHandler(io.MultiWriter(logFile, os.Stderr), &slog.HandlerOptions{Level: slog.LevelInfo}))
	r := &run{
		cfg: cfg, log: logger, logFile: logFile, dir: dir,
		token:   fmt.Sprintf("s%d-%s", cfg.Seed, started.Format("150405")),
		wl:      wl,
		nemRNG:  rand.New(rand.NewPCG(uint64(cfg.Seed), 7)), //nolint:gosec // seeded generator: reproducibility (spec 11.6)
		started: started,
		expects: map[string]invariants.Expect{}, responses: map[string][][]byte{}, counts: map[string]int{},
		sum: summary{
			Workload: cfg.Workload, Seed: cfg.Seed, Duration: cfg.DurationText, Started: started.Format(time.RFC3339),
			RunDir: abs, Nodes: []string{}, Ops: map[string]int{}, Nemeses: map[string]int{},
			Checkers: map[string]checkResult{}, Timings: map[string]int64{}, Failures: []string{},
		},
	}
	return r, nil
}

func (r *run) close() { _ = r.logFile.Close() }

// next hands out the next unique value and command number.
func (r *run) next() int64 { return r.seq.Add(1) }

// cmdID builds a command ID unique across runs: <kind>-s<seed>-<hhmmss>-<n>.
func (r *run) cmdID(kind string, n int64) string {
	return fmt.Sprintf("%s-%s-%d", kind, r.token, n)
}

// fail records a harness-level failure.
func (r *run) fail(msg string) {
	r.mu.Lock()
	r.sum.Failures = append(r.sum.Failures, msg)
	r.mu.Unlock()
	r.log.Error(msg)
}

func (r *run) phase(name string, since time.Time) {
	r.mu.Lock()
	r.sum.Timings[name] = time.Since(since).Milliseconds()
	r.mu.Unlock()
	r.log.Info("phase done", "phase", name, "elapsed", time.Since(since).Round(time.Millisecond).String())
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// execute runs the schedule of spec 11.6: reset, warm-up, mayhem, heal,
// drain, quiesce (L1), resolve (L2), final reads, checkers, summary. It
// returns an error only when the harness itself cannot proceed; checker
// verdicts are in r.sum.
func (r *run) execute(ctx context.Context) error {
	r.log.Info("chaos run", "workload", r.cfg.Workload, "seed", r.cfg.Seed, "duration", r.cfg.DurationText, "nodes", r.cfg.Nodes, "dir", r.dir)
	ctl, err := newController(ctx, r.cfg, r.log, r.dir)
	if err != nil {
		return err
	}
	r.ctl = ctl
	defer ctl.close()
	for _, n := range ctl.nodes {
		r.sum.Nodes = append(r.sum.Nodes, n.ID)
	}

	// Reset and topology.
	since := time.Now()
	if err := ctl.reset(ctx); err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	ctl.setRemoteMode(r.wl.remote(), 0)
	if err := ctl.applyModes(ctx); err != nil {
		return fmt.Errorf("apply modes: %w", err)
	}
	r.phase("reset", since)

	// The history clock starts here; nemesis events share it.
	r.rec = history.NewRecorder()
	nem, err := newNemesisLog(filepath.Join(r.dir, "nemesis.jsonl"))
	if err != nil {
		return err
	}
	r.nem = nem
	defer nem.close()
	baseline := ctl.goroutineCounts(ctx)
	r.log.Info("goroutine baseline", "counts", fmt.Sprint(baseline))

	// Warm-up: clients only.
	since = time.Now()
	stop := make(chan struct{})
	hardCtx, hardCancel := context.WithCancel(ctx)
	defer hardCancel()
	r.newClients()
	wg := r.runClients(hardCtx, stop)
	sleepCtx(ctx, r.cfg.Warmup)
	r.phase("warmup", since)
	if err := ctl.snapshotRedis(ctx); err != nil {
		r.log.Warn("redis snapshot failed; redis-restore-old will be a no-op", "error", err)
	}

	// Mayhem.
	since = time.Now()
	nemCtx, nemCancel := context.WithCancel(ctx)
	nemDone := make(chan struct{})
	go func() {
		defer close(nemDone)
		runNemeses(nemCtx, r)
	}()
	r.mayhem(ctx, since)
	nemCancel()
	<-nemDone
	r.phase("mayhem", since)

	// Heal.
	since = time.Now()
	if err := ctl.heal(ctx); err != nil {
		r.fail("heal: " + err.Error())
	}
	r.phase("heal", since)

	// Drain the clients: no new operations; retry loops get the bound.
	since = time.Now()
	close(stop)
	drained := make(chan struct{})
	go func() {
		wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(r.cfg.RecoveryBound):
		r.log.Warn("clients still busy after the recovery bound; abandoning their operations as info")
		hardCancel()
		<-drained
	}
	r.phase("drain_clients", since)

	// L1: quiescence within the bound.
	since = time.Now()
	env := ctl.env(r.cfg.RecoveryBound)
	vs, err := invariants.L1(ctx, env)
	r.sum.Checkers["L1"] = newCheck(since, vs, err)
	r.phase("quiesce", since)

	// L2: every retired info op of a keyed command resolves when re-sent.
	since = time.Now()
	if r.resolveInfos(ctx) {
		vs, err := invariants.L1(ctx, env)
		if len(vs) > 0 || err != nil {
			c := r.sum.Checkers["L1"]
			c.Violations = append(c.Violations, vs...)
			c.OK = false
			c.Note = "second quiescence after L2 re-sends failed"
			if err != nil {
				c.Error = err.Error()
			}
			r.sum.Checkers["L1"] = c
		}
	}
	r.phase("l2", since)

	// Final reads.
	since = time.Now()
	if err := r.wl.finalReads(ctx, r); err != nil {
		r.fail("final reads: " + err.Error())
	}
	r.phase("final_reads", since)

	// History, logs, checkers.
	since = time.Now()
	ops := r.rec.Ops()
	if err := r.writeHistory(ops); err != nil {
		r.fail("write history: " + err.Error())
	}
	r.mu.Lock()
	r.sum.Ops = map[string]int{"invoke": 0, "ok": r.counts["ok"], "fail": r.counts["fail"], "info": r.counts["info"]}
	r.mu.Unlock()
	for _, op := range ops {
		if op.Type == history.TypeInvoke {
			r.sum.Ops["invoke"]++
		}
	}
	events := nem.all()
	r.sum.Nemeses, r.sum.NemesisTotal = nemesisCounts(events)
	logs := r.collectLogs(ctx)
	for k, v := range offlineChecks(r.wl, ops, events, logs, filepath.Join(r.dir, "porcupine.html"), r.cfg.CheckTimeout) {
		r.sum.Checkers[k] = v
	}
	r.sum.Goroutines = ctl.goroutineCounts(ctx)
	r.sum.RelayReplayed = ctl.relayReplayed(ctx)
	for _, lines := range logs {
		for _, l := range lines {
			if strings.Contains(l, "stream data loss detected") {
				r.sum.RelayReplayedLog++
			}
		}
	}
	r.runInvariants(ctx, env, ops)
	dbStarted := time.Now()
	dbv, err := r.wl.checkDB(ctx, r, ops)
	r.sum.Checkers["workload_db"] = newCheck(dbStarted, dbv, err)
	r.phase("checks", since)

	if err := r.writeSummary(); err != nil {
		return err
	}
	r.log.Info("chaos run finished", "pass", r.sum.Pass, "ops", fmt.Sprint(r.sum.Ops), "nemeses", r.sum.NemesisTotal, "dir", r.dir)
	return nil
}

// mayhem waits out the configured duration, logging progress and, in soak
// mode, running the periodic invariant checks.
func (r *run) mayhem(ctx context.Context, since time.Time) {
	end := since.Add(r.cfg.Duration)
	progress := time.NewTicker(15 * time.Second)
	defer progress.Stop()
	var soak <-chan time.Time
	if r.cfg.Soak {
		t := time.NewTicker(r.cfg.SoakCheckEvery)
		defer t.Stop()
		soak = t.C
	}
	for {
		remaining := time.Until(end)
		if remaining <= 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(remaining):
			return
		case <-progress.C:
			r.mu.Lock()
			counts := fmt.Sprint(r.counts)
			r.mu.Unlock()
			r.log.Info("mayhem progress", "remaining", remaining.Round(time.Second).String(), "ops", counts, "nemeses", len(r.nem.all())/2)
		case <-soak:
			r.soakCheck(ctx)
		}
	}
}

// soakCheck runs the invariants that must hold at every instant (I4, I6,
// fencing from the apply log, bank conservation) against the database while
// mayhem continues (spec 11.7).
func (r *run) soakCheck(ctx context.Context) {
	env := r.ctl.env(r.cfg.RecoveryBound)
	var vs []history.Violation
	for _, check := range []func(context.Context, invariants.Env) ([]history.Violation, error){invariants.I4, invariants.I6, invariants.Fencing} {
		v, err := check(ctx, env)
		if err != nil {
			r.log.Warn("soak check error (stores may be faulted)", "error", err)
			continue
		}
		vs = append(vs, v...)
	}
	if _, isBank := r.wl.(*bankWL); isBank {
		var sum int64
		if err := r.ctl.pool.QueryRow(ctx, `SELECT coalesce(sum(balance), 0) FROM wl_bank`).Scan(&sum); err == nil && sum != workload.BankTotal() {
			vs = append(vs, history.Violation{ID: history.IDConservation, Msg: fmt.Sprintf("bank sums to %d, want %d", sum, workload.BankTotal())})
		}
	}
	r.mu.Lock()
	r.sum.Soak = append(r.sum.Soak, vs...)
	r.mu.Unlock()
	r.log.Info("soak check", "violations", len(vs))
}

// resolveInfos re-sends every retired info operation of a keyed command
// after healing and requires a definite answer (L2). It reports whether
// anything was re-sent.
func (r *run) resolveInfos(ctx context.Context) bool {
	started := time.Now()
	infos := r.takeInfos()
	l2 := &l2Summary{Unresolved: []map[string]any{}}
	var vs []history.Violation
	nodes := r.ctl.nodes
	for _, pi := range infos {
		l2.Resent++
		var res result
		for attempt := 0; attempt < 3*len(nodes); attempt++ {
			res = nodes[attempt%len(nodes)].call(ctx, pi.Op.Req)
			if res.Outcome != outcomeInfo {
				break
			}
			if !sleepCtx(ctx, time.Second) {
				break
			}
		}
		switch res.Outcome {
		case outcomeOK:
			l2.ResolvedOK++
		case outcomeFail:
			l2.ResolvedFail++
		default:
			d := map[string]any{"cmd": pi.Op.CmdID, "f": pi.Op.F, "key": pi.Op.Key, "process": pi.Process, "original_error": pi.Err, "error": res.Err}
			l2.Unresolved = append(l2.Unresolved, d)
			if len(vs) < 20 {
				vs = append(vs, history.Violation{ID: "L2", Msg: "retired info operation did not resolve when re-sent after healing", Details: d})
			}
		}
	}
	r.sum.L2 = l2
	c := newCheck(started, vs, nil)
	c.Note = fmt.Sprintf("resent %d: ok %d, fail %d, unresolved %d", l2.Resent, l2.ResolvedOK, l2.ResolvedFail, len(l2.Unresolved))
	r.sum.Checkers["L2"] = c
	r.log.Info("L2", "note", c.Note)
	return l2.Resent > 0
}

// runInvariants runs I1 to I6, the apply-log fencing check, and DLQEmpty
// against the stores (L1 ran already) and files them by ID.
func (r *run) runInvariants(ctx context.Context, env invariants.Env, ops []history.Op) {
	started := time.Now()
	r.mu.Lock()
	expects := make(map[string]invariants.Expect, len(r.expects))
	for k, v := range r.expects {
		expects[k] = v
	}
	responses := make(map[string][][]byte, len(r.responses))
	for k, v := range r.responses {
		responses[k] = v
	}
	r.mu.Unlock()
	opts := invariants.Options{Expect: expects, Responses: responses, SkipLiveness: true}
	rep, err := invariants.All(ctx, env, opts)
	wanted := []string{invariants.IDI1, invariants.IDI2, invariants.IDI3, invariants.IDI4, invariants.IDI5, invariants.IDI6, invariants.IDFencing, invariants.IDDLQ}
	for k, v := range groupByID(rep.Violations, wanted, started, err) {
		if k == invariants.IDFencing {
			k = "fencing_db"
		}
		r.sum.Checkers[k] = v
	}
	if c, ok := r.sum.Checkers[invariants.IDI1]; ok {
		c.Note = fmt.Sprintf("%d commands checked", len(expects))
		r.sum.Checkers[invariants.IDI1] = c
	}
	if c, ok := r.sum.Checkers[invariants.IDI4]; ok && len(responses) > 0 {
		c.Note = fmt.Sprintf("%d re-sent commands compared byte for byte", len(responses))
		r.sum.Checkers[invariants.IDI4] = c
	}
	_ = ops
}

func (r *run) writeHistory(ops []history.Op) error {
	f, err := os.Create(filepath.Join(r.dir, "history.jsonl"))
	if err != nil {
		return err
	}
	if err := history.WriteJSONL(f, ops); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// collectLogs saves the logs of every container to the run directory and
// returns the node logs as lines for the checkers.
func (r *run) collectLogs(ctx context.Context) map[string][]string {
	out := map[string][]string{}
	for _, n := range r.ctl.nodes {
		raw, err := r.ctl.docker.logs(ctx, n.Container, r.started)
		if err != nil {
			r.log.Warn("docker logs failed", "node", n.ID, "error", err)
		}
		_ = os.WriteFile(filepath.Join(r.dir, n.ID+".log"), []byte(raw), 0o600)
		out[n.ID] = splitLines(raw)
	}
	for _, svc := range []string{"postgres", "redis", "toxiproxy"} {
		raw, _ := r.ctl.docker.logs(ctx, r.ctl.container(svc), r.started)
		_ = os.WriteFile(filepath.Join(r.dir, svc+".log"), []byte(raw), 0o600)
	}
	return out
}

func (r *run) writeSummary() error {
	r.sum.Finished = time.Now().Format(time.RFC3339)
	r.sum.Failures = r.sum.failures()
	r.sum.Pass = len(r.sum.Failures) == 0
	b, err := json.Marshal(r.sum, json.Deterministic(true))
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(r.dir, "summary.json"), b, 0o600)
}
