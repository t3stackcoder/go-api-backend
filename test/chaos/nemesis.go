//go:build chaos

package chaos

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
)

// nemesisEvent is one line of nemesis.jsonl: the start or end of a
// nemesis with its parameters. Time is nanoseconds from the run start on
// the controller clock, like history times, so windows line up with ops.
type nemesisEvent struct {
	ID        int            `json:"id"`
	Name      string         `json:"name"`
	Type      string         `json:"type"` // start, end, error
	Time      int64          `json:"time"`
	Params    map[string]any `json:"params,omitempty"`
	ElapsedMS int64          `json:"elapsed_ms,omitempty"`
	Error     string         `json:"error,omitempty"`
}

// nemesisLog appends events to nemesis.jsonl as they happen and keeps them
// in memory for the checkers.
type nemesisLog struct {
	mu     sync.Mutex
	f      *os.File
	w      *bufio.Writer
	events []nemesisEvent
}

func newNemesisLog(path string) (*nemesisLog, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &nemesisLog{f: f, w: bufio.NewWriter(f)}, nil
}

func (l *nemesisLog) add(ev nemesisEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, ev)
	if l.w == nil {
		return
	}
	if b, err := json.Marshal(ev, json.Deterministic(true)); err == nil {
		_, _ = l.w.Write(b)
		_ = l.w.WriteByte('\n')
		_ = l.w.Flush()
	}
}

func (l *nemesisLog) all() []nemesisEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]nemesisEvent, len(l.events))
	copy(out, l.events)
	return out
}

func (l *nemesisLog) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w != nil {
		_ = l.w.Flush()
		_ = l.f.Close()
		l.w = nil
	}
}

// readNemesisLog reads nemesis.jsonl (TestReplay).
func readNemesisLog(path string) ([]nemesisEvent, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []nemesisEvent
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var ev nemesisEvent
		if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
			return out, err
		}
		out = append(out, ev)
	}
	return out, sc.Err()
}

// params are the drawn parameters of one nemesis instance. apply may add
// observations (exit codes) that end up in the end event.
type params map[string]any

// nemesis is one row of the nemeses table of spec 11.6.
type nemesis struct {
	name string
	// pick draws the parameters from the scheduler's seeded generator.
	pick func(r *run, rng *rand.Rand) params
	// resources are the exclusive locks the nemesis holds while active, so
	// two nemeses never fight over one container or service.
	resources func(p params) []string
	// apply starts the fault and returns how to end it (nil when nothing to
	// undo). The undo runs after the nemesis duration, or at once when the
	// mayhem phase ends.
	apply func(ctx context.Context, r *run, p params) (undo func(ctx context.Context) error, err error)
}

// nemesisNames lists the fifteen nemeses in the spec's order.
var nemesisNames = []string{
	"partition-pg", "partition-redis", "latency", "reset", "bandwidth", "pause", "kill", "graceful-restart",
	"pg-restart", "redis-restart", "redis-flush", "redis-restore-old", "clock-skew", "handler-rotate", "relay-kill",
}

func pickNode(r *run, rng *rand.Rand) int { return rng.IntN(len(r.ctl.nodes)) }

func pickScope(r *run, rng *rand.Rand, p params) {
	if rng.IntN(2) == 0 {
		p["scope"] = "one"
	} else {
		p["scope"] = "all"
	}
	p["node"] = r.ctl.nodes[pickNode(r, rng)].ID
}

// targetNodes resolves the scope of a toxic nemesis to nodes.
func targetNodes(r *run, p params) []*node {
	if p["scope"] == "all" {
		return r.ctl.nodes
	}
	for _, n := range r.ctl.nodes {
		if n.ID == p["node"] {
			return []*node{n}
		}
	}
	return nil
}

func proxyOf(n *node, service string) string {
	if service == "redis" {
		return n.RedisProxy
	}
	return n.PGProxy
}

// toxicNemesis builds a nemesis that adds one toxic per target node and
// stream and removes them at the end.
func toxicNemesis(name string, pick func(r *run, rng *rand.Rand) params, streams func(p params) []string, typ func(p params) string, attrs func(p params) map[string]any) nemesis {
	return nemesis{
		name:      name,
		pick:      pick,
		resources: func(params) []string { return nil },
		apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
			service, _ := p["service"].(string)
			id := p["id"]
			type added struct{ proxy, name string }
			var toxics []added
			var errs []error
			for _, n := range targetNodes(r, p) {
				for _, stream := range streams(p) {
					tn := fmt.Sprintf("n%v-%s", id, stream)
					proxy := proxyOf(n, service)
					if err := r.ctl.toxi.addToxic(ctx, proxy, tn, typ(p), stream, attrs(p)); err != nil {
						errs = append(errs, err)
						continue
					}
					toxics = append(toxics, added{proxy, tn})
				}
			}
			undo := func(ctx context.Context) error {
				var errs []error
				for _, t := range toxics {
					errs = append(errs, r.ctl.toxi.removeToxic(ctx, t.proxy, t.name))
				}
				return errors.Join(errs...)
			}
			return undo, errors.Join(errs...)
		},
	}
}

func partitionNemesis(name, service string) nemesis {
	return toxicNemesis(name,
		func(r *run, rng *rand.Rand) params {
			p := params{"service": service}
			pickScope(r, rng, p)
			p["direction"] = []string{"upstream", "downstream", "both"}[rng.IntN(3)]
			return p
		},
		func(p params) []string {
			if p["direction"] == "both" {
				return []string{"upstream", "downstream"}
			}
			return []string{p["direction"].(string)}
		},
		func(params) string { return "timeout" },
		func(params) map[string]any { return map[string]any{"timeout": 0} },
	)
}

func pickService(rng *rand.Rand) string {
	if rng.IntN(2) == 0 {
		return "pg"
	}
	return "redis"
}

func oneStream(p params) []string { return []string{p["stream"].(string)} }

// nemeses builds the fifteen nemeses for the run.
func nemeses(r *run) []nemesis {
	nodeLock := func(p params) []string { return []string{"node:" + p["node"].(string)} }
	restoreUndo := func(id string) func(context.Context) error {
		return func(ctx context.Context) error {
			for _, n := range r.ctl.nodes {
				if n.ID == id {
					return r.ctl.restoreNode(ctx, n)
				}
			}
			return nil
		}
	}
	nodeByID := func(id string) *node {
		for _, n := range r.ctl.nodes {
			if n.ID == id {
				return n
			}
		}
		return nil
	}
	return []nemesis{
		partitionNemesis("partition-pg", "pg"),
		partitionNemesis("partition-redis", "redis"),
		toxicNemesis("latency",
			func(r *run, rng *rand.Rand) params {
				p := params{"service": pickService(rng)}
				pickScope(r, rng, p)
				p["stream"] = []string{"upstream", "downstream"}[rng.IntN(2)]
				p["latency_ms"] = 50 + rng.IntN(951)
				p["jitter_ms"] = 50 + rng.IntN(1951)
				return p
			},
			oneStream,
			func(params) string { return "latency" },
			func(p params) map[string]any {
				return map[string]any{"latency": p["latency_ms"], "jitter": p["jitter_ms"]}
			},
		),
		toxicNemesis("reset",
			func(r *run, rng *rand.Rand) params {
				p := params{"service": pickService(rng), "stream": "downstream"}
				pickScope(r, rng, p)
				if rng.IntN(2) == 0 {
					p["kind"] = "reset_peer"
					p["timeout_ms"] = rng.IntN(2001)
				} else {
					p["kind"] = "slow_close"
					p["timeout_ms"] = 100 + rng.IntN(2901)
				}
				return p
			},
			oneStream,
			func(p params) string { return p["kind"].(string) },
			func(p params) map[string]any {
				if p["kind"] == "slow_close" {
					return map[string]any{"delay": p["timeout_ms"]}
				}
				return map[string]any{"timeout": p["timeout_ms"]}
			},
		),
		toxicNemesis("bandwidth",
			func(r *run, rng *rand.Rand) params {
				p := params{"service": pickService(rng), "rate_kbps": 8}
				pickScope(r, rng, p)
				p["stream"] = []string{"upstream", "downstream"}[rng.IntN(2)]
				return p
			},
			oneStream,
			func(params) string { return "bandwidth" },
			func(p params) map[string]any { return map[string]any{"rate": p["rate_kbps"]} },
		),
		{
			name:      "pause",
			pick:      func(r *run, rng *rand.Rand) params { return params{"node": r.ctl.nodes[pickNode(r, rng)].ID} },
			resources: nodeLock,
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				n := nodeByID(p["node"].(string))
				if err := r.ctl.docker.pause(ctx, n.Container); err != nil {
					return nil, err
				}
				return func(ctx context.Context) error { return r.ctl.docker.unpause(ctx, n.Container) }, nil
			},
		},
		{
			name:      "kill",
			pick:      func(r *run, rng *rand.Rand) params { return params{"node": r.ctl.nodes[pickNode(r, rng)].ID} },
			resources: nodeLock,
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				n := nodeByID(p["node"].(string))
				if err := r.ctl.docker.kill(ctx, n.Container, "KILL"); err != nil {
					return nil, err
				}
				return restoreUndo(n.ID), nil
			},
		},
		{
			name:      "graceful-restart",
			pick:      func(r *run, rng *rand.Rand) params { return params{"node": r.ctl.nodes[pickNode(r, rng)].ID} },
			resources: nodeLock,
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				n := nodeByID(p["node"].(string))
				started := time.Now()
				if err := r.ctl.docker.kill(ctx, n.Container, "TERM"); err != nil {
					return nil, err
				}
				code, exited, err := r.ctl.docker.wait(ctx, n.Container, r.cfg.DrainBound)
				p["exit_ms"] = time.Since(started).Milliseconds()
				p["exited"] = exited
				p["exit_code"] = code
				if err != nil {
					p["wait_error"] = err.Error()
				}
				if !exited {
					r.log.Warn("graceful-restart: node did not exit within the drain bound; killing", "node", n.ID)
					_ = r.ctl.docker.kill(ctx, n.Container, "KILL")
				}
				return restoreUndo(n.ID), nil
			},
		},
		{
			name:      "pg-restart",
			pick:      func(*run, *rand.Rand) params { return params{} },
			resources: func(params) []string { return []string{"postgres"} },
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				if err := r.ctl.docker.restart(ctx, r.ctl.container("postgres"), 10*time.Second); err != nil {
					return nil, err
				}
				return nil, r.ctl.waitPG(ctx, 60*time.Second)
			},
		},
		{
			name:      "redis-restart",
			pick:      func(*run, *rand.Rand) params { return params{} },
			resources: func(params) []string { return []string{"redis"} },
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				if err := r.ctl.docker.restart(ctx, r.ctl.container("redis"), 5*time.Second); err != nil {
					return nil, err
				}
				return nil, r.ctl.waitRedis(ctx, 60*time.Second)
			},
		},
		{
			name:      "redis-flush",
			pick:      func(*run, *rand.Rand) params { return params{} },
			resources: func(params) []string { return []string{"redis"} },
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				_, err := r.ctl.docker.exec(ctx, r.ctl.container("redis"), "redis-cli", "FLUSHALL")
				return nil, err
			},
		},
		{
			name:      "redis-restore-old",
			pick:      func(*run, *rand.Rand) params { return params{} },
			resources: func(params) []string { return []string{"redis"} },
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				if !r.ctl.hasSnapshot() {
					p["noop"] = true
					return nil, nil
				}
				return nil, r.ctl.restoreRedis(ctx)
			},
		},
		{
			name: "clock-skew",
			pick: func(r *run, rng *rand.Rand) params {
				return params{"node": r.ctl.nodes[pickNode(r, rng)].ID, "offset_s": rng.IntN(121) - 60}
			},
			resources: func(p params) []string { return []string{"clock:" + p["node"].(string)} },
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				n := nodeByID(p["node"].(string))
				offset := time.Duration(p["offset_s"].(int)) * time.Second
				if err := n.setClock(ctx, offset); err != nil {
					return nil, err
				}
				return func(ctx context.Context) error { return n.setClock(ctx, 0) }, nil
			},
		},
		{
			name: "handler-rotate",
			pick: func(r *run, rng *rand.Rand) params {
				// The next serving node is drawn among the others.
				cur := r.ctl.servingNode()
				next := rng.IntN(len(r.ctl.nodes) - 1)
				if next >= cur {
					next++
				}
				if len(r.ctl.nodes) == 1 {
					next = 0
				}
				return params{"to": r.ctl.nodes[next].ID, "to_index": next}
			},
			resources: func(params) []string { return []string{"handlers"} },
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				if !r.wl.remote() {
					p["noop"] = true
					return nil, nil
				}
				old, cur := r.ctl.rotateServing(p["to_index"].(int))
				p["from"] = r.ctl.nodes[old].ID
				var errs []error
				// Stop the old node first, so at most one node serves at a time.
				if err := r.ctl.nodes[old].setHandlers(ctx, "remote", false); err != nil {
					errs = append(errs, err)
				}
				if err := r.ctl.nodes[cur].setHandlers(ctx, "remote", true); err != nil {
					errs = append(errs, err)
				}
				return nil, errors.Join(errs...)
			},
		},
		{
			name:      "relay-kill",
			pick:      func(*run, *rand.Rand) params { return params{} },
			resources: func(params) []string { return []string{"relay"} },
			apply: func(ctx context.Context, r *run, p params) (func(context.Context) error, error) {
				var errs []error
				for _, n := range r.ctl.nodes {
					errs = append(errs, n.setRelay(ctx, false))
				}
				undo := func(ctx context.Context) error {
					var errs []error
					for _, n := range r.ctl.nodes {
						errs = append(errs, n.setRelay(ctx, true))
					}
					return errors.Join(errs...)
				}
				return undo, errors.Join(errs...)
			},
		},
	}
}

// maxActiveNemeses is the spec's cap on concurrently active nemeses.
const maxActiveNemeses = 2

// runNemeses is the mayhem scheduler: a nemesis every 5 to 15 s (scaled by
// the configured mean interval), at most two active, each with a random 2
// to 20 s duration, all drawn from the seeded generator. It returns when
// ctx ends and every active nemesis has been undone.
func runNemeses(ctx context.Context, r *run) {
	rng := r.nemRNG
	all := nemeses(r)
	var mu sync.Mutex
	active := 0
	busy := map[string]bool{}
	var wg sync.WaitGroup
	id := 0
	for {
		wait := time.Duration(float64(r.cfg.NemesisInterval) * (0.5 + rng.Float64()))
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-time.After(wait):
		}
		for try := 0; try < 4; try++ {
			nm := all[rng.IntN(len(all))]
			p := nm.pick(r, rng)
			dur := time.Duration(2000+rng.IntN(18001)) * time.Millisecond
			res := nm.resources(p)
			mu.Lock()
			if active >= maxActiveNemeses {
				mu.Unlock()
				break
			}
			conflict := false
			for _, x := range res {
				if busy[x] {
					conflict = true
				}
			}
			if conflict {
				mu.Unlock()
				continue
			}
			for _, x := range res {
				busy[x] = true
			}
			active++
			id++
			mu.Unlock()
			wg.Add(1)
			go func(id int, nm nemesis, p params, dur time.Duration) {
				defer wg.Done()
				r.runNemesis(ctx, id, nm, p, dur)
				mu.Lock()
				for _, x := range res {
					delete(busy, x)
				}
				active--
				mu.Unlock()
			}(id, nm, p, dur)
			break
		}
	}
}

// runNemesis applies one nemesis, holds it for dur (or until ctx ends),
// undoes it, and logs the start and end events.
func (r *run) runNemesis(ctx context.Context, id int, nm nemesis, p params, dur time.Duration) {
	p["id"] = id
	p["duration_ms"] = dur.Milliseconds()
	started := time.Now()
	r.nem.add(nemesisEvent{ID: id, Name: nm.name, Type: "start", Time: r.rec.Now(), Params: copyParams(p)})
	r.log.Info("nemesis start", "id", id, "name", nm.name, "params", fmt.Sprint(p))
	undo, err := nm.apply(ctx, r, p)
	if err != nil {
		r.log.Warn("nemesis apply failed", "id", id, "name", nm.name, "error", err)
		p["apply_error"] = err.Error()
	}
	if noop, _ := p["noop"].(bool); !noop && (err == nil || undo != nil) {
		select {
		case <-ctx.Done():
		case <-time.After(dur):
		}
	}
	var undoErr error
	if undo != nil {
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Minute)
		undoErr = undo(uctx)
		cancel()
		if undoErr != nil {
			r.log.Warn("nemesis undo failed", "id", id, "name", nm.name, "error", undoErr)
			p["undo_error"] = undoErr.Error()
		}
	}
	ev := nemesisEvent{ID: id, Name: nm.name, Type: "end", Time: r.rec.Now(), Params: copyParams(p), ElapsedMS: time.Since(started).Milliseconds()}
	if err != nil {
		ev.Type = "error"
		ev.Error = err.Error()
	}
	r.nem.add(ev)
	r.log.Info("nemesis end", "id", id, "name", nm.name, "elapsed", time.Since(started).String())
}

func copyParams(p params) map[string]any {
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = v
	}
	return out
}

// redisAffecting reports whether a nemesis could make a cache tag bump
// fail or serve stale cache state, which opens a degraded window for the
// cache checkers (G9).
func redisAffecting(ev nemesisEvent) bool {
	if noop, _ := ev.Params["noop"].(bool); noop {
		return false
	}
	switch ev.Name {
	case "partition-redis", "redis-restart", "redis-flush", "redis-restore-old":
		return true
	case "latency", "reset", "bandwidth":
		return ev.Params["service"] == "redis"
	}
	return false
}

// windowsFrom derives the degraded windows of the cache checkers from the
// nemesis log: one window per Redis-affecting nemesis from its start event
// to its end event (or the end of the run when it never ended).
func windowsFrom(events []nemesisEvent, runEnd int64) []history.Window {
	ends := map[int]int64{}
	for _, ev := range events {
		if ev.Type == "end" || ev.Type == "error" {
			ends[ev.ID] = ev.Time
		}
	}
	var out []history.Window
	for _, ev := range events {
		if ev.Type != "start" || !redisAffecting(ev) {
			continue
		}
		end, ok := ends[ev.ID]
		if !ok {
			end = runEnd
		}
		out = append(out, history.Window{Start: ev.Time, End: end, Kind: ev.Name})
	}
	return out
}

// shutdownCheck is the shutdown checker of spec 11.6: every
// graceful-restart exited with code 0 within the drain bound (G16).
func shutdownCheck(events []nemesisEvent) []history.Violation {
	var out []history.Violation
	for _, ev := range events {
		if ev.Name != "graceful-restart" || ev.Type == "start" {
			continue
		}
		exited, _ := ev.Params["exited"].(bool)
		code, _ := history.Int(ev.Params["exit_code"])
		if ev.Type == "error" || !exited || code != 0 {
			out = append(out, history.Violation{ID: "Shutdown", Msg: "graceful restart did not exit 0 within the drain bound", Details: map[string]any{"id": ev.ID, "node": ev.Params["node"], "exited": exited, "exit_code": code, "exit_ms": ev.Params["exit_ms"], "error": ev.Error}})
		}
	}
	return out
}

// nemesisCounts counts started nemeses by name.
func nemesisCounts(events []nemesisEvent) (map[string]int, int) {
	counts := map[string]int{}
	for _, name := range nemesisNames {
		counts[name] = 0
	}
	total := 0
	for _, ev := range events {
		if ev.Type == "start" {
			counts[ev.Name]++
			total++
		}
	}
	return counts, total
}
