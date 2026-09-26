package main

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// node holds everything the admin endpoints act on.
//
// Endpoints, all under /chaos/ on the workload listener:
//
//	GET  /chaos/clock                  {"offset":"1m0s"}
//	PUT  /chaos/clock?offset=30s       shift the injectable clock (also JSON {"offset":"30s"}; seconds when unitless)
//	GET  /chaos/fault                  hit counts and observed points (faultinject builds)
//	PUT  /chaos/fault                  arm schedules from a JSON array [{"point","kind","hit","delay"}]
//	DELETE /chaos/fault                disarm
//	GET  /chaos/handlers               dispatch mode, served commands, declared commands, groups
//	PUT  /chaos/handlers?dispatch=local|remote&serve=true|false
//	GET  /chaos/remote                 whether the remote server advertises and serves
//	PUT  /chaos/remote?serve=true|false  stop or start the remote server (handler-rotate)
//	GET  /chaos/relay                  whether the relay runs, and its stats
//	PUT  /chaos/relay?run=true|false   stop or start the relay (relay-kill)
//	GET  /chaos/stats                  consumers, relay, remote, goroutines
//	GET  /chaos/goroutines             {"goroutines":n}
//
// Every other path is served by the httpapi server of the active dispatch
// mode: /rpc/<Name> workload routes, /healthz, /readyz.
type node struct {
	cfg    config
	logger *slog.Logger
	clock  *testkit.OffsetClock

	m, mp        *mediator.Mediator
	local, proxy *httpapi.Server
	remoteMode   atomic.Bool
	remoteNames  []string

	remoteServer *switchable
	relay        *switchable
	relayImpl    *pg.Relay
	rs           *redisx.RemoteServer
	consumers    *redisx.Consumers
	remote       *redisx.Remote
	groups       []string

	started time.Time
}

// handler returns the node's HTTP handler: admin routes plus the active
// workload server.
func (n *node) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/chaos/clock", n.clockHandler)
	mux.HandleFunc("/chaos/fault", n.faultHandler)
	mux.HandleFunc("/chaos/handlers", n.handlersHandler)
	mux.HandleFunc("/chaos/remote", n.remoteHandler)
	mux.HandleFunc("/chaos/relay", n.relayHandler)
	mux.HandleFunc("/chaos/stats", n.statsHandler)
	mux.HandleFunc("/chaos/goroutines", n.goroutinesHandler)
	mux.HandleFunc("/chaos/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "unknown admin endpoint " + r.URL.Path})
	})
	mux.HandleFunc("/", n.dispatchHTTP)
	return mux
}

// dispatchHTTP routes workload requests to the server of the active mode.
func (n *node) dispatchHTTP(w http.ResponseWriter, r *http.Request) {
	if n.remoteMode.Load() && n.proxy != nil {
		n.proxy.ServeHTTP(w, r)
		return
	}
	n.local.ServeHTTP(w, r)
}

func (n *node) dispatchMode() string {
	if n.remoteMode.Load() {
		return DispatchRemote
	}
	return DispatchLocal
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v, json.Deterministic(true))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{"error": err.Error()})
}

// param reads a setting from the query string first, then from a JSON
// object body.
func param(r *http.Request, name string) (string, bool) {
	if v := r.URL.Query().Get(name); v != "" {
		return v, true
	}
	if r.Body == nil {
		return "", false
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
	if err != nil || len(raw) == 0 {
		return "", false
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return "", false
	}
	if v, ok := body[name]; ok {
		return fmt.Sprint(v), true
	}
	return "", false
}

func parseOffset(raw string) (time.Duration, error) {
	if d, err := time.ParseDuration(raw); err == nil {
		return d, nil
	}
	secs, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("offset %q is neither a duration nor seconds", raw)
	}
	return time.Duration(secs * float64(time.Second)), nil
}

func (n *node) clockHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"offset": n.clock.Offset().String(), "now": n.clock.Now().Format(time.RFC3339Nano)})
	case http.MethodPut, http.MethodPost:
		raw, ok := param(r, "offset")
		if !ok {
			writeError(w, http.StatusBadRequest, fmt.Errorf("offset is required"))
			return
		}
		d, err := parseOffset(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		n.clock.SetOffset(d)
		n.logger.Warn("clock skew set", "offset", d.String())
		writeJSON(w, http.StatusOK, map[string]any{"offset": d.String()})
	default:
		w.Header().Set("Allow", "GET, PUT")
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method %s not allowed", r.Method))
	}
}

func (n *node) faultHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, faultStatus())
	case http.MethodPut, http.MethodPost:
		if !faultInjectionEnabled {
			writeError(w, http.StatusNotImplemented, errNoFaultInjectionBuild)
			return
		}
		list, err := armFaults(r.Body)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		n.logger.Warn("fault schedules armed", "count", len(list))
		writeJSON(w, http.StatusOK, map[string]any{"armed": list})
	case http.MethodDelete:
		if !faultInjectionEnabled {
			writeError(w, http.StatusNotImplemented, errNoFaultInjectionBuild)
			return
		}
		disarmFaults()
		n.logger.Warn("fault schedules disarmed")
		writeJSON(w, http.StatusOK, map[string]any{"armed": []faultSchedule{}})
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method %s not allowed", r.Method))
	}
}

var errNoFaultInjectionBuild = fmt.Errorf("chaosnode: built without the faultinject tag")

func (n *node) handlersState() map[string]any {
	return map[string]any{
		"node":     n.cfg.NodeID,
		"dispatch": n.dispatchMode(),
		"serving":  n.remoteServer.Enabled(),
		"running":  n.remoteServer.Running(),
		"served":   n.rs.Served(),
		"remote":   n.remoteNames,
		"groups":   n.groups,
		"relay":    n.relay.Enabled(),
	}
}

func (n *node) handlersHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, n.handlersState())
	case http.MethodPut, http.MethodPost:
		raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<16))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		body := map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				writeError(w, http.StatusBadRequest, fmt.Errorf("decode body: %w", err))
				return
			}
		}
		get := func(name string) (string, bool) {
			if v := r.URL.Query().Get(name); v != "" {
				return v, true
			}
			if v, ok := body[name]; ok {
				return fmt.Sprint(v), true
			}
			return "", false
		}
		if mode, ok := get("dispatch"); ok {
			switch mode {
			case DispatchLocal:
				n.remoteMode.Store(false)
			case DispatchRemote:
				if n.proxy == nil {
					writeError(w, http.StatusBadRequest, fmt.Errorf("remote dispatch is not configured on this node"))
					return
				}
				n.remoteMode.Store(true)
			default:
				writeError(w, http.StatusBadRequest, fmt.Errorf("dispatch must be %q or %q", DispatchLocal, DispatchRemote))
				return
			}
			n.logger.Warn("dispatch mode set", "dispatch", mode)
		}
		if raw, ok := get("serve"); ok {
			on, err := strconv.ParseBool(raw)
			if err != nil {
				writeError(w, http.StatusBadRequest, fmt.Errorf("serve: %w", err))
				return
			}
			n.remoteServer.Set(on)
			n.logger.Warn("remote server switched", "serve", on)
		}
		writeJSON(w, http.StatusOK, n.handlersState())
	default:
		w.Header().Set("Allow", "GET, PUT")
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method %s not allowed", r.Method))
	}
}

func (n *node) remoteHandler(w http.ResponseWriter, r *http.Request) {
	state := func() map[string]any {
		return map[string]any{
			"node": n.cfg.NodeID, "serve": n.remoteServer.Enabled(), "running": n.remoteServer.Running(),
			"starts": n.remoteServer.Starts(), "served": n.rs.Served(), "waiting": n.remote.Waiting(),
		}
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, state())
	case http.MethodPut, http.MethodPost:
		raw, ok := param(r, "serve")
		if !ok {
			writeError(w, http.StatusBadRequest, fmt.Errorf("serve is required"))
			return
		}
		on, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("serve: %w", err))
			return
		}
		n.remoteServer.Set(on)
		n.logger.Warn("remote server switched", "serve", on)
		// Give the switch a moment so callers can observe the new state.
		waitState(n.remoteServer, on, 2*time.Second)
		writeJSON(w, http.StatusOK, state())
	default:
		w.Header().Set("Allow", "GET, PUT")
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method %s not allowed", r.Method))
	}
}

func (n *node) relayHandler(w http.ResponseWriter, r *http.Request) {
	state := func() map[string]any {
		return map[string]any{
			"node": n.cfg.NodeID, "run": n.relay.Enabled(), "running": n.relay.Running(),
			"starts": n.relay.Starts(), "stats": relayStatsJSON(n.relayImpl.Stats()),
		}
	}
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, state())
	case http.MethodPut, http.MethodPost:
		raw, ok := param(r, "run")
		if !ok {
			writeError(w, http.StatusBadRequest, fmt.Errorf("run is required"))
			return
		}
		on, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("run: %w", err))
			return
		}
		n.relay.Set(on)
		n.logger.Warn("relay switched", "run", on)
		waitState(n.relay, on, 2*time.Second)
		writeJSON(w, http.StatusOK, state())
	default:
		w.Header().Set("Allow", "GET, PUT")
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method %s not allowed", r.Method))
	}
}

// waitState waits until the switchable reports the requested running state
// or the timeout passes.
func waitState(s *switchable, running bool, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for s.Running() != running && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
}

func relayStatsJSON(st pg.RelayStats) map[string]any {
	owned := 0
	var unpublished int64
	var lastErr string
	for _, s := range st.Slots {
		if s.Owned {
			owned++
			unpublished += s.Unpublished
		}
		if s.LastError != "" {
			lastErr = s.LastError
		}
	}
	return map[string]any{
		"published": st.Published, "replayed": st.Replayed, "errors": st.Errors, "listening": st.Listening,
		"slots": len(st.Slots), "owned": owned, "unpublished_owned": unpublished, "last_error": lastErr,
	}
}

func consumerStatsJSON(st redisx.Stats) map[string]any {
	owned := map[string]int{}
	for k, v := range st.Owned {
		owned[k.Group+"/"+k.Topic] = v
	}
	handovers := map[string]int64{}
	for k, v := range st.Handovers {
		handovers[k.Group+"/"+k.Topic] = v
	}
	processed := map[string]int64{}
	for k, v := range st.Processed {
		processed[fmt.Sprintf("%v", k)] = v
	}
	parts := make([]map[string]any, 0, len(st.Partitions))
	for _, p := range st.Partitions {
		parts = append(parts, map[string]any{
			"group": p.Group, "topic": p.Topic, "partition": p.Partition, "epoch": p.Epoch,
			"pending": p.Pending, "oldest_age": p.OldestAge.String(), "halted": p.Halted,
		})
	}
	return map[string]any{
		"node": st.NodeID, "running": st.Running, "owned": owned, "handovers": handovers,
		"processed": processed, "dlq": st.DLQSize, "halted": st.Halted, "partitions": parts,
	}
}

func (n *node) statsJSON() map[string]any {
	out := map[string]any{
		"node":       n.cfg.NodeID,
		"uptime":     time.Since(n.started).String(),
		"dispatch":   n.dispatchMode(),
		"goroutines": runtime.NumGoroutine(),
		"clock":      map[string]any{"offset": n.clock.Offset().String()},
		"relay":      map[string]any{"run": n.relay.Enabled(), "running": n.relay.Running(), "stats": relayStatsJSON(n.relayImpl.Stats())},
		"remote":     map[string]any{"serve": n.remoteServer.Enabled(), "running": n.remoteServer.Running(), "served": n.rs.Served(), "waiting": n.remote.Waiting()},
	}
	if n.consumers != nil {
		out["consumers"] = consumerStatsJSON(n.consumers.Stats())
	}
	return out
}

func (n *node) statsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method %s not allowed", r.Method))
		return
	}
	writeJSON(w, http.StatusOK, n.statsJSON())
}

func (n *node) goroutinesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		writeError(w, http.StatusMethodNotAllowed, fmt.Errorf("method %s not allowed", r.Method))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"goroutines": runtime.NumGoroutine()})
}

// goroutineMarker logs "goroutines=<n>" every interval so the chaos log
// scan can compare the final count with the baseline (spec 11.6).
func goroutineMarker(logger *slog.Logger, interval time.Duration) mediator.Component {
	return mediator.ComponentFunc(func(ctx context.Context) error {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-t.C:
				logger.Info("goroutine count", "goroutines", runtime.NumGoroutine())
			}
		}
	})
}

// describeGroups renders the groups for logs.
func describeGroups(groups []string) string {
	if len(groups) == 0 {
		return "none"
	}
	return strings.Join(groups, ",")
}
