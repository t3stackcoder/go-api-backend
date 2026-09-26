// Package httpapi adapts a built mediator to HTTP: every registered request
// becomes one operation on a net/http.ServeMux (RPC default or a REST
// override through the Route trait), request fields are bound from the JSON
// body, path, query string, and headers, errors are RFC 9457 problem
// details, stream requests are served as Server-Sent Events, and health
// endpoints and the OpenAPI document are mounted beside the operations.
//
// The adapter never reaches into handlers: it decodes, binds, builds the
// request context (correlation ID, trace context, idempotency key,
// principal, remote address), calls Send or Stream, and encodes the result.
package httpapi

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
)

// Config configures a Server. The zero value is usable; see the defaults on
// each field.
type Config struct {
	// Prefix is mounted before every path, for example "/api". Default "".
	Prefix string
	// MaxBodyBytes bounds the request body. Default 1 MiB.
	MaxBodyBytes int64
	// AllowUnknownFields accepts JSON members that no field declares.
	// Default false: unknown members are validation errors.
	AllowUnknownFields bool
	// Authenticator derives the principal of a request. A nil Authenticator
	// leaves every request anonymous. An error is CodeUnauthorized.
	Authenticator func(*http.Request) (authz.Principal, error)
	// Logger receives internal errors and server diagnostics. Default
	// slog.Default().
	Logger *slog.Logger
	// Docs, when set, is served at {Prefix}/docs and {Prefix}/openapi.json.
	// The openapi package provides it.
	Docs http.Handler
	// ReadyChecks run on GET /readyz, each bounded by 2 s; any failure is 503.
	ReadyChecks []func(context.Context) error
	// ReadyMaxLag is the consumer lag above which readiness fails; the
	// transport package builds the corresponding ReadyCheck from it.
	// Default 60 s.
	ReadyMaxLag time.Duration
	// KeepAlive is the SSE heartbeat interval. Default 15 s.
	KeepAlive time.Duration
}

const (
	defaultMaxBodyBytes = 1 << 20
	defaultKeepAlive    = 15 * time.Second
	defaultReadyMaxLag  = 60 * time.Second
	readyCheckTimeout   = 2 * time.Second
)

func (c Config) withDefaults() Config {
	if c.MaxBodyBytes <= 0 {
		c.MaxBodyBytes = defaultMaxBodyBytes
	}
	if c.KeepAlive <= 0 {
		c.KeepAlive = defaultKeepAlive
	}
	if c.ReadyMaxLag <= 0 {
		c.ReadyMaxLag = defaultReadyMaxLag
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	c.Prefix = strings.TrimRight(c.Prefix, "/")
	if c.Prefix != "" && !strings.HasPrefix(c.Prefix, "/") {
		c.Prefix = "/" + c.Prefix
	}
	return c
}

// Server routes HTTP requests to a built mediator. It is an http.Handler;
// Handler returns it for mounting, and Listener serves it.
type Server struct {
	m      *mediator.Mediator
	cfg    Config
	logger *slog.Logger
	mux    *http.ServeMux
	routes []*route
	infos  []RouteInfo
}

// New builds the routing table of every registered request under the
// prefix, validates it exactly as BuildCheck does, and mounts the health
// endpoints and Docs. The mediator must be built.
func New(m *mediator.Mediator, cfg Config) (*Server, error) {
	if !m.Built() {
		return nil, errors.New("httpapi: mediator is not built")
	}
	cfg = cfg.withDefaults()
	routes, err := planRoutes(m, cfg.Prefix)
	if err != nil {
		return nil, err
	}
	s := &Server{m: m, cfg: cfg, logger: cfg.Logger, mux: http.NewServeMux(), routes: routes}
	for _, rt := range routes {
		s.mux.Handle(rt.pattern, s.handler(rt))
		s.infos = append(s.infos, RouteInfo{Method: rt.method, Path: rt.path, Info: rt.info, Bindings: slices.Clone(rt.bindings), Status: rt.status})
	}
	slices.SortFunc(s.infos, func(a, b RouteInfo) int {
		if c := strings.Compare(a.Path, b.Path); c != 0 {
			return c
		}
		return strings.Compare(a.Method, b.Method)
	})
	s.mux.HandleFunc("GET "+cfg.Prefix+"/healthz", s.healthz)
	s.mux.HandleFunc("GET "+cfg.Prefix+"/readyz", s.readyz)
	if cfg.Docs != nil {
		s.mux.Handle("GET "+cfg.Prefix+"/docs", cfg.Docs)
		s.mux.Handle("GET "+cfg.Prefix+"/openapi.json", cfg.Docs)
	}
	return s, nil
}

// Handler returns the server as an http.Handler.
func (s *Server) Handler() http.Handler { return s }

// Config returns the effective configuration with defaults applied.
func (s *Server) Config() Config { return s.cfg }

// Routes lists every operation sorted by path then method. The openapi
// package generates the document from it.
func (s *Server) Routes() []RouteInfo { return slices.Clone(s.infos) }

// ServeHTTP sets X-Correlation-ID on every response, dispatches through the
// ServeMux, and renders the mux's own 404 and 405 responses as problem
// details, keeping the Allow header of a 405.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	corr := ensureCorrelation(w, r)
	iw := &interceptor{ResponseWriter: w}
	s.mux.ServeHTTP(iw, r)
	if iw.intercepted == 0 {
		return
	}
	if iw.intercepted == http.StatusMethodNotAllowed {
		writeProblem(w, methodNotAllowed(r, w.Header().Get("Allow"), corr), 0)
		return
	}
	err := mediator.E(mediator.CodeNotFound, fmt.Sprintf("no route for %s %s", r.Method, r.URL.Path))
	writeProblem(w, ProblemOf(err, r.URL.Path, corr), 0)
}

// interceptor wraps the ResponseWriter for one request. Until a handler of
// this package claims the response with markRouted, a 404 or 405 status is
// held back and rendered as a problem by ServeHTTP after the mux returns.
type interceptor struct {
	http.ResponseWriter
	routed      bool
	intercepted int
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (i *interceptor) Unwrap() http.ResponseWriter { return i.ResponseWriter }

func (i *interceptor) WriteHeader(code int) {
	if !i.routed && i.intercepted == 0 && (code == http.StatusNotFound || code == http.StatusMethodNotAllowed) {
		i.intercepted = code
		return
	}
	i.ResponseWriter.WriteHeader(code)
}

func (i *interceptor) Write(b []byte) (int, error) {
	if i.intercepted != 0 {
		return len(b), nil
	}
	return i.ResponseWriter.Write(b)
}

// FlushError forwards to the underlying writer so both
// http.ResponseController and legacy http.Flusher assertions work.
func (i *interceptor) FlushError() error {
	return http.NewResponseController(i.ResponseWriter).Flush()
}

// Flush implements http.Flusher.
func (i *interceptor) Flush() { _ = i.FlushError() }

// markRouted tells the interceptor that a handler of this package owns the
// response, so its statuses pass through unchanged.
func markRouted(w http.ResponseWriter) {
	if iw, ok := w.(*interceptor); ok {
		iw.routed = true
	}
}

// HealthHandler returns a handler serving /healthz and /readyz without the
// prefix, for mounting on a separate management port. The same endpoints
// are mounted under the prefix by New.
func (s *Server) HealthHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	return mux
}

// healthz is 200 while the process is alive.
func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	markRouted(w)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz runs every ReadyCheck concurrently, each bounded by
// readyCheckTimeout, and answers 503 with the failures when any fails.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	markRouted(w)
	failures := runReadyChecks(r.Context(), s.cfg.ReadyChecks)
	if len(failures) == 0 {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}
	err := mediator.E(mediator.CodeUnavailable, fmt.Sprintf("%d of %d readiness checks failed", len(failures), len(s.cfg.ReadyChecks))).
		WithDetail("failures", failures)
	WriteProblem(w, r, err)
}

func runReadyChecks(ctx context.Context, checks []func(context.Context) error) []string {
	results := make([]error, len(checks))
	var wg sync.WaitGroup
	for i, check := range checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, readyCheckTimeout)
			defer cancel()
			results[i] = check(cctx)
		}()
	}
	wg.Wait()
	var failures []string
	for i, err := range results {
		if err != nil {
			failures = append(failures, fmt.Sprintf("check %d: %v", i, err))
		}
	}
	return failures
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, _ := json.Marshal(v, json.Deterministic(true))
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
