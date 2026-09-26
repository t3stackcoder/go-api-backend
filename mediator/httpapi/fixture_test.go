package httpapi_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
)

func init() {
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

// ---- request types of the fixture service ----

type orderLine struct {
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

type createOrder struct {
	mediator.Command[createOrderResult]
	CustomerID string      `json:"customerId"`
	Lines      []orderLine `json:"lines"`
	When       time.Time   `json:"when"`
	Tenant     string      `json:"tenant" header:"X-Tenant"`
}

func (createOrder) Route() httpapi.Route {
	return httpapi.Route{Method: "POST", Path: "/orders", Status: 201}
}

type createOrderResult struct {
	OrderID string    `json:"orderId"`
	Tenant  string    `json:"tenant"`
	Lines   int       `json:"lines"`
	When    time.Time `json:"when"`
}

type orderView struct {
	OrderID string   `json:"orderId"`
	Expand  []string `json:"expand"`
	Page    *int     `json:"page"`
	Custom  string   `json:"custom"`
}

type getOrder struct {
	mediator.Query[orderView]
	OrderID uuid.UUID `json:"orderId" path:"orderId"`
	Expand  []string  `json:"expand" query:"expand"`
	Page    *int      `json:"page" query:"page"`
	Custom  string    `json:"custom" header:"X-Custom"`
}

func (getOrder) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/orders/{orderId}"} }

// cancelOrder shares its path with getOrder under another method.
type cancelOrder struct {
	mediator.Command[mediator.Void]
	OrderID uuid.UUID `json:"orderId" path:"orderId"`
	Reason  string    `json:"reason"`
}

func (cancelOrder) Route() httpapi.Route {
	return httpapi.Route{Method: "DELETE", Path: "/orders/{orderId}"}
}

type level string

// color is a TextUnmarshaler bound from "r,g,b".
type color struct{ R, G, B uint8 }

func (c *color) UnmarshalText(b []byte) error {
	parts := strings.Split(string(b), ",")
	if len(parts) != 3 {
		return errors.New("want r,g,b")
	}
	var vals [3]uint8
	for i, p := range parts {
		n, err := strconv.ParseUint(p, 10, 8)
		if err != nil {
			return err
		}
		vals[i] = uint8(n)
	}
	c.R, c.G, c.B = vals[0], vals[1], vals[2]
	return nil
}

func (c color) MarshalText() ([]byte, error) {
	return []byte(fmt.Sprintf("%d,%d,%d", c.R, c.G, c.B)), nil
}

// listOrders is the RPC GET query: every field is read from the query string.
type listOrders struct {
	mediator.Query[listEcho]
	Status string      `json:"status"`
	Limit  int         `json:"limit"`
	Since  time.Time   `json:"since"`
	IDs    []uuid.UUID `json:"ids"`
	Active bool        `json:"active"`
	Ratio  float64     `json:"ratio"`
	Count  uint16      `json:"count"`
	Tags   []string    `json:"tags"`
	Small  int8        `json:"small"`
	Level  level       `json:"level"`
	Color  color       `json:"color"`
	Page   *int        `json:"page"`
	Nums   []int       `json:"nums"`
}

type listEcho struct {
	Status string      `json:"status"`
	Limit  int         `json:"limit"`
	Since  time.Time   `json:"since"`
	IDs    []uuid.UUID `json:"ids"`
	Active bool        `json:"active"`
	Ratio  float64     `json:"ratio"`
	Count  uint16      `json:"count"`
	Tags   []string    `json:"tags"`
	Small  int8        `json:"small"`
	Level  level       `json:"level"`
	Color  color       `json:"color"`
	Page   *int        `json:"page"`
	Nums   []int       `json:"nums"`
}

type orderFilter struct {
	Status string `json:"status"`
}

// searchOrders is the RPC POST query: Filter is not a scalar.
type searchOrders struct {
	mediator.Query[listEcho]
	Filter orderFilter `json:"filter"`
	Limit  int         `json:"limit"`
	ID     uuid.UUID   `json:"id"`
}

type ping struct {
	mediator.Command[mediator.Void]
}

// quiet returns a value but declares 204.
type quiet struct {
	mediator.Command[createOrderResult]
}

func (quiet) Route() httpapi.Route { return httpapi.Route{Method: "POST", Path: "/quiet", Status: 204} }

type ctxInfo struct {
	Correlation string `json:"correlation"`
	IdemKey     string `json:"idemKey"`
	HasIdemKey  bool   `json:"hasIdemKey"`
	NoCache     bool   `json:"noCache"`
	Subject     string `json:"subject"`
	Remote      string `json:"remote"`
	TraceID     string `json:"traceId"`
	Depth       int    `json:"depth"`
}

func infoOfCtx(ctx context.Context) ctxInfo {
	key, has := mediator.IdempotencyKeyFrom(ctx)
	return ctxInfo{
		Correlation: mediator.CorrelationID(ctx),
		IdemKey:     key,
		HasIdemKey:  has,
		NoCache:     mediator.NoCache(ctx),
		Subject:     authz.PrincipalFrom(ctx).Subject,
		Remote:      httpapi.RemoteAddr(ctx),
		TraceID:     trace.SpanContextFromContext(ctx).TraceID().String(),
		Depth:       mediator.Depth(ctx),
	}
}

type whoami struct {
	mediator.Command[ctxInfo]
	Cache string `json:"cache"`
}

type peek struct {
	mediator.Query[ctxInfo]
	Cache string `json:"cache"`
}

// fail returns the configured error.
type fail struct {
	mediator.Command[mediator.Void]
	Code    string `json:"code"`
	Msg     string `json:"msg"`
	RetryMs int    `json:"retryMs"`
	Mode    string `json:"mode"`
}

// weird returns a response that cannot be encoded.
type weird struct {
	mediator.Command[map[string]any]
}

// slow blocks until the fixture releases it.
type slow struct {
	mediator.Command[mediator.Void]
}

type base struct {
	ID string `json:"id" path:"id"`
}

// nested binds a path parameter through an embedded pointer.
type nested struct {
	mediator.Query[string]
	*Base
}

func (nested) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/nested/{id}"} }

// slashy has a trailing slash, registered as an exact match.
type slashy struct{ mediator.Query[string] }

func (slashy) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/slashy/"} }

type countTo struct {
	mediator.StreamQuery[int]
	N       int  `json:"n"`
	FailAt  int  `json:"failAt"`
	PanicAt int  `json:"panicAt"`
	Block   bool `json:"block"`
}

type tailEvent struct {
	ID    string `json:"id"`
	Value any    `json:"value"`
}

func (e tailEvent) EventID() string { return e.ID }

type tail struct {
	mediator.StreamQuery[tailEvent]
	LastID string `json:"lastId" header:"Last-Event-ID"`
	Bad    bool   `json:"bad" query:"bad"`
	N      int    `json:"n" query:"n"`
}

func (tail) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/tail"} }

// ---- fixture ----

type fixture struct {
	m   *mediator.Mediator
	srv *httpapi.Server
	log *logSink

	slowStarted, slowRelease       chan struct{}
	streamBlocked, streamCancelled chan struct{}
	once                           sync.Once
	blockedOnce                    sync.Once
}

// logSink collects slog records for assertions.
type logSink struct {
	mu      sync.Mutex
	records []slog.Record
}

func (s *logSink) Enabled(context.Context, slog.Level) bool { return true }
func (s *logSink) Handle(_ context.Context, r slog.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, r.Clone())
	return nil
}
func (s *logSink) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *logSink) WithGroup(string) slog.Handler      { return s }

// find returns the first record at level with the message, and its attrs.
func (s *logSink) find(level slog.Level, msg string) (map[string]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range s.records {
		if r.Level != level || r.Message != msg {
			continue
		}
		attrs := map[string]string{}
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			return true
		})
		return attrs, true
	}
	return nil, false
}

func newFixture(tb testing.TB, cfg httpapi.Config) *fixture {
	tb.Helper()
	f := &fixture{
		log:             &logSink{},
		slowStarted:     make(chan struct{}),
		slowRelease:     make(chan struct{}),
		streamBlocked:   make(chan struct{}),
		streamCancelled: make(chan struct{}),
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(f.log)
	}
	m := mediator.New(mediator.WithLogger(slog.New(f.log)))
	must := func(err error) {
		tb.Helper()
		if err != nil {
			tb.Fatal(err)
		}
	}
	must(mediator.HandleFunc(m, func(_ context.Context, c createOrder) (createOrderResult, error) {
		return createOrderResult{OrderID: "o-" + c.CustomerID, Tenant: c.Tenant, Lines: len(c.Lines), When: c.When}, nil
	}))
	must(mediator.HandleFunc(m, func(_ context.Context, q getOrder) (orderView, error) {
		return orderView{OrderID: q.OrderID.String(), Expand: q.Expand, Page: q.Page, Custom: q.Custom}, nil
	}))
	must(mediator.HandleFunc(m, func(context.Context, cancelOrder) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(mediator.HandleFunc(m, func(_ context.Context, q listOrders) (listEcho, error) {
		return listEcho{Status: q.Status, Limit: q.Limit, Since: q.Since, IDs: q.IDs, Active: q.Active, Ratio: q.Ratio,
			Count: q.Count, Tags: q.Tags, Small: q.Small, Level: q.Level, Color: q.Color, Page: q.Page, Nums: q.Nums}, nil
	}))
	must(mediator.HandleFunc(m, func(_ context.Context, q searchOrders) (listEcho, error) {
		return listEcho{Status: q.Filter.Status, Limit: q.Limit}, nil
	}))
	must(mediator.HandleFunc(m, func(context.Context, ping) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(mediator.HandleFunc(m, func(context.Context, quiet) (createOrderResult, error) {
		return createOrderResult{OrderID: "quiet"}, nil
	}))
	must(mediator.HandleFunc(m, func(ctx context.Context, c whoami) (ctxInfo, error) {
		if c.Cache != "" {
			httpapi.SetCacheResult(ctx, httpapi.CacheResult(c.Cache))
		}
		return infoOfCtx(ctx), nil
	}))
	must(mediator.HandleFunc(m, func(ctx context.Context, q peek) (ctxInfo, error) {
		if q.Cache != "" {
			httpapi.SetCacheResult(ctx, httpapi.CacheResult(q.Cache))
		}
		return infoOfCtx(ctx), nil
	}))
	must(mediator.HandleFunc(m, func(context.Context, fail) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(mediator.Pre(m, mediator.PreProcessorFunc[fail, mediator.Void](func(_ context.Context, c fail) error {
		switch c.Mode {
		case "panic":
			panic("kaboom " + c.Msg)
		case "plain":
			return errors.New(c.Msg)
		case "validation":
			return (&mediator.ValidationError{}).Add("/customerId", "uuid", "must be a UUID").Add("/lines/0/qty", "min", "must be at least 1")
		case "wrapped":
			return mediator.Wrap(mediator.Code(c.Code), c.Msg, errors.New("cause: "+c.Msg))
		case "ctx":
			return context.DeadlineExceeded
		}
		err := mediator.E(mediator.Code(c.Code), c.Msg)
		if c.RetryMs != 0 {
			err = err.WithDetail("retry_after_ms", c.RetryMs)
		}
		return err
	})))
	must(mediator.HandleFunc(m, func(context.Context, weird) (map[string]any, error) {
		return map[string]any{"f": func() {}}, nil
	}))
	must(mediator.HandleFunc(m, func(ctx context.Context, _ slow) (mediator.Void, error) {
		f.once.Do(func() { close(f.slowStarted) })
		select {
		case <-f.slowRelease:
		case <-ctx.Done():
		}
		return mediator.Void{}, nil
	}))
	must(mediator.HandleFunc(m, func(_ context.Context, q nested) (string, error) {
		if q.Base == nil {
			return "", nil
		}
		return q.ID, nil
	}))
	must(mediator.HandleFunc(m, func(context.Context, slashy) (string, error) { return "slashy", nil }))
	must(mediator.HandleStreamFunc(m, func(ctx context.Context, q countTo) iter.Seq2[int, error] {
		return func(yield func(int, error) bool) {
			for i := 1; i <= q.N; i++ {
				if q.FailAt == i {
					yield(0, mediator.E(mediator.CodeNotFound, "item gone"))
					return
				}
				if q.PanicAt == i {
					panic("stream panic")
				}
				if !yield(i, nil) {
					return
				}
			}
			if q.Block {
				f.blockedOnce.Do(func() { close(f.streamBlocked) })
				<-ctx.Done()
				close(f.streamCancelled)
			}
		}
	}))
	must(mediator.HandleStreamFunc(m, func(ctx context.Context, q tail) iter.Seq2[tailEvent, error] {
		return func(yield func(tailEvent, error) bool) {
			start, _ := strconv.Atoi(q.LastID)
			for i := start + 1; i <= start+q.N; i++ {
				ev := tailEvent{ID: strconv.Itoa(i), Value: i}
				if q.Bad {
					ev.Value = func() {}
				}
				if !yield(ev, nil) {
					return
				}
			}
		}
	}))
	must(m.Build())
	srv, err := httpapi.New(m, cfg)
	must(err)
	f.m, f.srv = m, srv
	return f
}

// do performs one request against the server. Header pairs follow the body.
func (f *fixture) do(tb testing.TB, method, target, body string, hdr ...string) *httptest.ResponseRecorder {
	tb.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, target, rd)
	if body != "" && !hasHeader(hdr, "Content-Type") {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	f.srv.ServeHTTP(rec, req)
	return rec
}

func hasHeader(pairs []string, name string) bool {
	for i := 0; i+1 < len(pairs); i += 2 {
		if strings.EqualFold(pairs[i], name) {
			return true
		}
	}
	return false
}

func problemOf(tb testing.TB, rec *httptest.ResponseRecorder) httpapi.Problem {
	tb.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != httpapi.ProblemContentType {
		tb.Fatalf("content type %q, body %s", ct, rec.Body.String())
	}
	var p httpapi.Problem
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		tb.Fatalf("problem body %q: %v", rec.Body.String(), err)
	}
	if p.Status != rec.Code {
		tb.Fatalf("problem status %d, response %d", p.Status, rec.Code)
	}
	return p
}

func decodeJSON[T any](tb testing.TB, rec *httptest.ResponseRecorder) T {
	tb.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		tb.Fatalf("content type %q, status %d, body %s", ct, rec.Code, rec.Body.String())
	}
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		tb.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	return v
}

// reg registers a handler returning the zero response, for routing tests.
func reg[Q mediator.Request[R], R any](tb testing.TB, m *mediator.Mediator) {
	tb.Helper()
	if err := mediator.HandleFunc(m, func(context.Context, Q) (R, error) { var z R; return z, nil }); err != nil {
		tb.Fatal(err)
	}
}

func regStream[Q mediator.StreamRequest[T], T any](tb testing.TB, m *mediator.Mediator) {
	tb.Helper()
	if err := mediator.HandleStreamFunc(m, func(context.Context, Q) iter.Seq2[T, error] { return nil }); err != nil {
		tb.Fatal(err)
	}
}

func mustBuild(tb testing.TB, m *mediator.Mediator) {
	tb.Helper()
	if err := m.Build(); err != nil {
		tb.Fatal(err)
	}
}

func headerOK(tb testing.TB, h http.Header, name, want string) {
	tb.Helper()
	if got := h.Get(name); got != want {
		tb.Errorf("header %s = %q, want %q", name, got, want)
	}
}
