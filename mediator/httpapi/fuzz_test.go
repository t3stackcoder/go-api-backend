package httpapi_test

import (
	"encoding/json/v2"
	"errors"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
)

// FuzzRequestDecode sends arbitrary bodies and parameters through the full
// handler of several request types. The handler must never panic and must
// always answer with JSON, a problem, or an empty 204.
func FuzzRequestDecode(f *testing.F) {
	fx := newFixture(f, httpapi.Config{MaxBodyBytes: 4096})
	seeds := []struct {
		body  []byte
		which uint8
		param string
	}{
		{[]byte(`{"customerId":"c1","lines":[{"sku":"a","qty":1}],"when":"2024-01-02T03:04:05Z"}`), 0, ""},
		{[]byte(`{"customerId":1}`), 0, ""},
		{[]byte(`{"filter":{"status":"open"},"limit":5}`), 1, ""},
		{[]byte(`[1,2,3]`), 1, ""},
		{[]byte(``), 2, "open"},
		{[]byte(`{"cache":"hit"}`), 3, ""},
		{[]byte(``), 4, "0192e1b4-0000-7000-8000-000000000001"},
		{[]byte(`{"a":`), 0, ""},
		{[]byte("{\"customerId\":\"\xff\"}"), 0, ""},
		{[]byte(`{"customerId":"a","customerId":"b"}`), 0, "\x00"},
		{[]byte(`null`), 2, "1e309"},
		{[]byte(`{"n":"x"}`), 5, "not-a-number"},
	}
	for _, s := range seeds {
		f.Add(s.body, s.which, s.param)
	}
	f.Fuzz(func(t *testing.T, body []byte, which uint8, param string) {
		var method, path, ct string
		switch which % 6 {
		case 0:
			method, path, ct = "POST", "/orders", "application/json"
		case 1:
			method, path, ct = "POST", "/rpc/searchOrders", "application/json"
		case 2:
			method, path = "GET", "/rpc/listOrders"
		case 3:
			method, path, ct = "POST", "/rpc/whoami", "application/json; charset=utf-8"
		case 4:
			method, path = "GET", "/orders/"+url.PathEscape(param)
		case 5:
			method, path = "GET", "/tail"
		}
		u := url.URL{Path: path, RawQuery: url.Values{"status": {param}, "limit": {param}, "page": {param}, "n": {param}, "expand": {param}}.Encode()}
		req := httptest.NewRequest(method, u.String(), strings.NewReader(string(body)))
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		req.Header.Set("X-Correlation-ID", param)
		req.Header.Set("X-Tenant", param)
		req.Header.Set("Last-Event-ID", param)
		rec := httptest.NewRecorder()
		fx.srv.ServeHTTP(rec, req)
		switch ctype := rec.Header().Get("Content-Type"); {
		case rec.Code/100 == 3:
			// ServeMux cleans paths such as /orders/. with a redirect.
			if rec.Header().Get("Location") == "" {
				t.Fatalf("redirect without Location")
			}
		case rec.Code == 204:
			if rec.Body.Len() != 0 {
				t.Fatalf("204 with body %q", rec.Body)
			}
		case ctype == "text/event-stream":
			// Streams are exercised elsewhere; the headers must have flushed.
			if rec.Code != 200 {
				t.Fatalf("stream status %d", rec.Code)
			}
		case ctype == httpapi.ProblemContentType:
			p := problemOf(t, rec)
			if p.Code == "" || !strings.HasPrefix(p.Type, httpapi.ProblemType) || p.Title == "" || p.Detail == "" || p.Instance == "" || p.CorrelationID == "" {
				t.Fatalf("incomplete problem %+v", p)
			}
			if rec.Code < 400 || rec.Code > 599 {
				t.Fatalf("problem with status %d", rec.Code)
			}
		case ctype == "application/json":
			var v any
			if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
				t.Fatalf("invalid JSON %q: %v", rec.Body, err)
			}
			if rec.Code != 200 && rec.Code != 201 {
				t.Fatalf("json with status %d", rec.Code)
			}
		default:
			t.Fatalf("unexpected response %d %q %q", rec.Code, ctype, rec.Body)
		}
		if rec.Header().Get("X-Correlation-ID") == "" {
			t.Fatal("no correlation header")
		}
	})
}

// FuzzProblemJSON renders arbitrary errors as problem details and checks
// the required members, the status agreement, and that internal causes stay
// out of the body.
func FuzzProblemJSON(f *testing.F) {
	f.Add("not_found", "order missing", 0, "orderId", int64(0), uint8(0))
	f.Add("validation", "", 2, "", int64(0), uint8(1))
	f.Add("internal", "secret", 0, "k", int64(5), uint8(2))
	f.Add("rate_limited", "slow", 0, "retry_after_ms", int64(1500), uint8(0))
	f.Add("idempotency_in_progress", "busy", 0, "retry_after_ms", int64(-1), uint8(2))
	f.Add("", "", 0, "", int64(0), uint8(3))
	f.Add("weird", "\x00\xff", 9, "\n", int64(-9), uint8(4))
	f.Fuzz(func(t *testing.T, code, msg string, nfields int, key string, num int64, kind uint8) {
		var err error
		switch kind % 5 {
		case 0:
			e := mediator.E(mediator.Code(code), msg)
			if key != "" {
				e = e.WithDetail(key, num)
			}
			err = e
		case 1:
			ve := &mediator.ValidationError{}
			for i := 0; i < nfields%6; i++ {
				ve = ve.Add("/f"+msg, code, msg)
			}
			err = ve
		case 2:
			err = mediator.Wrap(mediator.Code(code), msg, errors.New("cause-marker-7f3a"))
		case 3:
			err = errors.New("cause-marker-7f3a " + msg)
		case 4:
			err = nil
		}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/i", nil)
		req.Header.Set("X-Correlation-ID", key)
		httpapi.WriteProblem(rec, req, err)
		var p httpapi.Problem
		if uerr := json.Unmarshal(rec.Body.Bytes(), &p); uerr != nil {
			t.Fatalf("invalid problem JSON %q: %v", rec.Body, uerr)
		}
		if p.Status != rec.Code || p.Status < 400 || p.Status > 599 {
			t.Fatalf("status %d vs %d", p.Status, rec.Code)
		}
		if p.Type != httpapi.ProblemType+p.Code || p.Title == "" || p.Detail == "" || p.Instance != "/i" || p.CorrelationID == "" {
			t.Fatalf("incomplete %+v", p)
		}
		if p.Code == "internal" {
			if p.Detail != httpapi.InternalDetail || p.Details != nil {
				t.Fatalf("internal leaked %+v", p)
			}
		}
		if strings.Contains(rec.Body.String(), "cause-marker-7f3a") {
			t.Fatalf("cause leaked: %s", rec.Body)
		}
		if rec.Header().Get("Content-Type") != httpapi.ProblemContentType {
			t.Fatal("content type")
		}
	})
}

// fuzzRouted reads its route from a variable so the fuzzer can vary it.
type fuzzRouted struct {
	mediator.Query[int]
	ID string `json:"id" path:"id"`
}

var (
	fuzzRouteMu sync.Mutex
	fuzzRoute   httpapi.Route
)

func (fuzzRouted) Route() httpapi.Route { return fuzzRoute }

type fuzzFixed struct{ mediator.Query[int] }

func (fuzzFixed) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/fixed"} }

// FuzzRoutePattern feeds arbitrary Route values through BuildCheck and New:
// they are either rejected with an error or registered without a panic.
func FuzzRoutePattern(f *testing.F) {
	f.Add("GET", "/things/{id}", 0)
	f.Add("GET", "/fixed", 200)
	f.Add("POST", "/a/{id}/{$}", 201)
	f.Add("get", "/things/{id}", 0)
	f.Add("GET", "/{id...}", 0)
	f.Add("GET", "/{", 0)
	f.Add("GET", "", 0)
	f.Add("GET", "/things/{id}/{id}", 0)
	f.Add("DELETE", "/healthz", 500)
	f.Add("GET", "/a b/{id}", 0)
	f.Add("GET", "/things/{id}//", 204)
	f.Add("PUT", "/{$}/{id}", 0)
	f.Fuzz(func(t *testing.T, method, path string, status int) {
		fuzzRouteMu.Lock()
		defer fuzzRouteMu.Unlock()
		fuzzRoute = httpapi.Route{Method: method, Path: path, Status: status}
		m := mediator.New()
		reg[fuzzRouted](t, m)
		reg[fuzzFixed](t, m)
		mustBuild(t, m)
		err := httpapi.BuildCheck(m)
		srv, nerr := httpapi.New(m, httpapi.Config{Prefix: "/api"})
		if (err == nil) != (nerr == nil) {
			t.Fatalf("BuildCheck %v but New %v", err, nerr)
		}
		if err != nil {
			return
		}
		if len(srv.Routes()) != 2 {
			t.Fatalf("routes %v", srv.Routes())
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest("GET", "/api/fixed", nil))
		if rec.Code != 200 {
			t.Fatalf("fixed route %d", rec.Code)
		}
	})
}
