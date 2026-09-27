package httpapi_test

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
)

func TestDecode(t *testing.T) {
	f := newFixture(t, httpapi.Config{MaxBodyBytes: 64})
	// bodyOfLen is a valid createOrder body of exactly n bytes.
	bodyOfLen := func(n int) string {
		const frame = `{"customerId":""}`
		return `{"customerId":"` + strings.Repeat("x", n-len(frame)) + `"}`
	}
	rows := []struct {
		name, method, path, body, ct string
		status                       int
		code, ptr, rule, msg         string
	}{
		{"syntax", "POST", "/orders", `{"customerId":`, "application/json", 400, "bad_request", "", "", "body is not well-formed JSON"},
		{"invalid utf8", "POST", "/orders", "{\"customerId\":\"\xff\"}", "application/json", 400, "bad_request", "", "", "invalid UTF-8"},
		{"trailing data", "POST", "/orders", `{} x`, "application/json", 400, "bad_request", "", "", ""},
		{"not json at all", "POST", "/orders", `nope`, "application/json", 400, "bad_request", "", "", ""},
		{"invalid utf8 in path", "POST", "/orders%8d", `{}`, "application/json", 404, "not_found", "", "", "no route for POST /orders�"},
		{"unknown member", "POST", "/orders", `{"customerId":"c","bogus":1}`, "application/json", 422, "validation", "/bogus", "unknown", "unknown member"},
		{"nested unknown", "POST", "/orders", `{"lines":[{"sku":"s","qty":1,"zzz":2}]}`, "application/json", 422, "validation", "/lines/0/zzz", "unknown", ""},
		{"wrong type", "POST", "/orders", `{"customerId":1}`, "application/json", 422, "validation", "/customerId", "type", "expected string, got number"},
		{"nested type", "POST", "/orders", `{"lines":[{"qty":"x"}]}`, "application/json", 422, "validation", "/lines/0/qty", "type", "expected integer, got string"},
		{"array into struct", "POST", "/orders", `{"lines":{"a":1}}`, "application/json", 422, "validation", "/lines", "type", "expected array, got object"},
		{"duplicate", "POST", "/orders", `{"customerId":"a","customerId":"b"}`, "application/json", 422, "validation", "/customerId", "duplicate", "duplicate member"},
		{"nested duplicate", "POST", "/orders", `{"lines":[{"qty":1,"qty":2}]}`, "application/json", 422, "validation", "/lines/0/qty", "duplicate", ""},
		{"array at top", "POST", "/orders", `[1]`, "application/json", 422, "validation", "", "type", "expected object, got array"},
		{"bad time", "POST", "/orders", `{"when":"nope"}`, "application/json", 422, "validation", "/when", "format", "RFC 3339"},
		{"bad uuid", "POST", "/rpc/searchOrders", `{"id":"nope"}`, "application/json", 422, "validation", "/id", "format", "must be a UUID"},
		{"overflow", "POST", "/rpc/searchOrders", `{"limit":99999999999999999999}`, "application/json", 422, "validation", "/limit", "type", "invalid value"},
		{"bound member in body", "POST", "/orders", `{"tenant":"t"}`, "application/json", 422, "validation", "/tenant", "binding", "bound from the header"},
		{"bound member after others", "POST", "/orders", `{"customerId":"c","lines":[{"qty":1}],"tenant":"t"}`, "application/json", 422, "validation", "/tenant", "binding", ""},
		{"prescan tolerates truncated object", "POST", "/orders", `{`, "application/json", 400, "bad_request", "", "", ""},
		{"prescan tolerates truncated value", "POST", "/orders", `{"customerId":`, "application/json", 400, "bad_request", "", "", ""},
		{"prescan tolerates duplicate", "POST", "/orders", `{"customerId":"a","customerId":"b","tenant":"t"}`, "application/json", 422, "validation", "/customerId", "duplicate", ""},
		{"no content type", "POST", "/orders", `{}`, "", 415, "unsupported_media_type", "", "", "must be application/json"},
		{"text/plain", "POST", "/orders", `{}`, "text/plain", 415, "unsupported_media_type", "", "", "is not application/json"},
		{"bad media type", "POST", "/orders", `{}`, "application/", 415, "unsupported_media_type", "", "", ""},
		{"bad charset", "POST", "/orders", `{}`, "application/json; charset=latin1", 415, "unsupported_media_type", "", "", "charset must be UTF-8"},
		{"charset utf-8", "POST", "/orders", `{"customerId":"c"}`, "Application/JSON; charset=UTF-8", 201, "", "", "", ""},
		{"charset utf8", "POST", "/orders", `{"customerId":"c"}`, "application/json; charset=utf8", 201, "", "", "", ""},
		{"empty body", "POST", "/orders", "", "", 201, "", "", "", ""},
		{"null body", "POST", "/orders", `null`, "application/json", 201, "", "", "", ""},
		{"too large", "POST", "/orders", `{"customerId":"` + strings.Repeat("x", 80) + `"}`, "application/json", 413, "payload_too_large", "", "", "body exceeds 64 bytes"},
		{"body exactly at the limit", "POST", "/orders", bodyOfLen(64), "application/json", 201, "", "", "", ""},
		{"body one byte over the limit", "POST", "/orders", bodyOfLen(65), "application/json", 413, "payload_too_large", "", "", "body exceeds 64 bytes"},
		{"get with body decodes it", "GET", "/rpc/peek", `{"bogus":1}`, "application/json", 422, "validation", "/bogus", "unknown", ""},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			rec := f.do(t, row.method, row.path, row.body, "Content-Type", row.ct)
			if rec.Code != row.status {
				t.Fatalf("status %d, want %d: %s", rec.Code, row.status, rec.Body)
			}
			if row.code == "" {
				return
			}
			p := problemOf(t, rec)
			if p.Code != row.code {
				t.Errorf("code %q, want %q", p.Code, row.code)
			}
			if row.rule == "" {
				if row.msg != "" && !strings.Contains(p.Detail, row.msg) {
					t.Errorf("detail %q lacks %q", p.Detail, row.msg)
				}
				return
			}
			if len(p.Errors) != 1 {
				t.Fatalf("errors = %+v, want one", p.Errors)
			}
			fe := p.Errors[0]
			if fe.Path != row.ptr || fe.Rule != row.rule || !strings.Contains(fe.Message, row.msg) {
				t.Errorf("field error %+v, want path %q rule %q message containing %q", fe, row.ptr, row.rule, row.msg)
			}
			if p.Detail != "1 field is invalid" {
				t.Errorf("detail %q", p.Detail)
			}
		})
	}
	t.Run("streamed body over the limit", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/orders", strings.NewReader(`{"customerId":"`+strings.Repeat("y", 80)+`"}`))
		req.Header.Set("Content-Type", "application/json")
		req.ContentLength = -1
		rec := httptest.NewRecorder()
		f.srv.ServeHTTP(rec, req)
		if p := problemOf(t, rec); rec.Code != 413 || p.Code != "payload_too_large" {
			t.Fatalf("%d %+v", rec.Code, p)
		}
	})
	t.Run("body read error", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/orders", io.NopCloser(errReader{}))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		f.srv.ServeHTTP(rec, req)
		if p := problemOf(t, rec); rec.Code != 400 || !strings.Contains(p.Detail, "reading the request body failed") {
			t.Fatalf("%d %+v", rec.Code, p)
		}
	})
	t.Run("nil body", func(t *testing.T) {
		req := &http.Request{Method: "POST", URL: &url.URL{Path: "/orders"}, Header: http.Header{}, RemoteAddr: "unix"}
		rec := httptest.NewRecorder()
		f.srv.ServeHTTP(rec, req)
		if rec.Code != 201 {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
	})
	t.Run("allow unknown fields", func(t *testing.T) {
		g := newFixture(t, httpapi.Config{AllowUnknownFields: true})
		rec := g.do(t, "POST", "/orders", `{"customerId":"c","bogus":1}`)
		if rec.Code != 201 {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
		// Bound members are still rejected.
		rec = g.do(t, "POST", "/orders", `{"tenant":"t"}`)
		if rec.Code != 422 {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
	})
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestBind(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	id := uuid.MustParse("0192e1b4-0000-7000-8000-000000000001")
	t.Run("rest bindings", func(t *testing.T) {
		rec := f.do(t, "GET", "/orders/"+id.String()+"?expand=a,b&expand=c&page=2", "", "X-Custom", "hdr")
		if rec.Code != 200 {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
		v := decodeJSON[orderView](t, rec)
		page := 2
		want := orderView{OrderID: id.String(), Expand: []string{"a", "b", "c"}, Page: &page, Custom: "hdr"}
		if !reflect.DeepEqual(v, want) {
			t.Errorf("got %+v, want %+v", v, want)
		}
	})
	t.Run("absent optional fields", func(t *testing.T) {
		rec := f.do(t, "GET", "/orders/"+id.String()+"?page=", "")
		v := decodeJSON[orderView](t, rec)
		if v.Page != nil || len(v.Expand) != 0 || v.Custom != "" {
			t.Errorf("got %+v", v)
		}
	})
	t.Run("embedded pointer", func(t *testing.T) {
		rec := f.do(t, "GET", "/nested/xyz", "")
		if rec.Code != 200 || rec.Body.String() != `"xyz"` {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
	})
	t.Run("rpc query scalars", func(t *testing.T) {
		q := url.Values{
			"status": {"open"}, "limit": {"5"}, "since": {"2024-01-02T03:04:05Z"}, "ids": {id.String()},
			"active": {"true"}, "ratio": {"1.5"}, "count": {"7"}, "tags": {"x, y", "z"}, "small": {"127"},
			"level": {"high"}, "color": {"1,2,3"}, "page": {"3"}, "nums": {"1", "2,3"},
		}
		rec := f.do(t, "GET", "/rpc/listOrders?"+q.Encode(), "")
		if rec.Code != 200 {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
		got := decodeJSON[listEcho](t, rec)
		page := 3
		want := listEcho{Status: "open", Limit: 5, Since: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), IDs: []uuid.UUID{id},
			Active: true, Ratio: 1.5, Count: 7, Tags: []string{"x", "y", "z"}, Small: 127, Level: "high", Color: color{1, 2, 3}, Page: &page, Nums: []int{1, 2, 3}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %+v\nwant %+v", got, want)
		}
	})
	t.Run("rpc query empty", func(t *testing.T) {
		rec := f.do(t, "GET", "/rpc/listOrders", "")
		got := decodeJSON[listEcho](t, rec)
		if !reflect.DeepEqual(got, listEcho{IDs: []uuid.UUID{}, Tags: []string{}, Nums: []int{}}) {
			t.Errorf("got %+v", got)
		}
	})
	rows := []struct {
		name, target, ptr, msg string
	}{
		{"bad uuid path", "/orders/nope", "/orderId", "must be a UUID"},
		{"bad int", "/orders/" + id.String() + "?page=x", "/page", "must be an integer"},
		{"int range", "/orders/" + id.String() + "?page=99999999999999999999", "/page", "must fit in a signed 64-bit integer"},
		{"limit", "/rpc/listOrders?limit=abc", "/limit", "must be an integer"},
		{"time", "/rpc/listOrders?since=nope", "/since", "must be an RFC 3339 date-time"},
		{"uuid slice", "/rpc/listOrders?ids=bad", "/ids", "element 0 must be a UUID"},
		{"bool", "/rpc/listOrders?active=maybe", "/active", "must be a boolean"},
		{"float", "/rpc/listOrders?ratio=x", "/ratio", "must be a number"},
		{"uint negative", "/rpc/listOrders?count=-1", "/count", "must be a non-negative integer"},
		{"uint range", "/rpc/listOrders?count=70000", "/count", "must fit in an unsigned 16-bit integer"},
		{"int8 range", "/rpc/listOrders?small=200", "/small", "must fit in a signed 8-bit integer"},
		{"text unmarshaler", "/rpc/listOrders?color=1,2", "/color", "is invalid: want r,g,b"},
		{"slice element", "/rpc/listOrders?nums=1,x", "/nums", "element 1 must be an integer"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			rec := f.do(t, "GET", row.target, "")
			if rec.Code != 422 {
				t.Fatalf("%d %s", rec.Code, rec.Body)
			}
			p := problemOf(t, rec)
			if len(p.Errors) != 1 || p.Errors[0].Path != row.ptr || p.Errors[0].Rule != "type" || !strings.Contains(p.Errors[0].Message, row.msg) {
				t.Errorf("errors %+v, want %s %q", p.Errors, row.ptr, row.msg)
			}
		})
	}
	t.Run("several at once", func(t *testing.T) {
		rec := f.do(t, "GET", "/rpc/listOrders?limit=a&active=b&count=c", "")
		p := problemOf(t, rec)
		if len(p.Errors) != 3 || p.Detail != "3 fields are invalid" {
			t.Errorf("%+v", p)
		}
	})
}

func TestContext(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	t.Run("correlation echoed", func(t *testing.T) {
		rec := f.do(t, "POST", "/rpc/whoami", "", "X-Correlation-ID", "abc-123")
		headerOK(t, rec.Header(), "X-Correlation-ID", "abc-123")
		if v := decodeJSON[ctxInfo](t, rec); v.Correlation != "abc-123" || v.Depth != 1 {
			t.Errorf("%+v", v)
		}
	})
	t.Run("correlation generated", func(t *testing.T) {
		long := strings.Repeat("a", 128)
		for _, in := range []string{"", "\x01bad", strings.Repeat("a", 129), "\xff\xfe"} {
			rec := f.do(t, "POST", "/rpc/whoami", "", "X-Correlation-ID", in)
			got := rec.Header().Get("X-Correlation-ID")
			if id, err := uuid.Parse(got); err != nil || id.Version() != 7 {
				t.Errorf("input %q: header %q is not a UUIDv7", in, got)
			}
			if v := decodeJSON[ctxInfo](t, rec); v.Correlation != got {
				t.Errorf("body correlation %q != header %q", v.Correlation, got)
			}
		}
		rec := f.do(t, "POST", "/rpc/whoami", "", "X-Correlation-ID", long)
		headerOK(t, rec.Header(), "X-Correlation-ID", long)
	})
	t.Run("idempotency key on command", func(t *testing.T) {
		rec := f.do(t, "POST", "/rpc/whoami", "", "Idempotency-Key", "k1")
		if v := decodeJSON[ctxInfo](t, rec); !v.HasIdemKey || v.IdemKey != "k1" {
			t.Errorf("%+v", v)
		}
		if rec.Header().Get("Warning") != "" {
			t.Error("unexpected Warning")
		}
	})
	t.Run("idempotency key on query", func(t *testing.T) {
		rec := f.do(t, "GET", "/rpc/peek", "", "Idempotency-Key", "k1")
		if v := decodeJSON[ctxInfo](t, rec); v.HasIdemKey {
			t.Errorf("%+v", v)
		}
		headerOK(t, rec.Header(), "Warning", `299 - "Idempotency-Key ignored on queries"`)
	})
	t.Run("no-cache", func(t *testing.T) {
		rec := f.do(t, "GET", "/rpc/peek", "", "Cache-Control", "max-age=0, No-Cache")
		if v := decodeJSON[ctxInfo](t, rec); !v.NoCache {
			t.Errorf("%+v", v)
		}
		rec = f.do(t, "GET", "/rpc/peek", "", "Cache-Control", "no-store")
		if v := decodeJSON[ctxInfo](t, rec); v.NoCache {
			t.Errorf("%+v", v)
		}
	})
	t.Run("anonymous without authenticator", func(t *testing.T) {
		rec := f.do(t, "GET", "/rpc/peek", "", "Authorization", "Bearer x")
		if v := decodeJSON[ctxInfo](t, rec); v.Subject != "" {
			t.Errorf("%+v", v)
		}
	})
	t.Run("remote address", func(t *testing.T) {
		rec := f.do(t, "GET", "/rpc/peek", "")
		if v := decodeJSON[ctxInfo](t, rec); v.Remote != "192.0.2.1" {
			t.Errorf("%+v", v)
		}
		req := httptest.NewRequest("GET", "/rpc/peek", nil)
		req.RemoteAddr = "[::1]:9"
		rec = httptest.NewRecorder()
		f.srv.ServeHTTP(rec, req)
		if v := decodeJSON[ctxInfo](t, rec); v.Remote != "::1" {
			t.Errorf("%+v", v)
		}
		req.RemoteAddr = "pipe"
		rec = httptest.NewRecorder()
		f.srv.ServeHTTP(rec, req)
		if v := decodeJSON[ctxInfo](t, rec); v.Remote != "pipe" {
			t.Errorf("%+v", v)
		}
	})
	t.Run("trace context", func(t *testing.T) {
		tp := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
		rec := f.do(t, "GET", "/rpc/peek", "", "traceparent", tp)
		if v := decodeJSON[ctxInfo](t, rec); v.TraceID != "0af7651916cd43dd8448eb211c80319c" {
			t.Errorf("%+v", v)
		}
		if got := rec.Header().Get("traceparent"); !strings.Contains(got, "0af7651916cd43dd8448eb211c80319c") {
			t.Errorf("response traceparent %q", got)
		}
	})
	t.Run("cache result header", func(t *testing.T) {
		rec := f.do(t, "POST", "/rpc/whoami", `{"cache":"hit"}`)
		headerOK(t, rec.Header(), "X-Mediator-Cache", "hit")
		rec = f.do(t, "GET", "/rpc/peek?cache=miss", "")
		headerOK(t, rec.Header(), "X-Mediator-Cache", "miss")
		rec = f.do(t, "GET", "/rpc/peek", "")
		if _, ok := rec.Header()["X-Mediator-Cache"]; ok {
			t.Error("X-Mediator-Cache set without a result")
		}
	})
}

func TestAuthenticator(t *testing.T) {
	t.Run("principal", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{Authenticator: func(r *http.Request) (authz.Principal, error) {
			return authz.Principal{Subject: r.Header.Get("X-User")}, nil
		}})
		rec := f.do(t, "GET", "/rpc/peek", "", "X-User", "alice")
		if v := decodeJSON[ctxInfo](t, rec); v.Subject != "alice" {
			t.Errorf("%+v", v)
		}
	})
	t.Run("plain error is 401 with a fixed detail", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{Authenticator: func(*http.Request) (authz.Principal, error) {
			return authz.Principal{}, errors.New("secret reason")
		}})
		rec := f.do(t, "GET", "/rpc/peek", "")
		p := problemOf(t, rec)
		if rec.Code != 401 || p.Code != "unauthorized" || p.Detail != "authentication failed" || strings.Contains(rec.Body.String(), "secret") {
			t.Errorf("%d %+v", rec.Code, p)
		}
	})
	t.Run("mediator error keeps its message", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{Authenticator: func(*http.Request) (authz.Principal, error) {
			return authz.Principal{}, mediator.E(mediator.CodeUnauthorized, "token expired")
		}})
		rec := f.do(t, "GET", "/rpc/peek", "")
		if p := problemOf(t, rec); rec.Code != 401 || p.Detail != "token expired" {
			t.Errorf("%d %+v", rec.Code, p)
		}
	})
	t.Run("other mediator codes are still 401", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{Authenticator: func(*http.Request) (authz.Principal, error) {
			return authz.Principal{}, mediator.E(mediator.CodeForbidden, "nope")
		}})
		rec := f.do(t, "GET", "/rpc/peek", "")
		if p := problemOf(t, rec); rec.Code != 401 || p.Detail != "authentication failed" {
			t.Errorf("%d %+v", rec.Code, p)
		}
	})
	t.Run("bearer", func(t *testing.T) {
		var seen string
		auth := httpapi.BearerAuthenticator(func(token string) (authz.Principal, error) {
			seen = token
			if token == "bad" {
				return authz.Principal{}, mediator.E(mediator.CodeUnauthorized, "bad token")
			}
			return authz.Principal{Subject: "sub-" + token}, nil
		})
		req := httptest.NewRequest("GET", "/", nil)
		if p, err := auth(req); err != nil || !p.IsAnonymous() {
			t.Errorf("missing header: %+v %v", p, err)
		}
		req.Header.Set("Authorization", "bearer  tok ")
		if p, err := auth(req); err != nil || p.Subject != "sub-tok" || seen != "tok" {
			t.Errorf("bearer: %+v %v seen %q", p, err, seen)
		}
		req.Header.Set("Authorization", "Bearer bad")
		if _, err := auth(req); mediator.CodeOf(err) != mediator.CodeUnauthorized {
			t.Errorf("bad token: %v", err)
		}
		for _, h := range []string{"Basic abc", "Bearer", "Bearer   ", "Token x"} {
			req.Header.Set("Authorization", h)
			_, err := auth(req)
			if mediator.CodeOf(err) != mediator.CodeUnauthorized || !strings.Contains(err.Error(), "Bearer <token>") {
				t.Errorf("%q: %v", h, err)
			}
		}
	})
}

func TestResponses(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	t.Run("201 with body", func(t *testing.T) {
		rec := f.do(t, "POST", "/orders", `{"customerId":"c1","lines":[{"sku":"a","qty":1}],"when":"2024-05-06T07:08:09Z"}`, "X-Tenant", "t1")
		if rec.Code != 201 {
			t.Fatalf("%d %s", rec.Code, rec.Body)
		}
		v := decodeJSON[createOrderResult](t, rec)
		if v.OrderID != "o-c1" || v.Tenant != "t1" || v.Lines != 1 || !v.When.Equal(time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)) {
			t.Errorf("%+v", v)
		}
		headerOK(t, rec.Header(), "Content-Length", itoa(rec.Body.Len()))
	})
	t.Run("204 for Void", func(t *testing.T) {
		rec := f.do(t, "POST", "/rpc/ping", "")
		if rec.Code != 204 || rec.Body.Len() != 0 || rec.Header().Get("Content-Type") != "" {
			t.Errorf("%d %q %v", rec.Code, rec.Body, rec.Header())
		}
	})
	t.Run("204 declared", func(t *testing.T) {
		rec := f.do(t, "POST", "/quiet", "")
		if rec.Code != 204 || rec.Body.Len() != 0 {
			t.Errorf("%d %q", rec.Code, rec.Body)
		}
	})
	t.Run("200 default", func(t *testing.T) {
		rec := f.do(t, "GET", "/orders/"+uuid.Nil.String(), "")
		if rec.Code != 200 {
			t.Errorf("%d", rec.Code)
		}
	})
	t.Run("HEAD on a GET route", func(t *testing.T) {
		rec := f.do(t, "HEAD", "/orders/"+uuid.Nil.String(), "")
		if rec.Code != 200 {
			t.Errorf("%d", rec.Code)
		}
	})
	t.Run("encode failure is internal", func(t *testing.T) {
		rec := f.do(t, "POST", "/rpc/weird", "")
		p := problemOf(t, rec)
		if rec.Code != 500 || p.Detail != httpapi.InternalDetail {
			t.Errorf("%d %+v", rec.Code, p)
		}
		if attrs, ok := f.log.find(slog.LevelError, "internal error"); !ok || attrs["correlation_id"] != p.CorrelationID || !strings.Contains(attrs["error"], "encode response") {
			t.Errorf("log %v %v", attrs, ok)
		}
	})
	rows := []struct {
		name   string
		body   string
		status int
		code   string
		detail string
	}{
		{"not found", `{"code":"not_found","msg":"order missing"}`, 404, "not_found", "order missing"},
		{"conflict", `{"code":"conflict","msg":"dup"}`, 409, "conflict", "dup"},
		{"validation", `{"mode":"validation"}`, 422, "validation", "2 fields are invalid"},
		{"panic", `{"mode":"panic","msg":"secret"}`, 500, "internal", httpapi.InternalDetail},
		{"plain error", `{"mode":"plain","msg":"secret"}`, 500, "internal", httpapi.InternalDetail},
		{"wrapped internal", `{"mode":"wrapped","code":"internal","msg":"secret"}`, 500, "internal", httpapi.InternalDetail},
		{"wrapped conflict", `{"mode":"wrapped","code":"conflict","msg":"dup"}`, 409, "conflict", "dup"},
		{"context deadline", `{"mode":"ctx"}`, 504, "timeout", "Timeout"},
		{"unknown code", `{"code":"weird_code","msg":"m"}`, 500, "weird_code", "m"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			rec := f.do(t, "POST", "/rpc/fail", row.body)
			p := problemOf(t, rec)
			if rec.Code != row.status || p.Code != row.code || p.Detail != row.detail {
				t.Errorf("%d %+v", rec.Code, p)
			}
			if p.Type != "urn:mediator:error:"+row.code || p.Instance != "/rpc/fail" || p.CorrelationID == "" {
				t.Errorf("%+v", p)
			}
			if strings.Contains(rec.Body.String(), "secret") {
				t.Errorf("cause leaked: %s", rec.Body)
			}
			if row.name == "validation" && len(p.Errors) != 2 {
				t.Errorf("%+v", p.Errors)
			}
		})
	}
	t.Run("panic is logged with its stack", func(t *testing.T) {
		attrs, ok := f.log.find(slog.LevelError, "internal error")
		if !ok {
			t.Fatal("no log")
		}
		found := false
		f.log.mu.Lock()
		for _, r := range f.log.records {
			r.Attrs(func(a slog.Attr) bool {
				if a.Key == "stack" && strings.Contains(a.Value.String(), "goroutine") {
					found = true
				}
				return true
			})
		}
		f.log.mu.Unlock()
		if !found {
			t.Errorf("no stack attr in %v", attrs)
		}
	})
	t.Run("retry after", func(t *testing.T) {
		rec := f.do(t, "POST", "/rpc/fail", `{"code":"idempotency_in_progress","msg":"busy","retryMs":1500}`)
		headerOK(t, rec.Header(), "Retry-After", "2")
		if p := problemOf(t, rec); rec.Code != 409 || p.Details["retry_after_ms"] != float64(1500) {
			t.Errorf("%d %+v", rec.Code, p)
		}
		rec = f.do(t, "POST", "/rpc/fail", `{"code":"rate_limited","msg":"slow down"}`)
		headerOK(t, rec.Header(), "Retry-After", "1")
		rec = f.do(t, "POST", "/rpc/fail", `{"code":"conflict","msg":"x","retryMs":1500}`)
		if rec.Header().Get("Retry-After") != "" {
			t.Error("Retry-After on conflict")
		}
	})
}

func TestUnrouted(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	t.Run("404", func(t *testing.T) {
		rec := f.do(t, "GET", "/nope", "", "X-Correlation-ID", "c-404")
		p := problemOf(t, rec)
		if rec.Code != 404 || p.Code != "not_found" || p.Detail != "no route for GET /nope" || p.Instance != "/nope" || p.CorrelationID != "c-404" {
			t.Errorf("%d %+v", rec.Code, p)
		}
		headerOK(t, rec.Header(), "X-Correlation-ID", "c-404")
		if rec.Header().Get("X-Content-Type-Options") != "" {
			t.Error("plain text headers leaked")
		}
	})
	t.Run("405", func(t *testing.T) {
		rec := f.do(t, "DELETE", "/orders", "")
		p := problemOf(t, rec)
		if rec.Code != 405 || p.Code != "method_not_allowed" || p.Type != "urn:mediator:error:method_not_allowed" || p.Title != "Method not allowed" {
			t.Errorf("%d %+v", rec.Code, p)
		}
		if allow := rec.Header().Get("Allow"); !strings.Contains(allow, "POST") {
			t.Errorf("Allow = %q", allow)
		}
		if !strings.Contains(p.Detail, "DELETE is not allowed for /orders") {
			t.Errorf("detail %q", p.Detail)
		}
		if _, err := uuid.Parse(p.CorrelationID); err != nil {
			t.Errorf("correlation %q", p.CorrelationID)
		}
	})
}

func TestDocsAndHealthMounted(t *testing.T) {
	docs := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/docs":
			w.(http.Flusher).Flush() // legacy assertion works through the wrapper
			_, _ = io.WriteString(w, "<html>")
		case "/api/openapi.json":
			_, _ = io.WriteString(w, "{}")
		default:
			http.NotFound(w, r)
		}
	})
	f := newFixture(t, httpapi.Config{Prefix: "/api", Docs: docs})
	if rec := f.do(t, "GET", "/api/docs", ""); rec.Code != 200 || rec.Body.String() != "<html>" {
		t.Errorf("%d %s", rec.Code, rec.Body)
	}
	if rec := f.do(t, "GET", "/api/openapi.json", ""); rec.Code != 200 || rec.Body.String() != "{}" {
		t.Errorf("%d %s", rec.Code, rec.Body)
	}
	if rec := f.do(t, "GET", "/api/healthz", ""); rec.Code != 200 {
		t.Errorf("%d", rec.Code)
	}
	if rec := f.do(t, "GET", "/api/readyz", ""); rec.Code != 200 {
		t.Errorf("%d", rec.Code)
	}
	// Docs at a wrong prefix path goes through the mux's own 404.
	if rec := f.do(t, "GET", "/docs", ""); rec.Code != 404 {
		t.Errorf("%d", rec.Code)
	}
	// A 404 written by a mounted handler is rendered consistently.
	g := newFixture(t, httpapi.Config{Docs: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })})
	rec := g.do(t, "GET", "/docs", "")
	if p := problemOf(t, rec); rec.Code != 404 || p.Code != "not_found" {
		t.Errorf("%d %+v", rec.Code, p)
	}
	// Without Docs the paths are 404.
	if rec := f.do(t, "GET", "/api/nothing", ""); rec.Code != 404 {
		t.Errorf("%d", rec.Code)
	}
	h := newFixture(t, httpapi.Config{})
	if rec := h.do(t, "GET", "/docs", ""); rec.Code != 404 {
		t.Errorf("%d", rec.Code)
	}
}

func itoa(n int) string { return strconv.Itoa(n) }
