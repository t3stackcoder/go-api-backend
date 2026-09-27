package httpapi_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
)

func TestWriteProblem_Golden(t *testing.T) {
	ve := &mediator.ValidationError{Fields: []mediator.FieldError{
		{Path: "/lines/0/qty", Rule: "min", Message: "must be at least 1"},
		{Path: "/customerId", Rule: "uuid", Message: "must be a UUID"},
	}}
	req := httptest.NewRequest("POST", "/orders?x=1", nil)
	req = req.WithContext(mediator.WithCorrelationID(req.Context(), "0192e1b4-abc"))
	rec := httptest.NewRecorder()
	httpapi.WriteProblem(rec, req, ve)
	want := `{"type":"urn:mediator:error:validation","title":"Validation failed","status":422,"detail":"2 fields are invalid","instance":"/orders","correlationId":"0192e1b4-abc","code":"validation","errors":[{"path":"/lines/0/qty","rule":"min","message":"must be at least 1"},{"path":"/customerId","rule":"uuid","message":"must be a UUID"}]}`
	if rec.Body.String() != want {
		t.Errorf("body\n got %s\nwant %s", rec.Body.String(), want)
	}
	if rec.Code != 422 || rec.Header().Get("Content-Type") != "application/problem+json" || rec.Header().Get("X-Correlation-ID") != "0192e1b4-abc" {
		t.Errorf("%d %v", rec.Code, rec.Header())
	}
	headerOK(t, rec.Header(), "Content-Length", itoa(len(want)))
}

func TestWriteProblem_Internal(t *testing.T) {
	sink := &logSink{}
	req := httptest.NewRequest("GET", "/x", nil)
	ctx := mediator.WithCorrelationID(req.Context(), "corr-1")
	f := newFixture(t, httpapi.Config{Logger: slog.New(sink)})
	_ = f
	// Outside a server the default logger is used; inside, the server logger.
	rec := httptest.NewRecorder()
	httpapi.WriteProblem(rec, req.WithContext(ctx), mediator.Wrap(mediator.CodeInternal, "secret message", errors.New("db password")))
	want := `{"type":"urn:mediator:error:internal","title":"Internal error","status":500,"detail":"An internal error occurred","instance":"/x","correlationId":"corr-1","code":"internal"}`
	if rec.Body.String() != want {
		t.Errorf("body\n got %s\nwant %s", rec.Body.String(), want)
	}
	// Through the server, the configured logger receives the cause.
	rec = f.do(t, "POST", "/rpc/fail", `{"mode":"wrapped","code":"internal","msg":"secret message"}`, "X-Correlation-ID", "corr-2")
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "secret") {
		t.Errorf("%d %s", rec.Code, rec.Body)
	}
	attrs, ok := sink.find(slog.LevelError, "internal error")
	if !ok || attrs["correlation_id"] != "corr-2" || attrs["instance"] != "/rpc/fail" || !strings.Contains(attrs["error"], "cause: secret message") {
		t.Errorf("log attrs %v (%v)", attrs, ok)
	}
}

func TestProblemOf(t *testing.T) {
	rows := []struct {
		name    string
		err     error
		status  int
		code    string
		detail  string
		errors  int
		detail0 map[string]any
	}{
		{"nil", nil, 500, "internal", httpapi.InternalDetail, 0, nil},
		{"plain", errors.New("x"), 500, "internal", httpapi.InternalDetail, 0, nil},
		{"internal with details hides them", mediator.E(mediator.CodeInternal, "m").WithDetail("k", "v"), 500, "internal", httpapi.InternalDetail, 0, nil},
		{"message", mediator.E(mediator.CodeNotFound, "order missing"), 404, "not_found", "order missing", 0, nil},
		{"details", mediator.E(mediator.CodeConflict, "dup").WithDetail("orderId", "o1"), 409, "conflict", "dup", 0, map[string]any{"orderId": "o1"}},
		{"empty message falls back to title", mediator.E(mediator.CodeForbidden, ""), 403, "forbidden", "Forbidden", 0, nil},
		{"validation", (&mediator.ValidationError{}).Add("/a", "r", "m"), 422, "validation", "1 field is invalid", 1, nil},
		{"validation empty", &mediator.ValidationError{}, 422, "validation", "Validation failed", 0, nil},
		{"validation empty non-nil fields", &mediator.ValidationError{Fields: []mediator.FieldError{}}, 422, "validation", "Validation failed", 0, nil},
		{"empty details map", &mediator.Error{Code: mediator.CodeConflict, Message: "dup", Details: map[string]any{}}, 409, "conflict", "dup", 0, nil},
		{"validation wrapped keeps message", mediator.Wrap(mediator.CodeValidation, "invalid order", (&mediator.ValidationError{}).Add("/a", "r", "m").Add("/b", "r", "m")), 422, "validation", "invalid order", 2, nil},
		{"validation code without fields", mediator.E(mediator.CodeValidation, "custom"), 422, "validation", "custom", 0, nil},
		{"deadline", context.DeadlineExceeded, 504, "timeout", "Timeout", 0, nil},
		{"canceled", context.Canceled, 504, "timeout", "Timeout", 0, nil},
		{"handler not found", mediator.ErrHandlerNotFound, 501, "handler_not_found", "Handler not found", 0, nil},
		{"unavailable", mediator.E(mediator.CodeUnavailable, "down"), 503, "unavailable", "down", 0, nil},
		{"busy", mediator.E(mediator.CodeIdempotencyBusy, "busy"), 409, "idempotency_in_progress", "busy", 0, nil},
		{"mismatch", mediator.E(mediator.CodeIdempotencyMismatch, "m"), 422, "idempotency_mismatch", "m", 0, nil},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			p := httpapi.ProblemOf(row.err, "/i", "c")
			if p.Status != row.status || p.Code != row.code || p.Detail != row.detail || len(p.Errors) != row.errors {
				t.Errorf("%+v", p)
			}
			if p.Type != "urn:mediator:error:"+row.code || p.Title != mediator.Code(row.code).Title() || p.Instance != "/i" || p.CorrelationID != "c" {
				t.Errorf("%+v", p)
			}
			if row.detail0 == nil && p.Details != nil {
				t.Errorf("unexpected details %v", p.Details)
			}
			if row.errors == 0 && p.Errors != nil {
				t.Errorf("errors must be absent, not empty: %#v", p.Errors)
			}
			for k, v := range row.detail0 {
				if p.Details[k] != v {
					t.Errorf("details[%s] = %v", k, p.Details[k])
				}
			}
			// Marshaled body always has the required members.
			rec := httptest.NewRecorder()
			httpapi.WriteProblem(rec, httptest.NewRequest("GET", "/i", nil), row.err)
			for _, member := range []string{`"type":`, `"title":`, `"status":`, `"detail":`, `"instance":"/i"`, `"code":"` + row.code + `"`} {
				if !strings.Contains(rec.Body.String(), member) {
					t.Errorf("body %s lacks %s", rec.Body.String(), member)
				}
			}
		})
	}
}

func TestRetryAfter(t *testing.T) {
	rows := []struct {
		name string
		err  error
		want string
	}{
		{"busy 1500ms", mediator.E(mediator.CodeIdempotencyBusy, "b").WithDetail("retry_after_ms", 1500), "2"},
		{"busy exact seconds", mediator.E(mediator.CodeIdempotencyBusy, "b").WithDetail("retry_after_ms", int64(60000)), "60"},
		{"duration", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", 250*time.Millisecond), "1"},
		{"duration 2.5s", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", 2500*time.Millisecond), "3"},
		{"float", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", 1000.5), "2"},
		{"float32", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", float32(999)), "1"},
		{"tiny", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", 1), "1"},
		{"zero", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", 0), "1"},
		{"negative", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", -5000), "1"},
		{"missing", mediator.E(mediator.CodeRateLimited, "r"), "1"},
		{"string ignored", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", "3000"), "1"},
		{"int8", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", int8(100)), "1"},
		{"int16", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", int16(3000)), "3"},
		{"int32", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", int32(3001)), "4"},
		{"uint", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", uint(4000)), "4"},
		{"uint8", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", uint8(200)), "1"},
		{"uint16", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", uint16(5000)), "5"},
		{"uint32", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", uint32(6000)), "6"},
		{"uint64", mediator.E(mediator.CodeRateLimited, "r").WithDetail("retry_after_ms", uint64(7000)), "7"},
		{"wrapped busy", mediator.Wrap(mediator.CodeIdempotencyBusy, "b", errors.New("lock")).WithDetail("retry_after_ms", 999), "1"},
		{"not applicable", mediator.E(mediator.CodeConflict, "c").WithDetail("retry_after_ms", 5000), ""},
		{"plain", errors.New("x"), ""},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			httpapi.WriteProblem(rec, httptest.NewRequest("GET", "/", nil), row.err)
			if got := rec.Header().Get("Retry-After"); got != row.want {
				t.Errorf("Retry-After = %q, want %q", got, row.want)
			}
		})
	}
}

func TestWriteProblem_Edges(t *testing.T) {
	t.Run("unencodable details are dropped", func(t *testing.T) {
		rec := httptest.NewRecorder()
		err := mediator.E(mediator.CodeConflict, "dup").WithDetail("f", func() {})
		httpapi.WriteProblem(rec, httptest.NewRequest("GET", "/", nil), err)
		if rec.Code != 409 || strings.Contains(rec.Body.String(), "details") {
			t.Errorf("%d %s", rec.Code, rec.Body)
		}
	})
	t.Run("correlation from request header outside a server", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-Correlation-ID", "from-req")
		httpapi.WriteProblem(rec, req, mediator.E(mediator.CodeNotFound, "x"))
		headerOK(t, rec.Header(), "X-Correlation-ID", "from-req")
		if !strings.Contains(rec.Body.String(), `"correlationId":"from-req"`) {
			t.Errorf("%s", rec.Body)
		}
	})
	t.Run("correlation generated outside a server", func(t *testing.T) {
		rec := httptest.NewRecorder()
		httpapi.WriteProblem(rec, httptest.NewRequest("GET", "/", nil), mediator.E(mediator.CodeNotFound, "x"))
		if _, err := uuid.Parse(rec.Header().Get("X-Correlation-ID")); err != nil {
			t.Errorf("%q", rec.Header().Get("X-Correlation-ID"))
		}
	})
	t.Run("correlation already on the response wins", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rec.Header().Set("X-Correlation-ID", "resp")
		httpapi.WriteProblem(rec, httptest.NewRequest("GET", "/", nil), mediator.E(mediator.CodeNotFound, "x"))
		if !strings.Contains(rec.Body.String(), `"correlationId":"resp"`) {
			t.Errorf("%s", rec.Body)
		}
	})
}
