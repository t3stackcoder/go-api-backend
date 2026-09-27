package httpapi_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
)

func TestHealth(t *testing.T) {
	t.Run("healthz", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{ReadyChecks: []func(context.Context) error{func(context.Context) error { return errors.New("down") }}})
		rec := f.do(t, "GET", "/healthz", "")
		if rec.Code != 200 || rec.Body.String() != `{"status":"ok"}` {
			t.Errorf("%d %s", rec.Code, rec.Body)
		}
	})
	t.Run("readyz without checks", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{})
		rec := f.do(t, "GET", "/readyz", "")
		if rec.Code != 200 || rec.Body.String() != `{"status":"ok"}` {
			t.Errorf("%d %s", rec.Code, rec.Body)
		}
	})
	t.Run("readyz with passing checks", func(t *testing.T) {
		var calls atomic.Int32 // the checks run concurrently
		ok := func(context.Context) error { calls.Add(1); return nil }
		f := newFixture(t, httpapi.Config{ReadyChecks: []func(context.Context) error{ok, ok}})
		if rec := f.do(t, "GET", "/readyz", ""); rec.Code != 200 {
			t.Errorf("%d %s", rec.Code, rec.Body)
		}
		if calls.Load() != 2 {
			t.Errorf("calls %d", calls.Load())
		}
	})
	t.Run("readyz lists failures", func(t *testing.T) {
		checks := []func(context.Context) error{
			func(context.Context) error { return nil },
			func(context.Context) error { return errors.New("postgres: connection refused") },
			func(context.Context) error { return errors.New("lag 90s") },
		}
		f := newFixture(t, httpapi.Config{ReadyChecks: checks})
		rec := f.do(t, "GET", "/readyz", "")
		p := problemOf(t, rec)
		if rec.Code != 503 || p.Code != "unavailable" || p.Detail != "2 of 3 readiness checks failed" {
			t.Errorf("%d %+v", rec.Code, p)
		}
		failures, _ := p.Details["failures"].([]any)
		if len(failures) != 2 || failures[0] != "check 1: postgres: connection refused" || failures[1] != "check 2: lag 90s" {
			t.Errorf("%v", p.Details)
		}
	})
	t.Run("readyz bounds each check by 2s", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hang := func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }
			f := newFixture(t, httpapi.Config{ReadyChecks: []func(context.Context) error{hang, hang}})
			start := time.Now()
			rec := f.do(t, "GET", "/readyz", "")
			if time.Since(start) != 2*time.Second {
				t.Errorf("took %s", time.Since(start))
			}
			if p := problemOf(t, rec); rec.Code != 503 || !strings.Contains(rec.Body.String(), "deadline exceeded") || p.Detail != "2 of 2 readiness checks failed" {
				t.Errorf("%d %s", rec.Code, rec.Body)
			}
		})
	})
	t.Run("standalone handler", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{Prefix: "/api", ReadyChecks: []func(context.Context) error{func(context.Context) error { return errors.New("x") }}})
		h := f.srv.HealthHandler()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
		if rec.Code != 200 {
			t.Errorf("%d", rec.Code)
		}
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/readyz", nil))
		if rec.Code != 503 || rec.Header().Get("Content-Type") != httpapi.ProblemContentType {
			t.Errorf("%d %v", rec.Code, rec.Header())
		}
		rec = httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/healthz", nil))
		if rec.Code != 404 {
			t.Errorf("%d", rec.Code)
		}
	})
}
