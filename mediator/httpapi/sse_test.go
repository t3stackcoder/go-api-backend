package httpapi_test

import (
	"bufio"
	"context"
	"encoding/json/v2"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
)

// frame is one parsed SSE event.
type frame struct {
	comment, event, id, data string
}

// readFrame reads one event (up to the blank line). io.EOF means the stream
// ended cleanly.
func readFrame(br *bufio.Reader) (frame, error) {
	var f frame
	got := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if got && err == io.EOF {
				return f, nil
			}
			return f, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if got {
				return f, nil
			}
			continue
		}
		got = true
		switch {
		case strings.HasPrefix(line, ":"):
			f.comment = strings.TrimSpace(line[1:])
		case strings.HasPrefix(line, "event: "):
			f.event = line[len("event: "):]
		case strings.HasPrefix(line, "id: "):
			f.id = line[len("id: "):]
		case strings.HasPrefix(line, "data: "):
			f.data = line[len("data: "):]
		}
	}
}

type sseConn struct {
	resp   *http.Response
	br     *bufio.Reader
	cancel context.CancelFunc
}

func openStream(t *testing.T, client *http.Client, target string, hdr ...string) *sseConn {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, "GET", "http://example.com"+target, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Accept", "text/event-stream")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := client.Do(req)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	return &sseConn{resp: resp, br: bufio.NewReader(resp.Body), cancel: cancel}
}

func (c *sseConn) close() {
	c.cancel()
	_ = c.resp.Body.Close()
}

func (c *sseConn) next(t *testing.T) frame {
	t.Helper()
	f, err := readFrame(c.br)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	return f
}

func (c *sseConn) end(t *testing.T) {
	t.Helper()
	if f, err := readFrame(c.br); err != io.EOF {
		t.Fatalf("expected end of stream, got %+v %v", f, err)
	}
}

func TestSSE(t *testing.T) {
	t.Run("frames and headers", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{})
			ts := httptest.NewTestServer(t, f.srv)
			defer ts.Close()
			c := openStream(t, ts.Client(), "/rpc/countTo?n=3", "X-Correlation-ID", "sse-1", "Idempotency-Key", "k")
			defer c.close()
			h := c.resp.Header
			if c.resp.StatusCode != 200 {
				t.Fatalf("status %d", c.resp.StatusCode)
			}
			headerOK(t, h, "Content-Type", "text/event-stream")
			headerOK(t, h, "Cache-Control", "no-cache")
			headerOK(t, h, "X-Accel-Buffering", "no")
			headerOK(t, h, "X-Correlation-ID", "sse-1")
			headerOK(t, h, "Warning", `299 - "Idempotency-Key ignored on queries"`)
			for i := 1; i <= 3; i++ {
				if fr := c.next(t); fr.data != itoa(i) || fr.event != "" || fr.id != "" {
					t.Errorf("frame %d = %+v", i, fr)
				}
			}
			c.end(t)
		})
	})
	t.Run("error frame", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{})
			ts := httptest.NewTestServer(t, f.srv)
			defer ts.Close()
			c := openStream(t, ts.Client(), "/rpc/countTo?n=3&failAt=2")
			defer c.close()
			if fr := c.next(t); fr.data != "1" {
				t.Errorf("%+v", fr)
			}
			fr := c.next(t)
			if fr.event != "error" {
				t.Fatalf("%+v", fr)
			}
			var p httpapi.Problem
			if err := json.Unmarshal([]byte(fr.data), &p); err != nil {
				t.Fatal(err)
			}
			if p.Code != "not_found" || p.Status != 404 || p.Detail != "item gone" || p.Instance != "/rpc/countTo" || p.CorrelationID != c.resp.Header.Get("X-Correlation-ID") {
				t.Errorf("%+v", p)
			}
			c.end(t)
		})
	})
	t.Run("panic in the iterator is an internal error frame", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{})
			ts := httptest.NewTestServer(t, f.srv)
			defer ts.Close()
			c := openStream(t, ts.Client(), "/rpc/countTo?n=3&panicAt=2")
			defer c.close()
			c.next(t)
			fr := c.next(t)
			if fr.event != "error" || !strings.Contains(fr.data, httpapi.InternalDetail) || strings.Contains(fr.data, "stream panic") {
				t.Errorf("%+v", fr)
			}
			c.end(t)
			if _, ok := f.log.find(slog.LevelError, "internal error"); !ok {
				t.Error("panic not logged")
			}
		})
	})
	t.Run("keepalive and disconnect", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{})
			ts := httptest.NewTestServer(t, f.srv)
			defer ts.Close()
			start := time.Now()
			c := openStream(t, ts.Client(), "/rpc/countTo?n=1&block=true")
			defer c.close()
			if fr := c.next(t); fr.data != "1" {
				t.Errorf("%+v", fr)
			}
			for i := 1; i <= 3; i++ {
				fr := c.next(t)
				if fr.comment != "keepalive" {
					t.Fatalf("%+v", fr)
				}
				if since := time.Since(start); since != time.Duration(i)*15*time.Second {
					t.Errorf("keepalive %d at %s", i, since)
				}
			}
			<-f.streamBlocked
			c.cancel()
			select {
			case <-f.streamCancelled:
			case <-time.After(time.Minute):
				t.Fatal("handler context not canceled on disconnect")
			}
		})
	})
	t.Run("custom keepalive", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{KeepAlive: time.Second})
			ts := httptest.NewTestServer(t, f.srv)
			defer ts.Close()
			start := time.Now()
			c := openStream(t, ts.Client(), "/rpc/countTo?n=0&block=true")
			defer c.close()
			if fr := c.next(t); fr.comment != "keepalive" || time.Since(start) != time.Second {
				t.Errorf("%+v at %s", fr, time.Since(start))
			}
			c.cancel()
			<-f.streamCancelled
		})
	})
	t.Run("last event id and id lines", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{})
			ts := httptest.NewTestServer(t, f.srv)
			defer ts.Close()
			c := openStream(t, ts.Client(), "/tail?n=2", "Last-Event-ID", "5")
			defer c.close()
			for i := 6; i <= 7; i++ {
				fr := c.next(t)
				if fr.id != itoa(i) || fr.data != `{"id":"`+itoa(i)+`","value":`+itoa(i)+`}` {
					t.Errorf("%+v", fr)
				}
			}
			c.end(t)
		})
	})
	t.Run("item encode failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{})
			ts := httptest.NewTestServer(t, f.srv)
			defer ts.Close()
			c := openStream(t, ts.Client(), "/tail?n=1&bad=true")
			defer c.close()
			if fr := c.next(t); fr.event != "error" || !strings.Contains(fr.data, `"code":"internal"`) {
				t.Errorf("%+v", fr)
			}
			c.end(t)
		})
	})
	t.Run("binding error before the stream opens", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{})
			ts := httptest.NewTestServer(t, f.srv)
			defer ts.Close()
			c := openStream(t, ts.Client(), "/tail?n=x")
			defer c.close()
			if c.resp.StatusCode != 422 || c.resp.Header.Get("Content-Type") != httpapi.ProblemContentType {
				t.Errorf("%d %v", c.resp.StatusCode, c.resp.Header)
			}
		})
	})
	t.Run("stream ends after the client leaves mid-frame", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{})
			ts := httptest.NewTestServer(t, f.srv)
			defer ts.Close()
			c := openStream(t, ts.Client(), "/rpc/countTo?n=1000000")
			c.next(t)
			c.close()
			synctest.Wait()
		})
	})
}

// noFlush hides the flusher of the recorder.
type noFlush struct{ http.ResponseWriter }

// failWriter fails every write after the headers.
type failWriter struct {
	hdr    http.Header
	status int
	writes int
}

func (w *failWriter) Header() http.Header  { return w.hdr }
func (w *failWriter) WriteHeader(code int) { w.status = code }
func (w *failWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, errors.New("broken pipe")
}
func (w *failWriter) FlushError() error { return nil }

func TestSSE_WriterFailures(t *testing.T) {
	t.Run("no flusher", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{})
		rec := httptest.NewRecorder()
		f.srv.ServeHTTP(&noFlush{rec}, httptest.NewRequest("GET", "/rpc/countTo?n=2", nil))
		if rec.Code != 200 || rec.Body.Len() != 0 {
			t.Errorf("%d %q", rec.Code, rec.Body)
		}
		if _, ok := f.log.find(slog.LevelWarn, "sse: cannot flush response"); !ok {
			t.Error("missing warning")
		}
	})
	t.Run("data write fails", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{})
		w := &failWriter{hdr: http.Header{}}
		f.srv.ServeHTTP(w, httptest.NewRequest("GET", "/rpc/countTo?n=5", nil))
		if w.status != 200 || w.writes != 1 {
			t.Errorf("status %d writes %d", w.status, w.writes)
		}
	})
	t.Run("keepalive write fails", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			f := newFixture(t, httpapi.Config{})
			w := &failWriter{hdr: http.Header{}}
			start := time.Now()
			f.srv.ServeHTTP(w, httptest.NewRequest("GET", "/rpc/countTo?n=0&block=true", nil))
			if w.writes != 1 || time.Since(start) != 15*time.Second {
				t.Errorf("writes %d after %s", w.writes, time.Since(start))
			}
			<-f.streamCancelled
		})
	})
	t.Run("error frame write fails", func(t *testing.T) {
		f := newFixture(t, httpapi.Config{})
		w := &failWriter{hdr: http.Header{}}
		f.srv.ServeHTTP(w, httptest.NewRequest("GET", "/rpc/countTo?n=1&failAt=1", nil))
		if w.writes != 1 {
			t.Errorf("writes %d", w.writes)
		}
	})
}
