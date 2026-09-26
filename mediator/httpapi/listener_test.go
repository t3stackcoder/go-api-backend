package httpapi_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
)

func waitHealthy(t *testing.T, l *httpapi.Listener) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for l.Healthy() != nil {
		if time.Now().After(deadline) {
			t.Fatalf("listener not healthy: %v", l.Healthy())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestListener_ServeAndShutdown(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	l := httpapi.NewListener(f.srv, "127.0.0.1:0", 0)
	if err := l.Healthy(); err == nil || !strings.Contains(err.Error(), "not started") {
		t.Errorf("Healthy before Run = %v", err)
	}
	if l.Addr() != "" {
		t.Errorf("Addr before Run = %q", l.Addr())
	}
	var c mediator.Component = l
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	waitHealthy(t, l)
	if !strings.HasPrefix(l.Addr(), "127.0.0.1:") || strings.HasSuffix(l.Addr(), ":0") {
		t.Errorf("Addr = %q", l.Addr())
	}
	resp, err := http.Get("http://" + l.Addr() + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != `{"status":"ok"}` {
		t.Errorf("%d %s", resp.StatusCode, body)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	if err := l.Healthy(); err == nil || !strings.Contains(err.Error(), "stopped") {
		t.Errorf("Healthy after stop = %v", err)
	}
}

func TestListener_DrainsInFlight(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	l := httpapi.NewListener(f.srv, "127.0.0.1:0", 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	waitHealthy(t, l)
	type result struct {
		status int
		err    error
	}
	res := make(chan result, 1)
	go func() {
		resp, err := http.Post("http://"+l.Addr()+"/rpc/slow", "", nil)
		if err != nil {
			res <- result{err: err}
			return
		}
		resp.Body.Close()
		res <- result{status: resp.StatusCode}
	}()
	<-f.slowStarted
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned %v while a request was in flight", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(f.slowRelease)
	if r := <-res; r.err != nil || r.status != 204 {
		t.Errorf("in-flight request: %+v", r)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after drain")
	}
}

func TestListener_DrainTimeout(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	l := httpapi.NewListener(f.srv, "127.0.0.1:0", 200*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	waitHealthy(t, l)
	go func() {
		resp, err := http.Post("http://"+l.Addr()+"/rpc/slow", "", nil)
		if err == nil {
			resp.Body.Close()
		}
	}()
	<-f.slowStarted
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "drain did not finish within 200ms") || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	close(f.slowRelease)
}

func TestListener_ShutdownEndsStreams(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	l := httpapi.NewListener(f.srv, "127.0.0.1:0", 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Run(ctx) }()
	waitHealthy(t, l)
	resp, err := http.Get("http://" + l.Addr() + "/rpc/countTo?n=1&block=true")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	br := bufio.NewReader(resp.Body)
	if fr, err := readFrame(br); err != nil || fr.data != "1" {
		t.Fatalf("%+v %v", fr, err)
	}
	<-f.streamBlocked
	cancel()
	fr, err := readFrame(br)
	if err != nil || fr.event != "error" || !strings.Contains(fr.data, `"code":"unavailable"`) || !strings.Contains(fr.data, "shutting down") {
		t.Fatalf("%+v %v", fr, err)
	}
	if _, err := readFrame(br); err != io.EOF {
		t.Errorf("stream did not end: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	<-f.streamCancelled
}

func TestListener_ListenError(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	l := httpapi.NewListener(f.srv, "127.0.0.1:notaport", time.Second)
	err := l.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "listen 127.0.0.1:notaport") {
		t.Fatalf("Run = %v", err)
	}
	if h := l.Healthy(); h == nil {
		t.Error("healthy after listen failure")
	}
}
