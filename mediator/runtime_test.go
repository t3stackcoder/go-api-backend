package mediator_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type syncLog struct {
	mu sync.Mutex
	e  []string
}

func (l *syncLog) add(s string) {
	l.mu.Lock()
	l.e = append(l.e, s)
	l.mu.Unlock()
}

func (l *syncLog) entries() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.e...)
}

func (l *syncLog) index(s string) int {
	for i, e := range l.entries() {
		if e == s {
			return i
		}
	}
	return -1
}

// fakeComp is a Component with configurable behavior.
type fakeComp struct {
	name      string
	log       *syncLog
	healthy   error
	runErr    error         // returned immediately, before any shutdown
	exitEarly bool          // return nil immediately
	release   chan struct{} // when set, ignore ctx and wait for this instead
	panics    bool
	stopErr   error // returned after ctx is canceled instead of ctx.Err()
}

func (c *fakeComp) Run(ctx context.Context) error {
	c.log.add(c.name + ":start")
	switch {
	case c.panics:
		panic("boom in " + c.name)
	case c.runErr != nil:
		return c.runErr
	case c.exitEarly:
		return nil
	case c.release != nil:
		<-c.release
		c.log.add(c.name + ":stop")
		return nil
	}
	<-ctx.Done()
	c.log.add(c.name + ":stop")
	if c.stopErr != nil {
		return c.stopErr
	}
	return ctx.Err()
}

func (c *fakeComp) Healthy() error { return c.healthy }

type fakeCloser struct {
	name string
	log  *syncLog
	err  error
}

func (c fakeCloser) Close() error {
	c.log.add("close:" + c.name)
	return c.err
}

var quiet = slog.New(slog.DiscardHandler)

// runUntilStarted starts rt in the background, waits for every component to
// block, cancels, and returns Run's result.
func runAndCancel(t *testing.T, rt *mediator.Runtime) error {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()
	synctest.Wait()
	cancel()
	return <-done
}

func TestRuntime_StageOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &syncLog{}
		comp := func(n string) *fakeComp { return &fakeComp{name: n, log: log} }
		rt := &mediator.Runtime{
			HTTP: comp("http"), RemoteServer: comp("remote"), Consumers: comp("consumers"),
			Relay: comp("relay"), ReplyReader: comp("reply"), Janitor: comp("janitor"),
			Extra:   []mediator.Component{comp("extra0"), nil, comp("extra1")},
			Closers: []io.Closer{fakeCloser{"pool", log, nil}, nil, fakeCloser{"redis", log, nil}},
			Logger:  quiet,
		}
		if err := runAndCancel(t, rt); err != nil {
			t.Fatal(err)
		}
		stages := [][]string{{"http"}, {"remote"}, {"consumers", "extra0", "extra1"}, {"relay"}, {"reply", "janitor"}}
		last := -1
		for _, stage := range stages {
			first, max := len(log.e), -1
			for _, n := range stage {
				i := log.index(n + ":stop")
				if i < 0 {
					t.Fatalf("%s never stopped: %v", n, log.entries())
				}
				first, max = min(first, i), max2(max, i)
			}
			if first <= last {
				t.Fatalf("stage %v stopped before the previous stage finished: %v", stage, log.entries())
			}
			last = max
		}
		e := log.entries()
		if e[len(e)-2] != "close:pool" || e[len(e)-1] != "close:redis" {
			t.Fatalf("closers must run last, in order: %v", e)
		}
		if n := strings.Count(strings.Join(e, ","), ":start"); n != 8 {
			t.Fatalf("%d components started", n)
		}
	})
}

func max2(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func TestRuntime_ComponentFailureStopsEverything(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &syncLog{}
		rt := &mediator.Runtime{
			HTTP:      &fakeComp{name: "http", log: log},
			Consumers: &fakeComp{name: "consumers", log: log, runErr: errors.New("lease lost")},
			Relay:     &fakeComp{name: "relay", log: log, stopErr: errors.New("flush failed")},
			Janitor:   &fakeComp{name: "janitor", log: log},
			Logger:    quiet,
		}
		err := rt.Run(context.Background())
		wantContains(t, err, "component consumers: lease lost", "component relay: flush failed")
		if n := strings.Count(err.Error(), "lease lost"); n != 1 {
			t.Fatalf("the cause must be reported once, got %d: %v", n, err)
		}
		if strings.Contains(err.Error(), "http") || strings.Contains(err.Error(), "janitor") {
			t.Fatalf("clean stops must not be reported: %v", err)
		}
		for _, n := range []string{"http", "relay", "janitor"} {
			if log.index(n+":stop") < 0 {
				t.Fatalf("%s not stopped: %v", n, log.entries())
			}
		}
	})
}

func TestRuntime_EarlyExitAndPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &syncLog{}
		rt := &mediator.Runtime{
			HTTP:   &fakeComp{name: "http", log: log, exitEarly: true},
			Relay:  &fakeComp{name: "relay", log: log},
			Logger: quiet,
		}
		wantContains(t, rt.Run(context.Background()), "component http exited early")
		if log.index("relay:stop") < 0 {
			t.Fatal("relay not stopped")
		}
	})
	synctest.Test(t, func(t *testing.T) {
		log := &syncLog{}
		rt := &mediator.Runtime{
			RemoteServer: &fakeComp{name: "remote", log: log, panics: true},
			Relay:        &fakeComp{name: "relay", log: log},
			Logger:       quiet,
		}
		err := rt.Run(context.Background())
		wantContains(t, err, "component remote_server:", "boom in remote")
		if n := strings.Count(err.Error(), "boom in remote"); n != 1 {
			t.Fatalf("the panic must be reported once, got %d: %v", n, err)
		}
		var pe *mediator.PanicError
		if !errors.As(err, &pe) {
			t.Fatalf("want PanicError in %v", err)
		}
		if log.index("relay:stop") < 0 {
			t.Fatal("relay not stopped")
		}
	})
}

func TestRuntime_DrainTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &syncLog{}
		release := make(chan struct{})
		rt := &mediator.Runtime{
			HTTP:         &fakeComp{name: "http", log: log},
			ReplyReader:  &fakeComp{name: "reply", log: log, release: release},
			Janitor:      &fakeComp{name: "janitor", log: log, release: release},
			Closers:      []io.Closer{fakeCloser{"pool", log, errors.New("close failed")}},
			DrainTimeout: 100 * time.Millisecond,
			Logger:       quiet,
		}
		start := time.Now()
		err := runAndCancel(t, rt)
		wantContains(t, err, "component reply_reader did not stop within 100ms", "component janitor did not stop within 100ms", "close failed")
		if el := time.Since(start); el != 100*time.Millisecond {
			t.Fatalf("waited %v, want exactly one drain timeout for the stage", el)
		}
		if log.index("close:pool") < 0 || log.index("http:stop") < 0 {
			t.Fatalf("closers must still run: %v", log.entries())
		}
		close(release) // let the stuck components exit so the bubble can end
	})
	// Zero DrainTimeout and nil Logger use the defaults (15 s, slog.Default).
	synctest.Test(t, func(t *testing.T) {
		log := &syncLog{}
		release := make(chan struct{})
		rt := &mediator.Runtime{Janitor: &fakeComp{name: "janitor", log: log, release: release}}
		start := time.Now()
		err := runAndCancel(t, rt)
		wantContains(t, err, "did not stop within 15s")
		if el := time.Since(start); el != 15*time.Second {
			t.Fatalf("waited %v", el)
		}
		close(release)
	})
}

func TestRuntime_Healthy(t *testing.T) {
	log := &syncLog{}
	rt := &mediator.Runtime{
		HTTP:      &fakeComp{name: "http", log: log},
		Consumers: &fakeComp{name: "consumers", log: log, healthy: errors.New("no lease")},
		Janitor:   &fakeComp{name: "janitor", log: log, healthy: errors.New("behind")},
		Extra:     []mediator.Component{nil, &fakeComp{name: "x", log: log, healthy: errors.New("extra sick")}, mediator.ComponentFunc(nil)},
	}
	err := rt.Healthy()
	wantContains(t, err, "consumers: no lease", "janitor: behind", "extra_1: extra sick")
	if strings.Contains(err.Error(), "http") {
		t.Fatal(err)
	}
	if (&mediator.Runtime{}).Healthy() != nil {
		t.Fatal("empty runtime is healthy")
	}
	rt.Consumers, rt.Janitor, rt.Extra = nil, nil, nil
	if err := rt.Healthy(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntime_ComponentFunc(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ran := false
		f := mediator.ComponentFunc(func(ctx context.Context) error {
			ran = true
			<-ctx.Done()
			return ctx.Err()
		})
		if f.Healthy() != nil {
			t.Fatal("ComponentFunc is always healthy")
		}
		rt := &mediator.Runtime{Extra: []mediator.Component{f}, Logger: quiet}
		if err := runAndCancel(t, rt); err != nil || !ran {
			t.Fatal(err, ran)
		}
		if err := mediator.ComponentFunc(func(context.Context) error { return errors.New("direct") }).Run(context.Background()); err == nil {
			t.Fatal("Run must call f")
		}
	})
}

func TestRuntime_Empty(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if err := runAndCancel(t, &mediator.Runtime{Logger: quiet}); err != nil {
			t.Fatal(err)
		}
	})
}
