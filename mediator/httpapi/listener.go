package httpapi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	defaultDrain      = 15 * time.Second
	readHeaderTimeout = 10 * time.Second
)

// Listener serves a Server on a TCP address as a mediator.Component. Run
// listens, serves until ctx is canceled, then shuts down gracefully:
// in-flight requests finish within the drain timeout while SSE streams are
// ended at once through their contexts.
type Listener struct {
	s      *Server
	addr   string
	drain  time.Duration
	listen func(network, addr string) (net.Listener, error)

	mu    sync.Mutex
	bound string
	state listenerState
	err   error
}

type listenerState uint8

const (
	listenerIdle listenerState = iota
	listenerServing
	listenerStopped
)

// NewListener returns a Listener for addr (":0" picks a free port; see Addr)
// with the given drain timeout (default 15 s when zero).
func NewListener(s *Server, addr string, drain time.Duration) *Listener {
	if drain <= 0 {
		drain = defaultDrain
	}
	return &Listener{s: s, addr: addr, drain: drain, listen: net.Listen}
}

// Run listens and serves until ctx is canceled, then calls Shutdown with
// the drain timeout. It returns nil after a clean shutdown, the listen or
// serve error when serving failed, or the drain error when in-flight
// requests did not finish in time (the server is then closed forcibly).
func (l *Listener) Run(ctx context.Context) error {
	ln, err := l.listen("tcp", l.addr)
	if err != nil {
		l.setState(listenerStopped, "", err)
		return fmt.Errorf("httpapi: listen %s: %w", l.addr, err)
	}
	base, cancelBase := context.WithCancel(context.Background())
	defer cancelBase()
	draining := make(chan struct{})
	srv := &http.Server{
		Handler:           l.s,
		ReadHeaderTimeout: readHeaderTimeout,
		BaseContext:       func(net.Listener) context.Context { return withDrainSignal(base, draining) },
		ErrorLog:          slog.NewLogLogger(l.s.logger.Handler(), slog.LevelWarn),
	}
	srv.RegisterOnShutdown(func() { close(draining) })
	l.setState(listenerServing, ln.Addr().String(), nil)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case err := <-served:
		l.setState(listenerStopped, "", err)
		return fmt.Errorf("httpapi: serve %s: %w", l.addr, err)
	case <-ctx.Done():
	}
	l.setState(listenerStopped, "", errors.New("httpapi: listener stopped"))
	shutdownCtx, cancel := context.WithTimeout(context.Background(), l.drain)
	defer cancel()
	err = srv.Shutdown(shutdownCtx)
	cancelBase()
	<-served
	if err != nil {
		_ = srv.Close()
		return fmt.Errorf("httpapi: drain did not finish within %s: %w", l.drain, err)
	}
	return nil
}

// Healthy returns nil while the listener is serving.
func (l *Listener) Healthy() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	switch l.state {
	case listenerServing:
		return nil
	case listenerStopped:
		return l.err
	default:
		return errors.New("httpapi: listener not started")
	}
}

// Addr returns the bound address once Run is listening, or "" before that.
// With ":0" it carries the port the kernel chose.
func (l *Listener) Addr() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bound
}

func (l *Listener) setState(s listenerState, bound string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state, l.err = s, err
	if s == listenerServing {
		l.bound = bound
	}
}
