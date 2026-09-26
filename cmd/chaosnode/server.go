package main

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

// httpComponent serves one http.Handler on a TCP address as a
// mediator.Component: the node's admin endpoints under /chaos/ and the
// workload routes of the active mediator share this listener. Shutdown
// drains in-flight requests up to the drain timeout (G16).
type httpComponent struct {
	addr    string
	handler http.Handler
	drain   time.Duration
	logger  *slog.Logger

	mu      sync.Mutex
	serving bool
	err     error
}

func newHTTPComponent(addr string, h http.Handler, drain time.Duration, logger *slog.Logger) *httpComponent {
	if drain <= 0 {
		drain = 5 * time.Second
	}
	return &httpComponent{addr: addr, handler: h, drain: drain, logger: logger}
}

// Run listens and serves until ctx is done, then drains.
func (c *httpComponent) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", c.addr)
	if err != nil {
		c.set(false, err)
		return fmt.Errorf("chaosnode: listen %s: %w", c.addr, err)
	}
	srv := &http.Server{
		Handler:           c.handler,
		ReadHeaderTimeout: 10 * time.Second,
		ErrorLog:          slog.NewLogLogger(c.logger.Handler(), slog.LevelWarn),
	}
	c.set(true, nil)
	c.logger.Info("http listening", "addr", ln.Addr().String())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ln) }()
	select {
	case err := <-served:
		c.set(false, err)
		return fmt.Errorf("chaosnode: serve: %w", err)
	case <-ctx.Done():
	}
	c.set(false, errors.New("chaosnode: http stopped"))
	sctx, cancel := context.WithTimeout(context.Background(), c.drain)
	defer cancel()
	err = srv.Shutdown(sctx)
	<-served
	if err != nil {
		_ = srv.Close()
		return fmt.Errorf("chaosnode: http drain did not finish within %s: %w", c.drain, err)
	}
	return nil
}

// Healthy is nil while serving.
func (c *httpComponent) Healthy() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.serving {
		return nil
	}
	if c.err != nil {
		return c.err
	}
	return errors.New("chaosnode: http not started")
}

func (c *httpComponent) set(serving bool, err error) {
	c.mu.Lock()
	c.serving, c.err = serving, err
	c.mu.Unlock()
}
