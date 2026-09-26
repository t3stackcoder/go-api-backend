package mediator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// Component is anything with a goroutine: it runs until ctx is canceled and
// reports readiness through Healthy.
type Component interface {
	Run(ctx context.Context) error
	Healthy() error
}

// Runtime composes the components of a node and enforces the shutdown order:
// stop accepting HTTP, stop accepting remote requests, wait for in-flight
// requests up to DrainTimeout, stop consumers after the current message is
// acknowledged, stop the relay after the current batch is marked, stop the
// reply reader and janitor, close pools.
//
// Any nil component is skipped. A component that returns a non-nil error
// before shutdown begins stops the whole runtime.
type Runtime struct {
	HTTP         Component
	RemoteServer Component
	Consumers    Component
	Relay        Component
	ReplyReader  Component
	Janitor      Component
	// Extra components stop together with Consumers.
	Extra []Component
	// Closers are closed last, in order (pools, clients).
	Closers []io.Closer
	// DrainTimeout bounds each shutdown stage. Default 15 s.
	DrainTimeout time.Duration
	Logger       *slog.Logger
}

type running struct {
	name   string
	c      Component
	cancel context.CancelFunc
	done   chan error
}

// Run starts every component and blocks until ctx is canceled or a component
// fails. On cancellation it shuts down in stages and returns nil when every
// stage completed within DrainTimeout; otherwise it returns the joined errors.
func (r *Runtime) Run(ctx context.Context) error {
	logger := r.Logger
	if logger == nil {
		logger = slog.Default().WithGroup("mediator")
	}
	drain := r.DrainTimeout
	if drain <= 0 {
		drain = 15 * time.Second
	}
	stages := [][]struct {
		name string
		c    Component
	}{
		{{"http", r.HTTP}},
		{{"remote_server", r.RemoteServer}},
		{{"consumers", r.Consumers}},
		{{"relay", r.Relay}},
		{{"reply_reader", r.ReplyReader}, {"janitor", r.Janitor}},
	}
	for i, e := range r.Extra {
		stages[2] = append(stages[2], struct {
			name string
			c    Component
		}{fmt.Sprintf("extra_%d", i), e})
	}

	// failure is the first component to fail or exit early; its run is
	// remembered so the drain below does not report the same error twice.
	type failure struct {
		run *running
		err error
	}
	var byStage [][]*running
	failed := make(chan failure, 16)
	var wg sync.WaitGroup
	for _, stage := range stages {
		var rs []*running
		for _, s := range stage {
			if s.c == nil {
				continue
			}
			cctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			run := &running{name: s.name, c: s.c, cancel: cancel, done: make(chan error, 1)}
			rs = append(rs, run)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					if v := recover(); v != nil {
						err := fmt.Errorf("component %s: %w", run.name, recovered(v))
						run.done <- err
						failed <- failure{run, err}
					}
				}()
				err := run.c.Run(cctx)
				run.done <- err
				if err != nil && cctx.Err() == nil {
					failed <- failure{run, fmt.Errorf("component %s: %w", run.name, err)}
				} else if err == nil && cctx.Err() == nil {
					failed <- failure{run, fmt.Errorf("component %s exited early", run.name)}
				}
			}()
		}
		byStage = append(byStage, rs)
	}

	var cause failure
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case cause = <-failed:
		logger.Error("component failed; shutting down", "error", cause.err)
	}

	var errs []error
	if cause.err != nil {
		errs = append(errs, cause.err)
	}
	for _, stage := range byStage {
		for _, run := range stage {
			run.cancel()
		}
		timer := time.NewTimer(drain)
		for _, run := range stage {
			select {
			case err := <-run.done:
				// The component that triggered the shutdown is already in errs as the cause.
				if err != nil && run != cause.run && !errors.Is(err, context.Canceled) {
					errs = append(errs, fmt.Errorf("component %s: %w", run.name, err))
				}
				logger.Info("component stopped", "component", run.name)
			case <-timer.C:
				errs = append(errs, fmt.Errorf("component %s did not stop within %s", run.name, drain))
				timer.Reset(0)
			}
		}
		timer.Stop()
	}
	for _, c := range r.Closers {
		if c == nil {
			continue
		}
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	// Do not wait for components that ignored cancellation; they were reported.
	go wg.Wait()
	return errors.Join(errs...)
}

// Healthy returns nil when every component is healthy.
func (r *Runtime) Healthy() error {
	var errs []error
	check := func(name string, c Component) {
		if c == nil {
			return
		}
		if err := c.Healthy(); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	check("http", r.HTTP)
	check("remote_server", r.RemoteServer)
	check("consumers", r.Consumers)
	check("relay", r.Relay)
	check("reply_reader", r.ReplyReader)
	check("janitor", r.Janitor)
	for i, e := range r.Extra {
		check(fmt.Sprintf("extra_%d", i), e)
	}
	return errors.Join(errs...)
}

// ComponentFunc adapts a function to Component with an always-healthy status.
type ComponentFunc func(ctx context.Context) error

func (f ComponentFunc) Run(ctx context.Context) error { return f(ctx) }
func (ComponentFunc) Healthy() error                  { return nil }
