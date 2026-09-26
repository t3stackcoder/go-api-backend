package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// switchable wraps a restartable component so that an admin endpoint can
// stop and start it while the runtime keeps running: the relay for the
// relay-kill nemesis and the remote server for handler-rotate (spec 11.6).
// It is itself a mediator.Component that returns only when its context
// ends; while switched off it reports healthy, because being off is the
// operator's intent.
type switchable struct {
	name   string
	inner  mediator.Component
	logger *slog.Logger

	mu      sync.Mutex
	want    bool
	changed chan struct{}

	running atomic.Bool
	starts  atomic.Int64
}

func newSwitchable(name string, inner mediator.Component, on bool, logger *slog.Logger) *switchable {
	return &switchable{name: name, inner: inner, logger: logger, want: on, changed: make(chan struct{}, 1)}
}

// Set switches the component on or off.
func (s *switchable) Set(on bool) {
	s.mu.Lock()
	s.want = on
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

// Enabled reports the desired state.
func (s *switchable) Enabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.want
}

// Running reports whether the inner component currently runs.
func (s *switchable) Running() bool { return s.running.Load() }

// Starts counts how many times the inner component was started.
func (s *switchable) Starts() int64 { return s.starts.Load() }

// Run runs the inner component whenever it is enabled and stops it when it
// is disabled, until ctx is done. An inner component that fails, or that
// exits on its own while enabled, ends Run with an error so the runtime
// shuts the node down.
func (s *switchable) Run(ctx context.Context) error {
	for {
		for !s.Enabled() {
			select {
			case <-ctx.Done():
				return nil
			case <-s.changed:
			}
		}
		ictx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		s.starts.Add(1)
		s.running.Store(true)
		s.logger.Info("component starting", "component", s.name)
		go func() { done <- s.inner.Run(ictx) }()
		var err error
		stopped := false
	wait:
		for {
			select {
			case <-ctx.Done():
				cancel()
				err = <-done
				s.running.Store(false)
				if err != nil && !errors.Is(err, context.Canceled) {
					return err
				}
				return nil
			case <-s.changed:
				if !s.Enabled() {
					break wait
				}
			case err = <-done:
				stopped = true
				break wait
			}
		}
		cancel()
		if !stopped {
			err = <-done
		}
		s.running.Store(false)
		if stopped {
			if ctx.Err() != nil {
				return nil
			}
			if err != nil {
				return fmt.Errorf("%s: %w", s.name, err)
			}
			return fmt.Errorf("%s exited early", s.name)
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Warn("component stopped with error", "component", s.name, "error", err)
		} else {
			s.logger.Info("component stopped by admin", "component", s.name)
		}
	}
}

// Healthy is nil while switched off; otherwise the inner component decides.
func (s *switchable) Healthy() error {
	if !s.Enabled() {
		return nil
	}
	if !s.Running() {
		return fmt.Errorf("%s: enabled but not running yet", s.name)
	}
	return s.inner.Healthy()
}
