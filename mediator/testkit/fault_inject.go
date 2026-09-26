//go:build faultinject

package testkit

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// FaultInjectionEnabled reports whether this binary was built with the
// faultinject tag.
const FaultInjectionEnabled = true

// FaultKind is one way an I/O operation can fail.
type FaultKind string

const (
	// FaultError returns a transient error without performing the operation.
	FaultError FaultKind = "error"
	// FaultPermanent returns a non-transient error without performing the operation.
	FaultPermanent FaultKind = "permanent"
	// FaultTimeout blocks until the context deadline, then returns its error.
	FaultTimeout FaultKind = "timeout"
	// FaultDelay performs the operation after Delay.
	FaultDelay FaultKind = "delay"
	// FaultAmbiguous performs the operation, then returns a connection error.
	FaultAmbiguous FaultKind = "ambiguous"
	// FaultCrash exits the process with code 137 after performing the operation.
	FaultCrash FaultKind = "crash"
	// FaultCancel cancels the request context at this point.
	FaultCancel FaultKind = "cancel"
)

// AllFaultKinds lists every kind the sweep iterates over.
var AllFaultKinds = []FaultKind{FaultError, FaultPermanent, FaultTimeout, FaultDelay, FaultAmbiguous, FaultCrash, FaultCancel}

// InjectedError is the error type of every injected fault. It classifies
// itself through Transient so mediator.IsTransient agrees with the fault kind
// without testkit importing the core.
type InjectedError struct {
	Msg       string
	transient bool
	ambiguous bool
}

func (e *InjectedError) Error() string { return e.Msg }

// Transient reports whether the fault models a retryable failure.
func (e *InjectedError) Transient() bool { return e.transient }

// Ambiguous reports whether the operation was performed before the error
// was returned (the FaultAmbiguous kind).
func (e *InjectedError) Ambiguous() bool { return e.ambiguous }

// Timeout makes an ambiguous fault look like a connection-level failure to
// callers that check net.Error semantics.
func (e *InjectedError) Timeout() bool { return e.ambiguous }

// Temporary reports whether the failure is temporary.
func (e *InjectedError) Temporary() bool { return e.transient }

// ErrInjected is the transient error returned by FaultError.
var ErrInjected = &InjectedError{Msg: "testkit: injected transient fault", transient: true}

// ErrInjectedPermanent is the non-transient error returned by FaultPermanent.
var ErrInjectedPermanent = &InjectedError{Msg: "testkit: injected permanent fault"}

// ErrInjectedAmbiguous is returned after the operation was performed by FaultAmbiguous.
// It is classified as transient and as a connection error.
var ErrInjectedAmbiguous = &InjectedError{Msg: "testkit: injected ambiguous fault (operation performed)", transient: true, ambiguous: true}

// Schedule arms one fault: at the Nth hit (1-based) of Point, apply Kind.
// A zero Hit means every hit.
type Schedule struct {
	Point string
	Kind  FaultKind
	Hit   int
	Delay time.Duration
	// Cancel is invoked for FaultCancel; the sweep installs the request's cancel func.
	Cancel func()
	// OnCrash, when set, replaces os.Exit so in-process tests can observe crashes.
	OnCrash func(point string)
}

var (
	armed    atomic.Pointer[[]Schedule]
	hitMu    sync.Mutex
	hits     = map[string]int{}
	observed = map[string]struct{}{}
)

// Arm installs schedules for this process. Passing none disarms.
func Arm(s ...Schedule) {
	cp := append([]Schedule(nil), s...)
	armed.Store(&cp)
	hitMu.Lock()
	clear(hits)
	hitMu.Unlock()
}

// Disarm removes every schedule and resets hit counts.
func Disarm() { Arm() }

// Hits returns how many times each point was reached since the last Arm.
func Hits() map[string]int {
	hitMu.Lock()
	defer hitMu.Unlock()
	out := make(map[string]int, len(hits))
	for k, v := range hits {
		out[k] = v
	}
	return out
}

// Observed returns every point name reached since process start, so the
// sweep can prove each catalogued point was exercised.
func Observed() []string {
	hitMu.Lock()
	defer hitMu.Unlock()
	out := make([]string, 0, len(observed))
	for k := range observed {
		out = append(out, k)
	}
	return out
}

// Fault is the I/O call-site hook. It returns an error when an armed schedule
// matches the point at this hit; for the ambiguous and crash kinds it returns
// nil here and acts in FaultAfter.
func Fault(ctx context.Context, point string) error {
	p := armed.Load()
	hitMu.Lock()
	hits[point]++
	n := hits[point]
	observed[point] = struct{}{}
	hitMu.Unlock()
	if p == nil {
		return nil
	}
	for _, s := range *p {
		if s.Point != point || (s.Hit != 0 && s.Hit != n) {
			continue
		}
		switch s.Kind {
		case FaultError:
			return fmt.Errorf("%w at %s", ErrInjected, point)
		case FaultPermanent:
			return fmt.Errorf("%w at %s", ErrInjectedPermanent, point)
		case FaultTimeout:
			<-ctx.Done()
			return ctx.Err()
		case FaultDelay:
			t := time.NewTimer(s.Delay)
			select {
			case <-t.C:
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			}
			return nil
		case FaultCancel:
			if s.Cancel != nil {
				s.Cancel()
			}
			return context.Canceled
		case FaultAmbiguous, FaultCrash:
			// Acts after the operation; see FaultAfter.
			afterMu.Lock()
			after[afterKey{point, n}] = s
			afterMu.Unlock()
			return nil
		}
	}
	return nil
}

type afterKey struct {
	point string
	hit   int
}

var (
	afterMu sync.Mutex
	after   = map[afterKey]Schedule{}
)

// FaultAfter is called after an operation succeeded. It applies the ambiguous
// and crash kinds armed for the most recent hit of the point.
func FaultAfter(ctx context.Context, point string) error {
	hitMu.Lock()
	n := hits[point]
	hitMu.Unlock()
	afterMu.Lock()
	s, ok := after[afterKey{point, n}]
	if ok {
		delete(after, afterKey{point, n})
	}
	afterMu.Unlock()
	if !ok {
		return nil
	}
	switch s.Kind {
	case FaultAmbiguous:
		return fmt.Errorf("%w at %s", ErrInjectedAmbiguous, point)
	case FaultCrash:
		if s.OnCrash != nil {
			s.OnCrash(point)
			return nil
		}
		fmt.Fprintf(os.Stderr, "testkit: injected crash at %s\n", point)
		os.Exit(137)
	}
	return nil
}
