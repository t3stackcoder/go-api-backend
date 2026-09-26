package behavior

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// closed reports whether ch is closed right now.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// TestDeadlineCtx_ExpiresWithoutDone: a handler that never waits on Done
// still sees the deadline through Err, from the clock, and the answer is
// sticky across a later cancel.
func TestDeadlineCtx_ExpiresWithoutDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := withDeadline(context.Background(), time.Second)
		if d, ok := c.Deadline(); !ok || !d.Equal(time.Now().Add(time.Second)) {
			t.Fatalf("Deadline = %v %v", d, ok)
		}
		if err := c.Err(); err != nil {
			t.Fatalf("Err before the deadline: %v", err)
		}
		time.Sleep(time.Second)
		if err := c.Err(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Err after the deadline: %v", err)
		}
		if !closed(c.Done()) {
			t.Fatal("Done after the deadline must be closed")
		}
		if c.inner.Load() != nil {
			t.Fatal("no inner context is created for a context that already ended")
		}
		c.cancel()
		if err := c.Err(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Err must not change after cancel: %v", err)
		}
	})
}

// TestDeadlineCtx_ExpiresWithDone: the first Done creates the timer; the
// channel closes at the deadline and Cause agrees.
func TestDeadlineCtx_ExpiresWithDone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := withDeadline(context.Background(), time.Second)
		done := c.Done()
		if c.Done() != done {
			t.Fatal("Done must return the same channel")
		}
		if closed(done) || c.Err() != nil {
			t.Fatal("ended before the deadline")
		}
		<-done
		if err := c.Err(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Err = %v", err)
		}
		if err := context.Cause(c); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Cause = %v", err)
		}
		c.cancel()
		c.cancel()
		if err := c.Err(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Err after cancel = %v", err)
		}
	})
}

// TestDeadlineCtx_ParentCancellation: the parent's cancellation is visible
// through Err without Done, and closes Done when it exists.
func TestDeadlineCtx_ParentCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pctx, pcancel := context.WithCancel(context.Background())
		c := withDeadline(pctx, time.Hour)
		pcancel()
		if err := c.Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("Err = %v", err)
		}
		if !closed(c.Done()) || c.inner.Load() != nil {
			t.Fatal("Done after the parent ended is closed without an inner context")
		}

		pctx, pcancel = context.WithCancel(context.Background())
		c = withDeadline(pctx, time.Hour)
		done := c.Done()
		pcancel()
		<-done
		if err := c.Err(); !errors.Is(err, context.Canceled) {
			t.Fatalf("Err = %v", err)
		}
	})
}

// TestDeadlineCtx_ParentDeadlineWins: an earlier deadline on the parent is
// reported and enforced.
func TestDeadlineCtx_ParentDeadlineWins(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c := withDeadline(pctx, time.Hour)
		if d, _ := c.Deadline(); !d.Equal(time.Now().Add(time.Second)) {
			t.Fatalf("Deadline = %v", d)
		}
		<-c.Done()
		if err := c.Err(); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Err = %v", err)
		}
	})
}

// TestDeadlineCtx_Cancel: cancel ends the context whether or not Done was
// ever called.
func TestDeadlineCtx_Cancel(t *testing.T) {
	c := withDeadline(context.Background(), time.Hour)
	c.cancel()
	if err := c.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err = %v", err)
	}
	if !closed(c.Done()) || c.inner.Load() != nil {
		t.Fatal("Done after cancel is closed without an inner context")
	}

	c = withDeadline(context.Background(), time.Hour)
	done := c.Done()
	c.cancel()
	<-done
	if err := c.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("Err = %v", err)
	}
}

// TestDeadlineCtx_Value: values resolve through the parent before Done and
// through the inner context after it; a child derived through WithValue
// still hangs off the inner cancel context.
func TestDeadlineCtx_Value(t *testing.T) {
	type key struct{}
	parent := context.WithValue(context.Background(), key{}, "v")
	c := withDeadline(parent, time.Hour)
	if c.Value(key{}) != "v" {
		t.Fatal("Value before Done")
	}
	c.Done()
	if c.Value(key{}) != "v" {
		t.Fatal("Value after Done")
	}
	child, ccancel := context.WithCancel(context.WithValue(c, key{}, "w"))
	defer ccancel()
	if child.Value(key{}) != "w" {
		t.Fatal("child value")
	}
	c.cancel()
	<-child.Done()
	if err := child.Err(); !errors.Is(err, context.Canceled) {
		t.Fatalf("child Err = %v", err)
	}
}

// TestDeadlineCtx_ConcurrentDone: racing Done calls agree on one channel.
func TestDeadlineCtx_ConcurrentDone(t *testing.T) {
	c := withDeadline(context.Background(), time.Hour)
	defer c.cancel()
	chans := make([]<-chan struct{}, 16)
	var wg sync.WaitGroup
	for i := range chans {
		wg.Add(1)
		go func() {
			defer wg.Done()
			chans[i] = c.Done()
		}()
	}
	wg.Wait()
	for _, ch := range chans[1:] {
		if ch != chans[0] {
			t.Fatal("Done returned different channels")
		}
	}
}

// foreignCtx is a context the standard library cannot see through: its Done
// channel belongs to no cancelCtx, so children derived from it need a
// watcher goroutine each. It is the control of the goroutine test.
type foreignCtx struct {
	context.Context
	done chan struct{}
}

func (f *foreignCtx) Done() <-chan struct{} { return f.done }

func (f *foreignCtx) Err() error {
	if closed(f.done) {
		return context.Canceled
	}
	return nil
}

// watcherGoroutines counts the goroutines the context package started to
// watch a parent it cannot see through (propagateCancel's last resort), by
// their creator frame in a full stack dump. Unrelated goroutines starting or
// stopping do not disturb the count the way runtime.NumGoroutine would.
func watcherGoroutines() int {
	buf := make([]byte, 1<<16)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	return strings.Count(string(buf), "created by context.(*cancelCtx).propagateCancel")
}

// TestDeadlineCtx_NoWatcherGoroutines: deriving standard-library children
// from a deadlineCtx, directly or through WithValue, starts no watcher
// goroutine, while the same derivations from a foreign context start one
// each (which proves the count would catch a regression).
func TestDeadlineCtx_NoWatcherGoroutines(t *testing.T) {
	const n = 64
	c := withDeadline(context.Background(), time.Hour)
	before := watcherGoroutines()
	var children []context.Context
	for i := 0; i < n; i++ {
		child, cancel := context.WithCancel(c)
		defer cancel()
		grand, gcancel := context.WithTimeout(context.WithValue(child, foreignCtx{}, i), time.Hour)
		defer gcancel()
		stop := context.AfterFunc(grand, func() {})
		defer stop()
		children = append(children, child, grand)
	}
	if got := watcherGoroutines(); got != before {
		t.Fatalf("%d watcher goroutines appeared while deriving %d children", got-before, n)
	}
	c.cancel()
	for _, child := range children {
		<-child.Done()
	}

	f := &foreignCtx{Context: context.Background(), done: make(chan struct{})}
	for i := 0; i < n; i++ {
		_, cancel := context.WithCancel(f)
		defer cancel()
	}
	if got := watcherGoroutines(); got-before < n {
		t.Fatalf("control: only %d watcher goroutines for %d children of a foreign context", got-before, n)
	}
	close(f.done)
}
