package memstore

import (
	"context"
	"sync"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// lockTable emulates Postgres row locks: a key is held by one transaction
// until that transaction ends; waiters block until then, bounded by the
// transaction's lock timeout, after which they get pg.ErrLockTimeout.
// Deadlocks are not detected: both parties time out.
type lockTable struct {
	mu   sync.Mutex
	held map[any]*lockEntry
}

type lockEntry struct {
	owner    *Tx
	released chan struct{}
}

// acquire blocks until key is free or owner already holds it.
func (l *lockTable) acquire(ctx context.Context, key any, owner *Tx, timeout time.Duration, clock testkit.Clock) error {
	var deadline <-chan time.Time
	for {
		l.mu.Lock()
		if l.held == nil {
			l.held = map[any]*lockEntry{}
		}
		e, ok := l.held[key]
		if !ok {
			l.held[key] = &lockEntry{owner: owner, released: make(chan struct{})}
			l.mu.Unlock()
			owner.locks = append(owner.locks, key)
			return nil
		}
		if e.owner == owner {
			l.mu.Unlock()
			return nil
		}
		released := e.released
		l.mu.Unlock()
		if deadline == nil {
			deadline = clock.After(timeout)
		}
		select {
		case <-released:
		case <-deadline:
			return pg.ErrLockTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// release frees every key owner holds and wakes the waiters.
func (l *lockTable) release(owner *Tx) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, k := range owner.locks {
		if e, ok := l.held[k]; ok && e.owner == owner {
			delete(l.held, k)
			close(e.released)
		}
	}
	owner.locks = nil
}
