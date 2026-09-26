package pg

import (
	"context"
	"log/slog"
)

// OnCommit registers hook to run after the ambient transaction commits.
// Hooks run in registration order, in the goroutine that owns the unit of
// work (the caller of Send, or the consumer loop), before that call
// returns. A hook panic is logged, never propagated: the transaction is
// already durable. A hook registered by another hook runs after the ones
// already registered. Outside a unit of work, or once its hooks have all
// run, the hook runs immediately; after a rollback it is dropped.
func OnCommit(ctx context.Context, hook func(ctx context.Context)) {
	u, ok := uowFrom(ctx)
	if !ok {
		u, ok = ctx.Value(afterKey{}).(*unitOfWork)
	}
	if !ok {
		runHook(ctx, hook, slog.Default())
		return
	}
	u.mu.Lock()
	if u.done && !u.committed {
		u.mu.Unlock()
		return
	}
	if u.afterDone {
		u.mu.Unlock()
		runHook(ctx, hook, slog.Default())
		return
	}
	u.after = append(u.after, hook)
	u.mu.Unlock()
}

// BeforeCommit registers hook to run inside the transaction just before
// COMMIT, in registration order. An error aborts the commit: the
// transaction rolls back and the error is returned to the caller. Outside
// a unit of work the hook runs immediately and its error is logged.
func BeforeCommit(ctx context.Context, hook func(ctx context.Context) error) {
	u, ok := uowFrom(ctx)
	if !ok || u.isDone() {
		if err := hook(ctx); err != nil {
			slog.Default().Error("unit of work: before-commit hook outside a transaction failed", "error", err)
		}
		return
	}
	u.mu.Lock()
	u.before = append(u.before, hook)
	u.mu.Unlock()
}
