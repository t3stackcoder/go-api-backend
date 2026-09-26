package pg

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// UnitOfWorkConfig configures the unit of work behavior.
type UnitOfWorkConfig struct {
	// DefaultLockTimeout is the SET LOCAL lock_timeout when the request's
	// TxOptions do not set one. Default 5s.
	DefaultLockTimeout time.Duration
	// RollbackTimeout bounds ROLLBACK, which runs on a context detached from
	// the request so a canceled request still rolls back cleanly. Default 5s.
	RollbackTimeout time.Duration
	// Logger receives rollback failures and hook panics. Default slog.Default().
	Logger *slog.Logger
}

func (c UnitOfWorkConfig) withDefaults() UnitOfWorkConfig {
	if c.DefaultLockTimeout <= 0 {
		c.DefaultLockTimeout = DefaultLockTimeout
	}
	if c.RollbackTimeout <= 0 {
		c.RollbackTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

type uowKey struct{}

// afterKey carries the unit of work into its OnCommit hooks without exposing
// the closed transaction: a Send inside a hook opens a fresh unit of work,
// while a nested OnCommit still queues behind the hooks already registered.
type afterKey struct{}

// unitOfWork is the ambient transaction: the Tx, its hooks, and the relay
// notifications registered by AppendOutbox. It implements
// mediator.UnitOfWork so Publish can append durable events.
type unitOfWork struct {
	mu        sync.Mutex
	tx        Tx
	readOnly  bool
	before    []func(context.Context) error
	after     []func(context.Context)
	notified  map[string]struct{}
	done      bool // committed or rolled back
	committed bool
	afterDone bool // the OnCommit hooks have all run
}

// ReadOnly reports whether the transaction is read-only.
func (u *unitOfWork) ReadOnly() bool { return u.readOnly }

// AppendOutbox inserts the outbox row and registers, once per distinct
// (topic, partition), a BeforeCommit hook that issues the relay wake-up
// notification inside the transaction (6.3, design notes 3.2).
func (u *unitOfWork) AppendOutbox(ctx context.Context, env *mediator.Envelope, payload []byte) error {
	if u.readOnly {
		return mediator.ErrDurablePublishInQuery
	}
	if err := u.tx.OutboxAppend(ctx, env, payload); err != nil {
		return err
	}
	key := env.Topic + ":" + strconv.Itoa(env.Partition)
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.notified == nil {
		u.notified = map[string]struct{}{}
	}
	if _, seen := u.notified[key]; seen {
		return nil
	}
	u.notified[key] = struct{}{}
	u.before = append(u.before, func(ctx context.Context) error {
		return u.tx.Notify(ctx, NotifyChannel, key)
	})
	return nil
}

func (u *unitOfWork) attach(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, uowKey{}, u)
	return mediator.WithUnitOfWork(ctx, u)
}

func (u *unitOfWork) isDone() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.done
}

// rollback discards the transaction once. It runs on a context detached
// from the request so that a canceled request still rolls back cleanly.
func (u *unitOfWork) rollback(ctx context.Context, cfg UnitOfWorkConfig) {
	u.mu.Lock()
	if u.done {
		u.mu.Unlock()
		return
	}
	u.done = true
	u.mu.Unlock()
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.RollbackTimeout)
	defer cancel()
	if err := u.tx.Rollback(rctx); err != nil {
		cfg.Logger.Error("unit of work: rollback failed", "error", err)
	}
}

// commit runs the BeforeCommit hooks then COMMIT (6.1 steps 5 and 6). A
// hook error rolls back. A commit error is classified: a server-reported
// failure means nothing was committed; a connection-level failure means the
// outcome is unknown and the error is marked ambiguous.
func (u *unitOfWork) commit(ctx context.Context, cfg UnitOfWorkConfig) error {
	for i := 0; ; i++ {
		u.mu.Lock()
		if i >= len(u.before) {
			u.mu.Unlock()
			break
		}
		h := u.before[i]
		u.mu.Unlock()
		if err := h(ctx); err != nil {
			u.rollback(ctx, cfg)
			return err
		}
	}
	err := u.tx.Commit(ctx)
	if err == nil {
		u.mu.Lock()
		u.done, u.committed = true, true
		u.mu.Unlock()
		return nil
	}
	// Whatever happened, make sure the connection is not left in a
	// transaction: a closed transaction ignores the rollback.
	u.rollback(ctx, cfg)
	if isDefiniteCommitFailure(err) {
		if mediator.IsTransient(err) {
			return mediator.Wrap(mediator.CodeUnavailable, "commit failed", err)
		}
		return mediator.Wrap(mediator.CodeInternal, "commit failed", err)
	}
	return mediator.MarkAmbiguous(mediator.Wrap(mediator.CodeUnavailable, "commit outcome unknown", err))
}

// isDefiniteCommitFailure reports whether a commit error proves the
// transaction did not commit: any error with a SQLSTATE, or a transaction
// the driver knows is closed or aborted. Everything else (connection
// reset, context canceled mid-commit, lost acknowledgement) is ambiguous.
func isDefiniteCommitFailure(err error) bool {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return true
	}
	return errors.Is(err, ErrTxAborted) || errors.Is(err, ErrTxClosed) ||
		errors.Is(err, pgx.ErrTxCommitRollback) || errors.Is(err, pgx.ErrTxClosed)
}

// runAfter runs the OnCommit hooks in registration order in the calling
// goroutine. A hook may register another; a panic is logged.
func (u *unitOfWork) runAfter(ctx context.Context, cfg UnitOfWorkConfig) {
	ctx = context.WithValue(ctx, afterKey{}, u)
	for i := 0; ; i++ {
		u.mu.Lock()
		if i >= len(u.after) {
			u.afterDone = true
			u.mu.Unlock()
			return
		}
		h := u.after[i]
		u.mu.Unlock()
		runHook(ctx, h, cfg.Logger)
	}
}

func runHook(ctx context.Context, h func(context.Context), logger *slog.Logger) {
	defer func() {
		if p := recover(); p != nil {
			logger.Error("unit of work: on-commit hook panicked", "panic", p)
		}
	}()
	h(ctx)
}

func uowFrom(ctx context.Context) (*unitOfWork, bool) {
	u, ok := ctx.Value(uowKey{}).(*unitOfWork)
	return u, ok && u != nil
}

// TxFrom returns the pgx transaction of the ambient unit of work. Handlers
// use it for every database access. It is false outside a unit of work and
// with the in-memory store, whose transactions are not pgx transactions.
func TxFrom(ctx context.Context) (pgx.Tx, bool) {
	u, ok := uowFrom(ctx)
	if !ok {
		return nil, false
	}
	p, ok := u.tx.(pgxTxer)
	if !ok {
		return nil, false
	}
	return p.PgxTx(), true
}

// StoreTxFrom returns the Tx of the ambient unit of work.
func StoreTxFrom(ctx context.Context) (Tx, bool) {
	u, ok := uowFrom(ctx)
	if !ok {
		return nil, false
	}
	return u.tx, true
}

// UnitOfWork returns the behavior of 6.1 (name mediator.NameUnitOfWork).
// It implements mediator.StreamBehavior: a stream's transaction opens when
// the sequence is first iterated and closes when it ends.
//
// Defaults by kind: commands and consumers run READ COMMITTED read-write;
// queries and streams REPEATABLE READ read-only. A request implementing
// TxOptioner overrides them; its ReadOnly and Propagation are taken as
// given, an empty Isolation keeps the kind's default, and a zero
// LockTimeout takes UnitOfWorkConfig.DefaultLockTimeout.
func UnitOfWork(store Store, cfg UnitOfWorkConfig) mediator.Behavior {
	return &uowBehavior{store: store, cfg: cfg.withDefaults()}
}

type uowBehavior struct {
	store Store
	cfg   UnitOfWorkConfig
}

func (b *uowBehavior) Name() string { return mediator.NameUnitOfWork }

func (b *uowBehavior) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	opts := resolveTxOptions(req, info.Kind, b.cfg.DefaultLockTimeout)
	return execute(ctx, b.store, opts, b.cfg, func(ctx context.Context) (any, error) {
		return next(ctx, req)
	})
}

// HandleStream opens the transaction lazily at the first iteration, commits
// when the sequence ends normally, and rolls back when it yields an error,
// when the consumer stops early, or when the consumer's loop body panics
// (the panic is re-raised after the rollback).
func (b *uowBehavior) HandleStream(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.StreamNext) iter.Seq2[any, error] {
	opts := resolveTxOptions(req, info.Kind, b.cfg.DefaultLockTimeout)
	if opts.Propagation == Required {
		if _, ok := uowFrom(ctx); ok {
			return next(ctx, req)
		}
	}
	return func(yield func(any, error) bool) {
		tx, err := b.store.Begin(ctx, opts)
		if err != nil {
			yield(nil, beginError(err))
			return
		}
		u := &unitOfWork{tx: tx, readOnly: tx.ReadOnly()}
		txCtx := u.attach(ctx)
		settled := false
		defer func() {
			if p := recover(); p != nil {
				u.rollback(ctx, b.cfg)
				panic(p)
			}
			if !settled {
				u.rollback(ctx, b.cfg)
			}
		}()
		for v, err := range next(txCtx, req) {
			if err != nil {
				settled = true
				u.rollback(ctx, b.cfg)
				yield(nil, err)
				return
			}
			if !yield(v, nil) {
				settled = true
				u.rollback(ctx, b.cfg)
				return
			}
		}
		settled = true
		if err := u.commit(txCtx, b.cfg); err != nil {
			yield(nil, err)
			return
		}
		u.runAfter(ctx, b.cfg)
	}
}

// WithTx runs f inside a transaction with the same semantics as the
// behavior (join when Propagation is Required and a unit of work is ambient,
// rollback on error or panic, hooks around COMMIT), for use outside the
// pipeline: jobs, tests, migrations. A zero LockTimeout takes the store's
// default.
func WithTx(ctx context.Context, store Store, opts TxOptions, f func(ctx context.Context) error) error {
	_, err := execute(ctx, store, opts, UnitOfWorkConfig{}.withDefaults(), func(ctx context.Context) (any, error) {
		return nil, f(ctx)
	})
	return err
}

// execute is the one code path of 6.1 shared by Handle and WithTx.
func execute(ctx context.Context, store Store, opts TxOptions, cfg UnitOfWorkConfig, f func(context.Context) (any, error)) (res any, err error) {
	if opts.Propagation == Required {
		if _, ok := uowFrom(ctx); ok {
			return f(ctx)
		}
	}
	tx, err := store.Begin(ctx, opts)
	if err != nil {
		return nil, beginError(err)
	}
	u := &unitOfWork{tx: tx, readOnly: tx.ReadOnly()}
	txCtx := u.attach(ctx)
	defer func() {
		if p := recover(); p != nil {
			u.rollback(ctx, cfg)
			panic(p)
		}
	}()
	res, err = f(txCtx)
	if err != nil {
		u.rollback(ctx, cfg)
		return nil, err
	}
	if err := u.commit(txCtx, cfg); err != nil {
		return nil, err
	}
	u.runAfter(ctx, cfg)
	return res, nil
}

// beginError classifies a Begin failure: connection trouble is transient
// (CodeUnavailable); anything else is returned as is.
func beginError(err error) error {
	if mediator.IsTransient(err) && mediator.CodeOf(err) == mediator.CodeInternal {
		return mediator.Wrap(mediator.CodeUnavailable, "database unavailable", err)
	}
	return err
}

// defaultTxOptions returns the options of 6.1 for a kind.
func defaultTxOptions(kind mediator.Kind) TxOptions {
	switch kind {
	case mediator.KindQuery, mediator.KindStream:
		return TxOptions{Isolation: pgx.RepeatableRead, ReadOnly: true}
	default:
		return TxOptions{Isolation: pgx.ReadCommitted}
	}
}

// resolveTxOptions merges the TxOptioner trait of req over the kind's
// defaults.
func resolveTxOptions(req any, kind mediator.Kind, defaultLock time.Duration) TxOptions {
	opts := defaultTxOptions(kind)
	if t, ok := req.(TxOptioner); ok {
		o := t.TxOptions()
		if o.Isolation != "" {
			opts.Isolation = o.Isolation
		}
		opts.ReadOnly = o.ReadOnly
		opts.Propagation = o.Propagation
		if o.LockTimeout > 0 {
			opts.LockTimeout = o.LockTimeout
		}
	}
	if opts.LockTimeout <= 0 {
		opts.LockTimeout = defaultLock
	}
	return opts
}
