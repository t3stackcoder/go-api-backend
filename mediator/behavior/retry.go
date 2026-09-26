package behavior

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/retry"
)

// NewRetry returns the Retry behavior (5.11) for commands that implement
// RetryPolicy(). After a failure it re-invokes the rest of the chain, so
// each attempt runs in a fresh unit of work, when the policy's RetryIf
// (default mediator.IsTransient) accepts the error, the attempt budget is
// not exhausted, and the delay fits before the deadline. An ambiguous
// error (a commit whose outcome is unknown) is retried only when the
// request carries an idempotency key, from the trait or the context. The
// delay comes from retry.Policy.Delay (exponential, full jitter) and sleeps
// on the clock's After racing ctx.Done. Prepare rejects a policy on any
// NoUnitOfWork() request, a policy on anything but a command, and invalid
// delays.
//
// The NoUnitOfWork rule is stricter than the core's Build check, which lets
// RetryPolicy() and NoUnitOfWork() coexist when the request has
// IdempotencyKey(). That combination is only safe when something reserves
// the key before the handler runs, and in the standard set the Idempotency
// behavior is scoped to commands with a unit of work (it needs the
// transaction for the reservation row), so here a retry outside a unit of
// work could repeat the handler's effects and is refused at Build.
func NewRetry(cfg Config) mediator.Behavior {
	return &retryBehavior{clock: cfg.clock(), after: cfg.after(), logger: cfg.logger()}
}

type retryBehavior struct {
	clock  mediator.Clock
	after  afterFunc
	logger *slog.Logger
}

func (b *retryBehavior) Name() string { return Retry }

// Prepare validates every retry policy.
func (b *retryBehavior) Prepare(infos []*mediator.RequestInfo) error {
	var errs []error
	for _, info := range infos {
		if !info.Traits.RetryPolicy {
			continue
		}
		if info.Kind != mediator.KindCommand {
			errs = append(errs, fmt.Errorf("behavior: %s is a %s; RetryPolicy() applies to commands only", info.RequestType, info.Kind))
			continue
		}
		if info.Traits.NoUnitOfWork {
			errs = append(errs, fmt.Errorf("behavior: %s has RetryPolicy() and NoUnitOfWork(): retry without a unit of work can repeat effects; remove NoUnitOfWork or RetryPolicy", info.RequestType))
		}
		p := reflect.Zero(info.RequestType).Interface().(mediator.Retrier).RetryPolicy()
		if err := validatePolicy(p); err != nil {
			errs = append(errs, fmt.Errorf("behavior: %s: %w", info.RequestType, err))
		}
	}
	return errors.Join(errs...)
}

func validatePolicy(p retry.Policy) error {
	switch {
	case p.MaxAttempts < 1:
		return fmt.Errorf("retry policy: MaxAttempts %d must be at least 1", p.MaxAttempts)
	case p.BaseDelay < 0 || p.MaxDelay < 0:
		return fmt.Errorf("retry policy: delays must not be negative (base %s, max %s)", p.BaseDelay, p.MaxDelay)
	case p.MaxDelay > 0 && p.MaxDelay < p.BaseDelay:
		return fmt.Errorf("retry policy: MaxDelay %s is shorter than BaseDelay %s", p.MaxDelay, p.BaseDelay)
	}
	return nil
}

// hasIdempotencyKey reports whether a re-execution is protected by an
// idempotency reservation.
func hasIdempotencyKey(ctx context.Context, req any, info *mediator.RequestInfo) bool {
	if info.Traits.IdempotencyKey && req.(mediator.IdempotencyKeyer).IdempotencyKey() != "" {
		return true
	}
	_, ok := mediator.IdempotencyKeyFrom(ctx)
	return ok
}

func (b *retryBehavior) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	r, ok := req.(mediator.Retrier)
	if !ok {
		return next(ctx, req)
	}
	p := r.RetryPolicy()
	retryIf := p.RetryIf
	if retryIf == nil {
		retryIf = mediator.IsTransient
	}
	attempts := p.Attempts()
	for attempt := 1; ; attempt++ {
		res, err := next(ctx, req)
		if err == nil {
			return res, nil
		}
		if attempt >= attempts || !retryIf(err) || ctx.Err() != nil {
			return nil, err
		}
		if mediator.IsAmbiguous(err) && !hasIdempotencyKey(ctx, req, info) {
			return nil, err
		}
		delay := p.Delay(attempt, nil)
		if deadline, ok := ctx.Deadline(); ok && b.clock.Now().Add(delay).After(deadline) {
			return nil, err
		}
		b.logger.LogAttrs(ctx, slog.LevelDebug, "retrying after failure",
			slog.String("name", info.Name), slog.Int("attempt", attempt), slog.Duration("delay", delay),
			slog.String("correlation_id", mediator.CorrelationID(ctx)), slog.Any("error", err))
		select {
		case <-b.after(delay):
		case <-ctx.Done():
			return nil, err
		}
	}
}
