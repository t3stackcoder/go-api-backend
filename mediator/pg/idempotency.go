package pg

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"log/slog"
	"reflect"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
)

// MaxIdempotencyKeyLength is the longest key accepted (6.6).
const MaxIdempotencyKeyLength = 200

// Idempotency outcomes reported to IdempotencyConfig.Observer.
const (
	OutcomeExecuted = "executed"
	OutcomeReplayed = "replayed"
	OutcomeMismatch = "mismatch"
	OutcomeBusy     = "busy"
)

// IdempotencyConfig configures the idempotency behavior.
type IdempotencyConfig struct {
	// TTL is the retention of a completed row from its creation. Default 24h.
	TTL time.Duration
	// RetryAfter is reported as Details["retry_after_ms"] of a
	// CodeIdempotencyBusy error. Default 1s.
	RetryAfter time.Duration
	// Logger is unused by the behavior itself and kept for symmetry with the
	// other configurations. Default slog.Default().
	Logger *slog.Logger
	// Observer, when set, receives the outcome of every keyed command so
	// the metrics behavior can count executed, replayed, mismatch, and busy.
	Observer func(name, outcome string)
}

func (c IdempotencyConfig) withDefaults() IdempotencyConfig {
	if c.TTL <= 0 {
		c.TTL = 24 * time.Hour
	}
	if c.RetryAfter <= 0 {
		c.RetryAfter = time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	return c
}

// Idempotency returns the behavior of 6.6 (name mediator.NameIdempotency).
// It runs inside the unit of work. Commands without a key pass through.
// Scope is the command name prefixed by the principal's tenant when there
// is one. The key comes from IdempotencyKey() on the request, else from
// mediator.IdempotencyKeyFrom(ctx). Only successful outcomes are stored.
func Idempotency(cfg IdempotencyConfig) mediator.Behavior {
	return &idemBehavior{cfg: cfg.withDefaults()}
}

type idemBehavior struct{ cfg IdempotencyConfig }

func (b *idemBehavior) Name() string { return mediator.NameIdempotency }

func (b *idemBehavior) observe(name, outcome string) {
	if b.cfg.Observer != nil {
		b.cfg.Observer(name, outcome)
	}
}

func (b *idemBehavior) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	if info.Kind != mediator.KindCommand {
		return next(ctx, req)
	}
	key := idempotencyKey(ctx, req)
	if key == "" {
		return next(ctx, req)
	}
	if len(key) > MaxIdempotencyKeyLength {
		return nil, mediator.E(mediator.CodeValidation, "idempotency key must be 1 to 200 characters")
	}
	tx, ok := StoreTxFrom(ctx)
	if !ok {
		return nil, mediator.Wrap(mediator.CodeInternal, "idempotency: the unit of work must run before idempotency", ErrNoUnitOfWork)
	}
	scope := idempotencyScope(ctx, info)
	sum, err := mediator.CanonicalHash(req)
	if err != nil {
		return nil, mediator.Wrap(mediator.CodeInternal, "idempotency: hash request", err)
	}
	row, err := tx.IdempotencyReserve(ctx, scope, key, sum[:], b.cfg.TTL)
	if err != nil {
		if IsLockTimeout(err) {
			b.observe(info.Name, OutcomeBusy)
			return nil, mediator.Wrap(mediator.CodeIdempotencyBusy, "an identical request is in progress", err).
				WithDetail("retry_after_ms", b.cfg.RetryAfter.Milliseconds())
		}
		return nil, err
	}
	switch {
	case row.Hits == 0 && row.Response == nil:
		res, err := next(ctx, req)
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(res)
		if err != nil {
			return nil, mediator.Wrap(mediator.CodeInternal, "idempotency: encode response", err)
		}
		if err := tx.IdempotencyStore(ctx, scope, key, body); err != nil {
			return nil, err
		}
		b.observe(info.Name, OutcomeExecuted)
		return res, nil
	case row.Response == nil:
		// A committed reservation always has a response (6.6); the fault
		// sweep asserts it. Fail loudly rather than execute twice.
		return nil, mediator.E(mediator.CodeInternal, "idempotency: committed reservation without a response")
	case !bytes.Equal(row.RequestHash, sum[:]):
		b.observe(info.Name, OutcomeMismatch)
		return nil, mediator.E(mediator.CodeIdempotencyMismatch, "idempotency key reused with a different payload")
	default:
		res, err := decodeResponse(row.Response, info.ResponseType)
		if err != nil {
			return nil, mediator.Wrap(mediator.CodeInternal, "idempotency: decode stored response", err)
		}
		b.observe(info.Name, OutcomeReplayed)
		return res, nil
	}
}

// decodeResponse rebuilds a stored response as a value of the declared type.
func decodeResponse(body []byte, rt reflect.Type) (any, error) {
	if rt == nil {
		return nil, nil
	}
	ptr := reflect.New(rt)
	if err := json.Unmarshal(body, ptr.Interface()); err != nil {
		return nil, err
	}
	return ptr.Elem().Interface(), nil
}

// idempotencyScope is tenant:name when the principal has a tenant, else name.
func idempotencyScope(ctx context.Context, info *mediator.RequestInfo) string {
	if t := authz.PrincipalFrom(ctx).Tenant; t != "" {
		return t + ":" + info.Name
	}
	return info.Name
}

// idempotencyKey returns the request's key, else the context's, else "".
func idempotencyKey(ctx context.Context, req any) string {
	if k, ok := req.(mediator.IdempotencyKeyer); ok {
		if key := k.IdempotencyKey(); key != "" {
			return key
		}
	}
	key, _ := mediator.IdempotencyKeyFrom(ctx)
	return key
}
