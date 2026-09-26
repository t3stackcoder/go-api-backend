package behavior

import (
	"context"
	"errors"
	"fmt"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
)

// NewAuthorization returns the Authorization behavior (5.7) for requests
// that implement Requires(). The principal comes from the context. A check
// that reports authz.ErrUnauthenticated, or that fails for an anonymous
// caller against a non-empty requirement, returns CodeUnauthorized so an
// anonymous caller learns nothing more than that authentication is needed.
// Any other failure returns CodeForbidden with the check's message: the
// authz errors are written to be shown, and a Custom check chooses its own
// text. With Config.RequireAuthByDefault, Prepare rejects every request
// type that does not implement Requires(), listing all offenders.
func NewAuthorization(cfg Config) mediator.Behavior {
	return &authorization{requireAll: cfg.RequireAuthByDefault}
}

type authorization struct {
	requireAll bool
}

func (a *authorization) Name() string { return Authorization }

// Prepare enforces RequireAuthByDefault.
func (a *authorization) Prepare(infos []*mediator.RequestInfo) error {
	if !a.requireAll {
		return nil
	}
	var errs []error
	for _, info := range infos {
		if info.Kind.IsRequest() && !info.Traits.Requires {
			errs = append(errs, fmt.Errorf("behavior: %s %s does not implement Requires() and RequireAuthByDefault is set", info.Kind, info.RequestType))
		}
	}
	return errors.Join(errs...)
}

func (a *authorization) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	r, ok := req.(mediator.Requirer)
	if !ok {
		return next(ctx, req)
	}
	requirement := r.Requires()
	if requirement == nil {
		return next(ctx, req)
	}
	p := authz.PrincipalFrom(ctx)
	err := requirement.Check(ctx, p)
	if err == nil {
		return next(ctx, req)
	}
	if errors.Is(err, authz.ErrUnauthenticated) || (p.IsAnonymous() && !requirement.Describe().Empty()) {
		return nil, mediator.Wrap(mediator.CodeUnauthorized, "authentication required", err)
	}
	return nil, mediator.Wrap(mediator.CodeForbidden, err.Error(), err)
}
