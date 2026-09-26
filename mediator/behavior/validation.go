package behavior

import (
	"context"
	"errors"
	"fmt"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

// NewValidation returns the Validation behavior (5.8). Prepare compiles the
// tag rules of every request type with the shared validate.Validator so an
// unknown rule fails Build with every problem reported at once. Handle runs
// Check: tag rules over the whole graph, then Validate(ctx) on nested
// structs and the root. A *mediator.ValidationError (or the error of
// Validate) is returned unchanged.
func NewValidation(cfg Config) mediator.Behavior {
	return &validation{v: cfg.validator()}
}

type validation struct {
	v *validate.Validator
}

func (b *validation) Name() string { return Validation }

// Prepare compiles every request type.
func (b *validation) Prepare(infos []*mediator.RequestInfo) error {
	var errs []error
	for _, info := range infos {
		if !info.Kind.IsRequest() {
			continue
		}
		if err := b.v.Compile(info.RequestType); err != nil {
			errs = append(errs, fmt.Errorf("behavior: %s: %w", info.RequestType, err))
		}
	}
	return errors.Join(errs...)
}

func (b *validation) Handle(ctx context.Context, req any, info *mediator.RequestInfo, next mediator.Next) (any, error) {
	if err := b.v.Check(ctx, req); err != nil {
		return nil, err
	}
	return next(ctx, req)
}
