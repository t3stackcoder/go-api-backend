package validate

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Option configures a Validator.
type Option func(*Validator)

// WithTagKey changes the struct tag key that carries the rules. The default
// is "validate".
func WithTagKey(key string) Option {
	return func(v *Validator) { v.tagKey = key }
}

// WithMaxErrors caps the number of field errors one Check reports; further
// failures are dropped. Zero, the default, means no cap.
func WithMaxErrors(n int) Option {
	return func(v *Validator) { v.maxErrors = n }
}

// Validator compiles the tag rules of struct types once and runs them on
// values. The zero value is not usable; call New. A Validator is safe for
// concurrent use: Check never takes a lock for a type that was compiled, and
// a type seen for the first time is compiled under a mutex.
type Validator struct {
	tagKey    string
	maxErrors int

	mu    sync.Mutex // serializes compilation
	plans atomic.Pointer[map[reflect.Type]*structPlan]
	ctxs  sync.Pool
}

// New returns a Validator with the given options applied.
func New(opts ...Option) *Validator {
	v := &Validator{tagKey: "validate"}
	for _, o := range opts {
		o(v)
	}
	v.ctxs.New = func() any { return &checkCtx{} }
	return v
}

// ErrNilValue is returned by Check for a nil value or nil pointer.
var ErrNilValue = errors.New("validate: nil value")

// structType returns the struct type behind t, dereferencing one pointer.
func structType(t reflect.Type) (reflect.Type, error) {
	if t == nil {
		return nil, ErrNilValue
	}
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("validate: %s is not a struct", t)
	}
	return t, nil
}

// Compile parses every tag in the type graph of t (a struct or pointer to
// struct): nested structs, pointers, slices, maps, and embedded structs. All
// errors are reported at once. Plans are cached per type, so compiling at
// Build makes later Check calls lock-free.
func (v *Validator) Compile(t reflect.Type) error {
	st, err := structType(t)
	if err != nil {
		return err
	}
	_, err = v.plan(st)
	return err
}

// plan returns the compiled plan of struct type t, compiling on first use.
func (v *Validator) plan(t reflect.Type) (*structPlan, error) {
	if m := v.plans.Load(); m != nil {
		if p := (*m)[t]; p != nil {
			return p, nil
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.compileLocked(t)
}

// compileLocked compiles t with v.mu held, unless another goroutine compiled
// it while the caller waited for the lock.
func (v *Validator) compileLocked(t reflect.Type) (*structPlan, error) {
	c := &compiler{v: v, plans: map[reflect.Type]*structPlan{}}
	if m := v.plans.Load(); m != nil {
		if p := (*m)[t]; p != nil {
			return p, nil
		}
		maps.Copy(c.plans, *m)
	}
	p := c.structPlan(t)
	if err := c.finish(); err != nil {
		return nil, err
	}
	v.plans.Store(&c.plans)
	return p, nil
}

// Check validates val, a struct or pointer to struct. Tag rules run first
// over the whole graph; when they all pass, Validate(ctx) runs on every
// nested struct depth first and finally on the root. Field failures come back
// as a *mediator.ValidationError with JSON Pointer paths; a Validate method
// that returns any other error has that error returned as-is. A type that was
// not compiled is compiled here, and a compile error is returned unchanged.
func (v *Validator) Check(ctx context.Context, val any) error {
	if val == nil {
		return ErrNilValue
	}
	rv := reflect.ValueOf(val)
	if rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return ErrNilValue
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("validate: %s is not a struct", rv.Type())
	}
	p, err := v.plan(rv.Type())
	if err != nil {
		return err
	}
	if !p.hasCheck {
		return nil
	}
	if p.hasValidate && !rv.CanAddr() {
		cp := reflect.New(rv.Type()).Elem()
		cp.Set(rv)
		rv = cp
	}
	cc := v.ctxs.Get().(*checkCtx)
	cc.path, cc.errs, cc.max = cc.path[:0], cc.errs[:0], v.maxErrors
	defer v.ctxs.Put(cc)

	p.check(cc, rv)
	if len(cc.errs) == 0 && p.hasValidate {
		if err := p.validate(ctx, cc, rv); err != nil {
			return err
		}
	}
	if len(cc.errs) > 0 {
		return &mediator.ValidationError{Fields: slices.Clone(cc.errs)}
	}
	return nil
}
