// Package authz carries the principal of a request and the requirements a
// request type declares through Requires() authz.Requirement.
package authz

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Principal is the authenticated caller. The zero value is anonymous.
type Principal struct {
	Subject     string
	Roles       []string
	Permissions []string
	Tenant      string
	Claims      map[string]any
}

// IsAnonymous reports whether the principal carries no identity.
func (p Principal) IsAnonymous() bool { return p.Subject == "" }

// HasRole reports whether the principal has the role.
func (p Principal) HasRole(role string) bool { return slices.Contains(p.Roles, role) }

// HasPermission reports whether the principal has the permission.
func (p Principal) HasPermission(perm string) bool { return slices.Contains(p.Permissions, perm) }

type principalKey struct{}

// WithPrincipal returns a context carrying p.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// PrincipalFrom returns the principal in ctx. The zero principal means anonymous.
func PrincipalFrom(ctx context.Context) Principal {
	p, _ := ctx.Value(principalKey{}).(Principal)
	return p
}

// ErrUnauthenticated is returned by Check when a requirement needs a principal
// and the context has none. The authorization behavior maps it to CodeUnauthorized.
var ErrUnauthenticated = errors.New("authz: authentication required")

// ErrForbidden is returned by Check when a principal fails a requirement. The
// authorization behavior maps it to CodeForbidden.
var ErrForbidden = errors.New("authz: forbidden")

// Description is the static shape of a requirement, used to derive OpenAPI
// security scopes.
type Description struct {
	Roles         []string // any of
	Permissions   []string // all of
	Authenticated bool
	Custom        []string // names of custom checks
	Any           []Description
	All           []Description
}

// Scopes flattens the description into a sorted, de-duplicated scope list.
func (d Description) Scopes() []string {
	var out []string
	for _, r := range d.Roles {
		out = append(out, "role:"+r)
	}
	out = append(out, d.Permissions...)
	for _, c := range d.Custom {
		out = append(out, "custom:"+c)
	}
	for _, sub := range d.Any {
		out = append(out, sub.Scopes()...)
	}
	for _, sub := range d.All {
		out = append(out, sub.Scopes()...)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// Empty reports whether the description imposes nothing.
func (d Description) Empty() bool {
	return len(d.Roles) == 0 && len(d.Permissions) == 0 && !d.Authenticated &&
		len(d.Custom) == 0 && len(d.Any) == 0 && len(d.All) == 0
}

// Requirement is an authorization rule a request declares.
type Requirement interface {
	Check(ctx context.Context, p Principal) error
	Describe() Description
}

// Check evaluates req against the principal in ctx. It is the resource-level
// helper handlers call after loading an entity. A nil requirement passes.
func Check(ctx context.Context, req Requirement) error {
	if req == nil {
		return nil
	}
	return req.Check(ctx, PrincipalFrom(ctx))
}

type roleReq struct{ names []string }

// Role requires any one of the named roles.
func Role(names ...string) Requirement { return roleReq{names: slices.Clone(names)} }

func (r roleReq) Check(_ context.Context, p Principal) error {
	if p.IsAnonymous() {
		return ErrUnauthenticated
	}
	for _, n := range r.names {
		if p.HasRole(n) {
			return nil
		}
	}
	return fmt.Errorf("%w: requires one of roles [%s]", ErrForbidden, strings.Join(r.names, ", "))
}

func (r roleReq) Describe() Description { return Description{Roles: slices.Clone(r.names)} }

type permReq struct{ names []string }

// Permission requires all of the named permissions.
func Permission(names ...string) Requirement { return permReq{names: slices.Clone(names)} }

func (r permReq) Check(_ context.Context, p Principal) error {
	if p.IsAnonymous() {
		return ErrUnauthenticated
	}
	for _, n := range r.names {
		if !p.HasPermission(n) {
			return fmt.Errorf("%w: missing permission %q", ErrForbidden, n)
		}
	}
	return nil
}

func (r permReq) Describe() Description { return Description{Permissions: slices.Clone(r.names)} }

type authReq struct{}

// Authenticated requires any non-anonymous principal.
func Authenticated() Requirement { return authReq{} }

func (authReq) Check(_ context.Context, p Principal) error {
	if p.IsAnonymous() {
		return ErrUnauthenticated
	}
	return nil
}

func (authReq) Describe() Description { return Description{Authenticated: true} }

type anyReq struct{ reqs []Requirement }

// Any passes when at least one requirement passes. With no requirements it passes.
func Any(reqs ...Requirement) Requirement { return anyReq{reqs: slices.Clone(reqs)} }

func (r anyReq) Check(ctx context.Context, p Principal) error {
	if len(r.reqs) == 0 {
		return nil
	}
	var errs []error
	allUnauth := true
	for _, q := range r.reqs {
		err := q.Check(ctx, p)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrUnauthenticated) {
			allUnauth = false
		}
		errs = append(errs, err)
	}
	if allUnauth {
		return ErrUnauthenticated
	}
	// The alternatives' errors are reported as text only: wrapping them would
	// make the result match ErrUnauthenticated as well, and the status must
	// be unambiguous.
	return fmt.Errorf("%w: no alternative satisfied: %v", ErrForbidden, errors.Join(errs...))
}

func (r anyReq) Describe() Description {
	d := Description{}
	for _, q := range r.reqs {
		d.Any = append(d.Any, q.Describe())
	}
	return d
}

type allReq struct{ reqs []Requirement }

// All passes when every requirement passes. With no requirements it passes.
func All(reqs ...Requirement) Requirement { return allReq{reqs: slices.Clone(reqs)} }

func (r allReq) Check(ctx context.Context, p Principal) error {
	for _, q := range r.reqs {
		if err := q.Check(ctx, p); err != nil {
			return err
		}
	}
	return nil
}

func (r allReq) Describe() Description {
	d := Description{}
	for _, q := range r.reqs {
		d.All = append(d.All, q.Describe())
	}
	return d
}

type customReq struct {
	name string
	f    func(context.Context, Principal) error
}

// Custom wraps an arbitrary check. name appears in OpenAPI as "custom:<name>".
// The function decides whether an anonymous principal is acceptable; return
// ErrUnauthenticated or ErrForbidden (wrapped or bare) to select the status.
func Custom(name string, f func(context.Context, Principal) error) Requirement {
	return customReq{name: name, f: f}
}

func (r customReq) Check(ctx context.Context, p Principal) error {
	if r.f == nil {
		return nil
	}
	return r.f(ctx, p)
}

func (r customReq) Describe() Description { return Description{Custom: []string{r.name}} }
