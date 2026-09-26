package authz_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator/authz"
)

var (
	anon  = authz.Principal{}
	admin = authz.Principal{Subject: "u1", Roles: []string{"admin", "ops"}, Permissions: []string{"orders:read", "orders:write"}}
	user  = authz.Principal{Subject: "u2", Roles: []string{"user"}, Permissions: []string{"orders:read"}}
	noSub = authz.Principal{Roles: []string{"admin"}, Permissions: []string{"orders:read"}, Tenant: "t"}
)

func TestPrincipal(t *testing.T) {
	if !anon.IsAnonymous() || admin.IsAnonymous() || !noSub.IsAnonymous() {
		t.Fatal("IsAnonymous is by subject only")
	}
	if !admin.HasRole("ops") || admin.HasRole("user") || anon.HasRole("") || admin.HasRole("") {
		t.Fatal("HasRole")
	}
	if !user.HasPermission("orders:read") || user.HasPermission("orders:write") || anon.HasPermission("x") {
		t.Fatal("HasPermission")
	}
	ctx := context.Background()
	if p := authz.PrincipalFrom(ctx); !p.IsAnonymous() || p.Roles != nil {
		t.Fatal("no principal is anonymous")
	}
	got := authz.PrincipalFrom(authz.WithPrincipal(ctx, admin))
	if !reflect.DeepEqual(got, admin) {
		t.Fatalf("round trip: %+v", got)
	}
	if got := authz.PrincipalFrom(authz.WithPrincipal(authz.WithPrincipal(ctx, admin), anon)); !got.IsAnonymous() {
		t.Fatal("inner value wins")
	}
}

func TestRequirements(t *testing.T) {
	failing := authz.Custom("deny", func(context.Context, authz.Principal) error { return authz.ErrForbidden })
	unauth := authz.Custom("unauth", func(context.Context, authz.Principal) error { return authz.ErrUnauthenticated })
	passing := authz.Custom("allow", func(context.Context, authz.Principal) error { return nil })
	cases := []struct {
		name string
		req  authz.Requirement
		p    authz.Principal
		want error // nil, ErrUnauthenticated, or ErrForbidden
	}{
		{"role any-of hit", authz.Role("ops", "admin"), admin, nil},
		{"role any-of first", authz.Role("admin"), admin, nil},
		{"role miss", authz.Role("root", "user"), admin, authz.ErrForbidden},
		{"role empty list", authz.Role(), admin, authz.ErrForbidden},
		{"role anonymous", authz.Role("admin"), anon, authz.ErrUnauthenticated},
		{"role no subject", authz.Role("admin"), noSub, authz.ErrUnauthenticated},
		{"permission all-of hit", authz.Permission("orders:read", "orders:write"), admin, nil},
		{"permission all-of partial", authz.Permission("orders:read", "orders:write"), user, authz.ErrForbidden},
		{"permission empty list", authz.Permission(), user, nil},
		{"permission anonymous", authz.Permission("orders:read"), anon, authz.ErrUnauthenticated},
		{"authenticated", authz.Authenticated(), user, nil},
		{"authenticated anonymous", authz.Authenticated(), anon, authz.ErrUnauthenticated},
		{"any empty", authz.Any(), anon, nil},
		{"any one passes", authz.Any(authz.Role("root"), authz.Permission("orders:read")), user, nil},
		{"any short-circuits", authz.Any(passing, failing), anon, nil},
		{"any all forbidden", authz.Any(authz.Role("root"), failing), user, authz.ErrForbidden},
		{"any all unauthenticated", authz.Any(authz.Role("x"), authz.Authenticated(), unauth), anon, authz.ErrUnauthenticated},
		{"any mixed is forbidden", authz.Any(unauth, failing), anon, authz.ErrForbidden},
		{"all empty", authz.All(), anon, nil},
		{"all passes", authz.All(authz.Authenticated(), authz.Role("user"), authz.Permission("orders:read")), user, nil},
		{"all first failure wins", authz.All(authz.Role("root"), unauth), user, authz.ErrForbidden},
		{"all unauthenticated", authz.All(authz.Authenticated(), failing), anon, authz.ErrUnauthenticated},
		{"custom nil func", authz.Custom("noop", nil), anon, nil},
		{"custom passes", passing, anon, nil},
		{"custom forbids", failing, admin, authz.ErrForbidden},
		{"nested", authz.All(authz.Any(authz.Role("root"), authz.All(authz.Role("user"), passing)), authz.Permission("orders:read")), user, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := authz.WithPrincipal(context.Background(), c.p)
			err := authz.Check(ctx, c.req)
			if c.want == nil && err != nil {
				t.Fatalf("got %v", err)
			}
			if c.want != nil && !errors.Is(err, c.want) {
				t.Fatalf("got %v, want %v", err, c.want)
			}
			if c.want != nil && errors.Is(err, authz.ErrForbidden) && errors.Is(err, authz.ErrUnauthenticated) {
				t.Fatalf("error must be one or the other: %v", err)
			}
			// Check uses the principal in ctx; calling the requirement
			// directly with the same principal agrees.
			if direct := c.req.Check(ctx, c.p); (direct == nil) != (err == nil) {
				t.Fatalf("direct %v vs Check %v", direct, err)
			}
		})
	}
	if authz.Check(context.Background(), nil) != nil {
		t.Fatal("nil requirement passes")
	}
	// Messages carry the missing role or permission.
	err := authz.Role("a", "b").Check(context.Background(), user)
	if err == nil || err.Error() != "authz: forbidden: requires one of roles [a, b]" {
		t.Fatal(err)
	}
	err = authz.Permission("orders:write").Check(context.Background(), user)
	if err == nil || err.Error() != `authz: forbidden: missing permission "orders:write"` {
		t.Fatal(err)
	}
	// Requirements do not alias the caller's slice.
	names := []string{"a"}
	r := authz.Role(names...)
	names[0] = "z"
	if !reflect.DeepEqual(r.Describe().Roles, []string{"a"}) {
		t.Fatal("Role must clone")
	}
	d := r.Describe()
	d.Roles[0] = "q"
	if r.Describe().Roles[0] != "a" {
		t.Fatal("Describe must clone")
	}
}

func TestDescribeAndScopes(t *testing.T) {
	cases := []struct {
		name   string
		req    authz.Requirement
		desc   authz.Description
		scopes []string
		empty  bool
	}{
		{"role", authz.Role("b", "a"), authz.Description{Roles: []string{"b", "a"}}, []string{"role:a", "role:b"}, false},
		{"permission", authz.Permission("orders:write", "orders:read"), authz.Description{Permissions: []string{"orders:write", "orders:read"}}, []string{"orders:read", "orders:write"}, false},
		{"authenticated", authz.Authenticated(), authz.Description{Authenticated: true}, nil, false},
		{"custom", authz.Custom("owner", nil), authz.Description{Custom: []string{"owner"}}, []string{"custom:owner"}, false},
		{"any empty", authz.Any(), authz.Description{}, nil, true},
		{"all empty", authz.All(), authz.Description{}, nil, true},
		{"any", authz.Any(authz.Role("a"), authz.Permission("p")), authz.Description{Any: []authz.Description{{Roles: []string{"a"}}, {Permissions: []string{"p"}}}}, []string{"p", "role:a"}, false},
		{"all with duplicates", authz.All(authz.Role("a"), authz.Any(authz.Role("a"), authz.Custom("c", nil)), authz.Permission("p", "p")), authz.Description{All: []authz.Description{{Roles: []string{"a"}}, {Any: []authz.Description{{Roles: []string{"a"}}, {Custom: []string{"c"}}}}, {Permissions: []string{"p", "p"}}}}, []string{"custom:c", "p", "role:a"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := c.req.Describe()
			if !reflect.DeepEqual(d, c.desc) {
				t.Fatalf("Describe = %+v, want %+v", d, c.desc)
			}
			if got := d.Scopes(); !reflect.DeepEqual(got, c.scopes) {
				t.Fatalf("Scopes = %v, want %v", got, c.scopes)
			}
			if d.Empty() != c.empty {
				t.Fatalf("Empty = %v", d.Empty())
			}
		})
	}
	for _, d := range []authz.Description{
		{Roles: []string{"a"}}, {Permissions: []string{"p"}}, {Authenticated: true}, {Custom: []string{"c"}},
		{Any: []authz.Description{{}}}, {All: []authz.Description{{}}},
	} {
		if d.Empty() {
			t.Fatalf("%+v must not be empty", d)
		}
	}
	if !(authz.Description{}).Empty() {
		t.Fatal("zero description is empty")
	}
}
