package behavior_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
)

// customCmd carries a Custom requirement whose check the test scripts.
type customCmd struct {
	mediator.Command[mediator.Void]
	ID string `json:"id"`
}

var customCheck func(context.Context, authz.Principal) error

func (customCmd) Requires() authz.Requirement {
	return authz.Custom("owner", func(ctx context.Context, p authz.Principal) error { return customCheck(ctx, p) })
}

// nilReqCmd declares the trait but no requirement.
type nilReqCmd struct {
	mediator.Command[mediator.Void]
}

func (nilReqCmd) Requires() authz.Requirement { return nil }

// anyReqCmd declares an empty Any, which passes for everyone.
type anyReqCmd struct {
	mediator.Command[mediator.Void]
}

func (anyReqCmd) Requires() authz.Requirement { return authz.Any() }

func authzMediator(t *testing.T, cfg behavior.Config) *mediator.Mediator {
	t.Helper()
	m := mediator.New()
	register(t, m, defaultHooks())
	must(t, mediator.HandleFunc(m, func(context.Context, customCmd) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(t, mediator.HandleFunc(m, func(context.Context, nilReqCmd) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(t, mediator.HandleFunc(m, func(context.Context, anyReqCmd) (mediator.Void, error) { return mediator.Void{}, nil }))
	must(t, mediator.Use(m, behavior.NewAuthorization(cfg), mediator.Where(func(i *mediator.RequestInfo) bool { return i.Traits.Requires })))
	must(t, m.Build())
	return m
}

func TestAuthorization_Table(t *testing.T) {
	m := authzMediator(t, behavior.Config{})
	anon := context.Background()
	user := authz.WithPrincipal(anon, authz.Principal{Subject: "bob", Roles: []string{"user"}})
	adm := admin(anon)
	plain := errors.New("not the owner")
	cases := []struct {
		name  string
		ctx   context.Context
		req   any
		check func(context.Context, authz.Principal) error
		code  mediator.Code
		msg   string
	}{
		{"role, anonymous", anon, richCmd{ID: "a"}, nil, mediator.CodeUnauthorized, "authentication required"},
		{"role, wrong role", user, richCmd{ID: "a"}, nil, mediator.CodeForbidden, "requires one of roles [admin]"},
		{"role, admin", adm, richCmd{ID: "a"}, nil, "", ""},
		{"custom, anonymous, plain error", anon, customCmd{}, func(context.Context, authz.Principal) error { return plain }, mediator.CodeUnauthorized, ""},
		{"custom, principal, plain error", user, customCmd{}, func(context.Context, authz.Principal) error { return plain }, mediator.CodeForbidden, "not the owner"},
		{"custom, principal, unauthenticated", user, customCmd{}, func(context.Context, authz.Principal) error { return authz.ErrUnauthenticated }, mediator.CodeUnauthorized, ""},
		{"custom, principal, forbidden", user, customCmd{}, func(context.Context, authz.Principal) error { return authz.ErrForbidden }, mediator.CodeForbidden, "forbidden"},
		{"custom, passes", anon, customCmd{}, func(context.Context, authz.Principal) error { return nil }, "", ""},
		{"nil requirement", anon, nilReqCmd{}, nil, "", ""},
		{"empty Any, anonymous", anon, anyReqCmd{}, nil, "", ""},
		{"no trait", anon, plainCmd{ID: "x"}, nil, "", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			customCheck = c.check
			_, err := m.SendAny(c.ctx, c.req)
			if c.code == "" {
				if err != nil {
					t.Fatalf("unexpected %v", err)
				}
				return
			}
			codeIs(t, err, c.code)
			var e *mediator.Error
			if !errors.As(err, &e) || !strings.Contains(e.Message, c.msg) {
				t.Fatalf("message %q does not contain %q", e.Message, c.msg)
			}
		})
	}
}

func TestAuthorization_RequireAuthByDefault(t *testing.T) {
	m := mediator.New()
	register(t, m, defaultHooks())
	must(t, behavior.UseStandard(m, behavior.Config{RequireAuthByDefault: true}))
	err := m.Build()
	if err == nil {
		t.Fatal("want build error")
	}
	for _, offender := range []string{"plainCmd", "cachedQuery", "plainQuery", "numStream", "noUowCmd"} {
		if !strings.Contains(err.Error(), offender) {
			t.Errorf("offender %s not reported: %v", offender, err)
		}
	}
	for _, ok := range []string{"richCmd", "thingEvent", "thingStored"} {
		if strings.Contains(err.Error(), ok+" ") {
			t.Errorf("%s wrongly reported: %v", ok, err)
		}
	}

	// Every request declares a requirement: Build passes.
	m = mediator.New()
	must(t, mediator.HandleFunc(m, func(context.Context, richCmd) (cmdResult, error) { return cmdResult{}, nil }))
	must(t, mediator.OnFunc(m, func(context.Context, thingEvent) error { return nil }))
	must(t, behavior.UseStandard(m, behavior.Config{RequireAuthByDefault: true}))
	must(t, m.Build())
}

func TestAuthorization_DirectNonRequirer(t *testing.T) {
	b := behavior.NewAuthorization(behavior.Config{})
	res, err := b.Handle(context.Background(), plainCmd{}, &mediator.RequestInfo{}, func(context.Context, any) (any, error) { return 1, nil })
	if err != nil || res != 1 || b.Name() != behavior.Authorization {
		t.Fatal(res, err, b.Name())
	}
}
