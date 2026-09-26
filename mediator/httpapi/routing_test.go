package httpapi_test

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
)

// ---- types for RouteOf ----

type mapQuery struct {
	mediator.Query[int]
	M map[string]int `json:"m"`
}

type ptrScalarQuery struct {
	mediator.Query[int]
	P *uuid.UUID `json:"p"`
	T *time.Time `json:"t"`
	L []*int     `json:"l"`
}

type sliceStructQuery struct {
	mediator.Query[int]
	L []orderLine `json:"l"`
}

type bytesQuery struct {
	mediator.Query[int]
	B []byte `json:"b"`
}

type badBindQuery struct {
	mediator.Query[int]
	A string `path:"a" query:"a"`
}

type evt struct{ mediator.Event }

func TestRouteOf(t *testing.T) {
	m := mediator.New()
	reg[createOrder](t, m)
	reg[listOrders](t, m)
	reg[searchOrders](t, m)
	reg[ping](t, m)
	reg[mapQuery](t, m)
	reg[ptrScalarQuery](t, m)
	reg[sliceStructQuery](t, m)
	reg[bytesQuery](t, m)
	reg[badBindQuery](t, m)
	regStream[countTo](t, m)
	mustBuild(t, m)

	rows := []struct {
		typ  reflect.Type
		want httpapi.Route
	}{
		{reflect.TypeFor[createOrder](), httpapi.Route{Method: "POST", Path: "/orders", Status: 201}},
		{reflect.TypeFor[listOrders](), httpapi.Route{Method: "GET", Path: "/rpc/listOrders"}},
		{reflect.TypeFor[searchOrders](), httpapi.Route{Method: "POST", Path: "/rpc/searchOrders"}},
		{reflect.TypeFor[ping](), httpapi.Route{Method: "POST", Path: "/rpc/ping"}},
		{reflect.TypeFor[mapQuery](), httpapi.Route{Method: "POST", Path: "/rpc/mapQuery"}},
		{reflect.TypeFor[ptrScalarQuery](), httpapi.Route{Method: "GET", Path: "/rpc/ptrScalarQuery"}},
		{reflect.TypeFor[sliceStructQuery](), httpapi.Route{Method: "POST", Path: "/rpc/sliceStructQuery"}},
		{reflect.TypeFor[bytesQuery](), httpapi.Route{Method: "POST", Path: "/rpc/bytesQuery"}},
		{reflect.TypeFor[badBindQuery](), httpapi.Route{Method: "POST", Path: "/rpc/badBindQuery"}},
		{reflect.TypeFor[countTo](), httpapi.Route{Method: "GET", Path: "/rpc/countTo"}},
	}
	for _, row := range rows {
		info, ok := m.InfoOf(row.typ)
		if !ok {
			t.Fatalf("%s not registered", row.typ)
		}
		if got := httpapi.RouteOf(info); got != row.want {
			t.Errorf("%s: RouteOf = %+v, want %+v", row.typ, got, row.want)
		}
	}
	// Non-request kinds have no route.
	ev := &mediator.RequestInfo{Kind: mediator.KindNotification, RequestType: reflect.TypeFor[evt](), Name: "evt"}
	if got := httpapi.RouteOf(ev); got != (httpapi.Route{}) {
		t.Errorf("event route = %+v", got)
	}
}

// ---- types for BindingsOf ----

type Base struct {
	ID string `json:"id" path:"id"`
}

type embedded struct {
	mediator.Query[int]
	Base
	Hidden     string `json:"-"`
	unexported int
	Page       int `json:"page,omitempty" query:"page"`
	NoTag      string
	Empty      string   `json:"e" query:""`
	Hdr        []string `header:"X-List"`
	Opt        string   `json:",omitempty"`
}

type embPtr struct {
	mediator.Query[int]
	*Base
}

type namedEmbed struct {
	mediator.Query[int]
	Base `json:"base"`
}

type unexpPtr struct {
	mediator.Query[int]
	*base
}

type RecA struct {
	*RecB
	A int
}

type RecB struct {
	*RecA
	B int
}

type recQuery struct {
	mediator.Query[int]
	RecA
}

type twoSources struct {
	A string `path:"a" query:"a"`
}

type pathSlice struct {
	A []string `path:"a"`
}

type queryStruct struct {
	A orderLine `query:"a"`
}

type headerMap struct {
	A map[string]string `header:"A"`
}

type dupQuery struct {
	A string `query:"q"`
	B string `query:"q"`
}

type dupHeader struct {
	A string `header:"x-a"`
	B string `header:"X-A"`
}

func names(bs []httpapi.Binding) []string {
	var out []string
	for _, b := range bs {
		out = append(out, b.Source.String()+":"+b.Name+"/"+b.JSONName)
	}
	return out
}

func TestBindingsOf(t *testing.T) {
	rows := []struct {
		name string
		typ  reflect.Type
		want []string
		errs []string
	}{
		{"flattened and tagged", reflect.TypeFor[embedded](),
			[]string{"path:id/id", "query:page/page", "body:NoTag/NoTag", "query:e/e", "header:X-List/Hdr", "body:Opt/Opt"}, nil},
		{"pointer to type", reflect.TypeFor[*embedded](),
			[]string{"path:id/id", "query:page/page", "body:NoTag/NoTag", "query:e/e", "header:X-List/Hdr", "body:Opt/Opt"}, nil},
		{"embedded pointer", reflect.TypeFor[embPtr](), []string{"path:id/id"}, nil},
		{"embedded with json name", reflect.TypeFor[namedEmbed](), []string{"body:base/base"}, nil},
		{"unexported embedded pointer skipped", reflect.TypeFor[unexpPtr](), nil, nil},
		{"recursive embedding terminates", reflect.TypeFor[recQuery](), []string{"body:B/B", "body:A/A"}, nil},
		{"fixture command", reflect.TypeFor[createOrder](),
			[]string{"body:customerId/customerId", "body:lines/lines", "body:when/when", "header:X-Tenant/tenant"}, nil},
		{"two sources", reflect.TypeFor[twoSources](), nil, []string{"2 binding sources"}},
		{"path slice", reflect.TypeFor[pathSlice](), nil, []string{"bound from the path but []string is not a scalar"}},
		{"query struct", reflect.TypeFor[queryStruct](), nil, []string{"bound from the query but"}},
		{"header map", reflect.TypeFor[headerMap](), nil, []string{"bound from the header but"}},
		{"duplicate query", reflect.TypeFor[dupQuery](), []string{"query:q/A", "query:q/B"}, []string{`fields A and B both bind query "q"`}},
		{"duplicate header case-insensitive", reflect.TypeFor[dupHeader](), []string{"header:x-a/A", "header:X-A/B"}, []string{`both bind header "X-A"`}},
		{"not a struct", reflect.TypeFor[int](), nil, []string{"is not a struct"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			got, err := httpapi.BindingsOf(row.typ)
			if !reflect.DeepEqual(names(got), row.want) {
				t.Errorf("bindings = %v, want %v", names(got), row.want)
			}
			if len(row.errs) == 0 {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, want := range row.errs {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
		})
	}
	// Index paths address flattened fields.
	bs, _ := httpapi.BindingsOf(reflect.TypeFor[embedded]())
	if got := bs[0].Field.Index; !reflect.DeepEqual(got, []int{1, 0}) {
		t.Errorf("index of embedded id = %v", got)
	}
}

// ---- types for BuildCheck ----

type vMissingPath struct {
	mediator.Command[mediator.Void]
}

func (vMissingPath) Route() httpapi.Route { return httpapi.Route{Method: "POST", Path: "/v/{id}"} }

type vExtraPath struct {
	mediator.Command[mediator.Void]
	ID string `path:"id"`
}

func (vExtraPath) Route() httpapi.Route { return httpapi.Route{Method: "POST", Path: "/v"} }

type vGetBody struct {
	mediator.Query[int]
	Name string `json:"name"`
}

func (vGetBody) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/v"} }

type vBadMethod struct {
	mediator.Command[mediator.Void]
}

func (vBadMethod) Route() httpapi.Route { return httpapi.Route{Method: "get", Path: "/v"} }

type vNoSlash struct {
	mediator.Command[mediator.Void]
}

func (vNoSlash) Route() httpapi.Route { return httpapi.Route{Method: "POST", Path: "v"} }

type vSpace struct {
	mediator.Command[mediator.Void]
}

func (vSpace) Route() httpapi.Route { return httpapi.Route{Method: "POST", Path: "/v x"} }

type vReserved struct{ mediator.Query[int] }

func (vReserved) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/healthz"} }

type vStatus struct {
	mediator.Command[mediator.Void]
}

func (vStatus) Route() httpapi.Route { return httpapi.Route{Method: "POST", Path: "/v", Status: 500} }

type vConflictA struct {
	mediator.Query[int]
	A string `path:"a"`
}

func (vConflictA) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/c/{a}"} }

type vConflictB struct {
	mediator.Query[int]
	B string `path:"b"`
}

func (vConflictB) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/c/{b}"} }

type vBadPattern struct{ mediator.Query[int] }

func (vBadPattern) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/v/{"} }

type vBinding struct {
	mediator.Command[mediator.Void]
	A string `path:"a" query:"a"`
}

func (vBinding) Route() httpapi.Route { return httpapi.Route{Method: "POST", Path: "/v/{a}"} }

type vWildcard struct {
	mediator.Query[int]
	Rest string `path:"rest"`
}

func (vWildcard) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/files/{rest...}"} }

type vDollar struct{ mediator.Query[int] }

func (vDollar) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/exact/{$}"} }

type vDelete struct {
	mediator.Command[mediator.Void]
	Reason string `json:"reason"`
}

func (vDelete) Route() httpapi.Route { return httpapi.Route{Method: "DELETE", Path: "/v/{id}"} }

func TestBuildCheck_Violations(t *testing.T) {
	rows := []struct {
		name string
		reg  func(*testing.T, *mediator.Mediator)
		want string
	}{
		{"missing path field", func(t *testing.T, m *mediator.Mediator) { reg[vMissingPath](t, m) }, `path parameter {id} has no field tagged path:"id"`},
		{"extra path field", func(t *testing.T, m *mediator.Mediator) { reg[vExtraPath](t, m) }, `tagged path:"id" but the pattern "/v" has no {id}`},
		{"get with body", func(t *testing.T, m *mediator.Mediator) { reg[vGetBody](t, m) }, "GET route has body-bound fields [Name]"},
		{"bad method", func(t *testing.T, m *mediator.Mediator) { reg[vBadMethod](t, m) }, `method "get" is not one of`},
		{"no slash", func(t *testing.T, m *mediator.Mediator) { reg[vNoSlash](t, m) }, `path "v" must start with /`},
		{"whitespace", func(t *testing.T, m *mediator.Mediator) { reg[vSpace](t, m) }, `path "/v x" contains whitespace`},
		{"reserved", func(t *testing.T, m *mediator.Mediator) { reg[vReserved](t, m) }, `path "/healthz" is reserved`},
		{"status", func(t *testing.T, m *mediator.Mediator) { reg[vStatus](t, m) }, "status 500 is not 2xx"},
		{"conflict", func(t *testing.T, m *mediator.Mediator) { reg[vConflictA](t, m); reg[vConflictB](t, m) }, "conflicts with pattern"},
		{"bad pattern", func(t *testing.T, m *mediator.Mediator) { reg[vBadPattern](t, m) }, `pattern "GET /v/{"`},
		{"binding", func(t *testing.T, m *mediator.Mediator) { reg[vBinding](t, m) }, "2 binding sources"},
		{"delete with missing path", func(t *testing.T, m *mediator.Mediator) { reg[vDelete](t, m) }, `path parameter {id}`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			m := mediator.New()
			row.reg(t, m)
			mustBuild(t, m)
			err := httpapi.BuildCheck(m)
			if err == nil || !strings.Contains(err.Error(), row.want) {
				t.Fatalf("BuildCheck = %v, want %q", err, row.want)
			}
			if _, err := httpapi.New(m, httpapi.Config{}); err == nil {
				t.Fatal("New accepted an invalid registry")
			}
		})
	}
	t.Run("all at once", func(t *testing.T) {
		m := mediator.New()
		for _, row := range rows {
			row.reg(t, m)
		}
		mustBuild(t, m)
		err := httpapi.BuildCheck(m)
		if err == nil {
			t.Fatal("expected errors")
		}
		for _, row := range rows {
			if !strings.Contains(err.Error(), row.want) {
				t.Errorf("joined error lacks %q", row.want)
			}
		}
	})
	t.Run("valid", func(t *testing.T) {
		m := mediator.New()
		reg[vWildcard](t, m)
		reg[vDollar](t, m)
		reg[createOrder](t, m)
		reg[getOrder](t, m)
		reg[listOrders](t, m)
		reg[searchOrders](t, m)
		regStream[countTo](t, m)
		mustBuild(t, m)
		if err := httpapi.BuildCheck(m); err != nil {
			t.Fatal(err)
		}
		if _, err := httpapi.New(m, httpapi.Config{Prefix: "api/"}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("bad prefix", func(t *testing.T) {
		m := mediator.New()
		reg[ping](t, m)
		mustBuild(t, m)
		for _, prefix := range []string{"/a b", "/a//b", "/{x}", "a\tb"} {
			if _, err := httpapi.New(m, httpapi.Config{Prefix: prefix}); err == nil || !strings.Contains(err.Error(), "prefix") {
				t.Errorf("prefix %q: New = %v", prefix, err)
			}
		}
	})
}

func TestNew_NotBuilt(t *testing.T) {
	m := mediator.New()
	reg[ping](t, m)
	if _, err := httpapi.New(m, httpapi.Config{}); err == nil || !strings.Contains(err.Error(), "not built") {
		t.Fatalf("New = %v", err)
	}
}

func TestRoutes(t *testing.T) {
	f := newFixture(t, httpapi.Config{Prefix: "api"})
	cfg := f.srv.Config()
	if cfg.Prefix != "/api" || cfg.MaxBodyBytes != 1<<20 || cfg.KeepAlive != 15*time.Second || cfg.ReadyMaxLag != 60*time.Second || cfg.Logger == nil {
		t.Errorf("defaults not applied: %+v", cfg)
	}
	routes := f.srv.Routes()
	byKey := map[string]httpapi.RouteInfo{}
	var keys []string
	for _, r := range routes {
		k := r.Method + " " + r.Path
		byKey[k] = r
		keys = append(keys, k)
	}
	// Sorted by path then method.
	for i := 1; i < len(routes); i++ {
		a, b := routes[i-1], routes[i]
		if a.Path > b.Path || (a.Path == b.Path && a.Method > b.Method) {
			t.Errorf("routes not sorted at %d: %v", i, keys)
		}
	}
	want := map[string]int{
		"POST /api/orders":             201,
		"GET /api/orders/{orderId}":    200,
		"DELETE /api/orders/{orderId}": 204,
		"GET /api/rpc/listOrders":      200,
		"POST /api/rpc/searchOrders":   200,
		"POST /api/rpc/ping":           204,
		"POST /api/quiet":              204,
		"GET /api/rpc/countTo":         200,
		"GET /api/tail":                200,
		"GET /api/nested/{id}":         200,
		"GET /api/slashy/{$}":          200,
		"POST /api/rpc/whoami":         200,
		"GET /api/rpc/peek":            200,
		"POST /api/rpc/fail":           204,
		"POST /api/rpc/weird":          200,
		"POST /api/rpc/slow":           204,
	}
	for k, status := range want {
		r, ok := byKey[k]
		if !ok {
			t.Errorf("missing route %s in %v", k, keys)
			continue
		}
		if r.Status != status {
			t.Errorf("%s status = %d, want %d", k, r.Status, status)
		}
		if r.Info == nil {
			t.Errorf("%s has no info", k)
		}
	}
	if len(routes) != len(want) {
		t.Errorf("%d routes, want %d: %v", len(routes), len(want), keys)
	}
	// RPC GET query fields are reported as query-bound.
	for _, b := range byKey["GET /api/rpc/listOrders"].Bindings {
		if b.Source != httpapi.SourceQuery || b.Name != b.JSONName {
			t.Errorf("listOrders binding %+v should be query-bound under its JSON name", b)
		}
	}
	// Explicit routes keep their sources.
	var srcs []string
	for _, b := range byKey["GET /api/orders/{orderId}"].Bindings {
		srcs = append(srcs, b.Source.String())
	}
	if !reflect.DeepEqual(srcs, []string{"path", "query", "query", "header"}) {
		t.Errorf("getOrder sources = %v", srcs)
	}
	if byKey["POST /api/orders"].Info.Name != "createOrder" {
		t.Errorf("info name = %q", byKey["POST /api/orders"].Info.Name)
	}
	// Routes returns a copy.
	routes[0].Method = "X"
	if f.srv.Routes()[0].Method == "X" {
		t.Error("Routes exposed internal state")
	}
	// Source names.
	if httpapi.SourceBody.String() != "body" || httpapi.SourcePath.String() != "path" {
		t.Error("Source.String")
	}
	// Handler returns the server itself.
	if f.srv.Handler() != http.Handler(f.srv) {
		t.Error("Handler")
	}
	// Prefix and exact-match trailing slash work end to end.
	if rec := f.do(t, "GET", "/api/slashy/", ""); rec.Code != 200 {
		t.Errorf("GET /api/slashy/ = %d %s", rec.Code, rec.Body)
	}
	if rec := f.do(t, "GET", "/api/slashy/more", ""); rec.Code != 404 {
		t.Errorf("GET /api/slashy/more = %d, want 404", rec.Code)
	}
	if rec := f.do(t, "POST", "/orders", `{}`); rec.Code != 404 {
		t.Errorf("unprefixed path = %d, want 404", rec.Code)
	}
}

func TestContextHelpers(t *testing.T) {
	// The exported context helpers behave outside a request.
	ctx := context.Background()
	if httpapi.RemoteAddr(ctx) != "" {
		t.Error("RemoteAddr outside a request")
	}
	if _, ok := httpapi.CacheResultFrom(ctx); ok {
		t.Error("CacheResultFrom without holder")
	}
	httpapi.SetCacheResult(ctx, httpapi.CacheHit) // no-op, no panic
	ctx = httpapi.WithCacheResultHolder(ctx)
	if _, ok := httpapi.CacheResultFrom(ctx); ok {
		t.Error("CacheResultFrom before set")
	}
	httpapi.SetCacheResult(ctx, httpapi.CacheMiss)
	httpapi.SetCacheResult(ctx, httpapi.CacheBypass)
	if v, ok := httpapi.CacheResultFrom(ctx); !ok || v != httpapi.CacheBypass {
		t.Errorf("CacheResultFrom = %q %v", v, ok)
	}
	if got := httpapi.RemoteAddr(httpapi.WithRemoteAddr(ctx, "10.0.0.1")); got != "10.0.0.1" {
		t.Errorf("RemoteAddr = %q", got)
	}
}
