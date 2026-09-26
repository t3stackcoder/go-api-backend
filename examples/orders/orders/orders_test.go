package orders_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/t3stackcoder/go-api-backend/examples/orders/orders"
	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

const (
	customerID = "018f1c2e-4d3a-7b5c-9e8f-0a1b2c3d4e5f"
	orderID    = "018f1c2e-4d3a-7b5c-9e8f-0a1b2c3d4e60"
)

func registry(t *testing.T) *mediator.Mediator {
	t.Helper()
	m, err := orders.Registry()
	if err != nil {
		t.Fatalf("Registry: %v", err)
	}
	return m
}

func info(t *testing.T, m *mediator.Mediator, v any) *mediator.RequestInfo {
	t.Helper()
	i, ok := m.InfoOf(reflect.TypeOf(v))
	if !ok {
		t.Fatalf("%T is not registered", v)
	}
	return i
}

// principal returns a context carrying a principal with the permissions.
func principal(perms ...string) context.Context {
	return authz.WithPrincipal(context.Background(), authz.Principal{Subject: "alice", Permissions: perms})
}

func TestRegistry_NamesAndOrder(t *testing.T) {
	m := registry(t)
	want := map[string]mediator.Kind{
		"CreateOrder": mediator.KindCommand, "AddLine": mediator.KindCommand, "SubmitOrder": mediator.KindCommand,
		"ReserveStock": mediator.KindCommand, "GetOrder": mediator.KindQuery, "ListOrders": mediator.KindQuery,
		"WatchOrder": mediator.KindStream,
	}
	got := map[string]mediator.Kind{}
	groups := map[string]string{}
	for _, n := range m.Names() {
		switch n.Kind {
		case mediator.KindConsumer:
			groups[n.Group] = n.Name
		case mediator.KindNotification:
			if n.Name != "OrderSubmitted" || n.Topic != "OrderSubmitted" {
				t.Errorf("event %+v", n)
			}
		default:
			got[n.Name] = n.Kind
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("requests %v, want %v", got, want)
	}
	if groups[orders.GroupInventory] != "OrderSubmitted" || groups[orders.GroupReadModel] != "OrderSubmitted" || len(groups) != 2 {
		t.Errorf("consumers %v", groups)
	}
	if topics := m.Topics(); len(topics) != 1 || topics[0] != "OrderSubmitted" {
		t.Errorf("topics %v", topics)
	}
	// No store: the standard set without UnitOfWork, Idempotency, and Inbox;
	// no cache or limiter either.
	order := strings.Join(m.Order(), ",")
	for _, absent := range []string{mediator.NameUnitOfWork, mediator.NameIdempotency, mediator.NameInbox, mediator.NameCache, mediator.NameRateLimit} {
		if strings.Contains(order, absent) {
			t.Errorf("order %s contains %s without infrastructure", order, absent)
		}
	}
	for _, present := range []string{mediator.NameRecovery, mediator.NameAuthorization, mediator.NameValidation, mediator.NameRetry} {
		if !strings.Contains(order, present) {
			t.Errorf("order %s lacks %s", order, present)
		}
	}
	if _, ok := m.Lookup("debug.Sleep"); ok {
		t.Error("Registry must not register the debug requests")
	}
}

func TestTraits(t *testing.T) {
	m := registry(t)
	check := func(v any, want mediator.Traits) {
		t.Helper()
		got := info(t, m, v).Traits
		if got != want {
			t.Errorf("%T traits\n got %+v\nwant %+v", v, got, want)
		}
	}
	check(orders.CreateOrder{}, mediator.Traits{Requires: true, Invalidates: true, RetryPolicy: true, Validate: true})
	check(orders.AddLine{}, mediator.Traits{Requires: true, Invalidates: true})
	check(orders.SubmitOrder{}, mediator.Traits{Requires: true, Invalidates: true, RetryPolicy: true})
	check(orders.GetOrder{}, mediator.Traits{Requires: true, CacheTags: true, CacheTTL: true})
	check(orders.ListOrders{}, mediator.Traits{Requires: true, CacheTags: true})
	check(orders.WatchOrder{}, mediator.Traits{Requires: true, Timeout: true, NoUnitOfWork: true, Validate: true})
	check(orders.ReserveStock{}, mediator.Traits{Requires: true, IdempotencyKey: true})
	if ev, ok := m.InfoOf(reflect.TypeFor[orders.OrderSubmitted]()); ok {
		t.Errorf("events are not requests: %v", ev)
	}
	for _, e := range m.Events() {
		if !e.Traits.Durable || e.Handlers != 1 {
			t.Errorf("OrderSubmitted %+v: want durable with one in-process handler", e)
		}
	}
	// Values of the traits.
	if got := (orders.AddLine{OrderID: orderID}).Invalidates(); !reflect.DeepEqual(got, []string{"order:" + orderID, orders.TagOrdersList}) {
		t.Errorf("AddLine.Invalidates %v", got)
	}
	if got := (orders.SubmitOrder{OrderID: orderID}).Invalidates(); !reflect.DeepEqual(got, []string{"order:" + orderID, orders.TagOrdersList}) {
		t.Errorf("SubmitOrder.Invalidates %v", got)
	}
	if got := (orders.CreateOrder{}).Invalidates(); !reflect.DeepEqual(got, []string{orders.TagOrdersList}) {
		t.Errorf("CreateOrder.Invalidates %v", got)
	}
	if got := (orders.GetOrder{OrderID: orderID}).CacheTags(); !reflect.DeepEqual(got, []string{orders.TagOrder(orderID)}) {
		t.Errorf("GetOrder.CacheTags %v", got)
	}
	if (orders.GetOrder{}).CacheTTL() != 5*time.Minute || (orders.WatchOrder{}).Timeout() != orders.WatchTimeout {
		t.Error("ttl or timeout")
	}
	if (orders.ListOrders{}).CacheTags()[0] != orders.TagOrdersList {
		t.Error("ListOrders.CacheTags")
	}
	if p := (orders.CreateOrder{}).RetryPolicy(); p.MaxAttempts != 3 {
		t.Errorf("retry %+v", p)
	}
	if o := (orders.SubmitOrder{}).TxOptions(); o.Isolation != pgx.Serializable {
		t.Errorf("tx options %+v", o)
	}
	if (orders.ReserveStock{RequestID: "r1"}).IdempotencyKey() != "r1" {
		t.Error("ReserveStock.IdempotencyKey")
	}
	if (orders.OrderSubmitted{OrderID: orderID}).StreamKey() != orderID {
		t.Error("StreamKey")
	}
	if (orders.OrderEvent{ID: 42}).EventID() != "42" {
		t.Error("EventID")
	}
	if s := (orders.CreateOrder{}).Requires().Describe().Scopes(); !reflect.DeepEqual(s, []string{orders.PermissionOrdersWrite}) {
		t.Errorf("scopes %v", s)
	}
	if !(orders.GetOrder{}).Requires().Describe().Authenticated {
		t.Error("GetOrder requires authentication")
	}
	if d := (orders.AddLine{}).Describe(); d.Summary == "" || len(d.Errors) != 2 {
		t.Errorf("describe %+v", d)
	}
}

func TestRoutes(t *testing.T) {
	m := registry(t)
	srv, err := httpapi.New(m, httpapi.Config{})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{
		"POST /orders": 201, "POST /orders/{orderId}/lines": 204, "POST /orders/{orderId}/submit": 204,
		"GET /orders/{orderId}": 200, "GET /orders": 200, "GET /orders/{orderId}/events": 200, "POST /rpc/ReserveStock": 200,
	}
	got := map[string]int{}
	for _, r := range srv.Routes() {
		got[r.Method+" "+r.Path] = r.Status
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("routes %v, want %v", got, want)
	}
	for _, r := range srv.Routes() {
		if r.Info.Name != "WatchOrder" {
			continue
		}
		found := false
		for _, b := range r.Bindings {
			if b.Source == httpapi.SourceHeader && b.Name == "Last-Event-ID" {
				found = true
			}
		}
		if !found {
			t.Errorf("WatchOrder must bind Last-Event-ID: %+v", r.Bindings)
		}
	}
}

func TestValidation(t *testing.T) {
	v := validate.New()
	for _, typ := range []reflect.Type{
		reflect.TypeFor[orders.CreateOrder](), reflect.TypeFor[orders.AddLine](), reflect.TypeFor[orders.SubmitOrder](),
		reflect.TypeFor[orders.GetOrder](), reflect.TypeFor[orders.ListOrders](), reflect.TypeFor[orders.WatchOrder](),
		reflect.TypeFor[orders.ReserveStock](), reflect.TypeFor[orders.Sleep](),
	} {
		if err := v.Compile(typ); err != nil {
			t.Fatalf("compile %s: %v", typ, err)
		}
	}
	ctx := context.Background()
	fields := func(err error) map[string]string {
		t.Helper()
		var ve *mediator.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("want *ValidationError, got %v", err)
		}
		out := map[string]string{}
		for _, f := range ve.Fields {
			out[f.Path] = f.Rule
		}
		return out
	}
	one, zero, big := 1, 0, 101
	cases := []struct {
		name string
		req  any
		want map[string]string
	}{
		{"create ok", orders.CreateOrder{CustomerID: customerID, Lines: []orders.OrderLine{{SKU: "SKU-1", Qty: 1, UnitPrice: 100}}}, nil},
		{"create bad", orders.CreateOrder{CustomerID: "nope", Lines: []orders.OrderLine{{SKU: "x", Qty: 0, UnitPrice: -1}}},
			map[string]string{"/customerId": "uuid", "/lines/0/sku": "pattern", "/lines/0/qty": "min", "/lines/0/unitPrice": "min"}},
		{"create no lines", orders.CreateOrder{CustomerID: customerID}, map[string]string{"/lines": "required"}},
		{"create duplicate sku", orders.CreateOrder{CustomerID: customerID, Lines: []orders.OrderLine{{SKU: "SKU-1", Qty: 1}, {SKU: "SKU-1", Qty: 2}}},
			map[string]string{"/lines/1/sku": "unique"}},
		{"add line bad id", orders.AddLine{OrderID: "x", SKU: "SKU-1", Qty: 1}, map[string]string{"/orderId": "uuid"}},
		{"submit ok", orders.SubmitOrder{OrderID: orderID}, nil},
		{"get missing id", orders.GetOrder{}, map[string]string{"/orderId": "required"}},
		{"list ok defaults", orders.ListOrders{CustomerID: customerID}, nil},
		{"list ok explicit", orders.ListOrders{CustomerID: customerID, Page: &one, Size: &one}, nil},
		{"list page zero", orders.ListOrders{CustomerID: customerID, Page: &zero, Size: &big}, map[string]string{"/page": "min", "/size": "max"}},
		{"watch ok", orders.WatchOrder{OrderID: orderID, LastEventID: "7"}, nil},
		{"watch bad last id", orders.WatchOrder{OrderID: orderID, LastEventID: "seven"}, map[string]string{"/lastEventId": "integer"}},
		{"watch negative last id", orders.WatchOrder{OrderID: orderID, LastEventID: "-1"}, map[string]string{"/lastEventId": "integer"}},
		{"reserve bad", orders.ReserveStock{OrderID: orderID, SKU: "ok-1", Qty: 2000}, map[string]string{"/requestId": "required", "/sku": "pattern", "/qty": "max"}},
		{"sleep too long", orders.Sleep{Millis: 60001}, map[string]string{"/ms": "max"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := v.Check(ctx, c.req)
			if c.want == nil {
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				return
			}
			if got := fields(err); !reflect.DeepEqual(got, c.want) {
				t.Errorf("fields %v, want %v", got, c.want)
			}
		})
	}
}

// TestPipeline_WithoutInfrastructure runs requests through the built
// registry: authorization and validation reject before any handler, and a
// handler that reaches the database without a unit of work fails cleanly.
func TestPipeline_WithoutInfrastructure(t *testing.T) {
	m := registry(t)
	ctx := context.Background()
	create := orders.CreateOrder{CustomerID: customerID, Lines: []orders.OrderLine{{SKU: "SKU-1", Qty: 1}}}
	if _, err := mediator.Send(ctx, m, create); mediator.CodeOf(err) != mediator.CodeUnauthorized {
		t.Errorf("anonymous: %v", err)
	}
	if _, err := mediator.Send(principal("orders:read"), m, create); mediator.CodeOf(err) != mediator.CodeForbidden {
		t.Errorf("wrong permission: %v", err)
	}
	if _, err := mediator.Send(principal(orders.PermissionOrdersWrite), m, orders.CreateOrder{}); mediator.CodeOf(err) != mediator.CodeValidation {
		t.Errorf("invalid: %v", err)
	}
	// Valid and authorized, but no store: the handler reports the missing
	// unit of work as an internal error whose cause is ErrNoTransaction.
	for _, req := range []func() error{
		func() error { _, err := mediator.Send(principal(orders.PermissionOrdersWrite), m, create); return err },
		func() error {
			_, err := mediator.Send(principal(orders.PermissionOrdersWrite), m, orders.AddLine{OrderID: orderID, SKU: "SKU-1", Qty: 1})
			return err
		},
		func() error {
			_, err := mediator.Send(principal(orders.PermissionOrdersWrite), m, orders.SubmitOrder{OrderID: orderID})
			return err
		},
		func() error { _, err := mediator.Send(principal(), m, orders.GetOrder{OrderID: orderID}); return err },
		func() error {
			_, err := mediator.Send(principal(), m, orders.ListOrders{CustomerID: customerID})
			return err
		},
		func() error {
			_, err := mediator.Send(principal(orders.PermissionInventoryWrite), m, orders.ReserveStock{RequestID: "r", OrderID: orderID, SKU: "SKU-1", Qty: 1})
			return err
		},
	} {
		err := req()
		if mediator.CodeOf(err) != mediator.CodeInternal || !errors.Is(err, orders.ErrNoTransaction) {
			t.Errorf("without store: %v", err)
		}
	}
	// WatchOrder needs no unit of work but a querier.
	var got error
	for _, err := range mediator.Stream(principal(), m, orders.WatchOrder{OrderID: orderID}) {
		got = err
	}
	if mediator.CodeOf(got) != mediator.CodeInternal || !strings.Contains(got.Error(), "Querier") {
		t.Errorf("watch without querier: %v", got)
	}
}

func TestDebugRequests(t *testing.T) {
	m, err := orders.NewMediator(orders.Deps{Debug: true, NodeID: "n1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Lookup("debug.Sleep"); !ok {
		t.Fatal("debug.Sleep not registered")
	}
	res, err := mediator.Send(principal(), m, orders.Sleep{Millis: 1})
	if err != nil || res.SleptMillis != 1 || res.Node != "n1" {
		t.Errorf("%+v %v", res, err)
	}
	ctx, cancel := context.WithCancel(principal())
	cancel()
	if _, err := mediator.Send(ctx, m, orders.Sleep{Millis: 5000}); mediator.CodeOf(err) != mediator.CodeTimeout {
		t.Errorf("cancelled sleep: %v", err)
	}
	srv, err := httpapi.New(m, httpapi.Config{})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, r := range srv.Routes() {
		if r.Method == "GET" && r.Path == "/debug/sleep" {
			found = true
		}
	}
	if !found {
		t.Error("GET /debug/sleep not routed")
	}
}

func TestRegisterErrors(t *testing.T) {
	if err := orders.Register(nil); err == nil {
		t.Error("nil mediator must fail")
	}
	m := mediator.New()
	if err := orders.Register(m); err != nil {
		t.Fatal(err)
	}
	if err := orders.Register(m); err == nil {
		t.Error("double registration must fail")
	}
	if err := m.Build(); err != nil {
		t.Errorf("build after Register: %v", err)
	}
	if err := orders.RegisterDebug(m, "n"); !errors.Is(err, mediator.ErrAlreadyBuilt) {
		t.Errorf("register after build: %v", err)
	}
}

// TestOpenAPI_MatchesCommitted is the drift check of spec 8.6 without a
// process: the document generated from the registry is byte-equal to
// api/openapi.json.
func TestOpenAPI_MatchesCommitted(t *testing.T) {
	m := registry(t)
	cfg := orders.OpenAPIConfig()
	if cfg.Info.Title != "Orders" || cfg.Info.Version != orders.Version || cfg.Prefix != "" {
		t.Errorf("config %+v", cfg)
	}
	doc, err := openapi.Generate(m, cfg)
	if err != nil {
		t.Fatal(err)
	}
	got, err := doc.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("..", "..", "..", "api", "openapi.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("api/openapi.json is out of date: run `go run ./tools/task openapi`")
	}
	if !bytes.Contains(got, []byte(`"x-sse-item"`)) || !bytes.Contains(got, []byte(`"orders:write"`)) || bytes.Contains(got, []byte(`debug`)) {
		t.Error("document lacks the SSE item, the scope, or contains the debug requests")
	}
}
