package openapi_test

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/ratelimit"
	"github.com/t3stackcoder/go-api-backend/mediator/retry"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

// The fixture mirrors the example service of spec 13 and adds the shapes
// every generator rule needs: RPC defaults, pinned names with dots, an
// operationId collision, a Schemer, nested structs, pointers, maps,
// time.Time, uuid.UUID, []byte, enums, and doc descriptions.

// Currency is an enum through a oneof rule on a named string type.
type Currency string

// OrderStatus is a named string type without a rule: a plain string.
type OrderStatus string

// Address is a nested struct.
type Address struct {
	Street  string `json:"street" validate:"required" doc:"Street and number"`
	City    string `json:"city" validate:"required"`
	Country string `json:"country" validate:"required,len=2" doc:"ISO 3166-1 alpha-2 code"`
}

// OrderLine is the Appendix B example.
type OrderLine struct {
	SKU   string  `json:"sku" validate:"required,pattern=^[A-Z0-9-]{3,32}$" doc:"Stock keeping unit"`
	Qty   int     `json:"qty" validate:"required,min=1,max=1000"`
	Price float64 `json:"price" validate:"gte=0"`
	Note  *string `json:"note" validate:"max=200"`
}

// Money is a Schemer: it travels as a string and supplies its own schema.
type Money struct {
	Cents    int64
	Currency string
}

func (m Money) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "%d.%02d %s", m.Cents/100, m.Cents%100, m.Currency), nil
}

func (m *Money) UnmarshalText(b []byte) error {
	amount, cur, ok := strings.Cut(string(b), " ")
	if !ok {
		return errors.New("want amount and currency")
	}
	whole, frac, _ := strings.Cut(amount, ".")
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return err
	}
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return err
	}
	*m = Money{Cents: w*100 + f, Currency: cur}
	return nil
}

func (Money) JSONSchema() *validate.Schema {
	return &validate.Schema{
		Type:        validate.Type{"string"},
		Pattern:     `^[0-9]+\.[0-9]{2} [A-Z]{3}$`,
		Description: "Decimal amount followed by the ISO 4217 currency",
		Extra:       map[string]any{"x-money": true, "examples": []any{"12.50 EUR"}},
	}
}

// CreateOrder: POST /orders, 201, permission scope, header-bound tenant,
// idempotency key from the header, cache invalidation, retry.
type CreateOrder struct {
	mediator.Command[CreateOrderResult]
	CustomerID uuid.UUID         `json:"customerId" validate:"required" doc:"Customer placing the order"`
	Lines      []OrderLine       `json:"lines" validate:"required,min=1,max=100,dive"`
	Currency   Currency          `json:"currency" validate:"required,oneof=USD EUR GBP"`
	Shipping   Address           `json:"shipping"`
	Billing    *Address          `json:"billing" doc:"Defaults to the shipping address"`
	Metadata   map[string]string `json:"metadata" validate:"max=20,dive,max=100"`
	Attachment []byte            `json:"attachment"`
	DeliverAt  *time.Time        `json:"deliverAt" doc:"Requested delivery time"`
	Budget     Money             `json:"budget"`
	Tenant     string            `json:"tenant" header:"X-Tenant" validate:"required" doc:"Tenant of the caller"`
}

func (CreateOrder) Route() httpapi.Route {
	return httpapi.Route{Method: "POST", Path: "/orders", Status: 201}
}
func (CreateOrder) Requires() authz.Requirement { return authz.Permission("orders:write") }
func (CreateOrder) Invalidates() []string       { return []string{"orders:list"} }
func (CreateOrder) RetryPolicy() retry.Policy   { return retry.Policy{MaxAttempts: 3} }
func (CreateOrder) Describe() openapi.Operation {
	return openapi.Operation{
		Summary:     "Create an order",
		Description: "Creates a draft order for the customer. Lines are validated as a whole; cross-field rules apply.",
		Tags:        []string{"orders"},
		Errors:      []string{"conflict"},
	}
}

type CreateOrderResult struct {
	OrderID   uuid.UUID `json:"orderId"`
	CreatedAt time.Time `json:"createdAt"`
}

// AddLine: Void command with a path parameter and a body.
type AddLine struct {
	mediator.Command[mediator.Void]
	OrderID uuid.UUID `json:"orderId" path:"orderId" doc:"Order to extend"`
	Line    OrderLine `json:"line" validate:"required"`
}

func (AddLine) Route() httpapi.Route {
	return httpapi.Route{Method: "POST", Path: "/orders/{orderId}/lines"}
}
func (AddLine) Invalidates() []string { return []string{"order:{id}", "orders:list"} }
func (AddLine) Describe() openapi.Operation {
	return openapi.Operation{Summary: "Add a line", Errors: []string{"not_found"}}
}

// SubmitOrder: Void command without body fields and with an Any requirement.
type SubmitOrder struct {
	mediator.Command[mediator.Void]
	OrderID uuid.UUID `json:"orderId" path:"orderId"`
}

func (SubmitOrder) Route() httpapi.Route {
	return httpapi.Route{Method: "POST", Path: "/orders/{orderId}/submit"}
}
func (SubmitOrder) Requires() authz.Requirement {
	return authz.Any(authz.Role("admin"), authz.Permission("orders:submit"))
}
func (SubmitOrder) Describe() openapi.Operation {
	return openapi.Operation{Summary: "Submit an order", Errors: []string{"not_found", "precondition_failed"}}
}

// CancelOrder: DELETE with a body field.
type CancelOrder struct {
	mediator.Command[mediator.Void]
	OrderID uuid.UUID `json:"orderId" path:"orderId"`
	Reason  string    `json:"reason" validate:"max=500"`
}

func (CancelOrder) Route() httpapi.Route {
	return httpapi.Route{Method: "DELETE", Path: "/orders/{orderId}"}
}

// UpdateOrder and PatchOrder cover PUT and PATCH.
type UpdateOrder struct {
	mediator.Command[OrderView]
	OrderID  uuid.UUID `json:"orderId" path:"orderId"`
	Shipping Address   `json:"shipping"`
}

func (UpdateOrder) Route() httpapi.Route {
	return httpapi.Route{Method: "PUT", Path: "/orders/{orderId}"}
}

type PatchOrder struct {
	mediator.Command[OrderView]
	OrderID uuid.UUID `json:"orderId" path:"orderId"`
	Note    *string   `json:"note"`
}

func (PatchOrder) Route() httpapi.Route {
	return httpapi.Route{Method: "PATCH", Path: "/orders/{orderId}"}
}

// OrderExists covers HEAD.
type OrderExists struct {
	mediator.Query[mediator.Void]
	OrderID uuid.UUID `json:"orderId" path:"orderId"`
}

func (OrderExists) Route() httpapi.Route {
	return httpapi.Route{Method: "HEAD", Path: "/orders/{orderId}/exists"}
}

type OrderView struct {
	OrderID     uuid.UUID         `json:"orderId"`
	CustomerID  uuid.UUID         `json:"customerId"`
	Status      OrderStatus       `json:"status"`
	Lines       []OrderLine       `json:"lines"`
	Shipping    Address           `json:"shipping"`
	Total       Money             `json:"total"`
	Metadata    map[string]string `json:"metadata"`
	SubmittedAt *time.Time        `json:"submittedAt"`
	Extra       any               `json:"extra"`
}

// GetOrder: cached query with a path parameter, a constrained query slice,
// and an Authenticated() requirement (no scopes).
type GetOrder struct {
	mediator.Query[OrderView]
	OrderID uuid.UUID `json:"orderId" path:"orderId" doc:"Order identifier"`
	Expand  []string  `json:"expand" query:"expand" validate:"max=5,unique,dive,oneof=lines customer" doc:"Related data to include"`
}

func (GetOrder) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/orders/{orderId}"} }
func (GetOrder) CacheTags() []string  { return []string{"order:{id}"} }
func (GetOrder) CacheTTL() time.Duration {
	return 5 * time.Minute
}
func (GetOrder) Requires() authz.Requirement { return authz.Authenticated() }
func (GetOrder) Describe() openapi.Operation {
	return openapi.Operation{Summary: "Get an order", Tags: []string{"orders", "read"}, Errors: []string{"not_found"}}
}

// Page is the generic page type of the example service.
type Page[T any] struct {
	Items []T     `json:"items"`
	Total int     `json:"total"`
	Next  *string `json:"next" doc:"Cursor of the next page"`
}

type OrderSummary struct {
	OrderID uuid.UUID   `json:"orderId"`
	Status  OrderStatus `json:"status"`
	Total   Money       `json:"total"`
}

// ListOrders: GET /orders?customerId=&page=&size= with a required query
// parameter and an optional enum.
type ListOrders struct {
	mediator.Query[Page[OrderSummary]]
	CustomerID uuid.UUID    `json:"customerId" query:"customerId" validate:"required"`
	Page       int          `json:"page" query:"page" validate:"min=1" doc:"1-based page number"`
	Size       int          `json:"size" query:"size" validate:"min=1,max=100"`
	Status     *OrderStatus `json:"status" query:"status" validate:"oneof=draft submitted"`
}

func (ListOrders) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/orders"} }
func (ListOrders) CacheTags() []string  { return []string{"orders:list"} }

// OrderEvent is the SSE item type.
type OrderEvent struct {
	Seq     int64          `json:"seq"`
	Type    string         `json:"type"`
	At      time.Time      `json:"at"`
	Payload map[string]any `json:"payload"`
}

// WatchOrder: stream with a path parameter and the Last-Event-ID header.
type WatchOrder struct {
	mediator.StreamQuery[OrderEvent]
	OrderID     uuid.UUID `json:"orderId" path:"orderId"`
	LastEventID string    `json:"lastEventId" header:"Last-Event-ID" doc:"Resume after this event"`
}

func (WatchOrder) Route() httpapi.Route {
	return httpapi.Route{Method: "GET", Path: "/orders/{orderId}/events"}
}
func (WatchOrder) Describe() openapi.Operation {
	return openapi.Operation{Summary: "Watch order events", Tags: []string{"orders", "events"}}
}

// ReserveStock: RPC command whose idempotency key comes from the body.
type ReserveStock struct {
	mediator.Command[ReserveStockResult]
	RequestID string `json:"requestId" validate:"required,uuid"`
	SKU       string `json:"sku" validate:"required"`
	Qty       int    `json:"qty" validate:"required,min=1"`
}

func (r ReserveStock) IdempotencyKey() string { return r.RequestID }

type ReserveStockResult struct {
	Reserved bool `json:"reserved"`
}

// CountOrders: RPC GET query (scalar fields become query parameters) with a
// rate limit and a scalar response.
type CountOrders struct {
	mediator.Query[int]
	CustomerID uuid.UUID  `json:"customerId" validate:"required"`
	Since      *time.Time `json:"since"`
	Statuses   []string   `json:"statuses" validate:"unique"`
}

func (CountOrders) RateLimit() ratelimit.Policy {
	return ratelimit.Policy{Rate: 10, Period: time.Second, Burst: 5}
}

// SearchOrders: RPC POST query because Filter is not a scalar.
type OrderFilter struct {
	Statuses []OrderStatus `json:"statuses"`
	MinTotal *float64      `json:"minTotal" validate:"gte=0"`
}

type SearchOrders struct {
	mediator.Query[Page[OrderSummary]]
	Filter OrderFilter `json:"filter"`
	Page   int         `json:"page" validate:"min=1"`
}

func (SearchOrders) Describe() openapi.Operation {
	return openapi.Operation{Summary: "Search orders", Errors: []string{"conflict", "timeout"}}
}

// LegacyExport: pinned name with dots and underscores, deprecated.
type LegacyExport struct {
	mediator.Query[ExportResult]
	Format string `json:"format" validate:"oneof=csv json"`
}

func (LegacyExport) Name() string { return "orders.v1.export_csv" }
func (LegacyExport) Describe() openapi.Operation {
	return openapi.Operation{Summary: "Export orders (legacy)", Deprecated: true}
}

type ExportResult struct {
	URL string `json:"url"`
}

// OrdersGet and ordersGetPinned both camel-case to "ordersGet".
type OrdersGet struct {
	mediator.Query[OrderView]
	OrderID uuid.UUID `json:"orderId"`
}

type ordersGetPinned struct {
	mediator.Query[OrderView]
	OrderID uuid.UUID `json:"orderId"`
}

func (ordersGetPinned) Name() string { return "orders.get" }

// Ping: operationId override, no body fields.
type Ping struct {
	mediator.Command[mediator.Void]
}

func (Ping) Describe() openapi.Operation {
	return openapi.Operation{OperationID: "healthPing", Tags: []string{"system"}}
}

// Whoami: Requires() returning nil means no security.
type Whoami struct {
	mediator.Query[string]
}

func (Whoami) Requires() authz.Requirement { return nil }

// Replay binds the Idempotency-Key header itself.
type Replay struct {
	mediator.Command[mediator.Void]
	Key string `json:"key" header:"Idempotency-Key"`
}

// GetFile: wildcard path segment.
type GetFile struct {
	mediator.Query[[]byte]
	Path string `json:"path" path:"path"`
}

func (GetFile) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/files/{path...}"} }

// Reports: trailing slash, registered as an exact match.
type Reports struct {
	mediator.Query[[]string]
}

func (Reports) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/reports/"} }

// Root: the prefix itself, tagged "default".
type Root struct {
	mediator.Query[string]
}

func (Root) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/"} }

// Token: first segment is a parameter, tagged "default".
type Token struct {
	mediator.Query[string]
	Token string `json:"token" path:"token"`
}

func (Token) Route() httpapi.Route { return httpapi.Route{Method: "GET", Path: "/{token}"} }

// register registers a handler returning the zero response.
func register[Q mediator.Request[R], R any](tb testing.TB, m *mediator.Mediator) {
	tb.Helper()
	if err := mediator.HandleFunc(m, func(context.Context, Q) (R, error) { var z R; return z, nil }); err != nil {
		tb.Fatal(err)
	}
}

func registerStream[Q mediator.StreamRequest[T], T any](tb testing.TB, m *mediator.Mediator) {
	tb.Helper()
	if err := mediator.HandleStreamFunc(m, func(context.Context, Q) iter.Seq2[T, error] { return nil }); err != nil {
		tb.Fatal(err)
	}
}

// newFixture builds the example service.
func newFixture(tb testing.TB) *mediator.Mediator {
	tb.Helper()
	m := mediator.New()
	register[CreateOrder, CreateOrderResult](tb, m)
	register[AddLine, mediator.Void](tb, m)
	register[SubmitOrder, mediator.Void](tb, m)
	register[CancelOrder, mediator.Void](tb, m)
	register[UpdateOrder, OrderView](tb, m)
	register[PatchOrder, OrderView](tb, m)
	register[OrderExists, mediator.Void](tb, m)
	register[GetOrder, OrderView](tb, m)
	register[ListOrders, Page[OrderSummary]](tb, m)
	registerStream[WatchOrder, OrderEvent](tb, m)
	register[ReserveStock, ReserveStockResult](tb, m)
	register[CountOrders, int](tb, m)
	register[SearchOrders, Page[OrderSummary]](tb, m)
	register[LegacyExport, ExportResult](tb, m)
	register[OrdersGet, OrderView](tb, m)
	register[ordersGetPinned, OrderView](tb, m)
	register[Ping, mediator.Void](tb, m)
	register[Whoami, string](tb, m)
	register[Replay, mediator.Void](tb, m)
	register[GetFile, []byte](tb, m)
	register[Reports, []string](tb, m)
	register[Root, string](tb, m)
	register[Token, string](tb, m)
	if err := m.Build(); err != nil {
		tb.Fatal(err)
	}
	return m
}

// fixtureConfig is the configuration the golden file is generated with.
func fixtureConfig() openapi.Config {
	return openapi.Config{
		Info: openapi.Info{
			Title:          "Orders",
			Summary:        "The example service of spec 13",
			Description:    "Exercises every generator feature.",
			TermsOfService: "https://example.com/terms",
			Contact:        &openapi.Contact{Name: "API team", URL: "https://example.com", Email: "api@example.com"},
			License:        &openapi.License{Name: "MIT", Identifier: "MIT"},
			Version:        "1.2.3",
		},
		Prefix:  "/api",
		Servers: []openapi.Server{{URL: "http://localhost:8080", Description: "Local"}},
	}
}

// generate runs Generate on the fixture and fails the test on error.
func generate(tb testing.TB, cfg openapi.Config) *openapi.Document {
	tb.Helper()
	doc, err := openapi.Generate(newFixture(tb), cfg)
	if err != nil {
		tb.Fatalf("Generate: %v", err)
	}
	return doc
}
