package orders

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/retry"
)

// Permissions the requests require. The example authenticator maps the
// "permissions" claim of the JWT onto authz.Principal.Permissions.
const (
	PermissionOrdersWrite    = "orders:write"
	PermissionInventoryWrite = "inventory:write"
)

// TagOrdersList is the cache tag of every ListOrders page (spec 7.4).
const TagOrdersList = "orders:list"

// TagOrder returns the cache tag of one order: the GetOrder entry of that
// order depends on it, and AddLine and SubmitOrder of that order bump it.
func TagOrder(orderID string) string { return "order:" + orderID }

// Limits of the example service.
const (
	// DefaultPageSize is the ListOrders page size when the query names none.
	DefaultPageSize = 20
	// MaxPageSize bounds ListOrders.Size.
	MaxPageSize = 100
	// WatchTimeout bounds one WatchOrder stream. Streams are requests, so the
	// Timeout behavior applies; the default 30 s would cut a tail short.
	WatchTimeout = time.Hour
)

// Status is the lifecycle state of an order.
type Status string

// Order statuses. An order is created as a draft, takes lines while it is a
// draft, and becomes submitted exactly once.
const (
	StatusDraft     Status = "draft"
	StatusSubmitted Status = "submitted"
)

// Event types written to order_events and streamed by WatchOrder.
const (
	EventCreated   = "created"
	EventLineAdded = "line_added"
	EventSubmitted = "submitted"
)

// OrderLine is one line of an order, in the request body and in views.
type OrderLine struct {
	SKU       string `json:"sku" validate:"required,pattern=^[A-Z0-9-]{3,32}$" doc:"Stock keeping unit"`
	Qty       int    `json:"qty" validate:"required,min=1,max=1000" doc:"Quantity ordered"`
	UnitPrice int64  `json:"unitPrice" validate:"min=0" doc:"Unit price in minor currency units (cents)"`
}

// CreateOrder creates a draft order for a customer: POST /orders, 201. The
// idempotency key comes from the Idempotency-Key header (there is no
// IdempotencyKey method), so a retried POST replays the first response.
type CreateOrder struct {
	mediator.Command[CreateOrderResult]

	CustomerID string      `json:"customerId" validate:"required,uuid" doc:"Customer placing the order"`
	Lines      []OrderLine `json:"lines" validate:"required,min=1,max=100,dive" doc:"Initial lines; at least one, SKUs distinct"`
}

// CreateOrderResult is the response of CreateOrder.
type CreateOrderResult struct {
	OrderID string `json:"orderId" doc:"Identifier of the new order (UUID)"`
}

// Route is POST /orders with status 201.
func (CreateOrder) Route() httpapi.Route {
	return httpapi.Route{Method: http.MethodPost, Path: "/orders", Status: http.StatusCreated}
}

// Requires the orders:write permission.
func (CreateOrder) Requires() authz.Requirement { return authz.Permission(PermissionOrdersWrite) }

// Invalidates the list tag: a new order changes every listing of its customer.
func (CreateOrder) Invalidates() []string { return []string{TagOrdersList} }

// RetryPolicy re-runs the command up to three times on transient failures,
// each attempt in a fresh transaction.
func (CreateOrder) RetryPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: 500 * time.Millisecond}
}

// Validate is the cross-field rule of spec 5.8: SKUs within one order are
// distinct. Tag rules ran first, so every line is well-formed here.
func (c CreateOrder) Validate(context.Context) error {
	return distinctSKUs(c.Lines)
}

// Describe documents the operation.
func (CreateOrder) Describe() openapi.Operation {
	return openapi.Operation{
		Summary:     "Create an order",
		Description: "Creates a draft order for the customer with its initial lines. Send an Idempotency-Key header to make retries safe: the same key with the same body replays the first response.",
		Tags:        []string{"orders"},
	}
}

// AddLine appends a line to a draft order: POST /orders/{orderId}/lines.
type AddLine struct {
	mediator.Command[mediator.Void]

	OrderID   string `json:"orderId" path:"orderId" validate:"required,uuid" doc:"Order identifier"`
	SKU       string `json:"sku" validate:"required,pattern=^[A-Z0-9-]{3,32}$" doc:"Stock keeping unit"`
	Qty       int    `json:"qty" validate:"required,min=1,max=1000" doc:"Quantity ordered"`
	UnitPrice int64  `json:"unitPrice" validate:"min=0" doc:"Unit price in minor currency units (cents)"`
}

// Route is POST /orders/{orderId}/lines (204 on success: Void).
func (AddLine) Route() httpapi.Route {
	return httpapi.Route{Method: http.MethodPost, Path: "/orders/{orderId}/lines"}
}

// Requires the orders:write permission.
func (AddLine) Requires() authz.Requirement { return authz.Permission(PermissionOrdersWrite) }

// Invalidates the order's own tag and the list tag. The tag is built from
// the request value, so only the affected order's cache entry is dropped.
func (c AddLine) Invalidates() []string { return []string{TagOrder(c.OrderID), TagOrdersList} }

// Describe documents the operation and its domain errors.
func (AddLine) Describe() openapi.Operation {
	return openapi.Operation{
		Summary:     "Add a line",
		Description: "Appends a line to a draft order. A submitted order rejects new lines with precondition_failed.",
		Tags:        []string{"orders"},
		Errors:      []string{string(mediator.CodeNotFound), string(mediator.CodePrecondition)},
	}
}

// SubmitOrder submits a draft order: POST /orders/{orderId}/submit. It runs
// SERIALIZABLE so the total it publishes cannot race with a concurrent
// AddLine, and it publishes OrderSubmitted to the outbox.
type SubmitOrder struct {
	mediator.Command[mediator.Void]

	OrderID string `json:"orderId" path:"orderId" validate:"required,uuid" doc:"Order identifier"`
}

// Route is POST /orders/{orderId}/submit (204 on success: Void).
func (SubmitOrder) Route() httpapi.Route {
	return httpapi.Route{Method: http.MethodPost, Path: "/orders/{orderId}/submit"}
}

// Requires the orders:write permission.
func (SubmitOrder) Requires() authz.Requirement { return authz.Permission(PermissionOrdersWrite) }

// TxOptions runs the command at SERIALIZABLE isolation (spec 13).
func (SubmitOrder) TxOptions() pg.TxOptions { return pg.TxOptions{Isolation: pgx.Serializable} }

// RetryPolicy retries serialization failures, which are transient by
// definition (spec 5.11); that is what makes SERIALIZABLE practical.
func (SubmitOrder) RetryPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 3, BaseDelay: 50 * time.Millisecond, MaxDelay: 500 * time.Millisecond}
}

// Invalidates the order's tag (its status changed) and the list tag.
func (c SubmitOrder) Invalidates() []string { return []string{TagOrder(c.OrderID), TagOrdersList} }

// Describe documents the operation and its domain errors.
func (SubmitOrder) Describe() openapi.Operation {
	return openapi.Operation{
		Summary:     "Submit an order",
		Description: "Moves a draft order with at least one line to submitted and publishes OrderSubmitted for the inventory and read-model consumers.",
		Tags:        []string{"orders"},
		Errors:      []string{string(mediator.CodeNotFound), string(mediator.CodePrecondition)},
	}
}

// OrderView is the response of GetOrder.
type OrderView struct {
	OrderID     string      `json:"orderId"`
	CustomerID  string      `json:"customerId"`
	Status      Status      `json:"status" validate:"oneof=draft submitted"`
	Lines       []OrderLine `json:"lines"`
	Total       int64       `json:"total" doc:"Sum of qty times unitPrice over the lines, in minor units"`
	CreatedAt   time.Time   `json:"createdAt"`
	SubmittedAt *time.Time  `json:"submittedAt" doc:"Set once the order is submitted"`
}

// GetOrder reads one order: GET /orders/{orderId}, cached for five minutes
// under the order's tag.
type GetOrder struct {
	mediator.Query[OrderView]

	OrderID string `json:"orderId" path:"orderId" validate:"required,uuid" doc:"Order identifier"`
}

// Route is GET /orders/{orderId}.
func (GetOrder) Route() httpapi.Route {
	return httpapi.Route{Method: http.MethodGet, Path: "/orders/{orderId}"}
}

// Requires an authenticated caller.
func (GetOrder) Requires() authz.Requirement { return authz.Authenticated() }

// CacheTags names the order's tag, built from the request value.
func (q GetOrder) CacheTags() []string { return []string{TagOrder(q.OrderID)} }

// CacheTTL is five minutes.
func (GetOrder) CacheTTL() time.Duration { return 5 * time.Minute }

// Describe documents the operation and its not-found error.
func (GetOrder) Describe() openapi.Operation {
	return openapi.Operation{
		Summary: "Get an order",
		Tags:    []string{"orders"},
		Errors:  []string{string(mediator.CodeNotFound)},
	}
}

// Page is one page of a listing.
type Page[T any] struct {
	Items []T   `json:"items"`
	Page  int   `json:"page" doc:"1-based page number of this page"`
	Size  int   `json:"size" doc:"Page size that was applied"`
	Total int64 `json:"total" doc:"Total number of items across all pages"`
}

// OrderSummary is one row of ListOrders.
type OrderSummary struct {
	OrderID    string    `json:"orderId"`
	CustomerID string    `json:"customerId"`
	Status     Status    `json:"status" validate:"oneof=draft submitted"`
	LineCount  int       `json:"lineCount"`
	Total      int64     `json:"total" doc:"Sum of qty times unitPrice over the lines, in minor units"`
	CreatedAt  time.Time `json:"createdAt"`
}

// ListOrders lists the orders of a customer, newest first:
// GET /orders?customerId=&page=&size=. Page and Size are optional and
// default to 1 and DefaultPageSize; the result is cached under the list tag.
type ListOrders struct {
	mediator.Query[Page[OrderSummary]]

	CustomerID string `json:"customerId" query:"customerId" validate:"required,uuid" doc:"Customer whose orders are listed"`
	Page       *int   `json:"page" query:"page" validate:"min=1" doc:"1-based page number; default 1"`
	Size       *int   `json:"size" query:"size" validate:"min=1,max=100" doc:"Page size; default 20, at most 100"`
}

// Route is GET /orders.
func (ListOrders) Route() httpapi.Route {
	return httpapi.Route{Method: http.MethodGet, Path: "/orders"}
}

// Requires an authenticated caller.
func (ListOrders) Requires() authz.Requirement { return authz.Authenticated() }

// CacheTags names the list tag.
func (ListOrders) CacheTags() []string { return []string{TagOrdersList} }

// Describe documents the operation.
func (ListOrders) Describe() openapi.Operation {
	return openapi.Operation{Summary: "List the orders of a customer", Tags: []string{"orders"}}
}

// OrderEvent is one item of the WatchOrder stream. Its ID is the SSE event
// id, so a client that reconnects with Last-Event-ID resumes after it.
type OrderEvent struct {
	ID      int64          `json:"id" doc:"Monotonic event ID within the order; sent as the SSE id"`
	OrderID string         `json:"orderId"`
	Type    string         `json:"type" validate:"oneof=created line_added submitted"`
	At      time.Time      `json:"at"`
	Data    map[string]any `json:"data" doc:"Event-specific fields"`
}

// EventID implements httpapi.EventIDer.
func (e OrderEvent) EventID() string { return strconv.FormatInt(e.ID, 10) }

// WatchOrder tails the events of one order as Server-Sent Events:
// GET /orders/{orderId}/events. The stream ends after the submitted event.
// It opts out of the unit of work because it tails live data (spec 4.5).
type WatchOrder struct {
	mediator.StreamQuery[OrderEvent]

	OrderID     string `json:"orderId" path:"orderId" validate:"required,uuid" doc:"Order identifier"`
	LastEventID string `json:"lastEventId" header:"Last-Event-ID" doc:"Resume after this event ID (browsers send it on reconnect)"`
}

// Route is GET /orders/{orderId}/events.
func (WatchOrder) Route() httpapi.Route {
	return httpapi.Route{Method: http.MethodGet, Path: "/orders/{orderId}/events"}
}

// Requires an authenticated caller.
func (WatchOrder) Requires() authz.Requirement { return authz.Authenticated() }

// NoUnitOfWork keeps the tail outside any transaction: it polls live data
// with short reads, so no repeatable-read snapshot is held open.
func (WatchOrder) NoUnitOfWork() {}

// Timeout bounds the stream at WatchTimeout instead of the 30 s default.
func (WatchOrder) Timeout() time.Duration { return WatchTimeout }

// Validate rejects a Last-Event-ID that is not a non-negative integer.
func (q WatchOrder) Validate(context.Context) error {
	if q.LastEventID == "" {
		return nil
	}
	if _, err := parseEventID(q.LastEventID); err != nil {
		return (&mediator.ValidationError{}).Add("/lastEventId", "integer", "must be a non-negative integer event ID")
	}
	return nil
}

// Describe documents the operation.
func (WatchOrder) Describe() openapi.Operation {
	return openapi.Operation{
		Summary:     "Watch order events",
		Description: "Streams the events of the order as Server-Sent Events, from the beginning or after Last-Event-ID, and ends once the order is submitted.",
		Tags:        []string{"orders"},
		Errors:      []string{string(mediator.CodeNotFound)},
	}
}

// ReserveStock reserves stock for one line of a submitted order. It keeps
// the RPC default route (POST /rpc/ReserveStock) and takes its idempotency
// key from the body. The inventory consumer sends it through the mediator
// from inside its own unit of work, which the nested Send joins.
type ReserveStock struct {
	mediator.Command[ReserveStockResult]

	RequestID string `json:"requestId" validate:"required,max=200" doc:"Idempotency key of the reservation"`
	OrderID   string `json:"orderId" validate:"required,uuid" doc:"Order the stock is reserved for"`
	SKU       string `json:"sku" validate:"required,pattern=^[A-Z0-9-]{3,32}$" doc:"Stock keeping unit"`
	Qty       int    `json:"qty" validate:"required,min=1,max=1000" doc:"Quantity to reserve"`
}

// ReserveStockResult is the response of ReserveStock.
type ReserveStockResult struct {
	Reserved  bool  `json:"reserved" doc:"False when the SKU is unknown or has too little stock"`
	Available int64 `json:"available" doc:"Stock left after the reservation; -1 for an unknown SKU"`
}

// IdempotencyKey is the request ID: the key comes from the body, so the
// Idempotency-Key header is not offered for this operation.
func (c ReserveStock) IdempotencyKey() string { return c.RequestID }

// Requires the inventory:write permission. The inventory consumer acts as
// the system principal, which carries it.
func (ReserveStock) Requires() authz.Requirement { return authz.Permission(PermissionInventoryWrite) }

// Describe documents the operation.
func (ReserveStock) Describe() openapi.Operation {
	return openapi.Operation{
		Summary:     "Reserve stock",
		Description: "Reserves qty of sku for an order, recording a rejected reservation when stock is short. Uses the RPC default route and a body-sourced idempotency key.",
		Tags:        []string{"inventory"},
	}
}

// distinctSKUs is the cross-field rule of CreateOrder.
func distinctSKUs(lines []OrderLine) error {
	seen := make(map[string]int, len(lines))
	var ve *mediator.ValidationError
	for i, l := range lines {
		if first, dup := seen[l.SKU]; dup {
			if ve == nil {
				ve = &mediator.ValidationError{}
			}
			ve = ve.Add("/lines/"+strconv.Itoa(i)+"/sku", "unique", "duplicates the SKU of line "+strconv.Itoa(first))
			continue
		}
		seen[l.SKU] = i
	}
	if ve == nil {
		return nil
	}
	return ve
}

// parseEventID parses a Last-Event-ID value.
func parseEventID(s string) (int64, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return 0, strconv.ErrSyntax
	}
	return n, nil
}
