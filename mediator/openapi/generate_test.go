package openapi_test

import (
	"bytes"
	"encoding/json/v2"
	"flag"
	"iter"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

var update = flag.Bool("update", false, "rewrite testdata/golden.json from the fixture")

const goldenPath = "testdata/golden.json"

// golden returns the committed document, with line endings normalised so a
// checkout with core.autocrlf still compares equal.
func golden(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden (run with -update to create it): %v", err)
	}
	return bytes.ReplaceAll(b, []byte("\r\n"), []byte("\n"))
}

func TestGolden(t *testing.T) {
	doc := generate(t, fixtureConfig())
	got, err := doc.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if *update {
		if err := doc.WriteFile(goldenPath); err != nil {
			t.Fatal(err)
		}
	}
	if want := golden(t); !bytes.Equal(got, want) {
		t.Fatalf("document differs from %s (run with -update to accept):\n%s", goldenPath, firstDiff(got, want))
	}
	if err := openapi.ValidateJSON(got); err != nil {
		t.Fatalf("golden does not validate: %v", err)
	}
}

// firstDiff shows the first differing line of two texts.
func firstDiff(got, want []byte) string {
	g, w := strings.Split(string(got), "\n"), strings.Split(string(want), "\n")
	for i := range max(len(g), len(w)) {
		var gl, wl string
		if i < len(g) {
			gl = g[i]
		}
		if i < len(w) {
			wl = w[i]
		}
		if gl != wl {
			return "line " + itoa(i+1) + ":\n got: " + gl + "\nwant: " + wl
		}
	}
	return "lengths differ"
}

func itoa(n int) string { return strconv.Itoa(n) }

// TestGoldenRoundTrip decodes the golden into a Document and re-encodes it,
// proving the Go model covers every member the generator emits.
func TestGoldenRoundTrip(t *testing.T) {
	want := golden(t)
	var doc openapi.Document
	if err := json.Unmarshal(want, &doc); err != nil {
		t.Fatal(err)
	}
	got, err := doc.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round trip differs:\n%s", firstDiff(got, want))
	}
}

func TestDeterministic(t *testing.T) {
	a, err := generate(t, fixtureConfig()).MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	// A second mediator, a shared validator, and a security scheme object
	// built in another insertion order.
	cfg := fixtureConfig()
	cfg.Validator = validate.New()
	cfg.SecuritySchemeObject = map[string]any{}
	for _, k := range []string{"bearerFormat", "scheme", "type"} {
		cfg.SecuritySchemeObject[k] = openapi.DefaultSecuritySchemeObject()[k]
	}
	m := newFixture(t)
	for range 3 {
		b, err := openapi.Generate(m, cfg)
		if err != nil {
			t.Fatal(err)
		}
		got, err := b.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, a) {
			t.Fatalf("documents differ:\n%s", firstDiff(got, a))
		}
	}
}

// op returns an operation of the generated document.
func op(t *testing.T, doc *openapi.Document, method, path string) *openapi.OperationObject {
	t.Helper()
	o := doc.Paths[path].Operation(method)
	if o == nil {
		t.Fatalf("no %s %s", method, path)
	}
	return o
}

func statuses(o *openapi.OperationObject) []string { return slices.Sorted(mapsKeys(o.Responses)) }

func mapsKeys[V any](m openapi.Map[V]) iter.Seq[string] {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

func TestOperationIDs(t *testing.T) {
	doc := generate(t, fixtureConfig())
	want := map[string]string{
		"POST /api/orders":                  "createOrder",
		"GET /api/rpc/OrdersGet":            "ordersGet",
		"GET /api/rpc/orders.get":           "ordersGet2",
		"GET /api/rpc/orders.v1.export_csv": "ordersV1ExportCsv",
		"POST /api/rpc/Ping":                "healthPing",
		"HEAD /api/orders/{orderId}/exists": "orderExists",
		"GET /api/":                         "root",
	}
	for key, id := range want {
		method, path, _ := strings.Cut(key, " ")
		if got := op(t, doc, method, path).OperationID; got != id {
			t.Errorf("%s: operationId %q, want %q", key, got, id)
		}
	}
	seen := map[string]bool{}
	for _, item := range doc.Paths {
		for _, method := range []string{"GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"} {
			if o := item.Operation(method); o != nil {
				if seen[o.OperationID] {
					t.Errorf("duplicate operationId %q", o.OperationID)
				}
				seen[o.OperationID] = true
			}
		}
	}
	if len(seen) != 23 {
		t.Errorf("%d operations, want 23", len(seen))
	}
}

func TestResponsesPerOperation(t *testing.T) {
	doc := generate(t, fixtureConfig())
	want := map[string][]string{
		"POST /api/orders":                  {"201", "400", "401", "403", "409", "413", "415", "422", "500"},
		"POST /api/orders/{orderId}/lines":  {"204", "400", "404", "409", "413", "415", "422", "500"},
		"POST /api/orders/{orderId}/submit": {"204", "400", "401", "403", "404", "409", "412", "422", "500"},
		"DELETE /api/orders/{orderId}":      {"204", "400", "409", "413", "415", "422", "500"},
		"GET /api/orders/{orderId}":         {"200", "400", "401", "403", "404", "422", "500"},
		"GET /api/orders":                   {"200", "400", "422", "500"},
		"GET /api/orders/{orderId}/events":  {"200", "400", "422", "500"},
		"GET /api/rpc/CountOrders":          {"200", "400", "422", "429", "500"},
		"POST /api/rpc/SearchOrders":        {"200", "400", "409", "413", "415", "422", "500", "504"},
		"POST /api/rpc/Ping":                {"204", "400", "409", "422", "500"},
		"GET /api/rpc/Whoami":               {"200", "400", "422", "500"},
	}
	for key, codes := range want {
		method, path, _ := strings.Cut(key, " ")
		if got := statuses(op(t, doc, method, path)); !slices.Equal(got, codes) {
			t.Errorf("%s: statuses %v, want %v", key, got, codes)
		}
	}
	// Every error response references the shared Problem schema with the
	// problem media type, and commands document both 422 codes.
	for _, item := range doc.Paths {
		for _, method := range []string{"GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"} {
			o := item.Operation(method)
			if o == nil {
				continue
			}
			for status, resp := range o.Responses {
				if status[0] == '2' {
					continue
				}
				mt := resp.Content["application/problem+json"]
				if mt == nil || mt.Schema.Ref != "#/components/schemas/Problem" {
					t.Errorf("%s %s: %+v", o.OperationID, status, resp.Content)
				}
			}
		}
	}
	create := op(t, doc, "POST", "/api/orders")
	if d := create.Responses["422"].Description; !strings.Contains(d, "(validation)") || !strings.Contains(d, "(idempotency_mismatch)") {
		t.Errorf("422 description %q", d)
	}
	if d := create.Responses["409"].Description; !strings.Contains(d, "(idempotency_in_progress)") || !strings.Contains(d, "(conflict)") {
		t.Errorf("409 description %q", d)
	}
	if h := create.Responses["409"].Headers["Retry-After"]; h == nil || !strings.Contains(h.Description, "idempotency_in_progress") {
		t.Errorf("409 Retry-After %+v", h)
	}
	count := op(t, doc, "GET", "/api/rpc/CountOrders")
	if h := count.Responses["429"].Headers["Retry-After"]; h == nil || h.Schema.Type[0] != "integer" {
		t.Errorf("429 Retry-After %+v", h)
	}
	// A query documenting conflict has 409 without the idempotency header.
	search := op(t, doc, "POST", "/api/rpc/SearchOrders")
	if r := search.Responses["409"]; r.Headers != nil || r.Description != "Conflict (conflict)" {
		t.Errorf("409 on a query: %+v", r)
	}
	if r := search.Responses["504"]; r.Description != "Timeout (timeout)" {
		t.Errorf("504: %+v", r)
	}
}

func TestSecurity(t *testing.T) {
	doc := generate(t, fixtureConfig())
	want := map[string][]string{
		"POST /api/orders":                  {"orders:write"},
		"POST /api/orders/{orderId}/submit": {"orders:submit", "role:admin"},
		"GET /api/orders/{orderId}":         {},
		"GET /api/rpc/Whoami":               nil,
		"GET /api/orders":                   nil,
	}
	for key, scopes := range want {
		method, path, _ := strings.Cut(key, " ")
		o := op(t, doc, method, path)
		if scopes == nil {
			if o.Security != nil {
				t.Errorf("%s: unexpected security %v", key, o.Security)
			}
			continue
		}
		if len(o.Security) != 1 || !slices.Equal(o.Security[0]["bearerAuth"], scopes) {
			t.Errorf("%s: security %v, want %v", key, o.Security, scopes)
		}
	}
	scheme := doc.Components.SecuritySchemes["bearerAuth"]
	if scheme["type"] != "http" || scheme["scheme"] != "bearer" || scheme["bearerFormat"] != "JWT" {
		t.Errorf("security scheme %v", scheme)
	}
	// A custom scheme name and object flow through.
	cfg := fixtureConfig()
	cfg.SecurityScheme = "apiKey"
	cfg.SecuritySchemeObject = map[string]any{"type": "apiKey", "in": "header", "name": "X-API-Key"}
	doc = generate(t, cfg)
	if o := op(t, doc, "POST", "/api/orders"); o.Security[0]["apiKey"] == nil {
		t.Errorf("security %v", o.Security)
	}
	if doc.Components.SecuritySchemes["apiKey"]["name"] != "X-API-Key" {
		t.Errorf("schemes %v", doc.Components.SecuritySchemes)
	}
}

func param(o *openapi.OperationObject, in, name string) *openapi.Parameter {
	for _, p := range o.Parameters {
		if p.In == in && p.Name == name {
			return p
		}
	}
	return nil
}

func TestParameters(t *testing.T) {
	doc := generate(t, fixtureConfig())
	list := op(t, doc, "GET", "/api/orders")
	if p := param(list, "query", "customerId"); p == nil || !p.Required || p.Schema.Format != "uuid" {
		t.Errorf("customerId %+v", p)
	}
	if p := param(list, "query", "page"); p == nil || p.Required || p.Description != "1-based page number" || *p.Schema.Minimum != 1 || p.Schema.Description != "" {
		t.Errorf("page %+v", p)
	}
	if p := param(list, "query", "status"); p == nil || !slices.Equal(p.Schema.Type, validate.Type{"string", "null"}) || len(p.Schema.Enum) != 3 {
		t.Errorf("status %+v", p)
	}
	get := op(t, doc, "GET", "/api/orders/{orderId}")
	if p := param(get, "path", "orderId"); p == nil || !p.Required || p.Schema.Format != "uuid" || p.Description != "Order identifier" {
		t.Errorf("orderId %+v", p)
	}
	if p := param(get, "query", "expand"); p == nil || p.Required || p.Schema.Type[0] != "array" || !p.Schema.UniqueItems || len(p.Schema.Items.Enum) != 2 {
		t.Errorf("expand %+v", p)
	}
	create := op(t, doc, "POST", "/api/orders")
	if p := param(create, "header", "X-Tenant"); p == nil || !p.Required || *p.Schema.MinLength != 1 {
		t.Errorf("X-Tenant %+v", p)
	}
	if p := param(create, "header", "Idempotency-Key"); p == nil || p.Required {
		t.Errorf("Idempotency-Key %+v", p)
	}
	if n := len(create.Parameters); n != 2 {
		t.Errorf("%d parameters", n)
	}
	// The key trait and an explicit header binding both suppress the
	// generated header parameter.
	if o := op(t, doc, "POST", "/api/rpc/ReserveStock"); len(o.Parameters) != 0 {
		t.Errorf("ReserveStock parameters %+v", o.Parameters)
	}
	if o := op(t, doc, "POST", "/api/rpc/Replay"); len(o.Parameters) != 1 || o.Parameters[0].In != "header" {
		t.Errorf("Replay parameters %+v", o.Parameters)
	}
	// RPC GET query: body fields are query parameters and there is no body.
	count := op(t, doc, "GET", "/api/rpc/CountOrders")
	if count.RequestBody != nil || len(count.Parameters) != 3 || param(count, "query", "since").Schema.Format != "date-time" {
		t.Errorf("CountOrders %+v", count.Parameters)
	}
	// The Last-Event-ID header of the stream.
	watch := op(t, doc, "GET", "/api/orders/{orderId}/events")
	if p := param(watch, "header", "Last-Event-ID"); p == nil || p.Description != "Resume after this event" {
		t.Errorf("Last-Event-ID %+v", p)
	}
}

func TestRequestBody(t *testing.T) {
	doc := generate(t, fixtureConfig())
	create := op(t, doc, "POST", "/api/orders")
	if rb := create.RequestBody; rb == nil || !rb.Required || rb.Content["application/json"].Schema.Ref != "#/components/schemas/CreateOrder" {
		t.Fatalf("requestBody %+v", create.RequestBody)
	}
	body := doc.Components.Schemas["CreateOrder"]
	if body.Properties.Get("tenant") != nil {
		t.Error("bound field in the body schema")
	}
	if body.AdditionalProperties == nil || body.AdditionalProperties.Allow {
		t.Error("body is not closed")
	}
	if !slices.Equal(body.Required, []string{"customerId", "lines", "currency"}) {
		t.Errorf("required %v", body.Required)
	}
	if e := body.Properties.Get("currency").Enum; len(e) != 3 || e[0] != "USD" {
		t.Errorf("currency enum %v", e)
	}
	if s := body.Properties.Get("customerId"); s.Format != "uuid" || s.Description != "Customer placing the order" {
		t.Errorf("customerId %+v", s)
	}
	if s := body.Properties.Get("attachment"); s.ContentEncoding != "base64" {
		t.Errorf("attachment %+v", s)
	}
	if s := body.Properties.Get("deliverAt"); s.Format != "date-time" || !slices.Contains(s.Type, "null") {
		t.Errorf("deliverAt %+v", s)
	}
	if s := body.Properties.Get("billing"); len(s.AnyOf) != 2 || s.AnyOf[0].Ref != "#/components/schemas/Address" {
		t.Errorf("billing %+v", s)
	}
	if s := body.Properties.Get("metadata"); s.AdditionalProperties.Schema == nil || *s.MaxProperties != 20 {
		t.Errorf("metadata %+v", s)
	}
	if s := body.Properties.Get("budget"); s.Ref != "#/components/schemas/Money" {
		t.Errorf("budget %+v", s)
	}
	money := doc.Components.Schemas["Money"]
	if money.Extra["x-money"] != true || money.Pattern == "" {
		t.Errorf("Money %+v", money)
	}
	// No body fields: no requestBody, no 413/415; DELETE with a field: body.
	submit := op(t, doc, "POST", "/api/orders/{orderId}/submit")
	if submit.RequestBody != nil {
		t.Errorf("submit body %+v", submit.RequestBody)
	}
	cancel := op(t, doc, "DELETE", "/api/orders/{orderId}")
	if cancel.RequestBody == nil || cancel.RequestBody.Content["application/json"].Schema.Ref != "#/components/schemas/CancelOrder" {
		t.Errorf("cancel body %+v", cancel.RequestBody)
	}
	if get := op(t, doc, "GET", "/api/orders/{orderId}"); get.RequestBody != nil {
		t.Error("GET has a body")
	}
	// PUT and PATCH land in their slots.
	if op(t, doc, "PUT", "/api/orders/{orderId}").OperationID != "updateOrder" || op(t, doc, "PATCH", "/api/orders/{orderId}").OperationID != "patchOrder" {
		t.Error("put/patch")
	}
}

func TestSuccessResponses(t *testing.T) {
	doc := generate(t, fixtureConfig())
	add := op(t, doc, "POST", "/api/orders/{orderId}/lines")
	if r := add.Responses["204"]; r.Content != nil || r.Description != "No Content" {
		t.Errorf("Void 204 %+v", r)
	}
	if r := op(t, doc, "HEAD", "/api/orders/{orderId}/exists").Responses["204"]; r.Content != nil {
		t.Errorf("HEAD 204 %+v", r)
	}
	create := op(t, doc, "POST", "/api/orders")
	if r := create.Responses["201"]; r.Content["application/json"].Schema.Ref != "#/components/schemas/CreateOrderResult" || r.Headers != nil {
		t.Errorf("201 %+v", r)
	}
	get := op(t, doc, "GET", "/api/orders/{orderId}")
	if h := get.Responses["200"].Headers["X-Mediator-Cache"]; h == nil || len(h.Schema.Enum) != 3 {
		t.Errorf("cache header %+v", h)
	}
	if r := op(t, doc, "GET", "/api/rpc/CountOrders").Responses["200"]; r.Content["application/json"].Schema.Type[0] != "integer" {
		t.Errorf("scalar response %+v", r)
	}
	list := op(t, doc, "GET", "/api/orders")
	ref := list.Responses["200"].Content["application/json"].Schema.Ref
	if !strings.HasPrefix(ref, "#/components/schemas/Page_") {
		t.Errorf("page ref %q", ref)
	}
	page := doc.Components.Schemas[strings.TrimPrefix(ref, "#/components/schemas/")]
	if page == nil || page.Properties.Get("items").Items.Ref != "#/components/schemas/OrderSummary" {
		t.Errorf("page schema %+v", page)
	}
	view := doc.Components.Schemas["OrderView"]
	if view.Properties.Get("extra") == nil || view.Properties.Get("status").Type[0] != "string" {
		t.Errorf("OrderView %+v", view)
	}
}

func TestStream(t *testing.T) {
	doc := generate(t, fixtureConfig())
	watch := op(t, doc, "GET", "/api/orders/{orderId}/events")
	mt := watch.Responses["200"].Content["text/event-stream"]
	if mt == nil || mt.Schema.Type[0] != "string" || mt.SSEItem == nil || mt.SSEItem.Ref != "#/components/schemas/OrderEvent" {
		t.Fatalf("stream media type %+v", mt)
	}
	if _, ok := watch.Responses["200"].Content["application/json"]; ok {
		t.Error("stream has a JSON response")
	}
	if doc.Components.Schemas["OrderEvent"] == nil {
		t.Error("item schema not registered")
	}
	b, _ := doc.MarshalJSON()
	if !strings.Contains(string(b), `"x-sse-item": {`) {
		t.Error("x-sse-item not emitted")
	}
}

func TestTagsAndPaths(t *testing.T) {
	doc := generate(t, fixtureConfig())
	var names []string
	for _, tag := range doc.Tags {
		names = append(names, tag.Name)
	}
	if want := []string{"default", "events", "files", "orders", "read", "reports", "rpc", "system"}; !slices.Equal(names, want) {
		t.Errorf("tags %v, want %v", names, want)
	}
	if o := op(t, doc, "GET", "/api/rpc/Whoami"); !slices.Equal(o.Tags, []string{"rpc"}) {
		t.Errorf("rpc tags %v", o.Tags)
	}
	if o := op(t, doc, "GET", "/api/{token}"); !slices.Equal(o.Tags, []string{"default"}) {
		t.Errorf("token tags %v", o.Tags)
	}
	if o := op(t, doc, "GET", "/api/orders/{orderId}"); !slices.Equal(o.Tags, []string{"orders", "read"}) {
		t.Errorf("described tags %v", o.Tags)
	}
	for _, p := range []string{"/api/files/{path}", "/api/reports/", "/api/"} {
		if doc.Paths[p] == nil {
			t.Errorf("path %s missing; have %v", p, slices.Sorted(mapsKeys(doc.Paths)))
		}
	}
	if o := op(t, doc, "GET", "/api/rpc/orders.v1.export_csv"); !o.Deprecated || o.Summary != "Export orders (legacy)" {
		t.Errorf("legacy %+v", o)
	}
	if doc.OpenAPI != "3.1.0" || doc.JSONSchemaDialect != openapi.Dialect || doc.Info.Title != "Orders" || doc.Servers[0].URL != "http://localhost:8080" {
		t.Errorf("header %+v", doc)
	}
}

func TestProblemSchema(t *testing.T) {
	doc := generate(t, fixtureConfig())
	p := doc.Components.Schemas["Problem"]
	if p == nil {
		t.Fatal("no Problem schema")
	}
	if want := []string{"type", "title", "status", "detail", "instance", "code"}; !slices.Equal(p.Required, want) {
		t.Errorf("required %v", p.Required)
	}
	if p.Properties.Get("errors").Items.Ref != "#/components/schemas/FieldError" {
		t.Errorf("errors %+v", p.Properties.Get("errors"))
	}
	if fe := doc.Components.Schemas["FieldError"]; fe == nil || fe.Properties.Get("path") == nil {
		t.Errorf("FieldError %+v", fe)
	}
	if p.Description == "" {
		t.Error("no description")
	}
}

func TestOptions(t *testing.T) {
	cfg := fixtureConfig()
	cfg.NoDescriptions = true
	cfg.AllowUnknownFields = true
	doc := generate(t, cfg)
	body := doc.Components.Schemas["CreateOrder"]
	if body.AdditionalProperties != nil {
		t.Error("additionalProperties emitted with AllowUnknownFields")
	}
	if body.Properties.Get("customerId").Description != "" {
		t.Error("description emitted with NoDescriptions")
	}
	if p := param(op(t, doc, "GET", "/api/orders"), "query", "page"); p.Description != "" {
		t.Error("parameter description emitted with NoDescriptions")
	}
	// Zero config: defaults for title, version, and the security scheme.
	doc, err := openapi.Generate(newFixture(t), openapi.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if doc.Info.Title != openapi.DefaultTitle || doc.Info.Version != openapi.DefaultVersion || doc.Servers != nil {
		t.Errorf("defaults %+v", doc.Info)
	}
	if doc.Components.SecuritySchemes[openapi.DefaultSecurityScheme]["scheme"] != "bearer" {
		t.Errorf("schemes %v", doc.Components.SecuritySchemes)
	}
	if doc.Paths["/orders"] == nil || doc.Paths["/rpc/Ping"] == nil {
		t.Errorf("paths without prefix: %v", slices.Sorted(mapsKeys(doc.Paths)))
	}
	if o := op(t, doc, "POST", "/rpc/Ping"); !slices.Equal(o.Tags, []string{"system"}) {
		t.Errorf("tags %v", o.Tags)
	}
	if o := op(t, doc, "GET", "/rpc/Whoami"); !slices.Equal(o.Tags, []string{"rpc"}) {
		t.Errorf("tags %v", o.Tags)
	}
}

// ---- errors ----

type badView struct {
	X int `json:"x" validate:"email"`
}

type badBody struct {
	mediator.Command[mediator.Void]
	X int `json:"x" validate:"email"`
}

type badParam struct {
	mediator.Query[string]
	X int `json:"x" query:"x" validate:"email"`
}

type badResponse struct {
	mediator.Query[badView]
}

type badStream struct {
	mediator.StreamQuery[badView]
}

type badCode struct {
	mediator.Query[string]
}

func (badCode) Describe() openapi.Operation {
	return openapi.Operation{Errors: []string{"nope", "not_found"}}
}

func TestGenerateErrors(t *testing.T) {
	t.Run("unbuilt", func(t *testing.T) {
		if _, err := openapi.Generate(mediator.New(), openapi.Config{}); err == nil || !strings.Contains(err.Error(), "not built") {
			t.Errorf("err = %v", err)
		}
		if _, err := openapi.Generate(nil, openapi.Config{}); err == nil {
			t.Error("nil mediator accepted")
		}
	})
	t.Run("prefix", func(t *testing.T) {
		_, err := openapi.Generate(newFixture(t), openapi.Config{Prefix: "/{bad}"})
		if err == nil || !strings.Contains(err.Error(), "prefix") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("all at once", func(t *testing.T) {
		m := mediator.New()
		register[badBody, mediator.Void](t, m)
		register[badParam, string](t, m)
		register[badResponse, badView](t, m)
		registerStream[badStream, badView](t, m)
		register[badCode, string](t, m)
		if err := m.Build(); err != nil {
			t.Fatal(err)
		}
		_, err := openapi.Generate(m, openapi.Config{})
		if err == nil {
			t.Fatal("no error")
		}
		for _, want := range []string{
			"badBody: request body:", "badParam: query parameter \"x\":", "badResponse: response:", "badStream: response:",
			`badCode: Describe().Errors lists unknown code "nope"`, `rule "email" is not supported on int`,
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error lacks %q:\n%v", want, err)
			}
		}
	})
	t.Run("problem schema", func(t *testing.T) {
		// A validator reading rules from the json tag cannot compile
		// httpapi.Problem ("type" is not a rule).
		_, err := openapi.Generate(newFixture(t), openapi.Config{Validator: validate.New(validate.WithTagKey("json"))})
		if err == nil || !strings.Contains(err.Error(), "Problem schema") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("invalid security scheme", func(t *testing.T) {
		_, err := openapi.Generate(newFixture(t), openapi.Config{SecuritySchemeObject: map[string]any{"type": "bogus"}})
		if err == nil || !strings.Contains(err.Error(), "not valid OpenAPI 3.1") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("unmarshalable security scheme", func(t *testing.T) {
		_, err := openapi.Generate(newFixture(t), openapi.Config{SecuritySchemeObject: map[string]any{"type": func() {}}})
		if err == nil || !strings.Contains(err.Error(), "marshal document") {
			t.Errorf("err = %v", err)
		}
	})
}

// TestServedThroughHTTPAPI mounts the handler the way applications do.
func TestServedThroughHTTPAPI(t *testing.T) {
	m := newFixture(t)
	doc, err := openapi.Generate(m, fixtureConfig())
	if err != nil {
		t.Fatal(err)
	}
	srv, err := httpapi.New(m, httpapi.Config{Prefix: "/api", Docs: openapi.Handler(doc)})
	if err != nil {
		t.Fatal(err)
	}
	want, _ := doc.MarshalJSON()
	rec := do(srv, "GET", "/api/openapi.json")
	if rec.Code != 200 || !bytes.Equal(rec.Body.Bytes(), want) {
		t.Errorf("openapi.json: %d %s", rec.Code, rec.Body.String()[:min(80, rec.Body.Len())])
	}
	if err := openapi.ValidateJSON(rec.Body.Bytes()); err != nil {
		t.Error(err)
	}
	rec = do(srv, "GET", "/api/docs")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `url: './openapi.json'`) {
		t.Errorf("docs: %d %s", rec.Code, rec.Body.String())
	}
	if rec = do(srv, "HEAD", "/api/openapi.json"); rec.Code != 200 || rec.Body.Len() != 0 {
		t.Errorf("HEAD: %d %d bytes", rec.Code, rec.Body.Len())
	}
	if rec = do(srv, "GET", "/docs"); rec.Code != 404 {
		t.Errorf("unprefixed docs: %d", rec.Code)
	}
	// WriteFile produces the same bytes the handler serves.
	path := filepath.Join(t.TempDir(), "openapi.json")
	if err := doc.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(path); !bytes.Equal(b, want) {
		t.Error("file differs from the served document")
	}
}
