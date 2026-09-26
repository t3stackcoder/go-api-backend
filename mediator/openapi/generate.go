package openapi

import (
	"errors"
	"fmt"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

var (
	describerT = reflect.TypeFor[Describer]()
	problemT   = reflect.TypeFor[httpapi.Problem]()
	voidT      = reflect.TypeFor[mediator.Void]()
)

// knownCodes lists every mediator.Code; Describe().Errors must use them.
var knownCodes = []mediator.Code{
	mediator.CodeBadRequest, mediator.CodePayloadTooLarge, mediator.CodeUnsupportedMedia,
	mediator.CodeValidation, mediator.CodeNotFound, mediator.CodeConflict, mediator.CodePrecondition,
	mediator.CodeUnauthorized, mediator.CodeForbidden, mediator.CodeRateLimited, mediator.CodeTimeout,
	mediator.CodeUnavailable, mediator.CodeIdempotencyMismatch, mediator.CodeIdempotencyBusy,
	mediator.CodeHandlerNotFound, mediator.CodeInternal,
}

// Media types and headers the document mentions.
const (
	mediaJSON    = "application/json"
	mediaProblem = httpapi.ProblemContentType
	mediaSSE     = "text/event-stream"

	headerIdempotencyKey = "Idempotency-Key"
	headerRetryAfter     = "Retry-After"
	headerCacheResult    = "X-Mediator-Cache"

	defaultTag = "default"
)

// Generate builds the OpenAPI document of a built mediator following spec
// 8.6. It derives the routing table exactly as httpapi.New does with the
// configured prefix, assembles one operation per request, and validates the
// result against the embedded OpenAPI 3.1 schema. Every problem found while
// assembling is reported at once through errors.Join.
func Generate(m *mediator.Mediator, cfg Config) (*Document, error) {
	if m == nil || !m.Built() {
		return nil, errors.New("openapi: mediator is not built")
	}
	cfg = cfg.withDefaults()
	srv, err := httpapi.New(m, httpapi.Config{Prefix: cfg.Prefix, AllowUnknownFields: cfg.AllowUnknownFields})
	if err != nil {
		return nil, fmt.Errorf("openapi: %w", err)
	}
	g := &generator{
		cfg:    cfg,
		prefix: srv.Config().Prefix,
		reg:    &validate.Schemas{},
		ids:    map[string]bool{},
		tags:   map[string]bool{},
	}
	if err := g.registerProblem(); err != nil {
		return nil, err
	}
	doc := &Document{
		OpenAPI:           Version,
		JSONSchemaDialect: Dialect,
		Info:              cfg.Info,
		Servers:           cfg.Servers,
		Paths:             Map[*PathItem]{},
	}
	var errs []error
	for _, rt := range srv.Routes() {
		op, err := g.operation(rt)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		path := openAPIPath(rt.Path)
		item := doc.Paths[path]
		if item == nil {
			item = &PathItem{}
			doc.Paths[path] = item
		}
		*item.slot(rt.Method) = op
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	doc.Components = &Components{
		Schemas:         Map[*validate.Schema](g.reg.Defs),
		SecuritySchemes: Map[map[string]any]{cfg.SecurityScheme: cfg.SecuritySchemeObject},
	}
	for _, name := range slices.Sorted(maps.Keys(g.tags)) {
		doc.Tags = append(doc.Tags, Tag{Name: name})
	}
	if err := Validate(doc); err != nil {
		return nil, err
	}
	return doc, nil
}

// generator is the state of one Generate call.
type generator struct {
	cfg     Config
	prefix  string
	reg     *validate.Schemas
	problem *validate.Schema // $ref to the Problem schema, shared by every error response
	ids     map[string]bool  // operationIds handed out
	tags    map[string]bool  // tags used
}

// options returns the schema options of this document with an optional
// field filter.
func (g *generator) options(skip func(reflect.StructField) bool) validate.SchemaOptions {
	return validate.SchemaOptions{
		AllowUnknownFields: g.cfg.AllowUnknownFields,
		SkipField:          skip,
		Descriptions:       !g.cfg.NoDescriptions,
	}
}

// schemaOf returns the schema of t, registering named structs in the
// components.
func (g *generator) schemaOf(t reflect.Type) (*validate.Schema, error) {
	return g.cfg.Validator.SchemaFor(t, g.reg, g.options(nil))
}

// registerProblem registers the shared Problem schema built from
// httpapi.Problem. Its members are always present on the wire except those
// tagged omitempty, which validate cannot know, so the required list is set
// here from the json tags.
func (g *generator) registerProblem() error {
	s, err := g.schemaOf(problemT)
	if err != nil {
		return fmt.Errorf("openapi: Problem schema: %w", err)
	}
	name, _ := g.reg.NameOf(problemT)
	def := g.reg.Defs[name]
	def.Description = "RFC 9457 problem details. Every error response of the API has this shape."
	def.Required = nil
	for i := 0; i < problemT.NumField(); i++ {
		member, opts, _ := strings.Cut(problemT.Field(i).Tag.Get("json"), ",")
		if !slices.Contains(strings.Split(opts, ","), "omitempty") && !slices.Contains(strings.Split(opts, ","), "omitzero") {
			def.Required = append(def.Required, member)
		}
	}
	g.problem = s
	return nil
}

// operation assembles the operation of one route.
func (g *generator) operation(rt httpapi.RouteInfo) (*OperationObject, error) {
	info := rt.Info
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("openapi: %s: "+format, append([]any{info.Name}, args...)...))
	}
	var desc Operation
	if info.Implements(describerT) {
		d, _ := reflect.Zero(info.RequestType).Interface().(Describer) // Implements guarantees the assertion
		desc = d.Describe()
	}
	op := &OperationObject{
		Tags:        slices.Clone(desc.Tags),
		Summary:     desc.Summary,
		Description: desc.Description,
		Deprecated:  desc.Deprecated,
		Responses:   Map[*Response]{},
	}
	if len(op.Tags) == 0 {
		op.Tags = []string{g.defaultTag(rt.Path)}
	}
	for _, t := range op.Tags {
		g.tags[t] = true
	}
	id := desc.OperationID
	if id == "" {
		id = camelCase(info.Name)
	}
	op.OperationID = g.uniqueID(id)

	codes := &codeSet{}
	codes.add(mediator.CodeBadRequest, mediator.CodeValidation, mediator.CodeInternal)

	// Parameters and body.
	hasBody := false
	for _, b := range rt.Bindings {
		if b.Source == httpapi.SourceBody {
			hasBody = true
			continue
		}
		p, err := g.parameter(b)
		if err != nil {
			fail("%s parameter %q: %w", b.Source, b.Name, err)
			continue
		}
		op.Parameters = append(op.Parameters, p)
	}
	if hasBody && rt.Method != http.MethodGet && rt.Method != http.MethodHead {
		s, err := g.cfg.Validator.SchemaFor(info.RequestType, g.reg, g.options(isBound))
		if err != nil {
			fail("request body: %w", err)
		} else {
			op.RequestBody = &RequestBody{
				Required: true,
				Content:  Map[*MediaType]{mediaJSON: {Schema: s}},
			}
			codes.add(mediator.CodePayloadTooLarge, mediator.CodeUnsupportedMedia)
		}
	}

	// Traits.
	if info.Kind == mediator.KindCommand {
		if !info.Traits.IdempotencyKey && !bindsHeader(rt.Bindings, headerIdempotencyKey) {
			op.Parameters = append(op.Parameters, &Parameter{
				Name:        headerIdempotencyKey,
				In:          "header",
				Description: "Idempotency key of the command. A repeated key with the same payload replays the first response; a different payload is rejected with idempotency_mismatch.",
				Schema:      &validate.Schema{Type: validate.Type{"string"}},
			})
		}
		codes.add(mediator.CodeIdempotencyBusy, mediator.CodeIdempotencyMismatch)
	}
	if info.Traits.Requires {
		r, _ := reflect.Zero(info.RequestType).Interface().(mediator.Requirer) // Traits.Requires guarantees the assertion
		if req := r.Requires(); req != nil {
			scopes := req.Describe().Scopes()
			if scopes == nil {
				scopes = []string{}
			}
			op.Security = []SecurityRequirement{{g.cfg.SecurityScheme: scopes}}
			codes.add(mediator.CodeUnauthorized, mediator.CodeForbidden)
		}
	}
	if info.Traits.RateLimit {
		codes.add(mediator.CodeRateLimited)
	}
	for _, c := range desc.Errors {
		code := mediator.Code(c)
		if !slices.Contains(knownCodes, code) {
			fail("Describe().Errors lists unknown code %q", c)
			continue
		}
		codes.add(code)
	}

	// Responses.
	status, success, err := g.successResponse(rt)
	if err != nil {
		fail("response: %w", err)
	} else {
		op.Responses[strconv.Itoa(status)] = success
	}
	for _, st := range codes.statuses() {
		op.Responses[strconv.Itoa(st)] = g.errorResponse(st, codes.byStatus[st])
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return op, nil
}

// parameter builds the parameter of a path, query, or header binding. The
// schema comes from validate applied to the field alone, so the tag rules
// and doc of the field carry over. Path parameters are always required;
// the others only when their tag says required.
func (g *generator) parameter(b httpapi.Binding) (*Parameter, error) {
	one := reflect.StructOf([]reflect.StructField{{Name: b.Field.Name, Type: b.Field.Type, Tag: b.Field.Tag}})
	s, err := g.cfg.Validator.SchemaFor(one, g.reg, g.options(nil))
	if err != nil {
		return nil, err
	}
	fs := s.Properties.Get(b.JSONName)
	p := &Parameter{
		Name:        b.Name,
		In:          b.Source.String(),
		Description: fs.Description,
		Required:    b.Source == httpapi.SourcePath || slices.Contains(s.Required, b.JSONName),
		Schema:      fs,
	}
	fs.Description = ""
	return p, nil
}

// successResponse builds the success response and its status: 204 without
// content for Void or a declared 204, text/event-stream for streams, else
// application/json with the response schema.
func (g *generator) successResponse(rt httpapi.RouteInfo) (int, *Response, error) {
	info := rt.Info
	if info.Kind == mediator.KindStream {
		item, err := g.schemaOf(info.ResponseType)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusOK, &Response{
			Description: "Server-Sent Events stream. Every data: frame is one item encoded as JSON (see " + SSEItemKey + "); a terminal error is an event: error frame carrying a Problem.",
			Content:     Map[*MediaType]{mediaSSE: {Schema: &validate.Schema{Type: validate.Type{"string"}}, SSEItem: item}},
		}, nil
	}
	if info.ResponseType == voidT || rt.Status == http.StatusNoContent {
		return http.StatusNoContent, &Response{Description: http.StatusText(http.StatusNoContent)}, nil
	}
	s, err := g.schemaOf(info.ResponseType)
	if err != nil {
		return 0, nil, err
	}
	resp := &Response{
		Description: http.StatusText(rt.Status),
		Content:     Map[*MediaType]{mediaJSON: {Schema: s}},
	}
	if info.Traits.CacheTags {
		resp.Headers = Map[*Header]{headerCacheResult: {
			Description: "Whether the response came from the cache.",
			Schema:      &validate.Schema{Type: validate.Type{"string"}, Enum: []any{"hit", "miss", "bypass"}},
		}}
	}
	return rt.Status, resp, nil
}

// errorResponse builds the problem response of one status, describing every
// code that maps to it.
func (g *generator) errorResponse(status int, codes []mediator.Code) *Response {
	parts := make([]string, len(codes))
	for i, c := range codes {
		parts[i] = c.Title() + " (" + string(c) + ")"
	}
	resp := &Response{
		Description: strings.Join(parts, "; "),
		Content:     Map[*MediaType]{mediaProblem: {Schema: g.problem}},
	}
	if slices.Contains(codes, mediator.CodeRateLimited) || slices.Contains(codes, mediator.CodeIdempotencyBusy) {
		desc := "Seconds to wait before retrying."
		if status != http.StatusTooManyRequests {
			desc = "Seconds to wait before retrying; set for " + string(mediator.CodeIdempotencyBusy) + "."
		}
		one := 1.0
		resp.Headers = Map[*Header]{headerRetryAfter: {
			Description: desc,
			Schema:      &validate.Schema{Type: validate.Type{"integer"}, Minimum: &one},
		}}
	}
	return resp
}

// defaultTag is the first path segment after the prefix, or "default" when
// there is none or it is a parameter.
func (g *generator) defaultTag(path string) string {
	rel := strings.TrimPrefix(strings.TrimPrefix(path, g.prefix), "/")
	seg, _, _ := strings.Cut(rel, "/")
	if seg == "" || strings.HasPrefix(seg, "{") {
		return defaultTag
	}
	return seg
}

// uniqueID reserves id, suffixing a counter when it is taken.
func (g *generator) uniqueID(id string) string {
	out := id
	for i := 2; g.ids[out]; i++ {
		out = id + strconv.Itoa(i)
	}
	g.ids[out] = true
	return out
}

// codeSet groups error codes by their HTTP status, keeping the order in
// which codes were added within a status.
type codeSet struct {
	byStatus map[int][]mediator.Code
}

func (s *codeSet) add(codes ...mediator.Code) {
	if s.byStatus == nil {
		s.byStatus = map[int][]mediator.Code{}
	}
	for _, c := range codes {
		st := mediator.StatusOf(c)
		if !slices.Contains(s.byStatus[st], c) {
			s.byStatus[st] = append(s.byStatus[st], c)
		}
	}
}

func (s *codeSet) statuses() []int { return slices.Sorted(maps.Keys(s.byStatus)) }

// camelCase lowers the first rune of a request name. A pinned name with
// dots or underscores is treated as segments joined in camel case:
// "orders.v1.export_csv" becomes "ordersV1ExportCsv".
func camelCase(name string) string {
	var b strings.Builder
	for i, seg := range strings.FieldsFunc(name, func(r rune) bool { return r == '.' || r == '_' }) {
		r, size := utf8.DecodeRuneInString(seg)
		if i == 0 {
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(unicode.ToUpper(r))
		}
		b.WriteString(seg[size:])
	}
	return b.String()
}

// openAPIPath converts a ServeMux path to an OpenAPI path: the {$} exact
// match marker is dropped and a {name...} wildcard becomes {name}.
func openAPIPath(path string) string {
	path = strings.TrimSuffix(path, "{$}")
	return strings.ReplaceAll(path, "...}", "}")
}

// isBound reports whether a field is bound from the path, query, or a
// header and therefore does not belong to the body schema.
func isBound(sf reflect.StructField) bool {
	for _, key := range []string{"path", "query", "header"} {
		if _, ok := sf.Tag.Lookup(key); ok {
			return true
		}
	}
	return false
}

// bindsHeader reports whether a binding reads the named header.
func bindsHeader(bindings []httpapi.Binding, name string) bool {
	for _, b := range bindings {
		if b.Source == httpapi.SourceHeader && strings.EqualFold(b.Name, name) {
			return true
		}
	}
	return false
}
