package httpapi

import (
	"encoding"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Router is the trait a request implements to declare a REST route instead of
// the RPC default. It is detected at Build with
// RequestInfo.Implements(reflect.TypeFor[Router]()) and invoked on the zero
// value, so it must be a value-receiver method that returns a constant.
type Router interface{ Route() Route }

var (
	routerT          = reflect.TypeFor[Router]()
	textUnmarshalerT = reflect.TypeFor[encoding.TextUnmarshaler]()
	timeT            = reflect.TypeFor[time.Time]()
	voidT            = reflect.TypeFor[mediator.Void]()
)

// validMethods are the methods a Route may declare, upper case.
var validMethods = []string{http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete}

// reservedPaths are mounted under the prefix by every Server and may not be
// claimed by a request route.
var reservedPaths = []string{"/healthz", "/readyz", "/docs", "/openapi.json"}

// RouteOf returns the route of a registered request: the Route() trait when
// the type implements Router, else the RPC default. Commands are
// POST /rpc/{Name}; streams are GET /rpc/{Name}; queries are GET /rpc/{Name}
// with every body-bound field read from the query string, unless a
// body-bound field is not a scalar or a slice of scalars, in which case the
// query is POST with a JSON body. Scalars are strings, booleans, integers,
// floats, time.Time, and any encoding.TextUnmarshaler such as uuid.UUID,
// optionally behind a pointer.
func RouteOf(info *mediator.RequestInfo) Route {
	if info.Implements(routerT) {
		return reflect.Zero(info.RequestType).Interface().(Router).Route()
	}
	path := "/rpc/" + info.Name
	switch info.Kind {
	case mediator.KindCommand:
		return Route{Method: http.MethodPost, Path: path}
	case mediator.KindQuery:
		bindings, err := BindingsOf(info.RequestType)
		if err != nil || !allScalar(bindings) {
			// Bindings that cannot be computed are reported by BuildCheck;
			// a body is the safe default meanwhile.
			return Route{Method: http.MethodPost, Path: path}
		}
		return Route{Method: http.MethodGet, Path: path}
	case mediator.KindStream:
		return Route{Method: http.MethodGet, Path: path}
	}
	return Route{}
}

func allScalar(bindings []Binding) bool {
	for _, b := range bindings {
		if b.Source == SourceBody && !isScalarOrSlice(b.Field.Type) {
			return false
		}
	}
	return true
}

// Source is where a request field is bound from.
type Source uint8

const (
	// SourceBody fields come from the JSON body (or, for the RPC GET query
	// default, from the query string under their JSON name).
	SourceBody Source = iota
	// SourcePath fields come from a path segment: path:"orderId".
	SourcePath
	// SourceQuery fields come from the query string: query:"page".
	SourceQuery
	// SourceHeader fields come from a header: header:"X-Tenant".
	SourceHeader
)

// String returns the tag name of the source: body, path, query, or header.
func (s Source) String() string {
	switch s {
	case SourcePath:
		return "path"
	case SourceQuery:
		return "query"
	case SourceHeader:
		return "header"
	default:
		return "body"
	}
}

// Binding describes how one field of a request struct is populated.
type Binding struct {
	// Field is the struct field. Index is the full index path from the
	// request type, so embedded structs are addressed correctly.
	Field reflect.StructField
	// Source is where the value comes from.
	Source Source
	// Name is the path parameter, query key, or header name; for body
	// fields it equals JSONName.
	Name string
	// JSONName is the member name in the JSON body and in error paths.
	JSONName string
}

// BindingsOf computes the bindings of a request type from its struct tags.
// Marker fields, json:"-" fields, and unexported fields are skipped; embedded
// structs are flattened as encoding/json does. Path, query, and header fields
// must be scalars (path) or scalars and slices of scalars (query, header). A
// field may carry at most one of path, query, and header. An empty tag value
// (query:"") binds under the JSON name. Every violation is reported through
// errors.Join.
func BindingsOf(t reflect.Type) ([]Binding, error) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("httpapi: %s is not a struct", t)
	}
	var out []Binding
	var errs []error
	collectBindings(t, nil, map[reflect.Type]bool{}, &out, &errs)
	seen := map[string]string{}
	for _, b := range out {
		if b.Source == SourceBody {
			continue
		}
		key := b.Source.String() + ":" + b.Name
		if b.Source == SourceHeader {
			key = "header:" + http.CanonicalHeaderKey(b.Name)
		}
		if prev, dup := seen[key]; dup {
			errs = append(errs, fmt.Errorf("httpapi: %s: fields %s and %s both bind %s %q", t, prev, b.Field.Name, b.Source, b.Name))
			continue
		}
		seen[key] = b.Field.Name
	}
	return out, errors.Join(errs...)
}

func collectBindings(t reflect.Type, index []int, visiting map[reflect.Type]bool, out *[]Binding, errs *[]error) {
	if visiting[t] {
		return
	}
	visiting[t] = true
	defer delete(visiting, t)
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		idx := append(slices.Clone(index), i)
		jsonName, skip := jsonNameOf(f)
		if skip {
			continue
		}
		if f.Anonymous {
			ft := f.Type
			isPtr := ft.Kind() == reflect.Pointer
			if isPtr {
				ft = ft.Elem()
			}
			if mediator.IsMarker(ft) {
				continue
			}
			if ft.Kind() == reflect.Struct && !hasJSONName(f) {
				if isPtr && !f.IsExported() {
					continue // encoding/json ignores embedded pointers to unexported structs
				}
				collectBindings(ft, idx, visiting, out, errs)
				continue
			}
		}
		if !f.IsExported() {
			continue
		}
		f.Index = idx
		b := Binding{Field: f, Source: SourceBody, Name: jsonName, JSONName: jsonName}
		sources := 0
		for _, src := range []struct {
			tag string
			s   Source
		}{{"path", SourcePath}, {"query", SourceQuery}, {"header", SourceHeader}} {
			v, ok := f.Tag.Lookup(src.tag)
			if !ok {
				continue
			}
			sources++
			b.Source = src.s
			b.Name = v
			if v == "" {
				b.Name = jsonName
			}
		}
		if sources > 1 {
			*errs = append(*errs, fmt.Errorf("httpapi: %s.%s has %d binding sources; at most one of path, query, header is allowed", t, f.Name, sources))
			continue
		}
		switch b.Source {
		case SourcePath:
			if !isScalar(f.Type) {
				*errs = append(*errs, fmt.Errorf("httpapi: %s.%s is bound from the path but %s is not a scalar", t, f.Name, f.Type))
				continue
			}
		case SourceQuery, SourceHeader:
			if !isScalarOrSlice(f.Type) {
				*errs = append(*errs, fmt.Errorf("httpapi: %s.%s is bound from the %s but %s is not a scalar or a slice of scalars", t, f.Name, b.Source, f.Type))
				continue
			}
		}
		*out = append(*out, b)
	}
}

// jsonNameOf returns the JSON member name of f and whether json ignores it.
func jsonNameOf(f reflect.StructField) (string, bool) {
	tag, ok := f.Tag.Lookup("json")
	if !ok {
		return f.Name, false
	}
	if tag == "-" {
		return "", true
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return f.Name, false
	}
	return name, false
}

func hasJSONName(f reflect.StructField) bool {
	name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
	return name != ""
}

// isScalar reports whether t converts from a single string: a string, bool,
// integer, or float kind, time.Time, or an encoding.TextUnmarshaler,
// optionally behind one pointer.
func isScalar(t reflect.Type) bool {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == timeT || reflect.PointerTo(t).Implements(textUnmarshalerT) {
		return true
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

// isScalarOrSlice also accepts slices of scalars, except []byte, which JSON
// treats as a base64 string.
func isScalarOrSlice(t reflect.Type) bool {
	if isScalar(t) {
		return true
	}
	return t.Kind() == reflect.Slice && t.Elem().Kind() != reflect.Uint8 && isScalar(t.Elem())
}

// RouteInfo is one HTTP operation, for the openapi package.
type RouteInfo struct {
	// Method is the HTTP method.
	Method string
	// Path is the full ServeMux path pattern including the prefix.
	Path string
	// Info is the registered request.
	Info *mediator.RequestInfo
	// Bindings describes every field; body-bound fields of an RPC GET query
	// are reported as SourceQuery, which is where they are read from.
	Bindings []Binding
	// Status is the success status: Route.Status, else 204 for Void, else 200.
	Status int
}

// route is the compiled form of one operation.
type route struct {
	info       *mediator.RequestInfo
	method     string
	path       string // full path with prefix
	pattern    string // ServeMux pattern
	status     int
	bindings   []Binding
	params     []Binding         // path, query, and header bindings
	boundNames map[string]Source // JSON names of params, rejected in the body
}

// BuildCheck validates every registered request for the HTTP adapter and
// reports every violation at once: the Route() method is one of GET, HEAD,
// POST, PUT, PATCH, DELETE; the path starts with "/" and has no whitespace;
// Status is zero or 2xx; the bindings are well-formed (BindingsOf); every
// {param} in the pattern has a field tagged path:"param" and vice versa; GET
// and HEAD routes have no body-bound fields; no route claims a reserved path;
// and no two routes produce conflicting ServeMux patterns. New runs the same
// check with the configured prefix.
//
// It reads the registry through Mediator.Requests, so it must run after
// Build; attach it with m.OnBuild once the core invokes hooks outside its
// registry lock, or call it right after Build.
func BuildCheck(m *mediator.Mediator) error {
	_, err := planRoutes(m, "")
	return err
}

// planRoutes computes the routes of every registered request under prefix
// and validates them against a throwaway ServeMux.
func planRoutes(m *mediator.Mediator, prefix string) ([]*route, error) {
	var errs []error
	var routes []*route
	mux := http.NewServeMux()
	if strings.ContainsAny(prefix, "{} \t\r\n") || (prefix != "" && !strings.HasPrefix(prefix, "/")) {
		return nil, fmt.Errorf("httpapi: prefix %q must start with / and contain no whitespace or braces", prefix)
	}
	for _, p := range reservedPaths {
		if err := tryHandle(mux, http.MethodGet+" "+prefix+p); err != nil {
			errs = append(errs, fmt.Errorf("httpapi: prefix %q: %w", prefix, err))
		}
	}
	for _, info := range m.Requests() {
		rt, err := routeFor(info, prefix)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := tryHandle(mux, rt.pattern); err != nil {
			errs = append(errs, fmt.Errorf("httpapi: %s: %w", info.Name, err))
			continue
		}
		routes = append(routes, rt)
	}
	return routes, errors.Join(errs...)
}

// tryHandle registers pattern on mux and converts the panic ServeMux raises
// for an invalid or conflicting pattern into an error.
func tryHandle(mux *http.ServeMux, pattern string) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = fmt.Errorf("pattern %q: %v", pattern, v)
		}
	}()
	mux.Handle(pattern, http.NotFoundHandler())
	return nil
}

// routeFor computes and validates the route of one request.
func routeFor(info *mediator.RequestInfo, prefix string) (*route, error) {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("httpapi: %s: "+format, append([]any{info.Name}, args...)...))
	}
	r := RouteOf(info)
	explicit := info.Implements(routerT)
	if !slices.Contains(validMethods, r.Method) {
		fail("method %q is not one of %s", r.Method, strings.Join(validMethods, ", "))
	}
	switch {
	case !strings.HasPrefix(r.Path, "/"):
		fail("path %q must start with /", r.Path)
	case strings.IndexFunc(r.Path, unicode.IsSpace) >= 0:
		fail("path %q contains whitespace", r.Path)
	case slices.Contains(reservedPaths, r.Path):
		fail("path %q is reserved", r.Path)
	}
	if r.Status != 0 && (r.Status < 200 || r.Status > 299) {
		fail("status %d is not 2xx", r.Status)
	}
	bindings, err := BindingsOf(info.RequestType)
	if err != nil {
		errs = append(errs, err)
	}
	if !explicit && r.Method == http.MethodGet {
		// RPC GET query: body fields are read from the query string.
		for i := range bindings {
			if bindings[i].Source == SourceBody {
				bindings[i].Source = SourceQuery
			}
		}
	}
	params := pathParams(r.Path)
	var pathBound, bodyBound []string
	for _, b := range bindings {
		switch b.Source {
		case SourcePath:
			pathBound = append(pathBound, b.Name)
		case SourceBody:
			bodyBound = append(bodyBound, b.Field.Name)
		}
	}
	for _, p := range params {
		if !slices.Contains(pathBound, p) {
			fail("path parameter {%s} has no field tagged path:%q", p, p)
		}
	}
	for _, p := range pathBound {
		if !slices.Contains(params, p) {
			fail("a field is tagged path:%q but the pattern %q has no {%s}", p, r.Path, p)
		}
	}
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && len(bodyBound) > 0 {
		fail("%s route has body-bound fields %v; tag them with path, query, or header", r.Method, bodyBound)
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	path := prefix + r.Path
	if strings.HasSuffix(path, "/") {
		path += "{$}" // exact match rather than a subtree
	}
	rt := &route{
		info:       info,
		method:     r.Method,
		path:       path,
		pattern:    r.Method + " " + path,
		status:     r.Status,
		bindings:   bindings,
		boundNames: map[string]Source{},
	}
	if rt.status == 0 {
		rt.status = http.StatusOK
		if info.ResponseType == voidT {
			rt.status = http.StatusNoContent
		}
	}
	for _, b := range bindings {
		if b.Source != SourceBody {
			rt.params = append(rt.params, b)
			rt.boundNames[b.JSONName] = b.Source
		}
	}
	return rt, nil
}

// pathParams lists the wildcard names of a ServeMux path pattern: {name} and
// {name...}; {$} is not a parameter.
func pathParams(path string) []string {
	var out []string
	for _, seg := range strings.Split(path, "/") {
		if len(seg) < 3 || seg[0] != '{' || seg[len(seg)-1] != '}' {
			continue
		}
		name := strings.TrimSuffix(seg[1:len(seg)-1], "...")
		if name == "$" {
			continue
		}
		out = append(out, name)
	}
	return out
}
