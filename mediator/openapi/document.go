package openapi

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"slices"

	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

// Document versions.
const (
	// Version is the OpenAPI version the document declares.
	Version = "3.1.0"
	// Dialect is the JSON Schema dialect of every schema in the document.
	Dialect = "https://json-schema.org/draft/2020-12/schema"
	// SSEItemKey is the media type member carrying the item schema of a
	// text/event-stream response. OpenAPI 3.2 names it itemSchema.
	SSEItemKey = "x-sse-item"
)

// Document is the OpenAPI 3.1 document. Every object it contains is a Go
// struct whose members marshal in a fixed order, and every map is a Map,
// which marshals its keys sorted, so MarshalJSON is byte-stable across
// runs and platforms. Modify it freely before serving; Validate checks the
// result.
type Document struct {
	OpenAPI           string         `json:"openapi"`
	JSONSchemaDialect string         `json:"jsonSchemaDialect,omitzero"`
	Info              Info           `json:"info"`
	Servers           []Server       `json:"servers,omitzero"`
	Paths             Map[*PathItem] `json:"paths"`
	Components        *Components    `json:"components,omitzero"`
	Tags              []Tag          `json:"tags,omitzero"`
}

// Map is a string-keyed map that marshals its members in sorted key order,
// which keeps the document deterministic. It unmarshals like an ordinary
// map.
type Map[V any] map[string]V

// MarshalJSONTo implements json.MarshalerTo, writing the members sorted by
// key.
func (m Map[V]) MarshalJSONTo(enc *jsontext.Encoder) error {
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		return err
	}
	for _, k := range slices.Sorted(maps.Keys(m)) {
		if err := enc.WriteToken(jsontext.String(k)); err != nil {
			return err
		}
		if err := json.MarshalEncode(enc, m[k]); err != nil {
			return err
		}
	}
	return enc.WriteToken(jsontext.EndObject)
}

// PathItem is the OpenAPI Path Item Object: one operation per method.
type PathItem struct {
	Get    *OperationObject `json:"get,omitzero"`
	Put    *OperationObject `json:"put,omitzero"`
	Post   *OperationObject `json:"post,omitzero"`
	Delete *OperationObject `json:"delete,omitzero"`
	Head   *OperationObject `json:"head,omitzero"`
	Patch  *OperationObject `json:"patch,omitzero"`
}

// Operation returns the operation of an HTTP method (upper case) or nil.
func (p *PathItem) Operation(method string) *OperationObject {
	if p == nil {
		return nil
	}
	return *p.slot(method)
}

// slot returns the field holding the operation of method. Unknown methods
// cannot occur: httpapi rejects them at Build.
func (p *PathItem) slot(method string) **OperationObject {
	switch method {
	case http.MethodPut:
		return &p.Put
	case http.MethodPost:
		return &p.Post
	case http.MethodDelete:
		return &p.Delete
	case http.MethodHead:
		return &p.Head
	case http.MethodPatch:
		return &p.Patch
	default:
		return &p.Get
	}
}

// OperationObject is the OpenAPI Operation Object of one request. It is
// named after the object rather than Operation, which is the Describe()
// trait value.
type OperationObject struct {
	Tags        []string              `json:"tags,omitzero"`
	Summary     string                `json:"summary,omitzero"`
	Description string                `json:"description,omitzero"`
	OperationID string                `json:"operationId"`
	Deprecated  bool                  `json:"deprecated,omitzero"`
	Parameters  []*Parameter          `json:"parameters,omitzero"`
	RequestBody *RequestBody          `json:"requestBody,omitzero"`
	Responses   Map[*Response]        `json:"responses"`
	Security    []SecurityRequirement `json:"security,omitzero"`
}

// Parameter is the OpenAPI Parameter Object of a path, query, or header
// binding.
type Parameter struct {
	Name        string           `json:"name"`
	In          string           `json:"in"`
	Description string           `json:"description,omitzero"`
	Required    bool             `json:"required,omitzero"`
	Deprecated  bool             `json:"deprecated,omitzero"`
	Schema      *validate.Schema `json:"schema"`
}

// RequestBody is the OpenAPI Request Body Object.
type RequestBody struct {
	Description string          `json:"description,omitzero"`
	Content     Map[*MediaType] `json:"content"`
	Required    bool            `json:"required,omitzero"`
}

// MediaType is the OpenAPI Media Type Object. SSEItem is set on
// text/event-stream responses only and marshals as x-sse-item.
type MediaType struct {
	Schema  *validate.Schema `json:"schema,omitzero"`
	SSEItem *validate.Schema `json:"x-sse-item,omitzero"`
}

// Response is the OpenAPI Response Object.
type Response struct {
	Description string          `json:"description"`
	Headers     Map[*Header]    `json:"headers,omitzero"`
	Content     Map[*MediaType] `json:"content,omitzero"`
}

// Header is the OpenAPI Header Object of a response header.
type Header struct {
	Description string           `json:"description,omitzero"`
	Required    bool             `json:"required,omitzero"`
	Schema      *validate.Schema `json:"schema,omitzero"`
}

// SecurityRequirement is the OpenAPI Security Requirement Object: scheme
// name to scopes.
type SecurityRequirement = Map[[]string]

// Components is the OpenAPI Components Object.
type Components struct {
	Schemas         Map[*validate.Schema] `json:"schemas,omitzero"`
	SecuritySchemes Map[map[string]any]   `json:"securitySchemes,omitzero"`
}

// Tag is the OpenAPI Tag Object.
type Tag struct {
	Name        string `json:"name"`
	Description string `json:"description,omitzero"`
}

// MarshalJSON returns the document pretty-printed with two-space indent,
// deterministic member order, and a trailing newline. It is the encoding
// the drift check compares, so it never varies between runs or platforms.
func (d *Document) MarshalJSON() ([]byte, error) {
	type plain Document // no MarshalJSON, so no recursion
	b, err := json.Marshal((*plain)(d), json.Deterministic(true), jsontext.WithIndent("  "))
	if err != nil {
		return nil, fmt.Errorf("openapi: marshal document: %w", err)
	}
	return append(b, '\n'), nil
}

// Write writes MarshalJSON to w.
func (d *Document) Write(w io.Writer) error {
	b, err := d.MarshalJSON()
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// WriteFile writes MarshalJSON to path, replacing any existing file. The
// file is world-readable: the document describes the public API and is
// committed to the repository.
func (d *Document) WriteFile(path string) error {
	b, err := d.MarshalJSON()
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644) //nolint:gosec // public document, 0644 is intended
}
