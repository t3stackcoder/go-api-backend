package openapi

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// oasSchemaJSON is the OpenAPI 3.1 schema published at OASSchemaURL
// (2022-10-07 edition). It is self-contained apart from the JSON Schema
// 2020-12 meta-schema, which the jsonschema package bundles.
//
//go:embed schema/openapi-3.1.json
var oasSchemaJSON []byte

// OASSchemaURL is the canonical URL of the embedded OpenAPI 3.1 schema.
const OASSchemaURL = "https://spec.openapis.org/oas/3.1/schema/2022-10-07"

// oasSchema compiles the embedded schema once. It is a variable so tests
// can exercise the failure path.
var oasSchema = sync.OnceValues(func() (*jsonschema.Schema, error) { return compileOAS(oasSchemaJSON) })

// compileOAS compiles raw as the OpenAPI schema at OASSchemaURL with format
// assertions on. Nothing is fetched: the schema comes from memory and the
// JSON Schema 2020-12 meta-schema it declares is bundled by the library.
func compileOAS(raw []byte) (*jsonschema.Schema, error) {
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	c.UseLoader(memoryLoader{OASSchemaURL: raw})
	s, err := c.Compile(OASSchemaURL)
	if err != nil {
		return nil, fmt.Errorf("openapi: embedded OpenAPI schema: %w", err)
	}
	return s, nil
}

// memoryLoader serves schema documents from memory by URL and refuses every
// other URL, so compilation never touches the network.
type memoryLoader map[string][]byte

// Load implements jsonschema.URLLoader.
func (l memoryLoader) Load(url string) (any, error) {
	raw, ok := l[url]
	if !ok {
		return nil, fmt.Errorf("openapi: schema %s is not embedded", url)
	}
	return jsonschema.UnmarshalJSON(bytes.NewReader(raw))
}

// Validate checks that doc marshals to a valid OpenAPI 3.1 document: it is
// validated against the embedded OpenAPI schema (with format assertions),
// and every local $ref must resolve within the document, which the schema
// alone cannot check. Generate calls it before returning.
func Validate(doc *Document) error {
	if doc == nil {
		return errors.New("openapi: nil document")
	}
	b, err := doc.MarshalJSON()
	if err != nil {
		return err
	}
	return ValidateJSON(b)
}

// ValidateJSON is Validate for an already serialized document, for example
// the committed api/openapi.json.
func ValidateJSON(b []byte) error {
	schema, err := oasSchema()
	if err != nil {
		return err
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("openapi: document is not JSON: %w", err)
	}
	if err := schema.Validate(inst); err != nil {
		return fmt.Errorf("openapi: document is not valid OpenAPI 3.1: %w", err)
	}
	return checkRefs(inst)
}

// checkRefs walks the document and reports every local $ref that does not
// resolve, all at once.
func checkRefs(root any) error {
	var errs []error
	var walk func(v any, at string)
	walk = func(v any, at string) {
		switch x := v.(type) {
		case map[string]any:
			if ref, ok := x["$ref"].(string); ok && strings.HasPrefix(ref, "#/") && !resolves(root, ref[2:]) {
				errs = append(errs, fmt.Errorf("openapi: %s: $ref %q does not resolve", at, ref))
			}
			for _, k := range slices.Sorted(maps.Keys(x)) {
				walk(x[k], at+"/"+k)
			}
		case []any:
			for i, e := range x {
				walk(e, at+"/"+strconv.Itoa(i))
			}
		}
	}
	walk(root, "")
	return errors.Join(errs...)
}

// resolves reports whether the JSON Pointer (without the leading "#/")
// addresses a value in root.
func resolves(root any, pointer string) bool {
	cur := root
	for _, tok := range strings.Split(pointer, "/") {
		tok = strings.ReplaceAll(strings.ReplaceAll(tok, "~1", "/"), "~0", "~")
		switch x := cur.(type) {
		case map[string]any:
			v, ok := x[tok]
			if !ok {
				return false
			}
			cur = v
		case []any:
			i, err := strconv.Atoi(tok)
			if err != nil || i < 0 || i >= len(x) {
				return false
			}
			cur = x[i]
		default:
			return false
		}
	}
	return true
}
