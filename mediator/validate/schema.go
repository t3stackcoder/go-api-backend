package validate

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"reflect"
)

// Schema is the JSON Schema 2020-12 subset the generator emits and the
// OpenAPI document embeds. It marshals with encoding/json/v2: properties keep
// their order, Extra members are merged into the object, and the $defs and
// Extra maps follow the encoder's map order (pass json.Deterministic(true)
// for byte-stable output).
type Schema struct {
	// Schema is the $schema dialect URI, set only on standalone documents.
	Schema string `json:"$schema,omitzero"`
	// Ref references a registered schema, for example
	// "#/components/schemas/Line".
	Ref         string     `json:"$ref,omitzero"`
	Type        Type       `json:"type,omitzero"`
	Format      string     `json:"format,omitzero"`
	Description string     `json:"description,omitzero"`
	Properties  Properties `json:"properties,omitzero"`
	Required    []string   `json:"required,omitzero"`
	// AdditionalProperties is false for closed objects, a schema for maps,
	// and nil when unknown members are allowed.
	AdditionalProperties *AdditionalProperties `json:"additionalProperties,omitzero"`
	Items                *Schema               `json:"items,omitzero"`
	// AnyOf expresses a nullable reference: [{$ref}, {type: null}].
	AnyOf            []*Schema `json:"anyOf,omitzero"`
	Enum             []any     `json:"enum,omitzero"`
	Minimum          *float64  `json:"minimum,omitzero"`
	Maximum          *float64  `json:"maximum,omitzero"`
	ExclusiveMinimum *float64  `json:"exclusiveMinimum,omitzero"`
	ExclusiveMaximum *float64  `json:"exclusiveMaximum,omitzero"`
	MinLength        *int      `json:"minLength,omitzero"`
	MaxLength        *int      `json:"maxLength,omitzero"`
	Pattern          string    `json:"pattern,omitzero"`
	MinItems         *int      `json:"minItems,omitzero"`
	MaxItems         *int      `json:"maxItems,omitzero"`
	UniqueItems      bool      `json:"uniqueItems,omitzero"`
	MinProperties    *int      `json:"minProperties,omitzero"`
	MaxProperties    *int      `json:"maxProperties,omitzero"`
	ContentEncoding  string    `json:"contentEncoding,omitzero"`
	Deprecated       bool      `json:"deprecated,omitzero"`
	// Defs holds the definitions of a standalone document under $defs.
	Defs map[string]*Schema `json:"$defs,omitzero"`
	// Extra carries vendor extensions such as "x-sse-item"; its members are
	// merged into the schema object.
	Extra map[string]any `json:",embed"`
}

// Type is the JSON Schema "type" keyword: one type name, or several for a
// nullable schema such as ["string", "null"]. It marshals as a string when it
// has one element and as an array otherwise.
type Type []string

// MarshalJSONTo implements json.MarshalerTo.
func (t Type) MarshalJSONTo(enc *jsontext.Encoder) error {
	if len(t) == 1 {
		return enc.WriteToken(jsontext.String(t[0]))
	}
	return json.MarshalEncode(enc, []string(t))
}

// UnmarshalJSONFrom implements json.UnmarshalerFrom.
func (t *Type) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	switch dec.PeekKind() {
	case '"':
		var s string
		if err := json.UnmarshalDecode(dec, &s); err != nil {
			return err
		}
		*t = Type{s}
		return nil
	case '[':
		var ss []string
		if err := json.UnmarshalDecode(dec, &ss); err != nil {
			return err
		}
		*t = Type(ss)
		return nil
	}
	return fmt.Errorf("validate: schema type must be a string or an array of strings")
}

// Property is one named member schema of an object.
type Property struct {
	Name   string
	Schema *Schema
}

// Properties is an ordered list of object members. Order is the Go field
// order, which makes generated documents deterministic.
type Properties []Property

// Get returns the schema of the named property or nil.
func (p Properties) Get(name string) *Schema {
	for _, prop := range p {
		if prop.Name == name {
			return prop.Schema
		}
	}
	return nil
}

// Set replaces the named property or appends it.
func (p *Properties) Set(name string, s *Schema) {
	for i := range *p {
		if (*p)[i].Name == name {
			(*p)[i].Schema = s
			return
		}
	}
	*p = append(*p, Property{Name: name, Schema: s})
}

// MarshalJSONTo implements json.MarshalerTo, writing a JSON object in order.
func (p Properties) MarshalJSONTo(enc *jsontext.Encoder) error {
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		return err
	}
	for _, prop := range p {
		if err := enc.WriteToken(jsontext.String(prop.Name)); err != nil {
			return err
		}
		if err := json.MarshalEncode(enc, prop.Schema); err != nil {
			return err
		}
	}
	return enc.WriteToken(jsontext.EndObject)
}

// UnmarshalJSONFrom implements json.UnmarshalerFrom, keeping member order.
func (p *Properties) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	if dec.PeekKind() != '{' {
		return fmt.Errorf("validate: properties must be an object")
	}
	if _, err := dec.ReadToken(); err != nil {
		return err
	}
	var out Properties
	for dec.PeekKind() != '}' {
		tok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		name := tok.String() // read before the next decode voids the token
		s := new(Schema)
		if err := json.UnmarshalDecode(dec, s); err != nil {
			return err
		}
		out = append(out, Property{Name: name, Schema: s})
	}
	*p = out
	_, err := dec.ReadToken() // the closing brace the loop peeked
	return err
}

// AdditionalProperties is the additionalProperties keyword: a schema when
// Schema is set, otherwise the boolean Allow.
type AdditionalProperties struct {
	Schema *Schema
	Allow  bool
}

// MarshalJSONTo implements json.MarshalerTo.
func (a AdditionalProperties) MarshalJSONTo(enc *jsontext.Encoder) error {
	if a.Schema != nil {
		return json.MarshalEncode(enc, a.Schema)
	}
	return enc.WriteToken(jsontext.Bool(a.Allow))
}

// UnmarshalJSONFrom implements json.UnmarshalerFrom.
func (a *AdditionalProperties) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	switch dec.PeekKind() {
	case 't', 'f':
		var b bool
		if err := json.UnmarshalDecode(dec, &b); err != nil {
			return err
		}
		*a = AdditionalProperties{Allow: b}
		return nil
	case '{':
		s := new(Schema)
		if err := json.UnmarshalDecode(dec, s); err != nil {
			return err
		}
		*a = AdditionalProperties{Schema: s}
		return nil
	}
	return fmt.Errorf("validate: additionalProperties must be a boolean or a schema")
}

// Schemer is implemented by types that supply their own JSON Schema instead
// of the reflected one. The method is called on a zero value.
type Schemer interface{ JSONSchema() *Schema }

var schemerType = reflect.TypeFor[Schemer]()

// isEmptySchema reports whether s imposes no constraint at all.
func isEmptySchema(s *Schema) bool { return reflect.DeepEqual(s, &Schema{}) }
