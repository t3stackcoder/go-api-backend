package validate

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Line is the Appendix B example.
type Line struct {
	SKU   string  `json:"sku"   validate:"required,pattern=^[A-Z0-9-]{3,32}$"`
	Qty   int     `json:"qty"   validate:"required,min=1,max=1000"`
	Price float64 `json:"price" validate:"gte=0"`
	Note  *string `json:"note"  validate:"max=200"`
}

const appendixB = `{
  "type": "object",
  "additionalProperties": false,
  "required": ["sku", "qty"],
  "properties": {
    "sku":   {"type": "string", "pattern": "^[A-Z0-9-]{3,32}$", "minLength": 1},
    "qty":   {"type": "integer", "minimum": 1, "maximum": 1000},
    "price": {"type": "number", "minimum": 0},
    "note":  {"type": ["string", "null"], "maxLength": 200}
  }
}`

// jsonEqual compares two JSON documents structurally.
func jsonEqual(t *testing.T, got []byte, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("got is not JSON: %v\n%s", err, got)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("want is not JSON: %v", err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Fatalf("schema mismatch\ngot:  %s\nwant: %s", got, want)
	}
}

func mustSchema(t *testing.T, v *Validator, typ reflect.Type, reg *Schemas, opts SchemaOptions) []byte {
	t.Helper()
	s, err := v.SchemaFor(typ, reg, opts)
	if err != nil {
		t.Fatalf("SchemaFor(%s): %v", typ, err)
	}
	b, err := json.Marshal(s, json.Deterministic(true))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func TestAppendixB(t *testing.T) {
	v := New()
	reg := &Schemas{}
	b := mustSchema(t, v, reflect.TypeFor[Line](), reg, SchemaOptions{})
	jsonEqual(t, b, `{"$ref": "#/components/schemas/Line"}`)
	body, err := json.Marshal(reg.Defs["Line"], json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	jsonEqual(t, body, appendixB)
	if names := reg.Names(); len(names) != 1 || names[0] != "Line" {
		t.Fatalf("Names = %v", names)
	}
	if n, ok := reg.NameOf(reflect.TypeFor[Line]()); !ok || n != "Line" {
		t.Fatalf("NameOf = %q %v", n, ok)
	}
	// Property order follows field order.
	if want := `"properties":{"sku":`; !strings.Contains(string(body), want) {
		t.Fatalf("properties are not ordered: %s", body)
	}
	// Second call is cached and returns another $ref to the same entry.
	mustSchema(t, v, reflect.TypeFor[Line](), reg, SchemaOptions{})
	if len(reg.Defs) != 1 {
		t.Fatal("registered twice")
	}
}

type schemerValue struct{ A int }

func (schemerValue) JSONSchema() *Schema {
	return &Schema{Type: Type{"string"}, Enum: []any{"x"}, Extra: map[string]any{"x-custom": true}}
}

type schemerPtr struct{ A int }

func (*schemerPtr) JSONSchema() *Schema { return &Schema{Type: Type{"integer"}} }

type schemerNil struct{}

func (schemerNil) JSONSchema() *Schema { return nil }

type schemerString string

func (schemerString) JSONSchema() *Schema { return &Schema{Type: Type{"string"}, Format: "money"} }

type schemerRef struct{}

func (schemerRef) JSONSchema() *Schema { return &Schema{Ref: "#/components/schemas/External"} }

type mapping struct {
	B     bool                 `json:"b"`
	I     int8                 `json:"i" validate:"required"`
	U     uint64               `json:"u" validate:"gt=0,lt=10"`
	F     float32              `json:"f" validate:"min=1,gte=2,max=9,lte=8"`
	S     string               `json:"s" validate:"len=3"`
	T     time.Time            `json:"t"`
	PT    *time.Time           `json:"pt"`
	ID    uuid.UUID            `json:"id" validate:"required"`
	Raw   []byte               `json:"raw"`
	RawR  []byte               `json:"raw_r" validate:"required"`
	Arr   [4]byte              `json:"arr"`
	JSON  jsontext.Value       `json:"json"`
	PJSON *jsontext.Value      `json:"pjson"`
	Any   any                  `json:"any"`
	M     map[string]int       `json:"m" validate:"min=1,max=5,dive,gte=0"`
	MR    map[string]string    `json:"mr" validate:"required"`
	L     []string             `json:"l" validate:"max=3,unique"`
	LR    []int                `json:"lr" validate:"required,len=2"`
	A     [2]int               `json:"a" validate:"dive,min=1"`
	C     color                `json:"c" validate:"oneof=red green"`
	PC    *color               `json:"pc" validate:"oneof=red green"`
	IO    *int                 `json:"io" validate:"oneof=1 2"`
	IP    net.IP               `json:"ip"`
	MJ    marshalerStruct      `json:"mj"`
	MT    marshalerTo          `json:"mt"`
	SV    schemerValue         `json:"sv"`
	SP    *schemerPtr          `json:"sp"`
	SS    schemerString        `json:"ss"`
	PSS   *schemerString       `json:"pss"`
	SR    *schemerRef          `json:"sr"`
	Anon  struct{ X int }      `json:"anon"`
	PAnon *struct{ X int }     `json:"panon"`
	PL    *Line                `json:"pl"`
	PLR   *Line                `json:"plr" validate:"required"`
	Deep  map[string][]*[]Line `json:"deep"`
	Doc   string               `json:"doc" doc:"A documented field"`
	DocL  Line                 `json:"docl" doc:"A documented ref"`
	Skip  string               `json:"skip"`
	MInt  map[int]string       `json:"mint"`
	embBase
	mediator.Command[mediator.Void]
}

func TestTypeMapping(t *testing.T) {
	v := New()
	reg := &Schemas{}
	mustSchema(t, v, reflect.TypeFor[mapping](), reg, SchemaOptions{
		Descriptions: true,
		SkipField:    func(sf reflect.StructField) bool { return sf.Name == "Skip" },
	})
	body, err := json.Marshal(reg.Defs["mapping"], json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	jsonEqual(t, body, `{
	  "type": "object",
	  "additionalProperties": false,
	  "required": ["i", "id", "raw_r", "mr", "lr", "plr"],
	  "properties": {
	    "b": {"type": "boolean"},
	    "i": {"type": "integer"},
	    "u": {"type": "integer", "exclusiveMinimum": 0, "exclusiveMaximum": 10},
	    "f": {"type": "number", "minimum": 2, "maximum": 8},
	    "s": {"type": "string", "minLength": 3, "maxLength": 3},
	    "t": {"type": "string", "format": "date-time"},
	    "pt": {"type": ["string", "null"], "format": "date-time"},
	    "id": {"type": "string", "format": "uuid"},
	    "raw": {"type": ["string", "null"], "contentEncoding": "base64"},
	    "raw_r": {"type": "string", "contentEncoding": "base64"},
	    "arr": {"type": "string", "contentEncoding": "base64"},
	    "json": {},
	    "pjson": {},
	    "any": {},
	    "m": {"type": ["object", "null"], "additionalProperties": {"type": "integer", "minimum": 0}, "minProperties": 1, "maxProperties": 5},
	    "mr": {"type": "object", "additionalProperties": {"type": "string"}},
	    "l": {"type": ["array", "null"], "items": {"type": "string"}, "maxItems": 3, "uniqueItems": true},
	    "lr": {"type": "array", "items": {"type": "integer"}, "minItems": 2, "maxItems": 2},
	    "a": {"type": "array", "items": {"type": "integer", "minimum": 1}},
	    "c": {"type": "string", "enum": ["red", "green"]},
	    "pc": {"type": ["string", "null"], "enum": ["red", "green", null]},
	    "io": {"type": ["integer", "null"], "enum": [1, 2, null]},
	    "ip": {"type": "string"},
	    "mj": {},
	    "mt": {},
	    "sv": {"$ref": "#/components/schemas/schemerValue"},
	    "sp": {"anyOf": [{"$ref": "#/components/schemas/schemerPtr"}, {"type": "null"}]},
	    "ss": {"type": "string", "format": "money"},
	    "pss": {"type": ["string", "null"], "format": "money"},
	    "sr": {"anyOf": [{"$ref": "#/components/schemas/schemerRef"}, {"type": "null"}]},
	    "anon": {"type": "object", "additionalProperties": false, "properties": {"X": {"type": "integer"}}},
	    "panon": {"type": ["object", "null"], "additionalProperties": false, "properties": {"X": {"type": "integer"}}},
	    "pl": {"anyOf": [{"$ref": "#/components/schemas/Line"}, {"type": "null"}]},
	    "plr": {"$ref": "#/components/schemas/Line"},
	    "deep": {"type": ["object", "null"], "additionalProperties": {"type": ["array", "null"], "items": {"type": ["array", "null"], "items": {"$ref": "#/components/schemas/Line"}}}},
	    "doc": {"type": "string", "description": "A documented field"},
	    "docl": {"$ref": "#/components/schemas/Line", "description": "A documented ref"},
	    "mint": {"type": ["object", "null"], "additionalProperties": {"type": "string"}}
	  }
	}`)
	if names := reg.Names(); !reflect.DeepEqual(names, []string{"Line", "mapping", "schemerPtr", "schemerRef", "schemerValue"}) {
		t.Fatalf("Names = %v", names)
	}
	if reg.Defs["schemerRef"].Ref != "#/components/schemas/External" {
		t.Fatalf("Schemer body was not registered: %+v", reg.Defs["schemerRef"])
	}
	sv, _ := json.Marshal(reg.Defs["schemerValue"], json.Deterministic(true))
	jsonEqual(t, sv, `{"type": "string", "enum": ["x"], "x-custom": true}`)
	// Without descriptions and with unknown fields allowed.
	reg2 := &Schemas{}
	mustSchema(t, v, reflect.TypeFor[mapping](), reg2, SchemaOptions{AllowUnknownFields: true})
	body, _ = json.Marshal(reg2.Defs["mapping"], json.Deterministic(true))
	if strings.Contains(string(body), "description") || strings.Contains(string(body), "additionalProperties\":false") {
		t.Fatalf("options ignored: %s", body)
	}
	if !strings.Contains(string(body), `"skip"`) {
		t.Fatalf("SkipField leaked into a run without it: %s", body)
	}
}

func TestSchemaForInlineAndRefPrefix(t *testing.T) {
	v := New()
	// Nil registry inlines named structs.
	b := mustSchema(t, v, reflect.TypeFor[Line](), nil, SchemaOptions{})
	jsonEqual(t, b, appendixB)
	// A custom prefix.
	reg := &Schemas{}
	b = mustSchema(t, v, reflect.TypeFor[Line](), reg, SchemaOptions{RefPrefix: "#/$defs/"})
	jsonEqual(t, b, `{"$ref": "#/$defs/Line"}`)
	// Non-struct roots.
	b = mustSchema(t, v, reflect.TypeFor[[]Line](), reg, SchemaOptions{})
	jsonEqual(t, b, `{"type": ["array", "null"], "items": {"$ref": "#/components/schemas/Line"}}`)
	b = mustSchema(t, v, reflect.TypeFor[*Line](), reg, SchemaOptions{})
	jsonEqual(t, b, `{"anyOf": [{"$ref": "#/components/schemas/Line"}, {"type": "null"}]}`)
	b = mustSchema(t, v, reflect.TypeFor[int](), nil, SchemaOptions{})
	jsonEqual(t, b, `{"type": "integer"}`)
	// Recursive types need a registry.
	if _, err := v.SchemaFor(reflect.TypeFor[node](), nil, SchemaOptions{}); err == nil || !strings.Contains(err.Error(), "recursive type validate.node needs a registry") {
		t.Fatalf("recursive inline: %v", err)
	}
	b = mustSchema(t, v, reflect.TypeFor[node](), reg, SchemaOptions{})
	jsonEqual(t, b, `{"$ref": "#/components/schemas/node"}`)
	body, _ := json.Marshal(reg.Defs["node"], json.Deterministic(true))
	jsonEqual(t, body, `{"type": "object", "additionalProperties": false, "required": ["name"], "properties": {
	  "name": {"type": "string", "minLength": 1},
	  "children": {"type": ["array", "null"], "items": {"anyOf": [{"$ref": "#/components/schemas/node"}, {"type": "null"}]}}}}`)
}

func TestSchemaForErrors(t *testing.T) {
	v := New()
	if _, err := v.SchemaFor(nil, nil, SchemaOptions{}); !errors.Is(err, ErrNilValue) {
		t.Fatalf("nil type: %v", err)
	}
	if _, err := v.SchemaFor(reflect.TypeFor[chan int](), nil, SchemaOptions{}); err == nil || !strings.Contains(err.Error(), "cannot be encoded as JSON") {
		t.Fatalf("chan: %v", err)
	}
	if _, err := v.SchemaFor(reflect.TypeFor[namedForErrors](), &Schemas{}, SchemaOptions{}); err == nil || !strings.Contains(err.Error(), `unknown rule "bogus"`) {
		t.Fatalf("compile error: %v", err)
	}
	if _, err := v.SchemaFor(reflect.TypeFor[schemerNil](), nil, SchemaOptions{}); err == nil || !strings.Contains(err.Error(), "JSONSchema returned nil") {
		t.Fatalf("nil schemer: %v", err)
	}
	type withChan struct {
		A int      `json:"a"`
		C chan int `json:"c"`
	}
	type withFunc struct {
		F func() `json:"f"`
	}
	reg := &Schemas{}
	if _, err := v.SchemaFor(reflect.TypeFor[withChan](), reg, SchemaOptions{}); err == nil {
		t.Fatal("field error must propagate")
	}
	if _, err := v.SchemaFor(reflect.TypeFor[withFunc](), nil, SchemaOptions{}); err == nil {
		t.Fatal("inline field error must propagate")
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[*chan int](), reflect.TypeFor[[]chan int](), reflect.TypeFor[map[string]chan int]()} {
		if _, err := v.SchemaFor(typ, nil, SchemaOptions{}); err == nil {
			t.Fatalf("%s: element error must propagate", typ)
		}
	}
}

// Registry naming: mediator.Void and the two local Void types collide.
type Void struct {
	A int `json:"a"`
}

type Box[T any] struct {
	V T `json:"v"`
}

func TestSchemasNaming(t *testing.T) {
	v := New()
	reg := &Schemas{}
	first := mustSchema(t, v, reflect.TypeFor[mediator.Void](), reg, SchemaOptions{})
	jsonEqual(t, first, `{"$ref": "#/components/schemas/Void"}`)
	stub, _ := v.SchemaFor(reflect.TypeFor[mediator.Void](), reg, SchemaOptions{})

	second := mustSchema(t, v, reflect.TypeFor[Void](), reg, SchemaOptions{})
	jsonEqual(t, second, `{"$ref": "#/components/schemas/validate.Void"}`)
	if stub.Ref != "#/components/schemas/mediator.Void" {
		t.Fatalf("earlier $ref was not renamed: %s", stub.Ref)
	}
	if names := reg.Names(); !reflect.DeepEqual(names, []string{"mediator.Void", "validate.Void"}) {
		t.Fatalf("Names = %v", names)
	}
	if reg.Defs["mediator.Void"] == nil || len(reg.Defs["validate.Void"].Properties) != 1 {
		t.Fatal("definitions did not move with the rename")
	}

	// A third Void (function local) is qualified too and de-duplicated.
	type Void struct {
		B int `json:"b"`
	}
	third := mustSchema(t, v, reflect.TypeFor[Void](), reg, SchemaOptions{})
	jsonEqual(t, third, `{"$ref": "#/components/schemas/validate.Void_2"}`)

	// A foreign entry in Defs is respected.
	reg.Defs["Line"] = &Schema{Description: "foreign"}
	fourth := mustSchema(t, v, reflect.TypeFor[Line](), reg, SchemaOptions{})
	jsonEqual(t, fourth, `{"$ref": "#/components/schemas/validate.Line"}`)
	if reg.Defs["Line"].Description != "foreign" {
		t.Fatal("foreign entry was overwritten")
	}

	// Generic names are sanitized and their type arguments lose the package.
	b := mustSchema(t, v, reflect.TypeFor[Box[time.Time]](), reg, SchemaOptions{})
	jsonEqual(t, b, `{"$ref": "#/components/schemas/Box_Time_"}`)

	// A registry with a user-provided Defs map works, and NameOf misses.
	reg2 := &Schemas{Defs: map[string]*Schema{}}
	mustSchema(t, v, reflect.TypeFor[Line](), reg2, SchemaOptions{})
	if _, ok := reg2.NameOf(reflect.TypeFor[Void]()); ok {
		t.Fatal("NameOf must miss")
	}
	if len((&Schemas{}).Names()) != 0 {
		t.Fatal("empty registry")
	}
}

// TestStripTypeArgPackages covers the type argument shapes reflect prints.
func TestStripTypeArgPackages(t *testing.T) {
	cases := map[string]string{
		"Plain": "Plain",
		"Box[github.com/t3stackcoder/go-api-backend/examples/orders/orders.OrderSummary]": "Box[OrderSummary]",
		"Box[time.Time]":          "Box[Time]",
		"Box[string]":             "Box[string]",
		"Map[string,a/b.X]":       "Map[string,X]",
		"Map[a/b.K, c/d.V]":       "Map[K, V]",
		"Box[a/b.Wrapper[a/b.X]]": "Box[Wrapper[X]]",
		"Box[*a/b.X]":             "Box[*X]",
		"Box[[]a/b.X]":            "Box[[]X]",
		"Box[map[string]*a/b.X]":  "Box[map[string]*X]",
		"Map[a/b.X,[]a/b.Wrapper[*gopkg.in/yaml.v3.Node]]": "Map[X,[]Wrapper[*Node]]",
		"Box[struct { A int }]":                            "Box[struct { A int }]",
	}
	for in, want := range cases {
		if got := stripTypeArgPackages(in); got != want {
			t.Errorf("stripTypeArgPackages(%q) = %q, want %q", in, got, want)
		}
	}
	// Through reflect: the generic Box over types of this package and of
	// package mediator.
	if got := shortName(reflect.TypeFor[Box[Line]]()); got != "Box_Line_" {
		t.Errorf("shortName = %q", got)
	}
	if got := shortName(reflect.TypeFor[Box[*mediator.Void]]()); got != "Box__Void_" {
		t.Errorf("shortName = %q", got)
	}
	if got := shortName(reflect.TypeFor[Box[[]Box[Line]]]()); got != "Box___Box_Line__" {
		t.Errorf("shortName = %q", got)
	}
	if got := qualifiedName(reflect.TypeFor[Box[Line]]()); got != "validate.Box_Line_" {
		t.Errorf("qualifiedName = %q", got)
	}
}

// TestSchemasNamingGenericCollision: two instantiations whose arguments
// share a short name collide and are told apart by the qualified name and
// the counter, and every $ref handed out follows the rename.
func TestSchemasNamingGenericCollision(t *testing.T) {
	v := New()
	reg := &Schemas{}
	first := mustSchema(t, v, reflect.TypeFor[Box[mediator.Void]](), reg, SchemaOptions{})
	jsonEqual(t, first, `{"$ref": "#/components/schemas/Box_Void_"}`)
	stub, _ := v.SchemaFor(reflect.TypeFor[Box[mediator.Void]](), reg, SchemaOptions{})
	second := mustSchema(t, v, reflect.TypeFor[Box[Void]](), reg, SchemaOptions{})
	jsonEqual(t, second, `{"$ref": "#/components/schemas/validate.Box_Void__2"}`)
	if stub.Ref != "#/components/schemas/validate.Box_Void_" {
		t.Fatalf("earlier $ref was not renamed: %s", stub.Ref)
	}
	// The two Void argument types collide too and are qualified in turn.
	want := []string{"mediator.Void", "validate.Box_Void_", "validate.Box_Void__2", "validate.Void"}
	if names := reg.Names(); !reflect.DeepEqual(names, want) {
		t.Fatalf("Names = %v", names)
	}
}

func TestSchemaMarshalRoundTrip(t *testing.T) {
	s := &Schema{
		Schema: "https://json-schema.org/draft/2020-12/schema",
		Type:   Type{"object"},
		Properties: Properties{
			{Name: "z", Schema: &Schema{Type: Type{"string", "null"}, MinLength: ptr(1)}},
			{Name: "a", Schema: &Schema{Ref: "#/x"}},
		},
		Required:             []string{"z"},
		AdditionalProperties: &AdditionalProperties{Schema: &Schema{Type: Type{"integer"}}},
		Items:                &Schema{AdditionalProperties: &AdditionalProperties{Allow: true}},
		Enum:                 []any{"a", nil},
		Deprecated:           true,
		Defs:                 map[string]*Schema{"x": {Type: Type{"boolean"}}},
		Extra:                map[string]any{"x-sse-item": "y"},
	}
	b, err := json.Marshal(s, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"z":{"type":["string","null"],"minLength":1},"a":{"$ref":"#/x"}},"required":["z"],"additionalProperties":{"type":"integer"},"items":{"additionalProperties":true},"enum":["a",null],"deprecated":true,"$defs":{"x":{"type":"boolean"}},"x-sse-item":"y"}`
	if string(b) != want {
		t.Fatalf("marshal\ngot:  %s\nwant: %s", b, want)
	}
	var back Schema
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&back, s) {
		t.Fatalf("round trip\ngot:  %+v\nwant: %+v", back, *s)
	}
	if back.Properties.Get("a").Ref != "#/x" || back.Properties.Get("missing") != nil {
		t.Fatal("Properties.Get")
	}
	back.Properties.Set("a", &Schema{Type: Type{"null"}})
	back.Properties.Set("new", &Schema{})
	if len(back.Properties) != 3 || back.Properties.Get("a").Type[0] != "null" {
		t.Fatal("Properties.Set")
	}
	if b, _ := json.Marshal(Type(nil)); string(b) != "[]" {
		t.Fatalf("empty Type: %s", b)
	}
}

func TestSchemaUnmarshalErrors(t *testing.T) {
	bad := []string{
		`{"type": 1}`,
		`{"properties": []}`,
		`{"properties": {"a": 1}}`,
		`{"properties": {"a": {"type": 1}}}`,
		`{"additionalProperties": 1}`,
		`{"additionalProperties": {"type": 1}}`,
		`{"type": ["a", 1]}`,
		`{"additionalProperties": "x"}`,
	}
	for _, in := range bad {
		var s Schema
		if err := json.Unmarshal([]byte(in), &s); err == nil {
			t.Errorf("%s: expected error", in)
		}
	}
}

func TestNullable(t *testing.T) {
	already := &Schema{Type: Type{"string", "null"}}
	if nullable(already) != already {
		t.Fatal("already nullable must be returned as-is")
	}
	empty := &Schema{}
	if nullable(empty) != empty {
		t.Fatal("empty schema accepts null already")
	}
	composite := &Schema{AnyOf: []*Schema{{Type: Type{"string"}}}}
	if got := nullable(composite); len(got.AnyOf) != 2 || got.AnyOf[0] != composite {
		t.Fatal("composite must be wrapped")
	}
	custom := &Schema{Pattern: "^x$"}
	if got := nullable(custom); len(got.AnyOf) != 2 {
		t.Fatal("untyped constrained schema must be wrapped")
	}
}
