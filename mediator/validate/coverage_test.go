package validate

// Targeted tests for branches the table tests do not reach.

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"reflect"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type ptrLine struct {
	N int `json:"n"`
}

func (l *ptrLine) Validate(context.Context) error {
	if l.N < 0 {
		return (&mediator.ValidationError{}).Add("/n", "neg", "must not be negative")
	}
	return nil
}

// Map values are not addressable, so a pointer-receiver Validate needs a copy.
func TestValidateOnMapValuesCopies(t *testing.T) {
	type holder struct {
		M map[string]ptrLine `json:"m"`
	}
	v := New()
	got := fieldsOf(t, v.Check(bg, holder{M: map[string]ptrLine{"k": {N: -1}}}))
	if len(got) != 1 || got[0].Path != "/m/k/n" || got[0].Rule != "neg" {
		t.Fatalf("got %+v", got)
	}
	if err := v.Check(bg, holder{M: map[string]ptrLine{"k": {N: 1}}}); err != nil {
		t.Fatal(err)
	}
}

// EmbWithSub is exported so that embedding it by pointer is honored.
type EmbWithSub struct {
	Sub vLine `json:"sub"`
}

type embNilHost struct {
	*EmbWithSub
	Name string `json:"name"`
}

func TestNilEmbeddedPointerSkipsValidate(t *testing.T) {
	v := New()
	if err := v.Check(bg, embNilHost{}); err != nil {
		t.Fatalf("nil embedded pointer: %v", err)
	}
	got := fieldsOf(t, v.Check(bg, embNilHost{EmbWithSub: &EmbWithSub{Sub: vLine{Qty: 13}}}))
	if len(got) != 1 || got[0].Path != "/sub/qty" {
		t.Fatalf("got %+v", got)
	}
}

type diamondA struct{ embBase }
type diamondB struct{ embBase }
type diamond struct {
	diamondA
	diamondB
}

// SelfEmbed is exported so that its self-embedding pointer is walked.
type SelfEmbed struct {
	*SelfEmbed
	X string `json:"x" validate:"required"`
}

func TestDiamondAndSelfEmbedding(t *testing.T) {
	v := New()
	// The same type at the same depth through two paths is ambiguous, as in
	// encoding/json, so "id" is not a member at all.
	if err := v.Check(bg, diamond{}); err != nil {
		t.Fatalf("diamond fields must be annihilated: %v", err)
	}
	got := fieldsOf(t, v.Check(bg, SelfEmbed{}))
	if len(got) != 1 || got[0].Path != "/x" {
		t.Fatalf("self embedding: %+v", got)
	}
	if err := v.Check(bg, SelfEmbed{SelfEmbed: &SelfEmbed{}, X: "x"}); err != nil {
		t.Fatalf("self embedding with nested pointer: %v", err)
	}
}

// Leaf checkers implement the validate and constrain methods of the checker
// interface as no-ops that the plan never calls.
func TestLeafCheckerNoops(t *testing.T) {
	for _, c := range []checker{presenceChecker{}, &stringChecker{}, &numChecker[int64]{}} {
		if err := c.validate(bg, &checkCtx{}, reflect.Value{}); err != nil {
			t.Fatalf("%T.validate = %v", c, err)
		}
	}
	for _, c := range []checker{presenceChecker{}, &ptrChecker{}, &structChecker{}} {
		s := &Schema{}
		c.constrain(s)
		if !isEmptySchema(s) {
			t.Fatalf("%T.constrain changed the schema", c)
		}
	}
}

func TestLenWithMaxInSchema(t *testing.T) {
	type both struct {
		S string   `json:"s" validate:"len=3,max=5"`
		L []string `json:"l" validate:"len=2,max=4,min=1"`
	}
	v := New()
	b := mustSchema(t, v, reflect.TypeFor[both](), nil, SchemaOptions{})
	jsonEqual(t, b, `{"type": "object", "additionalProperties": false, "properties": {
	  "s": {"type": "string", "minLength": 3, "maxLength": 3},
	  "l": {"type": ["array", "null"], "items": {"type": "string"}, "minItems": 2, "maxItems": 2}}}`)
}

// TestZeroBoundsInSchema: a bound of 0 is a bound (unset is -1), so it
// reaches the schema; len=0 sets both ends. The compiler does not check that
// the rules of a field agree with each other, so max=0 beside len=3 is
// rendered as given: len raises the lower end and the smaller max keeps the
// upper end, exactly as Check would fail every value on one rule or the other.
func TestZeroBoundsInSchema(t *testing.T) {
	type zero struct {
		SMin string         `json:"smin" validate:"min=0"`
		SMax string         `json:"smax" validate:"max=0"`
		SLen string         `json:"slen" validate:"len=0"`
		SCap string         `json:"scap" validate:"max=0,len=3"`
		LMin []string       `json:"lmin" validate:"min=0"`
		LMax []string       `json:"lmax" validate:"max=0"`
		LLen []string       `json:"llen" validate:"len=0"`
		LCap []string       `json:"lcap" validate:"max=0,len=3"`
		MMin map[string]int `json:"mmin" validate:"min=0"`
		MMax map[string]int `json:"mmax" validate:"max=0"`
	}
	v := New()
	b := mustSchema(t, v, reflect.TypeFor[zero](), nil, SchemaOptions{})
	jsonEqual(t, b, `{"type": "object", "additionalProperties": false, "properties": {
	  "smin": {"type": "string", "minLength": 0},
	  "smax": {"type": "string", "maxLength": 0},
	  "slen": {"type": "string", "minLength": 0, "maxLength": 0},
	  "scap": {"type": "string", "minLength": 3, "maxLength": 0},
	  "lmin": {"type": ["array", "null"], "items": {"type": "string"}, "minItems": 0},
	  "lmax": {"type": ["array", "null"], "items": {"type": "string"}, "maxItems": 0},
	  "llen": {"type": ["array", "null"], "items": {"type": "string"}, "minItems": 0, "maxItems": 0},
	  "lcap": {"type": ["array", "null"], "items": {"type": "string"}, "minItems": 3, "maxItems": 0},
	  "mmin": {"type": ["object", "null"], "additionalProperties": {"type": "integer"}, "minProperties": 0},
	  "mmax": {"type": ["object", "null"], "additionalProperties": {"type": "integer"}, "maxProperties": 0}}}`)
}

func TestCompileLockedShortCircuit(t *testing.T) {
	v := New()
	if err := v.Compile(reflect.TypeFor[inner]()); err != nil {
		t.Fatal(err)
	}
	p1, _ := v.plan(reflect.TypeFor[inner]())
	v.mu.Lock()
	p2, err := v.compileLocked(reflect.TypeFor[inner]())
	v.mu.Unlock()
	if err != nil || p1 != p2 {
		t.Fatalf("compileLocked must return the plan compiled meanwhile: %v %v", err, p1 == p2)
	}
}

func TestPropertiesMarshalErrors(t *testing.T) {
	var buf bytes.Buffer
	// An encoder that expects an object name rejects the opening brace.
	enc := jsontext.NewEncoder(&buf)
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		t.Fatal(err)
	}
	if err := (Properties{}).MarshalJSONTo(enc); err == nil {
		t.Fatal("expected an error for a brace in name position")
	}
	// Duplicate member names are rejected by the encoder.
	enc = jsontext.NewEncoder(&buf)
	dup := Properties{{Name: "a", Schema: &Schema{}}, {Name: "a", Schema: &Schema{}}}
	if err := dup.MarshalJSONTo(enc); err == nil {
		t.Fatal("expected a duplicate name error")
	}
	// A member schema that cannot be encoded.
	enc = jsontext.NewEncoder(&buf)
	bad := Properties{{Name: "a", Schema: &Schema{Extra: map[string]any{"x": make(chan int)}}}}
	if err := bad.MarshalJSONTo(enc); err == nil {
		t.Fatal("expected an encode error")
	}
}

func TestSchemaUnmarshalMoreErrors(t *testing.T) {
	bad := []string{
		`{"type": "\uZZZZ"}`,
		`{"additionalProperties": tru}`,
		`{"properties": {1: 2}}`,
		`{"properties": {"a": {"$ref": 1}}}`,
	}
	for _, in := range bad {
		var s Schema
		if err := json.Unmarshal([]byte(in), &s); err == nil {
			t.Errorf("%s: expected error", in)
		}
	}
	// Nesting deeper than the decoder allows fails on a properties brace.
	var doc strings.Builder
	doc.WriteString(`{"a":`)
	for i := 0; i < 5001; i++ {
		doc.WriteString(`{"properties":{"p":`)
	}
	var m map[string]*Schema
	if err := json.Unmarshal([]byte(doc.String()), &m); err == nil {
		t.Fatal("expected a depth error")
	}
}

type withNilSchemer struct {
	S schemerNil `json:"s"`
}

func TestStructBodyErrors(t *testing.T) {
	v := New()
	if _, err := v.SchemaFor(reflect.TypeFor[withNilSchemer](), &Schemas{}, SchemaOptions{}); err == nil || !strings.Contains(err.Error(), "JSONSchema returned nil") {
		t.Fatalf("registered body error: %v", err)
	}
	if _, err := v.SchemaFor(reflect.TypeFor[withNilSchemer](), nil, SchemaOptions{}); err == nil {
		t.Fatal("inline body error must propagate")
	}
}

func TestCompileErrorsOnContainers(t *testing.T) {
	v := New()
	err := v.Compile(field1("gt=1", reflect.TypeFor[[]int]()))
	if err == nil || !strings.Contains(err.Error(), `rule "gt" is not supported on []int`) {
		t.Fatalf("slice: %v", err)
	}
	err = v.Compile(field1("email", reflect.TypeFor[map[string]int]()))
	if err == nil || !strings.Contains(err.Error(), `rule "email" is not supported on map[string]int`) {
		t.Fatalf("map: %v", err)
	}
}
