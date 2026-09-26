package validate

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// FuzzEmbed is embedded by fuzzed struct shapes.
type FuzzEmbed struct {
	Embedded string `json:"embedded" validate:"max=8"`
}

// FuzzNamed is a named struct referenced by fuzzed struct shapes.
type FuzzNamed struct {
	N int `json:"n" validate:"gte=0"`
}

// descriptor reads a fuzzed byte string as a stream of small decisions.
type descriptor struct {
	data []byte
	pos  int
}

func (d *descriptor) next(n int) int {
	if d.pos >= len(d.data) {
		return 0
	}
	b := d.data[d.pos]
	d.pos++
	return int(b) % n
}

// fuzzTags lists tag fragments per kind family; "bad" entries must fail to
// compile on that family.
var fuzzTags = map[string][]string{
	"string": {"", "required", "required,allowempty", "min=1", "max=3", "len=2", "pattern=^[a-z]{1,3}$", "oneof=a b", "email", "uuid", "url", "datetime", "ipv4", "ipv6", "hostname", "required,min=2,max=4"},
	"number": {"", "required", "min=1", "max=9", "gt=0", "gte=0", "lt=10", "lte=9", "oneof=1 2", "gt=0,lt=5"},
	"float":  {"", "required", "min=0.5", "max=9.5", "gt=0", "gte=0", "lt=1e3", "lte=9"},
	"slice":  {"", "required", "min=1", "max=3", "len=2", "unique", "dive", "dive,required", "required,min=1,dive,max=3"},
	"map":    {"", "required", "min=1", "max=3", "dive", "dive,gte=0"},
	"opaque": {"", "required"},
	"bad":    {"bogus", "min=x", "gt=1,email", "dive,dive,dive,min=1", "pattern=[", "unique,dive", "len=-1", "required,required", "pattern=^a$,min=1"},
}

// buildType constructs a struct type from the descriptor.
func buildType(d *descriptor, depth int) reflect.Type {
	n := d.next(6)
	fields := make([]reflect.StructField, 0, n+1)
	if d.next(4) == 0 {
		fields = append(fields, reflect.StructField{Name: "FuzzEmbed", Type: reflect.TypeFor[FuzzEmbed](), Anonymous: true})
	}
	for i := 0; i < n; i++ {
		var (
			typ    reflect.Type
			family string
		)
		switch d.next(24) {
		case 0:
			typ, family = reflect.TypeFor[string](), "string"
		case 1:
			typ, family = reflect.TypeFor[int](), "number"
		case 2:
			typ, family = reflect.TypeFor[int8](), "number"
		case 3:
			typ, family = reflect.TypeFor[uint16](), "number"
		case 4:
			typ, family = reflect.TypeFor[float64](), "float"
		case 5:
			typ, family = reflect.TypeFor[bool](), "opaque"
		case 6:
			typ, family = reflect.TypeFor[*string](), "string"
		case 7:
			typ, family = reflect.TypeFor[[]string](), "slice"
		case 8:
			typ, family = reflect.TypeFor[[]int](), "slice"
		case 9:
			typ, family = reflect.TypeFor[map[string]int](), "map"
		case 10:
			typ, family = reflect.TypeFor[[]byte](), "opaque"
		case 11:
			typ, family = reflect.TypeFor[time.Time](), "opaque"
		case 12:
			typ, family = reflect.TypeFor[uuid.UUID](), "opaque"
		case 13:
			typ, family = reflect.TypeFor[any](), "opaque"
		case 14:
			typ, family = reflect.TypeFor[jsontext.Value](), "opaque"
		case 15:
			typ, family = reflect.TypeFor[FuzzNamed](), "opaque"
		case 16:
			typ, family = reflect.TypeFor[*FuzzNamed](), "opaque"
		case 17:
			typ, family = reflect.TypeFor[[]FuzzNamed](), "slice"
		case 18:
			typ, family = reflect.TypeFor[map[string]*string](), "map"
		case 19:
			typ, family = reflect.TypeFor[[2]int](), "slice"
		case 20:
			typ, family = reflect.TypeFor[color](), "string"
		case 21:
			typ, family = reflect.TypeFor[mediator.Command[mediator.Void]](), "opaque"
		default:
			family = "opaque"
			if depth > 0 {
				typ = buildType(d, depth-1)
				if d.next(2) == 0 {
					typ = reflect.PointerTo(typ)
				} else if d.next(2) == 0 {
					typ = reflect.SliceOf(typ)
					family = "slice"
				}
			} else {
				typ = reflect.TypeFor[struct{}]()
			}
		}
		var tag strings.Builder
		switch d.next(8) {
		case 0:
			tag.WriteString(`json:"-" `)
		case 1:
			// Encoder-side options such as omitzero are deliberately absent: they
			// change what a marshaled Go value contains, not what a request may.
			tag.WriteString(`json:"f` + strconv.Itoa(i) + `" doc:"field ` + strconv.Itoa(i) + `" `)
		default:
			tag.WriteString(`json:"f` + strconv.Itoa(i) + `" `)
		}
		list := fuzzTags[family]
		if d.next(10) == 0 {
			list = fuzzTags["bad"]
		}
		tag.WriteString(`validate:` + strconv.Quote(list[d.next(len(list))]))
		fields = append(fields, reflect.StructField{Name: "F" + strconv.Itoa(i), Type: typ, Tag: reflect.StructTag(tag.String())})
	}
	return reflect.StructOf(fields)
}

// FuzzSchemaGen (spec 11.3): arbitrary struct shapes never panic SchemaFor,
// and every generated document marshals and compiles against the draft
// 2020-12 meta-schema.
func FuzzSchemaGen(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{3, 1, 0, 2, 1, 1, 3, 2, 7, 2, 6})
	f.Add([]byte{5, 0, 23, 2, 0, 0, 1, 23, 1, 1, 0, 2, 9, 2, 5, 17, 3, 7})
	f.Add([]byte{2, 1, 0, 0, 9, 0, 3, 0, 9, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		typ := buildType(&descriptor{data: data}, 3)
		v := New()
		reg := &Schemas{}
		root, err := v.SchemaFor(typ, reg, SchemaOptions{Descriptions: true, RefPrefix: "#/$defs/"})
		if err != nil {
			// Bad tags are rejected loudly, never silently.
			if root != nil {
				t.Fatalf("schema returned with error %v", err)
			}
			if cerr := v.Compile(typ); cerr == nil {
				t.Fatalf("SchemaFor failed with %v but Compile succeeded", err)
			}
			return
		}
		doc := &Schema{Schema: "https://json-schema.org/draft/2020-12/schema", Defs: reg.Defs}
		if root.Ref != "" {
			doc.Ref = root.Ref
		} else {
			doc.Type, doc.Properties, doc.Required, doc.AdditionalProperties = root.Type, root.Properties, root.Required, root.AdditionalProperties
		}
		b, err := json.Marshal(doc, json.Deterministic(true))
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		raw, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			t.Fatalf("not JSON: %v\n%s", err, b)
		}
		c := jsonschema.NewCompiler()
		c.AssertFormat()
		if err := c.AddResource("gen.json", raw); err != nil {
			t.Fatal(err)
		}
		sch, err := c.Compile("gen.json")
		if err != nil {
			t.Fatalf("generated schema does not compile: %v\n%s", err, b)
		}
		// The zero value checks without panicking and agrees with the schema.
		zero := reflect.New(typ).Interface()
		checkErr := v.Check(bg, zero)
		var ve *mediator.ValidationError
		if checkErr != nil && !errors.As(checkErr, &ve) {
			t.Fatalf("Check on zero value: %v", checkErr)
		}
		enc, err := json.Marshal(zero, json.FormatNilSliceAsNull(true), json.FormatNilMapAsNull(true))
		if err != nil {
			return // e.g. an embedded fallback conflict the encoder rejects
		}
		inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(enc))
		if err != nil {
			t.Fatal(err)
		}
		if schemaErr := sch.Validate(inst); (schemaErr == nil) != (checkErr == nil) {
			t.Fatalf("zero value disagreement on %s\nCheck: %v\nSchema: %v\n%s", enc, checkErr, schemaErr, b)
		}
	})
}
