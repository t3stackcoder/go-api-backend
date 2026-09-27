package validate

import (
	"encoding/json/v2"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type namedForErrors struct {
	SKU string `json:"sku" validate:"bogus"`
	Qty int    `json:"qty" validate:"min=abc"`
}

func TestCompileErrors(t *testing.T) {
	type nested struct {
		X string `validate:"min=1"`
	}
	tests := []struct {
		name string
		tag  string
		typ  reflect.Type
		want string
	}{
		{"unknown rule", "bogus", reflect.TypeFor[string](), `unknown rule "bogus"`},
		{"int bad number", "min=abc", reflect.TypeFor[int](), `rule "min": "abc" is not a valid int`},
		{"int fraction", "max=1.5", reflect.TypeFor[int](), `rule "max": "1.5" is not a valid int`},
		{"int8 overflow", "min=300", reflect.TypeFor[int8](), `rule "min": "300" is not a valid int8`},
		{"uint negative", "gte=-1", reflect.TypeFor[uint](), `rule "gte": "-1" is not a valid uint`},
		{"float bad", "lt=x", reflect.TypeFor[float64](), `rule "lt": "x" is not a valid float64`},
		{"int oneof bad", "oneof=1 x", reflect.TypeFor[int](), `rule "oneof": "x" is not a valid int`},
		{"string length negative", "min=-1", reflect.TypeFor[string](), `rule "min": "-1" is not a non-negative integer`},
		{"string length huge", "max=99999999999", reflect.TypeFor[string](), `rule "max": "99999999999" is not a non-negative integer`},
		{"string length ceiling plus one", "max=2147483648", reflect.TypeFor[string](), `rule "max": "2147483648" is not a non-negative integer`},
		{"string length text", "len=abc", reflect.TypeFor[string](), `rule "len": "abc" is not a non-negative integer`},
		{"slice length bad", "min=x", reflect.TypeFor[[]int](), `rule "min": "x" is not a non-negative integer`},
		{"map length bad", "max=x", reflect.TypeFor[map[string]int](), `rule "max": "x" is not a non-negative integer`},
		{"pattern invalid", "pattern=[", reflect.TypeFor[string](), `rule "pattern": error parsing regexp`},
		{"pattern not last", "pattern=^a$,min=1", reflect.TypeFor[string](), `rule "pattern" must be the last rule`},
		{"gt on string", "gt=1", reflect.TypeFor[string](), `rule "gt" is not supported on string`},
		{"email on int", "email", reflect.TypeFor[int](), `rule "email" is not supported on int`},
		{"oneof on float", "oneof=1 2", reflect.TypeFor[float64](), `rule "oneof" is not supported on float64`},
		{"allowempty on int", "allowempty", reflect.TypeFor[int](), `rule "allowempty" is not supported on int`},
		{"unique on structs", "unique", reflect.TypeFor[[]nested](), `rule "unique" requires a slice of scalars, not []validate.nested`},
		{"unique on nested slices", "unique", reflect.TypeFor[[][]int](), `rule "unique" requires a slice of scalars, not [][]int`},
		{"dive on string", "dive,min=1", reflect.TypeFor[string](), `rule "dive" is not supported on string`},
		{"dive on int", "dive", reflect.TypeFor[int](), `rule "dive" is not supported on int`},
		{"dive on float", "dive", reflect.TypeFor[float64](), `rule "dive" is not supported on float64`},
		{"dive on uint", "dive", reflect.TypeFor[uint](), `rule "dive" is not supported on uint`},
		{"dive on struct", "dive", reflect.TypeFor[nested](), `rule "dive" is not supported on validate.nested`},
		{"dive on time", "dive", reflect.TypeFor[time.Time](), `rule "dive" is not supported on time.Time`},
		{"dive on bytes", "dive", reflect.TypeFor[[]byte](), `rule "dive" is not supported on []uint8`},
		{"min on bool", "min=1", reflect.TypeFor[bool](), `rule "min" is not supported on bool`},
		{"min on time", "min=1", reflect.TypeFor[time.Time](), `rule "min" is not supported on time.Time`},
		{"min on bytes", "min=1", reflect.TypeFor[[]byte](), `rule "min" is not supported on []uint8`},
		{"min on struct", "min=1", reflect.TypeFor[nested](), `rule "min" is not supported on validate.nested`},
		{"len on map", "len=1", reflect.TypeFor[map[string]int](), `rule "len" is not supported on map[string]int`},
		{"unique on map", "unique", reflect.TypeFor[map[string]int](), `rule "unique" is not supported on map[string]int`},
		{"map key bool", "", reflect.TypeFor[map[bool]int](), `map key type bool is not supported`},
		{"chan", "", reflect.TypeFor[chan int](), `type chan int cannot be validated or encoded as JSON`},
		{"func", "required", reflect.TypeFor[func()](), `type func() cannot be validated or encoded as JSON`},
		{"complex", "", reflect.TypeFor[complex128](), `type complex128 cannot be validated or encoded as JSON`},
		{"element rule via dive", "dive,gt=1", reflect.TypeFor[[]string](), `rule "gt" is not supported on string`},
		{"pointer element", "dive,email", reflect.TypeFor[[]*int](), `rule "email" is not supported on int`},
		{"map value", "dive,min=x", reflect.TypeFor[map[string]int](), `rule "min": "x" is not a valid int`},
		{"nested struct field", "", reflect.TypeFor[struct {
			N string `validate:"nope"`
		}](), `unknown rule "nope"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := New()
			err := v.Compile(field1(tc.tag, tc.typ))
			if err == nil {
				t.Fatalf("tag %q on %s: expected error", tc.tag, tc.typ)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("tag %q on %s: error %q does not contain %q", tc.tag, tc.typ, err, tc.want)
			}
			if !strings.Contains(err.Error(), "validate: struct {") && !strings.Contains(err.Error(), ".V: ") {
				t.Fatalf("error should name the type and field: %q", err)
			}
		})
	}
}

// TestJSONNameAgreesWithJSONV2 (G13): validate derives member names from
// json tags exactly as encoding/json/v2 does and rejects every tag json/v2
// rejects. json/v2 itself is the oracle: each tag is put on a one-field
// struct, marshaled, and its member name (or its rejection) compared with
// the compiled plan (or the Compile error). SchemaFor fails on the same
// tags, so the generated schema cannot disagree either.
//
// The structs are built with reflect.StructOf because the tags under test
// are, by design, ones a linter objects to: staticcheck's SA5008 still
// follows the json/v2 experiment, which quoted such names ('-'), while the
// json/v2 shipped with Go 1.27 rejects quoted names and spells a member
// named "-" as json:"-,omitempty".
func TestJSONNameAgreesWithJSONV2(t *testing.T) {
	cases := []struct {
		tag  string
		name string // the member name; "" when skipped or rejected
		err  string // part of the Compile error; "" when json/v2 accepts the tag
	}{
		{tag: "", name: "A"},
		{tag: ",omitempty", name: "A"},
		{tag: "-"},
		{tag: "-,omitempty", name: "-"},
		{tag: "-,omitzero", name: "-"},
		{tag: "--", name: "--"},
		{tag: "my-name", name: "my-name"},
		{tag: "a b", name: "a b"},
		{tag: "1a", name: "1a"},
		{tag: "a,unknownoption", name: "a"},
		{tag: "a,omitempty,omitzero", name: "a"},
		{tag: "-,", err: `json tag "-," is malformed: trailing comma`},
		{tag: "a,", err: `json tag "a," is malformed: trailing comma`},
		{tag: ",", err: `json tag "," is malformed: trailing comma`},
		{tag: "a,,omitempty", err: `json tag "a,,omitempty" is malformed: empty option`},
		{tag: "'-'", err: `'\'' cannot appear in a member name`},
		{tag: "'quoted name',omitempty", err: `'\'' cannot appear in a member name`},
		{tag: "'abc", err: `'\'' cannot appear in a member name`},
		{tag: `a"b`, err: `'"' cannot appear in a member name`},
		{tag: `a\b`, err: `'\\' cannot appear in a member name`},
		{tag: "a\x60b", err: "'\x60' cannot appear in a member name"},
	}
	for _, tc := range cases {
		t.Run(strconv.Quote(tc.tag), func(t *testing.T) {
			typ := reflect.StructOf([]reflect.StructField{{Name: "A", Type: reflect.TypeFor[string](), Tag: reflect.StructTag("json:" + strconv.Quote(tc.tag))}})
			val := reflect.New(typ).Elem()
			val.Field(0).SetString("x")
			encoded, jerr := json.Marshal(val.Interface())

			v := New()
			cerr := v.Compile(typ)
			_, serr := v.SchemaFor(typ, nil, SchemaOptions{})
			if tc.err != "" {
				if jerr == nil {
					t.Fatalf("json/v2 accepts %q, so validate must too", tc.tag)
				}
				if cerr == nil || !strings.Contains(cerr.Error(), ".A: ") || !strings.Contains(cerr.Error(), tc.err) {
					t.Fatalf("Compile = %v, want %q naming the field", cerr, tc.err)
				}
				if serr == nil || !strings.Contains(serr.Error(), tc.err) {
					t.Fatalf("SchemaFor = %v", serr)
				}
				return
			}
			if jerr != nil {
				t.Fatalf("json/v2 rejects %q (%v), so validate must too", tc.tag, jerr)
			}
			if cerr != nil || serr != nil {
				t.Fatalf("Compile = %v, SchemaFor = %v", cerr, serr)
			}
			var members map[string]string
			if err := json.Unmarshal(encoded, &members); err != nil {
				t.Fatal(err)
			}
			var want []string
			for k := range members {
				want = append(want, k)
			}
			p, err := v.plan(typ)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for i := range p.fields {
				got = append(got, p.fields[i].name)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("validate sees members %q, json/v2 %q (%s)", got, want, encoded)
			}
			if tc.name != "" && (len(got) != 1 || got[0] != tc.name) {
				t.Fatalf("member %q, want %q", got, tc.name)
			}
		})
	}
}

// TestJSONTagOnUnexportedField: json/v2 refuses any tag other than "-" on an
// unexported, non-embedded field, so Compile does too; a plain unexported
// field and one tagged "-" are skipped as before.
func TestJSONTagOnUnexportedField(t *testing.T) {
	field := func(name, tag string) reflect.StructField {
		sf := reflect.StructField{Name: name, Type: reflect.TypeFor[string]()}
		if tag != "" {
			sf.Tag = reflect.StructTag("json:" + strconv.Quote(tag))
		}
		if name[0] >= 'a' && name[0] <= 'z' {
			sf.PkgPath = "validate"
		}
		return sf
	}
	ok := reflect.StructOf([]reflect.StructField{field("Public", "public"), field("hidden", ""), field("gone", "-")})
	if err := New().Compile(ok); err != nil {
		t.Fatalf("untagged and \"-\" unexported fields are skipped: %v", err)
	}
	for _, tag := range []string{"secret", ",omitempty", ""} {
		typ := reflect.StructOf([]reflect.StructField{field("Public", "public"), {Name: "secret", Type: reflect.TypeFor[string](), Tag: reflect.StructTag("json:" + strconv.Quote(tag)), PkgPath: "validate"}})
		val := reflect.New(typ).Elem()
		if _, jerr := json.Marshal(val.Interface()); jerr == nil {
			t.Fatalf("json/v2 accepts json:%q on an unexported field", tag)
		}
		err := New().Compile(typ)
		if err == nil || !strings.Contains(err.Error(), ".secret: unexported field has json tag "+strconv.Quote(tag)+", which encoding/json/v2 rejects") {
			t.Fatalf("json:%q: Compile = %v", tag, err)
		}
	}
	// A bad tag inside a nested struct is reported against the declaring type.
	promotedBad := reflect.StructOf([]reflect.StructField{{Name: "Other", Type: reflect.TypeFor[string](), Tag: reflect.StructTag("json:" + strconv.Quote("other,"))}})
	outer := reflect.StructOf([]reflect.StructField{{Name: "Head", Type: reflect.TypeFor[string](), Tag: `json:"head"`}, {Name: "Tail", Type: promotedBad, Tag: `json:"tail"`}})
	err := New().Compile(outer)
	if err == nil || !strings.Contains(err.Error(), `.Other: json tag "other," is malformed: trailing comma`) {
		t.Fatalf("Compile = %v", err)
	}
}

func TestCompileErrorsAreJoined(t *testing.T) {
	v := New()
	err := v.Compile(reflect.TypeFor[namedForErrors]())
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{
		`validate: namedForErrors.SKU: unknown rule "bogus"`,
		`validate: namedForErrors.Qty: rule "min": "abc" is not a valid int`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	// The same nested type is reported once even when referenced twice.
	type twice struct {
		A namedForErrors `json:"a"`
		B namedForErrors `json:"b"`
	}
	err = v.Compile(reflect.TypeFor[twice]())
	if n := strings.Count(err.Error(), `unknown rule "bogus"`); n != 1 {
		t.Fatalf("nested type errors reported %d times: %v", n, err)
	}
}

func TestCompileAcceptsEveryRuleOnce(t *testing.T) {
	type all struct {
		S1 string            `validate:"required,allowempty,min=1,max=2,len=1,oneof=a b,email"`
		S2 string            `validate:"uuid"`
		S3 string            `validate:"url"`
		S4 string            `validate:"datetime"`
		S5 string            `validate:"ipv4"`
		S6 string            `validate:"ipv6"`
		S7 string            `validate:"hostname,pattern=^a{1,2}$"`
		I  int               `validate:"required,min=1,max=2,gt=0,gte=1,lt=3,lte=2,oneof=1 2"`
		U  uint              `validate:"min=1,max=2,gt=0,gte=1,lt=3,lte=2,oneof=1 2"`
		F  float64           `validate:"min=1,max=2,gt=0,gte=1,lt=3,lte=2"`
		L  []string          `validate:"required,min=1,max=2,len=1,unique,dive,min=1"`
		M  map[string]string `validate:"required,min=1,max=2,dive,min=1"`
		A  [1]string         `validate:"required,min=1,max=2,len=1,unique,dive,min=1"`
		B  []byte            `validate:"required"`
		P  *int              `validate:"required,min=1"`
	}
	if err := New().Compile(reflect.TypeFor[all]()); err != nil {
		t.Fatal(err)
	}
}
