package validate

import (
	"reflect"
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
