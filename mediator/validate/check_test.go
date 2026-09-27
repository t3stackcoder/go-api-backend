package validate

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

var bg = context.Background()

// field1 builds struct{ V T `json:"v" validate:"tag"` }.
func field1(tag string, typ reflect.Type) reflect.Type {
	return reflect.StructOf([]reflect.StructField{{
		Name: "V", Type: typ, Tag: reflect.StructTag(`json:"v" validate:` + strconv.Quote(tag)),
	}})
}

// check1 validates a one-field struct holding val (nil leaves the zero value).
func check1(v *Validator, tag string, typ reflect.Type, val any) error {
	rv := reflect.New(field1(tag, typ))
	if val != nil {
		rv.Elem().Field(0).Set(reflect.ValueOf(val))
	}
	return v.Check(bg, rv.Interface())
}

type row struct {
	name string
	tag  string
	typ  reflect.Type
	val  any
	rule string // "" means valid
	msg  string
}

func fieldsOf(t *testing.T, err error) []mediator.FieldError {
	t.Helper()
	var ve *mediator.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("got %v (%T), want *mediator.ValidationError", err, err)
	}
	return ve.Fields
}

func runRows(t *testing.T, rows []row) {
	t.Helper()
	v := New()
	for _, r := range rows {
		t.Run(r.name, func(t *testing.T) {
			err := check1(v, r.tag, r.typ, r.val)
			if r.rule == "" {
				if err != nil {
					t.Fatalf("tag %q value %#v: unexpected error %v", r.tag, r.val, err)
				}
				return
			}
			got := fieldsOf(t, err)
			want := mediator.FieldError{Path: "/v", Rule: r.rule, Message: r.msg}
			if len(got) != 1 || got[0] != want {
				t.Fatalf("tag %q value %#v: got %+v, want [%+v]", r.tag, r.val, got, want)
			}
		})
	}
}

type color string

func TestStringRules(t *testing.T) {
	str := reflect.TypeFor[string]()
	runRows(t, []row{
		{"required empty", "required", str, "", "required", "is required"},
		{"required ok", "required", str, "x", "", ""},
		{"required allowempty", "required,allowempty", str, "", "", ""},
		{"required min short-circuits", "required,min=3,email", str, "", "required", "is required"},
		{"min short", "min=2", str, "a", "min", "must be at least 2 characters"},
		{"min ok", "min=2", str, "ab", "", ""},
		{"min counts runes", "min=2", str, "é", "min", "must be at least 2 characters"},
		{"min runes ok", "min=2", str, "日本", "", ""},
		{"min singular", "min=1", str, "", "min", "must be at least 1 character"},
		{"min zero ok", "min=0", str, "", "", ""},
		{"max long", "max=3", str, "abcd", "max", "must be at most 3 characters"},
		{"max ok", "max=3", str, "abc", "", ""},
		{"max runes ok", "max=3", str, "日本語", "", ""},
		{"max empty ok", "max=3", str, "", "", ""},
		{"max singular", "max=1", str, "ab", "max", "must be at most 1 character"},
		{"max zero ok", "max=0", str, "", "", ""},
		{"max zero rejects one rune", "max=0", str, "a", "max", "must be at most 0 characters"},
		{"max at the length ceiling ok", "max=2147483647", str, "a", "", ""},
		{"len short", "len=2", str, "a", "len", "must be exactly 2 characters"},
		{"len ok", "len=2", str, "ab", "", ""},
		{"len long", "len=2", str, "abc", "len", "must be exactly 2 characters"},
		{"len singular", "len=1", str, "", "len", "must be exactly 1 character"},
		{"len zero ok", "len=0", str, "", "", ""},
		{"len zero rejects one rune", "len=0", str, "a", "len", "must be exactly 0 characters"},
		{"pattern ok", "pattern=^[a-z]+$", str, "abc", "", ""},
		{"pattern bad", "pattern=^[a-z]+$", str, "ABC", "pattern", "must match ^[a-z]+$"},
		{"pattern with comma", "pattern=^[a-z]{2,3}$", str, "abcd", "pattern", "must match ^[a-z]{2,3}$"},
		{"oneof ok", "oneof=a b c", str, "b", "", ""},
		{"oneof bad", "oneof=a b c", str, "d", "oneof", "must be one of: a, b, c"},
		{"named oneof ok", "oneof=red green", reflect.TypeFor[color](), color("red"), "", ""},
		{"named oneof bad", "oneof=red green", reflect.TypeFor[color](), color("blue"), "oneof", "must be one of: red, green"},
		{"email ok", "email", str, "a@b.com", "", ""},
		{"email bad", "email", str, "nope", "email", "must be an email address"},
		{"email empty", "email", str, "", "email", "must be an email address"},
		{"uuid ok", "uuid", str, "123e4567-e89b-12d3-a456-426614174000", "", ""},
		{"uuid bad", "uuid", str, "x", "uuid", "must be a UUID"},
		{"url ok", "url", str, "https://x.io/p", "", ""},
		{"url bad", "url", str, "x", "url", "must be a URL"},
		{"datetime ok", "datetime", str, "2024-01-02T03:04:05Z", "", ""},
		{"datetime bad", "datetime", str, "2024-01-02", "datetime", "must be an RFC 3339 date-time"},
		{"ipv4 ok", "ipv4", str, "10.0.0.1", "", ""},
		{"ipv4 bad", "ipv4", str, "::1", "ipv4", "must be an IPv4 address"},
		{"ipv6 ok", "ipv6", str, "::1", "", ""},
		{"ipv6 bad", "ipv6", str, "10.0.0.1", "ipv6", "must be an IPv6 address"},
		{"hostname ok", "hostname", str, "example.com", "", ""},
		{"hostname bad", "hostname", str, "-x", "hostname", "must be a hostname"},
	})
}

func TestNumberRules(t *testing.T) {
	i, f := reflect.TypeFor[int](), reflect.TypeFor[float64]()
	runRows(t, []row{
		{"min low", "min=1", i, 0, "min", "must be at least 1"},
		{"min ok", "min=1", i, 1, "", ""},
		{"max high", "max=10", i, 11, "max", "must be at most 10"},
		{"max ok", "max=10", i, 10, "", ""},
		{"gt equal", "gt=0", i, 0, "gt", "must be greater than 0"},
		{"gt ok", "gt=0", i, 1, "", ""},
		{"gte low", "gte=0", i, -1, "gte", "must be at least 0"},
		{"gte ok", "gte=0", i, 0, "", ""},
		{"lt equal", "lt=5", i, 5, "lt", "must be less than 5"},
		{"lt ok", "lt=5", i, 4, "", ""},
		{"lte high", "lte=5", i, 6, "lte", "must be at most 5"},
		{"lte ok", "lte=5", i, 5, "", ""},
		{"oneof ok", "oneof=1 2 3", i, 2, "", ""},
		{"oneof bad", "oneof=1 2 3", i, 4, "oneof", "must be one of: 1, 2, 3"},
		{"required zero ok", "required", i, 0, "", ""},
		{"min and gte", "min=1,gte=2", i, 1, "gte", "must be at least 2"},
		{"int8 min", "min=-1", reflect.TypeFor[int8](), int8(-2), "min", "must be at least -1"},
		{"int64 max", "max=9007199254740993", reflect.TypeFor[int64](), int64(9007199254740994), "max", "must be at most 9007199254740993"},
		{"uint8 max", "max=200", reflect.TypeFor[uint8](), uint8(201), "max", "must be at most 200"},
		{"uint64 min", "min=1", reflect.TypeFor[uint64](), uint64(0), "min", "must be at least 1"},
		{"uint64 oneof", "oneof=1 18446744073709551615", reflect.TypeFor[uint64](), uint64(2), "oneof", "must be one of: 1, 18446744073709551615"},
		{"uintptr gte", "gte=1", reflect.TypeFor[uintptr](), uintptr(0), "gte", "must be at least 1"},
		{"float gte", "gte=0.5", f, 0.25, "gte", "must be at least 0.5"},
		{"float gte ok", "gte=0.5", f, 0.5, "", ""},
		{"float lt", "lt=1.5", f, 1.5, "lt", "must be less than 1.5"},
		{"float max", "max=1e6", f, 1000001.0, "max", "must be at most 1e+06"},
		{"float32 min", "min=0", reflect.TypeFor[float32](), float32(-1), "min", "must be at least 0"},
	})
}

type inner struct {
	SKU string `json:"sku" validate:"required"`
}

type marshalerStruct struct{ A int }

func (marshalerStruct) MarshalJSON() ([]byte, error) { return []byte(`"m"`), nil }

type marshalerTo struct{ A int }

func (*marshalerTo) MarshalJSONTo(enc *jsontext.Encoder) error {
	return enc.WriteToken(jsontext.String("m"))
}

func TestPointerRules(t *testing.T) {
	s := "abc"
	ab, empty := "ab", ""
	var nilp *string
	x := &s
	zero := 0
	var nilSlice []string
	runRows(t, []row{
		{"nil optional skips rules", "max=2", reflect.TypeFor[*string](), nil, "", ""},
		{"non-nil rule fails", "max=2", reflect.TypeFor[*string](), &s, "max", "must be at most 2 characters"},
		{"non-nil ok", "max=2", reflect.TypeFor[*string](), &ab, "", ""},
		{"required nil", "required", reflect.TypeFor[*string](), nil, "required", "is required"},
		{"required empty string", "required", reflect.TypeFor[*string](), &empty, "required", "is required"},
		{"required ok", "required", reflect.TypeFor[*string](), &s, "", ""},
		{"required allowempty", "required,allowempty", reflect.TypeFor[*string](), &empty, "", ""},
		{"int required nil", "required", reflect.TypeFor[*int](), nil, "required", "is required"},
		{"int required zero ok", "required", reflect.TypeFor[*int](), &zero, "", ""},
		{"pp required nil", "required", reflect.TypeFor[**string](), nil, "required", "is required"},
		{"pp inner nil", "required", reflect.TypeFor[**string](), &nilp, "required", "is required"},
		{"pp ok", "required", reflect.TypeFor[**string](), &x, "", ""},
		{"ptr slice required nil", "required", reflect.TypeFor[*[]string](), nil, "required", "is required"},
		{"ptr slice inner nil", "required", reflect.TypeFor[*[]string](), &nilSlice, "required", "is required"},
		{"ptr slice ok", "required", reflect.TypeFor[*[]string](), &[]string{}, "", ""},
		{"ptr slice dive", "dive,min=2", reflect.TypeFor[*[]string](), &[]string{"ab"}, "", ""},
		{"ptr struct nil", "", reflect.TypeFor[*inner](), nil, "", ""},
		{"ptr struct required nil", "required", reflect.TypeFor[*inner](), nil, "required", "is required"},
		{"ptr struct rules", "required", reflect.TypeFor[*inner](), &inner{SKU: "x"}, "", ""},
	})
}

func TestSliceRules(t *testing.T) {
	ss, is := reflect.TypeFor[[]string](), reflect.TypeFor[[]int]()
	runRows(t, []row{
		{"required nil", "required", ss, nil, "required", "is required"},
		{"required empty ok", "required", ss, []string{}, "", ""},
		{"min nil skipped", "min=1", ss, nil, "", ""},
		{"min empty", "min=1", ss, []string{}, "min", "must have at least 1 item"},
		{"min ok", "min=1", ss, []string{"a"}, "", ""},
		{"min plural", "min=2", ss, []string{"a"}, "min", "must have at least 2 items"},
		{"min zero ok", "min=0", ss, []string{}, "", ""},
		{"max", "max=2", ss, []string{"a", "b", "c"}, "max", "must have at most 2 items"},
		{"max singular", "max=1", ss, []string{"a", "b"}, "max", "must have at most 1 item"},
		{"max ok", "max=2", ss, []string{"a", "b"}, "", ""},
		{"max zero ok", "max=0", ss, []string{}, "", ""},
		{"max zero rejects one item", "max=0", ss, []string{"a"}, "max", "must have at most 0 items"},
		{"len short", "len=2", ss, []string{}, "len", "must have exactly 2 items"},
		{"len ok", "len=2", ss, []string{"a", "b"}, "", ""},
		{"len singular", "len=1", ss, []string{}, "len", "must have exactly 1 item"},
		{"len zero ok", "len=0", ss, []string{}, "", ""},
		{"len zero rejects one item", "len=0", ss, []string{"a"}, "len", "must have exactly 0 items"},
		{"unique dup", "unique", ss, []string{"a", "a"}, "unique", "must not contain duplicates"},
		{"unique ok", "unique", ss, []string{"a", "b"}, "", ""},
		{"unique ints", "unique", is, []int{1, 2, 1}, "unique", "must not contain duplicates"},
		{"unique floats ok", "unique", reflect.TypeFor[[]float64](), []float64{1, 1.5}, "", ""},
		{"unique bools", "unique", reflect.TypeFor[[]bool](), []bool{true, true}, "unique", "must not contain duplicates"},
		{"unique nil ok", "unique", ss, nil, "", ""},
		{"array dive ok", "dive,gte=0", reflect.TypeFor[[2]int](), [2]int{1, 0}, "", ""},
		{"array required ok", "required", reflect.TypeFor[[2]int](), [2]int{}, "", ""},
		{"array min ok", "min=1", reflect.TypeFor[[2]int](), [2]int{}, "", ""},
		{"array unique", "unique", reflect.TypeFor[[2]int](), [2]int{3, 3}, "unique", "must not contain duplicates"},
		{"bytes required nil", "required", reflect.TypeFor[[]byte](), nil, "required", "is required"},
		{"bytes required empty ok", "required", reflect.TypeFor[[]byte](), []byte{}, "", ""},
		{"bytes optional nil ok", "", reflect.TypeFor[[]byte](), nil, "", ""},
		{"byte array required ok", "required", reflect.TypeFor[[4]byte](), [4]byte{}, "", ""},
		{"byte array optional ok", "", reflect.TypeFor[[4]byte](), [4]byte{}, "", ""},
		{"named bytes required nil", "required", reflect.TypeFor[net.HardwareAddr](), nil, "required", "is required"},
	})
}

func TestMapRules(t *testing.T) {
	m := reflect.TypeFor[map[string]int]()
	runRows(t, []row{
		{"required nil", "required", m, nil, "required", "is required"},
		{"required empty ok", "required", m, map[string]int{}, "", ""},
		{"min nil skipped", "min=1", m, nil, "", ""},
		{"min empty", "min=1", m, map[string]int{}, "min", "must have at least 1 entry"},
		{"min plural", "min=2", m, map[string]int{"a": 1}, "min", "must have at least 2 entries"},
		{"min ok", "min=1", m, map[string]int{"a": 1}, "", ""},
		{"min zero ok", "min=0", m, map[string]int{}, "", ""},
		{"max", "max=1", m, map[string]int{"a": 1, "b": 2}, "max", "must have at most 1 entry"},
		{"max plural", "max=2", m, map[string]int{"a": 1, "b": 2, "c": 3}, "max", "must have at most 2 entries"},
		{"max ok", "max=1", m, map[string]int{"a": 1}, "", ""},
		{"max zero ok", "max=0", m, map[string]int{}, "", ""},
		{"max zero rejects one entry", "max=0", m, map[string]int{"a": 1}, "max", "must have at most 0 entries"},
	})
}

func TestOpaqueRules(t *testing.T) {
	runRows(t, []row{
		{"bool required false ok", "required", reflect.TypeFor[bool](), false, "", ""},
		{"time required zero ok", "required", reflect.TypeFor[time.Time](), time.Time{}, "", ""},
		{"uuid required nil ok", "required", reflect.TypeFor[uuid.UUID](), uuid.Nil, "", ""},
		{"any required nil ok", "required", reflect.TypeFor[any](), nil, "", ""},
		{"raw required nil ok", "required", reflect.TypeFor[jsontext.Value](), nil, "", ""},
		{"text marshaler required nil ok", "required", reflect.TypeFor[net.IP](), nil, "", ""},
		{"json marshaler required ok", "required", reflect.TypeFor[marshalerStruct](), marshalerStruct{}, "", ""},
		{"marshaler to required ok", "required", reflect.TypeFor[marshalerTo](), marshalerTo{}, "", ""},
		{"struct required ok", "required", reflect.TypeFor[inner](), inner{SKU: "x"}, "", ""},
		{"time ptr optional nil", "", reflect.TypeFor[*time.Time](), nil, "", ""},
		{"time ptr required nil", "required", reflect.TypeFor[*time.Time](), nil, "required", "is required"},
		{"time ptr required ok", "required", reflect.TypeFor[*time.Time](), &time.Time{}, "", ""},
		{"ip ptr required nil", "required", reflect.TypeFor[*net.IP](), nil, "required", "is required"},
		{"marshaler ptr required nil", "required", reflect.TypeFor[*marshalerStruct](), nil, "required", "is required"},
	})
	// required on a nested struct is presence only; its own rules still run.
	got := fieldsOf(t, check1(New(), "required", reflect.TypeFor[inner](), inner{}))
	if len(got) != 1 || got[0].Path != "/v/sku" || got[0].Rule != "required" {
		t.Fatalf("nested struct: %+v", got)
	}
}

func TestNestedPaths(t *testing.T) {
	type line struct {
		SKU string `json:"sku" validate:"required"`
		Qty int    `json:"qty" validate:"min=1"`
	}
	type order struct {
		Lines  []line            `json:"lines" validate:"required,min=1,dive"`
		ByKey  map[string]line   `json:"by_key"`
		Ptrs   []*line           `json:"ptrs" validate:"dive,required"`
		Tags   []string          `json:"tags" validate:"dive,min=2"`
		Nested [][]string        `json:"nested" validate:"dive,dive,min=1"`
		Opt    map[string]*line  `json:"opt" validate:"dive,required"`
		Keyed  map[int]string    `json:"keyed" validate:"dive,min=1"`
		UKeyed map[uint8]string  `json:"ukeyed" validate:"dive,min=1"`
		Weird  map[string]string `json:"weird" validate:"dive,min=1"`
		Main   line              `json:"main"`
		Ptr    *line             `json:"ptr"`
	}
	v := New()
	if err := v.Compile(reflect.TypeFor[*order]()); err != nil {
		t.Fatal(err)
	}
	err := v.Check(bg, &order{
		Lines:  []line{{SKU: "a", Qty: 1}, {SKU: "", Qty: 0}},
		ByKey:  map[string]line{"z": {SKU: "", Qty: 2}, "a": {SKU: "x", Qty: 0}},
		Ptrs:   []*line{nil, {SKU: "x", Qty: 1}},
		Tags:   []string{"ab", "c"},
		Nested: [][]string{{"a"}, {""}},
		Opt:    map[string]*line{"k": nil},
		Keyed:  map[int]string{5: ""},
		UKeyed: map[uint8]string{7: ""},
		Weird:  map[string]string{"a/b~c": ""},
		Main:   line{SKU: "", Qty: 1},
		Ptr:    &line{SKU: "p", Qty: 0},
	})
	got := fieldsOf(t, err)
	want := []mediator.FieldError{
		{Path: "/lines/1/sku", Rule: "required", Message: "is required"},
		{Path: "/lines/1/qty", Rule: "min", Message: "must be at least 1"},
		{Path: "/by_key/a/qty", Rule: "min", Message: "must be at least 1"},
		{Path: "/by_key/z/sku", Rule: "required", Message: "is required"},
		{Path: "/ptrs/0", Rule: "required", Message: "is required"},
		{Path: "/tags/1", Rule: "min", Message: "must be at least 2 characters"},
		{Path: "/nested/1/0", Rule: "min", Message: "must be at least 1 character"},
		{Path: "/opt/k", Rule: "required", Message: "is required"},
		{Path: "/keyed/5", Rule: "min", Message: "must be at least 1 character"},
		{Path: "/ukeyed/7", Rule: "min", Message: "must be at least 1 character"},
		{Path: "/weird/a~1b~0c", Rule: "min", Message: "must be at least 1 character"},
		{Path: "/main/sku", Rule: "required", Message: "is required"},
		{Path: "/ptr/qty", Rule: "min", Message: "must be at least 1"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got\n%+v\nwant\n%+v", got, want)
	}
	if err := v.Check(bg, order{Lines: []line{{SKU: "a", Qty: 1}}, Main: line{SKU: "m", Qty: 1}}); err != nil {
		t.Fatalf("valid order: %v", err)
	}
	if !strings.Contains(err.Error(), "/lines/1/sku is required (required)") {
		t.Fatalf("error text: %v", err)
	}
}

func TestMultipleFailuresOnOneField(t *testing.T) {
	v := New()
	err := check1(v, "min=3,pattern=^[a-z]+$", reflect.TypeFor[string](), "AB")
	got := fieldsOf(t, err)
	if len(got) != 2 || got[0].Rule != "min" || got[1].Rule != "pattern" {
		t.Fatalf("got %+v", got)
	}
}

// Embedded structs.

type embBase struct {
	ID string `json:"id" validate:"required"`
}

// EmbPtr is exported because encoding/json ignores unexported embedded pointers.
type EmbPtr struct {
	Code string `json:"code" validate:"required"`
	Opt  string `json:"opt" validate:"min=1"`
}

type embIgnoredPtr struct {
	Gone string `json:"gone" validate:"required"`
}

type embHidden struct {
	H string `json:"h" validate:"min=2"`
}

type embNamed struct {
	N string `json:"n" validate:"required"`
}

type embRoot struct {
	mediator.Command[mediator.Void]
	embBase
	*EmbPtr
	*embIgnoredPtr
	embHidden
	embNamed `json:"named"`
	Field    embBase `json:"field"`
	hidden   string  `validate:"required"` //lint:ignore U1000 unexported: the validator must skip it
	Skip     string  `json:"-" validate:"required"`
}

func TestEmbeddedFlattening(t *testing.T) {
	v := New()
	err := v.Check(bg, embRoot{})
	got := fieldsOf(t, err)
	want := []mediator.FieldError{
		{Path: "/id", Rule: "required", Message: "is required"},
		{Path: "/code", Rule: "required", Message: "is required"},
		{Path: "/h", Rule: "min", Message: "must be at least 2 characters"},
		{Path: "/named/n", Rule: "required", Message: "is required"},
		{Path: "/field/id", Rule: "required", Message: "is required"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got\n%+v\nwant\n%+v", got, want)
	}
	ok := embRoot{embBase: embBase{ID: "i"}, EmbPtr: &EmbPtr{Code: "c", Opt: "o"}, embHidden: embHidden{H: "hh"},
		embNamed: embNamed{N: "n"}, Field: embBase{ID: "f"}}
	if err := v.Check(bg, &ok); err != nil {
		t.Fatalf("valid: %v", err)
	}
}

// Dominance types are built with reflect.StructOf because go vet's structtag
// check rejects the repeated json names the test needs.
type DomA struct {
	X string `json:"x" validate:"required"`
}
type DomB struct {
	X string `json:"x" validate:"required"`
}
type DomC struct {
	X string `validate:"required"`
}
type DomD struct {
	X string `json:"X" validate:"min=5"`
}

func anon(t reflect.Type) reflect.StructField {
	return reflect.StructField{Name: t.Name(), Type: t, Anonymous: true}
}

func TestEmbeddedDominance(t *testing.T) {
	v := New()
	domA, domB, domC, domD := reflect.TypeFor[DomA](), reflect.TypeFor[DomB](), reflect.TypeFor[DomC](), reflect.TypeFor[DomD]()
	ambiguous := reflect.StructOf([]reflect.StructField{anon(domA), anon(domB)})
	if err := v.Check(bg, reflect.New(ambiguous).Interface()); err != nil {
		t.Fatalf("ambiguous fields must be dropped: %v", err)
	}
	shadow := reflect.StructOf([]reflect.StructField{anon(domA), {Name: "X", Type: reflect.TypeFor[string](), Tag: `json:"x"`}})
	if err := v.Check(bg, reflect.New(shadow).Interface()); err != nil {
		t.Fatalf("outer field must shadow: %v", err)
	}
	tagged := reflect.StructOf([]reflect.StructField{anon(domC), anon(domD)})
	got := fieldsOf(t, v.Check(bg, reflect.New(tagged).Interface()))
	if len(got) != 1 || got[0].Path != "/X" || got[0].Rule != "min" {
		t.Fatalf("tagged field must win: %+v", got)
	}
	// shadow.X and DomB.X are both tagged at depth 2: ambiguous, so both are
	// dropped and the deeper DomA.X does not resurface.
	sameDepth := reflect.StructOf([]reflect.StructField{{Name: "Inner", Type: shadow, Anonymous: true}, anon(domB)})
	if err := v.Check(bg, reflect.New(sameDepth).Interface()); err != nil {
		t.Fatalf("same-depth tagged fields must be dropped: %v", err)
	}
	// A depth-2 field beats a depth-3 field with the same name.
	wrapper := reflect.StructOf([]reflect.StructField{anon(domA)})
	mid := reflect.StructOf([]reflect.StructField{{Name: "X", Type: reflect.TypeFor[string](), Tag: `json:"x" validate:"min=5"`}})
	deep := reflect.StructOf([]reflect.StructField{{Name: "Inner", Type: wrapper, Anonymous: true}, {Name: "Mid", Type: mid, Anonymous: true}})
	got = fieldsOf(t, v.Check(bg, reflect.New(deep).Interface()))
	if len(got) != 1 || got[0].Path != "/x" || got[0].Rule != "min" {
		t.Fatalf("shallower field must win: %+v", got)
	}
}

type embValidator struct {
	Flag bool `json:"flag"`
}

func (e *embValidator) Validate(context.Context) error {
	if e.Flag {
		return (&mediator.ValidationError{}).Add("/flag", "embedded", "embedded says no")
	}
	return nil
}

type embHost struct {
	embValidator
	Name string `json:"name"`
}

type embUnexportedValidator struct {
	embValidatorValue
}

type embNamedValidator struct {
	embValidatorValue `json:"inner"`
}

type embValidatorValue struct{ N int }

func (e embValidatorValue) Validate(context.Context) error {
	if e.N < 0 {
		return errors.New("negative")
	}
	return nil
}

type embPtrValidator struct {
	*embValidator
}

func TestEmbeddedValidate(t *testing.T) {
	v := New()
	got := fieldsOf(t, v.Check(bg, embHost{embValidator: embValidator{Flag: true}}))
	if len(got) != 1 || got[0].Path != "/flag" || got[0].Rule != "embedded" {
		t.Fatalf("got %+v", got)
	}
	if err := v.Check(bg, embHost{}); err != nil {
		t.Fatal(err)
	}
	got = fieldsOf(t, v.Check(bg, embPtrValidator{embValidator: &embValidator{Flag: true}}))
	if len(got) != 1 || got[0].Path != "/flag" {
		t.Fatalf("got %+v", got)
	}
	// An unexported embedded struct contributes its Validate through promotion.
	if err := v.Check(bg, embUnexportedValidator{embValidatorValue{N: -1}}); err == nil || err.Error() != "negative" {
		t.Fatalf("promoted Validate: %v", err)
	}
	if err := v.Check(bg, embUnexportedValidator{}); err != nil {
		t.Fatal(err)
	}
	// With a JSON name the unexported embedded struct is a member reflect
	// cannot hand out, so its Validate is a compile error.
	err := v.Compile(reflect.TypeFor[embNamedValidator]())
	if err == nil || !strings.Contains(err.Error(), "unexported embedded field with a JSON name implements Validate") {
		t.Fatalf("got %v", err)
	}
}

// Validate merging.

type ctxKey struct{}

type vLine struct {
	Qty int `json:"qty" validate:"gte=0"`
}

func (l vLine) Validate(ctx context.Context) error {
	switch l.Qty {
	case 13:
		return (&mediator.ValidationError{}).Add("/qty", "unlucky", "is unlucky")
	case 99:
		return errBoom
	case 7:
		return &mediator.ValidationError{Fields: []mediator.FieldError{{Path: "", Rule: "whole", Message: "whole line rejected"}}}
	case 5:
		if ctx.Value(ctxKey{}) == nil {
			return errors.New("context not passed through")
		}
	}
	return nil
}

var errBoom = errors.New("boom")

type vRoot struct {
	Lines []vLine          `json:"lines"`
	M     map[string]vLine `json:"m"`
	P     *vLine           `json:"p"`
	Tag   string           `json:"tag" validate:"max=3"`
	calls *int
}

func (r *vRoot) Validate(context.Context) error {
	if r.calls != nil {
		*r.calls++
	}
	switch r.Tag {
	case "bad":
		return fmt.Errorf("wrapped: %w", (&mediator.ValidationError{}).Add("/tag", "root", "root says no"))
	case "err":
		return mediator.E(mediator.CodeConflict, "conflict")
	}
	return nil
}

func TestValidateMerging(t *testing.T) {
	v := New()
	err := v.Check(bg, &vRoot{Lines: []vLine{{Qty: 1}, {Qty: 13}}, M: map[string]vLine{"k": {Qty: 13}}, P: &vLine{Qty: 7}, Tag: "bad"})
	got := fieldsOf(t, err)
	want := []mediator.FieldError{
		{Path: "/lines/1/qty", Rule: "unlucky", Message: "is unlucky"},
		{Path: "/m/k/qty", Rule: "unlucky", Message: "is unlucky"},
		{Path: "/p", Rule: "whole", Message: "whole line rejected"},
		{Path: "/tag", Rule: "root", Message: "root says no"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got\n%+v\nwant\n%+v", got, want)
	}
	// Root passed by value with a pointer receiver is copied.
	got = fieldsOf(t, v.Check(bg, vRoot{Tag: "bad"}))
	if len(got) != 1 || got[0].Path != "/tag" {
		t.Fatalf("by value: %+v", got)
	}
	// Any other error is returned as-is, from nested and from root.
	if err := v.Check(bg, vRoot{Lines: []vLine{{Qty: 99}}}); !errors.Is(err, errBoom) {
		t.Fatalf("nested plain error: %v", err)
	}
	if err := v.Check(bg, vRoot{M: map[string]vLine{"a": {Qty: 99}}}); !errors.Is(err, errBoom) {
		t.Fatalf("map plain error: %v", err)
	}
	if err := v.Check(bg, vRoot{P: &vLine{Qty: 99}}); !errors.Is(err, errBoom) {
		t.Fatalf("pointer plain error: %v", err)
	}
	if err := v.Check(bg, vRoot{Tag: "err"}); mediator.CodeOf(err) != mediator.CodeConflict {
		t.Fatalf("root plain error: %v", err)
	}
	// Validate is not called when tag rules fail.
	calls := 0
	err = v.Check(bg, &vRoot{Tag: "toolong", calls: &calls, Lines: []vLine{{Qty: -1}}})
	if got := fieldsOf(t, err); len(got) != 2 || calls != 0 {
		t.Fatalf("tag failures must skip Validate: %+v calls=%d", got, calls)
	}
	// The context reaches nested Validate methods.
	ctx := context.WithValue(bg, ctxKey{}, true)
	if err := v.Check(ctx, vRoot{Lines: []vLine{{Qty: 5}}}); err != nil {
		t.Fatalf("context: %v", err)
	}
	if err := v.Check(bg, vRoot{Lines: []vLine{{Qty: 5}}}); err == nil {
		t.Fatal("missing context value should fail")
	}
	if err := v.Check(bg, vRoot{Lines: []vLine{{Qty: 1}}, Tag: "ok"}); err != nil {
		t.Fatalf("valid: %v", err)
	}
}

// Recursive types.

type node struct {
	Name     string  `json:"name" validate:"required"`
	Children []*node `json:"children"`
}

func (n *node) Validate(context.Context) error {
	if n.Name == "loop" {
		return (&mediator.ValidationError{}).Add("/name", "loop", "no loops")
	}
	return nil
}

type mutualA struct {
	B *mutualB `json:"b"`
}

func (a mutualA) Validate(context.Context) error {
	if a.B == nil {
		return (&mediator.ValidationError{}).Add("/b", "missing", "b is missing")
	}
	return nil
}

type mutualB struct {
	A *mutualA `json:"a"`
}

type quietA struct {
	B *quietB `json:"b"`
}

type quietB struct {
	A *quietA `json:"a"`
}

func TestRecursiveTypes(t *testing.T) {
	v := New()
	err := v.Check(bg, &node{Name: "root", Children: []*node{{Name: "a", Children: []*node{nil, {Name: "loop"}}}, {Name: ""}}})
	got := fieldsOf(t, err)
	if len(got) != 1 || got[0].Path != "/children/1/name" || got[0].Rule != "required" {
		t.Fatalf("tag phase: %+v", got)
	}
	err = v.Check(bg, &node{Name: "root", Children: []*node{{Name: "a", Children: []*node{nil, {Name: "loop"}}}}})
	got = fieldsOf(t, err)
	if len(got) != 1 || got[0].Path != "/children/0/children/1/name" || got[0].Rule != "loop" {
		t.Fatalf("validate phase: %+v", got)
	}
	// Mutual recursion: compiling B first still finds A's Validate below it.
	if err := v.Compile(reflect.TypeFor[mutualB]()); err != nil {
		t.Fatal(err)
	}
	got = fieldsOf(t, v.Check(bg, mutualB{A: &mutualA{}}))
	if len(got) != 1 || got[0].Path != "/a/b" {
		t.Fatalf("mutual: %+v", got)
	}
	if err := v.Check(bg, mutualB{A: &mutualA{B: &mutualB{}}}); err != nil {
		t.Fatal(err)
	}
	if err := v.Check(bg, quietB{A: &quietA{B: &quietB{}}}); err != nil {
		t.Fatal(err)
	}
}

func TestCheckErrors(t *testing.T) {
	v := New()
	if err := v.Check(bg, nil); !errors.Is(err, ErrNilValue) {
		t.Fatalf("nil: %v", err)
	}
	if err := v.Check(bg, (*inner)(nil)); !errors.Is(err, ErrNilValue) {
		t.Fatalf("nil pointer: %v", err)
	}
	if err := v.Check(bg, 42); err == nil || err.Error() != "validate: int is not a struct" {
		t.Fatalf("non-struct: %v", err)
	}
	type bad struct {
		X string `validate:"nope"`
	}
	if err := v.Check(bg, bad{}); err == nil || !strings.Contains(err.Error(), `unknown rule "nope"`) {
		t.Fatalf("compile error through Check: %v", err)
	}
	if err := v.Compile(nil); !errors.Is(err, ErrNilValue) {
		t.Fatalf("Compile(nil): %v", err)
	}
	if err := v.Compile(reflect.TypeFor[[]inner]()); err == nil || err.Error() != "validate: []validate.inner is not a struct" {
		t.Fatalf("Compile(slice): %v", err)
	}
	if err := v.Compile(reflect.TypeFor[*inner]()); err != nil {
		t.Fatal(err)
	}
	if err := v.Compile(reflect.TypeFor[inner]()); err != nil {
		t.Fatal(err)
	}
	// A struct with nothing to check returns nil without work.
	type empty struct{ A int }
	if err := v.Check(bg, empty{}); err != nil {
		t.Fatal(err)
	}
}

// TestPlanFlags: hasCheck and hasValidate are exact. Check skips the field
// walk without hasCheck and the Validate pass without hasValidate, and finish
// (which propagates hasValidate up from nested plans) must not mark a plan
// that has no Validate at or below it.
func TestPlanFlags(t *testing.T) {
	type bare struct{ A int }
	type rulesOnly struct {
		In inner `json:"in"`
	}
	type withValidate struct {
		L vLine `json:"l"`
	}
	v := New()
	for _, tc := range []struct {
		typ                   reflect.Type
		hasCheck, hasValidate bool
	}{
		{reflect.TypeFor[bare](), false, false},
		{reflect.TypeFor[rulesOnly](), true, false},
		{reflect.TypeFor[withValidate](), true, true},
	} {
		p, err := v.plan(tc.typ)
		if err != nil {
			t.Fatal(err)
		}
		if p.hasCheck != tc.hasCheck || p.hasValidate != tc.hasValidate {
			t.Errorf("%s: hasCheck=%v hasValidate=%v, want %v %v", tc.typ, p.hasCheck, p.hasValidate, tc.hasCheck, tc.hasValidate)
		}
	}
}

func TestPlanCaching(t *testing.T) {
	v := New()
	if err := v.Compile(reflect.TypeFor[inner]()); err != nil {
		t.Fatal(err)
	}
	p1, _ := v.plan(reflect.TypeFor[inner]())
	p2, _ := v.plan(reflect.TypeFor[inner]())
	if p1 != p2 {
		t.Fatal("plans are not cached")
	}
	n := len(*v.plans.Load())
	if err := v.Check(bg, embRoot{}); err == nil {
		t.Fatal("expected failures")
	}
	if len(*v.plans.Load()) <= n {
		t.Fatal("lazy compile did not cache the new plans")
	}
	// A failed compile publishes nothing.
	type bad struct {
		X string `validate:"nope"`
	}
	m := len(*v.plans.Load())
	if err := v.Compile(reflect.TypeFor[bad]()); err == nil {
		t.Fatal("expected error")
	}
	if len(*v.plans.Load()) != m {
		t.Fatal("failed compile must not publish plans")
	}
}

func TestOptions(t *testing.T) {
	type keyed struct {
		A string `json:"a" rules:"required"`
		B string `json:"b" validate:"required"`
	}
	v := New(WithTagKey("rules"))
	got := fieldsOf(t, v.Check(bg, keyed{}))
	if len(got) != 1 || got[0].Path != "/a" {
		t.Fatalf("tag key: %+v", got)
	}
	type many struct {
		Items []string `json:"items" validate:"dive,min=1"`
	}
	v = New(WithMaxErrors(2))
	got = fieldsOf(t, v.Check(bg, many{Items: []string{"", "", "", ""}}))
	if len(got) != 2 {
		t.Fatalf("max errors: %+v", got)
	}
	// The cap also applies to merged Validate results.
	got = fieldsOf(t, v.Check(bg, vRoot{Lines: []vLine{{Qty: 13}, {Qty: 13}, {Qty: 13}}}))
	if len(got) != 2 {
		t.Fatalf("max errors on Validate: %+v", got)
	}
}

func TestConcurrentCheck(t *testing.T) {
	type line struct {
		SKU string `json:"sku" validate:"required,len=3"`
		Qty int    `json:"qty" validate:"min=1,max=10"`
	}
	type order struct {
		Lines []line         `json:"lines" validate:"required,min=1,dive"`
		Meta  map[string]int `json:"meta" validate:"dive,gte=0"`
	}
	v := New()
	if err := v.Compile(reflect.TypeFor[order]()); err != nil {
		t.Fatal(err)
	}
	good := order{Lines: []line{{SKU: "abc", Qty: 1}}, Meta: map[string]int{"a": 0}}
	bad := order{Lines: []line{{SKU: "ab", Qty: 0}, {SKU: "abc", Qty: 11}}, Meta: map[string]int{"b": -1, "a": -2}}
	var wg sync.WaitGroup
	fail := make(chan string, 1024)
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				if err := v.Check(bg, &good); err != nil {
					fail <- "good: " + err.Error()
					return
				}
				err := v.Check(bg, bad)
				var ve *mediator.ValidationError
				if !errors.As(err, &ve) || len(ve.Fields) != 5 || ve.Fields[0].Path != "/lines/0/sku" || ve.Fields[3].Path != "/meta/a" {
					fail <- fmt.Sprintf("bad: %v", err)
					return
				}
			}
		}()
	}
	// Lazy compilation from many goroutines at once.
	type lazy struct {
		N int `json:"n" validate:"min=1"`
	}
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := v.Check(bg, lazy{}); err == nil {
				fail <- "lazy: expected error"
			}
		}()
	}
	wg.Wait()
	close(fail)
	for f := range fail {
		t.Error(f)
	}
}

func TestHelpers(t *testing.T) {
	if escapePointer("a/b~c") != "a~1b~0c" || escapePointer("plain") != "plain" {
		t.Fatal("escapePointer")
	}
	if plural(1, "item", "items") != "1 item" || plural(2, "entry", "entries") != "2 entries" {
		t.Fatal("plural")
	}
	if boolCompare(true, true) != 0 || boolCompare(true, false) != 1 || boolCompare(false, true) != -1 {
		t.Fatal("boolCompare")
	}
	if typeName(reflect.TypeFor[inner]()) != "inner" || typeName(reflect.TypeFor[struct{ A int }]()) != "struct { A int }" {
		t.Fatal("typeName")
	}
	cc := &checkCtx{}
	if cc.pointer() != "" {
		t.Fatal("root pointer must be empty")
	}
}
