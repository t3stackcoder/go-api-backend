package validate

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"pgregory.net/rapid"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// The property types together use every rule of the grammar.

type propColor string

type propStrings struct {
	Req    string    `json:"req" validate:"required"`
	ReqE   string    `json:"req_e" validate:"required,allowempty"`
	MinMax string    `json:"min_max" validate:"min=2,max=4"`
	Len    string    `json:"len" validate:"len=3"`
	Pat    string    `json:"pat" validate:"pattern=^[a-c]{1,3}$"`
	One    propColor `json:"one" validate:"oneof=red green blue"`
	Email  string    `json:"email" validate:"email"`
	UUID   string    `json:"uuid" validate:"uuid"`
	URL    string    `json:"url" validate:"url"`
	DT     string    `json:"dt" validate:"datetime"`
	IPv4   string    `json:"ipv4" validate:"ipv4"`
	IPv6   string    `json:"ipv6" validate:"ipv6"`
	Host   string    `json:"host" validate:"hostname"`
	OptPtr *string   `json:"opt_ptr" validate:"max=2"`
	ReqPtr *string   `json:"req_ptr" validate:"required"`
	OneP   *string   `json:"one_p" validate:"oneof=x y"`
}

type propNumbers struct {
	I   int       `json:"i" validate:"min=1,max=10"`
	I8  int8      `json:"i8" validate:"gt=-2,lt=2"`
	U   uint16    `json:"u" validate:"gte=3,lte=5"`
	F   float64   `json:"f" validate:"gte=0,lt=1.5"`
	One int       `json:"one" validate:"oneof=1 2 3"`
	P   *int      `json:"p" validate:"required,min=0"`
	OP  *float64  `json:"op" validate:"max=1"`
	B   bool      `json:"b" validate:"required"`
	T   time.Time `json:"t"`
	ID  uuid.UUID `json:"id" validate:"required"`
}

type propLine struct {
	SKU string `json:"sku" validate:"required,len=3"`
	Qty int    `json:"qty" validate:"gte=0,lte=5"`
}

type propEmbed struct {
	E string `json:"e" validate:"required,max=3"`
}

type propColl struct {
	Tags  []string          `json:"tags" validate:"required,min=1,max=3,unique,dive,min=1"`
	Opt   []int             `json:"opt" validate:"max=2,dive,gte=0"`
	Fixed []int             `json:"fixed" validate:"len=2"`
	M     map[string]int    `json:"m" validate:"min=1,max=3,dive,gt=0"`
	RM    map[string]string `json:"rm" validate:"required"`
	Lines []propLine        `json:"lines" validate:"dive"`
	Ptrs  []*propLine       `json:"ptrs" validate:"dive,required"`
	Sub   propLine          `json:"sub"`
	PSub  *propLine         `json:"psub"`
	Raw   []byte            `json:"raw"`
	RReq  []byte            `json:"rreq" validate:"required"`
	Any   any               `json:"any"`
	propEmbed
}

// Generators. Each takes valid=true to draw a value that satisfies the rules
// and otherwise draws from a mix biased to the boundaries.

var (
	lower = rapid.RuneFrom([]rune("abcdefghijklmnopqrstuvwxyz"))
	abc   = rapid.RuneFrom([]rune("abc"))
	anyR  = rapid.Rune()
)

func word(t *rapid.T, valid bool, runes *rapid.Generator[rune], lo, hi int) string {
	if valid {
		return rapid.StringOfN(runes, lo, hi, -1).Draw(t, "word")
	}
	return rapid.OneOf(
		rapid.Just(""),
		rapid.StringOfN(runes, max(lo-1, 0), hi+1, -1),
		rapid.StringOfN(anyR, 0, hi+2, -1),
		rapid.Just("日本語"),
	).Draw(t, "word")
}

func pick(t *rapid.T, valid bool, good, bad []string) string {
	if valid || rapid.Float64Range(0, 1).Draw(t, "p") < 0.7 {
		return rapid.SampledFrom(good).Draw(t, "good")
	}
	return rapid.OneOf(rapid.SampledFrom(bad), rapid.StringOfN(anyR, 0, 12, -1)).Draw(t, "bad")
}

func maybePtr[T any](t *rapid.T, valid bool, v T) *T {
	if !valid && rapid.Bool().Draw(t, "nil") {
		return nil
	}
	return &v
}

func genStrings(t *rapid.T, valid bool) propStrings {
	s := propStrings{
		Req:    word(t, valid, lower, 1, 5),
		ReqE:   word(t, true, lower, 0, 3),
		MinMax: word(t, valid, lower, 2, 4),
		Len:    word(t, valid, lower, 3, 3),
		Pat:    word(t, valid, abc, 1, 3),
		One:    propColor(pick(t, valid, []string{"red", "green", "blue"}, []string{"", "RED", "yellow"})),
		Email:  pick(t, valid, []string{"a@b.com", "x.y+z@example.org", "u@[10.0.0.1]"}, []string{"", "nope", "a@", "@b.com", "a b@c.d", ".a@b.com"}),
		UUID:   pick(t, valid, []string{uuid.New().String(), "00000000-0000-0000-0000-000000000000"}, []string{"", "x", "123e4567e89b12d3a456426614174000"}),
		URL:    pick(t, valid, []string{"https://example.com/p?q=1", "mailto:a@b.com", "http://[::1]/"}, []string{"", "/rel", "example.com", "://x"}),
		DT:     pick(t, valid, []string{"2024-01-02T03:04:05Z", "2024-06-30T23:59:59.5+02:00", time.Unix(rapid.Int64Range(0, 4e9).Draw(t, "unix"), 0).UTC().Format(time.RFC3339)}, []string{"", "2024-01-02", "2024-13-01T00:00:00Z", "yesterday"}),
		IPv4:   pick(t, valid, []string{"1.2.3.4", "0.0.0.0", "255.255.255.255"}, []string{"", "1.2.3", "256.1.1.1", "01.2.3.4", "::1"}),
		IPv6:   pick(t, valid, []string{"::1", "2001:db8::1", "::ffff:1.2.3.4"}, []string{"", "1.2.3.4", "::g", "fe80::1%eth0"}),
		Host:   pick(t, valid, []string{"localhost", "example.com", "a-b.c.d."}, []string{"", "-a.com", "a_b", "a..b", "a b"}),
		OptPtr: maybePtr(t, false, word(t, valid, lower, 0, 2)),
		ReqPtr: maybePtr(t, valid, word(t, valid, lower, 1, 3)),
		OneP:   maybePtr(t, false, pick(t, valid, []string{"x", "y"}, []string{"", "z"})),
	}
	return s
}

func genNumbers(t *rapid.T, valid bool) propNumbers {
	n := propNumbers{
		B:  rapid.Bool().Draw(t, "b"),
		T:  time.Unix(rapid.Int64Range(0, 4e9).Draw(t, "t"), 0).UTC(),
		ID: uuid.UUID(rapid.SliceOfN(rapid.Byte(), 16, 16).Draw(t, "id")),
	}
	if valid {
		n.I = rapid.IntRange(1, 10).Draw(t, "i")
		n.I8 = rapid.Int8Range(-1, 1).Draw(t, "i8")
		n.U = rapid.Uint16Range(3, 5).Draw(t, "u")
		n.F = rapid.Float64Range(0, 1.4999).Draw(t, "f")
		n.One = rapid.SampledFrom([]int{1, 2, 3}).Draw(t, "one")
		n.P = ptr(rapid.IntRange(0, 100).Draw(t, "p"))
		n.OP = maybePtr(t, false, rapid.Float64Range(-5, 1).Draw(t, "op"))
		return n
	}
	n.I = rapid.IntRange(-1, 12).Draw(t, "i")
	n.I8 = rapid.Int8Range(-3, 3).Draw(t, "i8")
	n.U = rapid.Uint16Range(0, 7).Draw(t, "u")
	n.F = rapid.Float64Range(-1, 3).Draw(t, "f")
	n.One = rapid.IntRange(0, 4).Draw(t, "one")
	n.P = maybePtr(t, false, rapid.IntRange(-2, 2).Draw(t, "p"))
	n.OP = maybePtr(t, false, rapid.Float64Range(0.5, 1.5).Draw(t, "op"))
	return n
}

func genLine(t *rapid.T, valid bool) propLine {
	if valid {
		return propLine{SKU: word(t, true, lower, 3, 3), Qty: rapid.IntRange(0, 5).Draw(t, "qty")}
	}
	return propLine{SKU: word(t, false, lower, 3, 3), Qty: rapid.IntRange(-1, 6).Draw(t, "qty")}
}

func genColl(t *rapid.T, valid bool) propColl {
	c := propColl{
		Any:  rapid.OneOf(rapid.Just[any](nil), rapid.Just[any]("s"), rapid.Just[any](1.5), rapid.Just[any](map[string]any{"k": []any{true}})).Draw(t, "any"),
		Raw:  rapid.OneOf(rapid.Just([]byte(nil)), rapid.Just([]byte{}), rapid.SliceOfN(rapid.Byte(), 1, 4)).Draw(t, "raw"),
		RReq: rapid.SliceOfN(rapid.Byte(), 0, 4).Draw(t, "rreq"),
		Sub:  genLine(t, valid),
	}
	c.E = word(t, valid, lower, 1, 3)
	if !valid && rapid.Bool().Draw(t, "rreq nil") {
		c.RReq = nil
	}
	lines := func(lo, hi int) []propLine {
		n := rapid.IntRange(lo, hi).Draw(t, "n")
		out := make([]propLine, 0, n)
		for i := 0; i < n; i++ {
			out = append(out, genLine(t, valid))
		}
		if n == 0 && rapid.Bool().Draw(t, "nil lines") {
			return nil
		}
		return out
	}
	c.Lines = lines(0, 3)
	for _, l := range lines(0, 2) {
		l := l
		c.Ptrs = append(c.Ptrs, &l)
	}
	if !valid && rapid.Bool().Draw(t, "nil ptr elem") {
		c.Ptrs = append(c.Ptrs, nil)
	}
	if rapid.Bool().Draw(t, "psub") {
		l := genLine(t, valid)
		c.PSub = &l
	}
	if valid {
		c.Tags = rapid.SliceOfNDistinct(rapid.StringOfN(lower, 1, 4, -1), 1, 3, rapid.ID[string]).Draw(t, "tags")
		c.Opt = rapid.OneOf(rapid.Just([]int(nil)), rapid.SliceOfN(rapid.IntRange(0, 9), 0, 2)).Draw(t, "opt")
		c.Fixed = rapid.OneOf(rapid.Just([]int(nil)), rapid.SliceOfN(rapid.Int(), 2, 2)).Draw(t, "fixed")
		c.M = rapid.OneOf(rapid.Just(map[string]int(nil)), rapid.MapOfN(rapid.String(), rapid.IntRange(1, 9), 1, 3)).Draw(t, "m")
		c.RM = rapid.MapOfN(rapid.String(), rapid.String(), 0, 2).Draw(t, "rm")
		return c
	}
	c.Tags = rapid.OneOf(rapid.Just([]string(nil)), rapid.SliceOfN(rapid.StringOfN(lower, 0, 2, -1), 0, 4)).Draw(t, "tags")
	c.Opt = rapid.SliceOfN(rapid.IntRange(-1, 2), 0, 3).Draw(t, "opt")
	c.Fixed = rapid.SliceOfN(rapid.Int(), 0, 3).Draw(t, "fixed")
	c.M = rapid.MapOfN(rapid.StringOfN(lower, 0, 2, -1), rapid.IntRange(-1, 2), 0, 4).Draw(t, "m")
	c.RM = rapid.OneOf(rapid.Just(map[string]string(nil)), rapid.MapOfN(rapid.String(), rapid.String(), 0, 2)).Draw(t, "rm")
	return c
}

// compileStandalone builds a draft 2020-12 document for typ with Defs inlined
// under $defs and compiles it with format assertions on.
func compileStandalone(t *testing.T, v *Validator, typ reflect.Type) *jsonschema.Schema {
	t.Helper()
	reg := &Schemas{}
	root, err := v.SchemaFor(typ, reg, SchemaOptions{RefPrefix: "#/$defs/"})
	if err != nil {
		t.Fatal(err)
	}
	doc := &Schema{Schema: "https://json-schema.org/draft/2020-12/schema", Ref: root.Ref, Defs: reg.Defs}
	b, err := json.Marshal(doc, json.Deterministic(true))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource("schema.json", raw); err != nil {
		t.Fatal(err)
	}
	sch, err := c.Compile("schema.json")
	if err != nil {
		t.Fatalf("%v\n%s", err, b)
	}
	return sch
}

// PropSchemaAgreement (spec 11.3): for any generated value, Check succeeds
// iff the generated JSON Schema accepts its JSON encoding.
func TestPropSchemaAgreement(t *testing.T) {
	v := New()
	cases := []struct {
		name string
		typ  reflect.Type
		gen  func(*rapid.T, bool) any
	}{
		{"strings", reflect.TypeFor[propStrings](), func(t *rapid.T, ok bool) any { return genStrings(t, ok) }},
		{"numbers", reflect.TypeFor[propNumbers](), func(t *rapid.T, ok bool) any { return genNumbers(t, ok) }},
		{"collections", reflect.TypeFor[propColl](), func(t *rapid.T, ok bool) any { return genColl(t, ok) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sch := compileStandalone(t, v, tc.typ)
			valid, invalid := 0, 0
			rapid.Check(t, func(rt *rapid.T) {
				val := tc.gen(rt, rapid.Bool().Draw(rt, "valid"))
				b, err := json.Marshal(val, json.FormatNilSliceAsNull(true), json.FormatNilMapAsNull(true))
				if err != nil {
					rt.Fatalf("marshal: %v", err)
				}
				inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
				if err != nil {
					rt.Fatalf("instance: %v", err)
				}
				checkErr := v.Check(bg, val)
				schemaErr := sch.Validate(inst)
				if (checkErr == nil) != (schemaErr == nil) {
					rt.Fatalf("disagreement on %s\nCheck:  %v\nSchema: %v", b, checkErr, schemaErr)
				}
				if checkErr != nil {
					var ve *mediator.ValidationError
					if !isValidationError(checkErr, &ve) {
						rt.Fatalf("unexpected error type %T: %v", checkErr, checkErr)
					}
					invalid++
				} else {
					valid++
				}
			})
			t.Logf("%s: %d valid, %d invalid values", tc.name, valid, invalid)
			if valid == 0 || invalid == 0 {
				t.Fatalf("generator is not mixed: %d valid, %d invalid", valid, invalid)
			}
		})
	}
}

func isValidationError(err error, target **mediator.ValidationError) bool {
	ve, ok := err.(*mediator.ValidationError)
	if ok {
		*target = ve
	}
	return ok
}

// Example documents the shape of a generated schema.
func Example() {
	v := New()
	reg := &Schemas{}
	ref, _ := v.SchemaFor(reflect.TypeFor[Line](), reg, SchemaOptions{})
	fmt.Println(ref.Ref, reg.Names())
	// Output: #/components/schemas/Line [Line]
}
