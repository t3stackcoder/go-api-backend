package mediator_test

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

func TestCanonicalizeJSON_Valid(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"null", "null", "null"},
		{"true", " true ", "true"},
		{"false", "false", "false"},
		{"string", `"aA\n"`, `"aA\n"`},
		{"unicode string", `"héllo ☃"`, `"héllo ☃"`},
		{"escaped unicode", `"☃"`, `"☃"`},
		{"integer", "42", "42"},
		{"negative integer", "-42", "-42"},
		{"negative zero", "-0", "0"},
		{"negative zero fraction", "-0.0", "0"},
		{"zero exponent", "0e5", "0"},
		{"big integer verbatim", "123456789012345678901234567890", "123456789012345678901234567890"},
		{"fraction", "1.50", "1.5"},
		{"exponent", "1.5e0", "1.5"},
		{"exponent capital", "1.5E0", "1.5"},
		{"integral fraction", "100.0", "100"},
		{"integral exponent", "1e2", "100"},
		{"million with fraction", "1000000.0", "1000000"},
		{"million exponent", "1e6", "1000000"},
		{"large integral", "1e20", "100000000000000000000"},
		{"huge float", "1e21", "1e21"},
		{"huge float plus", "1E+21", "1e21"},
		{"tiny float", "0.00001", "1e-05"},
		{"small negative", "-2.5e-3", "-0.0025"},
		{"out of range float kept verbatim", "1e99999", "1e99999"},
		{"empty object", "{}", "{}"},
		{"empty array", "[ ]", "[]"},
		{"sorted keys", `{"b":1,"a":2}`, `{"a":2,"b":1}`},
		{"nested", `{"z": {"y": [1.0, {"c": null, "b": false}], "x": "s"}, "a": []}`, `{"a":[],"z":{"x":"s","y":[1,{"b":false,"c":null}]}}`},
		{"whitespace", "\n{\t\"a\" :\r [ 1 , 2 ] }\n", `{"a":[1,2]}`},
		{"key order is by byte", `{"b":1,"B":2,"a":3,"A":4}`, `{"A":4,"B":2,"a":3,"b":1}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := mediator.CanonicalizeJSON([]byte(c.in))
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != c.want {
				t.Fatalf("got %s, want %s", got, c.want)
			}
			again, err := mediator.CanonicalizeJSON(got)
			if err != nil || string(again) != c.want {
				t.Fatalf("not idempotent: %s %v", again, err)
			}
		})
	}
}

func TestCanonicalizeJSON_Invalid(t *testing.T) {
	cases := []struct {
		name string
		in   string
		text string
	}{
		{"empty", "", "EOF"},
		{"whitespace only", "  ", "EOF"},
		{"truncated object", "{", ""},
		{"truncated after key", `{"a"`, ""},
		{"truncated after colon", `{"a":`, ""},
		{"truncated after member", `{"a":1,`, ""},
		{"truncated array", "[", ""},
		{"truncated array element", "[1", ""},
		{"truncated after element", "[1,", ""},
		{"trailing value", "{} {}", "trailing data"},
		{"trailing garbage", "{} x", ""},
		{"trailing comma garbage", "1 ,", ""},
		{"bare close", "}", ""},
		{"bare bracket", "]", ""},
		{"literal typo", "nul", ""},
		{"duplicate member", `{"a":1,"a":2}`, `duplicate object member name "a"`},
		{"duplicate member nested", `{"x":{"b":1,"c":2,"b":3}}`, `duplicate object member name "b"`},
		{"duplicate member spaced", `{"a":1, "b":2, "a":3}`, `duplicate object member name "a"`},
		{"invalid utf-8", "\"\xff\"", ""},
		{"unquoted key", `{a:1}`, ""},
		{"single quotes", `'a'`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := mediator.CanonicalizeJSON([]byte(c.in))
			if err == nil {
				t.Fatalf("want error, got %s", got)
			}
			if c.text != "" && !strings.Contains(err.Error(), c.text) {
				t.Fatalf("error %q lacks %q", err, c.text)
			}
		})
	}
}

func TestCanonicalJSONAndHash(t *testing.T) {
	type order struct {
		ID    string         `json:"id"`
		Lines []int          `json:"lines"`
		Meta  map[string]any `json:"meta"`
		Price float64        `json:"price"`
	}
	v := order{ID: "o1", Lines: []int{1, 2}, Meta: map[string]any{"z": 1, "a": "x", "m": map[string]any{"k": 1e6}}, Price: 100.0}
	b, err := mediator.CanonicalJSON(v)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"o1","lines":[1,2],"meta":{"a":"x","m":{"k":1000000},"z":1},"price":100}`
	if string(b) != want {
		t.Fatalf("got %s", b)
	}
	h, err := mediator.CanonicalHash(v)
	if err != nil || h != sha256.Sum256([]byte(want)) {
		t.Fatal("hash", err)
	}
	// Nil slices and maps encode as empty; zero value is stable.
	b, err = mediator.CanonicalJSON(order{})
	if err != nil || string(b) != `{"id":"","lines":[],"meta":{},"price":0}` {
		t.Fatalf("zero: %s %v", b, err)
	}
	if _, err := mediator.CanonicalJSON(make(chan int)); err == nil {
		t.Fatal("chan must fail")
	}
	if _, err := mediator.CanonicalHash(func() {}); err == nil {
		t.Fatal("func must fail")
	}
	if b, err := mediator.CanonicalJSON(nil); err != nil || string(b) != "null" {
		t.Fatalf("nil: %s %v", b, err)
	}
}
