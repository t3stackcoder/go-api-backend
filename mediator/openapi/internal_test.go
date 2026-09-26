package openapi

import (
	"errors"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

func TestCamelCase(t *testing.T) {
	for in, want := range map[string]string{
		"CreateOrder":          "createOrder",
		"getOrder":             "getOrder",
		"orders.v1.export_csv": "ordersV1ExportCsv",
		"a..b__c":              "aBC",
		"X":                    "x",
		"HTTPFetch":            "hTTPFetch",
		"Ünïcode.ßx":           "ünïcodeßx", // ß has no single-rune upper case
		"":                     "",
	} {
		if got := camelCase(in); got != want {
			t.Errorf("camelCase(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestOpenAPIPath(t *testing.T) {
	for in, want := range map[string]string{
		"/orders":                "/orders",
		"/orders/{$}":            "/orders/",
		"/api/{$}":               "/api/",
		"/files/{path...}":       "/files/{path}",
		"/a/{x}/b/{rest...}/{$}": "/a/{x}/b/{rest}/",
	} {
		if got := openAPIPath(in); got != want {
			t.Errorf("openAPIPath(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDefaultTag(t *testing.T) {
	g := &generator{prefix: "/api"}
	for in, want := range map[string]string{
		"/api/orders":      "orders",
		"/api/orders/{id}": "orders",
		"/api/":            "default",
		"/api/{id}":        "default",
		"/api/rpc/X":       "rpc",
	} {
		if got := g.defaultTag(in); got != want {
			t.Errorf("defaultTag(%q) = %q, want %q", in, got, want)
		}
	}
	g.prefix = ""
	if got := g.defaultTag("/orders"); got != "orders" {
		t.Errorf("no prefix: %q", got)
	}
}

func TestUniqueID(t *testing.T) {
	g := &generator{ids: map[string]bool{}}
	var got []string
	for range 3 {
		got = append(got, g.uniqueID("x"))
	}
	if strings.Join(got, ",") != "x,x2,x3" {
		t.Errorf("ids %v", got)
	}
	if id := g.uniqueID("x2"); id != "x22" {
		t.Errorf("x2 -> %q", id)
	}
}

func TestCodeSet(t *testing.T) {
	s := &codeSet{}
	s.add(mediator.CodeValidation, mediator.CodeIdempotencyMismatch, mediator.CodeValidation, mediator.CodeNotFound)
	if got := s.statuses(); len(got) != 2 || got[0] != 404 || got[1] != 422 {
		t.Errorf("statuses %v", got)
	}
	if got := s.byStatus[422]; len(got) != 2 || got[0] != mediator.CodeValidation || got[1] != mediator.CodeIdempotencyMismatch {
		t.Errorf("422 codes %v", got)
	}
}

func TestMatchesETag(t *testing.T) {
	const tag = `"abc"`
	for in, want := range map[string]bool{
		tag:              true,
		"W/" + tag:       true,
		`"x", ` + tag:    true,
		"*":              true,
		"":               false,
		`"x"`:            false,
		"abc":            false,
		`W/"x",W/"abc" `: true,
	} {
		if got := matchesETag(in, tag); got != want {
			t.Errorf("matchesETag(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestCompileOAS(t *testing.T) {
	if _, err := compileOAS([]byte("{")); err == nil || !strings.Contains(err.Error(), "embedded OpenAPI schema") {
		t.Errorf("not JSON: %v", err)
	}
	if _, err := compileOAS([]byte(`{"$schema": "https://json-schema.org/draft/2020-12/schema", "type": 5}`)); err == nil {
		t.Error("invalid schema compiled")
	}
	if _, err := (memoryLoader{}).Load("https://example.com/x"); err == nil || !strings.Contains(err.Error(), "not embedded") {
		t.Errorf("unknown url: %v", err)
	}
	if s, err := oasSchema(); err != nil || s == nil {
		t.Fatalf("embedded schema: %v", err)
	}
}

func TestValidatePropagatesCompileError(t *testing.T) {
	saved := oasSchema
	defer func() { oasSchema = saved }()
	oasSchema = func() (*jsonschema.Schema, error) { return nil, errors.New("boom") }
	if err := ValidateJSON([]byte("{}")); err == nil || err.Error() != "boom" {
		t.Errorf("err = %v", err)
	}
}

func TestKnownCodesComplete(t *testing.T) {
	// Every code has a distinct string and a non-500 status except internal;
	// a code not in the list would be rejected by Describe().Errors.
	seen := map[mediator.Code]bool{}
	for _, c := range knownCodes {
		if seen[c] {
			t.Errorf("duplicate %s", c)
		}
		seen[c] = true
		if mediator.StatusOf(c) == 500 && c != mediator.CodeInternal {
			t.Errorf("%s maps to 500", c)
		}
	}
	if !seen[mediator.CodeInternal] || len(seen) != 16 {
		t.Errorf("%d codes", len(seen))
	}
}
