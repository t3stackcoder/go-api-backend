package openapi_test

import (
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

func TestValidate(t *testing.T) {
	doc := generate(t, fixtureConfig())
	if err := openapi.Validate(doc); err != nil {
		t.Fatal(err)
	}
	if err := openapi.Validate(nil); err == nil || !strings.Contains(err.Error(), "nil document") {
		t.Errorf("nil: %v", err)
	}

	t.Run("wrong version", func(t *testing.T) {
		d := generate(t, fixtureConfig())
		d.OpenAPI = "2.0"
		if err := openapi.Validate(d); err == nil || !strings.Contains(err.Error(), "not valid OpenAPI 3.1") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("bad enum", func(t *testing.T) {
		d := generate(t, fixtureConfig())
		post := d.Paths["/api/orders"].Post
		post.Parameters = append(post.Parameters, &openapi.Parameter{Name: "x", In: "body", Schema: &validate.Schema{}})
		if err := openapi.Validate(d); err == nil || !strings.Contains(err.Error(), "parameters") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("bad format", func(t *testing.T) {
		d := generate(t, fixtureConfig())
		d.Info.Contact.Email = "not an email"
		if err := openapi.Validate(d); err == nil || !strings.Contains(err.Error(), "email") {
			t.Errorf("err = %v", err)
		}
	})
	t.Run("dangling ref", func(t *testing.T) {
		d := generate(t, fixtureConfig())
		d.Components.Schemas["Bad"] = &validate.Schema{Ref: "#/components/schemas/Missing"}
		d.Components.Schemas["Bad2"] = &validate.Schema{Type: validate.Type{"array"}, Items: &validate.Schema{Ref: "#/paths/~1nope/get"}}
		err := openapi.Validate(d)
		if err == nil {
			t.Fatal("no error")
		}
		for _, want := range []string{`/components/schemas/Bad: $ref "#/components/schemas/Missing" does not resolve`, `/components/schemas/Bad2/items: $ref "#/paths/~1nope/get" does not resolve`} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error lacks %q:\n%v", want, err)
			}
		}
	})
	t.Run("marshal failure", func(t *testing.T) {
		d := generate(t, fixtureConfig())
		d.Components.Schemas["Bad"] = &validate.Schema{Extra: map[string]any{"x": func() {}}}
		if err := openapi.Validate(d); err == nil || !strings.Contains(err.Error(), "marshal document") {
			t.Errorf("err = %v", err)
		}
	})
}

func TestValidateJSON(t *testing.T) {
	if err := openapi.ValidateJSON([]byte("{")); err == nil || !strings.Contains(err.Error(), "not JSON") {
		t.Errorf("err = %v", err)
	}
	// Local refs resolve through arrays, escaped tokens, and every kind of
	// JSON value; misses of every kind are reported.
	const doc = `{
	  "openapi": "3.1.0",
	  "info": {"title": "t", "version": "1"},
	  "servers": [{"url": "/"}],
	  "paths": {"/a~b": {"get": {"responses": {"200": {"description": "ok"}}}}},
	  "components": {"schemas": {
	    "A": {"$ref": "#/servers/0"},
	    "B": {"$ref": "#/paths/~1a~0b/get/responses/200"},
	    "C": {"$ref": "#/info/title"},
	    "D": {"$ref": "#/servers/1"},
	    "E": {"$ref": "#/servers/x"},
	    "F": {"$ref": "#/info/title/x"},
	    "G": {"$ref": "#/nope"},
	    "H": {"$ref": "https://example.com/external"},
	    "I": {"$ref": "#/components/schemas/A"}
	  }}
	}`
	err := openapi.ValidateJSON([]byte(doc))
	if err == nil {
		t.Fatal("no error")
	}
	for _, ok := range []string{"/A:", "/B:", "/C:", "/H:", "/I:"} {
		if strings.Contains(err.Error(), "schemas"+ok) {
			t.Errorf("%s reported as unresolved:\n%v", ok, err)
		}
	}
	for _, bad := range []string{"/D:", "/E:", "/F:", "/G:"} {
		if !strings.Contains(err.Error(), "schemas"+bad) {
			t.Errorf("%s not reported:\n%v", bad, err)
		}
	}
	if err := openapi.ValidateJSON(golden(t)); err != nil {
		t.Error(err)
	}
}
