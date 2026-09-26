package openapi_test

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

// do performs one request against h. Header pairs follow the target.
func do(h http.Handler, method, target string, hdr ...string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHandler(t *testing.T) {
	cfg := fixtureConfig()
	cfg.Info.Title = "Orders <beta> & more"
	doc := generate(t, cfg)
	want, _ := doc.MarshalJSON()
	sum := sha256.Sum256(want)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	h := openapi.Handler(doc)

	rec := do(h, "GET", "/openapi.json")
	if rec.Code != 200 || rec.Body.String() != string(want) {
		t.Fatalf("GET openapi.json: %d", rec.Code)
	}
	for name, value := range map[string]string{
		"Content-Type":           "application/json",
		"ETag":                   etag,
		"Cache-Control":          "no-cache",
		"X-Content-Type-Options": "nosniff",
	} {
		if got := rec.Header().Get(name); got != value {
			t.Errorf("%s = %q, want %q", name, got, value)
		}
	}
	if rec.Header().Get("Content-Length") == "" {
		t.Error("no Content-Length")
	}

	// Conditional requests.
	for _, inm := range []string{etag, "W/" + etag, `"other", ` + etag, "*"} {
		if rec := do(h, "GET", "/openapi.json", "If-None-Match", inm); rec.Code != 304 || rec.Body.Len() != 0 || rec.Header().Get("ETag") != etag {
			t.Errorf("If-None-Match %s: %d %d bytes", inm, rec.Code, rec.Body.Len())
		}
	}
	if rec := do(h, "GET", "/openapi.json", "If-None-Match", `"stale"`); rec.Code != 200 {
		t.Errorf("stale If-None-Match: %d", rec.Code)
	}

	// HEAD carries the headers and no body.
	if rec := do(h, "HEAD", "/prefix/openapi.json"); rec.Code != 200 || rec.Body.Len() != 0 || rec.Header().Get("ETag") != etag {
		t.Errorf("HEAD: %d %d bytes", rec.Code, rec.Body.Len())
	}

	// The reference page.
	rec = do(h, "GET", "/api/docs")
	body := rec.Body.String()
	if rec.Code != 200 || rec.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Fatalf("docs: %d %s", rec.Code, rec.Header().Get("Content-Type"))
	}
	for _, want := range []string{
		"<title>Orders &lt;beta&gt; &amp; more</title>",
		"https://cdn.jsdelivr.net/npm/@scalar/api-reference",
		"url: './openapi.json'",
		`<a href="./openapi.json">`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("docs page lacks %q", want)
		}
	}
	if strings.Contains(body, "{{title}}") {
		t.Error("title placeholder left in page")
	}
	if rec.Header().Get("ETag") == "" || rec.Header().Get("ETag") == etag {
		t.Errorf("docs ETag %q", rec.Header().Get("ETag"))
	}
	if rec := do(h, "GET", "/api/docs", "If-None-Match", rec.Header().Get("ETag")); rec.Code != 304 {
		t.Errorf("docs conditional: %d", rec.Code)
	}

	// Other paths and methods.
	if rec := do(h, "GET", "/api/other"); rec.Code != 404 {
		t.Errorf("other path: %d", rec.Code)
	}
	if rec := do(h, "POST", "/openapi.json"); rec.Code != 405 || rec.Header().Get("Allow") != "GET, HEAD" {
		t.Errorf("POST: %d %q", rec.Code, rec.Header().Get("Allow"))
	}
}

func TestHandlerBrokenDocument(t *testing.T) {
	doc := generate(t, fixtureConfig())
	doc.Components.Schemas["Broken"] = &validate.Schema{Extra: map[string]any{"x": func() {}}}
	h := openapi.Handler(doc)
	if rec := do(h, "GET", "/openapi.json"); rec.Code != 500 || !strings.Contains(rec.Body.String(), "marshal document") {
		t.Errorf("broken document: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, "GET", "/docs"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "<title>Orders</title>") {
		t.Errorf("docs with broken document: %d", rec.Code)
	}
	h = openapi.Handler(nil)
	if rec := do(h, "GET", "/openapi.json"); rec.Code != 500 || !strings.Contains(rec.Body.String(), "nil document") {
		t.Errorf("nil document: %d %s", rec.Code, rec.Body.String())
	}
	if rec := do(h, "GET", "/docs"); rec.Code != 200 || !strings.Contains(rec.Body.String(), "<title>"+openapi.DefaultTitle+"</title>") {
		t.Errorf("docs with nil document: %d", rec.Code)
	}
}
