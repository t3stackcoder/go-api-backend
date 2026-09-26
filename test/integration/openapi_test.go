//go:build integration

package integration

import (
	"encoding/json/v2"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
)

// committedOpenAPI is the living fixture of spec 8.6 relative to this
// package directory, which is the working directory of the test binary.
var committedOpenAPI = filepath.Join("..", "..", "api", "openapi.json")

// TestOpenAPI_ServedMatchesCommitted checks that the document a running
// node serves at /openapi.json is the committed api/openapi.json (compared
// as parsed JSON, so member order does not matter), that it is valid
// OpenAPI 3.1, and that /docs serves the reference page.
func TestOpenAPI_ServedMatchesCommitted(t *testing.T) {
	committed, err := os.ReadFile(committedOpenAPI)
	if err != nil {
		t.Fatalf("read %s: %v", committedOpenAPI, err)
	}
	db := newDatabase(t)
	// No ORDERS_DEBUG: the debug requests are not part of the committed
	// document.
	n := startNode(t, nodeConfig{db: db})
	n.waitReady(t, 60*time.Second)
	c := n.client("")

	served := c.do(t, http.MethodGet, "/openapi.json", nil, nil).expect(t, http.StatusOK, "GET /openapi.json")
	if ct := served.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("/openapi.json content type %q", ct)
	}
	var got, want any
	if err := json.Unmarshal(served.Body, &got); err != nil {
		t.Fatalf("served document: %v", err)
	}
	if err := json.Unmarshal(committed, &want); err != nil {
		t.Fatalf("committed document: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("served /openapi.json differs from %s; run `go run ./tools/task openapi` and compare\nserved: %s", committedOpenAPI, served.Body)
	}
	if err := openapi.ValidateJSON(served.Body); err != nil {
		t.Errorf("served document is not valid OpenAPI 3.1: %v", err)
	}
	if v, _ := got.(map[string]any)["openapi"].(string); !strings.HasPrefix(v, "3.1.") {
		t.Errorf("openapi version %q, want 3.1.x", v)
	}

	docs := c.do(t, http.MethodGet, "/docs", nil, nil).expect(t, http.StatusOK, "GET /docs")
	if ct := docs.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Errorf("/docs content type %q, want text/html", ct)
	}
	if body := strings.ToLower(string(docs.Body)); !strings.Contains(body, "<html") || !strings.Contains(body, "openapi.json") {
		t.Errorf("/docs is not the reference page: %.200s", docs.Body)
	}
}
