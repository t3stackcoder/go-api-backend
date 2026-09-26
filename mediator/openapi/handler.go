package openapi

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"html"
	"net/http"
	"path"
	"strconv"
	"strings"
)

// scalarHTML is the single-file reference page. It loads the Scalar viewer
// from the jsDelivr CDN in the browser and reads ./openapi.json relative to
// its own URL, so it works under any prefix.
//
//go:embed scalar/index.html
var scalarHTML string

// titlePlaceholder in the page is replaced with the document title.
const titlePlaceholder = "{{title}}"

// Route names the handler serves, as the last path segment.
const (
	// DocumentFile is the last path segment of the JSON document.
	DocumentFile = "openapi.json"
	// DocsPage is the last path segment of the reference page.
	DocsPage = "docs"
)

// Handler serves the document and the reference page. A request whose path
// ends in /openapi.json gets the document as application/json; one ending
// in /docs gets the embedded Scalar page, which fetches ./openapi.json next
// to itself. Both carry a strong ETag (SHA-256 of the body), answer
// If-None-Match with 304, allow GET and HEAD, and send Cache-Control:
// no-cache. Any other path is 404 and any other method 405.
//
// Set it as httpapi.Config.Docs, which mounts it at {Prefix}/docs and
// {Prefix}/openapi.json, or mount it on any mux. The document is serialized
// once, when Handler is called; a document that cannot be marshaled makes
// /openapi.json answer 500 while the page is still served.
func Handler(doc *Document) http.Handler {
	h := &handler{}
	title := DefaultTitle
	if doc == nil {
		h.err = errors.New("openapi: nil document")
	} else {
		title = doc.Info.Title
		if b, err := doc.MarshalJSON(); err != nil {
			h.err = err
		} else {
			h.json = b
			h.jsonTag = etagOf(b)
		}
	}
	h.html = []byte(strings.ReplaceAll(scalarHTML, titlePlaceholder, html.EscapeString(title)))
	h.htmlTag = etagOf(h.html)
	return h
}

// handler holds the serialized document and page with their ETags.
type handler struct {
	json, html       []byte
	jsonTag, htmlTag string
	err              error
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
	switch path.Base(r.URL.Path) {
	case DocumentFile:
		if h.err != nil {
			http.Error(w, h.err.Error(), http.StatusInternalServerError)
			return
		}
		serve(w, r, h.json, "application/json", h.jsonTag)
	case DocsPage:
		serve(w, r, h.html, "text/html; charset=utf-8", h.htmlTag)
	default:
		http.NotFound(w, r)
	}
}

// serve writes body with its ETag, honoring If-None-Match and HEAD.
func serve(w http.ResponseWriter, r *http.Request, body []byte, contentType, etag string) {
	hdr := w.Header()
	hdr.Set("ETag", etag)
	hdr.Set("Cache-Control", "no-cache")
	if matchesETag(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	hdr.Set("Content-Type", contentType)
	hdr.Set("Content-Length", strconv.Itoa(len(body)))
	hdr.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

// etagOf returns the strong ETag of body: its SHA-256 in hex, quoted.
func etagOf(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// matchesETag reports whether an If-None-Match value names etag: "*" or a
// comma-separated list whose entries may carry the W/ weakness prefix.
func matchesETag(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || strings.TrimPrefix(candidate, "W/") == etag {
			return true
		}
	}
	return false
}
