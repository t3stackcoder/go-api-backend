package httpapi

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

const (
	warnIdempotencyIgnored = `299 - "Idempotency-Key ignored on queries"`
	maxCorrelationLen      = 128
)

// handler returns the http.Handler of one route.
func (s *Server) handler(rt *route) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.serve(rt, w, r) })
}

// serve is the request pipeline of 8.2: correlation and trace context, body
// limit, content type, decode, bind, idempotency key, cache control,
// principal, dispatch, encode.
func (s *Server) serve(rt *route, w http.ResponseWriter, r *http.Request) {
	markRouted(w)
	corr := ensureCorrelation(w, r)
	ctx := withLogger(r.Context(), s.logger)
	ctx = mediator.WithCorrelationID(ctx, corr)
	ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.HeaderCarrier(r.Header))
	ctx = WithRemoteAddr(ctx, remoteIP(r.RemoteAddr))
	r = r.WithContext(ctx)

	body, err := s.readBody(w, r)
	if err != nil {
		WriteProblem(w, r, err)
		return
	}
	if len(body) > 0 {
		if err := checkContentType(r.Header.Get("Content-Type")); err != nil {
			WriteProblem(w, r, err)
			return
		}
	}
	if err := testkit.Fault(ctx, "http.decode"); err != nil {
		WriteProblem(w, r, err)
		return
	}
	// Equivalent to m.NewRequest(rt.info.Name): the route came from the registry.
	req := reflect.New(rt.info.RequestType).Interface()
	if len(body) > 0 {
		if err := decodeBody(body, req, rt, s.cfg.AllowUnknownFields); err != nil {
			WriteProblem(w, r, err)
			return
		}
	}
	if err := bindParams(req, rt, r); err != nil {
		WriteProblem(w, r, err)
		return
	}

	if key := r.Header.Get("Idempotency-Key"); key != "" {
		if rt.info.Kind == mediator.KindCommand {
			ctx = mediator.WithIdempotencyKey(ctx, key)
		} else {
			w.Header().Add("Warning", warnIdempotencyIgnored)
		}
	}
	if hasNoCache(r.Header) {
		ctx = mediator.WithNoCache(ctx)
	}
	if s.cfg.Authenticator != nil {
		p, err := s.cfg.Authenticator(r)
		if err != nil {
			WriteProblem(w, r, unauthorized(err))
			return
		}
		ctx = authz.WithPrincipal(ctx, p)
	}
	ctx = WithCacheResultHolder(ctx)
	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(w.Header()))
	r = r.WithContext(ctx)

	if rt.info.Kind == mediator.KindStream {
		s.serveStream(w, r, req)
		return
	}
	res, err := s.m.SendAny(ctx, req)
	if cr, ok := CacheResultFrom(ctx); ok {
		w.Header().Set("X-Mediator-Cache", string(cr))
	}
	if err != nil {
		WriteProblem(w, r, err)
		return
	}
	if err := testkit.Fault(ctx, "http.encode"); err != nil {
		WriteProblem(w, r, err)
		return
	}
	s.writeResponse(w, r, rt, res)
}

// writeResponse encodes res with the route's success status; Void and 204
// send no body.
func (s *Server) writeResponse(w http.ResponseWriter, r *http.Request, rt *route, res any) {
	if rt.info.ResponseType == voidT || rt.status == http.StatusNoContent {
		w.WriteHeader(rt.status)
		return
	}
	b, err := json.Marshal(res)
	if err != nil {
		WriteProblem(w, r, mediator.Wrap(mediator.CodeInternal, "encode response", err))
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(rt.status)
	_, _ = w.Write(b)
}

// ensureCorrelation returns the correlation ID of the response, setting the
// X-Correlation-ID header on first use: the request header when it is
// printable and at most 128 characters, else a fresh UUIDv7.
func ensureCorrelation(w http.ResponseWriter, r *http.Request) string {
	if c := w.Header().Get(headerCorrelation); c != "" {
		return c
	}
	c := r.Header.Get(headerCorrelation)
	if !validCorrelationID(c) {
		c = mediator.NewID(time.Now()).String()
	}
	w.Header().Set(headerCorrelation, c)
	return c
}

func validCorrelationID(s string) bool {
	if s == "" || len(s) > maxCorrelationLen || !utf8.ValidString(s) {
		return false
	}
	return strings.IndexFunc(s, func(r rune) bool { return !unicode.IsPrint(r) }) < 0
}

// remoteIP strips the port from a RemoteAddr of the form host:port.
func remoteIP(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

// readBody reads the whole body under MaxBodyBytes.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	limit := s.cfg.MaxBodyBytes
	if r.ContentLength > limit {
		return nil, tooLarge(limit)
	}
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil
	}
	raw := w
	if u, ok := w.(interface{ Unwrap() http.ResponseWriter }); ok {
		raw = u.Unwrap() // lets MaxBytesReader mark the connection for closing
	}
	b, err := io.ReadAll(http.MaxBytesReader(raw, r.Body, limit))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return nil, tooLarge(limit)
		}
		return nil, mediator.Wrap(mediator.CodeBadRequest, "reading the request body failed", err)
	}
	return b, nil
}

func tooLarge(limit int64) error {
	return mediator.E(mediator.CodePayloadTooLarge, fmt.Sprintf("body exceeds %d bytes", limit))
}

// checkContentType requires application/json with an optional UTF-8 charset.
func checkContentType(ct string) error {
	if ct == "" {
		return mediator.E(mediator.CodeUnsupportedMedia, "Content-Type must be application/json")
	}
	mt, params, err := mime.ParseMediaType(ct)
	if err != nil || mt != "application/json" {
		return mediator.E(mediator.CodeUnsupportedMedia, fmt.Sprintf("Content-Type %q is not application/json", ct))
	}
	if cs, ok := params["charset"]; ok && !strings.EqualFold(cs, "utf-8") && !strings.EqualFold(cs, "utf8") {
		return mediator.E(mediator.CodeUnsupportedMedia, "Content-Type charset must be UTF-8")
	}
	return nil
}

// hasNoCache reports whether Cache-Control lists the no-cache directive.
func hasNoCache(h http.Header) bool {
	for _, v := range h.Values("Cache-Control") {
		for _, d := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(d), "no-cache") {
				return true
			}
		}
	}
	return false
}

// decodeBody decodes body into req. Members that name a path, query, or
// header field are rejected first, then json/v2 decodes strictly:
// case-sensitive names, duplicates rejected, unknown members rejected unless
// allowUnknown.
func decodeBody(body []byte, req any, rt *route, allowUnknown bool) error {
	if len(rt.boundNames) > 0 {
		if ve := boundMembersInBody(body, rt.boundNames); ve != nil {
			return ve
		}
	}
	if err := json.Unmarshal(body, req, json.RejectUnknownMembers(!allowUnknown)); err != nil {
		return classifyDecodeError(err)
	}
	return nil
}

// boundMembersInBody scans the top-level member names of a JSON object and
// reports those that belong to a parameter. Anything that is not an object,
// or a syntax error, is left for the real decode to report.
func boundMembersInBody(body []byte, bound map[string]Source) *mediator.ValidationError {
	dec := jsontext.NewDecoder(bytes.NewReader(body))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return nil
	}
	var ve *mediator.ValidationError
	for {
		if k := dec.PeekKind(); k == '}' || k == 0 {
			break
		}
		tok, err := dec.ReadToken()
		if err != nil {
			break
		}
		name := tok.String()
		if src, ok := bound[name]; ok {
			if ve == nil {
				ve = &mediator.ValidationError{}
			}
			ve.Add("/"+escapePointer(name), "binding", fmt.Sprintf("is bound from the %s, not the body", src))
		}
		if err := dec.SkipValue(); err != nil {
			break
		}
	}
	return ve
}

// classifyDecodeError maps json/v2 errors to the error model: syntax errors
// are CodeBadRequest; duplicate names, unknown members, and values that do
// not fit the field are CodeValidation with the JSON Pointer of the member.
func classifyDecodeError(err error) error {
	var syn *jsontext.SyntacticError
	if errors.As(err, &syn) {
		if errors.Is(err, jsontext.ErrDuplicateName) {
			return (&mediator.ValidationError{}).Add(string(syn.JSONPointer), "duplicate", "duplicate member")
		}
		return mediator.Wrap(mediator.CodeBadRequest, fmt.Sprintf("body is not well-formed JSON: %v", syn.Err), err)
	}
	var sem *json.SemanticError
	if errors.As(err, &sem) {
		path := string(sem.JSONPointer)
		switch {
		case errors.Is(err, json.ErrUnknownName):
			return (&mediator.ValidationError{}).Add(path, "unknown", "unknown member")
		case sem.GoType == timeT:
			return (&mediator.ValidationError{}).Add(path, "format", "must be an RFC 3339 date-time")
		case sem.GoType == uuidT:
			return (&mediator.ValidationError{}).Add(path, "format", "must be a UUID")
		case sem.Err == nil && sem.JSONKind != 0 && sem.GoType != nil:
			return (&mediator.ValidationError{}).Add(path, "type", fmt.Sprintf("expected %s, got %s", jsonTypeName(sem.GoType), kindName(sem.JSONKind)))
		case sem.Err != nil:
			return (&mediator.ValidationError{}).Add(path, "type", "invalid value: "+sem.Err.Error())
		}
		return (&mediator.ValidationError{}).Add(path, "type", "invalid value")
	}
	return mediator.Wrap(mediator.CodeBadRequest, "cannot decode the request body", err)
}

// jsonTypeName describes a Go type in JSON terms for client messages.
func jsonTypeName(t reflect.Type) string {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == timeT {
		return "date-time string"
	}
	if reflect.PointerTo(t).Implements(textUnmarshalerT) {
		return "string"
	}
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Bool:
		return "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "integer"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Struct, reflect.Map:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	}
	return "value"
}

func kindName(k jsontext.Kind) string {
	switch k {
	case '"':
		return "string"
	case '0':
		return "number"
	case 't', 'f':
		return "boolean"
	case 'n':
		return "null"
	case '{':
		return "object"
	case '[':
		return "array"
	}
	return "value"
}
