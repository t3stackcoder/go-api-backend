package httpapi

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Problem is an RFC 9457 problem details body. Every error response of the
// adapter has this shape, with Content-Type application/problem+json. The
// openapi package describes it once as the shared Problem schema.
type Problem struct {
	// Type is "urn:mediator:error:<code>".
	Type string `json:"type"`
	// Title is the human title of the code (mediator.Code.Title).
	Title string `json:"title"`
	// Status is the HTTP status (mediator.StatusOf).
	Status int `json:"status"`
	// Detail is the client-safe message. For CodeInternal it is always
	// InternalDetail; for validation it counts the invalid fields.
	Detail string `json:"detail"`
	// Instance is the request path.
	Instance string `json:"instance"`
	// CorrelationID is the correlation ID of the request.
	CorrelationID string `json:"correlationId,omitempty"`
	// Code is the machine-readable mediator.Code.
	Code string `json:"code"`
	// Errors lists field failures for validation problems.
	Errors []mediator.FieldError `json:"errors,omitempty"`
	// Details carries the client-safe details of a mediator.Error.
	Details map[string]any `json:"details,omitempty"`
}

const (
	// ProblemType is the URN prefix of Problem.Type.
	ProblemType = "urn:mediator:error:"
	// ProblemContentType is the media type of problem responses.
	ProblemContentType = "application/problem+json"
	// InternalDetail is the fixed detail of every CodeInternal problem. The
	// cause is logged with the correlation ID and never sent.
	InternalDetail = "An internal error occurred"

	headerCorrelation = "X-Correlation-ID"
	headerRetryAfter  = "Retry-After"

	codeMethodNotAllowed = "method_not_allowed"
)

// ProblemOf classifies err into a Problem for the given request path and
// correlation ID. A nil error is treated as CodeInternal. It does not log;
// WriteProblem does.
func ProblemOf(err error, instance, correlationID string) Problem {
	code := mediator.CodeOf(err)
	if err == nil {
		code = mediator.CodeInternal
	}
	p := Problem{
		Type:          ProblemType + string(code),
		Title:         code.Title(),
		Status:        mediator.StatusOf(code),
		Instance:      instance,
		CorrelationID: correlationID,
		Code:          string(code),
	}
	var ve *mediator.ValidationError
	if errors.As(err, &ve) && len(ve.Fields) > 0 {
		p.Errors = slices.Clone(ve.Fields)
	}
	var me *mediator.Error
	hasMessage := errors.As(err, &me)
	switch {
	case code == mediator.CodeInternal:
		p.Detail = InternalDetail
	case hasMessage && me.Message != "":
		p.Detail = me.Message
	case ve != nil && len(ve.Fields) > 0:
		p.Detail = fieldCount(len(ve.Fields))
	default:
		p.Detail = code.Title()
	}
	if hasMessage && len(me.Details) > 0 && code != mediator.CodeInternal {
		p.Details = maps.Clone(me.Details)
	}
	return p
}

func fieldCount(n int) string {
	if n == 1 {
		return "1 field is invalid"
	}
	return fmt.Sprintf("%d fields are invalid", n)
}

// WriteProblem renders err as an application/problem+json response. The
// status comes from mediator.StatusOf; CodeIdempotencyBusy and
// CodeRateLimited add Retry-After (seconds, from Details["retry_after_ms"]
// rounded up, at least 1); CodeInternal sends InternalDetail and logs the
// cause at error level with the correlation ID. The correlation ID is taken
// from the request context, then from the X-Correlation-ID response header,
// then from the request header, else generated.
func WriteProblem(w http.ResponseWriter, r *http.Request, err error) {
	markRouted(w)
	corr := mediator.CorrelationID(r.Context())
	if corr == "" {
		corr = ensureCorrelation(w, r)
	}
	p := ProblemOf(err, r.URL.Path, corr)
	logProblem(loggerFrom(r.Context()), p, err)
	writeProblem(w, p, retryAfterSeconds(err))
}

// logProblem logs internal errors; the body never carries their cause.
func logProblem(l *slog.Logger, p Problem, err error) {
	if p.Code != string(mediator.CodeInternal) {
		return
	}
	attrs := []any{"correlation_id", p.CorrelationID, "instance", p.Instance, "error", err}
	var pe *mediator.PanicError
	if errors.As(err, &pe) {
		attrs = append(attrs, "stack", string(pe.Stack))
	}
	l.Error("internal error", attrs...)
}

// marshalProblem encodes p deterministically. Invalid UTF-8 (a request path
// such as /x%8d ends up in Instance and Detail) is replaced with U+FFFD, and
// Details that cannot be encoded (a func value or NaN, say) are dropped
// rather than failing the response. With Details gone the remaining members
// are plain strings and integers, which always encode.
func marshalProblem(p Problem) []byte {
	opts := json.JoinOptions(json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
	b, err := json.Marshal(p, opts)
	if err != nil {
		p.Details = nil
		b, _ = json.Marshal(p, opts)
	}
	return b
}

func writeProblem(w http.ResponseWriter, p Problem, retryAfter int) {
	b := marshalProblem(p)
	h := w.Header()
	h.Set("Content-Type", ProblemContentType)
	h.Set("Content-Length", strconv.Itoa(len(b)))
	h.Del("X-Content-Type-Options")
	if retryAfter > 0 {
		h.Set(headerRetryAfter, strconv.Itoa(retryAfter))
	}
	if p.CorrelationID != "" {
		h.Set(headerCorrelation, p.CorrelationID)
	}
	w.WriteHeader(p.Status)
	_, _ = w.Write(b) //nolint:gosec // G705: b is JSON from marshalProblem, served as application/problem+json, never as HTML
}

// retryAfterSeconds returns the Retry-After value for err, or 0 when the
// code does not carry one.
func retryAfterSeconds(err error) int {
	switch mediator.CodeOf(err) {
	case mediator.CodeIdempotencyBusy, mediator.CodeRateLimited:
	default:
		return 0
	}
	var ms float64
	var me *mediator.Error
	if errors.As(err, &me) {
		ms = numberOf(me.Details["retry_after_ms"])
	}
	secs := int(math.Ceil(ms / 1000))
	if secs < 1 {
		secs = 1
	}
	return secs
}

// numberOf converts the numeric detail types a behavior may store into a
// float64 of milliseconds; anything else is 0.
func numberOf(v any) float64 {
	switch n := v.(type) {
	case time.Duration:
		return float64(n) / float64(time.Millisecond)
	case int:
		return float64(n)
	case int8:
		return float64(n)
	case int16:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case uint:
		return float64(n)
	case uint8:
		return float64(n)
	case uint16:
		return float64(n)
	case uint32:
		return float64(n)
	case uint64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	}
	return 0
}

// methodNotAllowed builds the 405 problem. It has no mediator.Code because
// no handler can produce it; the code string is "method_not_allowed".
func methodNotAllowed(r *http.Request, allow, correlationID string) Problem {
	return Problem{
		Type:          ProblemType + codeMethodNotAllowed,
		Title:         "Method not allowed",
		Status:        http.StatusMethodNotAllowed,
		Detail:        fmt.Sprintf("%s is not allowed for %s; allowed: %s", r.Method, r.URL.Path, allow),
		Instance:      r.URL.Path,
		CorrelationID: correlationID,
		Code:          codeMethodNotAllowed,
	}
}
