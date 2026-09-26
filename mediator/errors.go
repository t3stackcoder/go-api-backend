package mediator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"syscall"
)

// Code is the machine-readable classification of an error. The HTTP adapter
// maps it to a status (see StatusOf) and clients branch on it.
type Code string

const (
	CodeBadRequest          Code = "bad_request"             // 400, body is not well-formed JSON
	CodePayloadTooLarge     Code = "payload_too_large"       // 413, body exceeds MaxBodyBytes
	CodeUnsupportedMedia    Code = "unsupported_media_type"  // 415, body Content-Type is not JSON
	CodeValidation          Code = "validation"              // 422
	CodeNotFound            Code = "not_found"               // 404
	CodeConflict            Code = "conflict"                // 409
	CodePrecondition        Code = "precondition_failed"     // 412
	CodeUnauthorized        Code = "unauthorized"            // 401
	CodeForbidden           Code = "forbidden"               // 403
	CodeRateLimited         Code = "rate_limited"            // 429
	CodeTimeout             Code = "timeout"                 // 504
	CodeUnavailable         Code = "unavailable"             // 503, transient
	CodeIdempotencyMismatch Code = "idempotency_mismatch"    // 422
	CodeIdempotencyBusy     Code = "idempotency_in_progress" // 409 + Retry-After
	CodeHandlerNotFound     Code = "handler_not_found"       // 501
	CodeInternal            Code = "internal"                // 500
)

// StatusOf maps a code to its HTTP status. Unknown codes are 500.
func StatusOf(c Code) int {
	switch c {
	case CodeBadRequest:
		return 400
	case CodePayloadTooLarge:
		return 413
	case CodeUnsupportedMedia:
		return 415
	case CodeValidation, CodeIdempotencyMismatch:
		return 422
	case CodeNotFound:
		return 404
	case CodeConflict, CodeIdempotencyBusy:
		return 409
	case CodePrecondition:
		return 412
	case CodeUnauthorized:
		return 401
	case CodeForbidden:
		return 403
	case CodeRateLimited:
		return 429
	case CodeTimeout:
		return 504
	case CodeUnavailable:
		return 503
	case CodeHandlerNotFound:
		return 501
	default:
		return 500
	}
}

// Title returns the human title used by problem details.
func (c Code) Title() string {
	switch c {
	case CodeBadRequest:
		return "Bad request"
	case CodePayloadTooLarge:
		return "Payload too large"
	case CodeUnsupportedMedia:
		return "Unsupported media type"
	case CodeValidation:
		return "Validation failed"
	case CodeNotFound:
		return "Not found"
	case CodeConflict:
		return "Conflict"
	case CodePrecondition:
		return "Precondition failed"
	case CodeUnauthorized:
		return "Unauthorized"
	case CodeForbidden:
		return "Forbidden"
	case CodeRateLimited:
		return "Rate limited"
	case CodeTimeout:
		return "Timeout"
	case CodeUnavailable:
		return "Service unavailable"
	case CodeIdempotencyMismatch:
		return "Idempotency key reused with a different payload"
	case CodeIdempotencyBusy:
		return "Idempotent request in progress"
	case CodeHandlerNotFound:
		return "Handler not found"
	default:
		return "Internal error"
	}
}

// Error is the one error type the framework produces. Message and Details are
// safe to show to clients; Err never leaves the process.
type Error struct {
	Code    Code
	Message string
	Details map[string]any
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// Unwrap returns the cause.
func (e *Error) Unwrap() error { return e.Err }

// Is treats two *Error values as equal when their codes match and the target
// carries no message, which lets sentinels such as ErrHandlerNotFound match
// any error of their code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	if t.Code != e.Code {
		return false
	}
	return t.Message == "" || t.Message == e.Message
}

// WithDetail returns a copy of e with one more detail.
func (e *Error) WithDetail(key string, value any) *Error {
	cp := *e
	cp.Details = make(map[string]any, len(e.Details)+1)
	for k, v := range e.Details {
		cp.Details[k] = v
	}
	cp.Details[key] = value
	return &cp
}

// E builds an error with a code and a client-safe message.
func E(code Code, msg string) *Error { return &Error{Code: code, Message: msg} }

// Wrap builds an error with a code, a client-safe message, and a cause.
func Wrap(code Code, msg string, err error) *Error { return &Error{Code: code, Message: msg, Err: err} }

// CodeOf classifies any error. Unknown errors are CodeInternal.
func CodeOf(err error) Code {
	if err == nil {
		return ""
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	var v *ValidationError
	if errors.As(err, &v) {
		return CodeValidation
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return CodeTimeout
	}
	return CodeInternal
}

// FieldError is one validation failure.
type FieldError struct {
	Path    string `json:"path"` // JSON Pointer, e.g. /lines/0/qty
	Rule    string `json:"rule"` // e.g. min
	Message string `json:"message"`
}

// ValidationError carries field-level failures. CodeOf reports CodeValidation.
type ValidationError struct {
	Fields []FieldError
}

func (v *ValidationError) Error() string {
	if len(v.Fields) == 0 {
		return "validation failed"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "validation failed (%d field", len(v.Fields))
	if len(v.Fields) != 1 {
		b.WriteString("s")
	}
	b.WriteString("):")
	for _, f := range v.Fields {
		fmt.Fprintf(&b, " %s %s (%s);", f.Path, f.Message, f.Rule)
	}
	return b.String()
}

// Add appends a field failure and returns v for chaining.
func (v *ValidationError) Add(path, rule, msg string) *ValidationError {
	v.Fields = append(v.Fields, FieldError{Path: path, Rule: rule, Message: msg})
	return v
}

// PanicError is the cause recorded when a panic is recovered.
type PanicError struct {
	Value any
	Stack []byte
}

func (p *PanicError) Error() string { return fmt.Sprintf("panic: %v", p.Value) }

// Sentinel errors. Compare with errors.Is.
var (
	// ErrHandlerNotFound is returned by Send for an unregistered request type.
	ErrHandlerNotFound = &Error{Code: CodeHandlerNotFound}
	// ErrNoUnitOfWork is returned by Publish for a durable event outside a transaction.
	ErrNoUnitOfWork = &Error{Code: CodeInternal, Message: "durable publish requires a unit of work"}
	// ErrDurablePublishInQuery is returned by Publish for a durable event in a read-only transaction.
	ErrDurablePublishInQuery = &Error{Code: CodeInternal, Message: "durable publish inside a read-only unit of work"}
	// ErrAlreadyBuilt is returned by every registration function after Build.
	ErrAlreadyBuilt = errors.New("mediator: already built")
	// ErrNotBuilt is returned by Send, Publish, and Stream before Build.
	ErrNotBuilt = &Error{Code: CodeInternal, Message: "mediator not built"}
	// ErrDepthExceeded is returned when nested Send exceeds the depth cap.
	ErrDepthExceeded = &Error{Code: CodeInternal, Message: "send nesting depth exceeded"}
)

type ambiguousError struct{ err error }

func (a *ambiguousError) Error() string { return a.err.Error() }
func (a *ambiguousError) Unwrap() error { return a.err }

// MarkAmbiguous wraps err so IsAmbiguous reports true. The unit of work uses it
// for a commit whose outcome is unknown.
func MarkAmbiguous(err error) error {
	if err == nil {
		return nil
	}
	return &ambiguousError{err: err}
}

// IsAmbiguous reports whether err describes an operation whose outcome is unknown.
func IsAmbiguous(err error) bool {
	var a *ambiguousError
	return errors.As(err, &a)
}

var (
	transientMu     sync.RWMutex
	transientChecks []func(error) bool
)

// RegisterTransient adds a classifier consulted by IsTransient. Drivers
// register their own error codes from init.
func RegisterTransient(f func(error) bool) {
	transientMu.Lock()
	transientChecks = append(transientChecks, f)
	transientMu.Unlock()
}

// Transienter is implemented by errors that classify themselves, such as the
// injected faults of testkit, which cannot import this package.
type Transienter interface{ Transient() bool }

// IsTransient reports whether retrying err makes sense: CodeUnavailable,
// CodeTimeout, context deadline, connection errors, errors that classify
// themselves through Transienter, and whatever drivers registered (Postgres
// serialization failure and deadlock, Redis connection errors).
func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var tr Transienter
	if errors.As(err, &tr) && tr.Transient() {
		return true
	}
	var e *Error
	if errors.As(err, &e) && (e.Code == CodeUnavailable || e.Code == CodeTimeout) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	var oe *net.OpError
	if errors.As(err, &oe) {
		return true
	}
	transientMu.RLock()
	defer transientMu.RUnlock()
	for _, f := range transientChecks {
		if f(err) {
			return true
		}
	}
	return false
}
