package mediator_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

var allCodes = []struct {
	code   mediator.Code
	status int
	title  string
}{
	{mediator.CodeBadRequest, 400, "Bad request"},
	{mediator.CodePayloadTooLarge, 413, "Payload too large"},
	{mediator.CodeUnsupportedMedia, 415, "Unsupported media type"},
	{mediator.CodeValidation, 422, "Validation failed"},
	{mediator.CodeNotFound, 404, "Not found"},
	{mediator.CodeConflict, 409, "Conflict"},
	{mediator.CodePrecondition, 412, "Precondition failed"},
	{mediator.CodeUnauthorized, 401, "Unauthorized"},
	{mediator.CodeForbidden, 403, "Forbidden"},
	{mediator.CodeRateLimited, 429, "Rate limited"},
	{mediator.CodeTimeout, 504, "Timeout"},
	{mediator.CodeUnavailable, 503, "Service unavailable"},
	{mediator.CodeIdempotencyMismatch, 422, "Idempotency key reused with a different payload"},
	{mediator.CodeIdempotencyBusy, 409, "Idempotent request in progress"},
	{mediator.CodeHandlerNotFound, 501, "Handler not found"},
	{mediator.CodeInternal, 500, "Internal error"},
	{mediator.Code(""), 500, "Internal error"},
	{mediator.Code("made_up"), 500, "Internal error"},
}

func TestStatusAndTitle(t *testing.T) {
	for _, c := range allCodes {
		if got := mediator.StatusOf(c.code); got != c.status {
			t.Errorf("StatusOf(%q) = %d, want %d", c.code, got, c.status)
		}
		if got := c.code.Title(); got != c.title {
			t.Errorf("%q.Title() = %q, want %q", c.code, got, c.title)
		}
	}
}

func TestError(t *testing.T) {
	cause := errors.New("root")
	e := mediator.Wrap(mediator.CodeNotFound, "order not found", cause)
	if e.Error() != "not_found: order not found: root" || e.Unwrap() != cause || !errors.Is(e, cause) {
		t.Fatalf("wrap: %v", e)
	}
	if e := mediator.E(mediator.CodeConflict, "dup"); e.Error() != "conflict: dup" || e.Unwrap() != nil {
		t.Fatalf("E: %v", e)
	}
	// Is: same code and empty target message, or equal messages.
	if !errors.Is(mediator.E(mediator.CodeHandlerNotFound, "x"), mediator.ErrHandlerNotFound) {
		t.Fatal("sentinel with empty message matches any message")
	}
	if !errors.Is(mediator.E(mediator.CodeInternal, "durable publish requires a unit of work"), mediator.ErrNoUnitOfWork) {
		t.Fatal("equal messages match")
	}
	if errors.Is(mediator.ErrNoUnitOfWork, mediator.ErrDurablePublishInQuery) {
		t.Fatal("same code, different messages must not match")
	}
	if errors.Is(mediator.E(mediator.CodeNotFound, ""), mediator.ErrHandlerNotFound) {
		t.Fatal("different codes must not match")
	}
	if errors.Is(mediator.E(mediator.CodeNotFound, ""), io.EOF) || errors.Is(io.EOF, mediator.ErrHandlerNotFound) {
		t.Fatal("non-Error targets never match")
	}
	if !errors.Is(fmt.Errorf("wrapped: %w", mediator.ErrDepthExceeded), mediator.ErrDepthExceeded) {
		t.Fatal("Is through fmt wrapping")
	}
	// WithDetail copies.
	base := mediator.E(mediator.CodeValidation, "bad").WithDetail("a", 1)
	d2 := base.WithDetail("b", 2)
	if len(base.Details) != 1 || len(d2.Details) != 2 || d2.Details["a"] != 1 || d2.Details["b"] != 2 || d2.Code != mediator.CodeValidation {
		t.Fatalf("details: %v %v", base.Details, d2.Details)
	}
	d3 := d2.WithDetail("a", 9)
	if d2.Details["a"] != 1 || d3.Details["a"] != 9 {
		t.Fatal("WithDetail must not mutate the receiver")
	}
	var target *mediator.Error
	if !errors.As(fmt.Errorf("ctx: %w", e), &target) || target != e {
		t.Fatal("As")
	}
}

func TestCodeOf(t *testing.T) {
	cases := []struct {
		err  error
		want mediator.Code
	}{
		{nil, ""},
		{mediator.E(mediator.CodeForbidden, "x"), mediator.CodeForbidden},
		{fmt.Errorf("wrap: %w", mediator.E(mediator.CodeRateLimited, "x")), mediator.CodeRateLimited},
		{&mediator.ValidationError{}, mediator.CodeValidation},
		{fmt.Errorf("wrap: %w", (&mediator.ValidationError{}).Add("/a", "min", "too small")), mediator.CodeValidation},
		{context.DeadlineExceeded, mediator.CodeTimeout},
		{context.Canceled, mediator.CodeTimeout},
		{fmt.Errorf("op: %w", context.Canceled), mediator.CodeTimeout},
		{errors.New("unknown"), mediator.CodeInternal},
		{io.EOF, mediator.CodeInternal},
		// An *Error wrapping a validation error keeps its own code.
		{mediator.Wrap(mediator.CodeInternal, "x", &mediator.ValidationError{}), mediator.CodeInternal},
	}
	for _, c := range cases {
		if got := mediator.CodeOf(c.err); got != c.want {
			t.Errorf("CodeOf(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}

func TestValidationError(t *testing.T) {
	v := &mediator.ValidationError{}
	if v.Error() != "validation failed" {
		t.Fatal(v.Error())
	}
	if got := v.Add("/name", "required", "is required").Error(); got != "validation failed (1 field): /name is required (required);" {
		t.Fatalf("%q", got)
	}
	if got := v.Add("/lines/0/qty", "min", "must be at least 1").Error(); got != "validation failed (2 fields): /name is required (required); /lines/0/qty must be at least 1 (min);" {
		t.Fatalf("%q", got)
	}
	if len(v.Fields) != 2 || v.Fields[1] != (mediator.FieldError{Path: "/lines/0/qty", Rule: "min", Message: "must be at least 1"}) {
		t.Fatalf("%+v", v.Fields)
	}
	if mediator.StatusOf(mediator.CodeOf(v)) != 422 {
		t.Fatal("status")
	}
}

func TestPanicError(t *testing.T) {
	p := &mediator.PanicError{Value: "boom", Stack: []byte("stack")}
	if p.Error() != "panic: boom" {
		t.Fatal(p.Error())
	}
	if (&mediator.PanicError{Value: 42}).Error() != "panic: 42" {
		t.Fatal("int value")
	}
}

func TestAmbiguous(t *testing.T) {
	if mediator.MarkAmbiguous(nil) != nil || mediator.IsAmbiguous(nil) {
		t.Fatal("nil")
	}
	base := mediator.E(mediator.CodeUnavailable, "commit outcome unknown")
	a := mediator.MarkAmbiguous(base)
	if !mediator.IsAmbiguous(a) || a.Error() != base.Error() || !errors.Is(a, base) || mediator.CodeOf(a) != mediator.CodeUnavailable {
		t.Fatalf("ambiguous: %v", a)
	}
	if !mediator.IsAmbiguous(fmt.Errorf("outer: %w", a)) || mediator.IsAmbiguous(base) || mediator.IsAmbiguous(errors.New("x")) {
		t.Fatal("IsAmbiguous through wrapping only")
	}
	if !mediator.IsAmbiguous(mediator.MarkAmbiguous(mediator.MarkAmbiguous(base))) {
		t.Fatal("double mark")
	}
}

type timeoutNetErr struct{ timeout bool }

func (e timeoutNetErr) Error() string   { return "net" }
func (e timeoutNetErr) Timeout() bool   { return e.timeout }
func (e timeoutNetErr) Temporary() bool { return false }

var errRegisteredTransient = errors.New("driver: serialization failure")

func TestIsTransient(t *testing.T) {
	mediator.RegisterTransient(func(err error) bool { return errors.Is(err, errRegisteredTransient) })
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unavailable", mediator.E(mediator.CodeUnavailable, "x"), true},
		{"timeout code", mediator.E(mediator.CodeTimeout, "x"), true},
		{"not found", mediator.E(mediator.CodeNotFound, "x"), false},
		{"internal wrapping deadline", mediator.Wrap(mediator.CodeInternal, "x", context.DeadlineExceeded), true},
		{"deadline", context.DeadlineExceeded, true},
		{"canceled is not transient", context.Canceled, false},
		{"unexpected eof", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
		{"eof", io.EOF, false},
		{"econnrefused", syscall.ECONNREFUSED, true},
		{"econnreset", fmt.Errorf("w: %w", syscall.ECONNRESET), true},
		{"epipe", syscall.EPIPE, true},
		{"net timeout", timeoutNetErr{timeout: true}, true},
		{"net non-timeout", timeoutNetErr{timeout: false}, false},
		{"op error", &net.OpError{Op: "dial", Err: errors.New("refused")}, true},
		{"registered", fmt.Errorf("tx: %w", errRegisteredTransient), true},
		{"plain", errors.New("plain"), false},
		{"validation", &mediator.ValidationError{}, false},
	}
	for _, c := range cases {
		if got := mediator.IsTransient(c.err); got != c.want {
			t.Errorf("%s: IsTransient(%v) = %v, want %v", c.name, c.err, got, c.want)
		}
	}
}

func TestSentinels(t *testing.T) {
	if mediator.CodeOf(mediator.ErrHandlerNotFound) != mediator.CodeHandlerNotFound ||
		mediator.CodeOf(mediator.ErrNoUnitOfWork) != mediator.CodeInternal ||
		mediator.CodeOf(mediator.ErrDurablePublishInQuery) != mediator.CodeInternal ||
		mediator.CodeOf(mediator.ErrNotBuilt) != mediator.CodeInternal ||
		mediator.CodeOf(mediator.ErrDepthExceeded) != mediator.CodeInternal ||
		mediator.CodeOf(mediator.ErrAlreadyBuilt) != mediator.CodeInternal {
		t.Fatal("sentinel codes")
	}
	if mediator.ErrAlreadyBuilt.Error() != "mediator: already built" {
		t.Fatal(mediator.ErrAlreadyBuilt)
	}
}
