package httpapi

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

func TestClassifyDecodeError_Internal(t *testing.T) {
	// A non-json error (impossible through the handler) is a bad request.
	err := classifyDecodeError(errors.New("odd"))
	if mediator.CodeOf(err) != mediator.CodeBadRequest || !strings.Contains(err.Error(), "cannot decode the request body") {
		t.Errorf("%v", err)
	}
	// A semantic error without kind or type information is a generic invalid value.
	err = classifyDecodeError(&json.SemanticError{JSONPointer: "/x"})
	var ve *mediator.ValidationError
	if !errors.As(err, &ve) || ve.Fields[0].Path != "/x" || ve.Fields[0].Message != "invalid value" {
		t.Errorf("%v", err)
	}
	err = classifyDecodeError(&json.SemanticError{JSONPointer: "/y", Err: errors.New("boom")})
	if !errors.As(err, &ve) || ve.Fields[0].Message != "invalid value: boom" {
		t.Errorf("%v", err)
	}
}

func TestJSONTypeNames(t *testing.T) {
	rows := map[reflect.Type]string{
		reflect.TypeFor[string]():         "string",
		reflect.TypeFor[*string]():        "string",
		reflect.TypeFor[bool]():           "boolean",
		reflect.TypeFor[int8]():           "integer",
		reflect.TypeFor[uint64]():         "integer",
		reflect.TypeFor[float32]():        "number",
		reflect.TypeFor[struct{}]():       "object",
		reflect.TypeFor[map[string]int](): "object",
		reflect.TypeFor[[]int]():          "array",
		reflect.TypeFor[[2]int]():         "array",
		reflect.TypeFor[time.Time]():      "date-time string",
		reflect.TypeFor[uuid.UUID]():      "string",
		reflect.TypeFor[any]():            "value",
		reflect.TypeFor[chan int]():       "value",
	}
	for typ, want := range rows {
		if got := jsonTypeName(typ); got != want {
			t.Errorf("%s = %q, want %q", typ, got, want)
		}
	}
	kinds := map[jsontext.Kind]string{'"': "string", '0': "number", 't': "boolean", 'f': "boolean", 'n': "null", '{': "object", '[': "array", 0: "value"}
	for k, want := range kinds {
		if got := kindName(k); got != want {
			t.Errorf("kind %q = %q, want %q", k, got, want)
		}
	}
}

func TestSetScalar_Unsupported(t *testing.T) {
	var c complex128
	err := setScalar(reflect.ValueOf(&c).Elem(), "1")
	if err == nil || !strings.Contains(err.Error(), "cannot be bound from a string") {
		t.Errorf("%v", err)
	}
	if got := escapePointer("a/b~c"); got != "a~1b~0c" {
		t.Errorf("%q", got)
	}
	if Source(99).String() != "body" {
		t.Error("unknown source")
	}
}

func TestNumberOf(t *testing.T) {
	if numberOf("x") != 0 || numberOf(nil) != 0 || numberOf(int64(3)) != 3 || numberOf(2*time.Second) != 2000 {
		t.Error("numberOf")
	}
}

func TestInterceptor_FlushWithoutFlusher(t *testing.T) {
	type bare struct{ http.ResponseWriter }
	rec := httptest.NewRecorder()
	iw := &interceptor{ResponseWriter: bare{rec}}
	if err := iw.FlushError(); !errors.Is(err, http.ErrNotSupported) {
		t.Errorf("FlushError = %v", err)
	}
	iw.Flush() // no panic
	// Interception is a one-shot: a second WriteHeader passes through.
	iw.WriteHeader(404)
	iw.WriteHeader(500)
	if iw.intercepted != 404 || rec.Code != 500 {
		t.Errorf("intercepted %d code %d", iw.intercepted, rec.Code)
	}
}

type fakeListener struct{ closed chan struct{} }

func (l *fakeListener) Accept() (net.Conn, error) {
	<-l.closed
	return nil, errors.New("accept exploded")
}
func (l *fakeListener) Close() error   { return nil }
func (l *fakeListener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1} }

func TestListener_ServeError(t *testing.T) {
	m := mediator.New()
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	s, err := New(m, Config{})
	if err != nil {
		t.Fatal(err)
	}
	fl := &fakeListener{closed: make(chan struct{})}
	l := NewListener(s, "fake", time.Second)
	l.listen = func(string, string) (net.Listener, error) { return fl, nil }
	done := make(chan error, 1)
	go func() { done <- l.Run(context.Background()) }()
	deadline := time.Now().Add(5 * time.Second)
	for l.Healthy() != nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if l.Addr() != "127.0.0.1:1" {
		t.Errorf("Addr = %q", l.Addr())
	}
	close(fl.closed)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "accept exploded") {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
	if err := l.Healthy(); err == nil || !strings.Contains(err.Error(), "accept exploded") {
		t.Errorf("Healthy = %v", err)
	}
}

// TestNewListener_Drain: zero and negative drains take the 15 s default; any
// positive drain, however small, is kept.
func TestNewListener_Drain(t *testing.T) {
	rows := []struct {
		name string
		in   time.Duration
		want time.Duration
	}{
		{"zero uses the default", 0, defaultDrain},
		{"negative uses the default", -time.Second, defaultDrain},
		{"smallest positive is kept", time.Nanosecond, time.Nanosecond},
		{"explicit", time.Second, time.Second},
	}
	for _, row := range rows {
		if got := NewListener(nil, ":0", row.in).drain; got != row.want {
			t.Errorf("%s: drain = %s, want %s", row.name, got, row.want)
		}
	}
}

func TestPathParams(t *testing.T) {
	got := pathParams("/a/{b}/{c...}/{$}/{}/x{y}")
	if !reflect.DeepEqual(got, []string{"b", "c"}) {
		t.Errorf("%v", got)
	}
}
