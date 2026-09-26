package openapi_test

import (
	"bytes"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

func TestMarshalFormat(t *testing.T) {
	doc := generate(t, fixtureConfig())
	b, err := doc.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.HasPrefix(b, []byte("{\n  \"openapi\": \"3.1.0\",\n  \"jsonSchemaDialect\"")) {
		t.Errorf("prefix %q", b[:60])
	}
	if !bytes.HasSuffix(b, []byte("}\n")) || bytes.HasSuffix(b, []byte("\n\n")) {
		t.Errorf("suffix %q", b[len(b)-4:])
	}
	if bytes.Contains(b, []byte("\t")) {
		t.Error("tabs in output")
	}
	// Write emits the same bytes.
	var buf bytes.Buffer
	if err := doc.Write(&buf); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), b) {
		t.Error("Write differs from MarshalJSON")
	}
	// json.Marshal of the document goes through MarshalJSON too.
	if b2, err := json.Marshal(doc); err != nil || !bytes.Contains(b2, []byte(`"openapi"`)) {
		t.Errorf("json.Marshal: %v", err)
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestWriteErrors(t *testing.T) {
	doc := generate(t, fixtureConfig())
	if err := doc.Write(failWriter{}); err == nil || err.Error() != "disk full" {
		t.Errorf("Write error %v", err)
	}
	if err := doc.WriteFile(filepath.Join(t.TempDir(), "missing", "openapi.json")); err == nil {
		t.Error("WriteFile into a missing directory succeeded")
	}
	path := filepath.Join(t.TempDir(), "openapi.json")
	if err := doc.WriteFile(path); err != nil {
		t.Fatal(err)
	}
	want, _ := doc.MarshalJSON()
	if got, _ := os.ReadFile(path); !bytes.Equal(got, want) {
		t.Error("WriteFile content differs")
	}
	// A document that cannot be marshaled fails every writer.
	doc.Components.Schemas["Broken"] = &validate.Schema{Extra: map[string]any{"x": make(chan int)}}
	if _, err := doc.MarshalJSON(); err == nil || !strings.Contains(err.Error(), "marshal document") {
		t.Errorf("MarshalJSON error %v", err)
	}
	if err := doc.Write(&bytes.Buffer{}); err == nil {
		t.Error("Write succeeded")
	}
	if err := doc.WriteFile(path); err == nil {
		t.Error("WriteFile succeeded")
	}
}

func TestMapSorted(t *testing.T) {
	m := openapi.Map[int]{"b": 1, "a": 2, "c": 3}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"a":2,"b":1,"c":3}` {
		t.Errorf("got %s", b)
	}
	if _, err := json.Marshal(openapi.Map[any]{"x": func() {}}); err == nil {
		t.Error("marshal of an unencodable value succeeded")
	}
	if _, err := json.Marshal(openapi.Map[int]{"\xff": 1}); err == nil {
		t.Error("marshal of an invalid UTF-8 key succeeded")
	}
	var deep any = openapi.Map[any]{}
	for range 10_001 { // beyond the encoder's nesting limit
		deep = openapi.Map[any]{"a": deep}
	}
	if _, err := json.Marshal(deep); err == nil {
		t.Error("marshal beyond the nesting limit succeeded")
	}
	if err := json.MarshalWrite(failWriter{}, openapi.Map[int]{"a": 1}); err == nil {
		t.Error("marshal to a failing writer succeeded")
	}
	var back openapi.Map[int]
	if err := json.Unmarshal(b, &back); err != nil || back["b"] != 1 {
		t.Errorf("unmarshal: %v %v", err, back)
	}
	// An empty map is an empty object; a nil one is omitted by omitzero.
	if b, _ := json.Marshal(openapi.Map[int]{}); string(b) != "{}" {
		t.Errorf("empty map %s", b)
	}
	if b, _ := json.Marshal(openapi.Response{Description: "d"}); string(b) != `{"description":"d"}` {
		t.Errorf("nil maps %s", b)
	}
}

func TestPathItemOperation(t *testing.T) {
	var nilItem *openapi.PathItem
	if nilItem.Operation("GET") != nil {
		t.Error("nil item")
	}
	ops := map[string]*openapi.OperationObject{}
	item := &openapi.PathItem{}
	for i, method := range []string{"GET", "PUT", "POST", "DELETE", "HEAD", "PATCH"} {
		ops[method] = &openapi.OperationObject{OperationID: strings.ToLower(method) + string(rune('0'+i))}
	}
	item.Get, item.Put, item.Post, item.Delete, item.Head, item.Patch = ops["GET"], ops["PUT"], ops["POST"], ops["DELETE"], ops["HEAD"], ops["PATCH"]
	for method, want := range ops {
		if got := item.Operation(method); got != want {
			t.Errorf("%s: %v", method, got)
		}
	}
	if item.Operation("OPTIONS") != ops["GET"] {
		t.Error("unknown method should fall back to GET")
	}
}
