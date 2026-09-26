package redisx

import (
	"reflect"
	"strings"
	"testing"
)

func TestCacheEntry_RoundTrip(t *testing.T) {
	cases := []struct {
		versions map[string]int64
		body     string
	}{
		{map[string]int64{"orders": 3, "customers": 0}, `{"a":1,"b":[1,2,3],"n":12345678901234567}`},
		{map[string]int64{}, `null`},
		{map[string]int64{`we"ird}tag\`: 7, "b": 1}, `{"b":{"v":{"b":1}}}`},
		{map[string]int64{"t": 1}, ``},
	}
	for _, c := range cases {
		entry := encodeCacheEntry(c.versions, []byte(c.body))
		if !strings.HasPrefix(string(entry), `{"v":{`) {
			t.Errorf("entry prefix: %s", entry)
		}
		vers, body, err := decodeCacheEntry(entry)
		if err != nil {
			t.Fatalf("%s: %v", entry, err)
		}
		if !reflect.DeepEqual(vers, c.versions) && (len(vers) != 0 || len(c.versions) != 0) {
			t.Errorf("versions = %v, want %v", vers, c.versions)
		}
		if string(body) != c.body {
			t.Errorf("body = %q, want %q", body, c.body)
		}
	}
	entry := encodeCacheEntry(map[string]int64{"b": 2, "a": 1}, []byte(`1`))
	if string(entry) != `{"v":{"a":1,"b":2},"b":1}` {
		t.Fatalf("entry = %s", entry)
	}
}

func TestCacheEntry_Malformed(t *testing.T) {
	for _, bad := range []string{``, `{}`, `{"v":{}`, `{"v":{},"x":1}`, `{"v":{"a":"x"},"b":1}`, `{"v":{"a":1}}`, `{"v":{"unterminated`} {
		if _, _, err := decodeCacheEntry([]byte(bad)); err == nil {
			t.Errorf("decodeCacheEntry(%q) accepted", bad)
		}
	}
}

func TestCacheKey(t *testing.T) {
	type q struct {
		B int `json:"b"`
		A int `json:"a"`
	}
	k1, err := CacheKey("GetOrder", q{A: 1, B: 2})
	if err != nil {
		t.Fatal(err)
	}
	k2, _ := CacheKey("GetOrder", map[string]int{"b": 2, "a": 1})
	if k1 != k2 {
		t.Fatalf("canonical hash differs: %s vs %s", k1, k2)
	}
	if !strings.HasPrefix(k1, "GetOrder:") || len(k1) != len("GetOrder:")+64 {
		t.Fatalf("key shape: %s", k1)
	}
	if _, err := CacheKey("X", make(chan int)); err == nil {
		t.Fatal("expected encode error")
	}
}
