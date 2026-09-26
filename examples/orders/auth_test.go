package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/v2"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// mint signs an HS256 token over the claims with key, for the tests.
func mint(t *testing.T, key []byte, claims map[string]any, alg string) string {
	t.Helper()
	header, _ := json.Marshal(map[string]string{"alg": alg, "typ": "JWT"})
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(signing))
	return signing + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func TestVerifyHS256(t *testing.T) {
	key := []byte("k")
	now := time.Unix(1_700_000_000, 0)
	good := map[string]any{"sub": "alice", "roles": []string{"admin"}, "permissions": []string{"orders:write"}, "tenant": "t1",
		"exp": now.Unix() + 60, "nbf": now.Unix() - 60, "extra": "x"}
	p, err := verifyHS256(mint(t, key, good, "HS256"), key, now)
	if err != nil {
		t.Fatal(err)
	}
	if p.Subject != "alice" || p.Tenant != "t1" || len(p.Roles) != 1 || p.Roles[0] != "admin" || !p.HasPermission("orders:write") || p.Claims["extra"] != "x" {
		t.Errorf("principal %+v", p)
	}
	cases := map[string]string{
		"two segments":    "a.b",
		"bad header b64":  "!!!.b.c",
		"bad sig b64":     mint(t, key, good, "HS256")[:len(mint(t, key, good, "HS256"))-3] + "!!!",
		"wrong key":       mint(t, []byte("other"), good, "HS256"),
		"wrong alg":       mint(t, key, good, "none"),
		"expired":         mint(t, key, map[string]any{"sub": "a", "exp": now.Unix() - 1}, "HS256"),
		"not yet valid":   mint(t, key, map[string]any{"sub": "a", "nbf": now.Unix() + 1}, "HS256"),
		"no subject":      mint(t, key, map[string]any{"exp": now.Unix() + 1}, "HS256"),
		"payload not obj": mint(t, key, nil, "HS256"),
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := verifyHS256(token, key, now); mediator.CodeOf(err) != mediator.CodeUnauthorized {
				t.Errorf("want unauthorized, got %v", err)
			}
		})
	}
	// A payload that is not base64url but a signature that matches it.
	parts := strings.Split(mint(t, key, good, "HS256"), ".")
	forged := parts[0] + ".!!!"
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(forged))
	forged += "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if _, err := verifyHS256(forged, key, now); mediator.CodeOf(err) != mediator.CodeUnauthorized {
		t.Errorf("bad payload b64: %v", err)
	}
}

func TestNewAuthenticator(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	t.Setenv("JWT_SECRET", "")
	auth := newAuthenticator(logger)
	if !strings.Contains(buf.String(), "development secret") {
		t.Error("missing warning for the development secret")
	}
	r := httptest.NewRequest("GET", "/", nil)
	if p, err := auth(r); err != nil || !p.IsAnonymous() {
		t.Errorf("no header: %+v %v", p, err)
	}
	r.Header.Set("Authorization", "Bearer "+mint(t, []byte(devSecret), map[string]any{"sub": "bob"}, "HS256"))
	if p, err := auth(r); err != nil || p.Subject != "bob" {
		t.Errorf("dev token: %+v %v", p, err)
	}
	t.Setenv("JWT_SECRET", "prod")
	buf.Reset()
	auth = newAuthenticator(logger)
	if buf.Len() != 0 {
		t.Error("no warning with JWT_SECRET set")
	}
	if _, err := auth(r); mediator.CodeOf(err) != mediator.CodeUnauthorized {
		t.Errorf("dev token against prod secret: %v", err)
	}
	r.Header.Set("Authorization", "Bearer "+mint(t, []byte("prod"), map[string]any{"sub": "bob"}, "HS256"))
	if p, err := auth(r); err != nil || p.Subject != "bob" {
		t.Errorf("prod token: %+v %v", p, err)
	}
}

func TestEnvHelpers(t *testing.T) {
	t.Setenv("ORDERS_TEST_X", "")
	if envOr("ORDERS_TEST_X", "d") != "d" {
		t.Error("default")
	}
	t.Setenv("ORDERS_TEST_X", "v")
	if envOr("ORDERS_TEST_X", "d") != "v" {
		t.Error("set")
	}
	t.Setenv("ORDERS_TEST_D", "150ms")
	if envDuration("ORDERS_TEST_D") != 150*time.Millisecond {
		t.Error("duration")
	}
	t.Setenv("ORDERS_TEST_D", "junk")
	if envDuration("ORDERS_TEST_D") != 0 {
		t.Error("malformed duration")
	}
}

func TestReadyChecks_RuntimeHealthy(t *testing.T) {
	var rt mediator.Runtime
	checks := readyChecks(nil, nil, redisx.Config{}, nil, &rt)
	if len(checks) != 4 {
		t.Fatalf("%d checks", len(checks))
	}
	if err := checks[2](context.Background()); err != nil {
		t.Errorf("empty runtime is healthy: %v", err)
	}
	rt.HTTP = mediator.ComponentFunc(func(context.Context) error { return nil })
	if err := checks[2](context.Background()); err != nil {
		t.Errorf("component func is healthy: %v", err)
	}
}
