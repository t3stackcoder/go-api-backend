package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json/v2"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
)

// devSecret signs tokens when JWT_SECRET is unset. It is for local runs
// only; the process logs a warning when it falls back to it.
const devSecret = "orders-dev-secret-change-me"

// claims are the JWT claims the service understands. Everything else in
// the payload is kept in Principal.Claims.
type claims struct {
	Subject     string   `json:"sub"`
	Roles       []string `json:"roles"`
	Permissions []string `json:"permissions"`
	Tenant      string   `json:"tenant"`
	ExpiresAt   int64    `json:"exp"`
	NotBefore   int64    `json:"nbf"`
}

// newAuthenticator returns the httpapi authenticator: Bearer tokens are
// HS256 JWTs signed with JWT_SECRET (or devSecret with a warning).
func newAuthenticator(logger *slog.Logger) func(*http.Request) (authz.Principal, error) {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		secret = devSecret
		logger.Warn("orders: JWT_SECRET is not set; using the development secret")
	}
	key := []byte(secret)
	return httpapi.BearerAuthenticator(func(token string) (authz.Principal, error) {
		return verifyHS256(token, key, time.Now())
	})
}

// verifyHS256 checks the signature and validity window of an HS256 JWT and
// maps its claims to a principal, with the standard library only.
func verifyHS256(token string, key []byte, now time.Time) (authz.Principal, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return authz.Principal{}, unauthorized("token must have three segments")
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return authz.Principal{}, unauthorized("token header is not base64url")
	}
	var h struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(header, &h); err != nil || h.Alg != "HS256" {
		return authz.Principal{}, unauthorized("token algorithm must be HS256")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return authz.Principal{}, unauthorized("token signature is not base64url")
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return authz.Principal{}, unauthorized("token signature is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return authz.Principal{}, unauthorized("token payload is not base64url")
	}
	var c claims
	if err := json.Unmarshal(payload, &c); err != nil {
		return authz.Principal{}, unauthorized("token payload is not a JSON object")
	}
	var all map[string]any
	if err := json.Unmarshal(payload, &all); err != nil {
		return authz.Principal{}, unauthorized("token payload is not a JSON object")
	}
	unix := now.Unix()
	if c.ExpiresAt != 0 && unix >= c.ExpiresAt {
		return authz.Principal{}, unauthorized("token has expired")
	}
	if c.NotBefore != 0 && unix < c.NotBefore {
		return authz.Principal{}, unauthorized("token is not valid yet")
	}
	if c.Subject == "" {
		return authz.Principal{}, unauthorized("token has no subject")
	}
	return authz.Principal{Subject: c.Subject, Roles: c.Roles, Permissions: c.Permissions, Tenant: c.Tenant, Claims: all}, nil
}

// unauthorized returns a CodeUnauthorized error with a client-safe message.
func unauthorized(msg string) error {
	return mediator.Wrap(mediator.CodeUnauthorized, msg, errors.New("jwt: "+msg))
}
