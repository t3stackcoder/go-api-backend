package httpapi

import (
	"errors"
	"net/http"
	"strings"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
)

// BearerAuthenticator adapts a token verifier to Config.Authenticator. A
// missing Authorization header yields the anonymous principal; a header that
// is not "Bearer <token>" is CodeUnauthorized; otherwise verify decides. The
// scheme is matched case-insensitively.
func BearerAuthenticator(verify func(token string) (authz.Principal, error)) func(*http.Request) (authz.Principal, error) {
	return func(r *http.Request) (authz.Principal, error) {
		h := r.Header.Get("Authorization")
		if h == "" {
			return authz.Principal{}, nil
		}
		scheme, token, ok := strings.Cut(h, " ")
		token = strings.TrimSpace(token)
		if !ok || !strings.EqualFold(scheme, "Bearer") || token == "" {
			return authz.Principal{}, mediator.E(mediator.CodeUnauthorized, "Authorization header must be Bearer <token>")
		}
		return verify(token)
	}
}

// unauthorized maps an authenticator error to CodeUnauthorized. A
// *mediator.Error that already carries that code is returned unchanged so
// the verifier can choose the message; anything else is wrapped with a fixed
// message and the cause is kept for logs only.
func unauthorized(err error) error {
	var me *mediator.Error
	if errors.As(err, &me) && me.Code == mediator.CodeUnauthorized {
		return err
	}
	return mediator.Wrap(mediator.CodeUnauthorized, "authentication failed", err)
}
