package openapi

import "github.com/t3stackcoder/go-api-backend/mediator/validate"

// Info is the OpenAPI Info Object: what the document describes.
type Info struct {
	Title          string   `json:"title"`
	Summary        string   `json:"summary,omitzero"`
	Description    string   `json:"description,omitzero"`
	TermsOfService string   `json:"termsOfService,omitzero"`
	Contact        *Contact `json:"contact,omitzero"`
	License        *License `json:"license,omitzero"`
	Version        string   `json:"version"`
}

// Contact is the OpenAPI Contact Object.
type Contact struct {
	Name  string `json:"name,omitzero"`
	URL   string `json:"url,omitzero"`
	Email string `json:"email,omitzero"`
}

// License is the OpenAPI License Object. Name is required; Identifier is an
// SPDX expression and is mutually exclusive with URL.
type License struct {
	Name       string `json:"name"`
	Identifier string `json:"identifier,omitzero"`
	URL        string `json:"url,omitzero"`
}

// Server is the OpenAPI Server Object. Paths in the document include the
// httpapi prefix, so a server URL names the host root, for example
// "https://api.example.com".
type Server struct {
	URL         string `json:"url"`
	Description string `json:"description,omitzero"`
}

// Defaults applied by Generate.
const (
	// DefaultSecurityScheme is the name of the security scheme in
	// components.securitySchemes and in every operation's security entry.
	DefaultSecurityScheme = "bearerAuth"
	// DefaultTitle is used when Info.Title is empty.
	DefaultTitle = "API"
	// DefaultVersion is used when Info.Version is empty.
	DefaultVersion = "0.0.0"
)

// DefaultSecuritySchemeObject returns the default security scheme: an HTTP
// bearer scheme carrying a JWT. Each call returns a fresh map.
func DefaultSecuritySchemeObject() map[string]any {
	return map[string]any{"type": "http", "scheme": "bearer", "bearerFormat": "JWT"}
}

// Config configures Generate. The zero value is usable; see the defaults on
// each field.
type Config struct {
	// Info describes the API. Title defaults to DefaultTitle and Version to
	// DefaultVersion when empty.
	Info Info
	// Prefix is the httpapi mount prefix, for example "/api". It is part of
	// every path in the document.
	Prefix string
	// AllowUnknownFields mirrors httpapi.Config.AllowUnknownFields: when
	// set, object schemas omit additionalProperties: false. It applies to
	// every schema in the document so a type shared between a request and a
	// response has one definition.
	AllowUnknownFields bool
	// Validator supplies the schemas. A nil Validator means validate.New().
	// Use the validator the Validation behavior runs so the document shows
	// exactly the rules the server enforces.
	Validator *validate.Validator
	// SecurityScheme names the security scheme. Default DefaultSecurityScheme.
	SecurityScheme string
	// SecuritySchemeObject is the OpenAPI Security Scheme Object registered
	// under SecurityScheme. Default DefaultSecuritySchemeObject().
	SecuritySchemeObject map[string]any
	// Servers is copied into the document. Empty means the OpenAPI default
	// of a single server at "/".
	Servers []Server
	// NoDescriptions drops the descriptions taken from doc:"..." struct tags.
	// Descriptions are emitted by default.
	NoDescriptions bool
}

// withDefaults returns c with every default applied.
func (c Config) withDefaults() Config {
	if c.Info.Title == "" {
		c.Info.Title = DefaultTitle
	}
	if c.Info.Version == "" {
		c.Info.Version = DefaultVersion
	}
	if c.Validator == nil {
		c.Validator = validate.New()
	}
	if c.SecurityScheme == "" {
		c.SecurityScheme = DefaultSecurityScheme
	}
	if c.SecuritySchemeObject == nil {
		c.SecuritySchemeObject = DefaultSecuritySchemeObject()
	}
	return c
}
