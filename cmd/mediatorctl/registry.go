package main

import "github.com/t3stackcoder/go-api-backend/examples/orders/orders"

// registry supplies the built mediator for `names`, `openapi export`, and
// the default groups and topics of `consumer lag`. It is the example
// service's registry built with no infrastructure (spec 13): only the
// registrations are needed to derive names and generate the document.
var registry = orders.Registry

// openAPI is the generator configuration of `openapi export`; --title,
// --version, and --prefix override its fields.
var openAPI = orders.OpenAPIConfig()
