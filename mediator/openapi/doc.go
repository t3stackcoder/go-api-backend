// Package openapi generates the OpenAPI 3.1 document of a built mediator
// (spec 8.6) and serves it together with an embedded Scalar reference page.
//
// Generate walks the routing table the httpapi package derives from the
// registry. Every request becomes one operation: its operationId is the
// request name in lowerCamelCase, fields bound from the path, query string,
// or headers become parameters, the rest of the request struct becomes the
// JSON body schema, the response type becomes the success response, and the
// traits contribute the remainder. Requires() adds a security requirement
// and the 401 and 403 responses, RateLimit() adds 429, commands add the
// idempotency codes, and Describe() adds the summary, description, tags,
// deprecation flag, and further error codes. Type mapping and validation
// constraints are the job of the validate package; this package assembles.
//
// The document is validated against the embedded OpenAPI 3.1 schema before
// Generate returns it, and it marshals byte for byte identically across runs
// and platforms, so committing it and diffing in CI is meaningful.
//
// Streams are described as text/event-stream responses whose media type
// carries the item schema under x-sse-item. OpenAPI 3.2 names the same
// member itemSchema; moving to 3.2 is a rename once the frontend toolchain
// supports it (spec 15.1).
//
// The reference page loads the Scalar viewer from the jsDelivr CDN in the
// browser; the server embeds only the HTML shell.
package openapi
