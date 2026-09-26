// Package integration holds the cross-package scenarios of the integration
// tier (spec 11.5) that run the example service of spec section 13 as a
// child process against real Postgres and Redis: runtime shutdown ordering
// (G16), consumer acknowledgement after commit (G7), the served OpenAPI
// document against the committed api/openapi.json, readiness reflecting a
// Redis outage (spec 9.2), and the order flow end to end over HTTP.
//
// Every test file carries the integration build tag. TestMain starts one
// postgres:18 and one redis:8 container through testcontainers (or uses
// PG_URL and REDIS_ADDR when set) and builds examples/orders once; each test
// then owns a database and a Redis key prefix so that nothing is shared
// between tests. Run with:
//
//	go test -tags integration -count=1 ./test/integration/...
package integration
