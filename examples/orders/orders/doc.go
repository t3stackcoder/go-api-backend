// Package orders is the example service of spec section 13: a small but
// complete order service that exercises every feature of the mediator
// framework and doubles as the fixture behind api/openapi.json.
//
// The package is a library. Register puts every request, event, handler,
// and consumer on a mediator; NewMediator builds one with the standard
// behavior set over the infrastructure in Deps; Registry builds one with no
// infrastructure at all, which is what mediatorctl uses for `names` and
// `openapi export`. examples/orders/main.go is the process that wires the
// package to Postgres, Redis, and HTTP.
//
// Every handler writes through pg.TxFrom(ctx): the unit of work behavior
// owns the transaction, so a command's rows, its outbox rows, the audit row
// of the in-process handler, and the idempotency reservation commit or roll
// back together (spec G4).
package orders
