// Command mediatorctl is the operations CLI of spec 9.3: migrations, names,
// outbox, inbox, idempotency, consumer lag, dead letters, leases, and the
// OpenAPI export. Every command is a function in mediator/ctl; this binary
// only wires signals, arguments, and the registry.
//
// Configuration comes from the environment (PG_URL, REDIS_ADDR,
// REDIS_USERNAME, REDIS_PASSWORD, REDIS_DB, REDIS_PREFIX); run with no
// arguments for the command list.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/t3stackcoder/go-api-backend/mediator/ctl"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := ctl.Main(ctx, os.Args[1:], ctl.Options{Registry: registry, OpenAPI: openAPI})
	stop()
	os.Exit(code)
}
