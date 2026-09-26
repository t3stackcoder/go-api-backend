package orders

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
)

// SystemSubject is the subject of the principal the consumers act as when
// they send commands through the mediator.
const SystemSubject = "system"

// systemPrincipal carries the permissions the consumers' nested commands
// require. Consumers run outside any HTTP request, so they attach it
// themselves before sending.
var systemPrincipal = authz.Principal{
	Subject:     SystemSubject,
	Roles:       []string{"system"},
	Permissions: []string{PermissionInventoryWrite},
}

// reserveInventory is the InventoryReserver: a durable consumer in group
// inventory. For every line of the submitted order it sends ReserveStock
// through the mediator from inside the consumer's unit of work; the nested
// Send joins that transaction, so the reservations, the inbox row, and the
// acknowledgement of the event are one atomic outcome (spec G7).
func (s *service) reserveInventory(ctx context.Context, e OrderSubmitted) error {
	tx, err := txFrom(ctx)
	if err != nil {
		return err
	}
	env, ok := mediator.EnvelopeFrom(ctx)
	if !ok {
		return mediator.E(mediator.CodeInternal, "consumer without an envelope")
	}
	rows, err := tx.Query(ctx, `SELECT sku, qty FROM order_lines WHERE order_id = $1 ORDER BY id`, e.OrderID)
	if err != nil {
		return dbErr("read lines", err)
	}
	// Collect before sending: the nested command reuses this connection.
	lines, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (OrderLine, error) {
		var l OrderLine
		err := row.Scan(&l.SKU, &l.Qty)
		return l, err
	})
	if err != nil {
		return dbErr("scan lines", err)
	}
	sysCtx := authz.WithPrincipal(ctx, systemPrincipal)
	for _, l := range lines {
		cmd := ReserveStock{RequestID: env.ID.String() + ":" + l.SKU, OrderID: e.OrderID, SKU: l.SKU, Qty: l.Qty}
		if _, err := mediator.Send(sysCtx, s.m, cmd); err != nil {
			return err
		}
	}
	return nil
}

// projectSummary is the OrderSummaryProjector: a durable consumer in group
// read_model that maintains order_summaries. last_seq is the envelope
// sequence of the applied event; an older event never overwrites a newer
// row, so redelivery in any order is harmless even without the inbox.
func (s *service) projectSummary(ctx context.Context, e OrderSubmitted) error {
	tx, err := txFrom(ctx)
	if err != nil {
		return err
	}
	env, ok := mediator.EnvelopeFrom(ctx)
	if !ok {
		return mediator.E(mediator.CodeInternal, "consumer without an envelope")
	}
	if s.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.delay):
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO order_summaries (order_id, customer_id, status, total, last_seq)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (order_id) DO UPDATE
    SET status = EXCLUDED.status, total = EXCLUDED.total, last_seq = EXCLUDED.last_seq, updated_at = now()
    WHERE order_summaries.last_seq < EXCLUDED.last_seq`,
		e.OrderID, e.CustomerID, StatusSubmitted, e.Total, env.Seq); err != nil {
		return dbErr("project summary", err)
	}
	return nil
}
