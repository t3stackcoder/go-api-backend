package orders

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// Querier is the read side WatchOrder tails with. It has no unit of work,
// so it reads through a pool rather than the ambient transaction;
// *pgxpool.Pool satisfies it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ErrNoTransaction is the cause of the internal error every handler returns
// when it runs outside a unit of work: the UnitOfWork behavior must wrap it.
var ErrNoTransaction = errors.New("orders: no transaction in context; the unit of work behavior must wrap the handler")

// service holds what the handlers need beyond the request.
type service struct {
	m       *mediator.Mediator
	querier Querier
	poll    time.Duration
	delay   time.Duration
	nodeID  string
}

// txFrom returns the ambient pgx transaction or an internal error.
func txFrom(ctx context.Context) (pgx.Tx, error) {
	tx, ok := pg.TxFrom(ctx)
	if !ok {
		return nil, mediator.Wrap(mediator.CodeInternal, "handler outside a unit of work", ErrNoTransaction)
	}
	return tx, nil
}

// dbErr wraps a database failure as an internal error; the text never
// reaches clients (spec 4.9) but transient classification still applies to
// the cause, which is what Retry consults.
func dbErr(op string, err error) error {
	return mediator.Wrap(mediator.CodeInternal, "database operation failed", fmt.Errorf("orders: %s: %w", op, err))
}

// lockOrder locks the order row for update and returns its status.
func lockOrder(ctx context.Context, tx pgx.Tx, orderID string) (Status, error) {
	var status Status
	err := tx.QueryRow(ctx, `SELECT status FROM orders WHERE id = $1 FOR UPDATE`, orderID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", mediator.E(mediator.CodeNotFound, "order not found")
	}
	if err != nil {
		return "", dbErr("lock order", err)
	}
	return status, nil
}

// appendEvent writes one order_events row.
func appendEvent(ctx context.Context, tx pgx.Tx, orderID, typ string, data map[string]any) error {
	if _, err := tx.Exec(ctx, `INSERT INTO order_events (order_id, type, data) VALUES ($1, $2, $3)`, orderID, typ, data); err != nil {
		return dbErr("append event", err)
	}
	return nil
}

func (s *service) createOrder(ctx context.Context, c CreateOrder) (CreateOrderResult, error) {
	tx, err := txFrom(ctx)
	if err != nil {
		return CreateOrderResult{}, err
	}
	id := mediator.NewID(s.m.Clock().Now()).String()
	if _, err := tx.Exec(ctx, `INSERT INTO orders (id, customer_id, status) VALUES ($1, $2, $3)`, id, c.CustomerID, StatusDraft); err != nil {
		return CreateOrderResult{}, dbErr("insert order", err)
	}
	for _, l := range c.Lines {
		if _, err := tx.Exec(ctx, `INSERT INTO order_lines (order_id, sku, qty, unit_price) VALUES ($1, $2, $3, $4)`, id, l.SKU, l.Qty, l.UnitPrice); err != nil {
			return CreateOrderResult{}, dbErr("insert line", err)
		}
	}
	if err := appendEvent(ctx, tx, id, EventCreated, map[string]any{"customerId": c.CustomerID, "lines": len(c.Lines)}); err != nil {
		return CreateOrderResult{}, err
	}
	return CreateOrderResult{OrderID: id}, nil
}

func (s *service) addLine(ctx context.Context, c AddLine) (mediator.Void, error) {
	tx, err := txFrom(ctx)
	if err != nil {
		return mediator.Void{}, err
	}
	status, err := lockOrder(ctx, tx, c.OrderID)
	if err != nil {
		return mediator.Void{}, err
	}
	if status != StatusDraft {
		return mediator.Void{}, mediator.E(mediator.CodePrecondition, "order is not a draft").WithDetail("status", string(status))
	}
	if _, err := tx.Exec(ctx, `INSERT INTO order_lines (order_id, sku, qty, unit_price) VALUES ($1, $2, $3, $4)`, c.OrderID, c.SKU, c.Qty, c.UnitPrice); err != nil {
		return mediator.Void{}, dbErr("insert line", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE orders SET updated_at = now() WHERE id = $1`, c.OrderID); err != nil {
		return mediator.Void{}, dbErr("touch order", err)
	}
	err = appendEvent(ctx, tx, c.OrderID, EventLineAdded, map[string]any{"sku": c.SKU, "qty": c.Qty, "unitPrice": c.UnitPrice})
	return mediator.Void{}, err
}

func (s *service) submitOrder(ctx context.Context, c SubmitOrder) (mediator.Void, error) {
	tx, err := txFrom(ctx)
	if err != nil {
		return mediator.Void{}, err
	}
	status, err := lockOrder(ctx, tx, c.OrderID)
	if err != nil {
		return mediator.Void{}, err
	}
	if status != StatusDraft {
		return mediator.Void{}, mediator.E(mediator.CodePrecondition, "order is not a draft").WithDetail("status", string(status))
	}
	var customerID string
	var lines int
	var total int64
	if err := tx.QueryRow(ctx, `SELECT o.customer_id, count(l.id), coalesce(sum(l.qty * l.unit_price), 0)
FROM orders o LEFT JOIN order_lines l ON l.order_id = o.id WHERE o.id = $1 GROUP BY o.customer_id`, c.OrderID).Scan(&customerID, &lines, &total); err != nil {
		return mediator.Void{}, dbErr("total", err)
	}
	if lines == 0 {
		return mediator.Void{}, mediator.E(mediator.CodePrecondition, "order has no lines")
	}
	if _, err := tx.Exec(ctx, `UPDATE orders SET status = $2, submitted_at = now(), updated_at = now() WHERE id = $1`, c.OrderID, StatusSubmitted); err != nil {
		return mediator.Void{}, dbErr("submit order", err)
	}
	if err := appendEvent(ctx, tx, c.OrderID, EventSubmitted, map[string]any{"total": total, "lines": lines}); err != nil {
		return mediator.Void{}, err
	}
	// The outbox row and the AuditLogger's row join this transaction.
	err = mediator.Publish(ctx, s.m, OrderSubmitted{OrderID: c.OrderID, CustomerID: customerID, Total: total})
	return mediator.Void{}, err
}

// auditSubmitted is the AuditLogger: an in-process handler that writes the
// audit row in the transaction of the command that published the event.
func (s *service) auditSubmitted(ctx context.Context, e OrderSubmitted) error {
	tx, err := txFrom(ctx)
	if err != nil {
		return err
	}
	actor := authz.PrincipalFrom(ctx).Subject
	if actor == "" {
		actor = "anonymous"
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_log (order_id, action, actor, correlation_id) VALUES ($1, $2, $3, $4)`,
		e.OrderID, "order.submitted", actor, mediator.CorrelationID(ctx)); err != nil {
		return dbErr("audit", err)
	}
	return nil
}

func (s *service) getOrder(ctx context.Context, q GetOrder) (OrderView, error) {
	tx, err := txFrom(ctx)
	if err != nil {
		return OrderView{}, err
	}
	v := OrderView{OrderID: q.OrderID, Lines: []OrderLine{}}
	err = tx.QueryRow(ctx, `SELECT customer_id, status, created_at, submitted_at FROM orders WHERE id = $1`, q.OrderID).
		Scan(&v.CustomerID, &v.Status, &v.CreatedAt, &v.SubmittedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return OrderView{}, mediator.E(mediator.CodeNotFound, "order not found")
	}
	if err != nil {
		return OrderView{}, dbErr("get order", err)
	}
	rows, err := tx.Query(ctx, `SELECT sku, qty, unit_price FROM order_lines WHERE order_id = $1 ORDER BY id`, q.OrderID)
	if err != nil {
		return OrderView{}, dbErr("get lines", err)
	}
	lines, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (OrderLine, error) {
		var l OrderLine
		err := row.Scan(&l.SKU, &l.Qty, &l.UnitPrice)
		return l, err
	})
	if err != nil {
		return OrderView{}, dbErr("scan lines", err)
	}
	v.Lines = lines
	for _, l := range lines {
		v.Total += int64(l.Qty) * l.UnitPrice
	}
	return v, nil
}

func (s *service) listOrders(ctx context.Context, q ListOrders) (Page[OrderSummary], error) {
	tx, err := txFrom(ctx)
	if err != nil {
		return Page[OrderSummary]{}, err
	}
	page, size := 1, DefaultPageSize
	if q.Page != nil {
		page = *q.Page
	}
	if q.Size != nil {
		size = *q.Size
	}
	out := Page[OrderSummary]{Items: []OrderSummary{}, Page: page, Size: size}
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM orders WHERE customer_id = $1`, q.CustomerID).Scan(&out.Total); err != nil {
		return Page[OrderSummary]{}, dbErr("count orders", err)
	}
	rows, err := tx.Query(ctx, `SELECT o.id, o.customer_id, o.status, o.created_at, count(l.id), coalesce(sum(l.qty * l.unit_price), 0)
FROM orders o LEFT JOIN order_lines l ON l.order_id = o.id
WHERE o.customer_id = $1
GROUP BY o.id ORDER BY o.created_at DESC, o.id DESC LIMIT $2 OFFSET $3`, q.CustomerID, size, (page-1)*size)
	if err != nil {
		return Page[OrderSummary]{}, dbErr("list orders", err)
	}
	out.Items, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (OrderSummary, error) {
		var o OrderSummary
		err := row.Scan(&o.OrderID, &o.CustomerID, &o.Status, &o.CreatedAt, &o.LineCount, &o.Total)
		return o, err
	})
	if err != nil {
		return Page[OrderSummary]{}, dbErr("scan orders", err)
	}
	return out, nil
}

// watchOrder tails order_events for one order with short reads on the
// querier every poll interval, honoring ctx, and ends after the submitted
// event (or at once when the order is already submitted and nothing is left
// to replay).
func (s *service) watchOrder(ctx context.Context, q WatchOrder) iter.Seq2[OrderEvent, error] {
	return func(yield func(OrderEvent, error) bool) {
		if s.querier == nil {
			yield(OrderEvent{}, mediator.E(mediator.CodeInternal, "WatchOrder needs a Querier; see Deps"))
			return
		}
		last, _ := parseEventID(q.LastEventID)
		var status Status
		err := s.querier.QueryRow(ctx, `SELECT status FROM orders WHERE id = $1`, q.OrderID).Scan(&status)
		if errors.Is(err, pgx.ErrNoRows) {
			yield(OrderEvent{}, mediator.E(mediator.CodeNotFound, "order not found"))
			return
		}
		if err != nil {
			if ctx.Err() == nil {
				yield(OrderEvent{}, dbErr("watch order", err))
			}
			return
		}
		for {
			events, err := s.eventsAfter(ctx, q.OrderID, last)
			if err != nil {
				if ctx.Err() == nil {
					yield(OrderEvent{}, err)
				}
				return
			}
			for _, e := range events {
				if !yield(e, nil) {
					return
				}
				last = e.ID
				if e.Type == EventSubmitted {
					return
				}
			}
			if len(events) == 0 && status == StatusSubmitted {
				return // nothing left to replay and no more events will come
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.poll):
			}
		}
	}
}

// eventsAfter reads the next batch of events of an order.
func (s *service) eventsAfter(ctx context.Context, orderID string, after int64) ([]OrderEvent, error) {
	rows, err := s.querier.Query(ctx, `SELECT id, type, data, at FROM order_events WHERE order_id = $1 AND id > $2 ORDER BY id LIMIT 100`, orderID, after)
	if err != nil {
		return nil, dbErr("read events", err)
	}
	events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (OrderEvent, error) {
		e := OrderEvent{OrderID: orderID}
		err := row.Scan(&e.ID, &e.Type, &e.Data, &e.At)
		return e, err
	})
	if err != nil {
		return nil, dbErr("scan events", err)
	}
	return events, nil
}

func (s *service) reserveStock(ctx context.Context, c ReserveStock) (ReserveStockResult, error) {
	tx, err := txFrom(ctx)
	if err != nil {
		return ReserveStockResult{}, err
	}
	res := ReserveStockResult{Available: -1}
	err = tx.QueryRow(ctx, `SELECT available FROM inventory WHERE sku = $1 FOR UPDATE`, c.SKU).Scan(&res.Available)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return ReserveStockResult{}, dbErr("lock inventory", err)
	}
	status := "rejected"
	if err == nil && res.Available >= int64(c.Qty) {
		res.Available -= int64(c.Qty)
		res.Reserved = true
		status = "reserved"
		if _, err := tx.Exec(ctx, `UPDATE inventory SET available = $2 WHERE sku = $1`, c.SKU, res.Available); err != nil {
			return ReserveStockResult{}, dbErr("update inventory", err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO reservations (request_id, order_id, sku, qty, status) VALUES ($1, $2, $3, $4, $5)`,
		c.RequestID, c.OrderID, c.SKU, c.Qty, status); err != nil {
		return ReserveStockResult{}, dbErr("insert reservation", err)
	}
	return res, nil
}
