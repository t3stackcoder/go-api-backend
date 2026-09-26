package orders

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is the application DDL. Every statement is idempotent so Migrate
// may run on every node start; the framework tables come from pg.Migrate.
const Schema = `
-- Write model.
CREATE TABLE IF NOT EXISTS orders (
    id           UUID        PRIMARY KEY,
    customer_id  UUID        NOT NULL,
    status       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (customer_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS order_lines (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id   UUID   NOT NULL REFERENCES orders (id),
    sku        TEXT   NOT NULL,
    qty        INT    NOT NULL,
    unit_price BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS order_lines_order_idx ON order_lines (order_id, id);

-- Per-order event log tailed by WatchOrder. Writers of one order serialize
-- on the orders row lock, so ids ascend in commit order within an order.
CREATE TABLE IF NOT EXISTS order_events (
    id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id UUID        NOT NULL,
    type     TEXT        NOT NULL,
    data     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS order_events_order_idx ON order_events (order_id, id);

-- Written by the in-process AuditLogger in the publisher's transaction.
CREATE TABLE IF NOT EXISTS audit_log (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id       UUID        NOT NULL,
    action         TEXT        NOT NULL,
    actor          TEXT        NOT NULL,
    correlation_id TEXT        NOT NULL,
    at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Inventory, maintained by ReserveStock.
CREATE TABLE IF NOT EXISTS inventory (
    sku       TEXT   PRIMARY KEY,
    available BIGINT NOT NULL CHECK (available >= 0)
);

CREATE TABLE IF NOT EXISTS reservations (
    id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    request_id TEXT        NOT NULL UNIQUE,
    order_id   UUID        NOT NULL,
    sku        TEXT        NOT NULL,
    qty        INT         NOT NULL,
    status     TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS reservations_order_idx ON reservations (order_id, id);

-- Read model maintained by the OrderSummaryProjector consumer; last_seq is
-- the envelope sequence of the last event applied to the row.
CREATE TABLE IF NOT EXISTS order_summaries (
    order_id    UUID        PRIMARY KEY,
    customer_id UUID        NOT NULL,
    status      TEXT        NOT NULL,
    total       BIGINT      NOT NULL,
    last_seq    BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

// Tables lists every application table, for tests and operators.
var Tables = []string{"orders", "order_lines", "order_events", "audit_log", "inventory", "reservations", "order_summaries"}

// Migrate applies Schema. It is idempotent.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("orders: migrate: %w", err)
	}
	return nil
}

// SeedInventory upserts the available stock of each SKU.
func SeedInventory(ctx context.Context, pool *pgxpool.Pool, stock map[string]int64) error {
	for sku, n := range stock {
		if _, err := pool.Exec(ctx, `INSERT INTO inventory (sku, available) VALUES ($1, $2)
ON CONFLICT (sku) DO UPDATE SET available = EXCLUDED.available`, sku, n); err != nil {
			return fmt.Errorf("orders: seed inventory %s: %w", sku, err)
		}
	}
	return nil
}
