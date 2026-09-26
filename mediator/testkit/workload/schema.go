// Package workload is the application under test of the fault-sweep and
// chaos tiers (spec 11.4 scenarios, 11.6 workloads table): a register, a
// bank, an idempotent append list, a bump counter with durable events, and
// the consumers that project those events. cmd/chaosnode and test/faultsweep
// register it on a mediator; testkit/invariants reads its tables directly.
//
// Every handler writes through pg.TxFrom(ctx), never through a pool, so that
// the atomicity guarantees of spec 10 (G4, G6, G7, G8) are what the
// invariants observe. Every command records a marker row in wl_cmd_log in
// its own transaction, every durable event carries the command ID in the
// envelope header HeaderCmd, and every keyed command increments the
// execution counter of wl_executions; those three are what I1 and I4 join
// on.
package workload

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema is the DDL of every workload table. All tables carry the wl_
// prefix and the statements are idempotent, so Migrate may run on every
// node start.
const Schema = `
-- Register workload (spec 11.6 register, register-cached, remote-send).
-- updated_by is the ID of the command that wrote the row (I1).
CREATE TABLE IF NOT EXISTS wl_register (
    key        TEXT   PRIMARY KEY,
    val        BIGINT NOT NULL,
    updated_by TEXT   NOT NULL
);

-- Bank workload (spec 11.6 bank, bank-idempotent); seeded by SeedBank.
CREATE TABLE IF NOT EXISTS wl_bank (
    account TEXT   PRIMARY KEY,
    balance BIGINT NOT NULL
);

-- Idempotent append workload: one row per applied Append, in apply order.
CREATE TABLE IF NOT EXISTS wl_appends (
    cmd_id  TEXT      PRIMARY KEY,
    key     TEXT      NOT NULL,
    val     BIGINT    NOT NULL,
    applied BIGSERIAL NOT NULL
);
CREATE INDEX IF NOT EXISTS wl_appends_key_idx ON wl_appends (key, applied);

-- Execution counter of every keyed command (I4): scope is the command name,
-- key its idempotency key. Incremented in the handler transaction.
CREATE TABLE IF NOT EXISTS wl_executions (
    scope TEXT NOT NULL,
    key   TEXT NOT NULL,
    n     INT  NOT NULL DEFAULT 0,
    PRIMARY KEY (scope, key)
);

-- Events workload: the counter each Bump increments and publishes.
CREATE TABLE IF NOT EXISTS wl_bumps (
    key TEXT   PRIMARY KEY,
    n   BIGINT NOT NULL
);

-- Projection maintained by the read-model consumer per (group, key).
CREATE TABLE IF NOT EXISTS wl_projection (
    grp      TEXT   NOT NULL,
    key      TEXT   NOT NULL,
    count    BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    PRIMARY KEY (grp, key)
);

-- Apply log of every consumer: apply order (I6) and the fencing token and
-- node of each apply (G14). No unique constraint on (grp, event_id): a
-- double apply must be visible to the invariants, not fail the handler.
CREATE TABLE IF NOT EXISTS wl_applied (
    applied  BIGSERIAL PRIMARY KEY,
    grp      TEXT   NOT NULL,
    event_id UUID   NOT NULL,
    key      TEXT   NOT NULL,
    seq      BIGINT NOT NULL,
    fencing  BIGINT NOT NULL,
    node     TEXT   NOT NULL
);
CREATE INDEX IF NOT EXISTS wl_applied_grp_key_idx ON wl_applied (grp, key, applied);

-- Audit row per event written by the audit consumer (I3).
CREATE TABLE IF NOT EXISTS wl_audit (
    grp      TEXT   NOT NULL,
    event_id UUID   NOT NULL,
    key      TEXT   NOT NULL,
    seq      BIGINT NOT NULL,
    PRIMARY KEY (grp, event_id)
);

-- Marker row written in every command's transaction (I1). request_id is
-- mediator.RequestID(ctx), which is the causation ID of the outbox rows the
-- command published.
CREATE TABLE IF NOT EXISTS wl_cmd_log (
    cmd_id     TEXT        PRIMARY KEY,
    name       TEXT        NOT NULL,
    key        TEXT        NOT NULL,
    node       TEXT        NOT NULL,
    request_id TEXT        NOT NULL,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Row written by the in-process AtomicDone handler in the publisher's
-- transaction (I1, G4).
CREATE TABLE IF NOT EXISTS wl_side (
    cmd_id TEXT PRIMARY KEY,
    note   TEXT NOT NULL
);
`

// Tables lists every workload table, for truncation and reports.
var Tables = []string{
	"wl_register", "wl_bank", "wl_appends", "wl_executions", "wl_bumps",
	"wl_projection", "wl_applied", "wl_audit", "wl_cmd_log", "wl_side",
}

// Migrate applies Schema. It is idempotent and safe to run concurrently
// from several nodes: every statement is CREATE ... IF NOT EXISTS.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, Schema); err != nil {
		return fmt.Errorf("workload: migrate: %w", err)
	}
	return nil
}

// Bank seed of spec 11.6: four accounts of 100.
var (
	Accounts       = []string{"a", "b", "c", "d"}
	InitialBalance = int64(100)
)

// BankTotal is the conserved sum of every balance.
func BankTotal() int64 { return InitialBalance * int64(len(Accounts)) }

// SeedBank inserts the four accounts with their initial balance when they
// do not exist yet.
func SeedBank(ctx context.Context, pool *pgxpool.Pool) error {
	for _, a := range Accounts {
		if _, err := pool.Exec(ctx, `INSERT INTO wl_bank (account, balance) VALUES ($1, $2) ON CONFLICT (account) DO NOTHING`, a, InitialBalance); err != nil {
			return fmt.Errorf("workload: seed bank: %w", err)
		}
	}
	return nil
}

// Truncate empties every workload table so a scenario starts clean. The
// framework tables are left alone.
func Truncate(ctx context.Context, pool *pgxpool.Pool) error {
	for _, t := range Tables {
		if _, err := pool.Exec(ctx, "TRUNCATE "+t+" RESTART IDENTITY"); err != nil {
			return fmt.Errorf("workload: truncate %s: %w", t, err)
		}
	}
	return nil
}
