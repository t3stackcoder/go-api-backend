package pg

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// DefaultLockTimeout is the SET LOCAL lock_timeout of a transaction when
// neither TxOptions nor the store configuration says otherwise (6.1).
const DefaultLockTimeout = 5 * time.Second

// StoreConfig configures PgStore.
type StoreConfig struct {
	// Partitions is P, the number of Redis streams per topic. Every node
	// must agree on it; CheckPartitions verifies it at startup. Default 1.
	Partitions int
	// Schema, when set, is applied with SET LOCAL search_path in every
	// transaction. Tests use it; production pools set search_path on the
	// connection instead (PoolConfig.SearchPath).
	Schema string
	// DefaultLockTimeout is used when TxOptions.LockTimeout is zero. Default 5s.
	DefaultLockTimeout time.Duration
}

// PgStore is the Store over a pgxpool.Pool.
type PgStore struct {
	pool        *pgxpool.Pool
	partitions  int
	schema      string
	lockTimeout time.Duration
	fencingSQL  string
}

// NewStore returns a Store over pool.
func NewStore(pool *pgxpool.Pool, cfg StoreConfig) *PgStore {
	s := &PgStore{pool: pool, partitions: cfg.Partitions, schema: cfg.Schema, lockTimeout: cfg.DefaultLockTimeout}
	if s.partitions <= 0 {
		s.partitions = 1
	}
	if s.lockTimeout <= 0 {
		s.lockTimeout = DefaultLockTimeout
	}
	// The fencing sequence is read outside a transaction, so SET LOCAL
	// search_path cannot reach it: qualify it when a schema is configured.
	s.fencingSQL = sqlFencingNext
	if s.schema != "" {
		s.fencingSQL = "SELECT nextval('" + strings.ReplaceAll(pgx.Identifier{s.schema, "mediator_fencing_seq"}.Sanitize(), "'", "''") + "')"
	}
	return s
}

// Pool returns the underlying pool.
func (s *PgStore) Pool() *pgxpool.Pool { return s.pool }

// Partitions returns P.
func (s *PgStore) Partitions() int { return s.partitions }

// Begin opens a transaction with the isolation and access mode of opts,
// SET LOCAL lock_timeout, and SET LOCAL idle_in_transaction_session_timeout
// from the context deadline, all in one round trip (6.1 step 2).
func (s *PgStore) Begin(ctx context.Context, opts TxOptions) (Tx, error) {
	if err := testkit.Fault(ctx, "pg.tx.begin"); err != nil {
		return nil, err
	}
	deadline, hasDeadline := ctx.Deadline()
	sql, err := beginSQL(opts, s.lockTimeout, s.schema, deadline, hasDeadline, time.Now())
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{BeginQuery: sql})
	if err != nil {
		return nil, fmt.Errorf("pg: begin: %w", err)
	}
	return &pgTx{tx: tx, readOnly: opts.ReadOnly, partitions: s.partitions}, nil
}

// beginSQL builds the multi-statement BEGIN of a unit of work. It is pure so
// tests can check every option without a database.
func beginSQL(opts TxOptions, defaultLock time.Duration, schema string, deadline time.Time, hasDeadline bool, now time.Time) (string, error) {
	iso := opts.Isolation
	if iso == "" {
		iso = pgx.ReadCommitted
	}
	switch iso {
	case pgx.ReadCommitted, pgx.RepeatableRead, pgx.Serializable, pgx.ReadUncommitted:
	default:
		return "", fmt.Errorf("pg: unknown isolation level %q", string(iso))
	}
	var b strings.Builder
	b.WriteString("BEGIN ISOLATION LEVEL ")
	b.WriteString(strings.ToUpper(string(iso)))
	if opts.ReadOnly {
		b.WriteString(" READ ONLY")
	} else {
		b.WriteString(" READ WRITE")
	}
	lock := opts.LockTimeout
	if lock <= 0 {
		lock = defaultLock
	}
	fmt.Fprintf(&b, "; SET LOCAL lock_timeout = %d", max(lock.Milliseconds(), 1))
	if hasDeadline {
		remaining := deadline.Sub(now).Milliseconds()
		if remaining < 1 {
			remaining = 1
		}
		fmt.Fprintf(&b, "; SET LOCAL idle_in_transaction_session_timeout = %d", remaining)
	}
	if schema != "" {
		fmt.Fprintf(&b, "; SET LOCAL search_path = %s", pgx.Identifier{schema}.Sanitize())
	}
	return b.String(), nil
}

// NextFencingToken returns nextval('mediator_fencing_seq') (7.2).
func (s *PgStore) NextFencingToken(ctx context.Context) (int64, error) {
	if err := testkit.Fault(ctx, "pg.lease.epoch"); err != nil {
		return 0, err
	}
	var n int64
	if err := s.pool.QueryRow(ctx, s.fencingSQL).Scan(&n); err != nil {
		return 0, fmt.Errorf("pg: fencing token: %w", err)
	}
	return n, nil
}

// Ping reports whether the database answers.
func (s *PgStore) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }
