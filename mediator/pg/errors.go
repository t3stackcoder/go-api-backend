package pg

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Sentinel errors shared by PgStore and the in-memory store so that behavior
// logic classifies both the same way.
var (
	// ErrLockTimeout is returned by the in-memory store when a row lock is
	// not obtained within the transaction's LockTimeout. IsLockTimeout also
	// recognizes the Postgres SQLSTATE 55P03.
	ErrLockTimeout = errors.New("pg: lock timeout")
	// ErrReadOnly is returned by the in-memory store for a write inside a
	// read-only transaction. IsReadOnly also recognizes SQLSTATE 25006.
	ErrReadOnly = errors.New("pg: cannot write in a read-only transaction")
	// ErrTxClosed is returned by every Tx method after Commit or Rollback.
	ErrTxClosed = errors.New("pg: transaction is closed")
	// ErrTxAborted is returned by Commit when an earlier statement of the
	// transaction failed; the transaction is rolled back instead.
	ErrTxAborted = errors.New("pg: transaction is aborted")
	// ErrNoUnitOfWork is returned by behaviors that need the ambient
	// transaction when the unit of work behavior did not run before them.
	ErrNoUnitOfWork = errors.New("pg: no unit of work in context")
)

// IsAmbiguous reports whether err describes a commit whose outcome is
// unknown (6.1 step 5). It is mediator.IsAmbiguous.
var IsAmbiguous = mediator.IsAmbiguous

// SQLState returns the SQLSTATE of a Postgres error in the chain of err, or "".
func SQLState(err error) string {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

// IsLockTimeout reports whether err is a lock timeout (SQLSTATE 55P03 or
// ErrLockTimeout). The idempotency behavior maps it to CodeIdempotencyBusy.
func IsLockTimeout(err error) bool {
	return errors.Is(err, ErrLockTimeout) || SQLState(err) == "55P03"
}

// IsSerializationFailure reports SQLSTATE 40001.
func IsSerializationFailure(err error) bool { return SQLState(err) == "40001" }

// IsDeadlock reports SQLSTATE 40P01.
func IsDeadlock(err error) bool { return SQLState(err) == "40P01" }

// IsReadOnly reports whether err is a write attempted in a read-only
// transaction (SQLSTATE 25006 or ErrReadOnly).
func IsReadOnly(err error) bool {
	return errors.Is(err, ErrReadOnly) || SQLState(err) == "25006"
}

// IsUniqueViolation reports SQLSTATE 23505.
func IsUniqueViolation(err error) bool { return SQLState(err) == "23505" }

func init() { mediator.RegisterTransient(isTransient) }

// isTransient is the Postgres classifier registered with
// mediator.RegisterTransient: serialization failure, deadlock, admin
// shutdown, crash shutdown, cannot connect now, connection exceptions (class
// 08), and errors pgconn reports as safe to retry.
func isTransient(err error) bool {
	if pgconn.SafeToRetry(err) {
		return true
	}
	switch code := SQLState(err); {
	case code == "40001", code == "40P01", code == "57P01", code == "57P02", code == "57P03":
		return true
	case len(code) == 5 && code[:2] == "08":
		return true
	}
	return false
}
