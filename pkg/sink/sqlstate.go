package sink

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"

	"github.com/go-sql-driver/mysql"
)

// sqlStater is implemented by driver errors that expose a SQL standard
// SQLSTATE code. *pq.Error implements this natively.
type sqlStater interface {
	SQLState() string
}

// SQLState extracts the SQLSTATE code from anywhere in err's chain. Returns
// "" if no driver error with a SQLSTATE code is found.
func SQLState(err error) string {
	var s sqlStater
	if errors.As(err, &s) {
		return s.SQLState()
	}
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) && myErr.SQLState != [5]byte{} {
		return string(myErr.SQLState[:])
	}
	return ""
}

// IsTransactionConflict reports whether err represents a transient
// transaction-rollback condition (deadlock, serialization failure) per
// SQLSTATE class "40", as opposed to a genuine, non-retryable failure.
func IsTransactionConflict(err error) bool {
	state := SQLState(err)
	return len(state) == 5 && state[:2] == "40"
}

// isRetryableSQLState reports whether err's SQLSTATE class indicates a
// transient condition worth retrying.
//
// At the moment only SQLSTATE class 25, "Invalid Transaction State"
// is considered non-retryable but the list may expand over time.
//
// An unknown/non-driver error (empty state) is treated as retryable, keeping
// the prior best-effort retry behavior.
func isRetryableSQLState(err error) bool {
	state := SQLState(err)
	return !(len(state) == 5 && state[:2] == "25")
}

// IsConnectionError reports whether err indicates the underlying database
// connection is unusable and must be replaced, as opposed to an ordinary
// SQL-level failure (constraint violation, serialization conflict, etc.).
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	// Both lib/pq (error.go handleError) and go-sql-driver/mysql
	// (connection.go markBadConn) normalize network/protocol failures into
	// driver.ErrBadConn. This already includes Postgres FATAL-severity server
	// disconnects such as admin_shutdown, idle_session_timeout, and
	// database_dropped (e.g. a replica failover killing the session). lib/pq
	// promotes any FATAL-severity *pq.Error to ErrBadConn itself
	// (error.go:306-308), so no separate SQLSTATE-class check is needed here,
	// and no Postgres-specific check exists in this function.
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	// database/sql's own retry-on-ErrBadConn logic (sql.go (*DB).retry) always
	// re-grabs the SAME *sql.Conn on retry, since our prepared statements are
	// pinned to one dedicated connection per worker (conn.PrepareContext). The
	// retry then observes sql.ErrConnDone instead of the original ErrBadConn
	// (sql.go grabConn / closemuRUnlockCondReleaseConn) which our
	// connection-pinned statement calls actually see, not ErrBadConn. Given
	// go-tpc's one connection per worker usage (i.e. no concurrent access, no
	// unrelated code closes a connection), ErrConnDone alwazys indicates
	// a prior connection failure.
	if errors.Is(err, sql.ErrConnDone) {
		return true
	}
	// go-sql-driver/mysql has no single normalization point like lib/pq. Its
	// read path (packets.go readPacket/readNext) converts read-side network
	// failures, including while waiting on a COMMIT response
	// (transaction.go -> connection.go exec -> readResultSetHeaderPacket), to
	// this sentinel instead of driver.ErrBadConn.
	if errors.Is(err, mysql.ErrInvalidConn) {
		return true
	}
	// Fallback for a case neither driver normalizes: go-sql-driver/mysql's
	// write path (packets.go writePacket) only converts a failure to a bad-conn
	// sentinel when no bytes were written; a failure after a partial write
	// returns the raw net error (typically *net.OpError). Harmless to check
	// even for postgres/no-op cases; at worst causes one unnecessary
	// refresh (TPC-C never retries on this, so no risk of a duplicate write).
	var netErr net.Error
	return errors.As(err, &netErr)
}
