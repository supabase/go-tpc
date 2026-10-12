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

// isDuplicateKey reports whether err is a unique-key violation: Postgres
// SQLSTATE 23505 or MySQL error 1062. MySQL is matched by error number because
// its SQLSTATE 23000 also covers other integrity violations.
func isDuplicateKey(err error) bool {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		return myErr.Number == 1062
	}
	return SQLState(err) == "23505"
}

// failureClass says how Flush handles a failed batch INSERT.
type failureClass int

const (
	// failFatal: retrying cannot help, so give up at once.
	failFatal failureClass = iota
	// retryRolledBack: a transient failure where the server did not commit.
	retryRolledBack
	// retryOutcomeUnknown: a transient failure where the server may have
	// committed the batch before the client saw the error.
	retryOutcomeUnknown
)

func (c failureClass) String() string {
	switch c {
	case retryRolledBack:
		return "retry, rolled back"
	case retryOutcomeUnknown:
		return "retry, outcome unknown"
	}
	return "fatal"
}

// classifyFailure decides whether a failed batch INSERT is worth retrying and
// whether it may have committed. Only SQLSTATEs known to be transient are
// retried. Everything else, such as data errors (22), integrity violations
// (23), invalid transaction state (25), syntax errors (42), disk full (53100),
// system errors (58) and internal errors (XX), fails at once.
//
// MySQL SQLSTATEs are too coarse to tell transient from permanent errors
// (HY000 covers both), so MySQL server errors are retried as before, except
// for class 25. They count as outcome unknown, which keeps the old rule that
// a duplicate entry on a retry means an earlier attempt committed.
func classifyFailure(err error) failureClass {
	var myErr *mysql.MySQLError
	if errors.As(err, &myErr) {
		if string(myErr.SQLState[:2]) == "25" {
			return failFatal
		}
		return retryOutcomeUnknown
	}

	switch SQLState(err) {
	case "":
		// No server error: a network, driver or timeout error. The server
		// may have committed before the connection broke.
		return retryOutcomeUnknown
	case "08000", // connection_exception
		"08003", // connection_does_not_exist
		"08006", // connection_failure, also sent by Multigres for an unavailable backend
		"08007", // transaction_resolution_unknown
		"57P01", // admin_shutdown, can end a session after a local commit
		"57P02", // crash_shutdown
		"57014", // query_canceled, sent by proxies such as Multigres for their own timeouts
		"40003": // statement_completion_unknown
		return retryOutcomeUnknown
	case "40001", // serialization_failure
		"40P01", // deadlock_detected
		"55P03", // lock_not_available
		"53200", // out_of_memory
		"53300", // too_many_connections
		"57P03", // cannot_connect_now
		"57P05", // idle_session_timeout
		"08001", // sqlclient_unable_to_establish_sqlconnection
		"08004", // sqlserver_rejected_establishment_of_sqlconnection
		"MTB01", // Multigres: failover buffer full, request evicted before execution
		"MTB02", // Multigres: failover buffer timeout, request evicted before execution
		"MTB03", // Multigres: failover buffer shutting down, request evicted before execution
		"MTF01": // Multigres: planned failover in progress, rejected before execution
		return retryRolledBack
	}
	return failFatal
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
