package sink

import (
	"errors"

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
