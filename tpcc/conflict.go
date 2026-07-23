package tpcc

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// sqlStater is implemented by driver errors that expose a SQL standard
// SQLSTATE code. *pq.Error implements this natively.
type sqlStater interface {
	SQLState() string
}

// sqlState extracts the SQLSTATE code from anywhere in err's chain.
// Returns "" if no driver error with a SQLSTATE is found.
func sqlState(err error) string {
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

// isTransactionConflict reports whether err represents a transient
// transaction-rollback condition (deadlock, serialization failure) per
// SQLSTATE class "40", as opposed to a genuine, non-retryable failure.
func isTransactionConflict(err error) bool {
	state := sqlState(err)
	return len(state) == 5 && state[:2] == "40"
}
