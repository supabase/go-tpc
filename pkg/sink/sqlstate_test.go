package sink

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestSQLState(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "postgres error",
			err:  fmt.Errorf("exec failed: %w", &pq.Error{Code: "25006", Message: "read-only"}),
			want: "25006",
		},
		{
			name: "mysql error",
			err:  fmt.Errorf("exec failed: %w", &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "deadlock"}),
			want: "40001",
		},
		{
			name: "plain error",
			err:  errors.New("connection reset"),
			want: "",
		},
		{
			name: "nil error",
			err:  nil,
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, SQLState(tc.err))
		})
	}
}

func TestIsTransactionConflict(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "postgres deadlock",
			err:  fmt.Errorf("exec %s failed %w", "SELECT ...", &pq.Error{Code: "40P01", Message: "deadlock detected"}),
			want: true,
		},
		{
			name: "postgres serialization failure",
			err:  fmt.Errorf("exec %s failed %w", "SELECT ...", &pq.Error{Code: "40001", Message: "could not serialize access"}),
			want: true,
		},
		{
			name: "mysql deadlock",
			err:  fmt.Errorf("exec %s failed %w", "SELECT ...", &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "Deadlock found"}),
			want: true,
		},
		{
			name: "postgres syntax error is not a conflict",
			err:  fmt.Errorf("exec %s failed %w", "SELECT ...", &pq.Error{Code: "42601", Message: "syntax error"}),
			want: false,
		},
		{
			name: "plain error is not a conflict",
			err:  errors.New("connection refused"),
			want: false,
		},
		{
			name: "nil error is not a conflict",
			err:  nil,
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsTransactionConflict(tc.err))
		})
	}
}

func TestIsRetryableSQLState(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "read-only transaction (25006) is not retryable",
			err:  &pq.Error{Code: "25006", Message: "cannot execute INSERT in a read-only transaction"},
			want: false,
		},
		{
			name: "other class-25 invalid transaction state is not retryable",
			err:  &pq.Error{Code: "25P02", Message: "current transaction is aborted"},
			want: false,
		},
		{
			name: "serialization failure (class 40) is retryable",
			err:  &pq.Error{Code: "40001", Message: "could not serialize access"},
			want: true,
		},
		{
			name: "plain non-driver error keeps prior best-effort retry behavior",
			err:  errors.New("connection reset"),
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isRetryableSQLState(tc.err))
		})
	}
}

func TestIsConnectionError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
			err:  nil,
			want: false,
		},
		{
			name: "bad connection",
			err:  driver.ErrBadConn,
			want: true,
		},
		{
			name: "wrapped bad connection",
			err:  fmt.Errorf("exec %s failed %w", "SELECT ...", driver.ErrBadConn),
			want: true,
		},
		{
			name: "connection already done",
			err:  sql.ErrConnDone,
			want: true,
		},
		{
			name: "mysql invalid connection",
			err:  fmt.Errorf("exec %s failed %w", "SELECT ...", mysql.ErrInvalidConn),
			want: true,
		},
		{
			name: "raw network error",
			err:  &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")},
			want: true,
		},
		{
			name: "postgres syntax error is not a connection error",
			err:  fmt.Errorf("exec %s failed %w", "SELECT ...", &pq.Error{Code: "42601", Message: "syntax error"}),
			want: false,
		},
		{
			name: "mysql deadlock is not a connection error",
			err:  fmt.Errorf("exec %s failed %w", "SELECT ...", &mysql.MySQLError{Number: 1213, SQLState: [5]byte{'4', '0', '0', '0', '1'}, Message: "Deadlock found"}),
			want: false,
		},
		{
			name: "plain error is not a connection error",
			err:  errors.New("connection reset"),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsConnectionError(tc.err))
		})
	}
}
