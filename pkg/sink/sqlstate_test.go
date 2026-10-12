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

func TestIsDuplicateKey(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "postgres unique violation (23505)",
			err:  &pq.Error{Code: "23505", Message: "duplicate key value violates unique constraint"},
			want: true,
		},
		{
			name: "wrapped postgres unique violation",
			err:  fmt.Errorf("exec: %w", &pq.Error{Code: "23505"}),
			want: true,
		},
		{
			name: "postgres foreign key violation (23503)",
			err:  &pq.Error{Code: "23503", Message: "violates foreign key constraint"},
			want: false,
		},
		{
			name: "mysql duplicate entry (1062)",
			err:  &mysql.MySQLError{Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "Duplicate entry '5' for key 'PRIMARY'"},
			want: true,
		},
		{
			name: "other mysql integrity violation with SQLSTATE 23000",
			err:  &mysql.MySQLError{Number: 1452, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "Cannot add or update a child row"},
			want: false,
		},
		{
			name: "plain error",
			err:  errors.New("connection reset"),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isDuplicateKey(tc.err))
		})
	}
}

func TestClassifyFailure(t *testing.T) {
	mysqlErr := func(number uint16, state string) error {
		e := &mysql.MySQLError{Number: number}
		copy(e.SQLState[:], state)
		return e
	}
	cases := []struct {
		err  error
		want failureClass
	}{
		{errors.New("connection reset"), retryOutcomeUnknown},
		{driver.ErrBadConn, retryOutcomeUnknown},
		{&net.OpError{Op: "read", Err: errors.New("connection reset by peer")}, retryOutcomeUnknown},
		{mysql.ErrInvalidConn, retryOutcomeUnknown},
		{&pq.Error{Code: "08006"}, retryOutcomeUnknown},
		{&pq.Error{Code: "08007"}, retryOutcomeUnknown},
		{&pq.Error{Code: "57P01"}, retryOutcomeUnknown},
		{&pq.Error{Code: "57014"}, retryOutcomeUnknown},
		{&pq.Error{Code: "40003"}, retryOutcomeUnknown},
		{fmt.Errorf("wrapped: %w", &pq.Error{Code: "08003"}), retryOutcomeUnknown},
		{&pq.Error{Code: "40001"}, retryRolledBack},
		{&pq.Error{Code: "40P01"}, retryRolledBack},
		{&pq.Error{Code: "55P03"}, retryRolledBack},
		{&pq.Error{Code: "53300"}, retryRolledBack},
		{&pq.Error{Code: "57P03"}, retryRolledBack},
		{&pq.Error{Code: "08001"}, retryRolledBack},
		{&pq.Error{Code: "25006"}, failFatal},
		{&pq.Error{Code: "23505"}, failFatal},
		{&pq.Error{Code: "23503"}, failFatal},
		{&pq.Error{Code: "42601"}, failFatal},
		{&pq.Error{Code: "53100"}, failFatal},
		{&pq.Error{Code: "58030"}, failFatal},
		{&pq.Error{Code: "XX000"}, failFatal},
		{&pq.Error{Code: "08P01"}, failFatal},
		{&pq.Error{Code: "57P04"}, failFatal},
		{mysqlErr(1205, "HY000"), retryOutcomeUnknown},
		{mysqlErr(1213, "40001"), retryOutcomeUnknown},
		{mysqlErr(1792, "25006"), failFatal},
	}
	for _, tc := range cases {
		t.Run(tc.err.Error(), func(t *testing.T) {
			require.Equal(t, tc.want, classifyFailure(tc.err))
		})
	}
}

func TestTableName(t *testing.T) {
	require.Equal(t, "stock", tableName("INSERT INTO stock (s_i_id) VALUES"))
	require.Equal(t, "t", tableName("insert into t(a) values"))
	require.Equal(t, "", tableName("VALUES"))
}
