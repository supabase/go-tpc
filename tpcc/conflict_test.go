package tpcc

import (
	"errors"
	"fmt"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
)

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
			if got := isTransactionConflict(tc.err); got != tc.want {
				t.Fatalf("isTransactionConflict(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
