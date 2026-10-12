package sink

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// fakeExecer returns errs[i] for the i-th call to exec, clamping to
// the last configured error once calls exceed len(errs) so a single
// persistent error can be configured once and still cover an unbounded
// number of attempts. onCall, if set, runs before each call returns.
type fakeExecer struct {
	errs   []error
	calls  int
	onCall func()
}

func (f *fakeExecer) exec(_ context.Context, _ string) error {
	idx := f.calls
	if idx >= len(f.errs) {
		idx = len(f.errs) - 1
	}
	f.calls++
	if f.onCall != nil {
		f.onCall()
	}
	return f.errs[idx]
}

func newFlushableSink(exec *fakeExecer, retryCount int) *SQLSink {
	s := &SQLSink{
		maxBatchRows:  1024,
		insertHint:    "INSERT INTO t VALUES",
		exec:          exec.exec,
		retryCount:    retryCount,
		retryInterval: time.Millisecond,
	}
	return s
}

func TestSQLSink_Flush_SucceedsOnFirstTry(t *testing.T) {
	exec := &fakeExecer{errs: []error{nil}}
	s := newFlushableSink(exec, 5)
	require.NoError(t, s.WriteRow(context.Background(), 1))
	require.NoError(t, s.Flush(context.Background()))
	require.Equal(t, 1, exec.calls)
}

func TestSQLSink_Flush_RetriesTransientErrorThenSucceeds(t *testing.T) {
	exec := &fakeExecer{errs: []error{
		errors.New("connection reset"),
		errors.New("connection reset"),
		nil,
	}}
	s := newFlushableSink(exec, 5)
	require.NoError(t, s.WriteRow(context.Background(), 1))
	require.NoError(t, s.Flush(context.Background()))
	require.Equal(t, 3, exec.calls)
}

// TestSQLSink_Flush_FailsFastOnPersistentSQLState tests that a read-only transaction
// failure is correctly aborted quickly instead of retrying.
func TestSQLSink_Flush_FailsFastOnPersistentSQLState(t *testing.T) {
	exec := &fakeExecer{errs: []error{
		&pq.Error{Code: "25006", Message: "cannot execute INSERT in a read-only transaction"},
	}}
	s := newFlushableSink(exec, 50)
	require.NoError(t, s.WriteRow(context.Background(), 1))
	err := s.Flush(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "read-only transaction")
	require.Equal(t, 1, exec.calls, "must not retry on an erroneous persistent SQLSTATE")
}

// TestSQLSink_Flush_ReturnsErrorAfterExhaustingRetries is a regression test
// for a prior silent-swallow bug: Flush unconditionally returned nil once the
// retry loop ended, even when every attempt had failed.
func TestSQLSink_Flush_ReturnsErrorAfterExhaustingRetries(t *testing.T) {
	exec := &fakeExecer{errs: []error{errors.New("connection reset")}}
	s := newFlushableSink(exec, 2)
	require.NoError(t, s.WriteRow(context.Background(), 1))
	err := s.Flush(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "connection reset")
	require.Equal(t, 3, exec.calls, "expected 1+retryCount attempts")
}

func mysqlDuplicateEntry() error {
	return &mysql.MySQLError{Number: 1062, SQLState: [5]byte{'2', '3', '0', '0', '0'}, Message: "Duplicate entry '5' for key 'PRIMARY'"}
}

func pqUniqueViolation() error {
	return &pq.Error{Code: "23505", Message: `duplicate key value violates unique constraint "stock_pkey"`}
}

func TestSQLSink_Flush_DuplicateKeyOnFirstAttemptFailsFast(t *testing.T) {
	for name, dup := range map[string]error{"mysql": mysqlDuplicateEntry(), "postgres": pqUniqueViolation()} {
		t.Run(name, func(t *testing.T) {
			exec := &fakeExecer{errs: []error{dup}}
			s := newFlushableSink(exec, 5)
			require.NoError(t, s.WriteRow(context.Background(), 1))
			require.Error(t, s.Flush(context.Background()))
			require.Equal(t, 1, exec.calls)
		})
	}
}

// A duplicate key after an attempt with unknown outcome (e.g. a lost
// connection) means that attempt committed the batch, so it is not a failure.
func TestSQLSink_Flush_DuplicateKeyAfterUnknownOutcomeIsTreatedAsSuccess(t *testing.T) {
	cases := map[string][]error{
		"mysql after network error":     {errors.New("connection reset"), mysqlDuplicateEntry()},
		"postgres after network error":  {errors.New("connection reset"), pqUniqueViolation()},
		"postgres after admin shutdown": {&pq.Error{Code: "57P01"}, pqUniqueViolation()},
		"wrapped postgres duplicate":    {errors.New("connection reset"), fmt.Errorf("exec: %w", pqUniqueViolation())},
		"unknown outcome before a rolled back attempt": {
			errors.New("connection reset"), &pq.Error{Code: "40001"}, pqUniqueViolation(),
		},
		"mysql after mysql server error": {
			&mysql.MySQLError{Number: 1205, SQLState: [5]byte{'H', 'Y', '0', '0', '0'}}, mysqlDuplicateEntry(),
		},
	}
	for name, errs := range cases {
		t.Run(name, func(t *testing.T) {
			exec := &fakeExecer{errs: errs}
			s := newFlushableSink(exec, 5)
			require.NoError(t, s.WriteRow(context.Background(), 1))
			require.NoError(t, s.Flush(context.Background()))
			require.Equal(t, len(errs), exec.calls)
		})
	}
}

// A duplicate key after attempts the server definitely rolled back cannot
// come from those attempts. Other data holds the keys, which is a real error.
func TestSQLSink_Flush_DuplicateKeyAfterDefiniteFailureFails(t *testing.T) {
	exec := &fakeExecer{errs: []error{&pq.Error{Code: "40P01"}, pqUniqueViolation()}}
	s := newFlushableSink(exec, 5)
	require.NoError(t, s.WriteRow(context.Background(), 1))
	err := s.Flush(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate key")
	require.Equal(t, 2, exec.calls)
}

func TestSQLSink_Flush_OtherIntegrityViolationIsNotTreatedAsDuplicate(t *testing.T) {
	exec := &fakeExecer{errs: []error{errors.New("connection reset"), &pq.Error{Code: "23503", Message: "violates foreign key constraint"}}}
	s := newFlushableSink(exec, 2)
	require.NoError(t, s.WriteRow(context.Background(), 1))
	err := s.Flush(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "foreign key")
}

func TestSQLSink_Flush_FailsFastOnFatalSQLState(t *testing.T) {
	for _, code := range []pq.ErrorCode{"XX000", "XX001", "58030", "42601", "22P02", "53100", "08P01"} {
		t.Run(string(code), func(t *testing.T) {
			exec := &fakeExecer{errs: []error{&pq.Error{Code: code, Message: "boom"}}}
			s := newFlushableSink(exec, 50)
			require.NoError(t, s.WriteRow(context.Background(), 1))
			err := s.Flush(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), "non-retryable error")
			require.Equal(t, 1, exec.calls)
		})
	}
}

func TestSQLSink_Flush_StopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exec := &fakeExecer{errs: []error{errors.New("connection reset")}, onCall: cancel}
	s := newFlushableSink(exec, 50)
	require.NoError(t, s.WriteRow(ctx, 1))
	err := s.Flush(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled")
	require.Equal(t, 1, exec.calls)
}

func TestSQLSink_Flush_StopsSleepingWhenContextIsDone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	exec := &fakeExecer{errs: []error{errors.New("connection reset")}}
	s := newFlushableSink(exec, 50)
	s.retryInterval = time.Hour
	require.NoError(t, s.WriteRow(ctx, 1))
	start := time.Now()
	err := s.Flush(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "context deadline exceeded")
	require.Less(t, time.Since(start), time.Second)
	require.Equal(t, 1, exec.calls)
}

func TestSQLSink_Flush_ErrorReportsAttemptHistory(t *testing.T) {
	exec := &fakeExecer{errs: []error{
		errors.New("connection reset"),
		errors.New("connection reset"),
		errors.New("connection reset"),
		&pq.Error{Severity: "ERROR", Code: "XX000", Message: "gateway: stream ended", Detail: "some detail"},
	}}
	s := newFlushableSink(exec, 50)
	s.insertHint = "INSERT INTO stock (s_i_id, s_w_id) VALUES"
	require.NoError(t, s.WriteRow(context.Background(), 1))
	require.NoError(t, s.WriteRow(context.Background(), 2))
	err := s.Flush(context.Background())
	require.Error(t, err)
	msg := err.Error()
	require.Contains(t, msg, `batch: table "stock", 2 rows`)
	require.Contains(t, msg, "attempts: 4 in")
	require.Contains(t, msg, "#1-#3 ")
	require.Contains(t, msg, "retry, outcome unknown: connection reset")
	require.Contains(t, msg, "type: *errors.errorString")
	require.Contains(t, msg, "#4 ")
	require.Contains(t, msg, "sqlstate: XX000 internal_error")
	require.Contains(t, msg, "message: gateway: stream ended")
	require.Contains(t, msg, "detail: some detail")
}
