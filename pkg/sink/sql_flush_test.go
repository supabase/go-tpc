package sink

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// fakeExecer returns errs[i] for the i-th call to ExecContext, clamping to
// the last configured error once calls exceed len(errs) so a single
// persistent error can be configured once and still cover an unbounded
// number of attempts.
type fakeExecer struct {
	errs  []error
	calls int
}

func (f *fakeExecer) ExecContext(_ context.Context, _ string, _ ...interface{}) (sql.Result, error) {
	idx := f.calls
	if idx >= len(f.errs) {
		idx = len(f.errs) - 1
	}
	f.calls++
	return nil, f.errs[idx]
}

func newFlushableSink(exec dbExecer, retryCount int) *SQLSink {
	s := &SQLSink{
		maxBatchRows:  1024,
		insertHint:    "INSERT INTO t VALUES",
		db:            exec,
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

func TestSQLSink_Flush_DuplicateEntryOnFirstAttemptFailsFast(t *testing.T) {
	exec := &fakeExecer{errs: []error{errors.New("Error 1062: Duplicate entry '5' for key 'PRIMARY'")}}
	s := newFlushableSink(exec, 5)
	require.NoError(t, s.WriteRow(context.Background(), 1))
	err := s.Flush(context.Background())
	require.Error(t, err)
	require.Equal(t, 1, exec.calls)
}

// TestSQLSink_Flush_DuplicateEntryAfterRetryIsTreatedAsSuccess preserves
// existing, intentional behavior: a duplicate-key error after at least one
// retry means a prior attempt's INSERT already committed server-side, so
// this isn't a new failure.
func TestSQLSink_Flush_DuplicateEntryAfterRetryIsTreatedAsSuccess(t *testing.T) {
	exec := &fakeExecer{errs: []error{
		errors.New("connection reset"),
		errors.New("Error 1062: Duplicate entry '5' for key 'PRIMARY'"),
	}}
	s := newFlushableSink(exec, 5)
	require.NoError(t, s.WriteRow(context.Background(), 1))
	require.NoError(t, s.Flush(context.Background()))
	require.Equal(t, 2, exec.calls)
}
