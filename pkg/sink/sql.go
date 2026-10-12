package sink

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"
)

// execFunc runs one statement. Abstracted so tests can simulate driver errors
// (e.g. a specific SQLSTATE) without a real database connection.
type execFunc func(ctx context.Context, query string) error

// execOnConn runs query on a connection taken from db. Unlike
// (*sql.DB).ExecContext, (*sql.Conn).ExecContext does not re-send the
// statement on driver.ErrBadConn. lib/pq reports ErrBadConn after a FATAL
// error or a connection closed mid-statement, when the statement may already
// have committed. A hidden re-send would then hit a duplicate key on what
// Flush sees as its first attempt. Using a dedicated connection makes every
// execution a visible attempt.
func execOnConn(db *sql.DB) execFunc {
	return func(ctx context.Context, query string) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()
		_, err = conn.ExecContext(ctx, query)
		return err
	}
}

// SQLSink inserts values to a database in batch.
type SQLSink struct {
	maxBatchRows int

	insertHint string
	exec       execFunc

	buf          bytes.Buffer
	bufferedRows int

	retryCount    int
	retryInterval time.Duration
}

var _ Sink = &SQLSink{}

// NewSQLSink creates a sink that inserts values to a database in batch.
func NewSQLSink(db *sql.DB, hint string, retryCount int, retryInterval time.Duration) *SQLSink {
	return &SQLSink{
		maxBatchRows:  1024,
		insertHint:    hint,
		exec:          execOnConn(db),
		retryCount:    retryCount,
		retryInterval: retryInterval,
	}
}

func buildSQLRow(values []interface{}) string {
	var buf bytes.Buffer
	buf.WriteString("(")
	for i, v := range values {
		if i > 0 {
			buf.WriteString(",")
		}
		ty := reflect.TypeOf(v)
		if ty == nil {
			buf.WriteString("NULL")
			continue
		}
		switch ty.Kind() {
		case reflect.String:
			// TODO: Escape string correctly
			_, _ = fmt.Fprintf(&buf, "'%s'", v)
			continue
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			_, _ = fmt.Fprintf(&buf, "%d", v)
			continue
		case reflect.Float32, reflect.Float64:
			_, _ = fmt.Fprintf(&buf, "%f", v)
			continue
		}
		switch v := v.(type) {
		case sql.NullString:
			if v.Valid {
				_, _ = fmt.Fprintf(&buf, "'%s'", v.String)
			} else {
				buf.WriteString("NULL")
			}
		case sql.NullInt64:
			if v.Valid {
				_, _ = fmt.Fprintf(&buf, "%d", v.Int64)
			} else {
				buf.WriteString("NULL")
			}
		case sql.NullFloat64:
			if v.Valid {
				_, _ = fmt.Fprintf(&buf, "%f", v.Float64)
			} else {
				buf.WriteString("NULL")
			}
		default:
			panic(fmt.Sprintf("unsupported type: %T", v))
		}
	}
	buf.WriteString(")")
	return buf.String()
}

// WriteRow writes a row to the database. The writing attempt may be deferred until reaching a batch.
func (s *SQLSink) WriteRow(ctx context.Context, values ...interface{}) error {
	row := buildSQLRow(values)

	if s.bufferedRows == 0 {
		s.buf.WriteString(s.insertHint)
		s.buf.WriteString(" ")
		s.buf.WriteString(row)
	} else {
		s.buf.WriteString(", ")
		s.buf.WriteString(row)
	}

	s.bufferedRows++
	if s.bufferedRows >= s.maxBatchRows {
		return s.Flush(ctx)
	}

	return nil
}

// Flush writes any buffered data to the db. It retries transient errors up to
// retryCount times with a fixed retryInterval sleep between attempts, and
// gives up at once on errors that retrying cannot fix (see classifyFailure).
// If the batch fails, Flush returns an error with the full history of
// attempts instead of silently discarding the batch.
func (s *SQLSink) Flush(ctx context.Context) error {
	if s.buf.Len() == 0 {
		return nil
	}
	err := s.execWithRetry(ctx)
	s.bufferedRows = 0
	s.buf.Reset()
	return err
}

func (s *SQLSink) execWithRetry(ctx context.Context) error {
	query := s.buf.String()
	var attempts []attempt
	// outcomeUnknown is true once an attempt failed in a way that may still
	// have committed the batch on the server.
	outcomeUnknown := false
	for i := 0; i < 1+s.retryCount; i++ {
		start := time.Now()
		err := s.exec(ctx, query)
		if err == nil {
			return nil
		}
		class := classifyFailure(err)
		a := attempt{start: start, duration: time.Since(start), label: class.String(), err: err}

		if isDuplicateKey(err) {
			a.label = "duplicate key"
			attempts = append(attempts, a)
			// The batch is a single INSERT, which commits all rows or none.
			// prepare starts from empty tables and each loader thread writes
			// its own key range. So a duplicate key after an attempt with
			// unknown outcome means that attempt committed the whole batch.
			// Otherwise other data holds the keys, which is a real error.
			if outcomeUnknown {
				fmt.Printf("%s exec statement: duplicate key after an attempt with unknown outcome, assuming it committed\n%s",
					formatTime(time.Now()), s.report(attempts))
				return nil
			}
			return s.failure("duplicate key", attempts)
		}
		attempts = append(attempts, a)
		if ctx.Err() != nil {
			return s.failure(ctx.Err().Error(), attempts)
		}
		if class == failFatal {
			return s.failure("non-retryable error", attempts)
		}
		outcomeUnknown = outcomeUnknown || class == retryOutcomeUnknown
		if i < s.retryCount {
			fmt.Printf("%s exec statement error (attempt %d of %d, %s): %v, try again later...\n",
				formatTime(time.Now()), i+1, 1+s.retryCount, class, err)
			select {
			case <-ctx.Done():
				return s.failure(ctx.Err().Error(), attempts)
			case <-time.After(s.retryInterval):
			}
		}
	}
	return s.failure("retry limit reached", attempts)
}

func (s *SQLSink) failure(reason string, attempts []attempt) error {
	return fmt.Errorf("exec statement error: %s: %v\n%s", reason, attempts[len(attempts)-1].err, s.report(attempts))
}

// report describes the batch and every attempt to execute it.
func (s *SQLSink) report(attempts []attempt) string {
	var b strings.Builder
	fmt.Fprintf(&b, "  batch: table %q, %d rows, %d bytes\n", tableName(s.insertHint), s.bufferedRows, s.buf.Len())
	first, last := attempts[0], attempts[len(attempts)-1]
	fmt.Fprintf(&b, "  attempts: %d in %s\n", len(attempts), last.start.Add(last.duration).Sub(first.start).Round(time.Millisecond))
	formatAttempts(&b, attempts)
	return b.String()
}

func (s *SQLSink) Close(ctx context.Context) error {
	return s.Flush(ctx)
}
