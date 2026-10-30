package connadmit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

// result is what one fake connection's predicate query returns.
type result struct {
	cols []string
	rows [][]driver.Value
	err  error
	// moreResultSets makes the rows report a further result set, as lib/pq
	// does for a multi-statement query.
	moreResultSets bool
}

func verdict(keep bool, label string) result {
	return result{cols: []string{"keep", "label"}, rows: [][]driver.Value{{keep, label}}}
}

// fakeDriver hands out one connection per scripted result, in order.
type fakeDriver struct {
	script []result
	opened []*fakeConn
}

func (d *fakeDriver) Open(string) (driver.Conn, error) {
	if len(d.opened) == len(d.script) {
		return nil, errors.New("fake driver: script exhausted")
	}
	c := &fakeConn{res: d.script[len(d.opened)]}
	d.opened = append(d.opened, c)
	return c, nil
}

type fakeConn struct {
	res    result
	closed bool
}

func (c *fakeConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("not supported") }
func (c *fakeConn) Begin() (driver.Tx, error)           { return nil, errors.New("not supported") }
func (c *fakeConn) Close() error                        { c.closed = true; return nil }

func (c *fakeConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	if c.res.err != nil {
		return nil, c.res.err
	}
	return &fakeRows{cols: c.res.cols, rows: c.res.rows, more: c.res.moreResultSets}, nil
}

type fakeRows struct {
	cols []string
	rows [][]driver.Value
	more bool
}

func (r *fakeRows) HasNextResultSet() bool { return r.more }
func (r *fakeRows) NextResultSet() error   { return io.EOF }

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

func TestConnect(t *testing.T) {
	const pred = "SELECT pg_backend_pid() % 2 = 0"
	cases := []struct {
		name    string
		script  []result
		max     int
		wantErr []string // substrings; nil means admission succeeds
		want    Connection
		// rejected is the total rejection count expected when admission fails.
		rejected int
	}{
		{
			name:   "accept first try",
			script: []result{verdict(true, "a")},
			max:    50,
			want:   Connection{Label: "a"},
		},
		{
			name:   "accept after rejections",
			script: []result{verdict(false, "x"), verdict(false, "y"), verdict(false, "z"), verdict(true, "a")},
			max:    50,
			want:   Connection{Label: "a", Rejections: 3},
		},
		{
			name:   "accept on the last allowed attempt",
			script: []result{verdict(false, "x"), verdict(false, "y"), verdict(true, "a")},
			max:    3,
			want:   Connection{Label: "a", Rejections: 2},
		},
		{
			name:   "boolean only, no label",
			script: []result{{cols: []string{"keep"}, rows: [][]driver.Value{{true}}}},
			max:    50,
			want:   Connection{},
		},
		{
			name:   "label as bytes",
			script: []result{{cols: []string{"keep", "label"}, rows: [][]driver.Value{{true, []byte("zone-a")}}}},
			max:    50,
			want:   Connection{Label: "zone-a"},
		},
		{
			name:     "retry cap exhausted",
			script:   []result{verdict(false, "x"), verdict(false, "y"), verdict(false, "z"), verdict(true, "a")},
			max:      3,
			wantErr:  []string{pred, "rejected 3 connection attempts", `last label "z"`},
			rejected: 3,
		},
		{
			name:     "malformed after a rejection",
			script:   []result{verdict(false, "x"), {err: errors.New("boom")}},
			max:      50,
			wantErr:  []string{pred, "boom"},
			rejected: 1,
		},
		{
			name:     "reconnect fails after a rejection",
			script:   []result{verdict(false, "x")}, // the second Open finds the script exhausted
			max:      50,
			wantErr:  []string{pred, "attempt 2 failed", "script exhausted"},
			rejected: 1,
		},
		{
			name:    "second result set",
			script:  []result{{cols: []string{"keep"}, rows: [][]driver.Value{{true}}, moreResultSets: true}},
			max:     50,
			wantErr: []string{pred, "more than one result set"},
		},
		{
			name:    "zero rows",
			script:  []result{{cols: []string{"keep"}}},
			max:     50,
			wantErr: []string{pred, "no rows"},
		},
		{
			name:    "two rows",
			script:  []result{{cols: []string{"keep"}, rows: [][]driver.Value{{true}, {true}}}},
			max:     50,
			wantErr: []string{pred, "more than one row"},
		},
		{
			name:    "integer first column",
			script:  []result{{cols: []string{"keep"}, rows: [][]driver.Value{{int64(1)}}}},
			max:     50,
			wantErr: []string{pred, "int64, want boolean"},
		},
		{
			name:    "text first column",
			script:  []result{{cols: []string{"keep"}, rows: [][]driver.Value{{"true"}}}},
			max:     50,
			wantErr: []string{pred, "string, want boolean"},
		},
		{
			name:    "null first column",
			script:  []result{{cols: []string{"keep"}, rows: [][]driver.Value{{nil}}}},
			max:     50,
			wantErr: []string{pred, "want boolean"},
		},
		{
			name:    "three columns",
			script:  []result{{cols: []string{"a", "b", "c"}, rows: [][]driver.Value{{true, "x", "y"}}}},
			max:     50,
			wantErr: []string{pred, "3 columns"},
		},
		{
			name:    "query error",
			script:  []result{{err: errors.New(`relation "nope" does not exist`)}},
			max:     50,
			wantErr: []string{pred, `relation "nope" does not exist`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			drv := &fakeDriver{script: tc.script}
			a := &Admitter{SQL: pred, MaxRejections: tc.max}
			db := sql.OpenDB(a.Connector(drv, "dsn"))
			defer db.Close()

			conn, err := db.Conn(context.Background())
			stats := a.Stats()

			if tc.wantErr != nil {
				if err == nil {
					conn.Close()
					t.Fatal("expected an error, got an admitted connection")
				}
				for _, s := range tc.wantErr {
					if !strings.Contains(err.Error(), s) {
						t.Errorf("error %q does not contain %q", err, s)
					}
				}
				for i, c := range drv.opened {
					if !c.closed {
						t.Errorf("connection %d was not closed", i)
					}
				}
				want := &Stats{SQL: pred, Rejections: tc.rejected}
				if !reflect.DeepEqual(stats, want) {
					t.Errorf("stats = %+v, want %+v", stats, want)
				}
				return
			}

			if err != nil {
				t.Fatalf("db.Conn: %v", err)
			}
			defer conn.Close()
			if got := len(drv.opened); got != tc.want.Rejections+1 {
				t.Fatalf("opened %d raw connections, want %d", got, tc.want.Rejections+1)
			}
			for i, c := range drv.opened {
				if wantClosed := i < len(drv.opened)-1; c.closed != wantClosed {
					t.Errorf("connection %d closed = %v, want %v", i, c.closed, wantClosed)
				}
			}
			want := &Stats{SQL: pred, Accepted: 1, Rejections: tc.want.Rejections, Connections: []Connection{tc.want}}
			if !reflect.DeepEqual(stats, want) {
				t.Errorf("stats = %+v, want %+v", stats, want)
			}
		})
	}
}

// An admitted connection returned to the pool is reused without opening,
// or re-checking, another physical connection.
func TestConnect_PoolReuseSkipsPredicate(t *testing.T) {
	drv := &fakeDriver{script: []result{verdict(false, "x"), verdict(true, "a")}}
	a := &Admitter{SQL: "SELECT true", MaxRejections: 50}
	db := sql.OpenDB(a.Connector(drv, "dsn"))
	defer db.Close()

	for i := 0; i < 3; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("db.Conn #%d: %v", i, err)
		}
		conn.Close()
	}
	if got := len(drv.opened); got != 2 {
		t.Errorf("opened %d raw connections, want 2", got)
	}
	if got := a.Stats().Accepted; got != 1 {
		t.Errorf("accepted = %d, want 1", got)
	}
}

func TestStats_NilAdmitter(t *testing.T) {
	var a *Admitter
	if s := a.Stats(); s != nil {
		t.Errorf("nil Admitter Stats() = %+v, want nil", s)
	}
}

// The first attempt's dial error is returned as is, so openDB can still
// recognise a missing database by its message.
func TestConnect_FirstOpenErrorUnwrapped(t *testing.T) {
	a := &Admitter{SQL: "SELECT true", MaxRejections: 50}
	_, err := a.Connector(&fakeDriver{}, "dsn").Connect(context.Background())
	if err == nil || err.Error() != "fake driver: script exhausted" {
		t.Fatalf("err = %v, want the driver's error unwrapped", err)
	}
}
