package pqdial

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
)

// servePostgres accepts connections and answers each startup message with
// AuthenticationOk and ReadyForQuery: enough for lib/pq to hand back an open
// connection. After that it answers simple queries the way a server does for
// BEGIN and COMMIT, which is all a transaction with no statements sends, and
// hangs up on any other message (lib/pq's Terminate on Close among them).
func servePostgres(t *testing.T) net.Addr {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				var length [4]byte
				if _, err := io.ReadFull(c, length[:]); err != nil {
					return
				}
				if _, err := io.CopyN(io.Discard, c, int64(binary.BigEndian.Uint32(length[:]))-4); err != nil {
					return
				}
				// 'R' AuthenticationOk, then 'Z' ReadyForQuery (idle).
				c.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0, 'Z', 0, 0, 0, 5, 'I'})
				for {
					var header [5]byte // message type, then length
					if _, err := io.ReadFull(c, header[:]); err != nil || header[0] != 'Q' {
						return
					}
					query := make([]byte, binary.BigEndian.Uint32(header[1:])-4)
					if _, err := io.ReadFull(c, query); err != nil {
						return
					}
					// 'C' CommandComplete with the command tag, then 'Z'
					// ReadyForQuery: in a transaction after BEGIN, idle after
					// COMMIT.
					tag, status := "COMMIT", byte('I')
					if strings.HasPrefix(string(query), "BEGIN") {
						tag, status = "BEGIN", 'T'
					}
					reply := append([]byte{'C', 0, 0, 0, byte(4 + len(tag) + 1)}, tag...)
					c.Write(append(reply, 0, 'Z', 0, 0, 0, 5, status))
				}
			}()
		}
	}()
	return ln.Addr()
}

// beginCommit runs one empty transaction on conn under a cancelable context,
// as every go-tpc transaction with --txn-timeout set is. lib/pq then leaves
// the transaction's cancel watcher on the conn, a closure that points back at
// the conn: a reference cycle, which Go never collects through a finalizer.
func beginCommit(t *testing.T, conn driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx, err := conn.(driver.ConnBeginTx).BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestOpen_RecordsRemoteAndForgetsCollectedConn(t *testing.T) {
	cases := []struct {
		name string
		use  func(*testing.T, driver.Conn)
	}{
		{"connection never used", func(*testing.T, driver.Conn) {}},
		{"connection that ran a transaction under a cancelable context", beginCommit},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			addr := servePostgres(t)
			before := entries()
			// Open, use and close in a helper so no reference to the
			// connection survives on this frame when the GC runs.
			func() {
				conn, err := Driver{}.Open("postgres://u@" + addr.String() + "/d?sslmode=disable")
				if err != nil {
					t.Fatal(err)
				}
				tc.use(t, conn)
				// A collection while the connection is still in use must
				// leave its entry alone.
				runtime.GC()
				if got := Remote(conn); got != addr.String() {
					t.Errorf("Remote = %q, want %q", got, addr)
				}
				if got := entries(); got != before+1 {
					t.Errorf("remotes has %d entries after Open, want %d", got, before+1)
				}
				conn.Close()
			}()
			for i := 0; i < 20 && entries() != before; i++ {
				runtime.GC()
				time.Sleep(10 * time.Millisecond)
			}
			if got := entries(); got != before {
				t.Errorf("remotes has %d entries after the connection was collected, want %d", got, before)
			}
		})
	}
}

func TestRecorder_KeepsRemoteAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	r := &recorder{}
	conn, err := r.DialContext(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if r.remote != ln.Addr().String() {
		t.Errorf("remote = %q, want %q", r.remote, ln.Addr())
	}
}

func TestRecorder_DialFailureRecordsNothing(t *testing.T) {
	r := &recorder{}
	if _, err := r.Dial("tcp", "127.0.0.1:1"); err == nil {
		t.Fatal("expected dial to a closed port to fail")
	}
	if r.remote != "" {
		t.Errorf("remote = %q, want empty", r.remote)
	}
}

func TestOpen_FailedHandshakeLeavesNoEntry(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	before := entries()
	_, err = Driver{}.Open("postgres://u@" + ln.Addr().String() + "/d?sslmode=disable&connect_timeout=2")
	if err == nil {
		t.Fatal("expected Open against a server that hangs up to fail")
	}
	if got := entries(); got != before {
		t.Errorf("remotes has %d entries after a failed Open, had %d before", got, before)
	}
}

// foreignConn is a connection of some other driver.
type foreignConn struct{ driver.Conn }

var staticConn foreignConn

func TestRemote_UnknownConnection(t *testing.T) {
	cases := []struct {
		name string
		conn driver.Conn
	}{
		{"nil", nil},
		{"pointer Driver did not open", &foreignConn{}},
		{"pointer outside the heap", &staticConn},
		{"not a pointer", foreignConn{}},
	}
	for _, tc := range cases {
		if got := Remote(tc.conn); got != "" {
			t.Errorf("Remote(%s) = %q, want empty", tc.name, got)
		}
	}
}

func TestOpen_IsPqDriver(t *testing.T) {
	// A DSN lib/pq rejects must fail the same way through Driver, before any
	// dial: Driver only changes the dialer.
	_, err := Driver{}.Open("postgres://u@127.0.0.1:1/d?sslmode=nonsense")
	if err == nil || !strings.Contains(err.Error(), "sslmode") {
		t.Fatalf("err = %v, want lib/pq's sslmode error", err)
	}
}

func entries() int {
	n := 0
	remotes.Range(func(any, any) bool { n++; return true })
	return n
}
