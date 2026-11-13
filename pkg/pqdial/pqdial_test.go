package pqdial

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/supabase/go-tpc/internal/tpctest"
)

// hangUp starts a server that accepts connections and closes each at once, so
// lib/pq's startup on it fails after a dial that succeeded.
func hangUp(t *testing.T) net.Addr {
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
			c.Close()
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

// forgotten reports whether the entry under key goes away, giving the garbage
// collector and the cleanup that follows it a few seconds to get there.
func forgotten(key weak.Pointer[byte]) bool {
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		if _, ok := remotes.Load(key); !ok {
			return true
		}
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	return false
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
			addr := tpctest.ServePostgres(t)
			// Open, use and close in a helper so no reference to the
			// connection survives on this frame when the GC runs. The weak
			// key it returns does not keep the connection alive.
			key := func() weak.Pointer[byte] {
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
				conn.Close()
				return weak.Make(object(conn))
			}()
			if !forgotten(key) {
				t.Error("the entry is still there after the connection should have been collected")
			}
		})
	}
}

func TestOpen_RecordsTheHostLibpqEndedUpOn(t *testing.T) {
	cases := []struct {
		name  string
		order func(good, bad net.Addr) []net.Addr
	}{
		// lib/pq dials bad, fails the startup there, then dials good.
		{"after a host whose startup failed", func(good, bad net.Addr) []net.Addr { return []net.Addr{bad, good} }},
		// lib/pq never gets to the second host.
		{"first host", func(good, bad net.Addr) []net.Addr { return []net.Addr{good, bad} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			good, bad := tpctest.ServePostgres(t), hangUp(t)
			var hosts, ports []string
			for _, addr := range tc.order(good, bad) {
				host, port, err := net.SplitHostPort(addr.String())
				if err != nil {
					t.Fatal(err)
				}
				hosts, ports = append(hosts, host), append(ports, port)
			}
			conn, err := Driver{}.Open(fmt.Sprintf("host=%s port=%s user=u dbname=d sslmode=disable connect_timeout=5",
				strings.Join(hosts, ","), strings.Join(ports, ",")))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if got := Remote(conn); got != good.String() {
				t.Errorf("Remote = %q, want %q, the host the connection is to", got, good)
			}
		})
	}
}

func TestRecorder_KeepsRemoteAddress(t *testing.T) {
	addr := hangUp(t)
	r := &recorder{}
	conn, err := r.DialContext(context.Background(), "tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if r.remote != addr.String() {
		t.Errorf("remote = %q, want %q", r.remote, addr)
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

// lib/pq dials its cancel requests through the connection's dialer after Open
// returned, from other goroutines. They must not touch what was recorded.
func TestRecorder_StopsRecordingOnceDone(t *testing.T) {
	first, later := hangUp(t), hangUp(t)
	r := &recorder{}
	conn, err := r.Dial("tcp", first.String())
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if got := r.done(); got != first.String() {
		t.Fatalf("done() = %q, want %q", got, first)
	}

	dials := map[string]func() (net.Conn, error){
		"Dial":        func() (net.Conn, error) { return r.Dial("tcp", later.String()) },
		"DialTimeout": func() (net.Conn, error) { return r.DialTimeout("tcp", later.String(), 5*time.Second) },
		"DialContext": func() (net.Conn, error) { return r.DialContext(context.Background(), "tcp", later.String()) },
	}
	for name, dial := range dials {
		conn, err := dial()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		conn.Close()
		if r.remote != first.String() {
			t.Fatalf("remote = %q after a later %s, want it left at %q", r.remote, name, first)
		}
	}
}

func TestOpen_FailedHandshakeLeavesNoEntry(t *testing.T) {
	addr := hangUp(t)
	before := entries()
	_, err := Driver{}.Open("postgres://u@" + addr.String() + "/d?sslmode=disable&connect_timeout=2")
	if err == nil {
		t.Fatal("expected Open against a server that hangs up to fail")
	}
	// Not "!=": entries of connections earlier tests opened may be collected
	// at any time.
	if got := entries(); got > before {
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

func TestRemember(t *testing.T) {
	// A pointer is recorded, and forgotten once it is collected.
	key := func() weak.Pointer[byte] {
		conn := &foreignConn{}
		remember(conn, "10.0.0.1:5432")
		if got := Remote(conn); got != "10.0.0.1:5432" {
			t.Errorf("Remote = %q, want the remembered address", got)
		}
		return weak.Make(object(conn))
	}()
	if !forgotten(key) {
		t.Error("the entry is still there after the connection should have been collected")
	}

	// A connection that is not a pointer cannot be keyed: nothing is
	// recorded, and nothing panics.
	before := entries()
	remember(foreignConn{}, "10.0.0.1:5432")
	if got := Remote(foreignConn{}); got != "" {
		t.Errorf("Remote of a non-pointer connection = %q, want empty", got)
	}
	if got := entries(); got > before {
		t.Errorf("remotes has %d entries after remembering a non-pointer connection, had %d before", got, before)
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
