// Package tpctest holds helpers shared by the tests of several packages.
package tpctest

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// Pool is a connector for sql.OpenDB whose connections go to its servers in
// turn, like a hostname with several addresses behind it. While Down is set
// it opens none.
type Pool struct {
	// Dial opens a connection from a DSN: the Open method of the driver
	// under test.
	Dial func(dsn string) (driver.Conn, error)
	// Servers are the addresses of ServePostgres servers.
	Servers []string
	Down    atomic.Bool

	opened atomic.Int32
}

// Connect implements driver.Connector.
func (p *Pool) Connect(context.Context) (driver.Conn, error) {
	if p.Down.Load() {
		return nil, errors.New("servers are down")
	}
	server := p.Servers[int(p.opened.Add(1)-1)%len(p.Servers)]
	return p.Dial("postgres://u@" + server + "/d?sslmode=disable")
}

// Driver implements driver.Connector.
func (p *Pool) Driver() driver.Driver { return p }

// Open implements driver.Driver. The name is ignored.
func (p *Pool) Open(string) (driver.Conn, error) { return p.Connect(context.Background()) }

// ServePostgres starts a fake PostgreSQL server on a loopback port that lives
// until the test ends, and returns its address.
//
// It answers each startup message with AuthenticationOk and ReadyForQuery,
// which is enough for lib/pq to hand back an open connection. After that it
// answers simple queries: BEGIN and COMMIT the way a server does, which is
// all a transaction with no statements sends, and any other query with one
// row holding a single boolean true, which is what a "SELECT true" returns.
// It hangs up on any other message (lib/pq's Terminate on Close among them).
func ServePostgres(t testing.TB) net.Addr {
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
			go serve(c)
		}
	}()
	return ln.Addr()
}

func serve(c net.Conn) {
	defer c.Close()
	// The startup message has no type byte: a length, then the payload.
	var length [4]byte
	if _, err := io.ReadFull(c, length[:]); err != nil {
		return
	}
	if _, err := io.CopyN(io.Discard, c, int64(binary.BigEndian.Uint32(length[:]))-4); err != nil {
		return
	}
	// AuthenticationOk, then ReadyForQuery (idle).
	c.Write(message('R', []byte{0, 0, 0, 0}))
	c.Write(message('Z', []byte{'I'}))
	for {
		var header [5]byte // message type, then length
		if _, err := io.ReadFull(c, header[:]); err != nil || header[0] != 'Q' {
			return
		}
		query := make([]byte, binary.BigEndian.Uint32(header[1:])-4)
		if _, err := io.ReadFull(c, query); err != nil {
			return
		}
		switch {
		case strings.HasPrefix(string(query), "BEGIN"):
			c.Write(message('C', []byte("BEGIN\x00")))
			c.Write(message('Z', []byte{'T'})) // in a transaction
		case strings.HasPrefix(string(query), "COMMIT"):
			c.Write(message('C', []byte("COMMIT\x00")))
			c.Write(message('Z', []byte{'I'}))
		default:
			// RowDescription: one column named "ok", no table (OID 0,
			// attribute 0), type bool (OID 16, 1 byte, no modifier), in
			// text format.
			c.Write(message('T', []byte{0, 1, 'o', 'k', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 16, 0, 1, 0xff, 0xff, 0xff, 0xff, 0, 0}))
			// DataRow: one column, one byte long, "t".
			c.Write(message('D', []byte{0, 1, 0, 0, 0, 1, 't'}))
			c.Write(message('C', []byte("SELECT 1\x00")))
			c.Write(message('Z', []byte{'I'}))
		}
	}
}

// message frames body as a backend message: the type byte, the length of the
// rest including the length itself, then the body.
func message(typ byte, body []byte) []byte {
	msg := []byte{typ, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(msg[1:], uint32(4+len(body)))
	return append(msg, body...)
}

// CaptureStdout returns what f prints to os.Stdout. os.Stdout is restored
// even when f panics. Not safe for tests that run in parallel.
func CaptureStdout(t testing.TB, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	// Drain while f runs, so f never blocks on a full pipe.
	out := make(chan string, 1)
	go func() {
		defer r.Close()
		b, _ := io.ReadAll(r)
		out <- string(b)
	}()
	func() {
		orig := os.Stdout
		os.Stdout = w
		defer func() {
			os.Stdout = orig
			w.Close()
		}()
		f()
	}()
	return <-out
}

// CaptureStdoutLines is CaptureStdout split into lines, without their
// newlines; nil when f printed nothing.
func CaptureStdoutLines(t testing.TB, f func()) []string {
	t.Helper()
	out := strings.TrimSuffix(CaptureStdout(t, f), "\n")
	if out == "" {
		return nil
	}
	return strings.Split(out, "\n")
}
