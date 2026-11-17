package workload

import (
	"context"
	"database/sql"
	"encoding/binary"
	"io"
	"net"
	"testing"

	"github.com/supabase/go-tpc/pkg/pqdial"
)

// servePostgres accepts connections and answers each startup message with
// AuthenticationOk and ReadyForQuery: enough for lib/pq to hand back an open
// connection. Every subsequent byte is discarded.
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
				io.Copy(io.Discard, c)
			}()
		}
	}()
	return ln.Addr()
}

func TestNewTpcState_RecordsDialedRemote(t *testing.T) {
	addr := servePostgres(t)
	db, err := sql.Open(pqdial.Name, "postgres://u@"+addr.String()+"/d?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	s := NewTpcState(context.Background(), db)
	defer func() { s.Conn.Close() }()
	if s.Remote != addr.String() {
		t.Errorf("Remote = %q, want %q", s.Remote, addr)
	}
	if err := s.RefreshConn(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Remote != addr.String() {
		t.Errorf("Remote after refresh = %q, want %q", s.Remote, addr)
	}
}

func TestNewTpcState_NoDB(t *testing.T) {
	s := NewTpcState(context.Background(), nil)
	if s.Conn != nil || s.Remote != "" {
		t.Errorf("Conn = %v, Remote = %q; want nil and empty", s.Conn, s.Remote)
	}
}
