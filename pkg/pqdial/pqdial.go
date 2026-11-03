// Package pqdial opens lib/pq connections and remembers the address each one
// was dialed to, so a worker can report which endpoint (for a hostname with
// several addresses, which load-balancer node) its connection landed on.
package pqdial

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"net"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/lib/pq"
)

// Name is the database/sql driver name Driver is registered under.
const Name = "pqdial"

func init() { sql.Register(Name, Driver{}) }

// remotes maps the pointer of every connection Open returned to its dialed
// address. A driver.Conn has no close hook short of wrapping it, and the pool
// hands the same physical connection to successive sql.Conn handles, so a
// lookup must stay answerable for the connection's lifetime. The key is the
// pointer value, not the connection, so the map does not pin lib/pq's conn
// and its read buffer; a finalizer drops the entry once the connection is
// collected. Go frees an object only after its finalizer has run, so a new
// connection cannot reuse the address of an entry that is still present.
var remotes sync.Map

func key(conn driver.Conn) uintptr { return reflect.ValueOf(conn).Pointer() }

// Driver is a driver.Driver that opens lib/pq connections through a dialer
// that records the remote address. It is otherwise pq.Driver.
type Driver struct{}

// Open implements driver.Driver.
func (Driver) Open(name string) (driver.Conn, error) {
	r := &recorder{}
	conn, err := pq.DialOpen(r, name)
	if err != nil {
		return nil, err
	}
	remotes.Store(key(conn), r.remote)
	runtime.SetFinalizer(conn, func(c driver.Conn) { remotes.Delete(key(c)) })
	return conn, nil
}

// Remote returns the address conn was dialed to, or "" when conn did not
// come from Driver.
func Remote(conn driver.Conn) string {
	if conn == nil {
		return ""
	}
	remote, _ := remotes.Load(key(conn))
	s, _ := remote.(string)
	return s
}

// recorder dials with net.Dialer defaults, as lib/pq does, and keeps the
// remote address of the last connection it made. lib/pq dials on the goroutine
// that called Open and may dial more than once for one Open (several hosts,
// target_session_attrs); the connection it keeps is the last one dialed.
type recorder struct {
	d      net.Dialer
	remote string
}

func (r *recorder) Dial(network, address string) (net.Conn, error) {
	return r.DialContext(context.Background(), network, address)
}

func (r *recorder) DialTimeout(network, address string, timeout time.Duration) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return r.DialContext(ctx, network, address)
}

func (r *recorder) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := r.d.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	r.remote = conn.RemoteAddr().String()
	return conn, nil
}
