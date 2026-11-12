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
	"weak"

	"github.com/lib/pq"
)

// Name is the database/sql driver name Driver is registered under.
const Name = "pqdial"

func init() { sql.Register(Name, Driver{}) }

// remotes maps every connection Open returned to its dialed address. A
// driver.Conn has no close hook short of wrapping it, and the pool hands the
// same physical connection to successive sql.Conn handles, so a lookup must
// stay answerable for the connection's lifetime.
//
// The keys are weak pointers, so the map does not pin lib/pq's conn and its
// read buffer, and a cleanup drops the entry once the connection is
// collected. It has to be a cleanup and not a finalizer: lib/pq stores the
// cancel watcher of a transaction begun under a cancelable context on the
// conn, and that closure points back at the conn. An object in a reference
// cycle is never finalized, and so never freed, while a cleanup runs for it
// like for any other. A weak pointer names the object and not its address,
// so the cleanup of a collected connection cannot drop the entry of a newer
// one allocated in the same place.
var remotes sync.Map // weak.Pointer[byte] -> string

// object is the address of the object conn points to, or nil when conn is not
// a pointer. lib/pq's conn type is unexported and cannot be named here, so
// the pointer is typed as the object's first byte: weak pointers and cleanups
// only go by the address.
func object(conn driver.Conn) *byte {
	v := reflect.ValueOf(conn)
	if v.Kind() != reflect.Pointer {
		return nil
	}
	return (*byte)(v.UnsafePointer())
}

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
	obj := object(conn)
	key := weak.Make(obj)
	remotes.Store(key, r.remote)
	runtime.AddCleanup(obj, func(key weak.Pointer[byte]) { remotes.Delete(key) }, key)
	return conn, nil
}

// Remote returns the address conn was dialed to, or "" when conn did not
// come from Driver.
func Remote(conn driver.Conn) string {
	remote, _ := remotes.Load(weak.Make(object(conn)))
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
