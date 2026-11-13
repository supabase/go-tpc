// Package pqdial opens lib/pq connections and remembers the address each one
// was dialed to, so a worker can report which endpoint (for a hostname with
// several addresses, which load-balancer node) its connection landed on.
//
// The address is the peer of the TCP connection, which is as far as a client
// can see. Behind a proxying load balancer it names the proxy and not the
// server behind it, so workers routed to different backends can report the
// same address. Telling those apart takes the server's own word, for example
// the label a --conn-admit-sql query returns (see package connadmit).
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
	remember(conn, r.done())
	return conn, nil
}

// remember records remote as the address conn was dialed to, for as long as
// conn is alive. A conn that is not a pointer has no identity to key on and
// is not recorded; lib/pq's always is one.
func remember(conn driver.Conn, remote string) {
	obj := object(conn)
	if obj == nil {
		return
	}
	key := weak.Make(obj)
	remotes.Store(key, remote)
	runtime.AddCleanup(obj, func(key weak.Pointer[byte]) { remotes.Delete(key) }, key)
}

// Remote returns the address conn was dialed to, or "" when conn did not
// come from Driver.
func Remote(conn driver.Conn) string {
	remote, _ := remotes.Load(weak.Make(object(conn)))
	s, _ := remote.(string)
	return s
}

// recorder dials with net.Dialer defaults, as lib/pq does, and keeps the
// remote address of the last connection it made while Open was running.
// lib/pq may dial more than once for one Open (several hosts, an sslmode
// fallback, target_session_attrs), all on the goroutine that called Open; the
// connection it keeps is the last one dialed.
//
// lib/pq goes on dialing through the recorder after Open has returned: every
// cancel request it sends for the connection is a new dial, made from a
// goroutine watching the query's context, and a transaction and a statement
// in it that share an expired context each send one at the same time. Those
// dials are not the connection, and recording them would be a data race, so
// done turns recording off. Open calls it before it hands the connection out,
// which is before any of those goroutines can exist.
type recorder struct {
	d       net.Dialer
	remote  string
	stopped bool
}

// done stops the recording and returns the address of the last connection
// dialed. It is the only way the address is read, so it cannot be read
// without also being frozen.
func (r *recorder) done() string {
	r.stopped = true
	return r.remote
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
	if !r.stopped {
		r.remote = conn.RemoteAddr().String()
	}
	return conn, nil
}
