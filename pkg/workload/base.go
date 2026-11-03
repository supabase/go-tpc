package workload

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"math/rand"
	"time"

	"github.com/supabase/go-tpc/pkg/pqdial"
	"github.com/supabase/go-tpc/pkg/util"
)

// TpcState saves state for each thread
type TpcState struct {
	DB   *sql.DB
	Conn *sql.Conn
	// Remote is the address Conn was dialed to, or "" when the driver does not
	// record it (only pqdial does).
	Remote string

	R *rand.Rand

	Buf *util.BufAllocator
}

func (t *TpcState) RefreshConn(ctx context.Context) error {
	if t.Conn != nil {
		t.Conn.Close()
	}
	conn, err := t.DB.Conn(ctx)
	if err != nil {
		return err
	}
	t.Conn = conn
	t.Remote = remoteOf(conn)
	return nil
}

// remoteOf claims the dialed address of a connection opened through pqdial.
func remoteOf(conn *sql.Conn) string {
	var remote string
	_ = conn.Raw(func(driverConn any) error {
		if dc, ok := driverConn.(driver.Conn); ok {
			remote = pqdial.Remote(dc)
		}
		return nil
	})
	return remote
}

// NewTpcState creates a base TpcState
func NewTpcState(ctx context.Context, db *sql.DB) *TpcState {
	var conn *sql.Conn
	var remote string
	if db != nil {
		var err error
		conn, err = db.Conn(ctx)
		if err != nil {
			panic(err.Error())
		}
		remote = remoteOf(conn)
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	s := &TpcState{
		DB:     db,
		Conn:   conn,
		Remote: remote,
		R:      r,
		Buf:    util.NewBufAllocator(),
	}
	return s
}
