// Package connadmit gates every new physical database connection behind an
// admission predicate: a query that returns one row whose first column is a
// boolean (true keeps the connection, false closes it and opens another) and
// whose optional second column is a text label recorded for the connection.
package connadmit

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Connection is the record of one admitted physical connection.
type Connection struct {
	Label      string `json:"label"`
	Rejections int    `json:"rejections"`
}

// Stats is the admission record written to the --summary-file document.
type Stats struct {
	SQL      string `json:"sql"`
	Accepted int    `json:"accepted"`
	// Rejections counts every rejected attempt, including those of a
	// Connect that gave up, so it can exceed the per-connection sum.
	Rejections  int          `json:"rejections"`
	Connections []Connection `json:"connections"`
}

// Admitter holds the predicate and records every admission outcome. It is
// safe for concurrent use.
type Admitter struct {
	SQL string
	// MaxRejections is the number of rejected attempts after which a single
	// Connect gives up. Exhausting it is an error, never a fallback to
	// accepting a connection.
	MaxRejections int

	mu    sync.Mutex
	stats Stats
}

// Stats returns a snapshot of the admission record, or nil for a nil
// Admitter so callers can leave an omitempty field unset.
func (a *Admitter) Stats() *Stats {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.stats
	s.SQL = a.SQL
	s.Connections = append([]Connection(nil), a.stats.Connections...)
	return &s
}

// record adds the outcome of one Connect: its rejections, and the admitted
// connection when accepted.
func (a *Admitter) record(rejections int, accepted bool, label string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.stats.Rejections += rejections
	if accepted {
		a.stats.Accepted++
		a.stats.Connections = append(a.stats.Connections, Connection{Label: label, Rejections: rejections})
	}
}

// Connector returns a Connector that opens name with drv and runs the
// predicate on each new connection.
func (a *Admitter) Connector(drv driver.Driver, name string) *Connector {
	return &Connector{admitter: a, drv: drv, name: name}
}

// Connector is a driver.Connector for sql.OpenDB. Admission has to happen
// at the driver level: sql.Conn.Close only returns a connection to the
// database/sql pool, where the next db.Conn would hand back the same rejected
// connection. Here a rejected raw connection is closed before another is
// opened, and only an admitted one ever reaches database/sql.
type Connector struct {
	admitter *Admitter
	drv      driver.Driver
	name     string
}

// Connect implements driver.Connector.
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	a := c.admitter
	var label string
	for rejections := 0; rejections < a.MaxRejections; rejections++ {
		conn, err := c.drv.Open(c.name)
		if err != nil {
			a.record(rejections, false, "")
			if rejections > 0 {
				// The first attempt's error stays unwrapped: openDB matches
				// on it to create a missing database.
				err = fmt.Errorf("--conn-admit-sql %q: attempt %d failed: %w", a.SQL, rejections+1, err)
			}
			return nil, err
		}
		var keep bool
		keep, label, err = admit(ctx, conn, a.SQL)
		if err == nil && keep {
			a.record(rejections, true, label)
			return conn, nil
		}
		conn.Close()
		if err != nil {
			a.record(rejections, false, "")
			return nil, fmt.Errorf("--conn-admit-sql %q: %w", a.SQL, err)
		}
	}
	a.record(a.MaxRejections, false, "")
	return nil, fmt.Errorf("--conn-admit-sql %q rejected %d connection attempts in a row (last label %q), giving up",
		a.SQL, a.MaxRejections, label)
}

// Driver implements driver.Connector. It returns the Connector itself, so
// sql.DB.Driver reports that admission is in use.
func (c *Connector) Driver() driver.Driver { return c }

// Open implements driver.Driver for Driver. The name is ignored: the
// Connector already knows its target.
func (c *Connector) Open(string) (driver.Conn, error) {
	return c.Connect(context.Background())
}

// admit runs query on conn and returns its verdict and label. Any result
// shape other than one row of a boolean and an optional label is an error.
func admit(ctx context.Context, conn driver.Conn, query string) (keep bool, label string, err error) {
	q, ok := conn.(driver.QueryerContext)
	if !ok {
		return false, "", errors.New("driver connection does not implement driver.QueryerContext")
	}
	rows, err := q.QueryContext(ctx, query, nil)
	if err != nil {
		return false, "", err
	}
	defer rows.Close()

	n := len(rows.Columns())
	if n < 1 || n > 2 {
		return false, "", fmt.Errorf("returned %d columns, want a boolean and an optional label", n)
	}
	dest := make([]driver.Value, n)
	if err := rows.Next(dest); err == io.EOF {
		return false, "", errors.New("returned no rows, want exactly one")
	} else if err != nil {
		return false, "", err
	}
	keep, ok = dest[0].(bool)
	if !ok {
		return false, "", fmt.Errorf("first column is %T, want boolean", dest[0])
	}
	if n == 2 {
		switch v := dest[1].(type) {
		case nil:
		case []byte:
			label = string(v)
		default:
			label = fmt.Sprint(v)
		}
	}
	if err := rows.Next(dest); err == nil {
		return false, "", errors.New("returned more than one row, want exactly one")
	} else if err != io.EOF {
		return false, "", err
	}
	// A multi-statement predicate such as "SELECT true; SELECT false" must
	// not be judged by its first result set alone.
	if rs, ok := rows.(driver.RowsNextResultSet); ok && rs.HasNextResultSet() {
		return false, "", errors.New("returned more than one result set, want exactly one")
	}
	return keep, label, nil
}
