// Package netfault breaks a Postgres connection at a chosen statement.
//
// Dialer is a pgconn DialFunc that wraps every connection it opens. Once
// armed with a fragment of SQL, the next write that carries the fragment
// either fails before anything is sent (FailWrite) or is sent whole and
// then has its reply withheld (DropAfterWrite). The driver reports a
// connection-level error for exactly that statement and the server aborts
// whatever transaction was open, which is what a proxy cutting the link at
// that statement would produce. The fault points of package testkit sit
// before and after each driver call and never fail the call itself, so the
// driver's own error branches (a failed COMMIT or ROLLBACK, an advisory
// lock that cannot be taken or released, a lost acknowledgement) are
// reached only this way, and deterministically.
//
// pgx writes the text of a statement in the Parse message of the extended
// protocol or the Query message of the simple protocol. With the statement
// cache a repeated statement carries only its name, so Config selects
// pgx.QueryExecModeDescribeExec, under which every execution sends the
// text, and a pool of one connection so the statement under test runs on
// the connection the rule watches.
package netfault

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrDropped is the error a broken write or read returns. pgx wraps it in
// its own connection error and closes the connection.
var ErrDropped = errors.New("netfault: connection dropped")

// Dialer opens connections whose writes can be made to fail. The zero
// value is ready to use; one rule is armed at a time and fires once.
type Dialer struct {
	mu     sync.Mutex
	needle []byte
	after  bool
	fired  atomic.Int64
	dialer net.Dialer
}

// FailWrite arms the dialer: the next write that contains sql fails with
// ErrDropped before anything is sent and the connection is closed.
func (d *Dialer) FailWrite(sql string) { d.arm(sql, false) }

// DropAfterWrite arms the dialer: the next write that contains sql is sent
// whole, then every later read and write of that connection fails with
// ErrDropped, so the reply is never seen. A COMMIT dropped this way is the
// lost acknowledgement of spec 6.1 step 5: the server commits and the
// client cannot know.
func (d *Dialer) DropAfterWrite(sql string) { d.arm(sql, true) }

func (d *Dialer) arm(sql string, after bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.needle, d.after = []byte(sql), after
}

// Disarm removes the armed rule.
func (d *Dialer) Disarm() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.needle = nil
}

// Armed reports whether a rule is waiting for its statement.
func (d *Dialer) Armed() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.needle != nil
}

// Fired reports how many rules have fired since the dialer was created.
func (d *Dialer) Fired() int64 { return d.fired.Load() }

// match consumes the rule when b contains the needle and reports whether
// the write goes through before the connection is dropped.
func (d *Dialer) match(b []byte) (hit, after bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.needle == nil || !bytes.Contains(b, d.needle) {
		return false, false
	}
	d.needle = nil
	d.fired.Add(1)
	return true, d.after
}

// Dial is the pgconn.DialFunc.
func (d *Dialer) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	c, err := d.dialer.DialContext(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	return &conn{Conn: c, d: d}, nil
}

// Config parses url into a pool configuration that dials through d: one
// connection and QueryExecModeDescribeExec. ParseConfig always creates the
// RuntimeParams map, so the caller can set search_path on it directly.
func (d *Dialer) Config(url string) (*pgxpool.Config, error) {
	pc, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	pc.MaxConns = 1
	pc.ConnConfig.DialFunc = d.Dial
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeDescribeExec
	return pc, nil
}

// conn is one wrapped connection. Once dropped, every read and write
// fails; a reader already blocked in the real connection is woken by a
// read deadline in the past.
type conn struct {
	net.Conn
	d       *Dialer
	dropped atomic.Bool
}

func (c *conn) Write(b []byte) (int, error) {
	if c.dropped.Load() {
		return 0, ErrDropped
	}
	hit, after := c.d.match(b)
	if !hit {
		return c.Conn.Write(b)
	}
	if !after {
		c.dropped.Store(true)
		_ = c.Conn.Close()
		return 0, ErrDropped
	}
	// Mark the drop and wake any blocked reader before the bytes go out, so
	// no reply can be observed: it does not exist yet, and every read from
	// now on fails without touching the connection.
	c.dropped.Store(true)
	_ = c.Conn.SetReadDeadline(time.Now())
	return c.Conn.Write(b)
}

func (c *conn) Read(b []byte) (int, error) {
	if c.dropped.Load() {
		return 0, ErrDropped
	}
	return c.Conn.Read(b)
}
