package netfault_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit/netfault"
)

// echo serves connections that write back what they read.
func echo(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}()
		}
	}()
	return ln
}

func roundTrip(t *testing.T, c net.Conn, msg string) {
	t.Helper()
	if _, err := c.Write([]byte(msg)); err != nil {
		t.Fatalf("write %q: %v", msg, err)
	}
	buf := make([]byte, len(msg))
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != msg {
		t.Fatalf("read back %q: %q %v", msg, buf, err)
	}
}

func TestDialer_RulesFireOnceAtTheMatchingWrite(t *testing.T) {
	ln := echo(t)
	ctx := context.Background()
	var d netfault.Dialer
	c, err := d.Dial(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	roundTrip(t, c, "hello")

	d.FailWrite("boom")
	if !d.Armed() {
		t.Fatal("armed")
	}
	roundTrip(t, c, "no match") // a write without the fragment passes
	if n, err := c.Write([]byte("xx boom xx")); n != 0 || !errors.Is(err, netfault.ErrDropped) {
		t.Fatalf("matching write: %d %v", n, err)
	}
	if d.Armed() || d.Fired() != 1 {
		t.Fatalf("the rule fires once: armed=%v fired=%d", d.Armed(), d.Fired())
	}
	if _, err := c.Write([]byte("after")); !errors.Is(err, netfault.ErrDropped) {
		t.Fatalf("write after the drop: %v", err)
	}
	if _, err := c.Read(make([]byte, 1)); !errors.Is(err, netfault.ErrDropped) {
		t.Fatalf("read after the drop: %v", err)
	}

	// After a write: the bytes reach the server, the reply never returns.
	c2, err := d.Dial(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	d.DropAfterWrite("ack")
	if n, err := c2.Write([]byte("ack")); n != 3 || err != nil {
		t.Fatalf("write goes through: %d %v", n, err)
	}
	if _, err := c2.Read(make([]byte, 3)); !errors.Is(err, netfault.ErrDropped) {
		t.Fatalf("the reply is withheld: %v", err)
	}
	if d.Fired() != 2 {
		t.Fatalf("fired %d", d.Fired())
	}

	// A reader already blocked in the connection is woken when the drop lands.
	c3, err := d.Dial(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c3.Close()
	readErr := make(chan error, 1)
	go func() {
		_, err := c3.Read(make([]byte, 1))
		readErr <- err
	}()
	time.Sleep(20 * time.Millisecond)
	d.DropAfterWrite("late")
	if _, err := c3.Write([]byte("late")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-readErr:
		if err == nil {
			t.Fatal("the blocked read must fail")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the blocked read was not woken")
	}

	d.FailWrite("never")
	d.Disarm()
	if d.Armed() {
		t.Fatal("disarmed")
	}
	c4, err := d.Dial(ctx, "tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c4.Close()
	roundTrip(t, c4, "never")
	if d.Fired() != 3 {
		t.Fatalf("fired %d", d.Fired())
	}
}

func TestDialer_ConfigAndDialErrors(t *testing.T) {
	var d netfault.Dialer
	pc, err := d.Config("postgres://u:p@localhost:5432/db?search_path=s")
	if err != nil {
		t.Fatal(err)
	}
	if pc.MaxConns != 1 || pc.ConnConfig.DialFunc == nil || pc.ConnConfig.DefaultQueryExecMode != pgx.QueryExecModeDescribeExec || pc.ConnConfig.RuntimeParams["search_path"] != "s" {
		t.Fatalf("config: %+v", pc)
	}
	if _, err := d.Config("://bad"); err == nil {
		t.Fatal("bad url must fail")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := d.Dial(ctx, "tcp", "127.0.0.1:1"); err == nil {
		t.Fatal("a refused connection must fail")
	}
}
