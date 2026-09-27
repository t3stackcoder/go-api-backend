package redisx

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

type remoteResult struct {
	OrderID string `json:"orderId"`
	N       int64  `json:"n"`
}

func TestDecodeReply(t *testing.T) {
	rt := reflect.TypeFor[remoteResult]()
	res, err := decodeReply(reply{status: ReplyStatusOK, body: []byte(`{"orderId":"o1","n":9007199254740993}`)}, rt)
	if err != nil {
		t.Fatal(err)
	}
	if got := res.(remoteResult); got.OrderID != "o1" || got.N != 9007199254740993 {
		t.Fatalf("got %+v", got)
	}
	// Void responses and empty bodies.
	res, err = decodeReply(reply{status: ReplyStatusOK, body: []byte(`{}`)}, reflect.TypeFor[mediator.Void]())
	if err != nil || res != (mediator.Void{}) {
		t.Fatalf("void: %v %v", res, err)
	}
	if res, err = decodeReply(reply{status: ReplyStatusOK}, rt); err != nil || res != (remoteResult{}) {
		t.Fatalf("empty body: %v %v", res, err)
	}
	if res, err = decodeReply(reply{status: ReplyStatusOK, body: []byte(`1`)}, nil); err != nil || res != nil {
		t.Fatalf("nil type: %v %v", res, err)
	}
	// Garbage body.
	if _, err = decodeReply(reply{status: ReplyStatusOK, body: []byte(`{`)}, rt); mediator.CodeOf(err) != mediator.CodeInternal {
		t.Fatalf("garbage: %v", err)
	}
	// Error replies rebuild *mediator.Error.
	_, err = decodeReply(reply{status: ReplyStatusErr, body: []byte(`{"code":"not_found","message":"order not found","details":{"id":"o1"}}`)}, rt)
	var me *mediator.Error
	if !errors.As(err, &me) || me.Code != mediator.CodeNotFound || me.Message != "order not found" || me.Details["id"] != "o1" {
		t.Fatalf("error reply: %v", err)
	}
	if !errors.Is(err, &mediator.Error{Code: mediator.CodeNotFound}) {
		t.Fatal("errors.Is by code")
	}
	if _, err = decodeReply(reply{status: ReplyStatusErr, body: []byte(`nope`)}, rt); mediator.CodeOf(err) != mediator.CodeInternal {
		t.Fatalf("garbage error: %v", err)
	}
	if _, err = decodeReply(reply{status: ReplyStatusErr, body: []byte(`{}`)}, rt); mediator.CodeOf(err) != mediator.CodeInternal {
		t.Fatalf("empty code: %v", err)
	}
	if _, err = decodeReply(reply{status: "weird"}, rt); mediator.CodeOf(err) != mediator.CodeInternal {
		t.Fatalf("unknown status: %v", err)
	}
}

func TestReplyFromValues(t *testing.T) {
	call, rep, ok := replyFromValues(map[string]any{ReplyFieldCall: "c1", ReplyFieldStatus: "ok", ReplyFieldBody: []byte(`{}`)})
	if !ok || call != "c1" || rep.status != "ok" || string(rep.body) != "{}" {
		t.Fatalf("got %q %+v %v", call, rep, ok)
	}
	if _, _, ok := replyFromValues(map[string]any{ReplyFieldStatus: "ok"}); ok {
		t.Fatal("missing call accepted")
	}
	if _, _, ok := replyFromValues(map[string]any{ReplyFieldCall: 42}); ok {
		t.Fatal("non-string call accepted")
	}
}

func TestWaiters(t *testing.T) {
	w := newWaiters()
	ch := w.add("a")
	if w.count() != 1 {
		t.Fatal("count")
	}
	if !w.deliver("a", reply{status: "ok"}) {
		t.Fatal("deliver")
	}
	if r := <-ch; r.status != "ok" {
		t.Fatal("reply")
	}
	if w.deliver("a", reply{}) || w.deliver("zzz", reply{}) {
		t.Fatal("delivered to nobody")
	}
	w.add("b")
	w.remove("b")
	if w.count() != 0 {
		t.Fatal("remove")
	}
}

func TestDecodeRequest(t *testing.T) {
	r := decodeRequest(map[string]any{
		RPCFieldCall: "c", RPCFieldCorr: "x", RPCFieldNode: []byte("n"), RPCFieldName: "Cmd", RPCFieldBody: "{}",
		RPCFieldPrincipal: "", RPCFieldIdem: "k", RPCFieldTrace: "00-t", RPCFieldDeadline: "1700000000000",
	})
	if r.call != "c" || r.corr != "x" || r.caller != "n" || r.name != "Cmd" || r.body != "{}" || r.idem != "k" || r.trace != "00-t" ||
		!r.deadline.Equal(time.UnixMilli(1700000000000)) {
		t.Fatalf("decoded %+v", r)
	}
	r = decodeRequest(map[string]any{RPCFieldDeadline: "soon", RPCFieldCall: 1})
	if !r.deadline.IsZero() || r.call != "" {
		t.Fatalf("garbage: %+v", r)
	}
	// A deadline of zero or below means none, not the Unix epoch.
	for _, none := range []string{"0", "-1", ""} {
		if r := decodeRequest(map[string]any{RPCFieldDeadline: none}); !r.deadline.IsZero() {
			t.Fatalf("deadline %q decoded as %s", none, r.deadline)
		}
	}
	if r := decodeRequest(map[string]any{RPCFieldDeadline: "1"}); !r.deadline.Equal(time.UnixMilli(1)) {
		t.Fatalf("deadline 1 ms decoded as %s", r.deadline)
	}
}

func TestRemote_SendNilInfo(t *testing.T) {
	r := NewRemote(nil, Config{NodeID: "n"})
	if _, err := r.Send(context.Background(), nil, nil); mediator.CodeOf(err) != mediator.CodeInternal {
		t.Fatalf("got %v", err)
	}
	if r.NodeID() != "n" || r.Waiting() != 0 {
		t.Fatal("node/waiting")
	}
	r.InvalidateView("x")
}
