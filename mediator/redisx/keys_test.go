package redisx

import "testing"

func TestKeys_Layout(t *testing.T) {
	k := Keys{Prefix: "mediator"}
	cases := map[string]string{
		k.Stream("orders", 3):               "mediator:{orders:p3}",
		k.DLQ("proj"):                       "mediator:dlq:{proj}",
		k.Lease("proj", "orders", 12):       "mediator:lease:{proj:orders:p12}",
		k.LeasePattern("proj"):              "mediator:lease:{proj:*}",
		k.Members("proj"):                   "mediator:members:proj",
		k.Cache("GetOrder:abc"):             "mediator:cache:GetOrder:abc",
		k.TagVer("orders"):                  "mediator:tagver:orders",
		k.TagVerPrefix():                    "mediator:tagver:",
		k.RateLimit("CreateOrder", "u1"):    "mediator:rl:CreateOrder:u1",
		k.RPC("CreateOrder"):                "mediator:rpc:CreateOrder",
		k.Reply("node-1"):                   "mediator:reply:node-1",
		k.Handlers("CreateOrder"):           "mediator:handlers:CreateOrder",
		Config{Prefix: "x"}.Keys().DLQ("g"): "x:dlq:{g}",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestKeys_ParseLease(t *testing.T) {
	k := Keys{Prefix: "mediator"}
	g, topic, p, ok := k.ParseLease(k.Lease("proj", "orders", 7))
	if !ok || g != "proj" || topic != "orders" || p != 7 {
		t.Fatalf("parse: %q %q %d %v", g, topic, p, ok)
	}
	// Partition 0 is the first partition, not a rejected value.
	if g, topic, p, ok := k.ParseLease(k.Lease("proj", "orders", 0)); !ok || g != "proj" || topic != "orders" || p != 0 {
		t.Fatalf("parse p0: %q %q %d %v", g, topic, p, ok)
	}
	for _, bad := range []string{
		"", "other:lease:{proj:orders:p1}", "mediator:lease:{proj:orders:p1", "mediator:lease:{proj:orders:x1}",
		"mediator:lease:{projorders:p1}", "mediator:lease:{proj::p1}", "mediator:lease:{:orders:p1}",
		"mediator:lease:{proj:orders:p-1}", "mediator:lease:{proj:orders:pabc}", "mediator:lease:{noparts}",
	} {
		if _, _, _, ok := k.ParseLease(bad); ok {
			t.Errorf("ParseLease(%q) accepted", bad)
		}
	}
}

func TestKeys_ParseRPC(t *testing.T) {
	k := Keys{Prefix: "m"}
	if name, ok := k.ParseRPC("m:rpc:CreateOrder"); !ok || name != "CreateOrder" {
		t.Fatalf("got %q %v", name, ok)
	}
	if _, ok := k.ParseRPC("m:rpc:"); ok {
		t.Fatal("empty name accepted")
	}
	if _, ok := k.ParseRPC("other"); ok {
		t.Fatal("foreign key accepted")
	}
}

func TestLeaseValue_RoundTrip(t *testing.T) {
	v := LeaseValue("host:with:colons-1", 42)
	if v != "host:with:colons-1:42" {
		t.Fatalf("value %q", v)
	}
	node, epoch, ok := ParseLeaseValue(v)
	if !ok || node != "host:with:colons-1" || epoch != 42 {
		t.Fatalf("parse: %q %d %v", node, epoch, ok)
	}
	for _, bad := range []string{"", "node", ":1", "node:x"} {
		if _, _, ok := ParseLeaseValue(bad); ok {
			t.Errorf("ParseLeaseValue(%q) accepted", bad)
		}
	}
}

func TestStreamIDMillis(t *testing.T) {
	if ms, ok := streamIDMillis("1700000000123-4"); !ok || ms != 1700000000123 {
		t.Fatalf("got %d %v", ms, ok)
	}
	if ms, ok := streamIDMillis("1700000000123"); !ok || ms != 1700000000123 {
		t.Fatalf("got %d %v", ms, ok)
	}
	if _, ok := streamIDMillis("abc-1"); ok {
		t.Fatal("garbage accepted")
	}
	// A leading dash leaves an empty millisecond part; it must not be read
	// as a negative number.
	for _, bad := range []string{"-5", "-", ""} {
		if ms, ok := streamIDMillis(bad); ok {
			t.Fatalf("streamIDMillis(%q) = %d, accepted", bad, ms)
		}
	}
}
