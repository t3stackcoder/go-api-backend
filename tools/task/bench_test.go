package main

import (
	"testing"
)

// sample is real benchstat -format csv output (warnings go to stderr and
// are not part of it).
const sampleCSV = `goos: linux
goarch: amd64
pkg: example.com/m/mediator
cpu: Intel
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
Send_DefaultChain-8,1.0010000000000002e-06,1%,1.301e-06,1%,+29.97%,p=0.002 n=6
Publish_InProcess-8,2.001e-06,1%,2.001e-06,1%,~,p=1.000 n=6
Cache_Hit-8,3e-06,1%,2.7e-06,1%,-10.00%,p=0.002 n=6
OnlyInNew-8,,,1e-06,1%
geomean,1.4152741783838232e-06,,1.613474821619476e-06,,+14.00%,

,old.txt,,new.txt,,,
,B/op,CI,B/op,CI,vs base,P
Send_DefaultChain-8,128,0%,256,0%,+100.00%,p=0.002 n=6
Publish_InProcess-8,256,0%,256,0%,~,p=1.000 n=6
geomean,181.01933598375615,,181.01933598375615,,+0.00%,

,old.txt,,new.txt,,,
,allocs/op,CI,allocs/op,CI,vs base,P
Send_DefaultChain-8,4,0%,4,0%,~,p=1.000 n=6
geomean,5.656854249492379,,5.656854249492379,,+0.00%,
`

func TestParseBenchstatCSV(t *testing.T) {
	got, err := parseBenchstatCSV(sampleCSV)
	if err != nil {
		t.Fatal(err)
	}
	want := []benchDelta{
		{Name: "Send_DefaultChain-8", Metric: "sec/op", Percent: 29.97, Significant: true, P: "p=0.002 n=6"},
		{Name: "Publish_InProcess-8", Metric: "sec/op", P: "p=1.000 n=6"},
		{Name: "Cache_Hit-8", Metric: "sec/op", Percent: -10, Significant: true, P: "p=0.002 n=6"},
		{Name: "Send_DefaultChain-8", Metric: "B/op", Percent: 100, Significant: true, P: "p=0.002 n=6"},
		{Name: "Publish_InProcess-8", Metric: "B/op", P: "p=1.000 n=6"},
		{Name: "Send_DefaultChain-8", Metric: "allocs/op", P: "p=1.000 n=6"},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d rows %+v, want %d", len(got), got, len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	bad := regressions(got, "sec/op", 10)
	if len(bad) != 1 || bad[0].Name != "Send_DefaultChain-8" {
		t.Errorf("regressions = %+v", bad)
	}
	if len(regressions(got, "sec/op", 30)) != 0 {
		t.Error("threshold 30 should pass")
	}
	if n := len(regressions(got, "B/op", 10)); n != 1 {
		t.Errorf("B/op regressions = %d, want 1", n)
	}
	if n := countMetric(got, "sec/op"); n != 3 {
		t.Errorf("countMetric = %d, want 3", n)
	}
}

func TestParseBenchstatCSVErrors(t *testing.T) {
	if _, err := parseBenchstatCSV(",sec/op,CI,sec/op,CI,vs base,P\nX-8,1,1%,1,1%,+abc%,p=0.1 n=6\n"); err == nil {
		t.Error("expected error for a bad delta")
	}
	if _, err := parseBenchstatCSV("\"unterminated\n,a\n"); err == nil {
		t.Error("expected a csv error")
	}
	got, err := parseBenchstatCSV("goos: linux\n")
	if err != nil || len(got) != 0 {
		t.Errorf("preamble only: %v %v", got, err)
	}
	// Rows before any header are ignored.
	got, err = parseBenchstatCSV("X-8,1,1%,1,1%,+5.00%,p=0.1 n=6\n")
	if err != nil || len(got) != 0 {
		t.Errorf("headerless rows: %v %v", got, err)
	}
}
