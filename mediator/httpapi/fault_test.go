//go:build faultinject

package httpapi_test

import (
	"bufio"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator/httpapi"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// These tests run only with -tags faultinject and cover the fault points of
// the adapter: http.decode, http.encode, http.sse.write.
func TestFaultPoints(t *testing.T) {
	f := newFixture(t, httpapi.Config{})
	t.Run("http.decode", func(t *testing.T) {
		testkit.Arm(testkit.Schedule{Point: "http.decode", Kind: testkit.FaultPermanent})
		defer testkit.Disarm()
		rec := f.do(t, "POST", "/orders", `{"customerId":"c"}`)
		if p := problemOf(t, rec); rec.Code != 500 || p.Detail != httpapi.InternalDetail {
			t.Errorf("%d %+v", rec.Code, p)
		}
	})
	t.Run("http.encode", func(t *testing.T) {
		testkit.Arm(testkit.Schedule{Point: "http.encode", Kind: testkit.FaultError})
		defer testkit.Disarm()
		rec := f.do(t, "POST", "/orders", `{"customerId":"c"}`)
		if p := problemOf(t, rec); rec.Code != 500 || p.Detail != httpapi.InternalDetail {
			t.Errorf("%d %+v", rec.Code, p)
		}
		if testkit.Hits()["http.encode"] != 1 {
			t.Errorf("hits %v", testkit.Hits())
		}
	})
	t.Run("http.sse.write", func(t *testing.T) {
		testkit.Arm(testkit.Schedule{Point: "http.sse.write", Kind: testkit.FaultError, Hit: 2})
		defer testkit.Disarm()
		rec := httptest.NewRecorder()
		f.srv.ServeHTTP(rec, httptest.NewRequest("GET", "/rpc/countTo?n=5", nil))
		br := bufio.NewReader(rec.Body)
		if fr, err := readFrame(br); err != nil || fr.data != "1" {
			t.Fatalf("%+v %v", fr, err)
		}
		if _, err := readFrame(br); err != io.EOF {
			t.Errorf("stream continued after the write fault: %v", err)
		}
	})
}
