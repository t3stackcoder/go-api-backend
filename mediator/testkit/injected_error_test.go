//go:build faultinject

package testkit_test

import (
	"errors"
	"net"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// TestInjectedError_Classification pins how the three injected errors
// classify themselves: the transient one retries, the permanent one does
// not, and the ambiguous one reads as a performed operation behind a
// connection-level failure, so mediator.IsTransient and net.Error callers
// agree with the fault kind.
func TestInjectedError_Classification(t *testing.T) {
	cases := []struct {
		name                                     string
		err                                      *testkit.InjectedError
		transient, ambiguous, timeout, temporary bool
	}{
		{"transient", testkit.ErrInjected, true, false, false, true},
		{"permanent", testkit.ErrInjectedPermanent, false, false, false, false},
		{"ambiguous", testkit.ErrInjectedAmbiguous, true, true, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.err.Transient() != tc.transient || tc.err.Ambiguous() != tc.ambiguous || tc.err.Timeout() != tc.timeout || tc.err.Temporary() != tc.temporary {
				t.Fatalf("%s: transient=%v ambiguous=%v timeout=%v temporary=%v", tc.err.Msg, tc.err.Transient(), tc.err.Ambiguous(), tc.err.Timeout(), tc.err.Temporary())
			}
			if tc.err.Error() != tc.err.Msg {
				t.Fatalf("Error() = %q", tc.err.Error())
			}
			// The ambiguous error satisfies net.Error through the chain.
			var ne net.Error
			if got := errors.As(error(tc.err), &ne) && ne.Timeout(); got != tc.timeout {
				t.Fatalf("net.Error timeout = %v, want %v", got, tc.timeout)
			}
		})
	}
}
