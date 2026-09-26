//go:build !faultinject

package testkit_test

import (
	"context"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

func TestFault_PlainBuildIsNoop(t *testing.T) {
	if testkit.FaultInjectionEnabled {
		t.Fatal("plain build must report fault injection disabled")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, point := range []string{"", "pg.tx.begin", "redis.xadd"} {
		if err := testkit.Fault(ctx, point); err != nil {
			t.Fatalf("Fault(%q) = %v", point, err)
		}
		if err := testkit.FaultAfter(ctx, point); err != nil {
			t.Fatalf("FaultAfter(%q) = %v", point, err)
		}
	}
	if n := testing.AllocsPerRun(100, func() { _ = testkit.Fault(ctx, "x") }); n != 0 {
		t.Fatalf("Fault allocates %v", n)
	}
}
