//go:build faultinject

package testkit_test

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// TestMain reaches a fault point before anything is armed, which is the only
// moment the "nothing armed" path is observable, since Arm() never restores it.
func TestMain(m *testing.M) {
	if err := testkit.Fault(context.Background(), "boot.unarmed"); err != nil {
		panic(err)
	}
	if err := testkit.FaultAfter(context.Background(), "boot.unarmed"); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func TestFaultInject_Enabled(t *testing.T) {
	if !testkit.FaultInjectionEnabled {
		t.Fatal("faultinject build must report enabled")
	}
	if len(testkit.AllFaultKinds) != 7 {
		t.Fatalf("kinds = %v", testkit.AllFaultKinds)
	}
	if !slices.Contains(testkit.Observed(), "boot.unarmed") {
		t.Fatalf("Observed = %v", testkit.Observed())
	}
}

func TestFaultInject_ArmDisarmHitsObserved(t *testing.T) {
	ctx := context.Background()
	testkit.Arm(testkit.Schedule{Point: "p.a", Kind: testkit.FaultError, Hit: 2})
	if err := testkit.Fault(ctx, "p.a"); err != nil {
		t.Fatalf("hit 1 must pass: %v", err)
	}
	err := testkit.Fault(ctx, "p.a")
	if !errors.Is(err, testkit.ErrInjected) {
		t.Fatalf("hit 2: %v", err)
	}
	if err := testkit.Fault(ctx, "p.a"); err != nil {
		t.Fatalf("hit 3 must pass: %v", err)
	}
	if err := testkit.Fault(ctx, "p.other"); err != nil {
		t.Fatalf("other point: %v", err)
	}
	hits := testkit.Hits()
	if hits["p.a"] != 3 || hits["p.other"] != 1 {
		t.Fatalf("hits = %v", hits)
	}
	hits["p.a"] = 99
	if testkit.Hits()["p.a"] != 3 {
		t.Fatal("Hits must return a copy")
	}
	obs := testkit.Observed()
	if !slices.Contains(obs, "p.a") || !slices.Contains(obs, "p.other") {
		t.Fatalf("observed = %v", obs)
	}
	// Arm resets hit counts; Hit 0 means every hit.
	testkit.Arm(testkit.Schedule{Point: "p.a", Kind: testkit.FaultPermanent})
	if len(testkit.Hits()) != 0 {
		t.Fatal("Arm must reset hits")
	}
	for i := 0; i < 3; i++ {
		if err := testkit.Fault(ctx, "p.a"); !errors.Is(err, testkit.ErrInjectedPermanent) {
			t.Fatalf("hit %d: %v", i+1, err)
		}
	}
	// Disarm removes schedules and resets hits.
	testkit.Disarm()
	if err := testkit.Fault(ctx, "p.a"); err != nil || testkit.Hits()["p.a"] != 1 {
		t.Fatalf("after Disarm: %v %v", err, testkit.Hits())
	}
	if !slices.Contains(testkit.Observed(), "p.a") {
		t.Fatal("Observed survives Disarm")
	}
}

func TestFaultInject_Kinds(t *testing.T) {
	bg := context.Background()
	t.Run("timeout with an expired context", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(bg, time.Now().Add(-time.Second))
		defer cancel()
		testkit.Arm(testkit.Schedule{Point: "p.t", Kind: testkit.FaultTimeout})
		if err := testkit.Fault(ctx, "p.t"); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	})
	t.Run("delay", func(t *testing.T) {
		testkit.Arm(testkit.Schedule{Point: "p.d", Kind: testkit.FaultDelay, Delay: 5 * time.Millisecond})
		start := time.Now()
		if err := testkit.Fault(bg, "p.d"); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) < 5*time.Millisecond {
			t.Fatal("did not delay")
		}
		// A canceled context cuts the delay short.
		ctx, cancel := context.WithCancel(bg)
		cancel()
		testkit.Arm(testkit.Schedule{Point: "p.d", Kind: testkit.FaultDelay, Delay: time.Hour})
		if err := testkit.Fault(ctx, "p.d"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
	t.Run("cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancel(bg)
		defer cancel()
		testkit.Arm(testkit.Schedule{Point: "p.c", Kind: testkit.FaultCancel, Cancel: cancel})
		if err := testkit.Fault(ctx, "p.c"); !errors.Is(err, context.Canceled) || ctx.Err() == nil {
			t.Fatalf("err=%v ctx=%v", err, ctx.Err())
		}
		testkit.Arm(testkit.Schedule{Point: "p.c", Kind: testkit.FaultCancel})
		if err := testkit.Fault(bg, "p.c"); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
	t.Run("ambiguous acts in FaultAfter", func(t *testing.T) {
		testkit.Arm(testkit.Schedule{Point: "p.amb", Kind: testkit.FaultAmbiguous, Hit: 1})
		if err := testkit.Fault(bg, "p.amb"); err != nil {
			t.Fatal("ambiguous must let the operation run")
		}
		if err := testkit.FaultAfter(bg, "p.amb"); !errors.Is(err, testkit.ErrInjectedAmbiguous) {
			t.Fatal(err)
		}
		// Consumed: a second FaultAfter for the same hit is a no-op, and the
		// second hit is not scheduled.
		if err := testkit.FaultAfter(bg, "p.amb"); err != nil {
			t.Fatal(err)
		}
		if err := testkit.Fault(bg, "p.amb"); err != nil {
			t.Fatal(err)
		}
		if err := testkit.FaultAfter(bg, "p.amb"); err != nil {
			t.Fatal(err)
		}
		if err := testkit.FaultAfter(bg, "never.hit"); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("crash with OnCrash", func(t *testing.T) {
		crashed := ""
		testkit.Arm(testkit.Schedule{Point: "p.crash", Kind: testkit.FaultCrash, OnCrash: func(p string) { crashed = p }})
		if err := testkit.Fault(bg, "p.crash"); err != nil {
			t.Fatal(err)
		}
		if err := testkit.FaultAfter(bg, "p.crash"); err != nil || crashed != "p.crash" {
			t.Fatalf("err=%v crashed=%q", err, crashed)
		}
	})
	t.Run("several schedules, first match wins", func(t *testing.T) {
		testkit.Arm(
			testkit.Schedule{Point: "p.x", Kind: testkit.FaultError, Hit: 5},
			testkit.Schedule{Point: "p.y", Kind: testkit.FaultPermanent},
			testkit.Schedule{Point: "p.x", Kind: testkit.FaultPermanent, Hit: 1},
		)
		if err := testkit.Fault(bg, "p.x"); !errors.Is(err, testkit.ErrInjectedPermanent) {
			t.Fatal(err)
		}
		if err := testkit.Fault(bg, "p.x"); err != nil {
			t.Fatal(err)
		}
		if err := testkit.Fault(bg, "p.y"); !errors.Is(err, testkit.ErrInjectedPermanent) {
			t.Fatal(err)
		}
	})
	t.Run("unknown kind is ignored", func(t *testing.T) {
		testkit.Arm(testkit.Schedule{Point: "p.u", Kind: testkit.FaultKind("weird")})
		if err := testkit.Fault(bg, "p.u"); err != nil {
			t.Fatal(err)
		}
	})
	testkit.Disarm()
}
