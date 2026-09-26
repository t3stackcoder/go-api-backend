//go:build faultsweep && faultinject

package faultsweep

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/invariants"
	"github.com/t3stackcoder/go-api-backend/test/faultsweep/plan"
)

// Markers the child prints on stdout.
const (
	markerIdle    = "FAULTSWEEP_IDLE"
	markerNoCrash = "FAULTSWEEP_NOCRASH"
	childTimeout  = 3 * time.Minute
	crashExitCode = 137
)

// observedByChildren accumulates the fault points recovery children
// reported, for the completeness report.
var observedByChildren sync.Map

type childResult struct {
	code    int
	out     string
	idle    bool
	noCrash bool
}

// spawnChild re-executes this test binary as a crash-sweep child.
func spawnChild(ctx context.Context, env *scenarioEnv, sc *scenario, run plan.Run, recover bool, observed string) (childResult, error) {
	exe, err := os.Executable()
	if err != nil {
		return childResult{}, err
	}
	args := plan.ChildArgs{
		Scenario: sc.name, Point: run.Point, Hit: run.Hit, Kind: run.Kind, Recover: recover,
		PGURL: pgURL, Schema: env.schema, RedisAddr: redisAddr, Prefix: env.prefix, ObservedFile: observed,
	}
	cmd := exec.CommandContext(ctx, exe, "-test.run", "^TestFaultSweepChild$", "-test.count=1", "-test.timeout="+childTimeout.String(), "-test.v")
	cmd.Env = append(os.Environ(), args.Environ()...)
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	err = cmd.Run()
	res := childResult{out: buf.String()}
	var ee *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &ee):
		res.code = ee.ExitCode()
	default:
		return res, err
	}
	res.idle = strings.Contains(res.out, markerIdle)
	res.noCrash = strings.Contains(res.out, markerNoCrash)
	return res, nil
}

// tail returns the last lines of a child's output for a failure message.
func tail(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) > 60 {
		lines = append([]string{"..."}, lines[len(lines)-60:]...)
	}
	return strings.Join(lines, "\n")
}

// executeCrashCell is the crash sweep of spec 11.4 for one cell: a child
// arms the crash schedule and runs the scenario; it dies with exit 137 at
// the point; the parent verifies the database and Redis directly, restarts
// Postgres when pgRestart is set, runs a recovery child, waits for it to
// report idle, and verifies again.
func executeCrashCell(t *testing.T, env *scenarioEnv, sc *scenario, run plan.Run, pgRestart bool) {
	t.Helper()
	if pgRestart && !opt.pgRestart {
		t.Skip("SWEEP_PG_RESTART=0")
	}
	ctx, cancel := context.WithTimeout(context.Background(), cellTimeout)
	defer cancel()
	if err := env.reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	observed := filepath.Join(t.TempDir(), "observed.txt")
	res, err := spawnChild(ctx, env, sc, run, false, observed)
	if err != nil {
		t.Fatalf("spawn crash child: %v", err)
	}
	switch res.code {
	case crashExitCode:
	case 0:
		if res.noCrash {
			t.Logf("child completed without reaching %s hit %d; not counted as coverage", run.Point, run.Hit)
			return
		}
		t.Fatalf("crash child exited 0 without the no-crash marker:\n%s", tail(res.out))
	default:
		t.Fatalf("crash child exited %d:\n%s", res.code, tail(res.out))
	}
	c := newCellRun(t, env, sc, run, nil)
	c.ctx = ctx
	c.phase = phaseRecover
	// The child cannot lie about what it committed: check the state now.
	v, err := env.verifier(ctx, t)
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	c.crashChecks(ctx, v)
	v.close()
	if t.Failed() {
		t.Logf("state after the crash violates the invariants; child output:\n%s", tail(res.out))
		return
	}
	if pgRestart {
		if err := restartPostgres(ctx); err != nil {
			if errors.Is(err, errNoDocker) {
				t.Skip(err.Error())
			}
			t.Fatalf("restart postgres: %v", err)
		}
	}
	rec, err := spawnChild(ctx, env, sc, run, true, observed)
	if err != nil {
		t.Fatalf("spawn recovery child: %v", err)
	}
	if rec.code != 0 || !rec.idle {
		t.Fatalf("recovery child exited %d (idle=%v):\n%s", rec.code, rec.idle, tail(rec.out))
	}
	if b, err := os.ReadFile(observed); err == nil {
		for _, p := range strings.Fields(string(b)) {
			observedByChildren.Store(p, true)
		}
	}
	c.verifyAll(ctx)
	c.report()
	if t.Failed() {
		t.Logf("crash child output:\n%s\nrecovery child output:\n%s", tail(res.out), tail(rec.out))
		return
	}
	coverage.Mark(run.Point, coverageKind(run.Kind))
}

// crashChecks are the point-in-time checks between a crash and recovery:
// atomicity (I1), single execution (I4), no half-done reservation (I5), and
// the partial consumer state.
func (c *cellRun) crashChecks(ctx context.Context, v *verifier) {
	c.t.Helper()
	if !c.sc.noInvariants {
		env := v.inv(c.sc.groups)
		report := func(vs []invariants.Violation, err error) {
			if err != nil {
				c.t.Errorf("crash check: %v", err)
			}
			for _, viol := range vs {
				c.t.Errorf("after crash: %s", viol)
			}
		}
		if c.sc.expect != nil {
			report(invariants.I1(ctx, env, c.sc.expect(c)))
		}
		report(invariants.I4(ctx, env))
		report(invariants.I5(ctx, env))
		if len(c.sc.groups) > 0 {
			checkPartialConsumerState(ctx, v, c.sc.groups)
		}
	}
	if c.sc.crashCheck != nil {
		c.sc.crashCheck(ctx, c, v)
	}
}

// TestFaultSweepChild is the child entry point of the crash sweep. In the
// parent process it is skipped. A crash child arms the schedule, runs the
// scenario, and expects to die; when it does not, it prints the no-crash
// marker. A recovery child rebuilds the scenario, recovers, verifies, and
// prints the idle marker.
func TestFaultSweepChild(t *testing.T) {
	if childArgs == nil {
		t.Skip("not a fault-sweep child process")
	}
	a := *childArgs
	sc := scenarioByName(a.Scenario)
	if sc == nil {
		t.Fatalf("unknown scenario %q", a.Scenario)
	}
	env := &scenarioEnv{name: sc.name, schema: a.Schema, prefix: a.Prefix, child: true}
	ctx, cancel := context.WithTimeout(context.Background(), childTimeout-15*time.Second)
	defer cancel()
	run := plan.Run{Point: a.Point, Hit: a.Hit, Kind: a.Kind}
	if a.Recover {
		run = plan.Run{}
	}
	c := newCellRun(t, env, sc, run, nil)
	c.child = true
	c.ctx = ctx
	if !a.Recover {
		c.phase = phaseRun
		testkit.Disarm()
		if s, armed := c.schedule(); armed {
			testkit.Arm(s)
		}
		if err := c.buildAndStart(ctx); err != nil {
			t.Fatalf("build: %v", err)
		}
		if err := sc.run(ctx, c); err != nil {
			c.note("run phase: %v", err)
		}
		for _, err := range c.stopAll() {
			c.note("stop: %v", err)
		}
		testkit.Disarm()
		c.report()
		fmt.Println(markerNoCrash)
		return
	}
	c.phase = phaseRecover
	if err := c.buildAndStart(ctx); err != nil {
		t.Fatalf("recovery build: %v", err)
	}
	if err := sc.recover(ctx, c); err != nil {
		t.Errorf("recovery: %v", err)
	}
	for _, err := range c.stopAll() {
		t.Errorf("recovery stop: %v", err)
	}
	c.verifyAll(ctx)
	c.report()
	if a.ObservedFile != "" {
		_ = os.WriteFile(a.ObservedFile, []byte(strings.Join(testkit.Observed(), "\n")), 0o600)
	}
	if !t.Failed() {
		fmt.Println(markerIdle)
	}
}
