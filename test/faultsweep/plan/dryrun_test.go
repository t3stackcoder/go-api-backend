package plan

import (
	"context"
	"errors"
	"testing"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// TestAtomicityDryRun exercises the atomicity logic of the pipeline (unit of
// work, idempotency, outbox) against memstore with every emulated fault, on
// every push and without Docker.
func TestAtomicityDryRun(t *testing.T) {
	ctx := context.Background()
	for _, mode := range DryModes {
		t.Run(mode, func(t *testing.T) {
			first, retry, violations, err := AtomicityDryRun(ctx, mode)
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range violations {
				t.Error(v)
			}
			if retry != nil {
				t.Fatalf("retry after the fault must succeed: %v", retry)
			}
			switch mode {
			case DryNone:
				if first != nil {
					t.Fatalf("clean run failed: %v", first)
				}
			case DryAmbiguous:
				if !errors.Is(first, ErrDryFault) || !mediator.IsAmbiguous(first) || mediator.CodeOf(first) != mediator.CodeUnavailable {
					t.Fatalf("ambiguous commit must be marked: %v", first)
				}
			case DryDefinite:
				if !errors.Is(first, ErrDryFault) || mediator.IsAmbiguous(first) {
					t.Fatalf("definite commit failure must not be ambiguous: %v", first)
				}
			case DryBegin:
				if !errors.Is(first, ErrDryFault) {
					t.Fatalf("begin failure: %v", first)
				}
			}
		})
	}
}

// TestIdempotencyDryRun races two clients on one key under every emulated
// fault: one execution commits at most, and every response is identical.
func TestIdempotencyDryRun(t *testing.T) {
	ctx := context.Background()
	for _, mode := range DryModes {
		t.Run(mode, func(t *testing.T) {
			executions, violations, err := IdempotencyDryRun(ctx, mode)
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range violations {
				t.Error(v)
			}
			if executions < 1 {
				t.Fatalf("the handler never ran")
			}
			if mode == DryNone && executions != 1 {
				t.Fatalf("clean run executed %d times", executions)
			}
		})
	}
}

// TestDryNode_CheckDetectsCorruption proves the dry-run invariants are not
// vacuous: a hand-made partial state is reported.
func TestDryNode_CheckDetectsCorruption(t *testing.T) {
	n, err := NewDryNode(0)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := n.Send(ctx, DryCommand{CmdID: "ok", Key1: "a", Key2: "b"}); err != nil {
		t.Fatal(err)
	}
	if vs := n.Check("ok", "absent"); len(vs) != 0 {
		t.Fatalf("clean state: %v", vs)
	}
	// A second execution that committed would double the outbox rows: emulate
	// it by sending under a different key that shares the cmd header.
	n.Arm(DryNone)
	if _, err := n.Send(ctx, DryCommand{CmdID: "ok2", Key1: "a", Key2: "b"}); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	n.responses["ok"] = append(n.responses["ok"], DryResult{Serial: 99})
	n.mu.Unlock()
	vs := n.Check("ok")
	if len(vs) != 1 || vs[0].Error() != "I4: responses for ok differ: {Serial:1} vs {Serial:99} (attempt 1)" {
		t.Fatalf("responses: %v", vs)
	}
}
