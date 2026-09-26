package plan

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

// Fault modes of the dry runs, emulated with memstore.Hooks.
const (
	// DryNone injects nothing.
	DryNone = "none"
	// DryBegin fails Begin (nothing starts).
	DryBegin = "begin"
	// DryDefinite fails COMMIT with a server-reported error: nothing is applied.
	DryDefinite = "definite"
	// DryAmbiguous applies the commit and returns a connection error
	// (the FaultAmbiguous kind of the real sweep).
	DryAmbiguous = "ambiguous"
)

// DryModes lists every mode.
var DryModes = []string{DryNone, DryBegin, DryDefinite, DryAmbiguous}

// ErrDryFault is the error every emulated fault wraps.
var ErrDryFault = errors.New("plan: emulated fault")

// DryCommand is the command of the dry runs: keyed on CmdID, it publishes
// two durable events on Key1 and Key2, so its outbox rows and its
// idempotency row are the observable effects (G4 without a state row).
type DryCommand struct {
	mediator.Command[DryResult]

	CmdID string `json:"cmdId"`
	Key1  string `json:"key1"`
	Key2  string `json:"key2"`
}

// Name pins the persisted name.
func (DryCommand) Name() string { return "dry.Atomic" }

// IdempotencyKey is CmdID.
func (c DryCommand) IdempotencyKey() string { return c.CmdID }

// DryResult is the response: the execution serial, so replays are
// byte-identical only when the handler ran once.
type DryResult struct {
	Serial int64 `json:"serial"`
}

// DryEvent is the durable event of DryCommand.
type DryEvent struct {
	mediator.Event

	Key   string `json:"key"`
	CmdID string `json:"cmdId"`
}

// Name pins the persisted name.
func (DryEvent) Name() string { return "dry.Event" }

// StreamKey is Key.
func (e DryEvent) StreamKey() string { return e.Key }

// Topic is the dry topic.
func (DryEvent) Topic() string { return "dry.topic" }

// DryNode is a mediator over memstore with the unit of work and
// idempotency behaviors, plus the counters the invariants read.
type DryNode struct {
	Store      *memstore.Store
	M          *mediator.Mediator
	executions atomic.Int64
	mu         sync.Mutex
	responses  map[string][]DryResult
}

// NewDryNode builds the node. lockTimeout bounds the idempotency row lock.
func NewDryNode(lockTimeout time.Duration) (*DryNode, error) {
	n := &DryNode{Store: memstore.New(memstore.Config{Partitions: 2, DefaultLockTimeout: lockTimeout}), responses: map[string][]DryResult{}}
	m := mediator.New()
	if err := mediator.Use(m, pg.UnitOfWork(n.Store, pg.UnitOfWorkConfig{DefaultLockTimeout: lockTimeout}), mediator.Requests()); err != nil {
		return nil, err
	}
	if err := mediator.Use(m, pg.Idempotency(pg.IdempotencyConfig{}), mediator.Commands()); err != nil {
		return nil, err
	}
	if err := mediator.HandleFunc(m, n.handle); err != nil {
		return nil, err
	}
	if err := mediator.RegisterEvent[DryEvent](m); err != nil {
		return nil, err
	}
	if err := m.Build(); err != nil {
		return nil, err
	}
	n.M = m
	return n, nil
}

func (n *DryNode) handle(ctx context.Context, c DryCommand) (DryResult, error) {
	serial := n.executions.Add(1)
	keys := []string{c.Key1, c.Key2}
	sort.Strings(keys)
	for _, k := range keys {
		if err := mediator.Publish(ctx, n.M, DryEvent{Key: k, CmdID: c.CmdID}, mediator.Headers(map[string]string{"cmd": c.CmdID})); err != nil {
			return DryResult{}, err
		}
	}
	return DryResult{Serial: serial}, nil
}

// Executions counts handler invocations, including rolled-back ones.
func (n *DryNode) Executions() int64 { return n.executions.Load() }

// Arm installs the hooks of one fault mode; DryNone clears them.
func (n *DryNode) Arm(mode string) {
	n.Store.Hooks = memstore.Hooks{}
	fault := func() error { return fmt.Errorf("%w: %s", ErrDryFault, mode) }
	switch mode {
	case DryBegin:
		n.Store.Hooks.Begin = func(pg.TxOptions) error { return fault() }
	case DryDefinite:
		n.Store.Hooks.BeforeCommit = fault
	case DryAmbiguous:
		n.Store.Hooks.AfterCommit = fault
	}
}

// Send runs the command once and records a successful response under its key.
func (n *DryNode) Send(ctx context.Context, c DryCommand) (DryResult, error) {
	res, err := mediator.Send(ctx, n.M, c)
	if err == nil {
		n.mu.Lock()
		n.responses[c.CmdID] = append(n.responses[c.CmdID], res)
		n.mu.Unlock()
	}
	return res, err
}

// Responses returns every successful response per key.
func (n *DryNode) Responses() map[string][]DryResult {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make(map[string][]DryResult, len(n.responses))
	for k, v := range n.responses {
		out[k] = append([]DryResult(nil), v...)
	}
	return out
}

// Check applies the invariants of SweepCommandAtomicity and
// SweepIdempotency to the committed state for the command IDs: outbox rows
// per command are 0 or 2 (I1), the idempotency row is present exactly when
// the rows are (I1), a committed row always has a response (I5), at most
// one committed execution per key (I4), and every response for a key is
// identical (I4). It returns one error per violation.
func (n *DryNode) Check(cmdIDs ...string) []error {
	var out []error
	rows := map[string]int{}
	for _, e := range n.Store.Outbox() {
		rows[e.Envelope.Headers["cmd"]]++
	}
	idem := n.Store.Idempotency()
	responses := n.Responses()
	for _, id := range cmdIDs {
		got := rows[id]
		if got != 0 && got != 2 {
			out = append(out, fmt.Errorf("I1: command %s has %d outbox rows, want 0 or 2", id, got))
		}
		row, present := idem[memstore.IdemKey{Scope: "dry.Atomic", Key: id}]
		if present != (got > 0) {
			out = append(out, fmt.Errorf("I1: command %s: idempotency row present=%v but outbox rows=%d", id, present, got))
		}
		if present && row.Response == nil {
			out = append(out, fmt.Errorf("I5: command %s: committed idempotency row with NULL response", id))
		}
		if got > 2 {
			out = append(out, fmt.Errorf("I4: command %s committed %d times", id, got/2))
		}
		for i, r := range responses[id] {
			if r != responses[id][0] {
				out = append(out, fmt.Errorf("I4: responses for %s differ: %+v vs %+v (attempt %d)", id, responses[id][0], r, i))
				break
			}
		}
	}
	return out
}

// AtomicityDryRun sends one keyed command with the fault mode armed, then
// retries it with the fault cleared (the client behavior after an
// ambiguous outcome), and checks the invariants. It returns the errors of
// the two attempts (for the caller's expectations) and the violations.
func AtomicityDryRun(ctx context.Context, mode string) (firstErr, retryErr error, violations []error, err error) {
	n, err := NewDryNode(200 * time.Millisecond)
	if err != nil {
		return nil, nil, nil, err
	}
	cmd := DryCommand{CmdID: "c1", Key1: "b", Key2: "a"}
	n.Arm(mode)
	_, firstErr = n.Send(ctx, cmd)
	n.Arm(DryNone)
	_, retryErr = n.Send(ctx, cmd)
	return firstErr, retryErr, n.Check(cmd.CmdID), nil
}

// IdempotencyDryRun runs two clients with the same key: the first with the
// fault mode armed, the second started once the first has reached the
// handler (the "every step" of the real sweep collapses to "during the
// first execution" without fault points). Both retry until they get a
// response. It returns the number of handler executions and the
// violations; at most one execution may have committed.
func IdempotencyDryRun(ctx context.Context, mode string) (executions int64, violations []error, err error) {
	n, err := NewDryNode(50 * time.Millisecond)
	if err != nil {
		return 0, nil, err
	}
	cmd := DryCommand{CmdID: "shared", Key1: "x", Key2: "y"}
	n.Arm(mode)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = n.Send(ctx, cmd)
	}()
	go func() {
		defer wg.Done()
		// Start once the first client is inside its transaction.
		deadline := time.Now().Add(time.Second)
		for n.Store.Begun() == 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		_, _ = n.Send(ctx, cmd)
	}()
	wg.Wait()
	n.Arm(DryNone)
	// Both clients retry until they hold a response.
	for i := 0; i < 5 && len(n.Responses()[cmd.CmdID]) < 2; i++ {
		_, _ = n.Send(ctx, cmd)
	}
	return n.Executions(), n.Check(cmd.CmdID), nil
}
