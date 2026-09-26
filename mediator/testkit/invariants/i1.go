package invariants

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// Expect describes the rows one command writes when it commits, so that I1
// can decide whether a partially present set of rows is a violation. A zero
// Expect is accepted: I1 then derives what it can from the wl_cmd_log row
// (the command name) and checks only that the marker row and every other
// row agree on presence. ExpectFor builds one from a workload request.
type Expect struct {
	// Name is the command name, the scope of its idempotency row.
	Name string
	// IdemKey is the idempotency key, "" for an unkeyed command.
	IdemKey string
	// Register lists the wl_register keys the command wrote; a row counts
	// as present when its updated_by is the command ID and as unknown when
	// another command overwrote it since.
	Register []string
	// Appends is true when the command writes a wl_appends row.
	Appends bool
	// Events is the number of outbox rows the command publishes, found by
	// the cmd envelope header.
	Events int
	// Side is true when the command's in-process handler writes wl_side.
	Side bool
}

// explicit reports whether the caller described the command.
func (e Expect) explicit() bool { return e.Name != "" }

// ExpectFor derives the Expect of a workload request (a value or pointer).
// ok is false for a type that is not a workload command.
func ExpectFor(req any) (e Expect, ok bool) {
	switch c := req.(type) {
	case *workload.SetValue:
		return ExpectFor(*c)
	case *workload.Transfer:
		return ExpectFor(*c)
	case *workload.Append:
		return ExpectFor(*c)
	case *workload.Bump:
		return ExpectFor(*c)
	case *workload.AtomicScenario:
		return ExpectFor(*c)
	case *workload.Touch:
		return ExpectFor(*c)
	case workload.SetValue:
		return Expect{Name: workload.NameSetValue, IdemKey: c.IdempotencyKey(), Register: []string{c.Key}}, true
	case workload.Transfer:
		return Expect{Name: workload.NameTransfer, IdemKey: c.IdempotencyKey()}, true
	case workload.Append:
		return Expect{Name: workload.NameAppend, IdemKey: c.IdempotencyKey(), Appends: true}, true
	case workload.Bump:
		return Expect{Name: workload.NameBump, IdemKey: c.IdempotencyKey(), Events: 1}, true
	case workload.AtomicScenario:
		return Expect{Name: workload.NameAtomicScenario, IdemKey: c.IdempotencyKey(), Register: []string{c.Key1}, Events: 2, Side: true}, true
	case workload.Touch:
		return Expect{Name: workload.NameTouch}, true
	}
	return Expect{}, false
}

// expectForName fills the parts of a zero Expect that follow from the
// command name alone.
func expectForName(name, cmdID string) Expect {
	switch name {
	case workload.NameAppend:
		return Expect{Name: name, IdemKey: cmdID, Appends: true}
	case workload.NameBump:
		return Expect{Name: name, Events: 1}
	case workload.NameAtomicScenario:
		return Expect{Name: name, Events: 2, Side: true}
	}
	return Expect{Name: name}
}

// signal is the presence of one kind of row for a command.
type signal struct {
	name     string
	present  bool
	unknown  bool // cannot tell (for example an overwritten register row)
	expected bool
	detail   any
}

// I1 checks command atomicity (G4): for every command, its wl_cmd_log
// marker, its state rows, its outbox rows (by the cmd header), its
// in-process handler's wl_side row, its idempotency row, and its execution
// counter are all present or all absent. It also cross-checks the causation
// ID of the outbox rows against the request ID the marker recorded. cmds
// maps command IDs to their expectations; a zero Expect is allowed.
func I1(ctx context.Context, env Env, cmds map[string]Expect) ([]Violation, error) {
	ids := make([]string, 0, len(cmds))
	for id := range cmds {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	var out []Violation
	for _, id := range ids {
		vs, err := checkCommand(ctx, env, id, cmds[id])
		if err != nil {
			return out, err
		}
		out = append(out, vs...)
	}
	return out, nil
}

func checkCommand(ctx context.Context, env Env, cmdID string, exp Expect) ([]Violation, error) {
	pool := env.Pool
	var logName, requestID string
	err := pool.QueryRow(ctx, `SELECT name, request_id FROM wl_cmd_log WHERE cmd_id = $1`, cmdID).Scan(&logName, &requestID)
	logPresent := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("invariants: cmd log: %w", err)
	}
	explicit := exp.explicit()
	if !explicit && logPresent {
		exp = expectForName(logName, cmdID)
	}
	var out []Violation
	if explicit && logPresent && logName != exp.Name {
		out = append(out, violation(IDI1, "wl_cmd_log names a different command", map[string]any{"cmd": cmdID, "logged": logName, "expected": exp.Name}))
	}
	signals := []signal{{name: "cmd_log", present: logPresent, expected: true}}

	// Idempotency row and execution counter.
	var idemSQL, execSQL string
	var args []any
	if exp.IdemKey != "" && exp.Name != "" {
		idemSQL = `SELECT count(*) FROM mediator_idempotency WHERE scope = $1 AND key = $2`
		execSQL = `SELECT coalesce(sum(n), 0) FROM wl_executions WHERE scope = $1 AND key = $2`
		args = []any{exp.Name, exp.IdemKey}
	} else {
		idemSQL = `SELECT count(*) FROM mediator_idempotency WHERE key = $1 AND scope = ANY($2)`
		execSQL = `SELECT coalesce(sum(n), 0) FROM wl_executions WHERE key = $1 AND scope = ANY($2)`
		args = []any{cmdID, workload.Names()}
	}
	idem, err := queryInt(ctx, pool, idemSQL, args...)
	if err != nil {
		return nil, err
	}
	exec, err := queryInt(ctx, pool, execSQL, args...)
	if err != nil {
		return nil, err
	}
	keyed := exp.IdemKey != ""
	signals = append(signals,
		signal{name: "idempotency", present: idem > 0, expected: keyed, detail: idem},
		signal{name: "executions", present: exec > 0, expected: keyed, detail: exec})

	// Outbox rows by the cmd header, and by causation ID.
	byHeader, err := queryInt(ctx, pool, `SELECT count(*) FROM mediator_outbox WHERE headers->'h'->>$1 = $2`, workload.HeaderCmd, cmdID)
	if err != nil {
		return nil, err
	}
	signals = append(signals, signal{name: "outbox", present: byHeader > 0, expected: exp.Events > 0, detail: byHeader})
	if exp.Events > 0 && byHeader > 0 && int(byHeader) != exp.Events {
		out = append(out, violation(IDI1, "outbox row count differs from the command's publications", map[string]any{"cmd": cmdID, "outbox": byHeader, "expected": exp.Events}))
	}
	if logPresent {
		byCause, err := queryInt(ctx, pool, `SELECT count(*) FROM mediator_outbox WHERE headers->>'cause' = $1`, requestID)
		if err != nil {
			return nil, err
		}
		if byCause != byHeader {
			out = append(out, violation(IDI1, "outbox rows by cmd header and by causation id disagree", map[string]any{"cmd": cmdID, "request_id": requestID, "by_header": byHeader, "by_cause": byCause}))
		}
	}

	// wl_side and wl_appends.
	side, err := queryInt(ctx, pool, `SELECT count(*) FROM wl_side WHERE cmd_id = $1`, cmdID)
	if err != nil {
		return nil, err
	}
	appends, err := queryInt(ctx, pool, `SELECT count(*) FROM wl_appends WHERE cmd_id = $1`, cmdID)
	if err != nil {
		return nil, err
	}
	signals = append(signals,
		signal{name: "side", present: side > 0, expected: exp.Side, detail: side},
		signal{name: "appends", present: appends > 0, expected: exp.Appends, detail: appends})

	// Register rows: present when written by this command, unknown when
	// overwritten since.
	for _, key := range exp.Register {
		var by string
		err := pool.QueryRow(ctx, `SELECT updated_by FROM wl_register WHERE key = $1`, key).Scan(&by)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			signals = append(signals, signal{name: "register:" + key, expected: true})
		case err != nil:
			return nil, fmt.Errorf("invariants: register: %w", err)
		case by == cmdID:
			signals = append(signals, signal{name: "register:" + key, present: true, expected: true})
		default:
			signals = append(signals, signal{name: "register:" + key, unknown: true, expected: true, detail: by})
		}
	}

	present := map[string]any{}
	missing := map[string]any{}
	unexpected := map[string]any{}
	for _, s := range signals {
		if s.unknown {
			continue
		}
		switch {
		case s.present:
			present[s.name] = s.detail
			if !s.expected && (explicit || !logPresent) {
				unexpected[s.name] = s.detail
			}
		case s.expected:
			missing[s.name] = nil
		}
	}
	details := func() map[string]any {
		return map[string]any{"cmd": cmdID, "name": exp.Name, "present": keysOf(present), "missing": keysOf(missing), "unexpected": keysOf(unexpected)}
	}
	switch {
	case !logPresent && len(present) > 0:
		out = append(out, violation(IDI1, "rows of a command exist without its wl_cmd_log marker", details()))
	case logPresent && len(missing) > 0:
		out = append(out, violation(IDI1, "command committed partially: expected rows are missing", details()))
	case logPresent && len(unexpected) > 0:
		out = append(out, violation(IDI1, "command left rows it must not write", details()))
	}
	return out, nil
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
