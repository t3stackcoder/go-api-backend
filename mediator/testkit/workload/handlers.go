package workload

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// Deps configures Register.
type Deps struct {
	// NodeID is recorded in wl_cmd_log and wl_applied. Default is the
	// mediator's node ID, else "node".
	NodeID string
	// Groups selects the consumers to register: any of AllGroups. Nil
	// registers DefaultGroups; an empty, non-nil slice registers none.
	Groups []string
	// StrictProjection makes the projector fail an event whose seq is not
	// last_seq+1 for its key, so a gap or reordering surfaces as a handler
	// error instead of a silently wrong projection.
	StrictProjection bool
	// PoisonKey is the key whose events the poison consumer always fails
	// (DLQ tests). Empty means the poison consumer never fails.
	PoisonKey string
	// NestedTouch makes the projector send a Touch command through the
	// mediator from inside its handler, joining the consumer's unit of work.
	NestedTouch bool
}

// ErrNoTransaction is returned by every handler that finds no pgx
// transaction in the context: the unit of work behavior must wrap it.
var ErrNoTransaction = errors.New("workload: no transaction in context; the unit of work must wrap the handler")

type handlers struct {
	m    *mediator.Mediator
	deps Deps
}

// Register registers every request, event, in-process handler, and the
// selected consumers on m with the generic API. It must run before Build.
func Register(m *mediator.Mediator, deps Deps) error {
	if deps.NodeID == "" {
		deps.NodeID = m.NodeID()
	}
	if deps.NodeID == "" {
		deps.NodeID = "node"
	}
	groups := deps.Groups
	if groups == nil {
		groups = DefaultGroups
	}
	h := &handlers{m: m, deps: deps}
	var errs []error
	errs = append(errs,
		mediator.HandleFunc(m, h.setValue),
		mediator.HandleFunc(m, h.getValue),
		mediator.HandleFunc(m, h.getValueCached),
		mediator.HandleFunc(m, h.transfer),
		mediator.HandleFunc(m, h.readAll),
		mediator.HandleFunc(m, h.append),
		mediator.HandleFunc(m, h.readList),
		mediator.HandleFunc(m, h.bump),
		mediator.HandleFunc(m, h.atomicScenario),
		mediator.HandleFunc(m, h.panic),
		mediator.HandleFunc(m, h.slow),
		mediator.HandleFunc(m, h.touch),
		mediator.OnFunc(m, h.atomicDone),
		mediator.RegisterEvent[Bumped](m),
	)
	for _, g := range groups {
		switch g {
		case GroupReadModel:
			errs = append(errs, mediator.ConsumeFunc(m, GroupReadModel, h.project))
		case GroupAudit:
			errs = append(errs, mediator.ConsumeFunc(m, GroupAudit, h.audit))
		case GroupPoison:
			errs = append(errs, mediator.ConsumeFunc(m, GroupPoison, h.poison))
		default:
			errs = append(errs, fmt.Errorf("workload: unknown consumer group %q", g))
		}
	}
	return errors.Join(errs...)
}

// tx returns the ambient pgx transaction or ErrNoTransaction.
func tx(ctx context.Context) (pgx.Tx, error) {
	t, ok := pg.TxFrom(ctx)
	if !ok {
		return nil, mediator.Wrap(mediator.CodeInternal, "workload: handler outside a unit of work", ErrNoTransaction)
	}
	return t, nil
}

// logCommand writes the wl_cmd_log marker of a command and, when idemKey
// is set, increments its execution counter (I1, I4).
func (h *handlers) logCommand(ctx context.Context, t pgx.Tx, name, key, cmdID, idemKey string) error {
	if _, err := t.Exec(ctx, `INSERT INTO wl_cmd_log (cmd_id, name, key, node, request_id)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (cmd_id) DO UPDATE SET name = EXCLUDED.name, key = EXCLUDED.key, node = EXCLUDED.node, request_id = EXCLUDED.request_id, at = now()`,
		cmdID, name, key, h.deps.NodeID, mediator.RequestID(ctx).String()); err != nil {
		return fmt.Errorf("workload: cmd log: %w", err)
	}
	if idemKey == "" {
		return nil
	}
	if _, err := t.Exec(ctx, `INSERT INTO wl_executions (scope, key, n) VALUES ($1, $2, 1)
ON CONFLICT (scope, key) DO UPDATE SET n = wl_executions.n + 1`, name, idemKey); err != nil {
		return fmt.Errorf("workload: executions: %w", err)
	}
	return nil
}

// cmdIDOr returns id, or a request-derived ID when the command carries none.
func cmdIDOr(ctx context.Context, id string) string {
	if id != "" {
		return id
	}
	return "req:" + mediator.RequestID(ctx).String()
}

func (h *handlers) setValue(ctx context.Context, c SetValue) (mediator.Void, error) {
	t, err := tx(ctx)
	if err != nil {
		return mediator.Void{}, err
	}
	if _, err := t.Exec(ctx, `INSERT INTO wl_register (key, val, updated_by) VALUES ($1, $2, $3)
ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val, updated_by = EXCLUDED.updated_by`, c.Key, c.Val, c.CmdID); err != nil {
		return mediator.Void{}, fmt.Errorf("workload: set value: %w", err)
	}
	return mediator.Void{}, h.logCommand(ctx, t, NameSetValue, c.Key, c.CmdID, c.IdempotencyKey())
}

func readValue(ctx context.Context, key string) (ValueView, error) {
	t, err := tx(ctx)
	if err != nil {
		return ValueView{}, err
	}
	v := ValueView{Key: key}
	err = t.QueryRow(ctx, `SELECT val FROM wl_register WHERE key = $1`, key).Scan(&v.Val)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return v, nil
	case err != nil:
		return ValueView{}, fmt.Errorf("workload: get value: %w", err)
	}
	v.Found = true
	return v, nil
}

func (h *handlers) getValue(ctx context.Context, q GetValue) (ValueView, error) {
	return readValue(ctx, q.Key)
}

func (h *handlers) getValueCached(ctx context.Context, q GetValueCached) (ValueView, error) {
	return readValue(ctx, q.Key)
}

func (h *handlers) transfer(ctx context.Context, c Transfer) (mediator.Void, error) {
	t, err := tx(ctx)
	if err != nil {
		return mediator.Void{}, err
	}
	// Lock both rows in account order so two opposite transfers cannot
	// deadlock, then check funds under the lock.
	rows, err := t.Query(ctx, `SELECT account, balance FROM wl_bank WHERE account = $1 OR account = $2 ORDER BY account FOR UPDATE`, c.From, c.To)
	if err != nil {
		return mediator.Void{}, fmt.Errorf("workload: transfer lock: %w", err)
	}
	balances := map[string]int64{}
	for rows.Next() {
		var a string
		var b int64
		if err := rows.Scan(&a, &b); err != nil {
			rows.Close()
			return mediator.Void{}, fmt.Errorf("workload: transfer scan: %w", err)
		}
		balances[a] = b
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return mediator.Void{}, fmt.Errorf("workload: transfer lock: %w", err)
	}
	for _, a := range []string{c.From, c.To} {
		if _, ok := balances[a]; !ok {
			return mediator.Void{}, mediator.E(mediator.CodeNotFound, "account not found").WithDetail("account", a)
		}
	}
	if balances[c.From] < c.Amt {
		return mediator.Void{}, mediator.E(mediator.CodePrecondition, "insufficient funds").
			WithDetail("account", c.From).WithDetail("balance", balances[c.From]).WithDetail("amt", c.Amt)
	}
	if _, err := t.Exec(ctx, `UPDATE wl_bank SET balance = balance - $2 WHERE account = $1`, c.From, c.Amt); err != nil {
		return mediator.Void{}, fmt.Errorf("workload: transfer debit: %w", err)
	}
	if _, err := t.Exec(ctx, `UPDATE wl_bank SET balance = balance + $2 WHERE account = $1`, c.To, c.Amt); err != nil {
		return mediator.Void{}, fmt.Errorf("workload: transfer credit: %w", err)
	}
	return mediator.Void{}, h.logCommand(ctx, t, NameTransfer, c.From+">"+c.To, c.CmdID, c.IdempotencyKey())
}

func (h *handlers) readAll(ctx context.Context, _ ReadAll) (BankView, error) {
	t, err := tx(ctx)
	if err != nil {
		return BankView{}, err
	}
	rows, err := t.Query(ctx, `SELECT account, balance FROM wl_bank ORDER BY account`)
	if err != nil {
		return BankView{}, fmt.Errorf("workload: read all: %w", err)
	}
	defer rows.Close()
	view := BankView{Balances: map[string]int64{}}
	for rows.Next() {
		var a string
		var b int64
		if err := rows.Scan(&a, &b); err != nil {
			return BankView{}, fmt.Errorf("workload: read all: %w", err)
		}
		view.Balances[a] = b
		view.Total += b
	}
	if err := rows.Err(); err != nil {
		return BankView{}, fmt.Errorf("workload: read all: %w", err)
	}
	return view, nil
}

func (h *handlers) append(ctx context.Context, c Append) (AppendResult, error) {
	t, err := tx(ctx)
	if err != nil {
		return AppendResult{}, err
	}
	var res AppendResult
	// The primary key on cmd_id turns a second execution of the same command
	// into an error: the idempotency behavior must have prevented it.
	if err := t.QueryRow(ctx, `INSERT INTO wl_appends (cmd_id, key, val) VALUES ($1, $2, $3) RETURNING applied`, c.CmdID, c.Key, c.Val).Scan(&res.Applied); err != nil {
		if pg.IsUniqueViolation(err) {
			return AppendResult{}, mediator.Wrap(mediator.CodeConflict, "append executed twice for one command id", err)
		}
		return AppendResult{}, fmt.Errorf("workload: append: %w", err)
	}
	return res, h.logCommand(ctx, t, NameAppend, c.Key, c.CmdID, c.IdempotencyKey())
}

func (h *handlers) readList(ctx context.Context, q ReadList) ([]int64, error) {
	t, err := tx(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := t.Query(ctx, `SELECT val FROM wl_appends WHERE key = $1 ORDER BY applied`, q.Key)
	if err != nil {
		return nil, fmt.Errorf("workload: read list: %w", err)
	}
	defer rows.Close()
	out := []int64{}
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("workload: read list: %w", err)
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workload: read list: %w", err)
	}
	return out, nil
}

// bumpKey increments the counter of key and publishes Bumped with the
// command header, in the ambient transaction.
func (h *handlers) bumpKey(ctx context.Context, t pgx.Tx, key, cmdID string) (int64, error) {
	var n int64
	if err := t.QueryRow(ctx, `INSERT INTO wl_bumps (key, n) VALUES ($1, 1)
ON CONFLICT (key) DO UPDATE SET n = wl_bumps.n + 1 RETURNING n`, key).Scan(&n); err != nil {
		return 0, fmt.Errorf("workload: bump: %w", err)
	}
	if err := mediator.Publish(ctx, h.m, Bumped{Key: key, N: n}, mediator.Headers(map[string]string{HeaderCmd: cmdID})); err != nil {
		return 0, err
	}
	return n, nil
}

func (h *handlers) bump(ctx context.Context, c Bump) (BumpResult, error) {
	t, err := tx(ctx)
	if err != nil {
		return BumpResult{}, err
	}
	cmdID := cmdIDOr(ctx, c.CmdID)
	n, err := h.bumpKey(ctx, t, c.Key, cmdID)
	if err != nil {
		return BumpResult{}, err
	}
	return BumpResult{N: n}, h.logCommand(ctx, t, NameBump, c.Key, cmdID, c.IdempotencyKey())
}

func (h *handlers) atomicScenario(ctx context.Context, c AtomicScenario) (mediator.Void, error) {
	t, err := tx(ctx)
	if err != nil {
		return mediator.Void{}, err
	}
	if _, err := t.Exec(ctx, `INSERT INTO wl_register (key, val, updated_by) VALUES ($1, $2, $3)
ON CONFLICT (key) DO UPDATE SET val = EXCLUDED.val, updated_by = EXCLUDED.updated_by`, c.Key1, c.Val, c.CmdID); err != nil {
		return mediator.Void{}, fmt.Errorf("workload: atomic register: %w", err)
	}
	// Two durable events on two keys, published in key order so that
	// concurrent scenarios take the per-key sequence locks in one order.
	keys := []string{c.Key1, c.Key2}
	slices.Sort(keys)
	for _, k := range keys {
		if _, err := h.bumpKey(ctx, t, k, c.CmdID); err != nil {
			return mediator.Void{}, err
		}
	}
	if err := mediator.Publish(ctx, h.m, AtomicDone{CmdID: c.CmdID, Note: "atomic " + c.Key1 + " " + c.Key2}); err != nil {
		return mediator.Void{}, err
	}
	return mediator.Void{}, h.logCommand(ctx, t, NameAtomicScenario, c.Key1, c.CmdID, c.IdempotencyKey())
}

// atomicDone is the in-process handler of AtomicDone: it writes wl_side in
// the publisher's transaction.
func (h *handlers) atomicDone(ctx context.Context, e AtomicDone) error {
	t, err := tx(ctx)
	if err != nil {
		return err
	}
	if _, err := t.Exec(ctx, `INSERT INTO wl_side (cmd_id, note) VALUES ($1, $2)
ON CONFLICT (cmd_id) DO UPDATE SET note = EXCLUDED.note`, e.CmdID, e.Note); err != nil {
		return fmt.Errorf("workload: side: %w", err)
	}
	return nil
}

func (h *handlers) panic(context.Context, Panic) (mediator.Void, error) {
	panic("workload: Panic command")
}

func (h *handlers) slow(ctx context.Context, c Slow) (mediator.Void, error) {
	timer := time.NewTimer(time.Duration(c.Millis) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return mediator.Void{}, nil
	case <-ctx.Done():
		return mediator.Void{}, mediator.Wrap(mediator.CodeTimeout, "slow command canceled", ctx.Err())
	}
}

func (h *handlers) touch(ctx context.Context, c Touch) (mediator.Void, error) {
	t, err := tx(ctx)
	if err != nil {
		return mediator.Void{}, err
	}
	return mediator.Void{}, h.logCommand(ctx, t, NameTouch, c.Key, c.CmdID, "")
}

// recordApply inserts the wl_applied row of one consumer apply.
func (h *handlers) recordApply(ctx context.Context, t pgx.Tx, group string, e Bumped) error {
	env, ok := mediator.EnvelopeFrom(ctx)
	if !ok {
		return mediator.E(mediator.CodeInternal, "workload: consumer without an envelope") // covergate:ignore Deliver always sets the envelope
	}
	token, _ := mediator.FencingToken(ctx)
	if _, err := t.Exec(ctx, `INSERT INTO wl_applied (grp, event_id, key, seq, fencing, node) VALUES ($1, $2, $3, $4, $5, $6)`,
		group, env.ID, e.Key, env.Seq, token, h.deps.NodeID); err != nil {
		return fmt.Errorf("workload: applied: %w", err)
	}
	return nil
}

// project is the read-model consumer: it maintains count and last_seq per
// key and records every apply with its fencing token (I3, I6, G14).
func (h *handlers) project(ctx context.Context, e Bumped) error {
	t, err := tx(ctx)
	if err != nil {
		return err
	}
	env, ok := mediator.EnvelopeFrom(ctx)
	if !ok {
		return mediator.E(mediator.CodeInternal, "workload: consumer without an envelope") // covergate:ignore Deliver always sets the envelope
	}
	if h.deps.StrictProjection {
		var last int64
		err := t.QueryRow(ctx, `SELECT last_seq FROM wl_projection WHERE grp = $1 AND key = $2 FOR UPDATE`, GroupReadModel, e.Key).Scan(&last)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("workload: projection read: %w", err)
		}
		if env.Seq != last+1 {
			return mediator.E(mediator.CodeInternal, fmt.Sprintf("workload: projection gap on key %s: got seq %d, want %d", e.Key, env.Seq, last+1)).
				WithDetail("key", e.Key).WithDetail("seq", env.Seq).WithDetail("last_seq", last)
		}
	}
	if _, err := t.Exec(ctx, `INSERT INTO wl_projection (grp, key, count, last_seq) VALUES ($1, $2, 1, $3)
ON CONFLICT (grp, key) DO UPDATE SET count = wl_projection.count + 1, last_seq = EXCLUDED.last_seq`, GroupReadModel, e.Key, env.Seq); err != nil {
		return fmt.Errorf("workload: projection: %w", err)
	}
	if err := h.recordApply(ctx, t, GroupReadModel, e); err != nil {
		return err
	}
	if h.deps.NestedTouch {
		if _, err := mediator.Send(ctx, h.m, Touch{Key: e.Key, CmdID: env.ID.String() + ":touch"}); err != nil {
			return err
		}
	}
	return nil
}

// audit is the audit consumer: one wl_audit row per event (I3).
func (h *handlers) audit(ctx context.Context, e Bumped) error {
	t, err := tx(ctx)
	if err != nil {
		return err
	}
	env, ok := mediator.EnvelopeFrom(ctx)
	if !ok {
		return mediator.E(mediator.CodeInternal, "workload: consumer without an envelope") // covergate:ignore Deliver always sets the envelope
	}
	if _, err := t.Exec(ctx, `INSERT INTO wl_audit (grp, event_id, key, seq) VALUES ($1, $2, $3, $4)`, GroupAudit, env.ID, e.Key, env.Seq); err != nil {
		if pg.IsUniqueViolation(err) {
			return mediator.Wrap(mediator.CodeConflict, "audit row applied twice for one event", err)
		}
		return fmt.Errorf("workload: audit: %w", err)
	}
	return h.recordApply(ctx, t, GroupAudit, e)
}

// poison is the failing consumer: every event of PoisonKey fails with a
// non-transient error so that it is dead-lettered after the attempt cap;
// every other event is recorded like the other groups.
func (h *handlers) poison(ctx context.Context, e Bumped) error {
	if h.deps.PoisonKey != "" && e.Key == h.deps.PoisonKey {
		return mediator.E(mediator.CodeInternal, "workload: poison key").WithDetail("key", e.Key)
	}
	t, err := tx(ctx)
	if err != nil {
		return err
	}
	return h.recordApply(ctx, t, GroupPoison, e)
}
