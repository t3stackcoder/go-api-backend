package invariants

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

type outboxRow struct {
	EventID   uuid.UUID
	Topic     string
	Partition int
	StreamKey string
	Seq       int64
}

func scanOutboxRow(rows pgx.CollectableRow) (outboxRow, error) {
	var r outboxRow
	err := rows.Scan(&r.EventID, &r.Topic, &r.Partition, &r.StreamKey, &r.Seq)
	return r, err
}

// I2 checks relay delivery (G5): every outbox row with published_at set
// appears in its partition stream at least once, by event ID, and no
// unpublished row is older than the bound. The stream part needs Redis.
func I2(ctx context.Context, env Env) ([]Violation, error) {
	var out []Violation
	old, err := query(ctx, env.Pool, func(rows pgx.CollectableRow) (int64, error) {
		var id int64
		return id, rows.Scan(&id)
	}, `SELECT id FROM mediator_outbox WHERE published_at IS NULL AND created_at < now() - $1::interval ORDER BY id`, env.bound())
	if err != nil {
		return nil, err
	}
	if len(old) > 0 {
		ids := old
		if len(ids) > maxDetails {
			ids = ids[:maxDetails]
		}
		out = append(out, violation(IDI2, "unpublished outbox rows older than the bound", map[string]any{"count": len(old), "bound": env.bound().String(), "ids": ids}))
	}
	if env.Redis == nil {
		return out, nil
	}
	published, err := query(ctx, env.Pool, scanOutboxRow,
		`SELECT event_id, topic, partition, stream_key, seq FROM mediator_outbox WHERE published_at IS NOT NULL AND topic = ANY($1) ORDER BY id`, env.topics())
	if err != nil {
		return nil, err
	}
	idx, err := scanStreams(ctx, env)
	if err != nil {
		return nil, err
	}
	keys := env.keys()
	missing := map[uuid.UUID]bool{}
	for _, r := range published {
		if !idx.byStream[keys.Stream(r.Topic, r.Partition)][r.EventID] {
			missing[r.EventID] = true
		}
	}
	if len(missing) > 0 {
		out = append(out, violation(IDI2, "published outbox rows missing from their partition stream", map[string]any{"count": len(missing), "event_ids": sortedIDs(missing)}))
	}
	return out, nil
}

// I3 checks consumer effects (G7, G12): per group, the inbox holds exactly
// the distinct event IDs delivered to the group's streams (minus its dead
// letters); the read-model projection equals the state derived from
// wl_bumps and the outbox; the audit log and every group's apply log hold
// exactly the committed events, each once. Run it after quiescence.
func I3(ctx context.Context, env Env) ([]Violation, error) {
	var out []Violation
	committed, err := query(ctx, env.Pool, scanOutboxRow,
		`SELECT event_id, topic, partition, stream_key, seq FROM mediator_outbox WHERE topic = ANY($1) ORDER BY id`, env.topics())
	if err != nil {
		return nil, err
	}
	committedIDs := map[uuid.UUID]bool{}
	perKey := map[string]int64{}
	for _, r := range committed {
		committedIDs[r.EventID] = true
		if r.Topic == workload.TopicBumped {
			perKey[r.StreamKey]++
		}
	}
	var idx *streamIndex
	if env.Redis != nil {
		if idx, err = scanStreams(ctx, env); err != nil {
			return nil, err
		}
	}
	groups := env.groups()
	for _, g := range groups {
		dlq, _, err := dlqIDs(ctx, env, g)
		if err != nil {
			return nil, err
		}
		// Inbox versus delivered.
		if idx != nil {
			inbox, err := query(ctx, env.Pool, func(rows pgx.CollectableRow) (uuid.UUID, error) {
				var id uuid.UUID
				return id, rows.Scan(&id)
			}, `SELECT event_id FROM mediator_inbox WHERE consumer_group = $1`, g)
			if err != nil {
				return nil, err
			}
			inboxIDs := map[uuid.UUID]bool{}
			for _, id := range inbox {
				inboxIDs[id] = true
			}
			missing, extra := map[uuid.UUID]bool{}, map[uuid.UUID]bool{}
			for id := range idx.all {
				if !inboxIDs[id] && !dlq[id] {
					missing[id] = true
				}
			}
			for id := range inboxIDs {
				if !idx.all[id] {
					extra[id] = true
				}
			}
			if len(missing) > 0 {
				out = append(out, violation(IDI3, "events delivered to the group's streams have no inbox row", map[string]any{"group": g, "count": len(missing), "event_ids": sortedIDs(missing)}))
			}
			if len(extra) > 0 {
				out = append(out, violation(IDI3, "inbox rows for events absent from every stream", map[string]any{"group": g, "count": len(extra), "event_ids": sortedIDs(extra)}))
			}
		}
		// Apply log versus committed events, once each.
		applied, err := query(ctx, env.Pool, func(rows pgx.CollectableRow) (uuid.UUID, error) {
			var id uuid.UUID
			return id, rows.Scan(&id)
		}, `SELECT event_id FROM wl_applied WHERE grp = $1 ORDER BY applied`, g)
		if err != nil {
			return nil, err
		}
		count := map[uuid.UUID]int{}
		for _, id := range applied {
			count[id]++
		}
		twice, unknown, unapplied := map[uuid.UUID]bool{}, map[uuid.UUID]bool{}, map[uuid.UUID]bool{}
		for id, n := range count {
			if n > 1 {
				twice[id] = true
			}
			if !committedIDs[id] {
				unknown[id] = true
			}
		}
		for id := range committedIDs {
			if count[id] == 0 && !dlq[id] {
				unapplied[id] = true
			}
		}
		if len(twice) > 0 {
			out = append(out, violation(IDI3, "events applied more than once by the group", map[string]any{"group": g, "count": len(twice), "event_ids": sortedIDs(twice)}))
		}
		if len(unknown) > 0 {
			out = append(out, violation(IDI3, "group applied events that are not in the outbox", map[string]any{"group": g, "count": len(unknown), "event_ids": sortedIDs(unknown)}))
		}
		if len(unapplied) > 0 {
			out = append(out, violation(IDI3, "committed events the group never applied", map[string]any{"group": g, "count": len(unapplied), "event_ids": sortedIDs(unapplied)}))
		}
		switch g {
		case workload.GroupReadModel:
			vs, err := checkProjection(ctx, env, perKey, dlq, committed)
			if err != nil {
				return nil, err
			}
			out = append(out, vs...)
		case workload.GroupAudit:
			audit, err := query(ctx, env.Pool, func(rows pgx.CollectableRow) (uuid.UUID, error) {
				var id uuid.UUID
				return id, rows.Scan(&id)
			}, `SELECT event_id FROM wl_audit WHERE grp = $1`, g)
			if err != nil {
				return nil, err
			}
			auditIDs := map[uuid.UUID]bool{}
			for _, id := range audit {
				auditIDs[id] = true
			}
			missing, extra := map[uuid.UUID]bool{}, map[uuid.UUID]bool{}
			for id := range committedIDs {
				if !auditIDs[id] && !dlq[id] {
					missing[id] = true
				}
			}
			for id := range auditIDs {
				if !committedIDs[id] {
					extra[id] = true
				}
			}
			if len(missing) > 0 {
				out = append(out, violation(IDI3, "committed events without an audit row", map[string]any{"group": g, "count": len(missing), "event_ids": sortedIDs(missing)}))
			}
			if len(extra) > 0 {
				out = append(out, violation(IDI3, "audit rows for events that are not in the outbox", map[string]any{"group": g, "count": len(extra), "event_ids": sortedIDs(extra)}))
			}
		}
	}
	return out, nil
}

// checkProjection compares wl_projection with wl_bumps and the outbox for
// the read-model group. Keys with dead letters are skipped: a gap is then
// expected (G6 precondition).
func checkProjection(ctx context.Context, env Env, outboxPerKey map[string]int64, dlq map[uuid.UUID]bool, committed []outboxRow) ([]Violation, error) {
	dlqKeys := map[string]bool{}
	for _, r := range committed {
		if dlq[r.EventID] {
			dlqKeys[r.StreamKey] = true
		}
	}
	type bump struct {
		Key string
		N   int64
	}
	bumps, err := query(ctx, env.Pool, func(rows pgx.CollectableRow) (bump, error) {
		var b bump
		return b, rows.Scan(&b.Key, &b.N)
	}, `SELECT key, n FROM wl_bumps ORDER BY key`)
	if err != nil {
		return nil, err
	}
	type proj struct {
		Key     string
		Count   int64
		LastSeq int64
	}
	projs, err := query(ctx, env.Pool, func(rows pgx.CollectableRow) (proj, error) {
		var p proj
		return p, rows.Scan(&p.Key, &p.Count, &p.LastSeq)
	}, `SELECT key, count, last_seq FROM wl_projection WHERE grp = $1 ORDER BY key`, workload.GroupReadModel)
	if err != nil {
		return nil, err
	}
	byKey := map[string]proj{}
	for _, p := range projs {
		byKey[p.Key] = p
	}
	var out []Violation
	seen := map[string]bool{}
	for _, b := range bumps {
		seen[b.Key] = true
		if n := outboxPerKey[b.Key]; n != b.N {
			out = append(out, violation(IDI3, "wl_bumps counter and outbox rows disagree for key", map[string]any{"key": b.Key, "bumps": b.N, "outbox": n}))
		}
		if dlqKeys[b.Key] {
			continue
		}
		p, ok := byKey[b.Key]
		if !ok {
			out = append(out, violation(IDI3, "no projection row for a bumped key", map[string]any{"key": b.Key, "bumps": b.N}))
			continue
		}
		if p.Count != b.N || p.LastSeq != b.N {
			out = append(out, violation(IDI3, "projection differs from the derived state", map[string]any{"key": b.Key, "count": p.Count, "last_seq": p.LastSeq, "bumps": b.N}))
		}
	}
	for _, p := range projs {
		if !seen[p.Key] {
			out = append(out, violation(IDI3, "projection row for a key that was never bumped", map[string]any{"key": p.Key, "count": p.Count}))
		}
	}
	return out, nil
}

// I4 checks idempotent execution (G8): no keyed command executed more than
// once (wl_executions.n <= 1), and every completed idempotency row of a
// workload command has an execution counter. Use SameResponses for the
// byte-identical response check on the responses the scenario collected.
func I4(ctx context.Context, env Env) ([]Violation, error) {
	type row struct {
		Scope, Key string
		N          int
	}
	multi, err := query(ctx, env.Pool, func(rows pgx.CollectableRow) (row, error) {
		var r row
		return r, rows.Scan(&r.Scope, &r.Key, &r.N)
	}, `SELECT scope, key, n FROM wl_executions WHERE n > 1 ORDER BY scope, key`)
	if err != nil {
		return nil, err
	}
	var out []Violation
	for _, r := range multi {
		out = append(out, violation(IDI4, "keyed command executed more than once", map[string]any{"scope": r.Scope, "key": r.Key, "executions": r.N}))
	}
	orphans, err := query(ctx, env.Pool, func(rows pgx.CollectableRow) (row, error) {
		var r row
		return r, rows.Scan(&r.Scope, &r.Key)
	}, `SELECT i.scope, i.key FROM mediator_idempotency i
WHERE i.response IS NOT NULL AND i.scope = ANY($1)
  AND NOT EXISTS (SELECT 1 FROM wl_executions e WHERE e.scope = i.scope AND e.key = i.key)
ORDER BY i.scope, i.key`, workload.Names())
	if err != nil {
		return nil, err
	}
	for _, r := range orphans {
		out = append(out, violation(IDI4, "completed idempotency row without an execution", map[string]any{"scope": r.Scope, "key": r.Key}))
	}
	return out, nil
}

// SameResponses checks that every response recorded for one key is
// byte-identical (G8): the map holds, per idempotency key, the response
// bodies every attempt received.
func SameResponses(responses map[string][][]byte) []Violation {
	keys := make([]string, 0, len(responses))
	for k := range responses {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out []Violation
	for _, k := range keys {
		bodies := responses[k]
		for i := 1; i < len(bodies); i++ {
			if !bytes.Equal(bodies[0], bodies[i]) {
				out = append(out, violation(IDI4, "responses for one idempotency key differ", map[string]any{"key": k, "first": string(bodies[0]), "other": string(bodies[i]), "index": i}))
				break
			}
		}
	}
	return out
}

// I5 checks that recovery left nothing half done: no idempotency row with a
// NULL response, and no pending stream entry idle for longer than the
// bound. Goroutine leaks are the caller's job: wrap the scenario with
// go.uber.org/goleak (VerifyNone) in the test process, because only the
// process that ran the scenario can count its goroutines.
func I5(ctx context.Context, env Env) ([]Violation, error) {
	type row struct {
		Scope, Key string
		Hits       int
		CreatedAt  time.Time
	}
	reserved, err := query(ctx, env.Pool, func(rows pgx.CollectableRow) (row, error) {
		var r row
		return r, rows.Scan(&r.Scope, &r.Key, &r.Hits, &r.CreatedAt)
	}, `SELECT scope, key, hits, created_at FROM mediator_idempotency WHERE response IS NULL ORDER BY scope, key`)
	if err != nil {
		return nil, err
	}
	var out []Violation
	for _, r := range reserved {
		out = append(out, violation(IDI5, "idempotency row with NULL response", map[string]any{"scope": r.Scope, "key": r.Key, "hits": r.Hits, "created_at": r.CreatedAt}))
	}
	if env.Redis == nil {
		return out, nil
	}
	keys := env.keys()
	for _, g := range env.groups() {
		for _, t := range env.topics() {
			for p := 0; p < env.partitions(); p++ {
				stream := keys.Stream(t, p)
				entries, err := env.Redis.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: stream, Group: g, Start: "-", End: "+", Count: 1000}).Result()
				if err != nil {
					if isMissing(err) {
						continue
					}
					return nil, fmt.Errorf("invariants: xpending %s %s: %w", stream, g, err)
				}
				for _, e := range entries {
					if e.Idle > env.bound() {
						out = append(out, violation(IDI5, "pending entry idle for longer than the bound", map[string]any{"group": g, "stream": stream, "id": e.ID, "consumer": e.Consumer, "idle": e.Idle.String(), "deliveries": e.RetryCount}))
					}
				}
			}
		}
	}
	return out, nil
}

// I6 checks per-key ordering (G6): for every group and key, the seqs in
// wl_applied are exactly 1..n in apply order. Keys with a dead letter in
// the group are skipped, because dead-lettering leaves a gap by design.
func I6(ctx context.Context, env Env) ([]Violation, error) {
	type row struct {
		Group, Key string
		Seq        int64
		EventID    uuid.UUID
	}
	rows, err := query(ctx, env.Pool, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.Group, &x.Key, &x.Seq, &x.EventID)
	}, `SELECT grp, key, seq, event_id FROM wl_applied ORDER BY grp, key, applied`)
	if err != nil {
		return nil, err
	}
	dlqByGroup := map[string]map[uuid.UUID]bool{}
	for _, g := range env.groups() {
		ids, _, err := dlqIDs(ctx, env, g)
		if err != nil {
			return nil, err
		}
		dlqByGroup[g] = ids
	}
	dlqKeys := map[string]bool{}
	if env.Redis != nil {
		committed, err := query(ctx, env.Pool, scanOutboxRow,
			`SELECT event_id, topic, partition, stream_key, seq FROM mediator_outbox WHERE topic = ANY($1)`, env.topics())
		if err != nil {
			return nil, err
		}
		for g, ids := range dlqByGroup {
			for _, r := range committed {
				if ids[r.EventID] {
					dlqKeys[g+"/"+r.StreamKey] = true
				}
			}
		}
	}
	type gk struct{ group, key string }
	seqs := map[gk][]int64{}
	var order []gk
	for _, r := range rows {
		k := gk{r.Group, r.Key}
		if _, ok := seqs[k]; !ok {
			order = append(order, k)
		}
		seqs[k] = append(seqs[k], r.Seq)
	}
	var out []Violation
	for _, k := range order {
		if dlqKeys[k.group+"/"+k.key] {
			continue
		}
		for i, s := range seqs[k] {
			if s != int64(i+1) {
				out = append(out, violation(IDI6, "apply order is not exactly 1..n", map[string]any{"group": k.group, "key": k.key, "index": i, "got": s, "want": i + 1, "seqs": capSeqs(seqs[k])}))
				break
			}
		}
	}
	return out, nil
}

func capSeqs(s []int64) []int64 {
	if len(s) > maxDetails {
		return s[:maxDetails]
	}
	return s
}

// I7 checks bounded staleness (G9) over recorded reads and writes: a cached
// read invoked after a write returned must not return an older value unless
// a degraded window covering one of its tags overlaps the read. Windows must
// already be extended by the TTL of the query (history.BoundedStalenessChecker
// takes the TTL instead); it is a pure function.
func I7(reads []history.Read, writes []history.Write, degraded []history.Window) []Violation {
	var out []Violation
	for _, s := range history.CheckStaleness(reads, writes, degraded, 0) {
		v := history.StaleViolation(s)
		v.ID = IDI7
		out = append(out, v)
	}
	return out
}

// L1 is the liveness check (G15): polled until the bound, the outbox has no
// unpublished row, no group has a pending entry, and no session is blocked
// on the idempotency table. The violation carries the last state observed.
func L1(ctx context.Context, env Env) ([]Violation, error) {
	deadline := time.Now().Add(env.bound())
	var last map[string]any
	for {
		state, quiet, err := quiescence(ctx, env)
		if err != nil {
			return nil, err
		}
		if quiet {
			return nil, nil
		}
		last = state
		if time.Now().After(deadline) {
			last["bound"] = env.bound().String()
			return []Violation{violation(IDL1, "system did not quiesce within the bound", last)}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(env.poll()):
		}
	}
}

// quiescence samples the three liveness conditions once.
func quiescence(ctx context.Context, env Env) (map[string]any, bool, error) {
	unpublished, err := queryInt(ctx, env.Pool, `SELECT count(*) FROM mediator_outbox WHERE published_at IS NULL`)
	if err != nil {
		return nil, false, err
	}
	blocked, err := queryInt(ctx, env.Pool, `SELECT count(*) FROM pg_stat_activity
WHERE wait_event_type = 'Lock' AND query ILIKE '%mediator_idempotency%'`)
	if err != nil {
		return nil, false, err
	}
	var pending int64
	if env.Redis != nil {
		lags, err := redisx.ConsumerLag(ctx, env.Redis, env.Cfg, env.groups(), env.topics())
		if err != nil {
			return nil, false, fmt.Errorf("invariants: %w", err)
		}
		for _, l := range lags {
			pending += l.Pending
		}
	}
	state := map[string]any{"unpublished": unpublished, "pending": pending, "blocked_idempotency": blocked}
	return state, unpublished == 0 && pending == 0 && blocked == 0, nil
}

// Fencing checks G14 from wl_applied: per group and partition, in apply
// order, tokens never decrease and a change of node comes with a token
// strictly greater than every token the partition saw before.
func Fencing(ctx context.Context, env Env) ([]Violation, error) {
	type row struct {
		Group, Key, Node string
		Token, Applied   int64
	}
	rows, err := query(ctx, env.Pool, func(r pgx.CollectableRow) (row, error) {
		var x row
		return x, r.Scan(&x.Group, &x.Key, &x.Node, &x.Token, &x.Applied)
	}, `SELECT grp, key, node, fencing, applied FROM wl_applied ORDER BY applied`)
	if err != nil {
		return nil, err
	}
	records := make([]history.FencingRecord, 0, len(rows))
	for _, r := range rows {
		records = append(records, history.FencingRecord{Group: r.Group, Partition: env.partitionOf(r.Key), Node: r.Node, Token: r.Token, Line: int(r.Applied)})
	}
	return history.CheckFencing(records), nil
}

// DLQEmpty checks that no group has dead letters (events workload).
func DLQEmpty(ctx context.Context, env Env) ([]Violation, error) {
	if env.Redis == nil {
		return nil, nil
	}
	var out []Violation
	for _, g := range env.groups() {
		_, entries, err := dlqIDs(ctx, env, g)
		if err != nil {
			return nil, err
		}
		for i, e := range entries {
			if i >= maxDetails {
				break
			}
			out = append(out, violation(IDDLQ, "dead letter present", map[string]any{"group": g, "id": e.ID, "event_id": e.Envelope.ID.String(), "key": e.Envelope.StreamKey, "seq": e.Envelope.Seq, "attempts": e.Attempts, "error": e.Error}))
		}
	}
	return out, nil
}

// Options selects the checks of All that need scenario data.
type Options struct {
	// Expect enables I1 for the listed commands.
	Expect map[string]Expect
	// Responses enables the SameResponses part of I4.
	Responses map[string][][]byte
	// Reads, Writes, and Degraded enable I7 when Reads is not empty.
	Reads    []history.Read
	Writes   []history.Write
	Degraded []history.Window
	// SkipLiveness skips L1, which otherwise runs first and may wait up to
	// the bound.
	SkipLiveness bool
	// SkipDLQ skips DLQEmpty for runs that dead-letter on purpose.
	SkipDLQ bool
}

// All runs L1 (unless skipped) and then every applicable check, collecting
// the violations of all of them. Check errors (a lost connection, a
// malformed row) are joined and returned together with the violations found
// so far.
func All(ctx context.Context, env Env, opts Options) (Report, error) {
	var rep Report
	var errs []error
	run := func(vs []Violation, err error) {
		rep.Add(vs...)
		if err != nil {
			errs = append(errs, err)
		}
	}
	if !opts.SkipLiveness {
		run(L1(ctx, env))
	}
	if len(opts.Expect) > 0 {
		run(I1(ctx, env, opts.Expect))
	}
	run(I2(ctx, env))
	run(I3(ctx, env))
	run(I4(ctx, env))
	if len(opts.Responses) > 0 {
		rep.Add(SameResponses(opts.Responses)...)
	}
	run(I5(ctx, env))
	run(I6(ctx, env))
	if len(opts.Reads) > 0 {
		rep.Add(I7(opts.Reads, opts.Writes, opts.Degraded)...)
	}
	run(Fencing(ctx, env))
	if !opts.SkipDLQ {
		run(DLQEmpty(ctx, env))
	}
	return rep, errors.Join(errs...)
}
