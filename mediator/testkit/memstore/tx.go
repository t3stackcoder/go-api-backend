package memstore

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

type txState uint8

const (
	txOpen txState = iota
	txAborted
	txCommitted
	txRolledBack
)

// Tx is one in-memory transaction. Like a Postgres transaction it is bound
// to one goroutine, and like Postgres a failed statement aborts it so that
// Commit rolls back instead.
type Tx struct {
	s           *Store
	opts        pg.TxOptions
	lockTimeout time.Duration
	state       txState
	locks       []any

	seqDelta   map[seqKey]int64
	outbox     []pg.OutboxEntry
	inbox      []inboxKey
	idemInsert map[IdemKey]*IdemRow
	idemHits   map[IdemKey]int
	idemResp   map[IdemKey][]byte
	notes      []Notification
}

// ReadOnly reports whether the transaction was opened read-only.
func (t *Tx) ReadOnly() bool { return t.opts.ReadOnly }

// Options returns the TxOptions the transaction was opened with.
func (t *Tx) Options() pg.TxOptions { return t.opts }

// check rejects statements on a closed, aborted, or canceled transaction,
// and writes on a read-only one (which aborts it, as in Postgres).
func (t *Tx) check(ctx context.Context, write bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch t.state {
	case txCommitted, txRolledBack:
		return pg.ErrTxClosed
	case txAborted:
		return pg.ErrTxAborted
	case txOpen:
		// Statements are allowed; the read-only check follows.
	}
	if write && t.opts.ReadOnly {
		t.state = txAborted
		return pg.ErrReadOnly
	}
	return nil
}

func (t *Tx) lock(ctx context.Context, key any) error {
	if err := t.s.locks.acquire(ctx, key, t, t.lockTimeout, t.s.cfg.Clock); err != nil {
		t.state = txAborted
		return err
	}
	return nil
}

// OutboxAppend locks the (topic, key) sequence row until the transaction
// ends, takes the next sequence number, and buffers the row (6.3). A
// rolled-back transaction releases its number, so committed sequences are
// dense.
func (t *Tx) OutboxAppend(ctx context.Context, env *mediator.Envelope, payload []byte) error {
	if err := t.check(ctx, true); err != nil {
		return err
	}
	k := seqKey{env.Topic, env.StreamKey}
	if err := t.lock(ctx, k); err != nil {
		return err
	}
	t.s.mu.Lock()
	base := t.s.seq[k]
	t.s.nextID++
	id := t.s.nextID
	t.s.mu.Unlock()
	if t.seqDelta == nil {
		t.seqDelta = map[seqKey]int64{}
	}
	t.seqDelta[k]++
	env.Seq = base + t.seqDelta[k]
	env.Partition = mediator.Partition(env.StreamKey, t.s.cfg.Partitions)
	e := pg.OutboxEntry{ID: id, Envelope: *env, Payload: cloneBytes(payload), CreatedAt: t.s.cfg.Clock.Now()}
	e.Envelope.Headers = maps.Clone(env.Headers)
	t.outbox = append(t.outbox, e)
	return nil
}

// InboxInsert records (group, eventID); false when the row exists,
// committed or pending in this transaction. A concurrent insert of the same
// key blocks until the first transaction ends (6.5).
func (t *Tx) InboxInsert(ctx context.Context, group string, eventID uuid.UUID) (bool, error) {
	if err := t.check(ctx, true); err != nil {
		return false, err
	}
	k := inboxKey{group, eventID}
	if err := t.lock(ctx, k); err != nil {
		return false, err
	}
	t.s.mu.Lock()
	_, exists := t.s.inbox[k]
	t.s.mu.Unlock()
	if exists || slices.Contains(t.inbox, k) {
		return false, nil
	}
	t.inbox = append(t.inbox, k)
	return true, nil
}

// IdempotencyReserve performs the upsert of 6.6 under the row lock.
func (t *Tx) IdempotencyReserve(ctx context.Context, scope, key string, requestHash []byte, ttl time.Duration) (pg.IdempotencyRow, error) {
	if err := t.check(ctx, true); err != nil {
		return pg.IdempotencyRow{}, err
	}
	k := IdemKey{scope, key}
	if err := t.lock(ctx, k); err != nil {
		return pg.IdempotencyRow{}, err
	}
	if row, ok := t.idemInsert[k]; ok {
		row.Hits++
		return pg.IdempotencyRow{Hits: row.Hits, RequestHash: cloneBytes(row.RequestHash), Response: cloneBytes(row.Response)}, nil
	}
	t.s.mu.Lock()
	row, exists := t.s.idem[k]
	var snapshot IdemRow
	if exists {
		snapshot = IdemRow{RequestHash: cloneBytes(row.RequestHash), Response: cloneBytes(row.Response), Hits: row.Hits}
	}
	t.s.mu.Unlock()
	if exists {
		if t.idemHits == nil {
			t.idemHits = map[IdemKey]int{}
		}
		t.idemHits[k]++
		resp := snapshot.Response
		if r, ok := t.idemResp[k]; ok {
			resp = cloneBytes(r)
		}
		return pg.IdempotencyRow{Hits: snapshot.Hits + t.idemHits[k], RequestHash: snapshot.RequestHash, Response: resp}, nil
	}
	now := t.s.cfg.Clock.Now()
	if t.idemInsert == nil {
		t.idemInsert = map[IdemKey]*IdemRow{}
	}
	t.idemInsert[k] = &IdemRow{RequestHash: cloneBytes(requestHash), CreatedAt: now, ExpiresAt: now.Add(ttl)}
	return pg.IdempotencyRow{RequestHash: cloneBytes(requestHash)}, nil
}

// IdempotencyStore records the response for (scope, key). Like an UPDATE
// of a missing row, storing for an unknown key does nothing.
func (t *Tx) IdempotencyStore(ctx context.Context, scope, key string, response []byte) error {
	if err := t.check(ctx, true); err != nil {
		return err
	}
	k := IdemKey{scope, key}
	if err := t.lock(ctx, k); err != nil {
		return err
	}
	if row, ok := t.idemInsert[k]; ok {
		row.Response = cloneBytes(response)
		return nil
	}
	t.s.mu.Lock()
	_, exists := t.s.idem[k]
	t.s.mu.Unlock()
	if exists {
		if t.idemResp == nil {
			t.idemResp = map[IdemKey][]byte{}
		}
		t.idemResp[k] = cloneBytes(response)
	}
	return nil
}

// Notify buffers a notification that becomes visible at commit.
func (t *Tx) Notify(ctx context.Context, channel, payload string) error {
	if err := t.check(ctx, false); err != nil {
		return err
	}
	t.notes = append(t.notes, Notification{Channel: channel, Payload: payload})
	return nil
}

// Commit applies the buffered writes and releases the locks. An aborted
// transaction rolls back and returns pg.ErrTxAborted.
func (t *Tx) Commit(ctx context.Context) error {
	switch t.state {
	case txCommitted, txRolledBack:
		return pg.ErrTxClosed
	case txAborted:
		t.discard()
		return pg.ErrTxAborted
	case txOpen:
		// Committed below.
	}
	if h := t.s.Hooks.BeforeCommit; h != nil {
		if err := h(); err != nil {
			t.discard()
			return fmt.Errorf("%w: %w", pg.ErrTxAborted, err)
		}
	}
	s := t.s
	s.mu.Lock()
	for k, d := range t.seqDelta {
		s.seq[k] += d
	}
	s.outbox = append(s.outbox, t.outbox...)
	now := s.cfg.Clock.Now()
	for _, k := range t.inbox {
		s.inbox[k] = now
	}
	for k, row := range t.idemInsert {
		cp := *row
		s.idem[k] = &cp
	}
	for k, n := range t.idemHits {
		if row := s.idem[k]; row != nil {
			row.Hits += n
		}
	}
	for k, r := range t.idemResp {
		if row := s.idem[k]; row != nil {
			row.Response = r
		}
	}
	s.notes = append(s.notes, t.notes...)
	s.committed++
	s.mu.Unlock()
	t.finish(txCommitted)
	if h := t.s.Hooks.AfterCommit; h != nil {
		return h()
	}
	return nil
}

// Rollback discards the buffered writes and releases the locks. Rolling
// back a closed transaction is not an error.
func (t *Tx) Rollback(ctx context.Context) error {
	switch t.state {
	case txCommitted, txRolledBack:
		return nil
	case txOpen, txAborted:
		// Discarded below.
	}
	t.discard()
	if h := t.s.Hooks.Rollback; h != nil {
		return h()
	}
	return nil
}

func (t *Tx) discard() {
	t.s.mu.Lock()
	t.s.rolledBack++
	t.s.mu.Unlock()
	t.finish(txRolledBack)
}

func (t *Tx) finish(state txState) {
	t.state = state
	t.s.locks.release(t)
}
