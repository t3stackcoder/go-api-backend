// Package storetest is the conformance suite every pg.Store implementation
// passes: PgStore in the integration tier and testkit/memstore in the unit
// tier, which is how the fake is kept honest (spec 11.2).
package storetest

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
)

// Factory returns a fresh, empty store for one subtest.
type Factory func(t *testing.T) pg.Store

// blockFor is how long a goroutine must stay blocked for the suite to
// accept that it is waiting on a row lock.
const blockFor = 150 * time.Millisecond

// Run executes the suite. Every subtest gets its own store and runs in
// parallel, so factory must return isolated stores.
func Run(t *testing.T, factory Factory) {
	tests := []struct {
		name string
		fn   func(t *testing.T, s pg.Store)
	}{
		{"BeginCommitRollback", testBeginCommitRollback},
		{"OutboxSeqDense", testOutboxSeqDense},
		{"OutboxSameKeyBlocksUntilCommit", testOutboxSameKeyBlocks},
		{"OutboxRollbackReleasesSeq", testOutboxRollbackReleases},
		{"OutboxPartitionAndKeys", testOutboxPartitionAndKeys},
		{"InboxDuplicate", testInboxDuplicate},
		{"InboxConcurrentBlocks", testInboxConcurrent},
		{"PartitionEpochFence", testPartitionEpochFence},
		{"IdempotencyReserveStoreReplay", testIdemReserveStoreReplay},
		{"IdempotencyConcurrentBlocksThenReplays", testIdemConcurrent},
		{"IdempotencyRollbackReleasesReservation", testIdemRollbackReleases},
		{"IdempotencyLockTimeout", testIdemLockTimeout},
		{"ReadOnlyRejectsWrites", testReadOnly},
		{"Notify", testNotify},
		{"FencingTokenMonotonic", testFencing},
		{"Ping", testPing},
		{"ClosedTransaction", testClosed},
		{"CancelledContext", testCancelled},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, factory(t))
		})
	}
}

func env(topic, key string) *mediator.Envelope {
	now := time.Now()
	return &mediator.Envelope{ID: mediator.NewID(now), Type: "Thing", Topic: topic, StreamKey: key, OccurredAt: now, SchemaVersion: 1,
		Headers: map[string]string{"h": "v"}}
}

func begin(t *testing.T, s pg.Store, opts pg.TxOptions) pg.Tx {
	t.Helper()
	tx, err := s.Begin(context.Background(), opts)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	// A failed subtest must not leave a transaction holding a connection.
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	return tx
}

// sameJSON compares two JSON documents structurally: Postgres re-serializes
// jsonb, so byte equality is not part of the contract.
func sameJSON(a, b []byte) bool {
	ca, err1 := mediator.CanonicalizeJSON(a)
	cb, err2 := mediator.CanonicalizeJSON(b)
	return err1 == nil && err2 == nil && bytes.Equal(ca, cb)
}

func appendSeq(t *testing.T, tx pg.Tx, topic, key string) int64 {
	t.Helper()
	e := env(topic, key)
	if err := tx.OutboxAppend(context.Background(), e, []byte(`{"n":1}`)); err != nil {
		t.Fatalf("append: %v", err)
	}
	return e.Seq
}

func commit(t *testing.T, tx pg.Tx) {
	t.Helper()
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func rollback(t *testing.T, tx pg.Tx) {
	t.Helper()
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("rollback: %v", err)
	}
}

// assertBlocked fails when done fires within blockFor.
func assertBlocked(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
		t.Fatal("expected the transaction to block on the row lock")
	case <-time.After(blockFor):
	}
}

func await(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("blocked transaction did not resume")
	}
}

func testBeginCommitRollback(t *testing.T, s pg.Store) {
	rw := begin(t, s, pg.TxOptions{})
	if rw.ReadOnly() {
		t.Fatal("read-write transaction reports ReadOnly")
	}
	commit(t, rw)
	ro := begin(t, s, pg.TxOptions{ReadOnly: true})
	if !ro.ReadOnly() {
		t.Fatal("read-only transaction reports read-write")
	}
	rollback(t, ro)
	tx := begin(t, s, pg.TxOptions{})
	appendSeq(t, tx, "t", "k")
	rollback(t, tx)
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatalf("second rollback: %v", err)
	}
}

func testOutboxSeqDense(t *testing.T, s pg.Store) {
	tx := begin(t, s, pg.TxOptions{})
	for want := int64(1); want <= 3; want++ {
		if got := appendSeq(t, tx, "t", "k"); got != want {
			t.Fatalf("seq %d, want %d", got, want)
		}
	}
	commit(t, tx)
	tx2 := begin(t, s, pg.TxOptions{})
	if got := appendSeq(t, tx2, "t", "k"); got != 4 {
		t.Fatalf("seq after commit %d, want 4", got)
	}
	if got := appendSeq(t, tx2, "t", "other"); got != 1 {
		t.Fatalf("seq for another key %d, want 1", got)
	}
	if got := appendSeq(t, tx2, "u", "k"); got != 1 {
		t.Fatalf("seq for another topic %d, want 1", got)
	}
	commit(t, tx2)
}

func testOutboxSameKeyBlocks(t *testing.T, s pg.Store) {
	tx1 := begin(t, s, pg.TxOptions{})
	if got := appendSeq(t, tx1, "t", "k"); got != 1 {
		t.Fatalf("seq %d, want 1", got)
	}
	tx2 := begin(t, s, pg.TxOptions{})
	done := make(chan struct{})
	var seq2 int64
	go func() {
		defer close(done)
		seq2 = appendSeq(t, tx2, "t", "k")
	}()
	assertBlocked(t, done)
	commit(t, tx1)
	await(t, done)
	if seq2 != 2 {
		t.Fatalf("second publisher got seq %d, want 2", seq2)
	}
	commit(t, tx2)
}

func testOutboxRollbackReleases(t *testing.T, s pg.Store) {
	tx1 := begin(t, s, pg.TxOptions{})
	appendSeq(t, tx1, "t", "k")
	rollback(t, tx1)
	tx2 := begin(t, s, pg.TxOptions{})
	if got := appendSeq(t, tx2, "t", "k"); got != 1 {
		t.Fatalf("seq after rollback %d, want 1 (dense)", got)
	}
	tx3 := begin(t, s, pg.TxOptions{})
	done := make(chan struct{})
	var seq3 int64
	go func() {
		defer close(done)
		seq3 = appendSeq(t, tx3, "t", "k")
	}()
	assertBlocked(t, done)
	rollback(t, tx2)
	await(t, done)
	if seq3 != 1 {
		t.Fatalf("waiter after rollback got seq %d, want 1", seq3)
	}
	commit(t, tx3)
}

func testOutboxPartitionAndKeys(t *testing.T, s pg.Store) {
	partitions := 1
	if p, ok := s.(interface{ Partitions() int }); ok {
		partitions = p.Partitions()
	}
	tx := begin(t, s, pg.TxOptions{})
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, k := range keys {
		e := env("t", k)
		if err := tx.OutboxAppend(context.Background(), e, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if want := mediator.Partition(k, partitions); e.Partition != want {
			t.Fatalf("key %q partition %d, want %d", k, e.Partition, want)
		}
	}
	commit(t, tx)
}

func testInboxDuplicate(t *testing.T, s pg.Store) {
	id := uuid.New()
	tx := begin(t, s, pg.TxOptions{})
	fresh, err := tx.InboxInsert(context.Background(), "g", id)
	if err != nil || !fresh {
		t.Fatalf("first insert fresh=%v err=%v", fresh, err)
	}
	fresh, err = tx.InboxInsert(context.Background(), "g", id)
	if err != nil || fresh {
		t.Fatalf("same-transaction duplicate fresh=%v err=%v", fresh, err)
	}
	commit(t, tx)
	tx2 := begin(t, s, pg.TxOptions{})
	fresh, err = tx2.InboxInsert(context.Background(), "g", id)
	if err != nil || fresh {
		t.Fatalf("committed duplicate fresh=%v err=%v", fresh, err)
	}
	fresh, err = tx2.InboxInsert(context.Background(), "other", id)
	if err != nil || !fresh {
		t.Fatalf("other group fresh=%v err=%v", fresh, err)
	}
	fresh, err = tx2.InboxInsert(context.Background(), "g", uuid.New())
	if err != nil || !fresh {
		t.Fatalf("other event fresh=%v err=%v", fresh, err)
	}
	commit(t, tx2)
}

func testInboxConcurrent(t *testing.T, s pg.Store) {
	id := uuid.New()
	tx1 := begin(t, s, pg.TxOptions{})
	if fresh, err := tx1.InboxInsert(context.Background(), "g", id); err != nil || !fresh {
		t.Fatalf("first insert fresh=%v err=%v", fresh, err)
	}
	tx2 := begin(t, s, pg.TxOptions{})
	done := make(chan struct{})
	var fresh2 bool
	var err2 error
	go func() {
		defer close(done)
		fresh2, err2 = tx2.InboxInsert(context.Background(), "g", id)
	}()
	assertBlocked(t, done)
	commit(t, tx1)
	await(t, done)
	if err2 != nil || fresh2 {
		t.Fatalf("after commit fresh=%v err=%v, want false", fresh2, err2)
	}
	commit(t, tx2)

	id2 := uuid.New()
	tx3 := begin(t, s, pg.TxOptions{})
	if _, err := tx3.InboxInsert(context.Background(), "g", id2); err != nil {
		t.Fatal(err)
	}
	tx4 := begin(t, s, pg.TxOptions{})
	done = make(chan struct{})
	go func() {
		defer close(done)
		fresh2, err2 = tx4.InboxInsert(context.Background(), "g", id2)
	}()
	assertBlocked(t, done)
	rollback(t, tx3)
	await(t, done)
	if err2 != nil || !fresh2 {
		t.Fatalf("after rollback fresh=%v err=%v, want true", fresh2, err2)
	}
	commit(t, tx4)
}

// testPartitionEpochFence checks FencePartition (7.2, G14): the epoch of a
// (group, topic, partition) row only moves up, an equal token is accepted
// again, rows are independent, a rolled-back fence leaves the committed
// epoch, and a concurrent fence of the same row blocks until the first
// transaction ends and then sees its epoch.
func testPartitionEpochFence(t *testing.T, s pg.Store) {
	ctx := context.Background()
	fence := func(tx pg.Tx, group, topic string, partition int, token int64) bool {
		t.Helper()
		ok, err := tx.FencePartition(ctx, group, topic, partition, token)
		if err != nil {
			t.Fatalf("fence (%s, %s, %d) with %d: %v", group, topic, partition, token, err)
		}
		return ok
	}
	// Monotonic: 5, then 7; 6 is rejected; 7 again is accepted.
	tx := begin(t, s, pg.TxOptions{})
	if !fence(tx, "g", "t", 0, 5) {
		t.Fatal("first token must be accepted")
	}
	commit(t, tx)
	tx = begin(t, s, pg.TxOptions{})
	if !fence(tx, "g", "t", 0, 7) {
		t.Fatal("higher token must be accepted")
	}
	commit(t, tx)
	tx = begin(t, s, pg.TxOptions{})
	if fence(tx, "g", "t", 0, 6) {
		t.Fatal("lower token must be rejected")
	}
	// A rejection leaves the transaction usable and other rows unaffected.
	if !fence(tx, "g", "t", 1, 6) || !fence(tx, "g", "u", 0, 6) || !fence(tx, "h", "t", 0, 6) {
		t.Fatal("another partition, topic, or group must accept its own token")
	}
	commit(t, tx)
	tx = begin(t, s, pg.TxOptions{})
	if !fence(tx, "g", "t", 0, 7) {
		t.Fatal("equal token must be accepted")
	}
	commit(t, tx)
	// Rollback keeps the committed epoch: 9 rolled back, then 8 is accepted.
	tx = begin(t, s, pg.TxOptions{})
	if !fence(tx, "g", "t", 0, 9) {
		t.Fatal("9 must be accepted before the rollback")
	}
	rollback(t, tx)
	tx = begin(t, s, pg.TxOptions{})
	if !fence(tx, "g", "t", 0, 8) {
		t.Fatal("after the rollback the committed epoch is 7, so 8 must be accepted")
	}
	commit(t, tx)
	// Concurrent: the second owner's fence blocks on the row until the
	// first commits, and its lower token is then rejected.
	tx1 := begin(t, s, pg.TxOptions{})
	if !fence(tx1, "g", "t", 0, 10) {
		t.Fatal("10 must be accepted")
	}
	tx2 := begin(t, s, pg.TxOptions{})
	done := make(chan struct{})
	var ok2 bool
	var err2 error
	go func() {
		defer close(done)
		ok2, err2 = tx2.FencePartition(ctx, "g", "t", 0, 9)
	}()
	assertBlocked(t, done)
	commit(t, tx1)
	await(t, done)
	if err2 != nil || ok2 {
		t.Fatalf("token 9 after the first owner committed 10: ok=%v err=%v, want rejected", ok2, err2)
	}
	commit(t, tx2)
	// A waiting higher token is accepted once the first owner commits.
	tx3 := begin(t, s, pg.TxOptions{})
	if !fence(tx3, "g", "t", 0, 11) {
		t.Fatal("11 must be accepted")
	}
	tx4 := begin(t, s, pg.TxOptions{})
	done = make(chan struct{})
	go func() {
		defer close(done)
		ok2, err2 = tx4.FencePartition(ctx, "g", "t", 0, 12)
	}()
	assertBlocked(t, done)
	commit(t, tx3)
	await(t, done)
	if err2 != nil || !ok2 {
		t.Fatalf("token 12 after the first owner committed 11: ok=%v err=%v, want accepted", ok2, err2)
	}
	commit(t, tx4)
}

var (
	hashA = bytes.Repeat([]byte{1}, 32)
	hashB = bytes.Repeat([]byte{2}, 32)
)

func testIdemReserveStoreReplay(t *testing.T, s pg.Store) {
	ctx := context.Background()
	tx := begin(t, s, pg.TxOptions{})
	row, err := tx.IdempotencyReserve(ctx, "Cmd", "k1", hashA, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if row.Hits != 0 || row.Response != nil || !bytes.Equal(row.RequestHash, hashA) {
		t.Fatalf("first reserve: %+v", row)
	}
	if err := tx.IdempotencyStore(ctx, "Cmd", "k1", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	commit(t, tx)
	tx2 := begin(t, s, pg.TxOptions{})
	row, err = tx2.IdempotencyReserve(ctx, "Cmd", "k1", hashA, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if row.Hits != 1 || !bytes.Equal(row.RequestHash, hashA) || !sameJSON(row.Response, []byte(`{"ok":true}`)) {
		t.Fatalf("replay: hits=%d hash=%x response=%s", row.Hits, row.RequestHash, row.Response)
	}
	commit(t, tx2)
	tx3 := begin(t, s, pg.TxOptions{})
	row, err = tx3.IdempotencyReserve(ctx, "Cmd", "k1", hashB, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if row.Hits != 2 || !bytes.Equal(row.RequestHash, hashA) {
		t.Fatalf("mismatching payload must still return the stored hash: %+v", row)
	}
	rollback(t, tx3)
	tx4 := begin(t, s, pg.TxOptions{})
	if row, err := tx4.IdempotencyReserve(ctx, "Other", "k1", hashA, time.Hour); err != nil || row.Hits != 0 {
		t.Fatalf("another scope must not collide: %+v %v", row, err)
	}
	rollback(t, tx4)
}

func testIdemConcurrent(t *testing.T, s pg.Store) {
	ctx := context.Background()
	tx1 := begin(t, s, pg.TxOptions{})
	if _, err := tx1.IdempotencyReserve(ctx, "Cmd", "k", hashA, time.Hour); err != nil {
		t.Fatal(err)
	}
	tx2 := begin(t, s, pg.TxOptions{})
	done := make(chan struct{})
	var row2 pg.IdempotencyRow
	var err2 error
	go func() {
		defer close(done)
		row2, err2 = tx2.IdempotencyReserve(ctx, "Cmd", "k", hashA, time.Hour)
	}()
	assertBlocked(t, done)
	if err := tx1.IdempotencyStore(ctx, "Cmd", "k", []byte(`"done"`)); err != nil {
		t.Fatal(err)
	}
	commit(t, tx1)
	await(t, done)
	if err2 != nil {
		t.Fatal(err2)
	}
	if row2.Hits != 1 || !sameJSON(row2.Response, []byte(`"done"`)) {
		t.Fatalf("waiter must see the completed row: %+v", row2)
	}
	commit(t, tx2)
}

func testIdemRollbackReleases(t *testing.T, s pg.Store) {
	ctx := context.Background()
	tx1 := begin(t, s, pg.TxOptions{})
	if _, err := tx1.IdempotencyReserve(ctx, "Cmd", "k", hashA, time.Hour); err != nil {
		t.Fatal(err)
	}
	tx2 := begin(t, s, pg.TxOptions{})
	done := make(chan struct{})
	var row2 pg.IdempotencyRow
	var err2 error
	go func() {
		defer close(done)
		row2, err2 = tx2.IdempotencyReserve(ctx, "Cmd", "k", hashA, time.Hour)
	}()
	assertBlocked(t, done)
	rollback(t, tx1)
	await(t, done)
	if err2 != nil {
		t.Fatal(err2)
	}
	if row2.Hits != 0 || row2.Response != nil {
		t.Fatalf("waiter must become the executor after a rollback: %+v", row2)
	}
	commit(t, tx2)
}

func testIdemLockTimeout(t *testing.T, s pg.Store) {
	ctx := context.Background()
	tx1 := begin(t, s, pg.TxOptions{})
	if _, err := tx1.IdempotencyReserve(ctx, "Cmd", "k", hashA, time.Hour); err != nil {
		t.Fatal(err)
	}
	tx2 := begin(t, s, pg.TxOptions{LockTimeout: 100 * time.Millisecond})
	_, err := tx2.IdempotencyReserve(ctx, "Cmd", "k", hashA, time.Hour)
	if !pg.IsLockTimeout(err) {
		t.Fatalf("want lock timeout, got %v", err)
	}
	if err := tx2.Commit(ctx); !errors.Is(err, pg.ErrTxAborted) {
		t.Fatalf("commit after a failed statement must report an aborted transaction, got %v", err)
	}
	rollback(t, tx1)
}

func testReadOnly(t *testing.T, s pg.Store) {
	ctx := context.Background()
	ops := []struct {
		name string
		fn   func(tx pg.Tx) error
	}{
		{"OutboxAppend", func(tx pg.Tx) error { return tx.OutboxAppend(ctx, env("t", "k"), []byte(`{}`)) }},
		{"InboxInsert", func(tx pg.Tx) error { _, err := tx.InboxInsert(ctx, "g", uuid.New()); return err }},
		{"FencePartition", func(tx pg.Tx) error { _, err := tx.FencePartition(ctx, "g", "t", 0, 1); return err }},
		{"IdempotencyReserve", func(tx pg.Tx) error { _, err := tx.IdempotencyReserve(ctx, "C", "k", hashA, time.Hour); return err }},
		{"IdempotencyStore", func(tx pg.Tx) error { return tx.IdempotencyStore(ctx, "C", "k", []byte(`1`)) }},
	}
	for _, op := range ops {
		tx := begin(t, s, pg.TxOptions{ReadOnly: true})
		if err := op.fn(tx); !pg.IsReadOnly(err) {
			t.Errorf("%s in a read-only transaction: want read-only error, got %v", op.name, err)
		}
		rollback(t, tx)
	}
}

func testNotify(t *testing.T, s pg.Store) {
	tx := begin(t, s, pg.TxOptions{})
	if err := tx.Notify(context.Background(), pg.NotifyChannel, "t:0"); err != nil {
		t.Fatal(err)
	}
	commit(t, tx)
}

func testFencing(t *testing.T, s pg.Store) {
	var prev int64
	for i := 0; i < 3; i++ {
		n, err := s.NextFencingToken(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if n <= prev {
			t.Fatalf("token %d not greater than %d", n, prev)
		}
		prev = n
	}
}

func testPing(t *testing.T, s pg.Store) {
	if err := s.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func testClosed(t *testing.T, s pg.Store) {
	ctx := context.Background()
	tx := begin(t, s, pg.TxOptions{})
	commit(t, tx)
	if err := tx.Commit(ctx); !errors.Is(err, pg.ErrTxClosed) {
		t.Fatalf("second commit: want ErrTxClosed, got %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback after commit: %v", err)
	}
	if err := tx.OutboxAppend(ctx, env("t", "k"), []byte(`{}`)); !errors.Is(err, pg.ErrTxClosed) {
		t.Fatalf("append after commit: want ErrTxClosed, got %v", err)
	}
	if _, err := tx.InboxInsert(ctx, "g", uuid.New()); !errors.Is(err, pg.ErrTxClosed) {
		t.Fatalf("inbox after commit: want ErrTxClosed, got %v", err)
	}
	if _, err := tx.FencePartition(ctx, "g", "t", 0, 1); !errors.Is(err, pg.ErrTxClosed) {
		t.Fatalf("fence after commit: want ErrTxClosed, got %v", err)
	}
	if _, err := tx.IdempotencyReserve(ctx, "C", "k", hashA, time.Hour); !errors.Is(err, pg.ErrTxClosed) {
		t.Fatalf("reserve after commit: want ErrTxClosed, got %v", err)
	}
	if err := tx.IdempotencyStore(ctx, "C", "k", nil); !errors.Is(err, pg.ErrTxClosed) {
		t.Fatalf("store after commit: want ErrTxClosed, got %v", err)
	}
	if err := tx.Notify(ctx, "c", "p"); !errors.Is(err, pg.ErrTxClosed) {
		t.Fatalf("notify after commit: want ErrTxClosed, got %v", err)
	}
}

func testCancelled(t *testing.T, s pg.Store) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Begin(canceled, pg.TxOptions{}); err == nil {
		t.Fatal("begin with a canceled context must fail")
	}
	if _, err := s.NextFencingToken(canceled); err == nil {
		t.Fatal("fencing token with a canceled context must fail")
	}
	tx := begin(t, s, pg.TxOptions{})
	if err := tx.OutboxAppend(canceled, env("t", "k"), []byte(`{}`)); err == nil {
		t.Fatal("append with a canceled context must fail")
	}
	_ = tx.Rollback(context.Background())
}
