package pg_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/authz"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

type idemCmd struct {
	mediator.Command[thingResult]
	Name string `json:"name"`
	Key  string `json:"key"`
}

func (c idemCmd) IdempotencyKey() string { return c.Key }

type plainCmd struct {
	mediator.Command[mediator.Void]
	Name string `json:"name"`
}

type badCmd struct {
	mediator.Command[mediator.Void]
	C chan int `json:"c"`
}

func (badCmd) IdempotencyKey() string { return "bad" }

// idemHarness wires UnitOfWork and Idempotency over a memstore with a
// counting handler for idemCmd and plainCmd.
type idemHarness struct {
	store    *memstore.Store
	m        *mediator.Mediator
	calls    int
	outcomes []string
	block    chan struct{} // when set, the idemCmd handler waits on it
	entered  chan struct{}
	fail     error
}

func newIdemHarness(t *testing.T, uowCfg pg.UnitOfWorkConfig) *idemHarness {
	h := &idemHarness{store: memstore.New(memstore.Config{})}
	var mu sync.Mutex
	idem := pg.Idempotency(pg.IdempotencyConfig{Observer: func(name, outcome string) {
		mu.Lock()
		h.outcomes = append(h.outcomes, name+":"+outcome)
		mu.Unlock()
	}})
	h.m = build(t, func(m *mediator.Mediator) {
		must(t, mediator.HandleFunc(m, func(ctx context.Context, c idemCmd) (thingResult, error) {
			mu.Lock()
			h.calls++
			mu.Unlock()
			if h.entered != nil {
				h.entered <- struct{}{}
			}
			if h.block != nil {
				<-h.block
			}
			if h.fail != nil {
				return thingResult{}, h.fail
			}
			return thingResult{ID: "id-" + c.Name}, nil
		}))
		must(t, mediator.HandleFunc(m, func(ctx context.Context, c plainCmd) (mediator.Void, error) {
			mu.Lock()
			h.calls++
			mu.Unlock()
			return mediator.Void{}, nil
		}))
		must(t, mediator.HandleFunc(m, func(ctx context.Context, c badCmd) (mediator.Void, error) { return mediator.Void{}, nil }))
		must(t, mediator.HandleFunc(m, func(ctx context.Context, q getThing) (thingResult, error) { return thingResult{ID: q.ID}, nil }))
	}, pg.UnitOfWork(h.store, uowCfg), idem)
	return h
}

func (h *idemHarness) row(scope, key string) (memstore.IdemRow, bool) {
	r, ok := h.store.Idempotency()[memstore.IdemKey{Scope: scope, Key: key}]
	return r, ok
}

func TestIdempotency_Outcomes(t *testing.T) {
	ctx := context.Background()

	t.Run("no key passes through", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		if _, err := mediator.Send(ctx, h.m, plainCmd{Name: "a"}); err != nil {
			t.Fatal(err)
		}
		if h.calls != 1 || len(h.store.Idempotency()) != 0 || len(h.outcomes) != 0 {
			t.Fatalf("calls=%d rows=%d outcomes=%v", h.calls, len(h.store.Idempotency()), h.outcomes)
		}
	})

	t.Run("executed then replayed", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		first, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"})
		if err != nil {
			t.Fatal(err)
		}
		row, ok := h.row("idemCmd", "k")
		if !ok || string(row.Response) != `{"id":"id-a"}` || row.Hits != 0 {
			t.Fatalf("row after execution: %+v ok=%v", row, ok)
		}
		second, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"})
		if err != nil {
			t.Fatal(err)
		}
		if first != second || h.calls != 1 {
			t.Fatalf("replay must return the stored response without running the handler: %+v %+v calls=%d", first, second, h.calls)
		}
		row, _ = h.row("idemCmd", "k")
		if row.Hits != 1 || h.store.Committed() != 2 {
			t.Fatalf("replay must commit the hits increment: hits=%d committed=%d", row.Hits, h.store.Committed())
		}
		if strings.Join(h.outcomes, ",") != "idemCmd:executed,idemCmd:replayed" {
			t.Fatalf("outcomes %v", h.outcomes)
		}
	})

	t.Run("mismatch", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		if _, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"}); err != nil {
			t.Fatal(err)
		}
		_, err := mediator.Send(ctx, h.m, idemCmd{Name: "b", Key: "k"})
		if mediator.CodeOf(err) != mediator.CodeIdempotencyMismatch {
			t.Fatalf("want mismatch, got %v", err)
		}
		if h.calls != 1 || h.store.RolledBack() != 1 || h.outcomes[1] != "idemCmd:mismatch" {
			t.Fatalf("calls=%d rb=%d outcomes=%v", h.calls, h.store.RolledBack(), h.outcomes)
		}
		if row, _ := h.row("idemCmd", "k"); row.Hits != 0 {
			t.Fatal("the mismatching attempt must roll back its hits increment")
		}
	})

	t.Run("key from context and trait precedence", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		if _, err := mediator.Send(mediator.WithIdempotencyKey(ctx, "ctx"), h.m, plainCmd{Name: "a"}); err != nil {
			t.Fatal(err)
		}
		if _, ok := h.row("plainCmd", "ctx"); !ok {
			t.Fatal("context key not used")
		}
		if _, err := mediator.Send(mediator.WithIdempotencyKey(ctx, "ctx"), h.m, idemCmd{Name: "a", Key: "trait"}); err != nil {
			t.Fatal(err)
		}
		if _, ok := h.row("idemCmd", "trait"); !ok {
			t.Fatal("trait key must win over the context key")
		}
		if _, ok := h.row("idemCmd", "ctx"); ok {
			t.Fatal("context key must not be used when the trait supplies one")
		}
		// Void responses round-trip.
		if _, err := mediator.Send(mediator.WithIdempotencyKey(ctx, "ctx"), h.m, plainCmd{Name: "a"}); err != nil {
			t.Fatal(err)
		}
		if h.calls != 2 {
			t.Fatalf("void replay ran the handler: calls=%d", h.calls)
		}
	})

	t.Run("tenant scope", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		pctx := authz.WithPrincipal(ctx, authz.Principal{Subject: "u", Tenant: "acme"})
		if _, err := mediator.Send(pctx, h.m, idemCmd{Name: "a", Key: "k"}); err != nil {
			t.Fatal(err)
		}
		if _, ok := h.row("acme:idemCmd", "k"); !ok {
			t.Fatalf("rows: %v", h.store.Idempotency())
		}
		// Another tenant with the same key executes independently.
		pctx = authz.WithPrincipal(ctx, authz.Principal{Subject: "u", Tenant: "other"})
		if _, err := mediator.Send(pctx, h.m, idemCmd{Name: "a", Key: "k"}); err != nil {
			t.Fatal(err)
		}
		if h.calls != 2 {
			t.Fatal("tenants must not share reservations")
		}
	})

	t.Run("key too long", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		_, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: strings.Repeat("k", 201)})
		if mediator.CodeOf(err) != mediator.CodeValidation || h.calls != 0 {
			t.Fatalf("want validation error, got %v (calls=%d)", err, h.calls)
		}
		if _, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: strings.Repeat("k", 200)}); err != nil {
			t.Fatalf("200 characters must be accepted: %v", err)
		}
	})

	t.Run("handler error releases the reservation", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		h.fail = errors.New("boom")
		if _, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"}); err == nil {
			t.Fatal("want error")
		}
		if _, ok := h.row("idemCmd", "k"); ok || h.store.RolledBack() != 1 {
			t.Fatal("failed execution must not leave a row")
		}
		h.fail = nil
		if _, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"}); err != nil || h.calls != 2 {
			t.Fatalf("retry must execute: err=%v calls=%d", err, h.calls)
		}
		if len(h.outcomes) != 1 || h.outcomes[0] != "idemCmd:executed" {
			t.Fatalf("only successful executions are observed: %v", h.outcomes)
		}
	})

	t.Run("busy while another attempt holds the reservation", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{DefaultLockTimeout: 50 * time.Millisecond})
		h.block = make(chan struct{})
		h.entered = make(chan struct{}, 1)
		done := make(chan error, 1)
		go func() {
			_, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"})
			done <- err
		}()
		<-h.entered
		_, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"})
		var me *mediator.Error
		if !errors.As(err, &me) || me.Code != mediator.CodeIdempotencyBusy || me.Details["retry_after_ms"] != int64(1000) {
			t.Fatalf("want busy with retry_after_ms, got %v", err)
		}
		if !pg.IsLockTimeout(err) {
			t.Fatal("the cause must remain a lock timeout")
		}
		close(h.block)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if h.calls != 1 || strings.Join(h.outcomes, ",") != "idemCmd:busy,idemCmd:executed" {
			t.Fatalf("calls=%d outcomes=%v", h.calls, h.outcomes)
		}
		// Once the first attempt committed, the same key replays.
		if _, err := mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"}); err != nil || h.calls != 1 {
			t.Fatalf("err=%v calls=%d", err, h.calls)
		}
	})

	t.Run("committed reservation without a response", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		sum, _ := mediator.CanonicalHash(idemCmd{Name: "a", Key: "k"})
		err := pg.WithTx(ctx, h.store, pg.TxOptions{}, func(ctx context.Context) error {
			tx, _ := pg.StoreTxFrom(ctx)
			_, err := tx.IdempotencyReserve(ctx, "idemCmd", "k", sum[:], time.Hour)
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"})
		if mediator.CodeOf(err) != mediator.CodeInternal || h.calls != 0 {
			t.Fatalf("want internal error without execution, got %v calls=%d", err, h.calls)
		}
	})

	t.Run("stored response that does not decode", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		sum, _ := mediator.CanonicalHash(idemCmd{Name: "a", Key: "k"})
		err := pg.WithTx(ctx, h.store, pg.TxOptions{}, func(ctx context.Context) error {
			tx, _ := pg.StoreTxFrom(ctx)
			if _, err := tx.IdempotencyReserve(ctx, "idemCmd", "k", sum[:], time.Hour); err != nil {
				return err
			}
			return tx.IdempotencyStore(ctx, "idemCmd", "k", []byte(`{"id":`))
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = mediator.Send(ctx, h.m, idemCmd{Name: "a", Key: "k"})
		if mediator.CodeOf(err) != mediator.CodeInternal || !strings.Contains(err.Error(), "decode") {
			t.Fatalf("want decode error, got %v", err)
		}
	})

	t.Run("request that cannot be hashed", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		_, err := mediator.Send(ctx, h.m, badCmd{C: make(chan int)})
		if mediator.CodeOf(err) != mediator.CodeInternal || !strings.Contains(err.Error(), "hash") {
			t.Fatalf("want hash error, got %v", err)
		}
	})

	t.Run("queries pass through", func(t *testing.T) {
		h := newIdemHarness(t, pg.UnitOfWorkConfig{})
		if _, err := mediator.Send(mediator.WithIdempotencyKey(ctx, "q"), h.m, getThing{ID: "x"}); err != nil {
			t.Fatal(err)
		}
		if len(h.store.Idempotency()) != 0 {
			t.Fatal("queries must not reserve keys")
		}
	})
}

func TestIdempotency_RequiresUnitOfWork(t *testing.T) {
	store := memstore.New(memstore.Config{})
	m := build(t, func(m *mediator.Mediator) {
		must(t, mediator.HandleFunc(m, func(ctx context.Context, c idemCmd) (thingResult, error) { return thingResult{}, nil }))
	}, pg.Idempotency(pg.IdempotencyConfig{}))
	_, err := mediator.Send(context.Background(), m, idemCmd{Key: "k"})
	if mediator.CodeOf(err) != mediator.CodeInternal || !errors.Is(err, pg.ErrNoUnitOfWork) {
		t.Fatalf("want ErrNoUnitOfWork with CodeInternal, got %v", err)
	}
	if store.Begun() != 0 {
		t.Fatal("store untouched")
	}
	if pg.Idempotency(pg.IdempotencyConfig{}).Name() != mediator.NameIdempotency {
		t.Fatal("name")
	}
}

func TestIdempotencyConfigDefaults(t *testing.T) {
	c := pg.IdempotencyConfig{}.WithDefaults()
	if c.TTL != 24*time.Hour || c.RetryAfter != time.Second || c.Logger == nil {
		t.Fatalf("%+v", c)
	}
	c = pg.IdempotencyConfig{TTL: time.Hour, RetryAfter: time.Minute}.WithDefaults()
	if c.TTL != time.Hour || c.RetryAfter != time.Minute {
		t.Fatalf("%+v", c)
	}
}
