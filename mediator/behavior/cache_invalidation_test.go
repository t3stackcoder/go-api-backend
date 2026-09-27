package behavior_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior"
	"github.com/t3stackcoder/go-api-backend/mediator/behavior/cachemodel"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/memstore"
)

func TestCacheInvalidation_BumpsAroundCommit(t *testing.T) {
	h := newHarness(t)
	ctx := admin(context.Background())
	var atCommit, afterHandler int64
	h.hooks.rich = func(ctx context.Context, c richCmd) (cmdResult, error) {
		afterHandler = h.backend.Version("things")
		pg.BeforeCommit(ctx, func(context.Context) error {
			atCommit = h.backend.Version("things")
			return nil
		})
		return cmdResult{ID: c.ID}, nil
	}
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Tags: []string{"things", "other"}}); err != nil {
		t.Fatal(err)
	}
	if afterHandler != 0 || atCommit != 1 || h.backend.Version("things") != 2 || h.backend.Version("other") != 2 {
		t.Fatalf("versions: in handler %d, at commit %d, after Send %d", afterHandler, atCommit, h.backend.Version("things"))
	}
	if h.backend.Ops(cachemodel.OpBumpPre) != 1 || h.backend.Ops(cachemodel.OpBumpPost) != 1 {
		t.Fatalf("pre %d post %d", h.backend.Ops(cachemodel.OpBumpPre), h.backend.Ops(cachemodel.OpBumpPost))
	}

	// A cached query is invalidated by the command.
	h.backend.Reset()
	if _, err := mediator.Send(ctx, h.m, cachedQuery{ID: "q"}); err != nil {
		t.Fatal(err)
	}
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Tags: []string{"things"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := mediator.Send(ctx, h.m, cachedQuery{ID: "q"}); err != nil {
		t.Fatal(err)
	}
	if h.backend.Ops(cachemodel.OpSet) != 2 {
		t.Fatalf("the second read must miss and store again: sets %d", h.backend.Ops(cachemodel.OpSet))
	}
}

func TestCacheInvalidation_NoBumpOnFailureOrNoTags(t *testing.T) {
	h := newHarness(t)
	ctx := admin(context.Background())
	h.hooks.rich = func(context.Context, richCmd) (cmdResult, error) {
		return cmdResult{}, mediator.E(mediator.CodeConflict, "no")
	}
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Tags: []string{"things"}}); err == nil {
		t.Fatal("want error")
	}
	h.hooks.rich = defaultHooks().rich
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a"}); err != nil {
		t.Fatal(err)
	}
	if h.backend.Ops(cachemodel.OpBumpPre)+h.backend.Ops(cachemodel.OpBumpPost) != 0 {
		t.Fatal("no bump expected")
	}

	// A replayed idempotent command never reaches the behavior.
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Key: "k", Tags: []string{"things"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := mediator.Send(ctx, h.m, richCmd{ID: "a", Key: "k", Tags: []string{"things"}}); err != nil {
		t.Fatal(err)
	}
	if h.backend.Version("things") != 2 {
		t.Fatalf("replay must not bump: version %d", h.backend.Version("things"))
	}
}

// TestCacheInvalidation_WithoutUnitOfWork: without a store the post-commit
// hook runs immediately.
func TestCacheInvalidation_WithoutUnitOfWork(t *testing.T) {
	h := newHarness(t, withoutStore())
	if _, err := mediator.Send(admin(context.Background()), h.m, richCmd{ID: "a", Tags: []string{"t"}}); err != nil {
		t.Fatal(err)
	}
	if h.backend.Version("t") != 2 {
		t.Fatalf("version %d", h.backend.Version("t"))
	}
}

// bubbleClock has only Now, so the behavior sleeps on time.After, which is
// virtual inside a synctest bubble.
type bubbleClock struct{}

func (bubbleClock) Now() time.Time { return time.Now() }

// invalidationBubble builds a standard set over memstore inside a synctest
// bubble with a backend whose bumps fail while failing is set. With
// bubbleClock the retries sleep on virtual time; with a testkit.FakeClock
// they sleep on its timers and run when Advance fires them.
func invalidationBubble(t *testing.T, ttl time.Duration, clock mediator.Clock) (*mediator.Mediator, *cachemodel.Memory, *logSink, *atomic.Bool, io.Closer) {
	t.Helper()
	backend := cachemodel.NewMemory()
	failing := new(atomic.Bool)
	backend.Fail = func(op string) error {
		if failing.Load() && strings.HasPrefix(op, "bump.") {
			return errors.New("redis down")
		}
		return nil
	}
	sink := newLogSink(slog.LevelDebug)
	m := mediator.New()
	register(t, m, defaultHooks())
	entries := behavior.Standard(behavior.Config{
		Logger: slog.New(sink), Clock: clock, Store: memstore.New(memstore.Config{}), Cache: backend, CacheTTL: ttl,
	})
	var closer io.Closer
	for _, e := range entries {
		must(t, mediator.Use(m, e.Behavior, e.Options...))
		if e.Behavior.Name() == behavior.CacheInvalidation {
			closer = e.Behavior.(io.Closer)
		}
	}
	must(t, m.Build())
	return m, backend, sink, failing, closer
}

func TestCacheInvalidation_RetryRecovers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, backend, sink, failing, closer := invalidationBubble(t, time.Minute, bubbleClock{})
		defer closer.Close()
		failing.Store(true)
		if _, err := mediator.Send(admin(context.Background()), m, richCmd{ID: "a", Tags: []string{"t1", "t2"}}); err != nil {
			t.Fatalf("a failed bump never fails the command: %v", err)
		}
		if backend.Version("t1") != 0 {
			t.Fatal("bumps must have failed")
		}
		warns := sink.find("cache invalidation failed; retrying with backoff")
		if len(warns) != 1 || warns[0].Level != slog.LevelWarn {
			t.Fatalf("one rate-limited warning per tag set, got %v", warns)
		}
		// First retry after 100 ms fails too (still down), second at +200 ms succeeds.
		time.Sleep(150 * time.Millisecond)
		synctest.Wait()
		if backend.Version("t1") != 0 {
			t.Fatal("retry must not have succeeded while down")
		}
		failing.Store(false)
		time.Sleep(250 * time.Millisecond)
		synctest.Wait()
		if backend.Version("t1") != 2 || backend.Version("t2") != 2 {
			t.Fatalf("both bumps must recover: t1=%d t2=%d", backend.Version("t1"), backend.Version("t2"))
		}
		recovered := sink.find("cache invalidation recovered")
		if len(recovered) != 2 {
			t.Fatalf("recovered records %v", recovered)
		}
		phases := map[any]bool{}
		for _, r := range recovered {
			phases[r.Attrs["phase"]] = true
			// The initial bump was attempt 1, the failed retry 2, the recovery 3.
			if r.Attrs["attempts"] != int64(3) {
				t.Fatalf("recovery must report attempt 3: %v", r)
			}
		}
		if !phases["pre-commit"] || !phases["post-commit"] {
			t.Fatalf("phases %v", phases)
		}
	})
}

func TestCacheInvalidation_GivesUpAfterTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, backend, sink, failing, closer := invalidationBubble(t, 3*time.Second, bubbleClock{})
		defer closer.Close()
		failing.Store(true)
		if _, err := mediator.Send(admin(context.Background()), m, richCmd{ID: "a", Tags: []string{"t"}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if backend.Version("t") != 0 {
			t.Fatal("no bump must have succeeded")
		}
		if abandoned := sink.find("cache invalidation abandoned; entries expire with the TTL"); len(abandoned) != 2 {
			t.Fatalf("abandoned records %v", abandoned)
		}
		attempts := backend.Ops(cachemodel.OpBumpPre)
		if attempts < 3 {
			t.Fatalf("expected several attempts within the window, got %d", attempts)
		}
		failing.Store(false)
		time.Sleep(time.Minute)
		synctest.Wait()
		if backend.Ops(cachemodel.OpBumpPre) != attempts {
			t.Fatal("retries must stop after the window")
		}
	})
}

func TestCacheInvalidation_Close(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m, backend, _, failing, closer := invalidationBubble(t, time.Hour, bubbleClock{})
		failing.Store(true)
		if _, err := mediator.Send(admin(context.Background()), m, richCmd{ID: "a", Tags: []string{"t"}}); err != nil {
			t.Fatal(err)
		}
		attempts := backend.Ops(cachemodel.OpBumpPre)
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
		failing.Store(false)
		time.Sleep(time.Hour)
		synctest.Wait()
		if backend.Ops(cachemodel.OpBumpPre) != attempts || backend.Version("t") != 0 {
			t.Fatal("no retry may run after Close")
		}
		// A failure after Close schedules nothing.
		failing.Store(true)
		if _, err := mediator.Send(admin(context.Background()), m, richCmd{ID: "b", Tags: []string{"t"}}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Hour)
		synctest.Wait()
		if backend.Ops(cachemodel.OpBumpPre) != attempts+1 {
			t.Fatalf("attempts %d", backend.Ops(cachemodel.OpBumpPre))
		}
	})
}

// TestCacheInvalidation_RetryAttemptsAreNumbered: the initial bump is
// attempt 1; each retry is numbered from there in the warning it logs (once
// the log window lets it through) and in the recovery record. A fake clock
// drives the backoff timers so a retry runs exactly when Advance fires it.
func TestCacheInvalidation_RetryAttemptsAreNumbered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const warn = "cache invalidation failed; retrying with backoff"
		clock := testkit.NewFakeClock(time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC))
		m, backend, sink, failing, closer := invalidationBubble(t, time.Hour, clock)
		defer closer.Close()
		failing.Store(true)
		if _, err := mediator.Send(admin(context.Background()), m, richCmd{ID: "a", Tags: []string{"t"}}); err != nil {
			t.Fatal(err)
		}
		warns := sink.find(warn)
		if len(warns) != 1 || warns[0].Attrs["attempt"] != int64(1) {
			t.Fatalf("initial warning %v", warns)
		}
		// Let both retry goroutines (pre and post phases) park on their
		// 100 ms timers. Firing them a minute later also puts the failure
		// past the log window, so it is logged, as attempt 2, once for the
		// tag set.
		synctest.Wait()
		clock.Advance(time.Minute)
		synctest.Wait()
		if backend.Version("t") != 0 {
			t.Fatal("the retry must have failed while down")
		}
		if warns = sink.find(warn); len(warns) != 2 || warns[1].Attrs["attempt"] != int64(2) {
			t.Fatalf("retry warning %v", warns)
		}
		// The second retry (200 ms of backoff) succeeds: attempt 3.
		failing.Store(false)
		clock.Advance(time.Second)
		synctest.Wait()
		if backend.Version("t") != 2 {
			t.Fatalf("both phases must recover: version %d", backend.Version("t"))
		}
		recovered := sink.find("cache invalidation recovered")
		if len(recovered) != 2 {
			t.Fatalf("recovered records %v", recovered)
		}
		for _, r := range recovered {
			if r.Attrs["attempts"] != int64(3) {
				t.Fatalf("recovery must report attempt 3: %v", r)
			}
		}
	})
}

func TestCacheInvalidation_FailedCounterAndDirect(t *testing.T) {
	h := newHarness(t)
	h.backend.Fail = func(op string) error {
		if op == cachemodel.OpBumpPost {
			return errors.New("down")
		}
		return nil
	}
	if _, err := mediator.Send(admin(context.Background()), h.m, richCmd{ID: "a", Tags: []string{"t1", "t2"}}); err != nil {
		t.Fatal(err)
	}
	rm := h.tel.collect(t)
	for _, tag := range []string{"t1", "t2"} {
		if p := point(t, rm, "mediator.cache.invalidation_failed", map[string]string{"tag": tag}); p.Int < 1 {
			t.Fatalf("%s: %d", tag, p.Int)
		}
	}
	warns := h.logs.find("cache invalidation failed; retrying with backoff")
	if len(warns) < 1 || warns[0].Attrs["phase"] != "post-commit" {
		t.Fatalf("warnings %v", warns)
	}

	b := behavior.NewCacheInvalidation(behavior.Config{})
	if err := b.(mediator.Preparer).Prepare(nil); err == nil {
		t.Fatal("Prepare without a backend must fail")
	}
	res, err := b.Handle(context.Background(), plainCmd{}, &mediator.RequestInfo{}, func(context.Context, any) (any, error) { return 1, nil })
	if err != nil || res != 1 || b.Name() != behavior.CacheInvalidation {
		t.Fatal(res, err)
	}
	must(t, b.(io.Closer).Close())
}
