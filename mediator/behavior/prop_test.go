package behavior_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"pgregory.net/rapid"

	"github.com/t3stackcoder/go-api-backend/mediator/behavior/cachemodel"
)

// warm runs one reader to completion so the backend holds an entry before
// the interleaved run starts; hits are then possible from the first step.
func warm(backend cachemodel.Backend, cfg cachemodel.Config) error {
	cfg.Readers, cfg.Writers = 1, 0
	return cachemodel.New(context.Background(), backend, cfg).Run(func(r []int) int { return r[0] })
}

// TestPropCacheProtocol is PropCacheProtocol (11.3): for any interleaving
// of reader steps (get, snapshot, read db, set) and writer steps (begin,
// pre-bump, commit, post-bump, return) chosen by rapid, a read served from
// the cache never returns a value older than the last committed write
// whose Send returned before the read was invoked (G9).
func TestPropCacheProtocol(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		backend := cachemodel.NewMemory()
		cfg := cachemodel.Config{
			Readers: rapid.IntRange(1, 4).Draw(rt, "readers"),
			Writers: rapid.IntRange(1, 3).Draw(rt, "writers"),
			Key:     "q",
			Tags:    rapid.SliceOfNDistinct(rapid.StringMatching(`[a-c]`), 1, 3, rapid.ID[string]).Draw(rt, "tags"),
		}
		if rapid.Bool().Draw(rt, "warm") {
			if err := warm(backend, cfg); err != nil {
				rt.Fatal(err)
			}
		}
		s := cachemodel.New(context.Background(), backend, cfg)
		if err := s.Run(func(r []int) int { return rapid.SampledFrom(r).Draw(rt, "actor") }); err != nil {
			rt.Fatal(err)
		}
		if len(s.Degraded()) != 0 {
			rt.Fatalf("unexpected backend errors: %v", s.Degraded())
		}
	})
}

// TestPropCacheProtocol_ModelHasTeeth: without the post-commit bump the
// model finds an interleaving that serves a stale value after the writer
// returned, so the property is not vacuous. (Without the pre-commit bump
// alone, versioned reads still hold the bound; that bump narrows the stale
// window before Send returns and covers a crash before the post-commit
// bump, which the model does not simulate.)
func TestPropCacheProtocol_ModelHasTeeth(t *testing.T) {
	found := false
	for seed := uint64(0); seed < 2000 && !found; seed++ {
		rng := rand.New(rand.NewPCG(seed, 1))
		s := cachemodel.New(context.Background(), cachemodel.NewMemory(), cachemodel.Config{
			Readers: 2, Writers: 1, Key: "q", Tags: []string{"a"}, SkipPostBump: true,
		})
		err := s.Run(func(r []int) int { return r[rng.IntN(len(r))] })
		if err != nil {
			v, ok := err.(*cachemodel.Violation)
			if !ok || s.Violation() != v || v.Got >= v.Bound {
				t.Fatalf("bad violation %v", err)
			}
			found = true
		}
	}
	if !found {
		t.Fatal("the model did not detect the missing post-commit bump")
	}
}
