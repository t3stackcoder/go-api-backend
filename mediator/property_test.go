package mediator_test

import (
	"context"
	"crypto/sha256"
	"encoding/json/v2"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"pgregory.net/rapid"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// TestPropPipeline_Composition: for any list of pure recording behaviors with
// satisfiable Before/After constraints, the observed call order equals the
// resolved order, every constraint holds, and inserting an identity behavior
// anywhere changes neither the handler's result nor the order of the others.
func TestPropPipeline_Composition(t *testing.T) {
	type spec struct {
		name   string
		after  string
		before string
	}
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 8).Draw(rt, "n")
		specs := make([]spec, n)
		for i := range specs {
			s := spec{name: fmt.Sprintf("b%d", i)}
			if i > 0 {
				// One constraint per behavior, always to an earlier one: the
				// constraint graph is a backward forest, so it is satisfiable.
				switch rapid.IntRange(0, 2).Draw(rt, fmt.Sprintf("kind%d", i)) {
				case 1:
					s.after = fmt.Sprintf("b%d", rapid.IntRange(0, i-1).Draw(rt, fmt.Sprintf("after%d", i)))
				case 2:
					s.before = fmt.Sprintf("b%d", rapid.IntRange(0, i-1).Draw(rt, fmt.Sprintf("before%d", i)))
				}
			}
			specs[i] = s
		}
		identityAt := rapid.IntRange(0, n).Draw(rt, "identityAt")
		identity := spec{name: "identity"}
		if n > 0 {
			switch rapid.IntRange(0, 2).Draw(rt, "identityKind") {
			case 1:
				identity.after = fmt.Sprintf("b%d", rapid.IntRange(0, n-1).Draw(rt, "identityAfter"))
			case 2:
				identity.before = fmt.Sprintf("b%d", rapid.IntRange(0, n-1).Draw(rt, "identityBefore"))
			}
		}
		opts := func(s spec) []mediator.UseOption {
			var o []mediator.UseOption
			if s.after != "" {
				o = append(o, mediator.After(s.after))
			}
			if s.before != "" {
				o = append(o, mediator.Before(s.before))
			}
			return o
		}
		run := func(withIdentity bool) (chain, observed []string, res int, err error) {
			m := mediator.New()
			var log []string
			if err := mediator.HandleFunc(m, func(_ context.Context, c pCmd) (int, error) { return c.N * 2, nil }); err != nil {
				rt.Fatal(err)
			}
			for i, s := range specs {
				if withIdentity && i == identityAt {
					if err := mediator.Use(m, rec{name: identity.name, log: &log}, opts(identity)...); err != nil {
						rt.Fatal(err)
					}
				}
				if err := mediator.Use(m, rec{name: s.name, log: &log}, opts(s)...); err != nil {
					rt.Fatal(err)
				}
			}
			if withIdentity && identityAt == n {
				if err := mediator.Use(m, rec{name: identity.name, log: &log}, opts(identity)...); err != nil {
					rt.Fatal(err)
				}
			}
			if err := m.Build(); err != nil {
				rt.Fatalf("build: %v (specs %+v identity %+v at %d)", err, specs, identity, identityAt)
			}
			chain = m.ChainFor(reflect.TypeFor[pCmd]())
			res, err = mediator.Send(context.Background(), m, pCmd{N: 21})
			return chain, forward(log), res, err
		}
		chain, observed, res, err := run(false)
		if err != nil || res != 42 {
			rt.Fatalf("send: %d %v", res, err)
		}
		if !reflect.DeepEqual(observed, chain) {
			rt.Fatalf("observed %v != resolved %v", observed, chain)
		}
		pos := map[string]int{}
		for i, name := range chain {
			pos[name] = i
		}
		if len(pos) != n {
			rt.Fatalf("chain %v lacks behaviors", chain)
		}
		for _, s := range specs {
			if s.after != "" && pos[s.name] < pos[s.after] {
				rt.Fatalf("%s must be after %s in %v", s.name, s.after, chain)
			}
			if s.before != "" && pos[s.name] > pos[s.before] {
				rt.Fatalf("%s must be before %s in %v", s.name, s.before, chain)
			}
		}
		chain2, observed2, res2, err2 := run(true)
		if err2 != nil || res2 != res {
			rt.Fatalf("identity changed the result: %d %v", res2, err2)
		}
		if !reflect.DeepEqual(observed2, chain2) {
			rt.Fatalf("observed %v != resolved %v", observed2, chain2)
		}
		var without []string
		found := false
		for _, name := range chain2 {
			if name == "identity" {
				found = true
				continue
			}
			without = append(without, name)
		}
		if !found {
			rt.Fatalf("identity missing from %v", chain2)
		}
		pos2 := map[string]int{}
		for i, name := range chain2 {
			pos2[name] = i
		}
		for _, s := range append(specs, identity) {
			if s.after != "" && pos2[s.name] < pos2[s.after] {
				rt.Fatalf("%s must be after %s in %v", s.name, s.after, chain2)
			}
			if s.before != "" && pos2[s.name] > pos2[s.before] {
				rt.Fatalf("%s must be before %s in %v", s.name, s.before, chain2)
			}
		}
		// An unconstrained identity leaves the order of the others alone. A
		// constrained one is a sibling of its anchor and may legitimately
		// change where later siblings land.
		if identity.after == "" && identity.before == "" && len(chain) > 0 && !reflect.DeepEqual(without, chain) {
			rt.Fatalf("identity changed the order: %v vs %v", chain2, chain)
		}
	})
}

// TestPropPipeline_HandlerAtMostOnce: for any behavior list without retry,
// where behaviors, processors, and handlers randomly error or panic, the
// handler runs at most once per Send and no panic escapes.
func TestPropPipeline_HandlerAtMostOnce(t *testing.T) {
	modes := rapid.SampledFrom([]string{"", "", "", "error", "panic", "nil", "wrong"})
	simple := rapid.SampledFrom([]string{"", "", "error", "panic"})
	fail := func(mode, where string) error {
		switch mode {
		case "error":
			return fmt.Errorf("error in %s", where)
		case "panic":
			panic("panic in " + where)
		}
		return nil
	}
	rapid.Check(t, func(rt *rapid.T) {
		n := rapid.IntRange(0, 6).Draw(rt, "n")
		bmodes := rapid.SliceOfN(modes, n, n).Draw(rt, "behaviors")
		typedMode := simple.Draw(rt, "typed")
		preMode := simple.Draw(rt, "pre")
		postMode := simple.Draw(rt, "post")
		errhMode := simple.Draw(rt, "errh")
		handlerMode := simple.Draw(rt, "handler")

		m := mediator.New()
		calls := 0
		var log []string
		must := func(err error) {
			if err != nil {
				rt.Fatal(err)
			}
		}
		must(mediator.HandleFunc(m, func(_ context.Context, c pCmd) (int, error) {
			calls++
			return c.N * 2, fail(handlerMode, "handler")
		}))
		for i, mode := range bmodes {
			must(mediator.Use(m, rec{name: fmt.Sprintf("b%d", i), log: &log, mode: mode}))
		}
		must(mediator.UseFor(m, mediator.TypedBehaviorFunc[pCmd, int](func(ctx context.Context, c pCmd, next func(context.Context, pCmd) (int, error)) (int, error) {
			if err := fail(typedMode, "typed"); err != nil {
				return 0, err
			}
			return next(ctx, c)
		})))
		must(mediator.Pre(m, mediator.PreProcessorFunc[pCmd, int](func(context.Context, pCmd) error { return fail(preMode, "pre") })))
		must(mediator.Post(m, mediator.PostProcessorFunc[pCmd, int](func(context.Context, pCmd, int) error { return fail(postMode, "post") })))
		must(mediator.OnError(m, mediator.ErrorHandlerFunc[pCmd, int](func(_ context.Context, _ pCmd, err error) (int, bool, error) {
			if e := fail(errhMode, "errh"); e != nil {
				return 0, false, e
			}
			return 0, false, nil
		})))
		must(m.Build())

		var res int
		var err error
		func() {
			defer func() {
				if v := recover(); v != nil {
					rt.Fatalf("panic escaped Send: %v", v)
				}
			}()
			res, err = mediator.Send(context.Background(), m, pCmd{N: 21})
		}()
		if calls > 1 {
			rt.Fatalf("handler ran %d times", calls)
		}
		// The first element that does not call next decides the outcome: a
		// "nil" short-circuit is a successful zero result, anything else an
		// error. Otherwise the handler runs once and its own or the
		// post-processor's failure decides.
		first := ""
		for _, mode := range bmodes {
			if mode != "" {
				first = mode
				break
			}
		}
		wantCalls, wantErr, wantRes := 1, handlerMode != "" || postMode != "", 42
		switch {
		case first == "nil":
			wantCalls, wantErr, wantRes = 0, false, 0
		case first != "", typedMode != "", preMode != "":
			wantCalls, wantErr = 0, true
		}
		if calls != wantCalls {
			rt.Fatalf("handler ran %d times, want %d (behaviors %v typed %q pre %q)", calls, wantCalls, bmodes, typedMode, preMode)
		}
		if (err != nil) != wantErr {
			rt.Fatalf("err = %v, want error %v", err, wantErr)
		}
		if !wantErr && res != wantRes {
			rt.Fatalf("res = %d, want %d", res, wantRes)
		}
	})
}

// ---- canonical JSON --------------------------------------------------------

func drawJSON(rt *rapid.T, depth int, label string) any {
	limit := 6
	if depth == 0 {
		limit = 4
	}
	switch rapid.IntRange(0, limit).Draw(rt, label+".k") {
	case 0:
		return nil
	case 1:
		return rapid.Bool().Draw(rt, label+".b")
	case 2:
		return rapid.Int64().Draw(rt, label+".i")
	case 3:
		return rapid.Float64().Draw(rt, label+".f")
	case 4:
		return rapid.String().Draw(rt, label+".s")
	case 5:
		n := rapid.IntRange(0, 4).Draw(rt, label+".n")
		out := make([]any, n)
		for i := range out {
			out[i] = drawJSON(rt, depth-1, fmt.Sprintf("%s[%d]", label, i))
		}
		return out
	default:
		n := rapid.IntRange(0, 4).Draw(rt, label+".n")
		out := map[string]any{}
		for i := 0; i < n; i++ {
			k := rapid.String().Draw(rt, fmt.Sprintf("%s.key%d", label, i))
			out[k] = drawJSON(rt, depth-1, fmt.Sprintf("%s.%q", label, k))
		}
		return out
	}
}

// spell writes v as JSON text with a random member order, random whitespace,
// random string escaping, and random (value-preserving) number spellings.
func spell(rt *rapid.T, v any, label string) string {
	ws := func() string { return rapid.SampledFrom([]string{"", " ", "\n\t"}).Draw(rt, label+".ws") }
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(x)
	case int64:
		if x > -1<<53 && x < 1<<53 {
			switch rapid.IntRange(0, 4).Draw(rt, label+".ispell") {
			case 1:
				return fmt.Sprintf("%d.0", x)
			case 2:
				return fmt.Sprintf("%de0", x)
			case 3:
				return fmt.Sprintf("%d.000E+0", x)
			case 4:
				return strconv.FormatFloat(float64(x), 'g', -1, 64)
			}
		}
		return strconv.FormatInt(x, 10)
	case float64:
		switch rapid.IntRange(0, 2).Draw(rt, label+".fspell") {
		case 1:
			return strconv.FormatFloat(x, 'e', -1, 64)
		case 2:
			if math.Abs(x) < 1e21 {
				return strconv.FormatFloat(x, 'f', -1, 64)
			}
		}
		return strconv.FormatFloat(x, 'g', -1, 64)
	case string:
		if !rapid.Bool().Draw(rt, label+".esc") {
			b, err := json.Marshal(x)
			if err != nil {
				rt.Fatal(err)
			}
			return string(b)
		}
		// Hand-rolled encoding that \u-escapes every ASCII character.
		var sb strings.Builder
		sb.WriteByte('"')
		for _, r := range x {
			if r < 0x80 {
				fmt.Fprintf(&sb, `\u%04X`, r)
			} else {
				sb.WriteRune(r)
			}
		}
		sb.WriteByte('"')
		return sb.String()
	case []any:
		parts := make([]string, len(x))
		for i, e := range x {
			parts[i] = spell(rt, e, fmt.Sprintf("%s[%d]", label, i))
		}
		return "[" + ws() + strings.Join(parts, ","+ws()) + ws() + "]"
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		keys = rapid.Permutation(keys).Draw(rt, label+".perm")
		parts := make([]string, len(keys))
		for i, k := range keys {
			kb, _ := json.Marshal(k)
			parts[i] = string(kb) + ws() + ":" + ws() + spell(rt, x[k], fmt.Sprintf("%s.%q", label, k))
		}
		return "{" + ws() + strings.Join(parts, ","+ws()) + ws() + "}"
	}
	rt.Fatalf("unexpected %T", v)
	return ""
}

// TestPropCanonicalJSON: hashing a value and hashing the same value with
// permuted map insertion order and re-encoded numbers gives the same digest,
// and canonicalization is idempotent.
func TestPropCanonicalJSON(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		v := drawJSON(rt, 3, "v")
		want, err := mediator.CanonicalHash(v)
		if err != nil {
			rt.Fatal(err)
		}
		canon, err := mediator.CanonicalJSON(v)
		if err != nil {
			rt.Fatal(err)
		}
		if again, err := mediator.CanonicalizeJSON(canon); err != nil || string(again) != string(canon) {
			rt.Fatalf("not idempotent: %s -> %s (%v)", canon, again, err)
		}
		for i := range 2 {
			text := spell(rt, v, fmt.Sprintf("spell%d", i))
			got, err := mediator.CanonicalizeJSON([]byte(text))
			if err != nil {
				rt.Fatalf("canonicalize %s: %v", text, err)
			}
			if sha256.Sum256(got) != want {
				rt.Fatalf("digest differs:\nvalue: %s\nspelled: %s\ngot: %s", canon, text, got)
			}
		}
	})
}

// TestPropPartition_Stable: partition assignment depends only on the key and
// P, and over 100k random keys no partition holds more than 3x the mean.
func TestPropPartition_Stable(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		key := rapid.String().Draw(rt, "key")
		p := rapid.IntRange(1, 64).Draw(rt, "p")
		a := mediator.Partition(key, p)
		if a < 0 || a >= p || a != mediator.Partition(key, p) || a != mediator.Partition(strings.Clone(key), p) {
			t.Fatalf("Partition(%q, %d) = %d", key, p, a)
		}
	})
	r := rand.New(rand.NewPCG(2026, 9))
	keys := make([]string, 100_000)
	for i := range keys {
		keys[i] = fmt.Sprintf("%016x-%d", r.Uint64(), i)
	}
	for _, p := range []int{1, 2, 16, 64} {
		counts := make([]int, p)
		for _, k := range keys {
			counts[mediator.Partition(k, p)]++
		}
		mean := float64(len(keys)) / float64(p)
		for i, c := range counts {
			if float64(c) > 3*mean {
				t.Fatalf("P=%d: partition %d holds %d, mean %.0f", p, i, c, mean)
			}
			if c == 0 {
				t.Fatalf("P=%d: partition %d is empty", p, i)
			}
		}
	}
}
