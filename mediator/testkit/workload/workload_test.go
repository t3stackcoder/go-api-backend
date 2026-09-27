package workload

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/validate"
)

// requestTypes lists every request type with a representative valid value.
var requestTypes = []any{
	SetValue{Key: "k", Val: 1, CmdID: "c1"},
	GetValue{Key: "k"},
	GetValueCached{Key: "k"},
	Transfer{From: "a", To: "b", Amt: 1, CmdID: "c2"},
	ReadAll{},
	Append{Key: "k", Val: 1, CmdID: "c3"},
	ReadList{Key: "k"},
	Bump{Key: "k"},
	AtomicScenario{CmdID: "c4", Key1: "k1", Key2: "k2"},
	Panic{},
	Slow{Millis: 1},
	Touch{Key: "k", CmdID: "c5"},
}

func TestValidationTagsCompileAndAccept(t *testing.T) {
	v := validate.New()
	for _, req := range requestTypes {
		rt := reflect.TypeOf(req)
		if err := v.Compile(rt); err != nil {
			t.Fatalf("%s: compile: %v", rt, err)
		}
		if err := v.Check(context.Background(), req); err != nil {
			t.Fatalf("%s: valid value rejected: %v", rt, err)
		}
	}
	for _, ev := range []any{Bumped{}, AtomicDone{}} {
		if err := v.Compile(reflect.TypeOf(ev)); err != nil {
			t.Fatalf("%T: compile: %v", ev, err)
		}
	}
	invalid := []any{
		SetValue{Key: "", CmdID: "c"},
		SetValue{Key: "k", CmdID: strings.Repeat("x", 201)},
		Transfer{From: "a", To: "b", Amt: 0, CmdID: "c"},
		Transfer{From: "a", To: "a", Amt: 1, CmdID: "c"},
		Append{Key: "k", CmdID: ""},
		Bump{Key: ""},
		AtomicScenario{CmdID: "c", Key1: "", Key2: "k2"},
		Slow{Millis: -1},
		Slow{Millis: 600001},
		Touch{Key: "k"},
	}
	for _, req := range invalid {
		var ve *mediator.ValidationError
		err := v.Check(context.Background(), req)
		if !errors.As(err, &ve) {
			t.Fatalf("%+v: want validation error, got %v", req, err)
		}
	}
}

func TestTraits(t *testing.T) {
	if (SetValue{CmdID: "c"}).IdempotencyKey() != "" || (SetValue{CmdID: "c", Keyed: true}).IdempotencyKey() != "c" {
		t.Fatal("SetValue key")
	}
	if (SetValue{Key: "k"}).Invalidates() != nil || (SetValue{Key: "k", Invalidate: true}).Invalidates()[0] != RegisterTag("k") {
		t.Fatal("SetValue invalidates")
	}
	if (Transfer{CmdID: "c"}).IdempotencyKey() != "" || (Transfer{CmdID: "c", Keyed: true}).IdempotencyKey() != "c" {
		t.Fatal("Transfer key")
	}
	if (Transfer{From: "a", To: "a"}).Validate(context.Background()) == nil || (Transfer{From: "a", To: "b"}).Validate(context.Background()) != nil {
		t.Fatal("Transfer validate")
	}
	if (Append{CmdID: "c"}).IdempotencyKey() != "c" || (Bump{CmdID: "c"}).IdempotencyKey() != "c" || (Bump{}).IdempotencyKey() != "" {
		t.Fatal("Append/Bump key")
	}
	if (AtomicScenario{CmdKey: "k"}).IdempotencyKey() != "k" || (AtomicScenario{}).IdempotencyKey() != "" {
		t.Fatal("AtomicScenario key")
	}
	q := GetValueCached{Key: "k"}
	if tags := q.CacheTags(); len(tags) != 1 || tags[0] != "wl:register:k" || q.CacheTTL() != 30*time.Second {
		t.Fatal("cache traits")
	}
	e := Bumped{Key: "k", N: 2}
	if e.StreamKey() != "k" || e.Topic() != TopicBumped || e.Name() != NameBumped {
		t.Fatal("Bumped traits")
	}
	if (AtomicDone{}).Name() != NameAtomicDone {
		t.Fatal("AtomicDone name")
	}
	if BankTotal() != 400 || len(Accounts) != 4 {
		t.Fatal("bank seed")
	}
	if !strings.Contains(Schema, "wl_cmd_log") || len(Tables) != 10 {
		t.Fatal("schema")
	}
}

func TestNames(t *testing.T) {
	names := Names()
	if !sort.StringsAreSorted(names) {
		t.Fatal("Names must be sorted")
	}
	seen := map[string]bool{}
	for _, n := range names {
		if seen[n] {
			t.Fatalf("duplicate name %q", n)
		}
		seen[n] = true
		if !mediator.NamePattern.MatchString(n) {
			t.Fatalf("name %q does not match %s", n, mediator.NamePattern)
		}
	}
	m := mediator.New()
	if err := Register(m, Deps{Groups: AllGroups}); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	for _, e := range m.Names() {
		if !seen[e.Name] {
			t.Fatalf("registry name %q missing from Names()", e.Name)
		}
		if !e.Pinned {
			t.Fatalf("%s must implement Named", e.Name)
		}
		if e.Kind == mediator.KindConsumer && !seen[e.Group] {
			t.Fatalf("group %q missing from Names()", e.Group)
		}
		if e.Topic != "" && !seen[e.Topic] {
			t.Fatalf("topic %q missing from Names()", e.Topic)
		}
	}
	for _, req := range requestTypes {
		info, ok := m.InfoOf(reflect.TypeOf(req))
		if !ok || !info.Local {
			t.Fatalf("%T not registered", req)
		}
	}
	regs := m.ConsumerRegistrations()
	if len(regs) != 3 || regs[0].Group != GroupAudit || regs[1].Group != GroupPoison || regs[2].Group != GroupReadModel || regs[0].Topic != TopicBumped {
		t.Fatalf("consumers: %+v", regs)
	}
}

func TestRegister_Groups(t *testing.T) {
	m := mediator.New(mediator.WithNodeID("n7"))
	if err := Register(m, Deps{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	groups := map[string]bool{}
	for _, r := range m.ConsumerRegistrations() {
		groups[r.Group] = true
	}
	if len(groups) != 2 || !groups[GroupReadModel] || !groups[GroupAudit] {
		t.Fatalf("default groups: %v", groups)
	}
	none := mediator.New()
	if err := Register(none, Deps{Groups: []string{}}); err != nil {
		t.Fatal(err)
	}
	if err := none.Build(); err != nil {
		t.Fatal(err)
	}
	if len(none.ConsumerRegistrations()) != 0 {
		t.Fatal("empty Groups must register no consumer")
	}
	bad := mediator.New()
	if err := Register(bad, Deps{Groups: []string{"nope"}}); err == nil || !strings.Contains(err.Error(), "unknown consumer group") {
		t.Fatalf("unknown group: %v", err)
	}
	twice := mediator.New()
	if err := Register(twice, Deps{}); err != nil {
		t.Fatal(err)
	}
	if err := Register(twice, Deps{}); err == nil {
		t.Fatal("second Register must fail on duplicate registrations")
	}
}

// build returns a built mediator with no behaviors: handlers run without a
// unit of work, which exercises their transaction guards.
func build(t *testing.T, deps Deps) *mediator.Mediator {
	t.Helper()
	m := mediator.New()
	if err := Register(m, deps); err != nil {
		t.Fatal(err)
	}
	if err := m.Build(); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestHandlers_RequireTransaction(t *testing.T) {
	m := build(t, Deps{Groups: AllGroups, NestedTouch: true})
	ctx := context.Background()
	for _, req := range []any{
		SetValue{Key: "k", CmdID: "c"}, GetValue{Key: "k"}, GetValueCached{Key: "k"},
		Transfer{From: "a", To: "b", Amt: 1, CmdID: "c"}, ReadAll{}, Append{Key: "k", CmdID: "c"},
		ReadList{Key: "k"}, Bump{Key: "k"}, AtomicScenario{CmdID: "c", Key1: "a", Key2: "b"}, Touch{Key: "k", CmdID: "c"},
	} {
		_, err := m.SendAny(ctx, req)
		if !errors.Is(err, ErrNoTransaction) || mediator.CodeOf(err) != mediator.CodeInternal {
			t.Fatalf("%T outside a unit of work: %v", req, err)
		}
	}
	// The in-process handler and every consumer also need the transaction.
	if err := mediator.Publish(ctx, m, AtomicDone{CmdID: "c"}); !errors.Is(err, ErrNoTransaction) {
		t.Fatalf("AtomicDone: %v", err)
	}
	env := mediator.Envelope{ID: mediator.NewID(time.Now()), Type: NameBumped, Topic: TopicBumped, StreamKey: "k", Seq: 1}
	payload := []byte(`{"key":"k","n":1}`)
	for _, g := range AllGroups {
		if err := m.Deliver(ctx, g, env, payload); !errors.Is(err, ErrNoTransaction) {
			t.Fatalf("%s: %v", g, err)
		}
	}
	// The poison consumer fails before touching the database.
	p := build(t, Deps{Groups: []string{GroupPoison}, PoisonKey: "k"})
	if err := p.Deliver(ctx, GroupPoison, env, payload); mediator.CodeOf(err) != mediator.CodeInternal || errors.Is(err, ErrNoTransaction) {
		t.Fatalf("poison: %v", err)
	}
}

func TestPanicAndSlow(t *testing.T) {
	m := build(t, Deps{Groups: []string{}})
	ctx := context.Background()
	if _, err := mediator.Send(ctx, m, Panic{}); err == nil || !strings.Contains(err.Error(), "Panic command") {
		t.Fatalf("panic: %v", err)
	}
	start := time.Now()
	if _, err := mediator.Send(ctx, m, Slow{Millis: 20}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Fatal("Slow returned early")
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err := mediator.Send(cctx, m, Slow{Millis: 10_000})
	if mediator.CodeOf(err) != mediator.CodeTimeout || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled slow: %v", err)
	}
}

// recordingStore is a pg.Store whose transaction exposes a recording pgx.Tx
// through PgxTx, so a handler runs inside pg.WithTx without a database and
// the test sees the SQL arguments it binds.
type recordingStore struct {
	pg.Store // nil: only Begin is called
	tx       *recordingTx
}

func (s *recordingStore) Begin(context.Context, pg.TxOptions) (pg.Tx, error) { return s.tx, nil }

type recordingTx struct {
	pg.Tx // nil: only Commit, Rollback, and ReadOnly are called
	execs [][]any
}

func (t *recordingTx) Commit(context.Context) error   { return nil }
func (t *recordingTx) Rollback(context.Context) error { return nil }
func (t *recordingTx) ReadOnly() bool                 { return false }
func (t *recordingTx) PgxTx() pgx.Tx                  { return &recordingPgxTx{tx: t} }

type recordingPgxTx struct {
	pgx.Tx // nil: only Exec is called
	tx     *recordingTx
}

func (p *recordingPgxTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	p.tx.execs = append(p.tx.execs, append([]any{sql}, args...))
	return pgconn.CommandTag{}, nil
}

func TestRegister_NodeID(t *testing.T) {
	// The node written to wl_cmd_log is Deps.NodeID, else the mediator's node
	// ID, else "node".
	cases := []struct {
		name string
		opts []mediator.Option
		deps Deps
		want string
	}{
		{"explicit", []mediator.Option{mediator.WithNodeID("n7")}, Deps{NodeID: "explicit"}, "explicit"},
		{"mediator", []mediator.Option{mediator.WithNodeID("n7")}, Deps{}, "n7"},
		{"default", nil, Deps{}, "node"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			m := mediator.New(c.opts...)
			c.deps.Groups = []string{}
			if err := Register(m, c.deps); err != nil {
				t.Fatal(err)
			}
			if err := m.Build(); err != nil {
				t.Fatal(err)
			}
			tx := &recordingTx{}
			err := pg.WithTx(context.Background(), &recordingStore{tx: tx}, pg.TxOptions{}, func(ctx context.Context) error {
				_, err := mediator.Send(ctx, m, Touch{Key: "k", CmdID: "c"})
				return err
			})
			if err != nil {
				t.Fatal(err)
			}
			// wl_cmd_log binds (cmd_id, name, key, node, request_id).
			if len(tx.execs) != 1 || len(tx.execs[0]) != 6 || tx.execs[0][1] != "c" || tx.execs[0][2] != NameTouch || tx.execs[0][4] != c.want {
				t.Fatalf("wl_cmd_log exec = %v, want node %q", tx.execs, c.want)
			}
		})
	}
}
