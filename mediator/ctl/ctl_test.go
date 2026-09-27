package ctl

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

var fixedNow = time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)

// fakePG is a Postgres whose methods are function fields; unset fields
// return zero values.
type fakePG struct {
	migrate       func(context.Context) error
	migrateDown   func(context.Context, int) error
	status        func(context.Context) ([]pg.MigrationInfo, error)
	outboxStats   func(context.Context) ([]pg.PartitionStats, error)
	outboxReplay  func(context.Context, pg.StreamSink, string, int, int64) (int, error)
	outboxReshard func(context.Context, int) (int64, error)
	inboxPurge    func(context.Context, time.Duration) (int64, error)
	idemPurge     func(context.Context) (int64, error)
	idemShow      func(context.Context, string, string) (pg.IdempotencyInfo, error)
	closed        int
}

func (f *fakePG) Migrate(ctx context.Context) error {
	if f.migrate != nil {
		return f.migrate(ctx)
	}
	return nil
}

func (f *fakePG) MigrateDown(ctx context.Context, to int) error {
	if f.migrateDown != nil {
		return f.migrateDown(ctx, to)
	}
	return nil
}

func (f *fakePG) MigrationStatus(ctx context.Context) ([]pg.MigrationInfo, error) {
	if f.status != nil {
		return f.status(ctx)
	}
	return nil, nil
}

func (f *fakePG) OutboxStats(ctx context.Context) ([]pg.PartitionStats, error) {
	if f.outboxStats != nil {
		return f.outboxStats(ctx)
	}
	return nil, nil
}

func (f *fakePG) OutboxReplay(ctx context.Context, sink pg.StreamSink, topic string, partition int, fromID int64) (int, error) {
	if f.outboxReplay != nil {
		return f.outboxReplay(ctx, sink, topic, partition, fromID)
	}
	return 0, nil
}

func (f *fakePG) OutboxReshard(ctx context.Context, p int) (int64, error) {
	if f.outboxReshard != nil {
		return f.outboxReshard(ctx, p)
	}
	return 0, nil
}

func (f *fakePG) InboxPurge(ctx context.Context, d time.Duration) (int64, error) {
	if f.inboxPurge != nil {
		return f.inboxPurge(ctx, d)
	}
	return 0, nil
}

func (f *fakePG) IdemPurge(ctx context.Context) (int64, error) {
	if f.idemPurge != nil {
		return f.idemPurge(ctx)
	}
	return 0, nil
}

func (f *fakePG) IdemShow(ctx context.Context, scope, key string) (pg.IdempotencyInfo, error) {
	if f.idemShow != nil {
		return f.idemShow(ctx, scope, key)
	}
	return pg.IdempotencyInfo{}, nil
}

func (f *fakePG) Close() error {
	f.closed++
	return nil
}

// fakeSink is the stream sink the fake Redis hands to outbox replay.
type fakeSink struct{}

func (fakeSink) Append(context.Context, string, int, []pg.OutboxEntry) (string, error) {
	return "", nil
}
func (fakeSink) Tail(context.Context, string, int) (string, int64, bool, error) {
	return "", 0, false, nil
}
func (fakeSink) EnsureGroups(context.Context, string, int, []string) error { return nil }

// fakeRedis is a Redis whose methods are function fields.
type fakeRedis struct {
	dlqList       func(context.Context, string) ([]redisx.DLQEntry, error)
	dlqRequeue    func(context.Context, string, string) error
	dlqDrop       func(context.Context, string, string) error
	partitionSkip func(context.Context, string, string, int, string) error
	leaseList     func(context.Context, string) ([]redisx.LeaseInfo, error)
	leaseRelease  func(context.Context, string, string, int) error
	consumerLag   func(context.Context, []string, []string) ([]redisx.LagInfo, error)
	sink          *fakeSink
	closed        int
}

func (f *fakeRedis) DLQList(ctx context.Context, group string) ([]redisx.DLQEntry, error) {
	if f.dlqList != nil {
		return f.dlqList(ctx, group)
	}
	return nil, nil
}

func (f *fakeRedis) DLQRequeue(ctx context.Context, group, id string) error {
	if f.dlqRequeue != nil {
		return f.dlqRequeue(ctx, group, id)
	}
	return nil
}

func (f *fakeRedis) DLQDrop(ctx context.Context, group, id string) error {
	if f.dlqDrop != nil {
		return f.dlqDrop(ctx, group, id)
	}
	return nil
}

func (f *fakeRedis) PartitionSkip(ctx context.Context, group, topic string, partition int, id string) error {
	if f.partitionSkip != nil {
		return f.partitionSkip(ctx, group, topic, partition, id)
	}
	return nil
}

func (f *fakeRedis) LeaseList(ctx context.Context, group string) ([]redisx.LeaseInfo, error) {
	if f.leaseList != nil {
		return f.leaseList(ctx, group)
	}
	return nil, nil
}

func (f *fakeRedis) LeaseRelease(ctx context.Context, group, topic string, partition int) error {
	if f.leaseRelease != nil {
		return f.leaseRelease(ctx, group, topic, partition)
	}
	return nil
}

func (f *fakeRedis) ConsumerLag(ctx context.Context, groups, topics []string) ([]redisx.LagInfo, error) {
	if f.consumerLag != nil {
		return f.consumerLag(ctx, groups, topics)
	}
	return nil, nil
}

func (f *fakeRedis) Sink() pg.StreamSink { return f.sink }

func (f *fakeRedis) Close() error {
	f.closed++
	return nil
}

type exportCall struct {
	cfg openapi.Config
	out string
}

// harness runs Main against fakes and captures its output.
type harness struct {
	t                   *testing.T
	pg                  *fakePG
	redis               *fakeRedis
	out, err            bytes.Buffer
	opts                Options
	pgOpens, redisOpens int
	exports             []exportCall
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, pg: &fakePG{}, redis: &fakeRedis{sink: &fakeSink{}}}
	h.opts = Options{
		Stdout: &h.out, Stderr: &h.err,
		PGURL: "postgres://fake", Redis: redisx.Config{Addr: "fake:6379", Prefix: "t"},
		Now:    func() time.Time { return fixedNow },
		getenv: func(string) string { return "" },
		openPostgres: func(context.Context, string) (Postgres, error) {
			h.pgOpens++
			return h.pg, nil
		},
		openRedis: func(context.Context, redisx.Config) (Redis, error) {
			h.redisOpens++
			return h.redis, nil
		},
		export: func(_ *mediator.Mediator, cfg openapi.Config, out string) error {
			h.exports = append(h.exports, exportCall{cfg: cfg, out: out})
			return nil
		},
	}
	return h
}

func (h *harness) run(args ...string) int {
	h.out.Reset()
	h.err.Reset()
	return Main(context.Background(), args, h.opts)
}

// expect runs args and fails unless the exit code matches.
func (h *harness) expect(code int, args ...string) (stdout, stderr string) {
	h.t.Helper()
	if got := h.run(args...); got != code {
		h.t.Fatalf("%v: exit %d, want %d\nstdout: %s\nstderr: %s", args, got, code, h.out.String(), h.err.String())
	}
	return h.out.String(), h.err.String()
}

// json runs args with --json and decodes the document.
func (h *harness) json(args ...string) map[string]any {
	h.t.Helper()
	stdout, _ := h.expect(ExitOK, append(args, "--json")...)
	var v map[string]any
	if err := json.Unmarshal([]byte(stdout), &v); err != nil {
		h.t.Fatalf("%v: invalid json %q: %v", args, stdout, err)
	}
	if !strings.HasSuffix(stdout, "}\n") {
		h.t.Fatalf("json output must end with a newline: %q", stdout)
	}
	return v
}

func mustContain(t *testing.T, s string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Fatalf("output lacks %q:\n%s", sub, s)
		}
	}
}

var spaces = regexp.MustCompile(` +`)

// lines returns the non-empty lines of s with runs of spaces squashed, so
// table assertions do not depend on column widths.
func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		l = strings.TrimSpace(spaces.ReplaceAllString(l, " "))
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

func wantLines(t *testing.T, got string, want ...string) {
	t.Helper()
	gl := lines(got)
	if len(gl) != len(want) {
		t.Fatalf("got %d lines, want %d:\n%s", len(gl), len(want), got)
	}
	for i := range want {
		if gl[i] != want[i] {
			t.Fatalf("line %d = %q, want %q\n%s", i, gl[i], want[i], got)
		}
	}
}

func list(t *testing.T, v map[string]any, key string) []map[string]any {
	t.Helper()
	raw, ok := v[key].([]any)
	if !ok {
		t.Fatalf("%q is not a list: %v", key, v)
	}
	out := make([]map[string]any, 0, len(raw))
	for _, e := range raw {
		out = append(out, e.(map[string]any))
	}
	return out
}

// Registry fixtures.
type ctlPing struct {
	mediator.Command[mediator.Void]
}
type ctlPinned struct{ mediator.Query[int] }

func (ctlPinned) Name() string { return "PinnedQuery" }

type ctlOrderSubmitted struct {
	mediator.Event
	OrderID string `json:"orderId"`
}

func (e ctlOrderSubmitted) StreamKey() string { return e.OrderID }

type ctlStockReserved struct {
	mediator.Event
	SKU string `json:"sku"`
}

func (e ctlStockReserved) StreamKey() string { return e.SKU }

func testRegistry(t *testing.T) *mediator.Mediator {
	t.Helper()
	m := mediator.New()
	mediator.MustHandle(m, mediator.HandlerFunc[ctlPing, mediator.Void](func(context.Context, ctlPing) (mediator.Void, error) {
		return mediator.Void{}, nil
	}))
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(mediator.HandleFunc(m, func(context.Context, ctlPinned) (int, error) { return 1, nil }))
	must(mediator.ConsumeFunc(m, "inventory", func(context.Context, ctlOrderSubmitted) error { return nil }))
	must(mediator.ConsumeFunc(m, "readmodel", func(context.Context, ctlOrderSubmitted) error { return nil }))
	must(mediator.ConsumeFunc(m, "inventory", func(context.Context, ctlStockReserved) error { return nil }))
	must(m.Build())
	return m
}

func (h *harness) withRegistry(m *mediator.Mediator) {
	h.opts.Registry = func() (*mediator.Mediator, error) { return m, nil }
}

func migrations(applied ...bool) []pg.MigrationInfo {
	out := []pg.MigrationInfo{{Version: 1, Name: "init"}, {Version: 2, Name: "audit"}}
	for i := range out {
		if i < len(applied) && applied[i] {
			out[i].Applied = true
			out[i].AppliedAt = fixedNow.Add(-time.Hour)
		}
	}
	return out
}

func TestMain_Usage(t *testing.T) {
	h := newHarness(t)
	_, stderr := h.expect(ExitUsage)
	mustContain(t, stderr, "usage: mediatorctl <command> [flags]", "migrate up", "openapi export", "--json")
	for _, arg := range []string{"help", "-h", "-help", "--help"} {
		stdout, _ := h.expect(ExitOK, arg)
		mustContain(t, stdout, "usage: mediatorctl", "consumer lag", EnvPGURL, EnvRedisPrefix)
	}
	_, stderr = h.expect(ExitUsage, "bogus")
	mustContain(t, stderr, `error: unknown command "bogus"`, "usage: mediatorctl")
	_, stderr = h.expect(ExitUsage, "migrate")
	mustContain(t, stderr, `error: unknown command "migrate"`)
	_, stderr = h.expect(ExitUsage, "migrate", "sideways", "--json")
	mustContain(t, stderr, `error: unknown command "migrate sideways"`)
	stdout, _ := h.expect(ExitOK, "idem", "show", "-h")
	mustContain(t, stdout, "usage: mediatorctl idem show [flags]", "show one idempotency row", "-scope", "-key", "-json")
	stdout, _ = h.expect(ExitOK, "names", "--help")
	mustContain(t, stdout, "usage: mediatorctl names [flags]")
	_, stderr = h.expect(ExitUsage, "names", "--bogus")
	mustContain(t, stderr, "error: flag provided but not defined: -bogus", "usage: mediatorctl names")
	_, stderr = h.expect(ExitUsage, "names", "extra")
	mustContain(t, stderr, `error: unexpected argument "extra"`)
	_, stderr = h.expect(ExitUsage, "outbox", "replay", "--partition", "two")
	mustContain(t, stderr, `invalid value "two" for flag -partition`)
	if h.pgOpens+h.redisOpens != 0 {
		t.Fatalf("usage paths opened connections: pg %d redis %d", h.pgOpens, h.redisOpens)
	}
	// Every command of spec 9.3 is in the table and has help.
	for _, name := range []string{"migrate up", "migrate status", "migrate down", "names", "outbox stats", "outbox replay",
		"outbox reshard", "inbox purge", "idem purge", "idem show", "consumer lag", "dlq list", "dlq requeue", "dlq drop",
		"dlq skip", "lease list", "lease release", "openapi export"} {
		stdout, _ := h.expect(ExitOK, append(strings.Fields(name), "-h")...)
		mustContain(t, stdout, "usage: mediatorctl "+name)
	}
}

func TestOptions_EnvDefaults(t *testing.T) {
	env := map[string]string{
		EnvPGURL: "postgres://env", EnvRedisAddr: "env:6379", EnvRedisUsername: "u", EnvRedisPassword: "p",
		EnvRedisDB: "3", EnvRedisPrefix: "px",
	}
	getenv := func(k string) string { return env[k] }
	o, err := Options{getenv: getenv}.withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	if o.PGURL != "postgres://env" || o.Redis.Addr != "env:6379" || o.Redis.Username != "u" || o.Redis.Password != "p" ||
		o.Redis.DB != 3 || o.Redis.Prefix != "px" {
		t.Fatalf("env defaults not applied: %+v", o)
	}
	if o.Stdout != os.Stdout || o.Stderr != os.Stderr || o.Now == nil || o.openPostgres == nil || o.openRedis == nil || o.export == nil {
		t.Fatalf("zero defaults not applied: %+v", o)
	}
	// Explicit values win over the environment.
	o, err = Options{PGURL: "postgres://explicit", Redis: redisx.Config{Addr: "x:1", DB: 1, Prefix: "e"}, getenv: getenv}.withDefaults()
	if err != nil || o.PGURL != "postgres://explicit" || o.Redis.Addr != "x:1" || o.Redis.DB != 1 || o.Redis.Prefix != "e" {
		t.Fatalf("explicit values overridden: %+v, %v", o, err)
	}
	// A malformed REDIS_DB is an error.
	env[EnvRedisDB] = "abc"
	if _, err := (Options{getenv: getenv}).withDefaults(); err == nil || !strings.Contains(err.Error(), `REDIS_DB="abc"`) {
		t.Fatalf("want REDIS_DB error, got %v", err)
	}
	// Without a getenv the process environment is read.
	t.Setenv(EnvPGURL, "postgres://process")
	o, err = Options{}.withDefaults()
	if err != nil || o.PGURL != "postgres://process" {
		t.Fatalf("os.Getenv default: %q, %v", o.PGURL, err)
	}
}

func TestMain_InvalidRedisDBReportedOnlyForRedis(t *testing.T) {
	h := newHarness(t)
	h.opts.getenv = func(k string) string {
		if k == EnvRedisDB {
			return "x"
		}
		return ""
	}
	h.expect(ExitOK, "idem", "purge")
	_, stderr := h.expect(ExitFailure, "dlq", "list", "--group", "g")
	mustContain(t, stderr, `error: REDIS_DB="x" is not an integer`)
	if h.redisOpens != 0 {
		t.Fatal("redis opened despite bad REDIS_DB")
	}
}

func TestMain_ConnectionErrors(t *testing.T) {
	h := newHarness(t)
	h.opts.PGURL = ""
	_, stderr := h.expect(ExitFailure, "migrate", "status")
	mustContain(t, stderr, "error: PG_URL is not set")
	h = newHarness(t)
	h.opts.openPostgres = func(context.Context, string) (Postgres, error) { return nil, errors.New("refused") }
	_, stderr = h.expect(ExitFailure, "migrate", "status")
	mustContain(t, stderr, "error: connect to postgres: refused")
	h.opts.openRedis = func(context.Context, redisx.Config) (Redis, error) { return nil, errors.New("nope") }
	_, stderr = h.expect(ExitFailure, "lease", "list", "--group", "g")
	mustContain(t, stderr, "error: connect to redis: nope")
}

func TestMain_Interrupted(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h.pg.idemPurge = func(ctx context.Context) (int64, error) { return 0, ctx.Err() }
	if code := Main(ctx, []string{"idem", "purge"}, h.opts); code != ExitInterrupted {
		t.Fatalf("exit %d, want %d: %s", code, ExitInterrupted, h.err.String())
	}
	mustContain(t, h.err.String(), "interrupted: context canceled")
	// A wrapped cancellation with a live context also counts as interrupted.
	h.pg.idemPurge = func(context.Context) (int64, error) { return 0, fmt.Errorf("query: %w", context.DeadlineExceeded) }
	h.expect(ExitInterrupted, "idem", "purge")
}

func TestMain_ClosesConnections(t *testing.T) {
	h := newHarness(t)
	h.expect(ExitOK, "consumer", "lag", "--group", "g", "--topic", "t")
	if h.pgOpens != 1 || h.redisOpens != 1 || h.pg.closed != 1 || h.redis.closed != 1 {
		t.Fatalf("opens pg=%d redis=%d closes pg=%d redis=%d", h.pgOpens, h.redisOpens, h.pg.closed, h.redis.closed)
	}
	h.expect(ExitOK, "idem", "purge")
	if h.pgOpens != 2 || h.redisOpens != 1 {
		t.Fatalf("idem purge opened redis: pg=%d redis=%d", h.pgOpens, h.redisOpens)
	}
	h.expect(ExitOK, "dlq", "list", "--group", "g")
	if h.pgOpens != 2 || h.redisOpens != 2 {
		t.Fatalf("dlq list opened postgres: pg=%d redis=%d", h.pgOpens, h.redisOpens)
	}
}

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestMain_RenderError(t *testing.T) {
	h := newHarness(t)
	h.opts.Stdout = failWriter{}
	_, stderr := h.expect(ExitFailure, "idem", "purge")
	mustContain(t, stderr, "error: disk full")
	_, stderr = h.expect(ExitFailure, "idem", "purge", "--json")
	mustContain(t, stderr, "error: disk full")
}

func TestMigrateStatus(t *testing.T) {
	h := newHarness(t)
	h.pg.status = func(context.Context) ([]pg.MigrationInfo, error) { return migrations(true, false), nil }
	stdout, _ := h.expect(ExitOK, "migrate", "status")
	wantLines(t, stdout,
		"VERSION NAME APPLIED APPLIED AT",
		"1 init true 2026-09-26T11:00:00Z",
		"2 audit false -",
		"current version: 1")
	v := h.json("migrate", "status")
	if v["current"] != 1.0 {
		t.Fatalf("current = %v", v["current"])
	}
	ms := list(t, v, "migrations")
	if ms[0]["appliedAt"] != "2026-09-26T11:00:00Z" || ms[0]["name"] != "init" {
		t.Fatalf("row 0 = %v", ms[0])
	}
	if _, has := ms[1]["appliedAt"]; has || ms[1]["applied"] != false {
		t.Fatalf("row 1 = %v", ms[1])
	}
	h.pg.status = func(context.Context) ([]pg.MigrationInfo, error) { return nil, errors.New("boom") }
	_, stderr := h.expect(ExitFailure, "migrate", "status")
	mustContain(t, stderr, "error: boom")
}

func TestMigrateUp(t *testing.T) {
	h := newHarness(t)
	state := migrations()
	h.pg.status = func(context.Context) ([]pg.MigrationInfo, error) { return state, nil }
	h.pg.migrate = func(context.Context) error {
		state = migrations(true, true)
		return nil
	}
	stdout, _ := h.expect(ExitOK, "migrate", "up")
	wantLines(t, stdout, "applied 0001_init", "applied 0002_audit", "current version: 2 (was 0)")
	stdout, _ = h.expect(ExitOK, "migrate", "up")
	wantLines(t, stdout, "nothing to do: current version 2")
	v := h.json("migrate", "up")
	if v["direction"] != "up" || v["before"] != 2.0 || v["current"] != 2.0 || len(list(t, v, "changed")) != 0 {
		t.Fatalf("json = %v", v)
	}
	// Partial application: only the newly applied row is reported.
	state = migrations(true, false)
	h.pg.migrate = func(context.Context) error {
		state = migrations(true, true)
		return nil
	}
	v = h.json("migrate", "up")
	if ch := list(t, v, "changed"); len(ch) != 1 || ch[0]["version"] != 2.0 || v["before"] != 1.0 {
		t.Fatalf("json = %v", v)
	}
	// Errors from each step.
	h.pg.migrate = func(context.Context) error { return errors.New("syntax error") }
	_, stderr := h.expect(ExitFailure, "migrate", "up")
	mustContain(t, stderr, "error: syntax error")
	calls := 0
	h.pg.migrate = func(context.Context) error { return nil }
	h.pg.status = func(context.Context) ([]pg.MigrationInfo, error) {
		calls++
		if calls == 2 {
			return nil, errors.New("second status failed")
		}
		return state, nil
	}
	_, stderr = h.expect(ExitFailure, "migrate", "up")
	mustContain(t, stderr, "error: second status failed")
	h.pg.status = func(context.Context) ([]pg.MigrationInfo, error) { return nil, errors.New("first status failed") }
	_, stderr = h.expect(ExitFailure, "migrate", "up")
	mustContain(t, stderr, "error: first status failed")
}

func TestMigrateDown(t *testing.T) {
	h := newHarness(t)
	state := migrations(true, true)
	h.pg.status = func(context.Context) ([]pg.MigrationInfo, error) { return state, nil }
	gotTo := -1
	h.pg.migrateDown = func(_ context.Context, to int) error {
		gotTo = to
		state = migrations(to >= 1, to >= 2)
		return nil
	}
	_, stderr := h.expect(ExitUsage, "migrate", "down")
	mustContain(t, stderr, "error: --to is required", "usage: mediatorctl migrate down")
	if h.pgOpens != 0 {
		t.Fatal("opened postgres before validating flags")
	}
	stdout, stderr := h.expect(ExitOK, "migrate", "down", "--to", "0")
	mustContain(t, stderr, "warning: migrate down exists for tests; migrations are append-only once merged")
	wantLines(t, stdout, "reverted 0002_audit", "reverted 0001_init", "current version: 0 (was 2)")
	if gotTo != 0 {
		t.Fatalf("to = %d", gotTo)
	}
	state = migrations(true, true)
	v := h.json("migrate", "down", "--to", "1")
	if ch := list(t, v, "changed"); v["direction"] != "down" || len(ch) != 1 || ch[0]["version"] != 2.0 || v["current"] != 1.0 {
		t.Fatalf("json = %v", v)
	}
	if _, err := MigrateDown(context.Background(), h.pg, -1); err == nil {
		t.Fatal("negative target accepted")
	}
	h.pg.migrateDown = func(context.Context, int) error { return errors.New("no down section") }
	_, stderr = h.expect(ExitFailure, "migrate", "down", "--to", "1")
	mustContain(t, stderr, "error: no down section")
}

func TestNames(t *testing.T) {
	h := newHarness(t)
	_, stderr := h.expect(ExitFailure, "names")
	mustContain(t, stderr, "error: no registry available")
	h.opts.Registry = func() (*mediator.Mediator, error) { return nil, errors.New("not linked") }
	_, stderr = h.expect(ExitFailure, "names")
	mustContain(t, stderr, "error: registry: not linked")
	h.opts.Registry = func() (*mediator.Mediator, error) { return nil, nil }
	_, stderr = h.expect(ExitFailure, "names")
	mustContain(t, stderr, "nil mediator")
	h.withRegistry(testRegistry(t))
	stdout, _ := h.expect(ExitOK, "names")
	wantLines(t, stdout,
		"KIND NAME GO TYPE GROUP TOPIC PINNED",
		"command ctlPing ctl.ctlPing - - false",
		"query PinnedQuery ctl.ctlPinned - - true",
		"consumer ctlOrderSubmitted ctl.ctlOrderSubmitted inventory ctlOrderSubmitted false",
		"consumer ctlOrderSubmitted ctl.ctlOrderSubmitted readmodel ctlOrderSubmitted false",
		"consumer ctlStockReserved ctl.ctlStockReserved inventory ctlStockReserved false")
	v := h.json("names")
	names := list(t, v, "names")
	if len(names) != 5 || names[1]["pinned"] != true || names[1]["name"] != "PinnedQuery" || names[2]["group"] != "inventory" {
		t.Fatalf("json = %v", names)
	}
	if h.pgOpens+h.redisOpens != 0 {
		t.Fatal("names opened a connection")
	}
	empty := mediator.New()
	if err := empty.Build(); err != nil {
		t.Fatal(err)
	}
	h.withRegistry(empty)
	stdout, _ = h.expect(ExitOK, "names")
	wantLines(t, stdout, "no registrations")
}

func TestOutboxStats(t *testing.T) {
	h := newHarness(t)
	stdout, _ := h.expect(ExitOK, "outbox", "stats")
	wantLines(t, stdout, "no unpublished outbox rows")
	h.pg.outboxStats = func(context.Context) ([]pg.PartitionStats, error) {
		return []pg.PartitionStats{
			{Topic: "orders", Partition: 0, Unpublished: 12, OldestAge: 90*time.Second + 400*time.Millisecond},
			{Topic: "orders", Partition: 3, Unpublished: 1},
		}, nil
	}
	stdout, _ = h.expect(ExitOK, "outbox", "stats")
	wantLines(t, stdout, "TOPIC PARTITION UNPUBLISHED OLDEST AGE", "orders 0 12 1m30s", "orders 3 1 0s")
	v := h.json("outbox", "stats")
	ps := list(t, v, "partitions")
	if len(ps) != 2 || ps[0]["oldestAgeSeconds"] != 90.4 || ps[0]["unpublished"] != 12.0 || ps[1]["partition"] != 3.0 {
		t.Fatalf("json = %v", ps)
	}
	h.pg.outboxStats = func(context.Context) ([]pg.PartitionStats, error) { return nil, errors.New("boom") }
	_, stderr := h.expect(ExitFailure, "outbox", "stats")
	mustContain(t, stderr, "error: boom")
}

func TestOutboxReplay(t *testing.T) {
	h := newHarness(t)
	_, stderr := h.expect(ExitUsage, "outbox", "replay")
	mustContain(t, stderr, "error: --topic is required")
	_, stderr = h.expect(ExitUsage, "outbox", "replay", "--topic", "orders")
	mustContain(t, stderr, "error: --partition is required")
	var got struct {
		sink      pg.StreamSink
		topic     string
		partition int
		from      int64
	}
	h.pg.outboxReplay = func(_ context.Context, sink pg.StreamSink, topic string, partition int, from int64) (int, error) {
		got.sink, got.topic, got.partition, got.from = sink, topic, partition, from
		return 7, nil
	}
	stdout, _ := h.expect(ExitOK, "outbox", "replay", "--topic", "orders", "--partition", "2", "--from-id", "100")
	wantLines(t, stdout, "replayed 7 rows of orders partition 2 from id 100")
	if got.sink != h.redis.sink || got.topic != "orders" || got.partition != 2 || got.from != 100 {
		t.Fatalf("replay args = %+v", got)
	}
	if h.pgOpens != 1 || h.redisOpens != 1 {
		t.Fatalf("opens pg=%d redis=%d", h.pgOpens, h.redisOpens)
	}
	v := h.json("outbox", "replay", "--topic", "orders", "--partition", "0")
	if v["replayed"] != 7.0 || v["fromId"] != 0.0 || v["partition"] != 0.0 || v["topic"] != "orders" {
		t.Fatalf("json = %v", v)
	}
	ctx := context.Background()
	if _, err := OutboxReplay(ctx, h.pg, nil, "", 0, 0); err == nil {
		t.Fatal("empty topic accepted")
	}
	if _, err := OutboxReplay(ctx, h.pg, nil, "t", -1, 0); err == nil {
		t.Fatal("negative partition accepted")
	}
	h.pg.outboxReplay = func(context.Context, pg.StreamSink, string, int, int64) (int, error) {
		return 0, errors.New("xadd failed")
	}
	_, stderr = h.expect(ExitFailure, "outbox", "replay", "--topic", "orders", "--partition", "0")
	mustContain(t, stderr, "error: xadd failed")
}

func TestOutboxReshard(t *testing.T) {
	h := newHarness(t)
	_, stderr := h.expect(ExitUsage, "outbox", "reshard")
	mustContain(t, stderr, "error: --partitions must be a positive count")
	_, stderr = h.expect(ExitUsage, "outbox", "reshard", "--partitions", "32")
	mustContain(t, stderr, "error: refusing to reshard without --yes", "every consumer and relay must be stopped")
	if h.pgOpens != 0 {
		t.Fatal("opened postgres without --yes")
	}
	gotP := 0
	h.pg.outboxReshard = func(_ context.Context, p int) (int64, error) {
		gotP = p
		return 5, nil
	}
	stdout, stderr := h.expect(ExitOK, "outbox", "reshard", "--partitions", "32", "--yes")
	mustContain(t, stderr, "warning: "+ReshardWarning)
	wantLines(t, stdout, "resharded to 32 partitions: 5 rows moved, relay cursors reset")
	if gotP != 32 {
		t.Fatalf("partitions = %d", gotP)
	}
	v := h.json("outbox", "reshard", "--partitions", "8", "--yes")
	if v["partitions"] != 8.0 || v["moved"] != 5.0 {
		t.Fatalf("json = %v", v)
	}
	if _, err := OutboxReshard(context.Background(), h.pg, 0); err == nil {
		t.Fatal("zero partitions accepted")
	}
	h.pg.outboxReshard = func(context.Context, int) (int64, error) { return 0, errors.New("locked") }
	_, stderr = h.expect(ExitFailure, "outbox", "reshard", "--partitions", "8", "--yes")
	mustContain(t, stderr, "error: locked")
}

func TestInboxPurge(t *testing.T) {
	h := newHarness(t)
	var got time.Duration
	h.pg.inboxPurge = func(_ context.Context, d time.Duration) (int64, error) {
		got = d
		return 3, nil
	}
	stdout, _ := h.expect(ExitOK, "inbox", "purge")
	if got != 7*24*time.Hour {
		t.Fatalf("default retention = %s", got)
	}
	wantLines(t, stdout, "deleted 3 rows of mediator_inbox older than 168h0m0s")
	h.expect(ExitOK, "inbox", "purge", "--older-than", "1d12h")
	if got != 36*time.Hour {
		t.Fatalf("retention = %s", got)
	}
	_, stderr := h.expect(ExitUsage, "inbox", "purge", "--older-than", "soon")
	mustContain(t, stderr, "error: --older-than: invalid duration")
	v := h.json("inbox", "purge", "--older-than", "48h")
	if v["olderThan"] != "48h0m0s" || v["deleted"] != 3.0 || v["table"] != "mediator_inbox" {
		t.Fatalf("json = %v", v)
	}
	if _, err := InboxPurge(context.Background(), h.pg, 0); err == nil {
		t.Fatal("zero retention accepted")
	}
	h.pg.inboxPurge = func(context.Context, time.Duration) (int64, error) { return 0, errors.New("boom") }
	_, stderr = h.expect(ExitFailure, "inbox", "purge")
	mustContain(t, stderr, "error: boom")
}

func TestIdemPurge(t *testing.T) {
	h := newHarness(t)
	h.pg.idemPurge = func(context.Context) (int64, error) { return 9, nil }
	stdout, _ := h.expect(ExitOK, "idem", "purge")
	wantLines(t, stdout, "deleted 9 expired rows of mediator_idempotency")
	var buf bytes.Buffer
	res, err := IdemPurge(context.Background(), h.pg)
	if err != nil || Render(&buf, res, true) != nil {
		t.Fatal(err)
	}
	if want := "{\n  \"table\": \"mediator_idempotency\",\n  \"deleted\": 9\n}\n"; buf.String() != want {
		t.Fatalf("json = %q, want %q", buf.String(), want)
	}
	h.pg.idemPurge = func(context.Context) (int64, error) { return 0, errors.New("boom") }
	_, stderr := h.expect(ExitFailure, "idem", "purge")
	mustContain(t, stderr, "error: boom")
}

func TestIdemShow(t *testing.T) {
	h := newHarness(t)
	_, stderr := h.expect(ExitUsage, "idem", "show")
	mustContain(t, stderr, "error: --scope is required")
	_, stderr = h.expect(ExitUsage, "idem", "show", "--scope", "CreateOrder")
	mustContain(t, stderr, "error: --key is required")
	h.pg.idemShow = func(_ context.Context, scope, key string) (pg.IdempotencyInfo, error) {
		return pg.IdempotencyInfo{
			Scope: scope, Key: key, RequestHash: []byte{0xde, 0xad}, Response: []byte(`{"orderId":"o-1"}`), Hits: 2,
			CreatedAt: fixedNow.Add(-time.Hour), ExpiresAt: fixedNow.Add(time.Hour),
		}, nil
	}
	stdout, _ := h.expect(ExitOK, "idem", "show", "--scope", "CreateOrder", "--key", "k1")
	wantLines(t, stdout,
		"FIELD VALUE",
		"scope CreateOrder",
		"key k1",
		"request hash dead",
		"hits 2",
		"created at 2026-09-26T11:00:00Z",
		"expires at 2026-09-26T13:00:00Z",
		"expired false",
		`response {"orderId":"o-1"}`)
	v := h.json("idem", "show", "--scope", "CreateOrder", "--key", "k1")
	if v["requestHash"] != "dead" || v["hits"] != 2.0 || v["expired"] != false || v["response"].(map[string]any)["orderId"] != "o-1" {
		t.Fatalf("json = %v", v)
	}
	// A reserved row has no response yet and this one has expired.
	h.pg.idemShow = func(_ context.Context, scope, key string) (pg.IdempotencyInfo, error) {
		return pg.IdempotencyInfo{Scope: scope, Key: key, CreatedAt: fixedNow.Add(-2 * time.Hour), ExpiresAt: fixedNow.Add(-time.Minute)}, nil
	}
	stdout, _ = h.expect(ExitOK, "idem", "show", "--scope", "s", "--key", "k")
	mustContain(t, strings.Join(lines(stdout), "\n"), "request hash -", "expired true", "response (reserved, no response yet)")
	v = h.json("idem", "show", "--scope", "s", "--key", "k")
	if _, has := v["response"]; has || v["expired"] != true {
		t.Fatalf("json = %v", v)
	}
	h.pg.idemShow = func(context.Context, string, string) (pg.IdempotencyInfo, error) {
		return pg.IdempotencyInfo{}, mediator.E(mediator.CodeNotFound, `no idempotency row for scope "s" key "k"`)
	}
	_, stderr = h.expect(ExitFailure, "idem", "show", "--scope", "s", "--key", "k")
	mustContain(t, stderr, `error: not_found: no idempotency row for scope "s" key "k"`)
	if _, err := IdemShow(context.Background(), h.pg, "", "k", fixedNow); err == nil {
		t.Fatal("empty scope accepted")
	}
}

func TestConsumerLag(t *testing.T) {
	h := newHarness(t)
	_, stderr := h.expect(ExitUsage, "consumer", "lag")
	mustContain(t, stderr, "error: --group and --topic are both required when no registry is available")
	h.expect(ExitUsage, "consumer", "lag", "--group", "g")
	h.expect(ExitUsage, "consumer", "lag", "--topic", "t")
	var gotGroups, gotTopics []string
	h.redis.consumerLag = func(_ context.Context, groups, topics []string) ([]redisx.LagInfo, error) {
		gotGroups, gotTopics = groups, topics
		var out []redisx.LagInfo
		for _, g := range groups {
			for _, t := range topics {
				for p := 0; p < 2; p++ {
					out = append(out, redisx.LagInfo{
						Group: g, Topic: t, Partition: p, Pending: int64(p * 3), OldestAge: time.Duration(p) * 45 * time.Second,
						Consumers: map[string]int64{"node-b": int64(p * 2), "node-a": int64(p)},
					})
				}
			}
		}
		return out, nil
	}
	h.pg.outboxStats = func(context.Context) ([]pg.PartitionStats, error) {
		return []pg.PartitionStats{{Topic: "orders", Partition: 1, Unpublished: 9}}, nil
	}
	stdout, _ := h.expect(ExitOK, "consumer", "lag", "--group", "inventory", "--topic", "orders")
	wantLines(t, stdout,
		"GROUP TOPIC PARTITION PENDING OLDEST AGE UNPUBLISHED CONSUMERS MISSING",
		"inventory orders 0 0 0s 0 node-a=0 node-b=0 false",
		"inventory orders 1 3 45s 9 node-a=1 node-b=2 false")
	if h.pgOpens != 1 || h.redisOpens != 1 {
		t.Fatalf("opens pg=%d redis=%d", h.pgOpens, h.redisOpens)
	}
	raw, _ := h.expect(ExitOK, "consumer", "lag", "--group", "inventory", "--topic", "orders", "--json")
	if strings.Index(raw, `"node-a"`) > strings.Index(raw, `"node-b"`) {
		t.Fatalf("map keys not sorted:\n%s", raw)
	}
	v := h.json("consumer", "lag", "--group", "inventory", "--topic", "orders")
	rows := list(t, v, "rows")
	if len(rows) != 2 || rows[1]["unpublished"] != 9.0 || rows[1]["oldestAgeSeconds"] != 45.0 || rows[1]["consumers"].(map[string]any)["node-b"] != 2.0 {
		t.Fatalf("json = %v", rows)
	}
	// The registry supplies the defaults.
	h.withRegistry(testRegistry(t))
	h.expect(ExitOK, "consumer", "lag")
	if fmt.Sprint(gotGroups) != "[inventory readmodel]" || fmt.Sprint(gotTopics) != "[ctlOrderSubmitted ctlStockReserved]" {
		t.Fatalf("defaults: groups %v topics %v", gotGroups, gotTopics)
	}
	h.expect(ExitOK, "consumer", "lag", "--group", "readmodel")
	if fmt.Sprint(gotGroups) != "[readmodel]" || fmt.Sprint(gotTopics) != "[ctlOrderSubmitted]" {
		t.Fatalf("group filter: groups %v topics %v", gotGroups, gotTopics)
	}
	h.expect(ExitOK, "consumer", "lag", "--topic", "ctlStockReserved")
	if fmt.Sprint(gotGroups) != "[inventory]" || fmt.Sprint(gotTopics) != "[ctlStockReserved]" {
		t.Fatalf("topic filter: groups %v topics %v", gotGroups, gotTopics)
	}
	_, stderr = h.expect(ExitUsage, "consumer", "lag", "--group", "nobody")
	mustContain(t, stderr, `error: no registered consumer matches group "nobody" and topic ""`)
	// Missing streams and nil consumer maps render as dashes.
	h.redis.consumerLag = func(context.Context, []string, []string) ([]redisx.LagInfo, error) {
		return []redisx.LagInfo{{Group: "g", Topic: "t", Partition: 0, Missing: true}}, nil
	}
	stdout, _ = h.expect(ExitOK, "consumer", "lag", "--group", "g", "--topic", "t")
	wantLines(t, stdout, "GROUP TOPIC PARTITION PENDING OLDEST AGE UNPUBLISHED CONSUMERS MISSING", "g t 0 0 0s 0 - true")
	v = h.json("consumer", "lag", "--group", "g", "--topic", "t")
	if rows := list(t, v, "rows"); len(rows[0]["consumers"].(map[string]any)) != 0 || rows[0]["missing"] != true {
		t.Fatalf("json = %v", rows)
	}
	h.redis.consumerLag = func(context.Context, []string, []string) ([]redisx.LagInfo, error) { return nil, nil }
	stdout, _ = h.expect(ExitOK, "consumer", "lag", "--group", "g", "--topic", "t")
	wantLines(t, stdout, "no groups or topics selected")
	// Errors from either side.
	h.redis.consumerLag = func(context.Context, []string, []string) ([]redisx.LagInfo, error) {
		return nil, errors.New("xpending failed")
	}
	_, stderr = h.expect(ExitFailure, "consumer", "lag", "--group", "g", "--topic", "t")
	mustContain(t, stderr, "error: xpending failed")
	h.redis.consumerLag = func(context.Context, []string, []string) ([]redisx.LagInfo, error) { return nil, nil }
	h.pg.outboxStats = func(context.Context) ([]pg.PartitionStats, error) { return nil, errors.New("pg down") }
	_, stderr = h.expect(ExitFailure, "consumer", "lag", "--group", "g", "--topic", "t")
	mustContain(t, stderr, "error: pg down")
	// The Go function tolerates a nil Postgres and rejects empty selections.
	ctx := context.Background()
	if res, err := ConsumerLag(ctx, h.redis, nil, []string{"g"}, []string{"t"}); err != nil || len(res.Rows) != 0 {
		t.Fatalf("nil db: %v %v", res, err)
	}
	if _, err := ConsumerLag(ctx, h.redis, nil, nil, []string{"t"}); err == nil {
		t.Fatal("empty groups accepted")
	}
}

func TestDLQ(t *testing.T) {
	h := newHarness(t)
	_, stderr := h.expect(ExitUsage, "dlq", "list")
	mustContain(t, stderr, "error: --group is required")
	stdout, _ := h.expect(ExitOK, "dlq", "list", "--group", "inventory")
	wantLines(t, stdout, "no dead letters in group inventory")
	id := uuid.MustParse("0192a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
	h.redis.dlqList = func(_ context.Context, group string) ([]redisx.DLQEntry, error) {
		return []redisx.DLQEntry{
			{
				ID: "1700000000000-0", Group: group, Stream: "t:{orders:p1}", StreamID: "1699999999999-0", Attempts: 10,
				Error: "boom", Node: "n1", FailedAt: fixedNow, OutboxID: 77,
				Envelope: mediator.Envelope{ID: id, Type: "OrderSubmitted", Topic: "orders", StreamKey: "o-1", Seq: 4, Partition: 1},
			},
			{ID: "1700000000001-0", Group: group, Attempts: 3, Error: "bad", DecodeErr: errors.New("missing id")},
		}, nil
	}
	stdout, _ = h.expect(ExitOK, "dlq", "list", "--group", "inventory")
	wantLines(t, stdout,
		"ID EVENT KEY SEQ ATTEMPTS FAILED AT NODE ERROR",
		"1700000000000-0 OrderSubmitted o-1 4 10 2026-09-26T12:00:00Z n1 boom",
		"1700000000001-0 (undecodable: missing id) - 0 3 - - bad")
	v := h.json("dlq", "list", "--group", "inventory")
	entries := list(t, v, "entries")
	if v["group"] != "inventory" || entries[0]["eventId"] != id.String() || entries[0]["outboxId"] != 77.0 || entries[0]["stream"] != "t:{orders:p1}" {
		t.Fatalf("json = %v", entries[0])
	}
	if _, has := entries[0]["decodeError"]; has || entries[1]["decodeError"] != "missing id" || entries[1]["eventId"] != "" {
		t.Fatalf("json = %v", entries[1])
	}
	h.redis.dlqList = func(context.Context, string) ([]redisx.DLQEntry, error) { return nil, errors.New("xrange failed") }
	_, stderr = h.expect(ExitFailure, "dlq", "list", "--group", "g")
	mustContain(t, stderr, "error: xrange failed")

	// requeue
	_, stderr = h.expect(ExitUsage, "dlq", "requeue", "--group", "g")
	mustContain(t, stderr, "error: --id is required")
	_, stderr = h.expect(ExitUsage, "dlq", "requeue", "--id", "x")
	mustContain(t, stderr, "error: --group is required")
	var gotGroup, gotID string
	h.redis.dlqRequeue = func(_ context.Context, g, id string) error {
		gotGroup, gotID = g, id
		return nil
	}
	stdout, _ = h.expect(ExitOK, "dlq", "requeue", "--group", "inventory", "--id", "1700000000000-0")
	wantLines(t, stdout, "requeued dead letter 1700000000000-0 of group inventory")
	if gotGroup != "inventory" || gotID != "1700000000000-0" {
		t.Fatalf("requeue args %s %s", gotGroup, gotID)
	}
	v = h.json("dlq", "requeue", "--group", "inventory", "--id", "1700000000000-0")
	if v["action"] != "requeued" || v["id"] != "1700000000000-0" {
		t.Fatalf("json = %v", v)
	}
	h.redis.dlqRequeue = func(context.Context, string, string) error {
		return fmt.Errorf("%w: x in group g", redisx.ErrDLQEntryNotFound)
	}
	_, stderr = h.expect(ExitFailure, "dlq", "requeue", "--group", "g", "--id", "x")
	mustContain(t, stderr, "error: redisx: dead-letter entry not found: x in group g")

	// drop
	_, stderr = h.expect(ExitUsage, "dlq", "drop", "--group", "g")
	mustContain(t, stderr, "error: --id is required")
	h.redis.dlqDrop = func(_ context.Context, g, id string) error {
		gotGroup, gotID = g, id
		return nil
	}
	stdout, _ = h.expect(ExitOK, "dlq", "drop", "--group", "inventory", "--id", "1700000000001-0")
	wantLines(t, stdout, "dropped dead letter 1700000000001-0 of group inventory")
	if gotGroup != "inventory" || gotID != "1700000000001-0" {
		t.Fatalf("drop args %s %s", gotGroup, gotID)
	}
	v = h.json("dlq", "drop", "--group", "inventory", "--id", "1700000000001-0")
	if v["action"] != "dropped" {
		t.Fatalf("json = %v", v)
	}
	h.redis.dlqDrop = func(context.Context, string, string) error { return errors.New("gone") }
	_, stderr = h.expect(ExitFailure, "dlq", "drop", "--group", "g", "--id", "x")
	mustContain(t, stderr, "error: gone")

	// skip
	_, stderr = h.expect(ExitUsage, "dlq", "skip", "--group", "g", "--topic", "t")
	mustContain(t, stderr, "error: --partition is required")
	_, stderr = h.expect(ExitUsage, "dlq", "skip", "--group", "g", "--topic", "t", "--partition", "0")
	mustContain(t, stderr, "error: --id is required")
	var skip struct {
		group, topic, id string
		partition        int
	}
	h.redis.partitionSkip = func(_ context.Context, g, topic string, p int, id string) error {
		skip.group, skip.topic, skip.partition, skip.id = g, topic, p, id
		return nil
	}
	stdout, _ = h.expect(ExitOK, "dlq", "skip", "--group", "inventory", "--topic", "orders", "--partition", "3", "--id", "1700000000000-0")
	mustContain(t, stdout, "acknowledged 1700000000000-0 on orders partition 3 for group inventory")
	if skip.group != "inventory" || skip.topic != "orders" || skip.partition != 3 || skip.id != "1700000000000-0" {
		t.Fatalf("skip args %+v", skip)
	}
	v = h.json("dlq", "skip", "--group", "inventory", "--topic", "orders", "--partition", "3", "--id", "x")
	if v["partition"] != 3.0 || v["id"] != "x" || v["topic"] != "orders" {
		t.Fatalf("json = %v", v)
	}
	h.redis.partitionSkip = func(context.Context, string, string, int, string) error { return errors.New("not pending") }
	_, stderr = h.expect(ExitFailure, "dlq", "skip", "--group", "g", "--topic", "t", "--partition", "0", "--id", "x")
	mustContain(t, stderr, "error: not pending")

	// Argument validation of the Go functions.
	ctx := context.Background()
	if _, err := DLQList(ctx, h.redis, ""); err == nil {
		t.Fatal("empty group accepted")
	}
	if _, err := DLQRequeue(ctx, h.redis, "g", ""); err == nil {
		t.Fatal("empty id accepted")
	}
	if _, err := DLQDrop(ctx, h.redis, "", "x"); err == nil {
		t.Fatal("empty group accepted")
	}
	if _, err := DLQSkip(ctx, h.redis, "g", "t", -1, "x"); err == nil {
		t.Fatal("negative partition accepted")
	}
	if _, err := DLQSkip(ctx, h.redis, "g", "", 0, "x"); err == nil {
		t.Fatal("empty topic accepted")
	}
}

func TestLease(t *testing.T) {
	h := newHarness(t)
	_, stderr := h.expect(ExitUsage, "lease", "list")
	mustContain(t, stderr, "error: --group is required")
	stdout, _ := h.expect(ExitOK, "lease", "list", "--group", "inventory")
	wantLines(t, stdout, "no leases held in group inventory")
	h.redis.leaseList = func(_ context.Context, group string) ([]redisx.LeaseInfo, error) {
		return []redisx.LeaseInfo{
			{Group: group, Topic: "orders", Partition: 0, Node: "n1", Epoch: 12, TTL: 14200 * time.Millisecond},
			{Group: group, Topic: "orders", Partition: 1, Node: "n2", Epoch: 13, TTL: 3 * time.Second},
		}, nil
	}
	stdout, _ = h.expect(ExitOK, "lease", "list", "--group", "inventory")
	wantLines(t, stdout, "TOPIC PARTITION NODE EPOCH TTL", "orders 0 n1 12 14s", "orders 1 n2 13 3s")
	v := h.json("lease", "list", "--group", "inventory")
	ls := list(t, v, "leases")
	if v["group"] != "inventory" || len(ls) != 2 || ls[0]["ttlSeconds"] != 14.2 || ls[0]["epoch"] != 12.0 || ls[1]["node"] != "n2" {
		t.Fatalf("json = %v", ls)
	}
	h.redis.leaseList = func(context.Context, string) ([]redisx.LeaseInfo, error) { return nil, errors.New("scan failed") }
	_, stderr = h.expect(ExitFailure, "lease", "list", "--group", "g")
	mustContain(t, stderr, "error: scan failed")

	_, stderr = h.expect(ExitUsage, "lease", "release", "--group", "g")
	mustContain(t, stderr, "error: --topic is required")
	_, stderr = h.expect(ExitUsage, "lease", "release", "--group", "g", "--topic", "t")
	mustContain(t, stderr, "error: --partition is required")
	var rel struct {
		group, topic string
		partition    int
	}
	h.redis.leaseRelease = func(_ context.Context, g, topic string, p int) error {
		rel.group, rel.topic, rel.partition = g, topic, p
		return nil
	}
	stdout, _ = h.expect(ExitOK, "lease", "release", "--group", "inventory", "--topic", "orders", "--partition", "2")
	mustContain(t, stdout, "released lease of orders partition 2 in group inventory")
	if rel.group != "inventory" || rel.topic != "orders" || rel.partition != 2 {
		t.Fatalf("release args %+v", rel)
	}
	v = h.json("lease", "release", "--group", "inventory", "--topic", "orders", "--partition", "2")
	if v["released"] != true || v["partition"] != 2.0 {
		t.Fatalf("json = %v", v)
	}
	h.redis.leaseRelease = func(context.Context, string, string, int) error { return errors.New("del failed") }
	_, stderr = h.expect(ExitFailure, "lease", "release", "--group", "g", "--topic", "t", "--partition", "0")
	mustContain(t, stderr, "error: del failed")
	ctx := context.Background()
	if _, err := LeaseList(ctx, h.redis, ""); err == nil {
		t.Fatal("empty group accepted")
	}
	if _, err := LeaseRelease(ctx, h.redis, "g", "", 0); err == nil {
		t.Fatal("empty topic accepted")
	}
	if _, err := LeaseRelease(ctx, h.redis, "g", "t", -1); err == nil {
		t.Fatal("negative partition accepted")
	}
}

func TestOpenAPIExport(t *testing.T) {
	h := newHarness(t)
	_, stderr := h.expect(ExitFailure, "openapi", "export")
	mustContain(t, stderr, "error: no registry available")
	m := testRegistry(t)
	h.withRegistry(m)
	h.opts.OpenAPI = openapi.Config{Info: openapi.Info{Title: "Orders", Version: "1.2.3"}, Prefix: "/api"}
	stdout, _ := h.expect(ExitOK, "openapi", "export")
	wantLines(t, stdout, "wrote api/openapi.json (Orders 1.2.3)")
	if len(h.exports) != 1 || h.exports[0].out != "api/openapi.json" || h.exports[0].cfg.Info.Title != "Orders" || h.exports[0].cfg.Prefix != "/api" {
		t.Fatalf("exports = %+v", h.exports)
	}
	h.expect(ExitOK, "openapi", "export", "--out", "x.json", "--title", "T", "--version", "9", "--prefix", "/v2")
	if e := h.exports[1]; e.out != "x.json" || e.cfg.Info.Title != "T" || e.cfg.Info.Version != "9" || e.cfg.Prefix != "/v2" {
		t.Fatalf("overrides = %+v", e)
	}
	v := h.json("openapi", "export", "--out", "y.json")
	if v["out"] != "y.json" || v["title"] != "Orders" || v["version"] != "1.2.3" || v["prefix"] != "/api" {
		t.Fatalf("json = %v", v)
	}
	_, stderr = h.expect(ExitUsage, "openapi", "export", "--out", "")
	mustContain(t, stderr, "error: --out is required")
	if h.pgOpens+h.redisOpens != 0 {
		t.Fatal("openapi export opened a connection")
	}
	h.opts.OpenAPI = openapi.Config{}
	stdout, _ = h.expect(ExitOK, "openapi", "export")
	wantLines(t, stdout, "wrote api/openapi.json (API 0.0.0)")
	h.opts.export = func(*mediator.Mediator, openapi.Config, string) error { return errors.New("invalid document") }
	_, stderr = h.expect(ExitFailure, "openapi", "export")
	mustContain(t, stderr, "error: ctl: openapi export: invalid document")

	if _, err := OpenAPIExport(nil, openapi.Config{}, "x.json"); err == nil {
		t.Fatal("nil mediator accepted")
	}
	if _, err := OpenAPIExport(m, openapi.Config{}, ""); err == nil {
		t.Fatal("empty path accepted")
	}
	// The real generator writes a document for the test registry.
	out := filepath.Join(t.TempDir(), "openapi.json")
	res, err := OpenAPIExport(m, openapi.Config{Info: openapi.Info{Title: "Test", Version: "1"}}, out)
	if err != nil {
		t.Fatalf("real export: %v", err)
	}
	if res.Out != out || res.Title != "Test" {
		t.Fatalf("result = %+v", res)
	}
	b, err := os.ReadFile(out)
	if err != nil || !bytes.Contains(b, []byte(`"openapi"`)) {
		t.Fatalf("document not written: %v %s", err, b)
	}
}

func TestParseDuration(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"7d", 7 * 24 * time.Hour, true}, {"36h", 36 * time.Hour, true}, {"1d12h", 36 * time.Hour, true},
		{"90m", 90 * time.Minute, true}, {"0d1s", time.Second, true},
		{"", 0, false}, {"d", 0, false}, {"-1d", 0, false}, {"0s", 0, false}, {"1dx", 0, false},
		{"abc", 0, false}, {"1.5d", 0, false}, {"-5m", 0, false},
	}
	for _, c := range cases {
		got, err := parseDuration(c.in)
		if (err == nil) != c.ok || got != c.want {
			t.Errorf("parseDuration(%q) = %s, %v; want %s, ok=%v", c.in, got, err, c.want, c.ok)
		}
	}
	// A day count is parsed even when it is empty ("d..."), so the error
	// names the whole input instead of handing "d12h" to time.ParseDuration.
	messages := map[string]string{
		"":     "empty duration",
		"d":    `invalid duration "d"`,
		"d12h": `invalid duration "d12h"`,
		"1dx":  `invalid duration: time: invalid duration "x"`,
		"0s":   "duration must be positive, got 0s",
	}
	for in, want := range messages {
		if _, err := parseDuration(in); err == nil || err.Error() != want {
			t.Errorf("parseDuration(%q) error = %v, want %q", in, err, want)
		}
	}
}

func TestRenderHelpers(t *testing.T) {
	if formatAge(-time.Second) != "0s" || formatAge(1500*time.Millisecond) != "2s" {
		t.Fatal("formatAge")
	}
	if formatTime(time.Time{}) != "-" || formatTime(fixedNow) != "2026-09-26T12:00:00Z" {
		t.Fatal("formatTime")
	}
	if formatConsumers(nil) != "-" || formatConsumers(map[string]int64{"b": 1, "a": 2}) != "a=2 b=1" {
		t.Fatal("formatConsumers")
	}
	if hexBytes(nil) != "-" || hexBytes([]byte{1, 255}) != "01ff" || dash("") != "-" || dash("x") != "x" {
		t.Fatal("hexBytes/dash")
	}
	if _, err := parseDuration("2d"); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := Render(&buf, badResult{}, true); err == nil || !strings.Contains(err.Error(), "ctl: encode json") {
		t.Fatalf("marshal error not reported: %v", err)
	}
	if err := Render(failWriter{}, badResult{}, true); err == nil {
		t.Fatal("write error not reported")
	}
	if err := table(failWriter{}, []string{"a"}, [][]string{{"b"}}); err == nil {
		t.Fatal("table write error not reported")
	}
	for _, r := range []Result{
		&MigrationStatusResult{Migrations: []MigrationRow{{Version: 1, Name: "x"}}},
		&MigrateResult{Changed: []MigrationRow{{Version: 1, Name: "x"}}},
		&NamesResult{Names: []NameRow{{Kind: "command"}}},
		&OutboxStatsResult{Partitions: []OutboxPartitionRow{{Topic: "t"}}},
		&IdemShowResult{},
		&ConsumerLagResult{Rows: []ConsumerLagRow{{Group: "g"}}},
		&DLQListResult{Entries: []DLQRow{{ID: "1"}}},
		&LeaseListResult{Leases: []LeaseRow{{Topic: "t"}}},
	} {
		if err := r.WriteTable(failWriter{}); err == nil {
			t.Fatalf("%T did not report the write error", r)
		}
	}
}

// badResult cannot be marshaled.
type badResult struct{ Ch chan int }

func (badResult) WriteTable(io.Writer) error { return nil }

// silentLogger keeps go-redis from logging the dial failures the
// unreachable-server test provokes on purpose.
type silentLogger struct{}

func (silentLogger) Printf(context.Context, string, ...any) {}

func TestAdapters_Unreachable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	redis.SetLogger(silentLogger{})
	cfg, err := pgxpool.ParseConfig("postgres://u:p@127.0.0.1:1/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := NewPostgres(pool)
	if err := db.Migrate(ctx); err == nil {
		t.Fatal("Migrate reached a database")
	}
	if err := db.MigrateDown(ctx, 0); err == nil {
		t.Fatal("MigrateDown reached a database")
	}
	if _, err := db.MigrationStatus(ctx); err == nil {
		t.Fatal("MigrationStatus reached a database")
	}
	if _, err := db.OutboxStats(ctx); err == nil {
		t.Fatal("OutboxStats reached a database")
	}
	if _, err := db.OutboxReplay(ctx, fakeSink{}, "t", 0, 0); err == nil {
		t.Fatal("OutboxReplay reached a database")
	}
	if _, err := db.OutboxReshard(ctx, 2); err == nil {
		t.Fatal("OutboxReshard reached a database")
	}
	if _, err := db.InboxPurge(ctx, time.Hour); err == nil {
		t.Fatal("InboxPurge reached a database")
	}
	if _, err := db.IdemPurge(ctx); err == nil {
		t.Fatal("IdemPurge reached a database")
	}
	if _, err := db.IdemShow(ctx, "s", "k"); err == nil {
		t.Fatal("IdemShow reached a database")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 2 * time.Second})
	r := NewRedis(client, redisx.Config{Prefix: "t", PartitionsPerTopic: 1})
	if _, err := r.DLQList(ctx, "g"); err == nil {
		t.Fatal("DLQList reached a server")
	}
	if err := r.DLQRequeue(ctx, "g", "1-0"); err == nil {
		t.Fatal("DLQRequeue reached a server")
	}
	if err := r.DLQDrop(ctx, "g", "1-0"); err == nil {
		t.Fatal("DLQDrop reached a server")
	}
	if err := r.PartitionSkip(ctx, "g", "t", 0, "1-0"); err == nil {
		t.Fatal("PartitionSkip reached a server")
	}
	if _, err := r.LeaseList(ctx, "g"); err == nil {
		t.Fatal("LeaseList reached a server")
	}
	if err := r.LeaseRelease(ctx, "g", "t", 0); err == nil {
		t.Fatal("LeaseRelease reached a server")
	}
	if _, err := r.ConsumerLag(ctx, []string{"g"}, []string{"t"}); err == nil {
		t.Fatal("ConsumerLag reached a server")
	}
	if r.Sink() == nil {
		t.Fatal("nil sink")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := openPostgres(ctx, "postgres://u:p@127.0.0.1:1/db?sslmode=disable&connect_timeout=2"); err == nil {
		t.Fatal("openPostgres reached a database")
	}
	if _, err := openRedis(ctx, redisx.Config{Addr: "127.0.0.1:1"}); err == nil {
		t.Fatal("openRedis reached a server")
	}
}
