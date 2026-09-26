package pg_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/migrations"
)

type retryable struct{}

func (retryable) Error() string     { return "retryable" }
func (retryable) SafeToRetry() bool { return true }

func TestErrorClassifiers(t *testing.T) {
	pe := func(code string) error { return &pgconn.PgError{Code: code} }
	if pg.SQLState(pe("40001")) != "40001" || pg.SQLState(errors.New("x")) != "" {
		t.Fatal("SQLState")
	}
	if !pg.IsLockTimeout(pe("55P03")) || !pg.IsLockTimeout(pg.ErrLockTimeout) || pg.IsLockTimeout(pe("40001")) {
		t.Fatal("IsLockTimeout")
	}
	if !pg.IsSerializationFailure(pe("40001")) || !pg.IsDeadlock(pe("40P01")) || !pg.IsUniqueViolation(pe("23505")) {
		t.Fatal("code predicates")
	}
	if !pg.IsReadOnly(pe("25006")) || !pg.IsReadOnly(pg.ErrReadOnly) || pg.IsReadOnly(pe("23505")) {
		t.Fatal("IsReadOnly")
	}
	if !pg.IsAmbiguous(mediator.MarkAmbiguous(errors.New("x"))) || pg.IsAmbiguous(errors.New("x")) {
		t.Fatal("IsAmbiguous")
	}
	for _, code := range []string{"40001", "40P01", "57P01", "57P02", "57P03", "08000", "08006", "08P01"} {
		if !pg.IsTransientPg(pe(code)) || !mediator.IsTransient(pe(code)) {
			t.Errorf("%s must be transient", code)
		}
	}
	for _, code := range []string{"23505", "55P03", "25006", "42P01", "0800"} {
		if pg.IsTransientPg(pe(code)) {
			t.Errorf("%s must not be transient", code)
		}
	}
	if !pg.IsTransientPg(retryable{}) || pg.IsTransientPg(errors.New("plain")) {
		t.Fatal("SafeToRetry")
	}
}

func TestMigrationsEmbedded(t *testing.T) {
	ms, err := pg.LoadMigrations(migrations.FS)
	if err != nil || len(ms) < 1 {
		t.Fatalf("%v %d", err, len(ms))
	}
	first := ms[0]
	if first.Version() != 1 || first.Name() != "init" {
		t.Fatalf("first migration %s/%d", first.Name(), first.Version())
	}
	for _, table := range []string{"mediator_outbox", "mediator_stream_seq", "mediator_inbox", "mediator_idempotency", "mediator_relay_cursor", "mediator_schema_version", "mediator_fencing_seq"} {
		if !strings.Contains(first.Up(), table) || !strings.Contains(first.Down(), table) {
			t.Errorf("%s missing from up or down", table)
		}
	}
	if strings.Contains(first.Up(), "DROP TABLE") || !strings.HasPrefix(first.Down(), "DROP SEQUENCE") {
		t.Fatal("down section not split at the marker")
	}
	for i := 1; i < len(ms); i++ {
		if ms[i].Version() <= ms[i-1].Version() {
			t.Fatal("migrations must be sorted")
		}
	}
	up, down := pg.SplitMigration("CREATE TABLE a (x int);\n")
	if up != "CREATE TABLE a (x int);" || down != "" {
		t.Fatalf("split without marker: %q %q", up, down)
	}
	up, down = pg.SplitMigration("A;\n--   DOWN  \nB;")
	if up != "A;" || down != "B;" {
		t.Fatalf("split with marker: %q %q", up, down)
	}
	fsys := fstest.MapFS{
		"0002_b.sql":   {Data: []byte("b")},
		"0001_a.sql":   {Data: []byte("a")},
		"README.md":    {Data: []byte("ignored")},
		"0003_x.txt":   {Data: []byte("ignored")},
		"sub/0004.sql": {Data: []byte("ignored")},
	}
	ms, err = pg.LoadMigrations(fsys)
	if err != nil || len(ms) != 2 || ms[0].Name() != "a" || ms[1].Name() != "b" {
		t.Fatalf("%v %+v", err, ms)
	}
	fsys["0001_dup.sql"] = &fstest.MapFile{Data: []byte("dup")}
	if _, err := pg.LoadMigrations(fsys); err == nil {
		t.Fatal("duplicate versions must be rejected")
	}
}

func TestOutboxHeadersRoundTrip(t *testing.T) {
	env := &mediator.Envelope{OccurredAt: time.Unix(1, 0).UTC(), CorrelationID: "c", CausationID: "k", TraceParent: "00-t", Headers: map[string]string{"a": "b"}}
	raw, err := pg.EncodeOutboxHeaders(env)
	if err != nil {
		t.Fatal(err)
	}
	var back mediator.Envelope
	if err := pg.DecodeOutboxHeaders(raw, &back); err != nil {
		t.Fatal(err)
	}
	if !back.OccurredAt.Equal(env.OccurredAt) || back.CorrelationID != "c" || back.CausationID != "k" || back.TraceParent != "00-t" || back.Headers["a"] != "b" {
		t.Fatalf("round trip: %+v", back)
	}
	if err := pg.DecodeOutboxHeaders(nil, &back); err != nil || back.CorrelationID != "" {
		t.Fatalf("empty headers must reset: %v %+v", err, back)
	}
	if err := pg.DecodeOutboxHeaders([]byte(`{`), &back); err == nil {
		t.Fatal("invalid JSON must fail")
	}
}

func TestConfigDefaultsAndPool(t *testing.T) {
	j := pg.JanitorConfig{}.WithDefaults()
	if j.Interval != time.Minute || j.OutboxRetention != 7*24*time.Hour || j.InboxRetention != j.OutboxRetention || j.BatchSize != 5000 || j.Partitions != 1 || j.Logger == nil || j.Clock == nil {
		t.Fatalf("janitor defaults: %+v", j)
	}
	u := pg.UnitOfWorkConfig{}.WithDefaults()
	if u.DefaultLockTimeout != 5*time.Second || u.RollbackTimeout != 5*time.Second || u.Logger == nil {
		t.Fatalf("uow defaults: %+v", u)
	}
	if err := pg.ValidateRetention(time.Hour, 2*time.Hour); err == nil {
		t.Fatal("inbox shorter than outbox must be rejected")
	}
	if err := pg.ValidateRetention(2*time.Hour, 2*time.Hour); err != nil {
		t.Fatal(err)
	}
	pc, err := pgxpool.ParseConfig("postgres://u:p@localhost:5432/db")
	if err != nil {
		t.Fatal(err)
	}
	pg.ApplyPoolConfig(pc, pg.PoolConfig{MaxConns: 9, MinConns: 2, MaxConnLifetime: time.Hour, MaxConnIdleTime: time.Minute, HealthCheckPeriod: 3 * time.Second, ConnectTimeout: 250 * time.Millisecond, ApplicationName: "app", SearchPath: "s"})
	if pc.MaxConns != 9 || pc.MinConns != 2 || pc.MaxConnLifetime != time.Hour || pc.MaxConnIdleTime != time.Minute || pc.HealthCheckPeriod != 3*time.Second || pc.ConnConfig.ConnectTimeout != 250*time.Millisecond || pc.ConnConfig.RuntimeParams["application_name"] != "app" || pc.ConnConfig.RuntimeParams["search_path"] != "s" {
		t.Fatalf("pool config: %+v %+v", pc, pc.ConnConfig.RuntimeParams)
	}
	pc, _ = pgxpool.ParseConfig("postgres://u:p@localhost:5432/db")
	pg.ApplyPoolConfig(pc, pg.PoolConfig{})
	if pc.HealthCheckPeriod != 30*time.Second || pc.ConnConfig.ConnectTimeout != 5*time.Second || pc.ConnConfig.RuntimeParams["application_name"] != "mediator" {
		t.Fatalf("pool defaults: %+v", pc.ConnConfig.RuntimeParams)
	}
	if _, err := pg.NewPool(context.Background(), "://bad", pg.PoolConfig{}); err == nil {
		t.Fatal("bad url must fail")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := pg.NewPool(ctx, "postgres://u:p@127.0.0.1:1/db", pg.PoolConfig{ConnectTimeout: 200 * time.Millisecond}); err == nil {
		t.Fatal("unreachable database must fail the startup ping")
	}
	s := pg.NewStore(nil, pg.StoreConfig{})
	if s.Partitions() != 1 || s.Pool() != nil {
		t.Fatal("store defaults")
	}
	if pg.NewStore(nil, pg.StoreConfig{Partitions: 8}).Partitions() != 8 {
		t.Fatal("store partitions")
	}
}
