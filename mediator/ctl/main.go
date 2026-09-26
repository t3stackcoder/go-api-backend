package ctl

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
)

// Exit codes of Main.
const (
	ExitOK          = 0
	ExitFailure     = 1
	ExitUsage       = 2
	ExitInterrupted = 130
)

// usageError is a problem with the arguments: reported with the command's
// usage and ExitUsage.
type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

func usagef(format string, args ...any) error {
	return &usageError{msg: fmt.Sprintf(format, args...)}
}

// runFunc executes a parsed command.
type runFunc func(ctx context.Context, a *app) (Result, error)

// command is one entry of the dispatch table. setup registers the command's
// flags on fs and returns the function that reads them.
type command struct {
	name    string // "migrate up", "names", ...
	summary string
	setup   func(fs *flag.FlagSet) runFunc
}

// app is the state of one Main invocation: options with defaults applied and
// the connections opened so far.
type app struct {
	opts   Options
	envErr error
	pg     Postgres
	redis  Redis
	reg    *mediator.Mediator
}

func newApp(opts Options) *app {
	o, err := opts.withDefaults()
	return &app{opts: o, envErr: err}
}

// postgres opens the Postgres connection once per invocation.
func (a *app) postgres(ctx context.Context) (Postgres, error) {
	if a.pg != nil {
		return a.pg, nil
	}
	if a.opts.PGURL == "" {
		return nil, fmt.Errorf("%s is not set (or Options.PGURL)", EnvPGURL)
	}
	db, err := a.opts.openPostgres(ctx, a.opts.PGURL)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	a.pg = db
	return db, nil
}

// redisConn opens the Redis connection once per invocation.
func (a *app) redisConn(ctx context.Context) (Redis, error) {
	if a.redis != nil {
		return a.redis, nil
	}
	if a.envErr != nil {
		return nil, a.envErr
	}
	r, err := a.opts.openRedis(ctx, a.opts.Redis)
	if err != nil {
		return nil, fmt.Errorf("connect to redis: %w", err)
	}
	a.redis = r
	return r, nil
}

// registry builds the mediator once per invocation.
func (a *app) registry() (*mediator.Mediator, error) {
	if a.reg != nil {
		return a.reg, nil
	}
	if a.opts.Registry == nil {
		return nil, errors.New("no registry available (Options.Registry is nil)")
	}
	m, err := a.opts.Registry()
	if err != nil {
		return nil, fmt.Errorf("registry: %w", err)
	}
	if m == nil {
		return nil, errors.New("registry returned a nil mediator")
	}
	a.reg = m
	return m, nil
}

// close releases the connections opened during the invocation.
func (a *app) close() {
	if a.pg != nil {
		_ = a.pg.Close()
	}
	if a.redis != nil {
		_ = a.redis.Close()
	}
}

func (a *app) warnf(format string, args ...any) {
	fmt.Fprintf(a.opts.Stderr, "warning: "+format+"\n", args...)
}

// commands is the dispatch table in the order of spec 9.3.
var commands = []command{
	{name: "migrate up", summary: "apply every pending migration", setup: func(*flag.FlagSet) runFunc {
		return func(ctx context.Context, a *app) (Result, error) {
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			return MigrateUp(ctx, db)
		}
	}},
	{name: "migrate status", summary: "list migrations and whether they are applied", setup: func(*flag.FlagSet) runFunc {
		return func(ctx context.Context, a *app) (Result, error) {
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			return MigrateStatus(ctx, db)
		}
	}},
	{name: "migrate down", summary: "revert migrations above --to (tests only)", setup: func(fs *flag.FlagSet) runFunc {
		to := fs.Int("to", -1, "version to revert down to (0 reverts everything)")
		return func(ctx context.Context, a *app) (Result, error) {
			if *to < 0 {
				return nil, usagef("--to is required")
			}
			a.warnf("migrate down exists for tests; migrations are append-only once merged (spec 6.7)")
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			return MigrateDown(ctx, db, *to)
		}
	}},
	{name: "names", summary: "print every persisted name the registry derives", setup: func(*flag.FlagSet) runFunc {
		return func(_ context.Context, a *app) (Result, error) {
			m, err := a.registry()
			if err != nil {
				return nil, err
			}
			return Names(m), nil
		}
	}},
	{name: "outbox stats", summary: "unpublished count and oldest age per partition", setup: func(*flag.FlagSet) runFunc {
		return func(ctx context.Context, a *app) (Result, error) {
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			return OutboxStats(ctx, db)
		}
	}},
	{name: "outbox replay", summary: "re-add published rows of a partition to Redis", setup: func(fs *flag.FlagSet) runFunc {
		topic := fs.String("topic", "", "topic (required)")
		partition := fs.Int("partition", -1, "partition (required)")
		fromID := fs.Int64("from-id", 0, "first outbox id to replay (default: all published rows)")
		return func(ctx context.Context, a *app) (Result, error) {
			if err := require(*topic != "", "--topic", *partition >= 0, "--partition"); err != nil {
				return nil, err
			}
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			r, err := a.redisConn(ctx)
			if err != nil {
				return nil, err
			}
			return OutboxReplay(ctx, db, r.Sink(), *topic, *partition, *fromID)
		}
	}},
	{name: "outbox reshard", summary: "recompute partitions for a new partition count (consumers stopped)", setup: func(fs *flag.FlagSet) runFunc {
		partitions := fs.Int("partitions", 0, "new partition count P (required)")
		yes := fs.Bool("yes", false, "confirm that every consumer and relay is stopped")
		return func(ctx context.Context, a *app) (Result, error) {
			if *partitions <= 0 {
				return nil, usagef("--partitions must be a positive count")
			}
			if !*yes {
				return nil, usagef("refusing to reshard without --yes: %s", ReshardWarning)
			}
			a.warnf("%s (spec 6.4)", ReshardWarning)
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			return OutboxReshard(ctx, db, *partitions)
		}
	}},
	{name: "inbox purge", summary: "delete inbox rows processed longer ago than --older-than", setup: func(fs *flag.FlagSet) runFunc {
		olderThan := fs.String("older-than", "7d", "retention, for example 7d, 36h, 1d12h")
		return func(ctx context.Context, a *app) (Result, error) {
			d, err := parseDuration(*olderThan)
			if err != nil {
				return nil, usagef("--older-than: %v", err)
			}
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			return InboxPurge(ctx, db, d)
		}
	}},
	{name: "idem purge", summary: "delete expired idempotency rows", setup: func(*flag.FlagSet) runFunc {
		return func(ctx context.Context, a *app) (Result, error) {
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			return IdemPurge(ctx, db)
		}
	}},
	{name: "idem show", summary: "show one idempotency row", setup: func(fs *flag.FlagSet) runFunc {
		scope := fs.String("scope", "", "idempotency scope, for example CreateOrder or tenant:CreateOrder (required)")
		key := fs.String("key", "", "idempotency key (required)")
		return func(ctx context.Context, a *app) (Result, error) {
			if err := require(*scope != "", "--scope", *key != "", "--key"); err != nil {
				return nil, err
			}
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			return IdemShow(ctx, db, *scope, *key, a.opts.Now())
		}
	}},
	{name: "consumer lag", summary: "pending entries and outbox backlog per group, topic, and partition", setup: func(fs *flag.FlagSet) runFunc {
		group := fs.String("group", "", "consumer group (default: every group of the registry)")
		topic := fs.String("topic", "", "topic (default: every topic the selected groups consume)")
		return func(ctx context.Context, a *app) (Result, error) {
			groups, topics, err := a.lagTargets(*group, *topic)
			if err != nil {
				return nil, err
			}
			r, err := a.redisConn(ctx)
			if err != nil {
				return nil, err
			}
			db, err := a.postgres(ctx)
			if err != nil {
				return nil, err
			}
			return ConsumerLag(ctx, r, db, groups, topics)
		}
	}},
	{name: "dlq list", summary: "list the dead letters of a group", setup: func(fs *flag.FlagSet) runFunc {
		group := fs.String("group", "", "consumer group (required)")
		return func(ctx context.Context, a *app) (Result, error) {
			if err := require(*group != "", "--group"); err != nil {
				return nil, err
			}
			r, err := a.redisConn(ctx)
			if err != nil {
				return nil, err
			}
			return DLQList(ctx, r, *group)
		}
	}},
	{name: "dlq requeue", summary: "re-add a dead letter to its partition stream", setup: func(fs *flag.FlagSet) runFunc {
		group := fs.String("group", "", "consumer group (required)")
		id := fs.String("id", "", "dead-letter entry id from dlq list (required)")
		return func(ctx context.Context, a *app) (Result, error) {
			if err := require(*group != "", "--group", *id != "", "--id"); err != nil {
				return nil, err
			}
			r, err := a.redisConn(ctx)
			if err != nil {
				return nil, err
			}
			return DLQRequeue(ctx, r, *group, *id)
		}
	}},
	{name: "dlq drop", summary: "delete a dead letter", setup: func(fs *flag.FlagSet) runFunc {
		group := fs.String("group", "", "consumer group (required)")
		id := fs.String("id", "", "dead-letter entry id from dlq list (required)")
		return func(ctx context.Context, a *app) (Result, error) {
			if err := require(*group != "", "--group", *id != "", "--id"); err != nil {
				return nil, err
			}
			r, err := a.redisConn(ctx)
			if err != nil {
				return nil, err
			}
			return DLQDrop(ctx, r, *group, *id)
		}
	}},
	{name: "dlq skip", summary: "acknowledge a poison entry so a halted partition resumes", setup: func(fs *flag.FlagSet) runFunc {
		group := fs.String("group", "", "consumer group (required)")
		topic := fs.String("topic", "", "topic (required)")
		partition := fs.Int("partition", -1, "partition (required)")
		id := fs.String("id", "", "stream entry id (required)")
		return func(ctx context.Context, a *app) (Result, error) {
			if err := require(*group != "", "--group", *topic != "", "--topic", *partition >= 0, "--partition", *id != "", "--id"); err != nil {
				return nil, err
			}
			r, err := a.redisConn(ctx)
			if err != nil {
				return nil, err
			}
			return DLQSkip(ctx, r, *group, *topic, *partition, *id)
		}
	}},
	{name: "lease list", summary: "list the partition leases of a group", setup: func(fs *flag.FlagSet) runFunc {
		group := fs.String("group", "", "consumer group (required)")
		return func(ctx context.Context, a *app) (Result, error) {
			if err := require(*group != "", "--group"); err != nil {
				return nil, err
			}
			r, err := a.redisConn(ctx)
			if err != nil {
				return nil, err
			}
			return LeaseList(ctx, r, *group)
		}
	}},
	{name: "lease release", summary: "delete a lease to force a handover", setup: func(fs *flag.FlagSet) runFunc {
		group := fs.String("group", "", "consumer group (required)")
		topic := fs.String("topic", "", "topic (required)")
		partition := fs.Int("partition", -1, "partition (required)")
		return func(ctx context.Context, a *app) (Result, error) {
			if err := require(*group != "", "--group", *topic != "", "--topic", *partition >= 0, "--partition"); err != nil {
				return nil, err
			}
			r, err := a.redisConn(ctx)
			if err != nil {
				return nil, err
			}
			return LeaseRelease(ctx, r, *group, *topic, *partition)
		}
	}},
	{name: "openapi export", summary: "generate the OpenAPI document of the registry", setup: func(fs *flag.FlagSet) runFunc {
		out := fs.String("out", "api/openapi.json", "output file")
		title := fs.String("title", "", "document title (default: Options.OpenAPI.Info.Title)")
		version := fs.String("version", "", "document version (default: Options.OpenAPI.Info.Version)")
		prefix := fs.String("prefix", "", "HTTP mount prefix (default: Options.OpenAPI.Prefix)")
		return func(_ context.Context, a *app) (Result, error) {
			if *out == "" {
				return nil, usagef("--out is required")
			}
			m, err := a.registry()
			if err != nil {
				return nil, err
			}
			cfg := a.opts.OpenAPI
			if *title != "" {
				cfg.Info.Title = *title
			}
			if *version != "" {
				cfg.Info.Version = *version
			}
			if *prefix != "" {
				cfg.Prefix = *prefix
			}
			return openAPIExport(m, cfg, *out, a.opts.export)
		}
	}},
}

// require returns a usage error naming the first flag whose condition is
// false. Arguments alternate condition, flag name.
func require(pairs ...any) error {
	for i := 0; i+1 < len(pairs); i += 2 {
		if !pairs[i].(bool) {
			return usagef("%s is required", pairs[i+1])
		}
	}
	return nil
}

// lagTargets resolves the groups and topics of consumer lag: the flags when
// both are given, otherwise the registry fills in the missing side.
func (a *app) lagTargets(group, topic string) (groups, topics []string, err error) {
	if group != "" && topic != "" {
		return []string{group}, []string{topic}, nil
	}
	m, err := a.registry()
	if err != nil {
		return nil, nil, usagef("--group and --topic are both required when no registry is available: %v", err)
	}
	seenGroup, seenTopic := map[string]bool{}, map[string]bool{}
	for _, c := range m.ConsumerRegistrations() {
		if (group != "" && c.Group != group) || (topic != "" && c.Topic != topic) {
			continue
		}
		if !seenGroup[c.Group] {
			seenGroup[c.Group] = true
			groups = append(groups, c.Group)
		}
		if !seenTopic[c.Topic] {
			seenTopic[c.Topic] = true
			topics = append(topics, c.Topic)
		}
	}
	if len(groups) == 0 {
		return nil, nil, usagef("no registered consumer matches group %q and topic %q; pass both --group and --topic", group, topic)
	}
	return groups, topics, nil
}

// parseDuration parses a Go duration with an optional leading day count:
// "7d", "36h", "1d12h".
func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, errors.New("empty duration")
	}
	var days int64
	if i := strings.IndexByte(s, 'd'); i >= 0 {
		n, err := strconv.ParseInt(s[:i], 10, 64)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		days, s = n, s[i+1:]
	}
	var rest time.Duration
	if s != "" {
		var err error
		if rest, err = time.ParseDuration(s); err != nil {
			return 0, fmt.Errorf("invalid duration: %w", err)
		}
	}
	d := time.Duration(days)*24*time.Hour + rest
	if d <= 0 {
		return 0, fmt.Errorf("duration must be positive, got %s", d)
	}
	return d, nil
}

// lookup finds the command named by the leading arguments and returns the
// remaining arguments.
func lookup(args []string) (*command, []string, bool) {
	for i := range commands {
		c := &commands[i]
		words := strings.Fields(c.name)
		if len(args) < len(words) {
			continue
		}
		if strings.Join(args[:len(words)], " ") == c.name {
			return c, args[len(words):], true
		}
	}
	return nil, nil, false
}

// usage prints the command list.
func usage(w io.Writer) {
	fmt.Fprintln(w, "usage: mediatorctl <command> [flags]")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "commands:")
	width := 0
	for _, c := range commands {
		width = max(width, len(c.name))
	}
	for _, c := range commands {
		fmt.Fprintf(w, "  %-*s  %s\n", width, c.name, c.summary)
	}
	fmt.Fprintln(w)
	fmt.Fprintln(w, "every command accepts --json for machine-readable output and -h for its flags")
	fmt.Fprintf(w, "environment: %s, %s, %s, %s, %s, %s\n", EnvPGURL, EnvRedisAddr, EnvRedisUsername, EnvRedisPassword, EnvRedisDB, EnvRedisPrefix)
}

// commandUsage prints the usage of one command with its flags.
func commandUsage(w io.Writer, c *command, fs *flag.FlagSet) {
	fmt.Fprintf(w, "usage: mediatorctl %s [flags]\n\n%s\n\nflags:\n", c.name, c.summary)
	fs.SetOutput(w)
	fs.PrintDefaults()
}

// Main parses args, runs the command, renders its result to opts.Stdout, and
// returns the exit code: ExitOK, ExitFailure with "error: <msg>" on Stderr,
// ExitUsage after a usage message, or ExitInterrupted when ctx was canceled.
func Main(ctx context.Context, args []string, opts Options) int {
	a := newApp(opts)
	defer a.close()
	stdout, stderr := a.opts.Stdout, a.opts.Stderr
	if len(args) == 0 {
		usage(stderr)
		return ExitUsage
	}
	switch args[0] {
	case "help", "-h", "-help", "--help":
		usage(stdout)
		return ExitOK
	}
	c, rest, ok := lookup(args)
	if !ok {
		fmt.Fprintf(stderr, "error: unknown command %q\n\n", strings.Join(args[:min(2, len(args))], " "))
		usage(stderr)
		return ExitUsage
	}
	fs := flag.NewFlagSet(c.name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Usage = func() {}
	asJSON := fs.Bool("json", false, "print the result as JSON")
	run := c.setup(fs)
	if err := fs.Parse(rest); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			commandUsage(stdout, c, fs)
			return ExitOK
		}
		fmt.Fprintf(stderr, "error: %v\n\n", err)
		commandUsage(stderr, c, fs)
		return ExitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "error: unexpected argument %q\n\n", fs.Arg(0))
		commandUsage(stderr, c, fs)
		return ExitUsage
	}
	res, err := run(ctx, a)
	if err != nil {
		return fail(ctx, stderr, c, fs, err)
	}
	if err := Render(stdout, res, *asJSON); err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return ExitFailure
	}
	return ExitOK
}

// fail reports err and picks the exit code.
func fail(ctx context.Context, stderr io.Writer, c *command, fs *flag.FlagSet, err error) int {
	var ue *usageError
	if errors.As(err, &ue) {
		fmt.Fprintf(stderr, "error: %v\n\n", err)
		commandUsage(stderr, c, fs)
		return ExitUsage
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		fmt.Fprintf(stderr, "interrupted: %v\n", err)
		return ExitInterrupted
	}
	fmt.Fprintf(stderr, "error: %v\n", err)
	return ExitFailure
}
