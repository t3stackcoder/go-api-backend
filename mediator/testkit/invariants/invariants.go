// Package invariants implements the checks I1 to I7, the liveness check L1,
// the fencing monotonicity check, and the dead-letter check shared by the
// fault-sweep and chaos tiers (spec 11.4 and 11.6). Every check reads
// Postgres and Redis directly, never through the nodes, and returns a list
// of Violation values rather than failing a test, so the sweep can attach
// the fault that produced them.
//
// The checks assume the workload package's tables and naming (wl_ tables,
// the cmd header on durable events, the wl_executions counter). They are
// point-in-time checks except L1, which polls until the bound; run L1 first
// so the others see a quiescent system.
package invariants

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/history"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit/workload"
)

// Violation is one failed check; it is the history package's type so that
// offline checkers and database checks report the same way.
type Violation = history.Violation

// Report collects violations; see history.Report.
type Report = history.Report

// Violation IDs.
const (
	IDI1      = "I1"
	IDI2      = "I2"
	IDI3      = "I3"
	IDI4      = "I4"
	IDI5      = "I5"
	IDI6      = "I6"
	IDI7      = "I7"
	IDL1      = "L1"
	IDFencing = history.IDFencing
	IDDLQ     = "DLQ"
)

// DefaultBound is RecoveryBound (G15).
const DefaultBound = 60 * time.Second

// Env is what the checks need: the database, Redis, the key layout, and the
// consumer topology of the run.
type Env struct {
	Pool *pgxpool.Pool
	// Redis may be nil, in which case the stream, pending, and dead-letter
	// parts of the checks are skipped.
	Redis *redis.Client
	// Cfg supplies the key prefix and the default partition count.
	Cfg redisx.Config
	// Groups are the consumer groups of the run. Default workload.DefaultGroups.
	Groups []string
	// Topics are the topics the groups consume. Default workload.TopicBumped.
	Topics []string
	// Partitions is P. Default Cfg.PartitionsPerTopic.
	Partitions int
	// Bound is RecoveryBound. Default 60 s.
	Bound time.Duration
	// Poll is the polling interval of L1. Default 250 ms.
	Poll time.Duration
}

func (e Env) bound() time.Duration {
	if e.Bound > 0 {
		return e.Bound
	}
	return DefaultBound
}

func (e Env) poll() time.Duration {
	if e.Poll > 0 {
		return e.Poll
	}
	return 250 * time.Millisecond
}

func (e Env) partitions() int {
	if e.Partitions > 0 {
		return e.Partitions
	}
	return e.Cfg.WithDefaults().PartitionsPerTopic
}

func (e Env) groups() []string {
	if e.Groups != nil {
		return e.Groups
	}
	return workload.DefaultGroups
}

func (e Env) topics() []string {
	if e.Topics != nil {
		return e.Topics
	}
	return []string{workload.TopicBumped}
}

func (e Env) keys() redisx.Keys { return e.Cfg.WithDefaults().Keys() }

// maxDetails caps the number of example items carried in one violation.
const maxDetails = 20

func violation(id, msg string, details map[string]any) Violation {
	return Violation{ID: id, Msg: msg, Details: details}
}

// query runs sql and scans every row with scan.
func query[T any](ctx context.Context, pool *pgxpool.Pool, scan func(pgx.CollectableRow) (T, error), sql string, args ...any) ([]T, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("invariants: %w", err)
	}
	defer rows.Close()
	out, err := pgx.CollectRows(rows, scan)
	if err != nil {
		return nil, fmt.Errorf("invariants: %w", err)
	}
	return out, nil
}

func queryInt(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) (int64, error) {
	var n int64
	if err := pool.QueryRow(ctx, sql, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("invariants: %w", err)
	}
	return n, nil
}

// streamIndex is the set of event IDs present in the partition streams of
// the run, per stream and in total.
type streamIndex struct {
	all      map[uuid.UUID]bool
	byStream map[string]map[uuid.UUID]bool
}

// scanStreams reads every partition stream of every topic with XRANGE in
// batches and indexes the event IDs.
func scanStreams(ctx context.Context, env Env) (*streamIndex, error) {
	idx := &streamIndex{all: map[uuid.UUID]bool{}, byStream: map[string]map[uuid.UUID]bool{}}
	keys := env.keys()
	for _, topic := range env.topics() {
		for p := 0; p < env.partitions(); p++ {
			name := keys.Stream(topic, p)
			ids, err := scanStream(ctx, env.Redis, name)
			if err != nil {
				return nil, err
			}
			idx.byStream[name] = ids
			for id := range ids {
				idx.all[id] = true
			}
		}
	}
	return idx, nil
}

func scanStream(ctx context.Context, client *redis.Client, name string) (map[uuid.UUID]bool, error) {
	ids := map[uuid.UUID]bool{}
	start := "-"
	for {
		msgs, err := client.XRangeN(ctx, name, start, "+", 1000).Result()
		if err != nil {
			return nil, fmt.Errorf("invariants: xrange %s: %w", name, err)
		}
		for _, m := range msgs {
			if raw, ok := m.Values[redisx.FieldID]; ok {
				if id, err := uuid.Parse(fmt.Sprint(raw)); err == nil {
					ids[id] = true
				}
			}
		}
		if len(msgs) < 1000 {
			return ids, nil
		}
		start = "(" + msgs[len(msgs)-1].ID
	}
}

// dlqIDs returns the event IDs dead-lettered for the group.
func dlqIDs(ctx context.Context, env Env, group string) (map[uuid.UUID]bool, []redisx.DLQEntry, error) {
	ids := map[uuid.UUID]bool{}
	if env.Redis == nil {
		return ids, nil, nil
	}
	entries, err := redisx.DLQList(ctx, env.Redis, env.Cfg, group)
	if err != nil {
		return nil, nil, fmt.Errorf("invariants: %w", err)
	}
	for _, e := range entries {
		if e.DecodeErr == nil {
			ids[e.Envelope.ID] = true
		}
	}
	return ids, entries, nil
}

// sortedIDs renders at most maxDetails IDs for a violation.
func sortedIDs(ids map[uuid.UUID]bool) []string {
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id.String())
	}
	sort.Strings(out)
	if len(out) > maxDetails {
		out = out[:maxDetails]
	}
	return out
}

// partitionOf is the partition of a stream key under the run's P.
func (e Env) partitionOf(key string) int { return mediator.Partition(key, e.partitions()) }

// isMissing reports whether a Redis error means the stream or group does
// not exist, which the checks treat as empty.
func isMissing(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "NOGROUP") || strings.Contains(msg, "no such key")
}
