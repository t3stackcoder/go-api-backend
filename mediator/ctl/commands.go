package ctl

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/openapi"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// Every command is an exported function taking typed arguments and returning
// a typed Result, so tests and other programs use them without going through
// argument parsing. Main is the only caller that renders them.

// MigrateStatus lists every embedded migration with whether and when it was
// applied (migrate status).
func MigrateStatus(ctx context.Context, db Postgres) (*MigrationStatusResult, error) {
	infos, err := db.MigrationStatus(ctx)
	if err != nil {
		return nil, err
	}
	res := &MigrationStatusResult{Migrations: make([]MigrationRow, 0, len(infos))}
	for _, m := range infos {
		res.Migrations = append(res.Migrations, migrationRow(m))
		if m.Applied {
			res.Current = max(res.Current, m.Version)
		}
	}
	return res, nil
}

func migrationRow(m pg.MigrationInfo) MigrationRow {
	return MigrationRow{Version: m.Version, Name: m.Name, Applied: m.Applied, AppliedAt: m.AppliedAt}
}

// MigrateUp applies every pending migration (migrate up) and reports which
// ones were applied.
func MigrateUp(ctx context.Context, db Postgres) (*MigrateResult, error) {
	return migrate(ctx, db, "up", db.Migrate)
}

// MigrateDown reverts applied migrations above toVersion (migrate down). It
// exists for tests; migrations are append-only once merged (spec 6.7).
func MigrateDown(ctx context.Context, db Postgres, toVersion int) (*MigrateResult, error) {
	if toVersion < 0 {
		return nil, errors.New("ctl: migrate down: target version must not be negative")
	}
	return migrate(ctx, db, "down", func(ctx context.Context) error { return db.MigrateDown(ctx, toVersion) })
}

// migrate runs step between two status snapshots and reports the difference.
func migrate(ctx context.Context, db Postgres, direction string, step func(context.Context) error) (*MigrateResult, error) {
	before, err := MigrateStatus(ctx, db)
	if err != nil {
		return nil, err
	}
	if err := step(ctx); err != nil {
		return nil, err
	}
	after, err := MigrateStatus(ctx, db)
	if err != nil {
		return nil, err
	}
	res := &MigrateResult{Direction: direction, Before: before.Current, Current: after.Current, Changed: []MigrationRow{}}
	was := map[int]bool{}
	for _, m := range before.Migrations {
		was[m.Version] = m.Applied
	}
	for _, m := range after.Migrations {
		if m.Applied != was[m.Version] {
			res.Changed = append(res.Changed, m)
		}
	}
	if direction == "down" {
		sort.Slice(res.Changed, func(i, j int) bool { return res.Changed[i].Version > res.Changed[j].Version })
	}
	return res, nil
}

// Names lists every persisted name the registry derives (names).
func Names(m *mediator.Mediator) *NamesResult {
	entries := m.Names()
	res := &NamesResult{Names: make([]NameRow, 0, len(entries))}
	for _, e := range entries {
		res.Names = append(res.Names, NameRow{Kind: e.Kind.String(), Name: e.Name, GoType: e.GoType, Group: e.Group, Topic: e.Topic, Pinned: e.Pinned})
	}
	return res
}

// OutboxStats reports the unpublished count and oldest age per partition
// (outbox stats).
func OutboxStats(ctx context.Context, db Postgres) (*OutboxStatsResult, error) {
	stats, err := db.OutboxStats(ctx)
	if err != nil {
		return nil, err
	}
	res := &OutboxStatsResult{Partitions: make([]OutboxPartitionRow, 0, len(stats))}
	for _, s := range stats {
		res.Partitions = append(res.Partitions, OutboxPartitionRow{Topic: s.Topic, Partition: s.Partition, Unpublished: s.Unpublished, OldestAgeSeconds: seconds(s.OldestAge)})
	}
	return res, nil
}

// OutboxReplay re-appends the published rows of a partition with id >= fromID
// to the sink (outbox replay). Consumers deduplicate through the inbox.
func OutboxReplay(ctx context.Context, db Postgres, sink pg.StreamSink, topic string, partition int, fromID int64) (*OutboxReplayResult, error) {
	if topic == "" {
		return nil, errors.New("ctl: outbox replay: topic is required")
	}
	if partition < 0 {
		return nil, errors.New("ctl: outbox replay: partition must not be negative")
	}
	n, err := db.OutboxReplay(ctx, sink, topic, partition, fromID)
	if err != nil {
		return nil, err
	}
	return &OutboxReplayResult{Topic: topic, Partition: partition, FromID: fromID, Replayed: n}, nil
}

// ReshardWarning is printed before outbox reshard runs and quoted when --yes
// is missing (spec 6.4).
const ReshardWarning = "outbox reshard rewrites the partition of every outbox row and resets the relay cursors; " +
	"every consumer and relay must be stopped while it runs and restarted with the new partition count afterwards"

// OutboxReshard recomputes the partition of every outbox row for a new P and
// resets the relay cursors (outbox reshard). Consumers and relays must be
// stopped while it runs.
func OutboxReshard(ctx context.Context, db Postgres, partitions int) (*OutboxReshardResult, error) {
	if partitions <= 0 {
		return nil, errors.New("ctl: outbox reshard: partitions must be positive")
	}
	moved, err := db.OutboxReshard(ctx, partitions)
	if err != nil {
		return nil, err
	}
	return &OutboxReshardResult{Partitions: partitions, Moved: moved}, nil
}

// InboxPurge deletes inbox rows processed more than olderThan ago (inbox purge).
func InboxPurge(ctx context.Context, db Postgres, olderThan time.Duration) (*PurgeResult, error) {
	if olderThan <= 0 {
		return nil, errors.New("ctl: inbox purge: retention must be positive")
	}
	n, err := db.InboxPurge(ctx, olderThan)
	if err != nil {
		return nil, err
	}
	return &PurgeResult{Table: "mediator_inbox", OlderThan: olderThan.String(), Deleted: n}, nil
}

// IdemPurge deletes expired idempotency rows (idem purge).
func IdemPurge(ctx context.Context, db Postgres) (*PurgeResult, error) {
	n, err := db.IdemPurge(ctx)
	if err != nil {
		return nil, err
	}
	return &PurgeResult{Table: "mediator_idempotency", Deleted: n}, nil
}

// IdemShow returns the idempotency row of (scope, key) (idem show). A missing
// row is an error with mediator.CodeNotFound. now decides Expired.
func IdemShow(ctx context.Context, db Postgres, scope, key string, now time.Time) (*IdemShowResult, error) {
	if scope == "" || key == "" {
		return nil, errors.New("ctl: idem show: scope and key are required")
	}
	info, err := db.IdemShow(ctx, scope, key)
	if err != nil {
		return nil, err
	}
	res := &IdemShowResult{
		Scope: info.Scope, Key: info.Key, RequestHash: hexBytes(info.RequestHash), Hits: info.Hits,
		CreatedAt: info.CreatedAt, ExpiresAt: info.ExpiresAt, Expired: !info.ExpiresAt.After(now),
	}
	if info.Response != nil {
		res.Response = jsontext.Value(info.Response)
	}
	return res, nil
}

// ConsumerLag reports the pending entries of every (group, topic, partition)
// from Redis and, when db is not nil, the unpublished outbox backlog of each
// partition (consumer lag).
func ConsumerLag(ctx context.Context, r Redis, db Postgres, groups, topics []string) (*ConsumerLagResult, error) {
	if len(groups) == 0 || len(topics) == 0 {
		return nil, errors.New("ctl: consumer lag: at least one group and one topic are required")
	}
	lag, err := r.ConsumerLag(ctx, groups, topics)
	if err != nil {
		return nil, err
	}
	type slot struct {
		topic     string
		partition int
	}
	unpublished := map[slot]int64{}
	if db != nil {
		stats, err := db.OutboxStats(ctx)
		if err != nil {
			return nil, err
		}
		for _, s := range stats {
			unpublished[slot{s.Topic, s.Partition}] = s.Unpublished
		}
	}
	res := &ConsumerLagResult{Rows: make([]ConsumerLagRow, 0, len(lag))}
	for _, l := range lag {
		consumers := l.Consumers
		if consumers == nil {
			consumers = map[string]int64{}
		}
		res.Rows = append(res.Rows, ConsumerLagRow{
			Group: l.Group, Topic: l.Topic, Partition: l.Partition, Pending: l.Pending,
			OldestAgeSeconds: seconds(l.OldestAge), Consumers: consumers,
			Unpublished: unpublished[slot{l.Topic, l.Partition}], Missing: l.Missing,
		})
	}
	return res, nil
}

// DLQList lists the dead letters of a group in stream order (dlq list).
func DLQList(ctx context.Context, r Redis, group string) (*DLQListResult, error) {
	if group == "" {
		return nil, errors.New("ctl: dlq list: group is required")
	}
	entries, err := r.DLQList(ctx, group)
	if err != nil {
		return nil, err
	}
	res := &DLQListResult{Group: group, Entries: make([]DLQRow, 0, len(entries))}
	for _, e := range entries {
		res.Entries = append(res.Entries, dlqRow(e))
	}
	return res, nil
}

func dlqRow(e redisx.DLQEntry) DLQRow {
	row := DLQRow{
		ID: e.ID, Stream: e.Stream, StreamID: e.StreamID, Attempts: e.Attempts, Error: e.Error, Node: e.Node, FailedAt: e.FailedAt,
		EventType: e.Envelope.Type, Topic: e.Envelope.Topic, Key: e.Envelope.StreamKey, Seq: e.Envelope.Seq,
		Partition: e.Envelope.Partition, OutboxID: e.OutboxID,
	}
	if e.Envelope.ID != [16]byte{} {
		row.EventID = e.Envelope.ID.String()
	}
	if e.DecodeErr != nil {
		row.DecodeError = e.DecodeErr.Error()
	}
	return row
}

// DLQRequeue re-adds a dead letter to its original partition stream (dlq requeue).
func DLQRequeue(ctx context.Context, r Redis, group, id string) (*DLQActionResult, error) {
	if group == "" || id == "" {
		return nil, errors.New("ctl: dlq requeue: group and id are required")
	}
	if err := r.DLQRequeue(ctx, group, id); err != nil {
		return nil, err
	}
	return &DLQActionResult{Group: group, ID: id, Action: "requeued"}, nil
}

// DLQDrop deletes a dead letter (dlq drop).
func DLQDrop(ctx context.Context, r Redis, group, id string) (*DLQActionResult, error) {
	if group == "" || id == "" {
		return nil, errors.New("ctl: dlq drop: group and id are required")
	}
	if err := r.DLQDrop(ctx, group, id); err != nil {
		return nil, err
	}
	return &DLQActionResult{Group: group, ID: id, Action: "dropped"}, nil
}

// DLQSkip acknowledges one pending entry on behalf of the group so a partition
// halted under StrictOrder moves past it (dlq skip).
func DLQSkip(ctx context.Context, r Redis, group, topic string, partition int, id string) (*PartitionSkipResult, error) {
	if group == "" || topic == "" || id == "" {
		return nil, errors.New("ctl: dlq skip: group, topic, and id are required")
	}
	if partition < 0 {
		return nil, errors.New("ctl: dlq skip: partition must not be negative")
	}
	if err := r.PartitionSkip(ctx, group, topic, partition, id); err != nil {
		return nil, err
	}
	return &PartitionSkipResult{Group: group, Topic: topic, Partition: partition, ID: id}, nil
}

// LeaseList lists the partition leases of a group (lease list).
func LeaseList(ctx context.Context, r Redis, group string) (*LeaseListResult, error) {
	if group == "" {
		return nil, errors.New("ctl: lease list: group is required")
	}
	leases, err := r.LeaseList(ctx, group)
	if err != nil {
		return nil, err
	}
	res := &LeaseListResult{Group: group, Leases: make([]LeaseRow, 0, len(leases))}
	for _, l := range leases {
		res.Leases = append(res.Leases, LeaseRow{Group: l.Group, Topic: l.Topic, Partition: l.Partition, Node: l.Node, Epoch: l.Epoch, TTLSeconds: seconds(l.TTL)})
	}
	return res, nil
}

// LeaseRelease deletes a lease to force a handover (lease release).
func LeaseRelease(ctx context.Context, r Redis, group, topic string, partition int) (*LeaseReleaseResult, error) {
	if group == "" || topic == "" {
		return nil, errors.New("ctl: lease release: group and topic are required")
	}
	if partition < 0 {
		return nil, errors.New("ctl: lease release: partition must not be negative")
	}
	if err := r.LeaseRelease(ctx, group, topic, partition); err != nil {
		return nil, err
	}
	return &LeaseReleaseResult{Group: group, Topic: topic, Partition: partition, Released: true}, nil
}

// OpenAPIExport generates the OpenAPI document of m and writes it to out
// (openapi export). Empty Info fields take the generator defaults.
func OpenAPIExport(m *mediator.Mediator, cfg openapi.Config, out string) (*OpenAPIExportResult, error) {
	return openAPIExport(m, cfg, out, exportOpenAPI)
}

// openAPIExport is OpenAPIExport with the exporter injected.
func openAPIExport(m *mediator.Mediator, cfg openapi.Config, out string, export func(*mediator.Mediator, openapi.Config, string) error) (*OpenAPIExportResult, error) {
	if out == "" {
		return nil, errors.New("ctl: openapi export: output path is required")
	}
	if m == nil {
		return nil, errors.New("ctl: openapi export: nil mediator")
	}
	if err := export(m, cfg, out); err != nil {
		return nil, fmt.Errorf("ctl: openapi export: %w", err)
	}
	title, version := cfg.Info.Title, cfg.Info.Version
	if title == "" {
		title = openapi.DefaultTitle
	}
	if version == "" {
		version = openapi.DefaultVersion
	}
	return &OpenAPIExportResult{Out: out, Title: title, Version: version, Prefix: cfg.Prefix}, nil
}
