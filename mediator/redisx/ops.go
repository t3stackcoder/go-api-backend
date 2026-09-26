package redisx

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/testkit"
)

// DLQEntry is one dead letter (spec 7.3, 9.3).
type DLQEntry struct {
	// ID is the entry's ID in the dead-letter stream.
	ID string
	// Group is the consumer group that gave up.
	Group string
	// Stream and StreamID locate the original entry.
	Stream   string
	StreamID string
	// Attempts is the delivery count at the time of dead-lettering.
	Attempts int
	// Error is the last error text.
	Error string
	// Node dead-lettered the entry at FailedAt.
	Node     string
	FailedAt time.Time
	// Envelope, Payload, and OutboxID are decoded from the original fields;
	// DecodeErr is set when they could not be.
	Envelope  mediator.Envelope
	Payload   []byte
	OutboxID  int64
	DecodeErr error
	// Fields are the original entry fields, without the dlq_* additions.
	Fields map[string]string
}

func parseDLQ(msg redis.XMessage, group string) DLQEntry {
	e := DLQEntry{ID: msg.ID, Group: group, Fields: map[string]string{}}
	orig := map[string]any{}
	for k, v := range msg.Values {
		s := fmt.Sprint(v)
		switch k {
		case DLQFieldError:
			e.Error = s
		case DLQFieldAttempts:
			e.Attempts, _ = strconv.Atoi(s)
		case DLQFieldStreamID:
			e.StreamID = s
		case DLQFieldStream:
			e.Stream = s
		case DLQFieldGroup:
			if s != "" {
				e.Group = s
			}
		case DLQFieldNode:
			e.Node = s
		case DLQFieldAt:
			e.FailedAt, _ = time.Parse(time.RFC3339Nano, s)
		default:
			e.Fields[k] = s
			orig[k] = v
		}
	}
	e.Envelope, e.Payload, e.OutboxID, e.DecodeErr = DecodeEntry(orig)
	return e
}

// DLQList returns every dead letter of the group in stream order.
func DLQList(ctx context.Context, client *redis.Client, cfg Config, group string) ([]DLQEntry, error) {
	cfg = cfg.WithDefaults()
	msgs, err := client.XRange(ctx, cfg.Keys().DLQ(group), "-", "+").Result()
	if err != nil {
		return nil, fmt.Errorf("redisx: dlq list %s: %w", group, err)
	}
	out := make([]DLQEntry, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, parseDLQ(m, group))
	}
	return out, nil
}

// ErrDLQEntryNotFound is returned by DLQRequeue and DLQDrop for an unknown ID.
var ErrDLQEntryNotFound = errors.New("redisx: dead-letter entry not found")

// DLQRequeue re-adds a dead letter to its original partition stream and
// removes it from the DLQ. The entry gets a new stream ID and a fresh
// delivery count, so the consumer group processes it again after any entries
// added since; per-key order relative to those is not restored.
func DLQRequeue(ctx context.Context, client *redis.Client, cfg Config, group, id string) error {
	cfg = cfg.WithDefaults()
	keys := cfg.Keys()
	msgs, err := client.XRangeN(ctx, keys.DLQ(group), id, id, 1).Result()
	if err != nil {
		return fmt.Errorf("redisx: dlq requeue %s: %w", id, err)
	}
	if len(msgs) == 0 {
		return fmt.Errorf("%w: %s in group %s", ErrDLQEntryNotFound, id, group)
	}
	e := parseDLQ(msgs[0], group)
	stream := e.Stream
	if stream == "" {
		if e.DecodeErr != nil {
			return fmt.Errorf("redisx: dlq requeue %s: no original stream recorded and %w", id, e.DecodeErr)
		}
		stream = keys.Stream(e.Envelope.Topic, e.Envelope.Partition)
	}
	values := make(map[string]any, len(e.Fields))
	for k, v := range e.Fields {
		values[k] = v
	}
	if err := testkit.Fault(ctx, "redis.xadd"); err != nil {
		return err
	}
	if err := client.XAdd(ctx, &redis.XAddArgs{Stream: stream, Values: values}).Err(); err != nil {
		return fmt.Errorf("redisx: dlq requeue %s to %s: %w", id, stream, err)
	}
	if err := testkit.FaultAfter(ctx, "redis.xadd"); err != nil {
		return err
	}
	if err := client.XDel(ctx, keys.DLQ(group), id).Err(); err != nil {
		return fmt.Errorf("redisx: dlq requeue %s: delete: %w", id, err)
	}
	return nil
}

// DLQDrop deletes a dead letter.
func DLQDrop(ctx context.Context, client *redis.Client, cfg Config, group, id string) error {
	cfg = cfg.WithDefaults()
	n, err := client.XDel(ctx, cfg.Keys().DLQ(group), id).Result()
	if err != nil {
		return fmt.Errorf("redisx: dlq drop %s: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: %s in group %s", ErrDLQEntryNotFound, id, group)
	}
	return nil
}

// LeaseInfo describes one partition lease.
type LeaseInfo struct {
	Group     string
	Topic     string
	Partition int
	Node      string
	Epoch     int64
	// TTL is the remaining lifetime.
	TTL time.Duration
}

// LeaseList returns the leases of a group (every topic and partition), sorted.
func LeaseList(ctx context.Context, client *redis.Client, cfg Config, group string) ([]LeaseInfo, error) {
	cfg = cfg.WithDefaults()
	keys := cfg.Keys()
	var names []string
	iter := client.Scan(ctx, 0, keys.LeasePattern(group), 100).Iterator()
	for iter.Next(ctx) {
		names = append(names, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redisx: lease list %s: %w", group, err)
	}
	var out []LeaseInfo
	for _, name := range names {
		g, topic, partition, ok := keys.ParseLease(name)
		if !ok || g != group {
			continue
		}
		pipe := client.Pipeline()
		val := pipe.Get(ctx, name)
		ttl := pipe.PTTL(ctx, name)
		if _, err := pipe.Exec(ctx); err != nil {
			if errors.Is(err, redis.Nil) {
				continue // expired between SCAN and GET
			}
			return nil, fmt.Errorf("redisx: lease list %s: %w", name, err)
		}
		node, epoch, _ := ParseLeaseValue(val.Val())
		out = append(out, LeaseInfo{Group: g, Topic: topic, Partition: partition, Node: node, Epoch: epoch, TTL: ttl.Val()})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out, nil
}

// LeaseRelease deletes a lease to force a handover. The current owner
// notices at its next renewal and stops reading the partition; the next
// owner drains the pending entries first (spec 7.2).
func LeaseRelease(ctx context.Context, client *redis.Client, cfg Config, group, topic string, partition int) error {
	cfg = cfg.WithDefaults()
	if err := client.Del(ctx, cfg.Keys().Lease(group, topic, partition)).Err(); err != nil {
		return fmt.Errorf("redisx: lease release: %w", err)
	}
	return nil
}

// LagInfo is the pending summary of one (group, topic, partition).
type LagInfo struct {
	Group     string
	Topic     string
	Partition int
	// Pending is the number of delivered, unacknowledged entries.
	Pending int64
	// OldestAge is the age of the oldest pending entry, from its stream ID.
	OldestAge time.Duration
	// Consumers is the pending count per consumer name.
	Consumers map[string]int64
	// Missing is set when the stream or group does not exist.
	Missing bool
}

// ConsumerLag returns the XPENDING summary of every (group, topic, partition).
func ConsumerLag(ctx context.Context, client *redis.Client, cfg Config, groups, topics []string) ([]LagInfo, error) {
	cfg = cfg.WithDefaults()
	keys := cfg.Keys()
	now := time.Now()
	var out []LagInfo
	for _, g := range groups {
		for _, t := range topics {
			for p := 0; p < cfg.PartitionsPerTopic; p++ {
				info := LagInfo{Group: g, Topic: t, Partition: p, Consumers: map[string]int64{}}
				res, err := client.XPending(ctx, keys.Stream(t, p), g).Result()
				switch {
				case err != nil && (isNoGroup(err) || strings.Contains(err.Error(), "no such key")):
					info.Missing = true
				case err != nil:
					return nil, fmt.Errorf("redisx: consumer lag %s/%s/p%d: %w", g, t, p, err)
				default:
					info.Pending = res.Count
					if res.Count > 0 {
						if ms, ok := streamIDMillis(res.Lower); ok {
							info.OldestAge = now.Sub(time.UnixMilli(ms))
							if info.OldestAge < 0 {
								info.OldestAge = 0
							}
						}
					}
					for c, n := range res.Consumers {
						info.Consumers[c] = n
					}
				}
				out = append(out, info)
			}
		}
	}
	return out, nil
}

// PartitionSkip acknowledges one entry on behalf of the group so a partition
// halted under StrictOrder resumes past a poison entry. The halted worker
// notices within LeaseRenew.
func PartitionSkip(ctx context.Context, client *redis.Client, cfg Config, group, topic string, partition int, id string) error {
	cfg = cfg.WithDefaults()
	n, err := client.XAck(ctx, cfg.Keys().Stream(topic, partition), group, id).Result()
	if err != nil {
		return fmt.Errorf("redisx: partition skip %s: %w", id, err)
	}
	if n == 0 {
		return fmt.Errorf("redisx: partition skip: entry %s is not pending for group %s", id, group)
	}
	return nil
}
