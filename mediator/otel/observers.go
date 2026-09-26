package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/t3stackcoder/go-api-backend/mediator"
	"github.com/t3stackcoder/go-api-backend/mediator/pg"
	"github.com/t3stackcoder/go-api-backend/mediator/redisx"
)

// NewConsumersObserver adapts the instruments to redisx.Observer: pass it
// to redisx.WithObserver. It feeds mediator.lease.owned,
// mediator.lease.handovers, mediator.consumer.processed,
// mediator.consumer.halted, mediator.consumer.pending, mediator.consumer.lag,
// and mediator.dlq.size.
func NewConsumersObserver(inst *Instruments) redisx.Observer {
	return consumersObserver{inst: inst}
}

type consumersObserver struct{ inst *Instruments }

func scopeAttrs(group, topic string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String(AttrGroup, group), attribute.String(AttrTopic, topic))
}

func partitionAttrs(group, topic string, partition int) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String(AttrGroup, group), attribute.String(AttrTopic, topic), attribute.Int(AttrPartition, partition))
}

// LeaseAcquired counts a handover.
func (o consumersObserver) LeaseAcquired(group, topic string, _ int, _ int64) {
	o.inst.LeaseHandovers.Add(context.Background(), 1, scopeAttrs(group, topic))
}

// LeaseEnded has no instrument of its own; lease.owned reflects it.
func (o consumersObserver) LeaseEnded(string, string, int, string) {}

// LeasesOwned records the owned gauge.
func (o consumersObserver) LeasesOwned(group, topic string, owned int) {
	o.inst.LeaseOwned.Record(context.Background(), int64(owned), scopeAttrs(group, topic))
}

// Processed counts one delivery outcome.
func (o consumersObserver) Processed(group, outcome string) {
	o.inst.ConsumerProcessed.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String(AttrGroup, group), attribute.String(AttrOutcome, outcome)))
}

// Halted records the halted gauge of a partition.
func (o consumersObserver) Halted(group, topic string, partition int, halted bool) {
	var v int64
	if halted {
		v = 1
	}
	o.inst.ConsumerHalted.Record(context.Background(), v, partitionAttrs(group, topic, partition))
}

// Pending records the pending count and lag of a partition.
func (o consumersObserver) Pending(group, topic string, partition int, pending int64, oldest time.Duration) {
	attrs := partitionAttrs(group, topic, partition)
	o.inst.ConsumerPending.Record(context.Background(), pending, attrs)
	o.inst.ConsumerLag.Record(context.Background(), oldest.Seconds(), attrs)
}

// DLQSize records the dead-letter stream length of a group.
func (o consumersObserver) DLQSize(group string, size int64) {
	o.inst.DLQSize.Record(context.Background(), size, metric.WithAttributes(attribute.String(AttrGroup, group)))
}

// NewIdempotencyObserver returns the pg.IdempotencyConfig.Observer that
// counts mediator.idempotency.outcome.
func NewIdempotencyObserver(inst *Instruments) func(name, outcome string) {
	return func(name, outcome string) {
		inst.IdempotencyOutcome.Add(context.Background(), 1,
			metric.WithAttributes(attribute.String(AttrName, name), attribute.String(AttrOutcome, outcome)))
	}
}

// RelayStats is what RelayCollector reads; *pg.Relay satisfies it.
type RelayStats interface {
	Stats() pg.RelayStats
}

// RelayCollector registers an asynchronous callback that observes
// mediator.outbox.unpublished and mediator.outbox.oldest_unpublished_age per
// owned slot, and mediator.relay.published and mediator.relay.replayed, from
// relay.Stats(). The relay keeps its counters as process totals, so the two
// counters carry no topic and partition attributes. Unregister the returned
// registration when the relay stops.
func RelayCollector(inst *Instruments, relay RelayStats) (metric.Registration, error) {
	return inst.meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		st := relay.Stats()
		o.ObserveInt64(inst.RelayPublished, st.Published)
		o.ObserveInt64(inst.RelayReplayed, st.Replayed)
		for _, s := range st.Slots {
			if !s.Owned {
				continue
			}
			attrs := metric.WithAttributes(attribute.String(AttrTopic, s.Topic), attribute.Int(AttrPartition, s.Partition))
			o.ObserveInt64(inst.OutboxUnpublished, s.Unpublished, attrs)
			o.ObserveFloat64(inst.OutboxOldestUnpublishedAge, s.OldestAge.Seconds(), attrs)
		}
		return nil
	}, inst.RelayPublished, inst.RelayReplayed, inst.OutboxUnpublished, inst.OutboxOldestUnpublishedAge)
}

// InstrumentRemote wraps a remote dispatcher so every Send records
// mediator.remote.duration with the request name and outcome (ok, error,
// or timeout). redisx.Remote exposes no observer hook, so this decorator is
// how the metric is produced: pass the result to mediator.WithRemote.
func InstrumentRemote(inst *Instruments, next mediator.RemoteDispatcher) mediator.RemoteDispatcher {
	return &remoteObserver{inst: inst, next: next}
}

type remoteObserver struct {
	inst *Instruments
	next mediator.RemoteDispatcher
}

func (r *remoteObserver) Send(ctx context.Context, info *mediator.RequestInfo, req any) (any, error) {
	start := time.Now()
	res, err := r.next.Send(ctx, info, req)
	outcome := RemoteOutcomeOK
	switch {
	case err == nil:
	case mediator.CodeOf(err) == mediator.CodeTimeout:
		outcome = RemoteOutcomeTimeout
	default:
		outcome = RemoteOutcomeError
	}
	r.inst.RemoteDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(attribute.String(AttrName, info.Name), attribute.String(AttrOutcome, outcome)))
	return res, err
}
